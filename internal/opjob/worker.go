package opjob

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Handler runs one attempt of a job. Returning nil completes the job; returning
// an error retries it with backoff until the attempt budget is spent. The
// handler's context is canceled when the job's cancellation is requested or its
// lease is lost — a handler that observes ctx promptly keeps takeover and cancel
// semantics tight.
type Handler func(ctx context.Context, job Job) (resultRef string, err error)

// Reporter is the attempt-scoped handle a handler uses to publish progress and
// resumable checkpoints while it runs. It is bound to one lease (job ID + lease
// token), so every write is fenced: once the lease is lost to takeover or expiry
// the write is rejected (ErrLeaseLost) and the handler learns it must abandon the
// attempt. Progress is monotonic; a nil checkpoint advances progress without
// disturbing the stored checkpoint. A handler retrieves it with
// ReporterFromContext; a handler invoked directly (e.g. in a test) gets nil and
// Report is then a safe no-op.
type Reporter struct {
	repo       Repository
	jobID      string
	leaseToken string
	leaseFor   time.Duration

	mu         sync.Mutex
	progress   uint64
	checkpoint json.RawMessage
}

// Report records progress (monotonic) and, when non-nil, the latest resumable
// checkpoint, then persists both under the lease. ErrLeaseLost means the lease
// is gone and the handler should stop.
func (r *Reporter) Report(ctx context.Context, done uint64, checkpoint json.RawMessage) error {
	if r == nil {
		return nil
	}
	_, err := r.persist(ctx, done, checkpoint)
	return err
}

func (r *Reporter) persist(ctx context.Context, done uint64, checkpoint json.RawMessage) (bool, error) {
	r.mu.Lock()
	if done > r.progress {
		r.progress = done
	}
	if len(checkpoint) > 0 {
		r.checkpoint = checkpoint
	}
	progress, cp := r.progress, r.checkpoint
	r.mu.Unlock()
	return r.repo.Heartbeat(ctx, r.jobID, r.leaseToken, r.leaseFor, progress, cp)
}

func (r *Reporter) flush(ctx context.Context) (bool, error) {
	return r.persist(ctx, 0, nil)
}

type reporterKey struct{}

// ReporterFromContext returns the running attempt's progress reporter, or nil
// when none is installed.
func ReporterFromContext(ctx context.Context) *Reporter {
	reporter, _ := ctx.Value(reporterKey{}).(*Reporter)
	return reporter
}

// Worker polls for due jobs of one type and runs them through Handler.
type Worker struct {
	Repo         Repository
	JobType      string
	Owner        string
	Handler      Handler
	PollInterval time.Duration // default 2s
	LeaseFor     time.Duration // default 30s; heartbeat every third
	MaxAttempts  uint32        // default 5
	RetryBase    time.Duration // default 30s, doubled per attempt, capped 10m
	Logf         func(format string, args ...any)
	// OnTerminalFailure, when set, fires once after a job of this type is durably
	// recorded as terminally failed. code is CodeTerminal for a non-retryable
	// error, otherwise the retryable code that exhausted the budget.
	OnTerminalFailure func(job Job, code string)
}

func (w *Worker) logf(format string, args ...any) {
	if w.Logf != nil {
		w.Logf(format, args...)
	}
}

// Run polls for due jobs until the context ends. It never returns an error:
// repository hiccups are logged and retried on the next poll.
func (w *Worker) Run(ctx context.Context) {
	if w == nil || w.Repo == nil || w.JobType == "" || w.Owner == "" || w.Handler == nil {
		return
	}
	poll := w.PollInterval
	if poll <= 0 {
		poll = 2 * time.Second
	}
	for {
		job, err := w.Repo.LeaseNext(ctx, w.JobType, w.Owner, w.leaseFor())
		switch {
		case err == nil:
			w.runAttempt(ctx, job)
			continue
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return
		case !errors.Is(err, sql.ErrNoRows):
			w.logf("operation job worker %s lease: %v", w.JobType, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(poll):
		}
	}
}

func (w *Worker) leaseFor() time.Duration {
	if w.LeaseFor > 0 {
		return w.LeaseFor
	}
	return 30 * time.Second
}

func (w *Worker) runAttempt(ctx context.Context, job Job) {
	handlerCtx, cancelHandler := context.WithCancel(ctx)
	defer cancelHandler()

	// The reporter starts at the leased job's persisted progress/checkpoint so a
	// takeover resumes forward from where the previous owner left off, fenced by
	// this attempt's lease token.
	reporter := &Reporter{
		repo:       w.Repo,
		jobID:      job.ID,
		leaseToken: job.LeaseToken,
		leaseFor:   w.leaseFor(),
		progress:   job.ProgressDone,
		checkpoint: job.CheckpointJSON,
	}
	handlerCtx = context.WithValue(handlerCtx, reporterKey{}, reporter)

	type attemptResult struct {
		resultRef string
		err       error
	}
	done := make(chan attemptResult, 1)
	go func() {
		resultRef, err := w.Handler(handlerCtx, job)
		done <- attemptResult{resultRef: resultRef, err: err}
	}()

	heartbeatEvery := w.leaseFor() / 3
	if heartbeatEvery <= 0 {
		heartbeatEvery = time.Second
	}
	ticker := time.NewTicker(heartbeatEvery)
	defer ticker.Stop()

	// A job leased in cancel_requested state (takeover after the previous owner
	// died mid-cancel) starts already canceled.
	cancelRequested := job.Status == StatusCancelRequested
	if cancelRequested {
		cancelHandler()
	}
	leaseLost := false
	for {
		select {
		case <-ticker.C:
			requested, err := reporter.flush(ctx)
			if errors.Is(err, ErrLeaseLost) {
				leaseLost = true
				cancelHandler()
				continue
			}
			if err != nil {
				w.logf("operation job %s heartbeat: %v", job.ID, err)
				continue
			}
			if requested && !cancelRequested {
				cancelRequested = true
				cancelHandler()
			}
		case result := <-done:
			if leaseLost {
				return // another owner holds the job now
			}
			w.finishAttempt(ctx, job, cancelRequested, result.resultRef, result.err)
			return
		}
	}
}

func (w *Worker) finishAttempt(ctx context.Context, job Job, cancelRequested bool, resultRef string, handlerErr error) {
	switch {
	case cancelRequested:
		if err := w.Repo.CompleteCanceled(ctx, job.ID, job.LeaseToken); err != nil {
			w.logf("operation job %s cancel finish: %v", job.ID, err)
		}
	case handlerErr == nil:
		if err := w.Repo.CompleteSucceeded(ctx, job.ID, job.LeaseToken, resultRef); err != nil {
			w.logf("operation job %s success finish: %v", job.ID, err)
		}
	default:
		maxAttempts := w.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = 5
		}
		// A terminal error (bad payload, unknown schema version) will not fix
		// itself on retry, so it fails immediately regardless of the budget.
		code := "HANDLER_FAILED"
		retry := job.AttemptCount < maxAttempts
		if IsTerminalError(handlerErr) {
			retry = false
			code = CodeTerminal
		}
		retryAt := time.Now().UTC().Add(w.retryBackoff(job.AttemptCount))
		detail := fmt.Sprintf("attempt %d: %v", job.AttemptCount, handlerErr)
		if err := w.Repo.CompleteFailed(ctx, job.ID, job.LeaseToken, code, detail, retry, retryAt); err != nil {
			w.logf("operation job %s failure finish: %v", job.ID, err)
			return
		}
		if !retry && w.OnTerminalFailure != nil {
			w.OnTerminalFailure(job, code)
		}
	}
}

func (w *Worker) retryBackoff(attempt uint32) time.Duration {
	base := w.RetryBase
	if base <= 0 {
		base = 30 * time.Second
	}
	backoff := base
	for i := uint32(1); i < attempt && backoff < 10*time.Minute; i++ {
		backoff *= 2
	}
	if backoff > 10*time.Minute {
		backoff = 10 * time.Minute
	}
	return backoff
}

// terminalError marks a handler failure that a retry cannot fix (a malformed or
// wrong-version payload). The worker fails such a job immediately.
type terminalError struct{ err error }

func (e terminalError) Error() string { return e.err.Error() }
func (e terminalError) Unwrap() error { return e.err }

// TerminalError wraps err so the worker treats it as non-retryable.
func TerminalError(err error) error {
	if err == nil {
		return nil
	}
	return terminalError{err: err}
}

func IsTerminalError(err error) bool {
	var terminal terminalError
	return errors.As(err, &terminal)
}

// jobPayloadEnvelope versions every job payload so a future payload change can be
// recognized rather than silently misparsed.
type jobPayloadEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	Payload       json.RawMessage `json:"payload"`
}

// EncodePayload wraps a typed payload in a versioned envelope for storage in
// checkpoint_json.
func EncodePayload(schemaVersion int, payload any) (json.RawMessage, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(jobPayloadEnvelope{SchemaVersion: schemaVersion, Payload: raw})
}

// DecodePayload reads the envelope, rejects any schema version the caller does
// not support (a terminal error), and unmarshals the inner payload into out.
func DecodePayload(checkpoint json.RawMessage, supported int, out any) error {
	var envelope jobPayloadEnvelope
	if err := json.Unmarshal(checkpoint, &envelope); err != nil {
		return TerminalError(fmt.Errorf("job payload envelope is malformed: %w", err))
	}
	if envelope.SchemaVersion != supported {
		return TerminalError(fmt.Errorf("unsupported job payload schema version %d (handler supports %d)", envelope.SchemaVersion, supported))
	}
	if err := json.Unmarshal(envelope.Payload, out); err != nil {
		return TerminalError(fmt.Errorf("job payload is malformed: %w", err))
	}
	return nil
}
