package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// OperationJobHandler runs one attempt of a job. Returning nil completes the
// job; returning an error retries it with backoff until the attempt budget is
// spent. The handler's context is canceled when the job's cancellation is
// requested or its lease is lost — a handler that observes ctx promptly keeps
// takeover and cancel semantics tight.
type OperationJobHandler func(ctx context.Context, job OperationJob) (resultRef string, err error)

type OperationJobWorker struct {
	Repo         OperationJobRepository
	JobType      string
	Owner        string
	Handler      OperationJobHandler
	PollInterval time.Duration // default 2s
	LeaseFor     time.Duration // default 30s; heartbeat every third
	MaxAttempts  uint32        // default 5
	RetryBase    time.Duration // default 30s, doubled per attempt, capped 10m
	Logf         func(format string, args ...any)
}

func (w *OperationJobWorker) logf(format string, args ...any) {
	if w.Logf != nil {
		w.Logf(format, args...)
	}
}

// Run polls for due jobs until the context ends. It never returns an error:
// repository hiccups are logged and retried on the next poll.
func (w *OperationJobWorker) Run(ctx context.Context) {
	if w == nil || w.Repo == nil || w.JobType == "" || w.Owner == "" || w.Handler == nil {
		return
	}
	poll := w.PollInterval
	if poll <= 0 {
		poll = 2 * time.Second
	}
	for {
		job, err := w.Repo.LeaseNextOperationJob(ctx, w.JobType, w.Owner, w.leaseFor())
		switch {
		case err == nil:
			w.runAttempt(ctx, job)
			continue // look for the next job immediately
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

func (w *OperationJobWorker) leaseFor() time.Duration {
	if w.LeaseFor > 0 {
		return w.LeaseFor
	}
	return 30 * time.Second
}

func (w *OperationJobWorker) runAttempt(ctx context.Context, job OperationJob) {
	handlerCtx, cancelHandler := context.WithCancel(ctx)
	defer cancelHandler()

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

	// A job leased in cancel_requested state (takeover after the previous
	// owner died mid-cancel) starts already canceled.
	cancelRequested := job.Status == OperationJobStatusCancelRequested
	if cancelRequested {
		cancelHandler()
	}
	leaseLost := false
	for {
		select {
		case <-ticker.C:
			requested, err := w.Repo.HeartbeatOperationJob(ctx, job.ID, job.LeaseToken, w.leaseFor(), job.ProgressDone, nil)
			if errors.Is(err, ErrOperationJobLeaseLost) {
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
				// Another owner holds the job now; it is not ours to finish.
				return
			}
			w.finishAttempt(ctx, job, cancelRequested, result.resultRef, result.err)
			return
		}
	}
}

func (w *OperationJobWorker) finishAttempt(ctx context.Context, job OperationJob, cancelRequested bool, resultRef string, handlerErr error) {
	switch {
	case cancelRequested:
		if err := w.Repo.CompleteOperationJobCanceled(ctx, job.ID, job.LeaseToken); err != nil {
			w.logf("operation job %s cancel finish: %v", job.ID, err)
		}
	case handlerErr == nil:
		if err := w.Repo.CompleteOperationJobSucceeded(ctx, job.ID, job.LeaseToken, resultRef); err != nil {
			w.logf("operation job %s success finish: %v", job.ID, err)
		}
	default:
		maxAttempts := w.MaxAttempts
		if maxAttempts == 0 {
			maxAttempts = 5
		}
		retry := job.AttemptCount < maxAttempts
		retryAt := time.Now().UTC().Add(w.retryBackoff(job.AttemptCount))
		detail := fmt.Sprintf("attempt %d: %v", job.AttemptCount, handlerErr)
		if err := w.Repo.CompleteOperationJobFailed(ctx, job.ID, job.LeaseToken, "HANDLER_FAILED", detail, retry, retryAt); err != nil {
			w.logf("operation job %s failure finish: %v", job.ID, err)
		}
	}
}

func (w *OperationJobWorker) retryBackoff(attempt uint32) time.Duration {
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

// TargetDeleteJobType names the async target deletion job (PLAT-04).
const TargetDeleteJobType = "target_delete"

type targetDeleteJobPayload struct {
	TargetID ID `json:"target_id"`
}

// NewTargetDeleteJobHandler deletes the target and its VictoriaMetrics series.
// Both steps are idempotent, so a retried or taken-over attempt converges.
func NewTargetDeleteJobHandler(targets TargetRepository, cleaner SeriesCleaner) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		var payload targetDeleteJobPayload
		if err := json.Unmarshal(job.CheckpointJSON, &payload); err != nil || payload.TargetID == "" {
			return "", fmt.Errorf("target delete job payload is invalid: %v", err)
		}
		if err := targets.DeleteTarget(ctx, job.TenantID, payload.TargetID); err != nil {
			return "", err
		}
		if cleaner != nil {
			if err := cleaner.DeleteSeries(ctx, []string{`{target_id="` + string(payload.TargetID) + `"}`}); err != nil {
				return "", err
			}
		}
		return "deleted:" + string(payload.TargetID), nil
	}
}
