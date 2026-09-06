package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// OperationJobHandler runs one attempt of a job. Returning nil completes the
// job; returning an error retries it with backoff until the attempt budget is
// spent. The handler's context is canceled when the job's cancellation is
// requested or its lease is lost — a handler that observes ctx promptly keeps
// takeover and cancel semantics tight.
type OperationJobHandler func(ctx context.Context, job OperationJob) (resultRef string, err error)

// OperationJobReporter is the attempt-scoped handle a handler uses to publish
// progress and resumable checkpoints while it runs. It is bound to one lease
// (job ID + lease token), so every write is fenced: once the lease is lost to
// takeover or expiry the write is rejected (ErrOperationJobLeaseLost) and the
// handler learns it must abandon the attempt. Progress is monotonic — a value
// below what was already reported is ignored — and a nil checkpoint advances
// progress without disturbing the stored checkpoint. A handler retrieves it
// with OperationJobReporterFromContext; a handler invoked directly (e.g. in a
// unit test) gets nil and Report is then a safe no-op.
type OperationJobReporter struct {
	repo       OperationJobRepository
	jobID      ID
	leaseToken string
	leaseFor   time.Duration

	mu         sync.Mutex
	progress   uint64
	checkpoint json.RawMessage
}

// Report records progress (monotonic) and, when non-nil, the latest resumable
// checkpoint, then persists both under the lease. ErrOperationJobLeaseLost
// means the lease is gone and the handler should stop; the attempt-scoped
// binding guarantees a superseded attempt cannot overwrite the new owner's
// state.
func (r *OperationJobReporter) Report(ctx context.Context, done uint64, checkpoint json.RawMessage) error {
	if r == nil {
		return nil
	}
	_, err := r.persist(ctx, done, checkpoint)
	return err
}

// persist merges done/checkpoint into the reporter's state under the mutex and
// flushes the whole state in one fenced heartbeat. It returns whether
// cancellation has been requested so the worker's background heartbeat can
// drive handler cancellation from the same write. Serializing every write
// through the mutex keeps progress monotonic even though the handler and the
// heartbeat ticker flush concurrently.
func (r *OperationJobReporter) persist(ctx context.Context, done uint64, checkpoint json.RawMessage) (bool, error) {
	r.mu.Lock()
	if done > r.progress {
		r.progress = done
	}
	if len(checkpoint) > 0 {
		r.checkpoint = checkpoint
	}
	progress, cp := r.progress, r.checkpoint
	r.mu.Unlock()
	return r.repo.HeartbeatOperationJob(ctx, r.jobID, r.leaseToken, r.leaseFor, progress, cp)
}

// flush persists the reporter's current state without advancing it — the
// worker's heartbeat ticker uses it to keep the lease alive and push the
// handler's latest reported progress/checkpoint.
func (r *OperationJobReporter) flush(ctx context.Context) (bool, error) {
	return r.persist(ctx, 0, nil)
}

type operationJobReporterKey struct{}

// OperationJobReporterFromContext returns the running attempt's progress
// reporter, or nil when none is installed.
func OperationJobReporterFromContext(ctx context.Context) *OperationJobReporter {
	reporter, _ := ctx.Value(operationJobReporterKey{}).(*OperationJobReporter)
	return reporter
}

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

	// The reporter starts at the leased job's persisted progress/checkpoint so a
	// takeover resumes forward from where the previous owner left off, and is
	// fenced by this attempt's lease token.
	reporter := &OperationJobReporter{
		repo:       w.Repo,
		jobID:      job.ID,
		leaseToken: job.LeaseToken,
		leaseFor:   w.leaseFor(),
		progress:   job.ProgressDone,
		checkpoint: job.CheckpointJSON,
	}
	handlerCtx = context.WithValue(handlerCtx, operationJobReporterKey{}, reporter)

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
			// Flush the reporter's latest state rather than the job's static
			// snapshot, so progress and checkpoints the handler published are
			// persisted and the lease stays alive.
			requested, err := reporter.flush(ctx)
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
		// A terminal error (bad payload, unknown schema version) will not fix
		// itself on retry, so it fails immediately regardless of the budget.
		code := "HANDLER_FAILED"
		retry := job.AttemptCount < maxAttempts
		if IsTerminalJobError(handlerErr) {
			retry = false
			code = "TERMINAL"
		}
		retryAt := time.Now().UTC().Add(w.retryBackoff(job.AttemptCount))
		detail := fmt.Sprintf("attempt %d: %v", job.AttemptCount, handlerErr)
		if err := w.Repo.CompleteOperationJobFailed(ctx, job.ID, job.LeaseToken, code, detail, retry, retryAt); err != nil {
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

// terminalJobError marks a handler failure that a retry cannot fix (a
// malformed or wrong-version payload). The worker fails such a job
// immediately instead of consuming its retry budget.
type terminalJobError struct{ err error }

func (e terminalJobError) Error() string { return e.err.Error() }
func (e terminalJobError) Unwrap() error { return e.err }

// TerminalJobError wraps err so the worker treats it as non-retryable.
func TerminalJobError(err error) error {
	if err == nil {
		return nil
	}
	return terminalJobError{err: err}
}

func IsTerminalJobError(err error) bool {
	var terminal terminalJobError
	return errors.As(err, &terminal)
}

// jobPayloadEnvelope versions every job payload so a future payload change can
// be recognized rather than silently misparsed. schema_version is the payload
// contract for this job type, independent of the plan/API versions.
type jobPayloadEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	Payload       json.RawMessage `json:"payload"`
}

// EncodeJobPayload wraps a typed payload in a versioned envelope for storage
// in operation_jobs.checkpoint_json.
func EncodeJobPayload(schemaVersion int, payload any) (json.RawMessage, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(jobPayloadEnvelope{SchemaVersion: schemaVersion, Payload: raw})
}

// DecodeJobPayload reads the envelope, rejects any schema version the caller
// does not support (a terminal error), and unmarshals the inner payload into
// out. Legacy jobs written without an envelope are read as schema version 0.
func DecodeJobPayload(checkpoint json.RawMessage, supported int, out any) error {
	var envelope jobPayloadEnvelope
	if err := json.Unmarshal(checkpoint, &envelope); err != nil {
		return TerminalJobError(fmt.Errorf("job payload envelope is malformed: %w", err))
	}
	if envelope.SchemaVersion != supported {
		return TerminalJobError(fmt.Errorf("unsupported job payload schema version %d (handler supports %d)", envelope.SchemaVersion, supported))
	}
	if err := json.Unmarshal(envelope.Payload, out); err != nil {
		return TerminalJobError(fmt.Errorf("job payload is malformed: %w", err))
	}
	return nil
}

// TargetDeleteJobType names the async target deletion job (PLAT-04).
const TargetDeleteJobType = "target_delete"

// TargetDeletePayloadVersion is the payload schema version for target_delete.
// v2 carries the delete-preview impact summary so the destruction receipt can
// record what was removed after the rows are gone.
const TargetDeletePayloadVersion = 2

type targetDeleteJobPayload struct {
	TargetID ID             `json:"target_id"`
	Impact   map[string]int `json:"impact,omitempty"`
}

// EncodeTargetDeletePayload builds the versioned envelope the enqueue side
// stores, so the handler and enqueue agree on the payload contract.
func EncodeTargetDeletePayload(targetID ID, impact map[string]int) (json.RawMessage, error) {
	return EncodeJobPayload(TargetDeletePayloadVersion, targetDeleteJobPayload{TargetID: targetID, Impact: impact})
}

// NewTargetDeleteJobHandler deletes the target and its VictoriaMetrics series,
// then records a destruction receipt. Every step is idempotent, so a retried
// or taken-over attempt converges and writes exactly one receipt.
func NewTargetDeleteJobHandler(targets TargetRepository, cleaner SeriesCleaner, receipts DestructionReceiptRecorder) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		var payload targetDeleteJobPayload
		if err := DecodeJobPayload(job.CheckpointJSON, TargetDeletePayloadVersion, &payload); err != nil {
			return "", err
		}
		if payload.TargetID == "" {
			return "", TerminalJobError(fmt.Errorf("target delete job payload has no target_id"))
		}
		if err := targets.DeleteTarget(ctx, job.TenantID, payload.TargetID); err != nil {
			return "", err
		}
		seriesMatch := `{target_id="` + string(payload.TargetID) + `"}`
		if cleaner != nil {
			if err := cleaner.DeleteSeries(ctx, []string{seriesMatch}); err != nil {
				return "", err
			}
		}
		if receipts != nil {
			if err := receipts.RecordDestructionReceipt(ctx, DestructionReceipt{
				TenantID: job.TenantID, JobID: job.ID, ResourceType: "target",
				ResourceID: payload.TargetID, ActorID: job.CreatedBy,
				Impact: payload.Impact, SeriesMatch: seriesMatch, DestroyedAt: time.Now().UTC(),
			}); err != nil {
				return "", err
			}
		}
		return "deleted:" + string(payload.TargetID), nil
	}
}

// DeviceDeleteJobType names the async network-device deletion job (PLAT-04).
const DeviceDeleteJobType = "device_delete"

// DeviceDeletePayloadVersion is the payload schema version for device_delete.
const DeviceDeletePayloadVersion = 1

type deviceDeleteJobPayload struct {
	DeviceID ID             `json:"device_id"`
	Impact   map[string]int `json:"impact,omitempty"`
}

// EncodeDeviceDeletePayload builds the versioned envelope for a device delete.
func EncodeDeviceDeletePayload(deviceID ID, impact map[string]int) (json.RawMessage, error) {
	return EncodeJobPayload(DeviceDeletePayloadVersion, deviceDeleteJobPayload{DeviceID: deviceID, Impact: impact})
}

// NewDeviceDeleteJobHandler deletes the device (its dependents cascade), clears
// its VictoriaMetrics series and records a destruction receipt. Every step is
// idempotent so a retry or takeover converges and writes exactly one receipt.
func NewDeviceDeleteJobHandler(network NetworkRepository, cleaner SeriesCleaner, receipts DestructionReceiptRecorder) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		var payload deviceDeleteJobPayload
		if err := DecodeJobPayload(job.CheckpointJSON, DeviceDeletePayloadVersion, &payload); err != nil {
			return "", err
		}
		if payload.DeviceID == "" {
			return "", TerminalJobError(fmt.Errorf("device delete job payload has no device_id"))
		}
		if err := network.DeleteDevice(ctx, job.TenantID, payload.DeviceID); err != nil {
			return "", err
		}
		seriesMatch := `{device_id="` + string(payload.DeviceID) + `"}`
		if cleaner != nil {
			if err := cleaner.DeleteSeries(ctx, []string{seriesMatch}); err != nil {
				return "", err
			}
		}
		if receipts != nil {
			if err := receipts.RecordDestructionReceipt(ctx, DestructionReceipt{
				TenantID: job.TenantID, JobID: job.ID, ResourceType: "network_device",
				ResourceID: payload.DeviceID, ActorID: job.CreatedBy,
				Impact: payload.Impact, SeriesMatch: seriesMatch, DestroyedAt: time.Now().UTC(),
			}); err != nil {
				return "", err
			}
		}
		return "deleted:" + string(payload.DeviceID), nil
	}
}

// PortDeleteJobType names the async network-port deletion job (PLAT-04).
const PortDeleteJobType = "port_delete"

// PortDeletePayloadVersion is the payload schema version for port_delete.
const PortDeletePayloadVersion = 1

type portDeleteJobPayload struct {
	PortID ID             `json:"port_id"`
	Impact map[string]int `json:"impact,omitempty"`
}

// EncodePortDeletePayload builds the versioned envelope for a port delete.
func EncodePortDeletePayload(portID ID, impact map[string]int) (json.RawMessage, error) {
	return EncodeJobPayload(PortDeletePayloadVersion, portDeleteJobPayload{PortID: portID, Impact: impact})
}

// NewPortDeleteJobHandler deletes the port (its owned rows cascade, references
// detach), clears its VictoriaMetrics series and records a destruction
// receipt. Every step is idempotent so a retry or takeover converges.
func NewPortDeleteJobHandler(network NetworkRepository, cleaner SeriesCleaner, receipts DestructionReceiptRecorder) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		var payload portDeleteJobPayload
		if err := DecodeJobPayload(job.CheckpointJSON, PortDeletePayloadVersion, &payload); err != nil {
			return "", err
		}
		if payload.PortID == "" {
			return "", TerminalJobError(fmt.Errorf("port delete job payload has no port_id"))
		}
		if err := network.DeletePort(ctx, job.TenantID, payload.PortID); err != nil {
			return "", err
		}
		seriesMatch := `{port_id="` + string(payload.PortID) + `"}`
		if cleaner != nil {
			if err := cleaner.DeleteSeries(ctx, []string{seriesMatch}); err != nil {
				return "", err
			}
		}
		if receipts != nil {
			if err := receipts.RecordDestructionReceipt(ctx, DestructionReceipt{
				TenantID: job.TenantID, JobID: job.ID, ResourceType: "network_port",
				ResourceID: payload.PortID, ActorID: job.CreatedBy,
				Impact: payload.Impact, SeriesMatch: seriesMatch, DestroyedAt: time.Now().UTC(),
			}); err != nil {
				return "", err
			}
		}
		return "deleted:" + string(payload.PortID), nil
	}
}

// CollectorDeleteJobType names the async collector deletion job (PLAT-04).
const CollectorDeleteJobType = "collector_delete"

// CollectorDeletePayloadVersion is the payload schema version for
// collector_delete.
const CollectorDeletePayloadVersion = 1

type collectorDeleteJobPayload struct {
	CollectorID ID             `json:"collector_id"`
	Impact      map[string]int `json:"impact,omitempty"`
}

// EncodeCollectorDeletePayload builds the versioned envelope for a collector
// delete.
func EncodeCollectorDeletePayload(collectorID ID, impact map[string]int) (json.RawMessage, error) {
	return EncodeJobPayload(CollectorDeletePayloadVersion, collectorDeleteJobPayload{CollectorID: collectorID, Impact: impact})
}

// CollectorDeleter deletes a registry collector, refusing when RESTRICT
// ownership evidence remains.
type CollectorDeleter interface {
	DeleteCollector(ctx context.Context, tenantID, collectorID ID) error
}

// NewCollectorDeleteJobHandler deletes the collector (bindings and plan
// revisions cascade) and records a destruction receipt. Blocked deletes
// (ownership evidence still present) fail terminally — they will not clear on
// retry.
func NewCollectorDeleteJobHandler(collectors CollectorDeleter, receipts DestructionReceiptRecorder) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		var payload collectorDeleteJobPayload
		if err := DecodeJobPayload(job.CheckpointJSON, CollectorDeletePayloadVersion, &payload); err != nil {
			return "", err
		}
		if payload.CollectorID == "" {
			return "", TerminalJobError(fmt.Errorf("collector delete job payload has no collector_id"))
		}
		if err := collectors.DeleteCollector(ctx, job.TenantID, payload.CollectorID); err != nil {
			if errors.Is(err, ErrCollectorDeleteBlocked) {
				return "", TerminalJobError(err)
			}
			return "", err
		}
		if receipts != nil {
			if err := receipts.RecordDestructionReceipt(ctx, DestructionReceipt{
				TenantID: job.TenantID, JobID: job.ID, ResourceType: "collector",
				ResourceID: payload.CollectorID, ActorID: job.CreatedBy,
				Impact: payload.Impact, DestroyedAt: time.Now().UTC(),
			}); err != nil {
				return "", err
			}
		}
		return "deleted:" + string(payload.CollectorID), nil
	}
}
