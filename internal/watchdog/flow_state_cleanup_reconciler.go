package watchdog

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

var (
	ErrFlowStateCleanupReplacementNotObserved = errors.New("flow state-cleanup replacement is not visible")
	ErrFlowStateCleanupInvalidReplacement     = errors.New("flow state-cleanup replacement is invalid")
	ErrFlowStateCleanupOldStatePresent        = errors.New("flow state-cleanup old state is present after tombstone")
	ErrFlowStateCleanupBoundaryNotAdvanced    = errors.New("flow state-cleanup Kafka boundary has not advanced")
)

type FlowStateCleanupRestoreProof struct {
	RestoredOldOwnershipEpoch  uint64
	RestoredOldGeneration      uint64
	NewEpochBaselineGeneration uint64
}

type FlowStateCleanupEvidenceProvider interface {
	OwnershipFence(context.Context, flowcollect.StateCleanupSnapshot) (flowcollect.OwnershipFenceEvidence, error)
	ReplacementRestoreProof(context.Context, flowcollect.StateCleanupSnapshot) (FlowStateCleanupRestoreProof, error)
}

type FlowStateCleanupStateReader interface {
	ObserveReplacement(context.Context, []byte, flowcollect.StateCheckpointKind, FlowStateCleanupRestoreProof) (flowcollect.StateCleanupCheckpoint, flowcollect.FrozenReplacementObservation, error)
	VerifyTombstone(context.Context, []byte, flowcollect.StateTombstoneReceipt) (flowcollect.FrozenTombstoneVerification, error)
}

type FlowStateCleanupTombstoneWriter interface {
	Publish(context.Context, flowcollect.StateTombstone, time.Time) (flowcollect.StateTombstoneReceipt, error)
}

type flowStateCleanupKeyScanner interface {
	Scan(context.Context, []byte) (flowcollect.KafkaStateKeySnapshot, error)
}

// KafkaFlowStateCleanupStateReader is the narrow adapter between the watchdog
// reconciler and the exact-key, frozen-boundary Kafka scanner.
type KafkaFlowStateCleanupStateReader struct {
	scanner flowStateCleanupKeyScanner
}

func NewKafkaFlowStateCleanupStateReader(scanner *flowcollect.KafkaStateKeyScanner) (*KafkaFlowStateCleanupStateReader, error) {
	if scanner == nil {
		return nil, errors.New("Kafka flow state-cleanup scanner is required")
	}
	return &KafkaFlowStateCleanupStateReader{scanner: scanner}, nil
}

func (r *KafkaFlowStateCleanupStateReader) ObserveReplacement(ctx context.Context, key []byte, kind flowcollect.StateCheckpointKind, proof FlowStateCleanupRestoreProof) (flowcollect.StateCleanupCheckpoint, flowcollect.FrozenReplacementObservation, error) {
	if r == nil || r.scanner == nil || ctx == nil || len(key) == 0 || proof.RestoredOldOwnershipEpoch == 0 || proof.RestoredOldGeneration == 0 {
		return flowcollect.StateCleanupCheckpoint{}, flowcollect.FrozenReplacementObservation{}, errors.New("flow state-cleanup replacement scan input is invalid")
	}
	snapshot, err := r.scanner.Scan(ctx, key)
	if err != nil {
		return flowcollect.StateCleanupCheckpoint{}, flowcollect.FrozenReplacementObservation{}, err
	}
	if !snapshot.Present {
		return flowcollect.StateCleanupCheckpoint{}, flowcollect.FrozenReplacementObservation{}, ErrFlowStateCleanupReplacementNotObserved
	}
	checkpoint, err := flowcollect.DecodeStateCleanupCheckpoint(kind, snapshot.Value)
	if err != nil {
		return flowcollect.StateCleanupCheckpoint{}, flowcollect.FrozenReplacementObservation{}, fmt.Errorf("%w: %v", ErrFlowStateCleanupInvalidReplacement, err)
	}
	observation, err := snapshot.ReplacementObservation(proof.RestoredOldOwnershipEpoch, proof.RestoredOldGeneration, proof.NewEpochBaselineGeneration)
	if err != nil {
		return flowcollect.StateCleanupCheckpoint{}, flowcollect.FrozenReplacementObservation{}, fmt.Errorf("%w: %v", ErrFlowStateCleanupInvalidReplacement, err)
	}
	return checkpoint, observation, nil
}

func (r *KafkaFlowStateCleanupStateReader) VerifyTombstone(ctx context.Context, key []byte, receipt flowcollect.StateTombstoneReceipt) (flowcollect.FrozenTombstoneVerification, error) {
	if r == nil || r.scanner == nil || ctx == nil || len(key) == 0 {
		return flowcollect.FrozenTombstoneVerification{}, errors.New("flow state-cleanup tombstone scan input is invalid")
	}
	snapshot, err := r.scanner.Scan(ctx, key)
	if err != nil {
		return flowcollect.FrozenTombstoneVerification{}, err
	}
	if snapshot.Present {
		return flowcollect.FrozenTombstoneVerification{}, ErrFlowStateCleanupOldStatePresent
	}
	verification, err := snapshot.TombstoneVerification(receipt)
	if err != nil {
		return flowcollect.FrozenTombstoneVerification{}, fmt.Errorf("%w: %v", ErrFlowStateCleanupBoundaryNotAdvanced, err)
	}
	return verification, nil
}

type FlowStateCleanupReconcilerConfig struct {
	WorkerID          string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	StepTimeout       time.Duration
	PollInterval      time.Duration
	RetryMin          time.Duration
	RetryMax          time.Duration
	MaxAttempts       uint32
}

func DefaultFlowStateCleanupReconcilerConfig(workerID string) FlowStateCleanupReconcilerConfig {
	return FlowStateCleanupReconcilerConfig{
		WorkerID: workerID, LeaseDuration: 30 * time.Second, HeartbeatInterval: 10 * time.Second,
		StepTimeout: 2 * time.Minute, PollInterval: time.Second, RetryMin: time.Second, RetryMax: time.Minute,
	}
}

type FlowStateCleanupReconciler struct {
	repository FlowStateCleanupRepository
	evidence   FlowStateCleanupEvidenceProvider
	reader     FlowStateCleanupStateReader
	writer     FlowStateCleanupTombstoneWriter
	config     FlowStateCleanupReconcilerConfig
	now        func() time.Time
	newToken   func() (string, error)
}

func NewFlowStateCleanupReconciler(repository FlowStateCleanupRepository, evidence FlowStateCleanupEvidenceProvider, reader FlowStateCleanupStateReader, writer FlowStateCleanupTombstoneWriter, config FlowStateCleanupReconcilerConfig) (*FlowStateCleanupReconciler, error) {
	if repository == nil || evidence == nil || reader == nil || writer == nil {
		return nil, errors.New("flow state-cleanup repository, evidence, reader, and writer are required")
	}
	if strings.TrimSpace(config.WorkerID) == "" || len(config.WorkerID) > 128 || config.LeaseDuration <= 0 || config.LeaseDuration > 24*time.Hour || config.HeartbeatInterval <= 0 || config.HeartbeatInterval >= config.LeaseDuration/2 || config.StepTimeout <= 0 || config.StepTimeout > 24*time.Hour || config.PollInterval <= 0 || config.PollInterval > time.Hour || config.RetryMin <= 0 || config.RetryMax < config.RetryMin || config.RetryMax > 24*time.Hour {
		return nil, errors.New("flow state-cleanup reconciler configuration is invalid")
	}
	return &FlowStateCleanupReconciler{
		repository: repository, evidence: evidence, reader: reader, writer: writer, config: config,
		now: time.Now, newToken: newFlowStateCleanupLeaseToken,
	}, nil
}

// Run processes due jobs serially. Multiple watchdog instances scale through
// database leases; no process-local queue is an ownership authority.
func (r *FlowStateCleanupReconciler) Run(ctx context.Context) error {
	if r == nil || ctx == nil {
		return errors.New("flow state-cleanup reconciler and context are required")
	}
	for {
		worked, err := r.ReconcileOne(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return nil
			}
			return err
		}
		if worked {
			continue
		}
		timer := time.NewTimer(r.config.PollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

// ReconcileOne claims one due job and persists every successful phase before
// executing the next. A dependency wait requeues the job; an unsafe proof
// fails it; a lost lease leaves it running for expiry-based takeover.
func (r *FlowStateCleanupReconciler) ReconcileOne(ctx context.Context) (bool, error) {
	if r == nil || ctx == nil {
		return false, errors.New("flow state-cleanup reconciler and context are required")
	}
	leaseToken, err := r.newToken()
	if err != nil {
		return false, fmt.Errorf("create flow state-cleanup lease token: %w", err)
	}
	job, found, err := r.repository.ClaimFlowStateCleanupJob(ctx, r.config.WorkerID, leaseToken, r.config.LeaseDuration)
	if err != nil || !found {
		return found, err
	}

	leaseCtx, stopLease := context.WithCancel(ctx)
	heartbeatDone := make(chan error, 1)
	jobID := job.ID
	go func() {
		heartbeatDone <- r.maintainLease(leaseCtx, stopLease, jobID, leaseToken)
	}()
	stopHeartbeat := func() error {
		stopLease()
		return <-heartbeatDone
	}

	for {
		machine, err := flowcollect.RestoreStateCleanupJob(job.Snapshot)
		if err != nil {
			_ = stopHeartbeat()
			return true, fmt.Errorf("restore claimed flow state-cleanup job: %w", err)
		}
		stepCtx, cancelStep := context.WithTimeout(leaseCtx, r.config.StepTimeout)
		failure := r.advanceOne(stepCtx, machine)
		cancelStep()

		if leaseCtx.Err() != nil {
			heartbeatErr := stopHeartbeat()
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			if heartbeatErr != nil {
				return true, heartbeatErr
			}
			return true, leaseCtx.Err()
		}
		if failure != nil {
			if err := stopHeartbeat(); err != nil {
				return true, err
			}
			return true, r.persistFailure(ctx, job, failure)
		}

		next := machine.Snapshot()
		terminal := next.Phase == flowcollect.StateCleanupComplete
		if terminal {
			if err := stopHeartbeat(); err != nil {
				return true, err
			}
		}
		updated, err := r.repository.SaveFlowStateCleanupCheckpoint(ctx, job.ID, leaseToken, job.RowVersion, next, r.now().UTC())
		if err != nil {
			if !terminal {
				_ = stopHeartbeat()
			}
			return true, err
		}
		if terminal {
			return true, nil
		}
		job = updated
	}
}

type flowStateCleanupStepFailure struct {
	code      string
	retryable bool
	err       error
}

func (r *FlowStateCleanupReconciler) advanceOne(ctx context.Context, machine *flowcollect.StateCleanupJob) *flowStateCleanupStepFailure {
	snapshot := machine.Snapshot()
	switch snapshot.Phase {
	case flowcollect.StateCleanupAwaitingFence:
		evidence, err := r.evidence.OwnershipFence(ctx, snapshot)
		if err != nil {
			return cleanupStepRetry("FENCE_EVIDENCE_UNAVAILABLE", err)
		}
		if err := machine.ConfirmFence(evidence, r.now().UTC()); err != nil {
			if errors.Is(err, flowcollect.ErrStateCleanupFenceNotMature) {
				return cleanupStepRetry("FENCE_NOT_MATURE", err)
			}
			return cleanupStepTerminal("INVALID_FENCE_EVIDENCE", err)
		}
	case flowcollect.StateCleanupAwaitingReplacement:
		key, err := machine.ReplacementKafkaKey()
		if err != nil {
			return cleanupStepTerminal("INVALID_REPLACEMENT_KEY", err)
		}
		proof, err := r.evidence.ReplacementRestoreProof(ctx, snapshot)
		if err != nil {
			return cleanupStepRetry("RESTORE_PROOF_UNAVAILABLE", err)
		}
		replacement, observation, err := r.reader.ObserveReplacement(ctx, key, snapshot.Old.Kind, proof)
		if err != nil {
			switch {
			case errors.Is(err, ErrFlowStateCleanupReplacementNotObserved):
				return cleanupStepRetry("REPLACEMENT_NOT_OBSERVED", err)
			case errors.Is(err, ErrFlowStateCleanupInvalidReplacement):
				return cleanupStepTerminal("INVALID_REPLACEMENT", err)
			default:
				return cleanupStepRetry("STATE_SCAN_FAILED", err)
			}
		}
		if err := machine.ObserveReplacementCheckpoint(replacement, observation); err != nil {
			return cleanupStepTerminal("INVALID_REPLACEMENT_EVIDENCE", err)
		}
	case flowcollect.StateCleanupReadyToTombstone:
		tombstone, err := machine.Tombstone()
		if err != nil {
			return cleanupStepTerminal("TOMBSTONE_NOT_AUTHORIZED", err)
		}
		receipt, err := r.writer.Publish(ctx, tombstone, r.now().UTC())
		if err != nil {
			return cleanupStepRetry("TOMBSTONE_PUBLISH_FAILED", err)
		}
		if err := machine.MarkTombstonePublished(receipt); err != nil {
			return cleanupStepTerminal("INVALID_TOMBSTONE_RECEIPT", err)
		}
	case flowcollect.StateCleanupAwaitingVerification:
		if snapshot.TombstoneReceipt == nil {
			return cleanupStepTerminal("MISSING_TOMBSTONE_RECEIPT", errors.New("cleanup checkpoint has no tombstone receipt"))
		}
		verification, err := r.reader.VerifyTombstone(ctx, snapshot.Old.KafkaKey, *snapshot.TombstoneReceipt)
		if err != nil {
			switch {
			case errors.Is(err, ErrFlowStateCleanupOldStatePresent):
				return cleanupStepTerminal("OLD_STATE_REAPPEARED", err)
			case errors.Is(err, ErrFlowStateCleanupBoundaryNotAdvanced):
				return cleanupStepRetry("TOMBSTONE_NOT_VISIBLE", err)
			default:
				return cleanupStepRetry("STATE_SCAN_FAILED", err)
			}
		}
		if err := machine.VerifyTombstone(verification); err != nil {
			return cleanupStepTerminal("INVALID_TOMBSTONE_VERIFICATION", err)
		}
	case flowcollect.StateCleanupComplete:
		return cleanupStepTerminal("INVALID_COMPLETED_JOB", errors.New("completed cleanup job remained claimable"))
	default:
		return cleanupStepTerminal("INVALID_CLEANUP_PHASE", fmt.Errorf("unknown cleanup phase %q", snapshot.Phase))
	}
	return nil
}

func cleanupStepRetry(code string, err error) *flowStateCleanupStepFailure {
	return &flowStateCleanupStepFailure{code: code, retryable: true, err: err}
}

func cleanupStepTerminal(code string, err error) *flowStateCleanupStepFailure {
	return &flowStateCleanupStepFailure{code: code, err: err}
}

func (r *FlowStateCleanupReconciler) persistFailure(ctx context.Context, job FlowStateCleanupJob, failure *flowStateCleanupStepFailure) error {
	detail := flowStateCleanupErrorDetail(failure.err)
	if failure.retryable && (r.config.MaxAttempts == 0 || job.AttemptCount < r.config.MaxAttempts) {
		nextAttempt := r.now().UTC().Add(r.retryDelay(job.ID, job.AttemptCount))
		_, err := r.repository.RequeueFlowStateCleanupJob(ctx, job.ID, job.LeaseToken, job.RowVersion, failure.code, detail, nextAttempt)
		return err
	}
	if failure.retryable {
		detail = flowStateCleanupErrorDetail(fmt.Errorf("retry limit reached after %d attempts: %w", job.AttemptCount, failure.err))
	}
	_, err := r.repository.FailFlowStateCleanupJob(ctx, job.ID, job.LeaseToken, job.RowVersion, failure.code, detail)
	return err
}

func (r *FlowStateCleanupReconciler) maintainLease(ctx context.Context, cancel context.CancelFunc, jobID ID, leaseToken string) error {
	ticker := time.NewTicker(r.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.repository.RenewFlowStateCleanupLease(ctx, jobID, leaseToken, r.config.LeaseDuration); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				cancel()
				return fmt.Errorf("renew flow state-cleanup lease: %w", err)
			}
		}
	}
}

func (r *FlowStateCleanupReconciler) retryDelay(jobID ID, attempt uint32) time.Duration {
	shift := attempt
	if shift > 16 {
		shift = 16
	}
	delay := r.config.RetryMin
	for index := uint32(1); index < shift && delay < r.config.RetryMax; index++ {
		if delay > r.config.RetryMax/2 {
			delay = r.config.RetryMax
			break
		}
		delay *= 2
	}
	if delay > r.config.RetryMax {
		delay = r.config.RetryMax
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", jobID, attempt)))
	percent := int64(binary.BigEndian.Uint16(digest[:2])%41) - 20
	jittered := delay + time.Duration(int64(delay)*percent/100)
	if jittered < r.config.RetryMin {
		return r.config.RetryMin
	}
	if jittered > r.config.RetryMax {
		return r.config.RetryMax
	}
	return jittered
}

func newFlowStateCleanupLeaseToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func flowStateCleanupErrorDetail(err error) string {
	if err == nil {
		return "operation failed"
	}
	value := strings.Map(func(value rune) rune {
		if unicode.IsControl(value) {
			return ' '
		}
		return value
	}, err.Error())
	value = strings.Join(strings.Fields(value), " ")
	runes := []rune(value)
	if len(runes) > 1024 {
		value = string(runes[:1024])
	}
	if value == "" {
		return "operation failed"
	}
	return value
}

var _ FlowStateCleanupStateReader = (*KafkaFlowStateCleanupStateReader)(nil)
