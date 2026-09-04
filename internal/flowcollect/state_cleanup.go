package flowcollect

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

const stateCleanupSchemaVersion = 1

var ErrStateCleanupFenceNotMature = errors.New("state cleanup ownership fence is not mature")

type StateCheckpointKind string

const (
	StateCheckpointDecoder StateCheckpointKind = "decoder"
	StateCheckpointQuality StateCheckpointKind = "quality"
)

type StateCleanupPhase string

const (
	StateCleanupAwaitingFence        StateCleanupPhase = "awaiting_fence"
	StateCleanupAwaitingReplacement  StateCleanupPhase = "awaiting_replacement"
	StateCleanupReadyToTombstone     StateCleanupPhase = "ready_to_tombstone"
	StateCleanupAwaitingVerification StateCleanupPhase = "awaiting_verification"
	StateCleanupComplete             StateCleanupPhase = "complete"
)

type KafkaRecordPosition struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

type StateCleanupCheckpoint struct {
	Kind            StateCheckpointKind `json:"kind"`
	KafkaKey        []byte              `json:"kafka_key"`
	IdentityKey     []byte              `json:"identity_key"`
	PayloadSHA256   []byte              `json:"payload_sha256"`
	TenantID        string              `json:"tenant_id"`
	ExporterID      string              `json:"exporter_id"`
	CollectorID     string              `json:"collector_id"`
	RegistryVersion uint64              `json:"registry_version"`
	OwnershipEpoch  uint64              `json:"ownership_epoch"`
	StateGeneration uint64              `json:"state_generation"`
	CheckpointAt    time.Time           `json:"checkpoint_at"`
}

type OwnershipFenceEvidence struct {
	OldCollectorID             string        `json:"old_collector_id"`
	NewCollectorID             string        `json:"new_collector_id"`
	OldPlanRevision            uint64        `json:"old_plan_revision"`
	NewPlanRevision            uint64        `json:"new_plan_revision"`
	OldOwnershipEpoch          uint64        `json:"old_ownership_epoch"`
	NewOwnershipEpoch          uint64        `json:"new_ownership_epoch"`
	OldPlanRevokedAt           time.Time     `json:"old_plan_revoked_at"`
	OldPlanExpiresAt           time.Time     `json:"old_plan_expires_at"`
	OldOwnerDrainedAt          time.Time     `json:"old_owner_drained_at"`
	OldPrincipalWriteRevokedAt time.Time     `json:"old_principal_write_revoked_at"`
	NewPlanActivatedAt         time.Time     `json:"new_plan_activated_at"`
	MaxClockSkew               time.Duration `json:"max_clock_skew"`
	ACLPropagationDelay        time.Duration `json:"acl_propagation_delay"`
	UniqueOldPrincipal         bool          `json:"unique_old_principal"`
}

type FrozenReplacementObservation struct {
	CapturedAt                 time.Time           `json:"captured_at"`
	Position                   KafkaRecordPosition `json:"position"`
	HighWatermark              int64               `json:"high_watermark"`
	RestoredOldOwnershipEpoch  uint64              `json:"restored_old_ownership_epoch"`
	RestoredOldGeneration      uint64              `json:"restored_old_generation"`
	NewEpochBaselineGeneration uint64              `json:"new_epoch_baseline_generation"`
}

type StateTombstoneReceipt struct {
	Position       KafkaRecordPosition `json:"position"`
	AcknowledgedAt time.Time           `json:"acknowledged_at"`
}

type FrozenTombstoneVerification struct {
	CapturedAt    time.Time `json:"captured_at"`
	Partition     int32     `json:"partition"`
	HighWatermark int64     `json:"high_watermark"`
	KeyAbsent     bool      `json:"key_absent"`
}

type StateCleanupSnapshot struct {
	SchemaVersion          uint32                        `json:"schema_version"`
	JobID                  string                        `json:"job_id"`
	ApprovalID             string                        `json:"approval_id"`
	RequestedBy            string                        `json:"requested_by"`
	Phase                  StateCleanupPhase             `json:"phase"`
	Old                    StateCleanupCheckpoint        `json:"old"`
	Fence                  *OwnershipFenceEvidence       `json:"fence,omitempty"`
	FenceConfirmedAt       time.Time                     `json:"fence_confirmed_at,omitempty"`
	Replacement            *StateCleanupCheckpoint       `json:"replacement,omitempty"`
	ReplacementObservation *FrozenReplacementObservation `json:"replacement_observation,omitempty"`
	TombstoneReceipt       *StateTombstoneReceipt        `json:"tombstone_receipt,omitempty"`
	Verification           *FrozenTombstoneVerification  `json:"verification,omitempty"`
	CreatedAt              time.Time                     `json:"created_at"`
	UpdatedAt              time.Time                     `json:"updated_at"`
}

// StateTombstone can only be obtained from a cleanup job which passed the
// ownership fence and replacement checks. Its key is kept private so callers
// cannot use the reconciler writer as an arbitrary compacted-topic delete API.
type StateTombstone struct {
	kind StateCheckpointKind
	key  []byte
}

func (t StateTombstone) Kind() StateCheckpointKind { return t.kind }
func (t StateTombstone) Key() []byte               { return bytes.Clone(t.key) }

type StateCleanupJob struct {
	mu       sync.Mutex
	snapshot StateCleanupSnapshot
}

func NewCollectStateCleanupJob(jobID, approvalID, requestedBy string, old *flowpb.CollectState, createdAt time.Time) (*StateCleanupJob, error) {
	checkpoint, err := cleanupCheckpointFromCollectState(old)
	if err != nil {
		return nil, err
	}
	return newStateCleanupJob(jobID, approvalID, requestedBy, checkpoint, createdAt)
}

func NewQualityStateCleanupJob(jobID, approvalID, requestedBy string, old *flowpb.QualityCheckpoint, createdAt time.Time) (*StateCleanupJob, error) {
	checkpoint, err := cleanupCheckpointFromQualityState(old)
	if err != nil {
		return nil, err
	}
	return newStateCleanupJob(jobID, approvalID, requestedBy, checkpoint, createdAt)
}

func newStateCleanupJob(jobID, approvalID, requestedBy string, old StateCleanupCheckpoint, createdAt time.Time) (*StateCleanupJob, error) {
	if jobID == "" || approvalID == "" || requestedBy == "" || createdAt.IsZero() || createdAt.Before(old.CheckpointAt) {
		return nil, errors.New("state cleanup job, approval, actor, and creation time are required")
	}
	if err := validateStateCleanupCheckpoint(old); err != nil {
		return nil, err
	}
	return &StateCleanupJob{snapshot: StateCleanupSnapshot{
		SchemaVersion: stateCleanupSchemaVersion,
		JobID:         jobID, ApprovalID: approvalID, RequestedBy: requestedBy,
		Phase: StateCleanupAwaitingFence, Old: cloneStateCleanupCheckpoint(old),
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}}, nil
}

func RestoreStateCleanupJob(snapshot StateCleanupSnapshot) (*StateCleanupJob, error) {
	cloned := cloneStateCleanupSnapshot(snapshot)
	if err := validateStateCleanupSnapshot(cloned); err != nil {
		return nil, err
	}
	return &StateCleanupJob{snapshot: cloned}, nil
}

func (j *StateCleanupJob) Snapshot() StateCleanupSnapshot {
	j.mu.Lock()
	defer j.mu.Unlock()
	return cloneStateCleanupSnapshot(j.snapshot)
}

// ReplacementKafkaKey derives the only key that may satisfy the ownership
// transfer recorded by this job. Keeping the derivation in the state machine
// prevents the management plane from guessing typed compacted-topic keys.
func (j *StateCleanupJob) ReplacementKafkaKey() ([]byte, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.snapshot.Phase != StateCleanupAwaitingReplacement || j.snapshot.Fence == nil {
		return nil, fmt.Errorf("state cleanup replacement key is unavailable in phase %q", j.snapshot.Phase)
	}
	return stateCleanupKafkaKey(j.snapshot.Old.Kind, j.snapshot.Old.IdentityKey, j.snapshot.Fence.NewOwnershipEpoch)
}

func (j *StateCleanupJob) ConfirmFence(evidence OwnershipFenceEvidence, now time.Time) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.snapshot.Fence != nil {
		if equalOwnershipFenceEvidence(*j.snapshot.Fence, evidence) {
			return nil
		}
		return errors.New("state cleanup ownership fence evidence changed")
	}
	if j.snapshot.Phase != StateCleanupAwaitingFence {
		return fmt.Errorf("state cleanup cannot confirm fence in phase %q", j.snapshot.Phase)
	}
	if now.Before(j.snapshot.CreatedAt) {
		return errors.New("state cleanup fence confirmation predates the job")
	}
	if err := validateOwnershipFenceEvidence(evidence, j.snapshot.Old, now); err != nil {
		return err
	}
	j.snapshot.Fence = cloneOwnershipFenceEvidence(&evidence)
	j.snapshot.FenceConfirmedAt = now
	j.snapshot.Phase = StateCleanupAwaitingReplacement
	j.snapshot.UpdatedAt = now
	return nil
}

func (j *StateCleanupJob) ObserveCollectStateReplacement(replacement *flowpb.CollectState, observation FrozenReplacementObservation) error {
	checkpoint, err := cleanupCheckpointFromCollectState(replacement)
	if err != nil {
		return err
	}
	return j.observeReplacement(checkpoint, observation)
}

func (j *StateCleanupJob) ObserveQualityStateReplacement(replacement *flowpb.QualityCheckpoint, observation FrozenReplacementObservation) error {
	checkpoint, err := cleanupCheckpointFromQualityState(replacement)
	if err != nil {
		return err
	}
	return j.observeReplacement(checkpoint, observation)
}

// ObserveReplacementCheckpoint applies an already decoded and validated
// checkpoint representation. Kafka-backed callers should obtain it through
// DecodeStateCleanupCheckpoint rather than constructing fields from metadata.
func (j *StateCleanupJob) ObserveReplacementCheckpoint(replacement StateCleanupCheckpoint, observation FrozenReplacementObservation) error {
	return j.observeReplacement(replacement, observation)
}

// ObserveReplacementPayload decodes according to the checkpoint kind already
// frozen in the job, then reuses the complete replacement validation path.
func (j *StateCleanupJob) ObserveReplacementPayload(payload []byte, observation FrozenReplacementObservation) error {
	j.mu.Lock()
	kind := j.snapshot.Old.Kind
	j.mu.Unlock()
	checkpoint, err := DecodeStateCleanupCheckpoint(kind, payload)
	if err != nil {
		return err
	}
	return j.ObserveReplacementCheckpoint(checkpoint, observation)
}

// DecodeStateCleanupCheckpoint validates the protobuf and converts it into the
// immutable representation used by the cleanup state machine.
func DecodeStateCleanupCheckpoint(kind StateCheckpointKind, payload []byte) (StateCleanupCheckpoint, error) {
	if len(payload) == 0 || len(payload) > collectStateMaxBytes-collectStateHeaderSize {
		return StateCleanupCheckpoint{}, errors.New("state cleanup replacement payload size is invalid")
	}
	switch kind {
	case StateCheckpointDecoder:
		state := &flowpb.CollectState{}
		if err := proto.Unmarshal(payload, state); err != nil {
			return StateCleanupCheckpoint{}, fmt.Errorf("decode state cleanup replacement: %w", err)
		}
		return cleanupCheckpointFromCollectState(state)
	case StateCheckpointQuality:
		checkpoint := &flowpb.QualityCheckpoint{}
		if err := proto.Unmarshal(payload, checkpoint); err != nil {
			return StateCleanupCheckpoint{}, fmt.Errorf("decode quality state cleanup replacement: %w", err)
		}
		return cleanupCheckpointFromQualityState(checkpoint)
	default:
		return StateCleanupCheckpoint{}, errors.New("state cleanup replacement kind is invalid")
	}
}

func (j *StateCleanupJob) observeReplacement(replacement StateCleanupCheckpoint, observation FrozenReplacementObservation) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.snapshot.Replacement != nil {
		if equalStateCleanupCheckpoint(*j.snapshot.Replacement, replacement) && equalFrozenReplacementObservation(*j.snapshot.ReplacementObservation, observation) {
			return nil
		}
		return errors.New("state cleanup replacement evidence changed")
	}
	if j.snapshot.Phase != StateCleanupAwaitingReplacement || j.snapshot.Fence == nil {
		return fmt.Errorf("state cleanup cannot observe replacement in phase %q", j.snapshot.Phase)
	}
	if observation.CapturedAt.Before(j.snapshot.FenceConfirmedAt) {
		return errors.New("state cleanup replacement observation predates fence confirmation")
	}
	if err := validateStateCleanupReplacement(j.snapshot.Old, replacement, *j.snapshot.Fence, observation); err != nil {
		return err
	}
	j.snapshot.Replacement = stateCleanupCheckpointPointer(replacement)
	j.snapshot.ReplacementObservation = frozenReplacementObservationPointer(observation)
	j.snapshot.Phase = StateCleanupReadyToTombstone
	j.snapshot.UpdatedAt = observation.CapturedAt
	return nil
}

func (j *StateCleanupJob) Tombstone() (StateTombstone, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.snapshot.Phase != StateCleanupReadyToTombstone {
		return StateTombstone{}, fmt.Errorf("state cleanup tombstone is unavailable in phase %q", j.snapshot.Phase)
	}
	return StateTombstone{kind: j.snapshot.Old.Kind, key: bytes.Clone(j.snapshot.Old.KafkaKey)}, nil
}

func (j *StateCleanupJob) MarkTombstonePublished(receipt StateTombstoneReceipt) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.snapshot.TombstoneReceipt != nil {
		if equalStateTombstoneReceipt(*j.snapshot.TombstoneReceipt, receipt) {
			return nil
		}
		return errors.New("state cleanup tombstone receipt changed")
	}
	if j.snapshot.Phase != StateCleanupReadyToTombstone || j.snapshot.ReplacementObservation == nil {
		return fmt.Errorf("state cleanup cannot record tombstone in phase %q", j.snapshot.Phase)
	}
	if receipt.Position.Partition < 0 || receipt.Position.Offset < 0 || receipt.AcknowledgedAt.Before(j.snapshot.ReplacementObservation.CapturedAt) {
		return errors.New("state cleanup tombstone receipt is invalid")
	}
	j.snapshot.TombstoneReceipt = stateTombstoneReceiptPointer(receipt)
	j.snapshot.Phase = StateCleanupAwaitingVerification
	j.snapshot.UpdatedAt = receipt.AcknowledgedAt
	return nil
}

func (j *StateCleanupJob) VerifyTombstone(verification FrozenTombstoneVerification) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.snapshot.Verification != nil {
		if equalFrozenTombstoneVerification(*j.snapshot.Verification, verification) {
			return nil
		}
		return errors.New("state cleanup tombstone verification changed")
	}
	if j.snapshot.Phase != StateCleanupAwaitingVerification || j.snapshot.TombstoneReceipt == nil {
		return fmt.Errorf("state cleanup cannot verify tombstone in phase %q", j.snapshot.Phase)
	}
	receipt := j.snapshot.TombstoneReceipt
	if verification.CapturedAt.Before(receipt.AcknowledgedAt) || verification.Partition != receipt.Position.Partition || verification.HighWatermark <= receipt.Position.Offset || !verification.KeyAbsent {
		return errors.New("state cleanup tombstone is not absent at a later frozen boundary")
	}
	j.snapshot.Verification = frozenTombstoneVerificationPointer(verification)
	j.snapshot.Phase = StateCleanupComplete
	j.snapshot.UpdatedAt = verification.CapturedAt
	return nil
}

func cleanupCheckpointFromCollectState(state *flowpb.CollectState) (StateCleanupCheckpoint, error) {
	if err := validateCollectState(state, ""); err != nil {
		return StateCleanupCheckpoint{}, err
	}
	if state.StateSchemaVersion != collectStateSchemaVersion {
		return StateCleanupCheckpoint{}, errors.New("only owner-fenced collect state can be reconciled")
	}
	checkpoint := StateCleanupCheckpoint{
		Kind: StateCheckpointDecoder, KafkaKey: bytes.Clone(state.StateKey), IdentityKey: bytes.Clone(state.StateIdentityKey), PayloadSHA256: bytes.Clone(state.PayloadSha256),
		TenantID: state.TenantId, ExporterID: state.ExporterId, CollectorID: state.CollectorId, RegistryVersion: state.RegistryVersion,
		OwnershipEpoch: state.OwnershipEpoch, StateGeneration: state.StateGeneration, CheckpointAt: time.UnixMilli(state.ReceivedAtUnixMs),
	}
	return checkpoint, validateStateCleanupCheckpoint(checkpoint)
}

func cleanupCheckpointFromQualityState(state *flowpb.QualityCheckpoint) (StateCleanupCheckpoint, error) {
	if err := validateQualityCheckpoint(state, 0); err != nil {
		return StateCleanupCheckpoint{}, err
	}
	key, err := qualityCheckpointKafkaKey(state)
	if err != nil {
		return StateCleanupCheckpoint{}, err
	}
	checkpoint := StateCleanupCheckpoint{
		Kind: StateCheckpointQuality, KafkaKey: key, IdentityKey: bytes.Clone(state.StateIdentityKey), PayloadSHA256: bytes.Clone(state.PayloadSha256),
		TenantID: state.TenantId, ExporterID: state.ExporterId, CollectorID: state.CollectorId, RegistryVersion: state.RegistryVersion,
		OwnershipEpoch: state.OwnershipEpoch, StateGeneration: state.StateGeneration, CheckpointAt: time.UnixMilli(state.CommittedAtUnixMs),
	}
	return checkpoint, validateStateCleanupCheckpoint(checkpoint)
}

func validateStateCleanupCheckpoint(checkpoint StateCleanupCheckpoint) error {
	if len(checkpoint.IdentityKey) != sha256.Size || len(checkpoint.PayloadSHA256) != sha256.Size || checkpoint.TenantID == "" || checkpoint.ExporterID == "" || checkpoint.CollectorID == "" || checkpoint.RegistryVersion == 0 || checkpoint.OwnershipEpoch == 0 || checkpoint.StateGeneration == 0 || checkpoint.CheckpointAt.IsZero() {
		return errors.New("state cleanup checkpoint is incomplete")
	}
	switch checkpoint.Kind {
	case StateCheckpointDecoder:
		var identity [sha256.Size]byte
		copy(identity[:], checkpoint.IdentityKey)
		expected := makeCollectStateKey(identity, checkpoint.OwnershipEpoch)
		if !bytes.Equal(checkpoint.KafkaKey, expected[:]) {
			return errors.New("decoder cleanup key is invalid")
		}
	case StateCheckpointQuality:
		var identity [sha256.Size]byte
		copy(identity[:], checkpoint.IdentityKey)
		expectedStateKey := makeQualityCheckpointKey(identity, checkpoint.OwnershipEpoch)
		expected := append([]byte{qualityCheckpointKeyPrefix}, expectedStateKey[:]...)
		if !bytes.Equal(checkpoint.KafkaKey, expected) {
			return errors.New("quality cleanup key is invalid")
		}
	default:
		return errors.New("state cleanup checkpoint kind is invalid")
	}
	return nil
}

func stateCleanupKafkaKey(kind StateCheckpointKind, identityKey []byte, ownershipEpoch uint64) ([]byte, error) {
	if len(identityKey) != sha256.Size || ownershipEpoch == 0 {
		return nil, errors.New("state cleanup key identity and ownership epoch are required")
	}
	var identity [sha256.Size]byte
	copy(identity[:], identityKey)
	switch kind {
	case StateCheckpointDecoder:
		key := makeCollectStateKey(identity, ownershipEpoch)
		return bytes.Clone(key[:]), nil
	case StateCheckpointQuality:
		stateKey := makeQualityCheckpointKey(identity, ownershipEpoch)
		return append([]byte{qualityCheckpointKeyPrefix}, stateKey[:]...), nil
	default:
		return nil, errors.New("state cleanup checkpoint kind is invalid")
	}
}

func validateOwnershipFenceEvidence(evidence OwnershipFenceEvidence, old StateCleanupCheckpoint, now time.Time) error {
	if now.IsZero() || evidence.OldCollectorID != old.CollectorID || evidence.OldPlanRevision != old.RegistryVersion || evidence.OldOwnershipEpoch != old.OwnershipEpoch || evidence.NewCollectorID == "" || evidence.NewCollectorID == old.CollectorID || evidence.NewPlanRevision == 0 || evidence.NewOwnershipEpoch <= old.OwnershipEpoch || !evidence.UniqueOldPrincipal || evidence.OldPlanRevokedAt.IsZero() || evidence.OldPlanExpiresAt.IsZero() || evidence.OldOwnerDrainedAt.IsZero() || evidence.OldPrincipalWriteRevokedAt.IsZero() || evidence.NewPlanActivatedAt.IsZero() || evidence.MaxClockSkew < 0 || evidence.ACLPropagationDelay < 0 {
		return errors.New("state cleanup ownership fence evidence is invalid")
	}
	if evidence.NewPlanActivatedAt.Before(evidence.OldPlanRevokedAt) || evidence.OldOwnerDrainedAt.After(now) || evidence.NewPlanActivatedAt.After(now) {
		return errors.New("state cleanup ownership fence timestamps are inconsistent")
	}
	if now.Before(evidence.safeAfter()) {
		return ErrStateCleanupFenceNotMature
	}
	return nil
}

func (e OwnershipFenceEvidence) safeAfter() time.Time {
	planSafe := e.OldPlanExpiresAt.Add(e.MaxClockSkew)
	aclSafe := e.OldPrincipalWriteRevokedAt.Add(e.ACLPropagationDelay)
	if aclSafe.After(planSafe) {
		planSafe = aclSafe
	}
	if e.OldOwnerDrainedAt.After(planSafe) {
		planSafe = e.OldOwnerDrainedAt
	}
	return planSafe
}

func validateStateCleanupReplacement(old, replacement StateCleanupCheckpoint, fence OwnershipFenceEvidence, observation FrozenReplacementObservation) error {
	if err := validateStateCleanupCheckpoint(replacement); err != nil {
		return err
	}
	if replacement.Kind != old.Kind || !bytes.Equal(replacement.IdentityKey, old.IdentityKey) || replacement.TenantID != old.TenantID || replacement.ExporterID != old.ExporterID || replacement.CollectorID != fence.NewCollectorID || replacement.RegistryVersion != fence.NewPlanRevision || replacement.OwnershipEpoch != fence.NewOwnershipEpoch || replacement.OwnershipEpoch <= old.OwnershipEpoch || bytes.Equal(replacement.KafkaKey, old.KafkaKey) {
		return errors.New("state cleanup replacement does not match the fenced ownership transfer")
	}
	if observation.CapturedAt.IsZero() || observation.Position.Partition < 0 || observation.Position.Offset < 0 || observation.HighWatermark <= observation.Position.Offset || observation.RestoredOldOwnershipEpoch != old.OwnershipEpoch || observation.RestoredOldGeneration != old.StateGeneration || replacement.StateGeneration <= observation.NewEpochBaselineGeneration {
		return errors.New("state cleanup replacement observation is invalid")
	}
	if observation.CapturedAt.Before(fence.safeAfter()) || observation.CapturedAt.Before(fence.NewPlanActivatedAt) || replacement.CheckpointAt.Before(fence.NewPlanActivatedAt.Add(-fence.MaxClockSkew)) || replacement.CheckpointAt.After(observation.CapturedAt.Add(fence.MaxClockSkew)) {
		return errors.New("state cleanup replacement was not observed after the ownership fence")
	}
	return nil
}

func validateStateCleanupSnapshot(snapshot StateCleanupSnapshot) error {
	if snapshot.SchemaVersion != stateCleanupSchemaVersion || snapshot.JobID == "" || snapshot.ApprovalID == "" || snapshot.RequestedBy == "" || snapshot.CreatedAt.IsZero() || snapshot.UpdatedAt.Before(snapshot.CreatedAt) {
		return errors.New("state cleanup snapshot header is invalid")
	}
	if err := validateStateCleanupCheckpoint(snapshot.Old); err != nil {
		return err
	}
	switch snapshot.Phase {
	case StateCleanupAwaitingFence:
		if snapshot.Fence != nil || !snapshot.FenceConfirmedAt.IsZero() || snapshot.Replacement != nil || snapshot.ReplacementObservation != nil || snapshot.TombstoneReceipt != nil || snapshot.Verification != nil {
			return errors.New("awaiting-fence cleanup snapshot contains later evidence")
		}
		return nil
	case StateCleanupAwaitingReplacement, StateCleanupReadyToTombstone, StateCleanupAwaitingVerification, StateCleanupComplete:
		if snapshot.Fence == nil || snapshot.FenceConfirmedAt.IsZero() || snapshot.FenceConfirmedAt.Before(snapshot.CreatedAt) {
			return errors.New("state cleanup snapshot is missing fence evidence")
		}
		if err := validateOwnershipFenceEvidence(*snapshot.Fence, snapshot.Old, snapshot.FenceConfirmedAt); err != nil {
			return err
		}
	default:
		return errors.New("state cleanup snapshot phase is invalid")
	}
	if snapshot.Phase == StateCleanupAwaitingReplacement {
		if snapshot.Replacement != nil || snapshot.ReplacementObservation != nil || snapshot.TombstoneReceipt != nil || snapshot.Verification != nil {
			return errors.New("awaiting-replacement cleanup snapshot contains later evidence")
		}
		return nil
	}
	if snapshot.Replacement == nil || snapshot.ReplacementObservation == nil {
		return errors.New("state cleanup snapshot is missing replacement evidence")
	}
	if snapshot.ReplacementObservation.CapturedAt.Before(snapshot.FenceConfirmedAt) {
		return errors.New("state cleanup snapshot replacement predates fence confirmation")
	}
	if err := validateStateCleanupReplacement(snapshot.Old, *snapshot.Replacement, *snapshot.Fence, *snapshot.ReplacementObservation); err != nil {
		return err
	}
	if snapshot.Phase == StateCleanupReadyToTombstone {
		if snapshot.TombstoneReceipt != nil || snapshot.Verification != nil {
			return errors.New("ready cleanup snapshot contains publication evidence")
		}
		return nil
	}
	if snapshot.TombstoneReceipt == nil || snapshot.TombstoneReceipt.Position.Partition < 0 || snapshot.TombstoneReceipt.Position.Offset < 0 || snapshot.TombstoneReceipt.AcknowledgedAt.Before(snapshot.ReplacementObservation.CapturedAt) {
		return errors.New("state cleanup snapshot tombstone receipt is invalid")
	}
	if snapshot.Phase == StateCleanupAwaitingVerification {
		if snapshot.Verification != nil {
			return errors.New("awaiting-verification cleanup snapshot contains verification")
		}
		return nil
	}
	if snapshot.Verification == nil || snapshot.Verification.CapturedAt.Before(snapshot.TombstoneReceipt.AcknowledgedAt) || snapshot.Verification.Partition != snapshot.TombstoneReceipt.Position.Partition || snapshot.Verification.HighWatermark <= snapshot.TombstoneReceipt.Position.Offset || !snapshot.Verification.KeyAbsent {
		return errors.New("completed cleanup snapshot verification is invalid")
	}
	return nil
}

func cloneStateCleanupSnapshot(snapshot StateCleanupSnapshot) StateCleanupSnapshot {
	result := snapshot
	result.Old = cloneStateCleanupCheckpoint(snapshot.Old)
	result.Fence = cloneOwnershipFenceEvidence(snapshot.Fence)
	if snapshot.Replacement != nil {
		result.Replacement = stateCleanupCheckpointPointer(*snapshot.Replacement)
	}
	if snapshot.ReplacementObservation != nil {
		result.ReplacementObservation = frozenReplacementObservationPointer(*snapshot.ReplacementObservation)
	}
	if snapshot.TombstoneReceipt != nil {
		result.TombstoneReceipt = stateTombstoneReceiptPointer(*snapshot.TombstoneReceipt)
	}
	if snapshot.Verification != nil {
		result.Verification = frozenTombstoneVerificationPointer(*snapshot.Verification)
	}
	return result
}

func cloneStateCleanupCheckpoint(checkpoint StateCleanupCheckpoint) StateCleanupCheckpoint {
	checkpoint.KafkaKey = bytes.Clone(checkpoint.KafkaKey)
	checkpoint.IdentityKey = bytes.Clone(checkpoint.IdentityKey)
	checkpoint.PayloadSHA256 = bytes.Clone(checkpoint.PayloadSHA256)
	return checkpoint
}

func stateCleanupCheckpointPointer(checkpoint StateCleanupCheckpoint) *StateCleanupCheckpoint {
	clone := cloneStateCleanupCheckpoint(checkpoint)
	return &clone
}

func cloneOwnershipFenceEvidence(evidence *OwnershipFenceEvidence) *OwnershipFenceEvidence {
	if evidence == nil {
		return nil
	}
	clone := *evidence
	return &clone
}

func frozenReplacementObservationPointer(observation FrozenReplacementObservation) *FrozenReplacementObservation {
	clone := observation
	return &clone
}

func stateTombstoneReceiptPointer(receipt StateTombstoneReceipt) *StateTombstoneReceipt {
	clone := receipt
	return &clone
}

func frozenTombstoneVerificationPointer(verification FrozenTombstoneVerification) *FrozenTombstoneVerification {
	clone := verification
	return &clone
}

func equalStateCleanupCheckpoint(left, right StateCleanupCheckpoint) bool {
	return left.Kind == right.Kind && bytes.Equal(left.KafkaKey, right.KafkaKey) && bytes.Equal(left.IdentityKey, right.IdentityKey) && bytes.Equal(left.PayloadSHA256, right.PayloadSHA256) && left.TenantID == right.TenantID && left.ExporterID == right.ExporterID && left.CollectorID == right.CollectorID && left.RegistryVersion == right.RegistryVersion && left.OwnershipEpoch == right.OwnershipEpoch && left.StateGeneration == right.StateGeneration && left.CheckpointAt.Equal(right.CheckpointAt)
}

func equalOwnershipFenceEvidence(left, right OwnershipFenceEvidence) bool {
	return left.OldCollectorID == right.OldCollectorID && left.NewCollectorID == right.NewCollectorID && left.OldPlanRevision == right.OldPlanRevision && left.NewPlanRevision == right.NewPlanRevision && left.OldOwnershipEpoch == right.OldOwnershipEpoch && left.NewOwnershipEpoch == right.NewOwnershipEpoch && left.OldPlanRevokedAt.Equal(right.OldPlanRevokedAt) && left.OldPlanExpiresAt.Equal(right.OldPlanExpiresAt) && left.OldOwnerDrainedAt.Equal(right.OldOwnerDrainedAt) && left.OldPrincipalWriteRevokedAt.Equal(right.OldPrincipalWriteRevokedAt) && left.NewPlanActivatedAt.Equal(right.NewPlanActivatedAt) && left.MaxClockSkew == right.MaxClockSkew && left.ACLPropagationDelay == right.ACLPropagationDelay && left.UniqueOldPrincipal == right.UniqueOldPrincipal
}

func equalFrozenReplacementObservation(left, right FrozenReplacementObservation) bool {
	return left.CapturedAt.Equal(right.CapturedAt) && left.Position == right.Position && left.HighWatermark == right.HighWatermark && left.RestoredOldOwnershipEpoch == right.RestoredOldOwnershipEpoch && left.RestoredOldGeneration == right.RestoredOldGeneration && left.NewEpochBaselineGeneration == right.NewEpochBaselineGeneration
}

func equalStateTombstoneReceipt(left, right StateTombstoneReceipt) bool {
	return left.Position == right.Position && left.AcknowledgedAt.Equal(right.AcknowledgedAt)
}

func equalFrozenTombstoneVerification(left, right FrozenTombstoneVerification) bool {
	return left.CapturedAt.Equal(right.CapturedAt) && left.Partition == right.Partition && left.HighWatermark == right.HighWatermark && left.KeyAbsent == right.KeyAbsent
}
