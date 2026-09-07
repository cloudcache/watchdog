package watchdog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

const (
	AddressDimensionObjectGCJob     = "address_dimension_object_gc"
	AddressDimensionObjectGCPayload = 1
	// Transient object-store/DB failures must not strand a retired object
	// behind a terminal job. Permanent payload and eligibility failures are
	// wrapped as TerminalJobError and still stop immediately.
	AddressDimensionObjectGCMaxAttempts uint32 = ^uint32(0)
)

var ErrAddressDimensionGCNotEligible = errors.New("address dimension object is not eligible for deletion")

type DimensionPublicationGCCandidate struct {
	TenantID       ID        `json:"tenant_id"`
	SnapshotID     ID        `json:"snapshot_id"`
	Version        uint64    `json:"version"`
	ObjectRef      string    `json:"object_ref"`
	Checksum       string    `json:"checksum"`
	RetentionUntil time.Time `json:"retention_until"`
	RetiredAt      time.Time `json:"retired_at"`
}

type AddressDimensionGCCandidate = DimensionPublicationGCCandidate

type DimensionPublicationGCFilter struct {
	Limit  int
	Cursor string
}

type AddressDimensionGCFilter = DimensionPublicationGCFilter

type DimensionPublicationObjectDeletion struct {
	SnapshotID ID
	ObjectRef  string
	DeletedAt  time.Time
}

type AddressDimensionObjectDeletion = DimensionPublicationObjectDeletion

// AddressDimensionGCRepository owns the management-plane eligibility rules.
// The operation-job handler never infers safety from its payload alone.
type AddressDimensionGCRepository interface {
	ScheduleAddressDimensionObjectGC(context.Context, ID, ID, ID, uint64, time.Time) (AddressDimensionSnapshot, error)
	ListAddressDimensionGCCandidates(context.Context, ID, time.Time, AddressDimensionGCFilter) ([]AddressDimensionGCCandidate, string, error)
	ListAllAddressDimensionGCCandidates(context.Context, time.Time, int) ([]AddressDimensionGCCandidate, error)
	DeleteAddressDimensionObject(context.Context, ID, ID, ID, ID, string, string, time.Time, time.Time) (AddressDimensionObjectDeletion, error)
}

type addressDimensionObjectGCJobPayload struct {
	SnapshotID     ID     `json:"snapshot_id"`
	ObjectRef      string `json:"object_ref"`
	Checksum       string `json:"checksum"`
	RetentionUntil string `json:"retention_until"`
}

func EncodeAddressDimensionObjectGCJobPayload(candidate AddressDimensionGCCandidate) ([]byte, error) {
	if candidate.TenantID == "" || candidate.SnapshotID == "" || candidate.ObjectRef == "" || !validSHA256Digest(candidate.Checksum) ||
		candidate.RetentionUntil.IsZero() || candidate.RetentionUntil.Nanosecond()%int(time.Millisecond) != 0 {
		return nil, ErrAddressDimensionInvalid
	}
	return EncodeJobPayload(AddressDimensionObjectGCPayload, addressDimensionObjectGCJobPayload{
		SnapshotID: candidate.SnapshotID, ObjectRef: candidate.ObjectRef, Checksum: candidate.Checksum,
		RetentionUntil: candidate.RetentionUntil.UTC().Format(time.RFC3339Nano),
	})
}

func EnqueueAddressDimensionObjectGC(ctx context.Context, jobs OperationJobRepository, candidate AddressDimensionGCCandidate) (OperationJob, error) {
	if jobs == nil {
		return OperationJob{}, errors.New("operation job repository is required")
	}
	payload, err := EncodeAddressDimensionObjectGCJobPayload(candidate)
	if err != nil {
		return OperationJob{}, err
	}
	hash := sha256.Sum256(payload)
	return jobs.EnqueueOperationJob(ctx, OperationJob{
		TenantID: candidate.TenantID, JobType: AddressDimensionObjectGCJob,
		IdempotencyKey: "address-dimension-object:" + string(candidate.SnapshotID) + ":" + candidate.RetentionUntil.UTC().Format("20060102T150405.000Z"),
		RequestHash:    hex.EncodeToString(hash[:]), CheckpointJSON: payload,
	})
}

func NewAddressDimensionObjectGCJobHandler(repository AddressDimensionGCRepository, now func() time.Time) OperationJobHandler {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return func(ctx context.Context, job OperationJob) (string, error) {
		if repository == nil {
			return "", TerminalJobError(errors.New("address dimension GC dependencies are not configured"))
		}
		var payload addressDimensionObjectGCJobPayload
		if err := DecodeJobPayload(job.CheckpointJSON, AddressDimensionObjectGCPayload, &payload); err != nil {
			return "", err
		}
		retentionUntil, err := time.Parse(time.RFC3339Nano, payload.RetentionUntil)
		if err != nil || job.TenantID == "" || payload.SnapshotID == "" || payload.ObjectRef == "" || !validSHA256Digest(payload.Checksum) ||
			retentionUntil.Nanosecond()%int(time.Millisecond) != 0 {
			return "", TerminalJobError(ErrAddressDimensionInvalid)
		}
		// Once deletion starts it is a short, idempotent commit sequence. Let a
		// canceled or superseded attempt finish the DB marker; a takeover then
		// observes the marker and only repairs the idempotent receipt.
		durableContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		deleted, err := repository.DeleteAddressDimensionObject(
			durableContext, job.TenantID, payload.SnapshotID, job.ID, job.CreatedBy,
			payload.ObjectRef, payload.Checksum, retentionUntil.UTC(), now().UTC(),
		)
		if errors.Is(err, ErrAddressDimensionGCNotEligible) || errors.Is(err, ErrAddressDimensionInvalid) || errors.Is(err, ErrAddressDimensionConflict) {
			return "", TerminalJobError(err)
		}
		if err != nil {
			return "", err
		}
		return "dimension-object-deleted:" + string(deleted.SnapshotID), nil
	}
}

// AddressDimensionGCProducer is only a durable-job producer. Deletion,
// retries, leases, cancellation and takeover remain exclusively in
// operation_jobs.
type AddressDimensionGCProducer struct {
	Repository AddressDimensionGCRepository
	Jobs       OperationJobRepository
	Interval   time.Duration
	Batch      int
	Now        func() time.Time
	Logf       func(string, ...any)
}

func (producer AddressDimensionGCProducer) RunOnce(ctx context.Context) (int, error) {
	if producer.Repository == nil || producer.Jobs == nil {
		return 0, errors.New("address dimension GC producer dependencies are required")
	}
	batch := producer.Batch
	if batch <= 0 {
		batch = defaultAddressObjectGCBatch
	}
	if batch > 1_000 {
		return 0, errors.New("address dimension GC batch exceeds 1000")
	}
	now := time.Now().UTC()
	if producer.Now != nil {
		now = producer.Now().UTC()
	}
	candidates, err := producer.Repository.ListAllAddressDimensionGCCandidates(ctx, now, batch)
	if err != nil {
		return 0, err
	}
	enqueued := 0
	for _, candidate := range candidates {
		if _, err := EnqueueAddressDimensionObjectGC(ctx, producer.Jobs, candidate); err != nil {
			return enqueued, fmt.Errorf("enqueue address dimension object GC for %s: %w", candidate.SnapshotID, err)
		}
		enqueued++
	}
	return enqueued, nil
}

func (producer AddressDimensionGCProducer) Run(ctx context.Context) {
	interval := producer.Interval
	if interval <= 0 {
		interval = defaultAddressObjectGCInterval
	}
	run := func() {
		count, err := producer.RunOnce(ctx)
		if err != nil && producer.Logf != nil {
			producer.Logf("address dimension object GC scan failed: %v", err)
		} else if count > 0 && producer.Logf != nil {
			producer.Logf("address dimension object GC enqueued=%d", count)
		}
	}
	run()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}
