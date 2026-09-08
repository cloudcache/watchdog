package address

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/opjob"
)

const (
	AddressDimensionObjectGCJob     = "address_dimension_object_gc"
	AddressDimensionObjectGCPayload = 1

	defaultAddressObjectGCBatch    = 100
	defaultAddressObjectGCInterval = 15 * time.Minute
)

var ErrAddressDimensionGCNotEligible = errors.New("address dimension object is not eligible for deletion")

type DimensionPublicationGCCandidate struct {
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

// addressDimensionGCCandidateSafety keeps only retired snapshots past retention,
// not referenced, not the current/future activation, and superseded on every
// worker that installed them. De-tenanted (module_key/dimension_key kept).
// It binds five as-of timestamps in order.
const addressDimensionGCCandidateSafety = `
	s.status = 'retired'
	AND s.retired_at IS NOT NULL
	AND s.retention_until IS NOT NULL
	AND s.retention_until <= ?
	AND s.object_deleted_at IS NULL
	AND NOT EXISTS (
		SELECT 1 FROM dimension_snapshot_references AS snapshot_reference
		WHERE snapshot_reference.snapshot_id = s.id AND snapshot_reference.retain_until > ?
	)
	AND NOT EXISTS (
		SELECT 1 FROM dimension_snapshot_activations AS future_activation
		WHERE future_activation.module_key = s.module_key AND future_activation.dimension_key = s.dimension_key
		  AND future_activation.snapshot_id = s.id AND future_activation.effective_from > ?
	)
	AND NOT EXISTS (
		SELECT 1 FROM dimension_snapshot_activations AS current_activation
		WHERE current_activation.module_key = s.module_key AND current_activation.dimension_key = s.dimension_key
		  AND current_activation.snapshot_id = s.id AND current_activation.effective_from <= ?
		  AND NOT EXISTS (
			SELECT 1 FROM dimension_snapshot_activations AS later_activation
			WHERE later_activation.module_key = current_activation.module_key
			  AND later_activation.dimension_key = current_activation.dimension_key
			  AND later_activation.effective_from <= ? AND later_activation.effective_from > current_activation.effective_from
		  )
	)
	AND NOT EXISTS (
		SELECT 1 FROM dimension_snapshot_acks AS candidate_ack
		WHERE candidate_ack.snapshot_id = s.id AND candidate_ack.state = 'installed'
		  AND NOT EXISTS (
			SELECT 1 FROM dimension_snapshot_acks AS later_ack
			JOIN dimension_snapshots AS later_snapshot ON later_snapshot.id = later_ack.snapshot_id
			WHERE later_ack.worker_id = candidate_ack.worker_id AND later_ack.state = 'installed'
			  AND later_snapshot.module_key = s.module_key AND later_snapshot.dimension_key = s.dimension_key
			  AND (
				later_ack.installed_at > candidate_ack.installed_at
				OR (later_ack.installed_at = candidate_ack.installed_at AND later_snapshot.version > s.version)
				OR (later_ack.installed_at = candidate_ack.installed_at AND later_snapshot.version = s.version AND later_ack.snapshot_id > candidate_ack.snapshot_id)
			  )
		  )
	)`

func (p *Publisher) ScheduleAddressDimensionObjectGC(ctx context.Context, actorID, snapshotID ID, expectedRowVersion uint64, retentionUntil time.Time) (AddressDimensionSnapshot, error) {
	if p == nil || p.store == nil || actorID == "" || snapshotID == "" || expectedRowVersion == 0 || retentionUntil.IsZero() || retentionUntil.Nanosecond()%int(time.Millisecond) != 0 {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	retentionUntil = retentionUntil.UTC()
	if !retentionUntil.After(p.now().UTC()) {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublication(ctx, tx); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, snapshotID, true)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if snapshot.RowVersion != expectedRowVersion {
		return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusRetired || snapshot.ObjectDeletedAt != nil || (snapshot.RetentionUntil != nil && retentionUntil.Before(*snapshot.RetentionUntil)) {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET retention_until = ?, row_version = row_version + 1
		WHERE module_key = ? AND dimension_key = ? AND id = ? AND row_version = ? AND object_deleted_at IS NULL
	`, retentionUntil, p.scope.ModuleKey, p.scope.DimensionKey, snapshotID, expectedRowVersion)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, actorID, snapshotID, "dimension.snapshot.gc_scheduled", map[string]any{
		"retention_until": retentionUntil, "version": snapshot.Version,
	}); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	return p.GetDimensionPublicationSnapshot(ctx, snapshotID)
}

func (p *Publisher) ListAddressDimensionGCCandidates(ctx context.Context, asOf time.Time, filter AddressDimensionGCFilter) ([]AddressDimensionGCCandidate, string, error) {
	if p == nil || p.store == nil || asOf.IsZero() {
		return nil, "", ErrAddressDimensionInvalid
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	query := `
		SELECT s.id, s.version, s.object_ref, s.checksum, s.retention_until, s.retired_at
		FROM dimension_snapshots AS s
		WHERE s.module_key = ? AND s.dimension_key = ? AND ` + addressDimensionGCCandidateSafety
	args := []any{p.scope.ModuleKey, p.scope.DimensionKey, asOf.UTC(), asOf.UTC(), asOf.UTC(), asOf.UTC(), asOf.UTC()}
	if filter.Cursor != "" {
		query += ` AND s.id > ?`
		args = append(args, filter.Cursor)
	}
	query += ` ORDER BY s.id LIMIT ?`
	args = append(args, filter.Limit+1)
	items, err := p.scanDimensionPublicationGCCandidates(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(items) > filter.Limit {
		items = items[:filter.Limit]
		next = items[len(items)-1].SnapshotID
	}
	return items, next, nil
}

func (p *Publisher) ListAllAddressDimensionGCCandidates(ctx context.Context, asOf time.Time, limit int) ([]AddressDimensionGCCandidate, error) {
	if p == nil || p.store == nil || asOf.IsZero() || limit <= 0 || limit > 1_000 {
		return nil, ErrAddressDimensionInvalid
	}
	query := `
		SELECT s.id, s.version, s.object_ref, s.checksum, s.retention_until, s.retired_at
		FROM dimension_snapshots AS s
		WHERE s.module_key = ? AND s.dimension_key = ? AND ` + addressDimensionGCCandidateSafety + `
		ORDER BY s.id LIMIT ?`
	return p.scanDimensionPublicationGCCandidates(ctx, query,
		p.scope.ModuleKey, p.scope.DimensionKey, asOf.UTC(), asOf.UTC(), asOf.UTC(), asOf.UTC(), asOf.UTC(), limit)
}

func (p *Publisher) scanDimensionPublicationGCCandidates(ctx context.Context, query string, args ...any) ([]AddressDimensionGCCandidate, error) {
	rows, err := p.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]AddressDimensionGCCandidate, 0)
	for rows.Next() {
		var item AddressDimensionGCCandidate
		if err := rows.Scan(&item.SnapshotID, &item.Version, &item.ObjectRef, &item.Checksum, &item.RetentionUntil, &item.RetiredAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (p *Publisher) DeleteAddressDimensionObject(ctx context.Context, snapshotID, jobID, actorID ID, expectedObjectRef, expectedChecksum string, expectedRetention, asOf time.Time) (AddressDimensionObjectDeletion, error) {
	if p == nil || p.store == nil || snapshotID == "" || jobID == "" || expectedObjectRef == "" || !validSHA256Digest(expectedChecksum) || expectedRetention.IsZero() || asOf.IsZero() {
		return AddressDimensionObjectDeletion{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublication(ctx, tx); err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, snapshotID, true)
	if err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	if snapshot.ObjectRef != expectedObjectRef || snapshot.Checksum != expectedChecksum || snapshot.RetentionUntil == nil || !snapshot.RetentionUntil.Equal(expectedRetention.UTC()) {
		return AddressDimensionObjectDeletion{}, ErrAddressDimensionConflict
	}
	if snapshot.ObjectDeletedAt != nil {
		if err := tx.Commit(); err != nil {
			return AddressDimensionObjectDeletion{}, err
		}
		return AddressDimensionObjectDeletion{SnapshotID: snapshot.ID, ObjectRef: snapshot.ObjectRef, DeletedAt: snapshot.ObjectDeletedAt.UTC()}, nil
	}
	eligible, err := dimensionPublicationObjectGCEligibleTx(ctx, tx, snapshot, asOf.UTC())
	if err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	if !eligible {
		return AddressDimensionObjectDeletion{}, ErrAddressDimensionGCNotEligible
	}
	if err := p.objects.RemoveDimensionObject(snapshot.ObjectRef); err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	deletedAt := asOf.UTC().Truncate(time.Millisecond)
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET object_deleted_at = ?, row_version = row_version + 1
		WHERE module_key = ? AND dimension_key = ? AND id = ? AND object_deleted_at IS NULL
	`, deletedAt, p.scope.ModuleKey, p.scope.DimensionKey, snapshotID)
	if err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, actorID, snapshot.ID, "dimension.object.deleted", map[string]any{
		"object_ref": snapshot.ObjectRef, "version": snapshot.Version, "job_id": jobID, "deleted_at": deletedAt,
	}); err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	return AddressDimensionObjectDeletion{SnapshotID: snapshot.ID, ObjectRef: snapshot.ObjectRef, DeletedAt: deletedAt}, nil
}

func dimensionPublicationObjectGCEligibleTx(ctx context.Context, tx *sql.Tx, snapshot DimensionPublicationSnapshot, asOf time.Time) (bool, error) {
	if snapshot.Status != AddressDimensionStatusRetired || snapshot.RetiredAt == nil || snapshot.RetentionUntil == nil || asOf.Before(*snapshot.RetentionUntil) {
		return false, nil
	}
	var blockers uint64
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM dimension_snapshot_references WHERE snapshot_id = ? AND retain_until > ?
	`, snapshot.ID, asOf).Scan(&blockers); err != nil || blockers > 0 {
		return false, err
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM dimension_snapshot_activations AS activation
		WHERE activation.module_key = ? AND activation.dimension_key = ? AND activation.snapshot_id = ?
		  AND (
			activation.effective_from > ?
			OR activation.effective_from = (
				SELECT MAX(current_activation.effective_from)
				FROM dimension_snapshot_activations AS current_activation
				WHERE current_activation.module_key = activation.module_key
				  AND current_activation.dimension_key = activation.dimension_key
				  AND current_activation.effective_from <= ?
			)
		  )
	`, snapshot.ModuleKey, snapshot.DimensionKey, snapshot.ID, asOf, asOf).Scan(&blockers); err != nil || blockers > 0 {
		return false, err
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM dimension_snapshot_acks AS candidate_ack
		WHERE candidate_ack.snapshot_id = ? AND candidate_ack.state = 'installed'
		  AND NOT EXISTS (
			SELECT 1
			FROM dimension_snapshot_acks AS later_ack
			JOIN dimension_snapshots AS later_snapshot ON later_snapshot.id = later_ack.snapshot_id
			WHERE later_ack.worker_id = candidate_ack.worker_id AND later_ack.state = 'installed'
			  AND later_snapshot.module_key = ? AND later_snapshot.dimension_key = ?
			  AND (
				later_ack.installed_at > candidate_ack.installed_at
				OR (later_ack.installed_at = candidate_ack.installed_at AND later_snapshot.version > ?)
				OR (later_ack.installed_at = candidate_ack.installed_at AND later_snapshot.version = ? AND later_ack.snapshot_id > candidate_ack.snapshot_id)
			  )
		  )
	`, snapshot.ID, snapshot.ModuleKey, snapshot.DimensionKey, snapshot.Version, snapshot.Version).Scan(&blockers); err != nil || blockers > 0 {
		return false, err
	}
	return true, nil
}

// EncodeAddressDimensionObjectGCJobPayload / EnqueueAddressDimensionObjectGC drive
// the object reclamation through the opjob engine; eligibility is always
// re-checked at delete time, never inferred from the payload.
func EncodeAddressDimensionObjectGCJobPayload(candidate AddressDimensionGCCandidate) ([]byte, error) {
	if candidate.SnapshotID == "" || candidate.ObjectRef == "" || !validSHA256Digest(candidate.Checksum) ||
		candidate.RetentionUntil.IsZero() || candidate.RetentionUntil.Nanosecond()%int(time.Millisecond) != 0 {
		return nil, ErrAddressDimensionInvalid
	}
	return opjob.EncodePayload(AddressDimensionObjectGCPayload, addressDimensionObjectGCJobPayload{
		SnapshotID: candidate.SnapshotID, ObjectRef: candidate.ObjectRef, Checksum: candidate.Checksum,
		RetentionUntil: candidate.RetentionUntil.UTC().Format(time.RFC3339Nano),
	})
}

type addressDimensionObjectGCJobPayload struct {
	SnapshotID     ID     `json:"snapshot_id"`
	ObjectRef      string `json:"object_ref"`
	Checksum       string `json:"checksum"`
	RetentionUntil string `json:"retention_until"`
}

func EnqueueAddressDimensionObjectGC(ctx context.Context, jobs opjob.Repository, candidate AddressDimensionGCCandidate) (opjob.Job, error) {
	if jobs == nil {
		return opjob.Job{}, errors.New("operation job repository is required")
	}
	payload, err := EncodeAddressDimensionObjectGCJobPayload(candidate)
	if err != nil {
		return opjob.Job{}, err
	}
	return jobs.Enqueue(ctx, opjob.Job{
		JobType:        AddressDimensionObjectGCJob,
		IdempotencyKey: "address-dimension-object:" + string(candidate.SnapshotID) + ":" + candidate.RetentionUntil.UTC().Format("20060102T150405.000Z"),
		RequestHash:    sha256Checksum(payload)[7:], CheckpointJSON: payload,
	})
}

func NewAddressDimensionObjectGCJobHandler(publisher *Publisher, now func() time.Time) opjob.Handler {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return func(ctx context.Context, job opjob.Job) (string, error) {
		if publisher == nil {
			return "", opjob.TerminalError(errors.New("address dimension GC dependencies are not configured"))
		}
		var payload addressDimensionObjectGCJobPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, AddressDimensionObjectGCPayload, &payload); err != nil {
			return "", err
		}
		retentionUntil, err := time.Parse(time.RFC3339Nano, payload.RetentionUntil)
		if err != nil || payload.SnapshotID == "" || payload.ObjectRef == "" || !validSHA256Digest(payload.Checksum) || retentionUntil.Nanosecond()%int(time.Millisecond) != 0 {
			return "", opjob.TerminalError(ErrAddressDimensionInvalid)
		}
		durableContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		deleted, err := publisher.DeleteAddressDimensionObject(durableContext, payload.SnapshotID, job.ID, job.CreatedBy,
			payload.ObjectRef, payload.Checksum, retentionUntil.UTC(), now().UTC())
		if errors.Is(err, ErrAddressDimensionGCNotEligible) || errors.Is(err, ErrAddressDimensionInvalid) || errors.Is(err, ErrAddressDimensionConflict) {
			return "", opjob.TerminalError(err)
		}
		if err != nil {
			return "", err
		}
		return "dimension-object-deleted:" + string(deleted.SnapshotID), nil
	}
}

// AddressDimensionGCProducer scans for eligible objects and enqueues reclamation
// jobs; deletion/retry/lease/cancel stay in the opjob engine.
type AddressDimensionGCProducer struct {
	Publisher *Publisher
	Jobs      opjob.Repository
	Interval  time.Duration
	Batch     int
	Now       func() time.Time
	Logf      func(string, ...any)
}

func (producer AddressDimensionGCProducer) RunOnce(ctx context.Context) (int, error) {
	if producer.Publisher == nil || producer.Jobs == nil {
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
	candidates, err := producer.Publisher.ListAllAddressDimensionGCCandidates(ctx, now, batch)
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
