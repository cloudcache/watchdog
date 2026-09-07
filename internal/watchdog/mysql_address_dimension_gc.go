package watchdog

import (
	"context"
	"database/sql"
	"time"
)

const addressDimensionGCCandidateSafety = `
	s.status = 'retired'
	AND s.retired_at IS NOT NULL
	AND s.retention_until IS NOT NULL
	AND s.retention_until <= ?
	AND s.object_deleted_at IS NULL
	AND NOT EXISTS (
		SELECT 1 FROM dimension_snapshot_references AS snapshot_reference
		WHERE snapshot_reference.tenant_id = s.tenant_id AND snapshot_reference.snapshot_id = s.id
		  AND snapshot_reference.retain_until > ?
	)
	AND NOT EXISTS (
		SELECT 1 FROM dimension_snapshot_activations AS future_activation
		WHERE future_activation.tenant_id = s.tenant_id
		  AND future_activation.module_key = s.module_key
		  AND future_activation.dimension_key = s.dimension_key
		  AND future_activation.snapshot_id = s.id
		  AND future_activation.effective_from > ?
	)
	AND NOT EXISTS (
		SELECT 1 FROM dimension_snapshot_activations AS current_activation
		WHERE current_activation.tenant_id = s.tenant_id
		  AND current_activation.module_key = s.module_key
		  AND current_activation.dimension_key = s.dimension_key
		  AND current_activation.snapshot_id = s.id
		  AND current_activation.effective_from <= ?
		  AND NOT EXISTS (
			SELECT 1 FROM dimension_snapshot_activations AS later_activation
			WHERE later_activation.tenant_id = current_activation.tenant_id
			  AND later_activation.module_key = current_activation.module_key
			  AND later_activation.dimension_key = current_activation.dimension_key
			  AND later_activation.effective_from <= ?
			  AND later_activation.effective_from > current_activation.effective_from
		  )
	)
	AND NOT EXISTS (
		SELECT 1 FROM dimension_snapshot_acks AS candidate_ack
		WHERE candidate_ack.tenant_id = s.tenant_id
		  AND candidate_ack.snapshot_id = s.id
		  AND candidate_ack.state = 'installed'
		  AND NOT EXISTS (
			SELECT 1
			FROM dimension_snapshot_acks AS later_ack
			JOIN dimension_snapshots AS later_snapshot
			  ON later_snapshot.tenant_id = later_ack.tenant_id
			 AND later_snapshot.id = later_ack.snapshot_id
			WHERE later_ack.tenant_id = candidate_ack.tenant_id
			  AND later_ack.worker_id = candidate_ack.worker_id
			  AND later_ack.state = 'installed'
			  AND later_snapshot.module_key = s.module_key
			  AND later_snapshot.dimension_key = s.dimension_key
			  AND (
				later_ack.installed_at > candidate_ack.installed_at
				OR (later_ack.installed_at = candidate_ack.installed_at AND later_snapshot.version > s.version)
				OR (later_ack.installed_at = candidate_ack.installed_at AND later_snapshot.version = s.version AND later_ack.snapshot_id > candidate_ack.snapshot_id)
			  )
		  )
	)`

func (p *MySQLDimensionPublicationStore) ScheduleDimensionPublicationObjectGC(ctx context.Context, tenantID, actorID, snapshotID ID, expectedRowVersion uint64, retentionUntil time.Time) (DimensionPublicationSnapshot, error) {
	if p == nil || p.store == nil || tenantID == "" || actorID == "" || snapshotID == "" || expectedRowVersion == 0 || retentionUntil.IsZero() || retentionUntil.Nanosecond()%int(time.Millisecond) != 0 {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	retentionUntil = retentionUntil.UTC()
	now := p.now().UTC()
	if !retentionUntil.After(now) {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublicationTenant(ctx, tx, tenantID); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, tenantID, snapshotID, true)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if snapshot.RowVersion != expectedRowVersion {
		return AddressDimensionSnapshot{}, ErrAddressDimensionConflict
	}
	if snapshot.Status != AddressDimensionStatusRetired || snapshot.ObjectDeletedAt != nil || (snapshot.RetentionUntil != nil && retentionUntil.Before(*snapshot.RetentionUntil)) {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	var latestReference sql.NullTime
	if err := tx.QueryRowContext(ctx, `
		SELECT MAX(retain_until) FROM dimension_snapshot_references
		WHERE tenant_id = ? AND snapshot_id = ?
	`, tenantID, snapshotID).Scan(&latestReference); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if latestReference.Valid && retentionUntil.Before(latestReference.Time) {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalidTransition
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE dimension_snapshots
		SET retention_until = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND id = ? AND row_version = ? AND object_deleted_at IS NULL
	`, retentionUntil, tenantID, p.scope.ModuleKey, p.scope.DimensionKey, snapshotID, expectedRowVersion)
	if err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := insertAddressDimensionAudit(ctx, tx, tenantID, actorID, snapshotID, "dimension.snapshot.gc_scheduled", map[string]any{
		"retention_until": retentionUntil, "version": snapshot.Version,
	}); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionSnapshot{}, err
	}
	return p.GetDimensionPublicationSnapshot(ctx, tenantID, snapshotID)
}

func (p *MySQLDimensionPublicationStore) ListDimensionPublicationGCCandidates(ctx context.Context, tenantID ID, asOf time.Time, filter DimensionPublicationGCFilter) ([]DimensionPublicationGCCandidate, string, error) {
	if p == nil || p.store == nil || tenantID == "" || asOf.IsZero() {
		return nil, "", ErrAddressDimensionInvalid
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	query := `
		SELECT s.tenant_id, s.id, s.version, s.object_ref, s.checksum, s.retention_until, s.retired_at
		FROM dimension_snapshots AS s
		WHERE s.tenant_id = ? AND s.module_key = ? AND s.dimension_key = ? AND ` + addressDimensionGCCandidateSafety
	args := []any{tenantID, p.scope.ModuleKey, p.scope.DimensionKey, asOf.UTC(), asOf.UTC(), asOf.UTC(), asOf.UTC(), asOf.UTC()}
	if filter.Cursor != "" {
		cursorTenant, cursorSnapshot, err := decodeStringCursor(filter.Cursor)
		if err != nil || ID(cursorTenant) != tenantID || cursorSnapshot == "" {
			return nil, "", ErrAddressDimensionInvalid
		}
		query += ` AND s.id > ?`
		args = append(args, cursorSnapshot)
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
		last := items[len(items)-1]
		next = encodeStringCursor(string(last.TenantID), last.SnapshotID)
	}
	return items, next, nil
}

func (p *MySQLDimensionPublicationStore) ListAllDimensionPublicationGCCandidates(ctx context.Context, asOf time.Time, limit int) ([]DimensionPublicationGCCandidate, error) {
	if p == nil || p.store == nil || asOf.IsZero() || limit <= 0 || limit > 1_000 {
		return nil, ErrAddressDimensionInvalid
	}
	query := `
		SELECT s.tenant_id, s.id, s.version, s.object_ref, s.checksum, s.retention_until, s.retired_at
		FROM dimension_snapshots AS s
		WHERE s.module_key = ? AND s.dimension_key = ? AND ` + addressDimensionGCCandidateSafety + `
		ORDER BY s.tenant_id, s.id LIMIT ?`
	return p.scanDimensionPublicationGCCandidates(ctx, query,
		p.scope.ModuleKey, p.scope.DimensionKey,
		asOf.UTC(), asOf.UTC(), asOf.UTC(), asOf.UTC(), asOf.UTC(), limit,
	)
}

func (p *MySQLDimensionPublicationStore) scanDimensionPublicationGCCandidates(ctx context.Context, query string, args ...any) ([]DimensionPublicationGCCandidate, error) {
	rows, err := p.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]AddressDimensionGCCandidate, 0)
	for rows.Next() {
		var item AddressDimensionGCCandidate
		if err := rows.Scan(&item.TenantID, &item.SnapshotID, &item.Version, &item.ObjectRef, &item.Checksum, &item.RetentionUntil, &item.RetiredAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (p *MySQLDimensionPublicationStore) DeleteDimensionPublicationObject(ctx context.Context, tenantID, snapshotID, jobID, actorID ID, expectedObjectRef, expectedChecksum string, expectedRetention, asOf time.Time) (DimensionPublicationObjectDeletion, error) {
	if p == nil || p.store == nil || tenantID == "" || snapshotID == "" || jobID == "" || expectedObjectRef == "" || !validSHA256Digest(expectedChecksum) || expectedRetention.IsZero() || asOf.IsZero() {
		return AddressDimensionObjectDeletion{}, ErrAddressDimensionInvalid
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublicationTenant(ctx, tx, tenantID); err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	snapshot, err := getDimensionPublicationSnapshotTx(ctx, tx, p.scope, tenantID, snapshotID, true)
	if err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	if snapshot.ObjectRef != expectedObjectRef || snapshot.Checksum != expectedChecksum || snapshot.RetentionUntil == nil || !snapshot.RetentionUntil.Equal(expectedRetention.UTC()) {
		return AddressDimensionObjectDeletion{}, ErrAddressDimensionConflict
	}
	if snapshot.ObjectDeletedAt != nil {
		if err := recordDimensionPublicationObjectDestruction(ctx, tx, snapshot, jobID, actorID, snapshot.ObjectDeletedAt.UTC()); err != nil {
			return AddressDimensionObjectDeletion{}, err
		}
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
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND id = ? AND object_deleted_at IS NULL
	`, deletedAt, tenantID, p.scope.ModuleKey, p.scope.DimensionKey, snapshotID)
	if err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	if err := requireOneAddressDimensionRow(result); err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	if err := recordDimensionPublicationObjectDestruction(ctx, tx, snapshot, jobID, actorID, deletedAt); err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	if err := tx.Commit(); err != nil {
		return AddressDimensionObjectDeletion{}, err
	}
	return AddressDimensionObjectDeletion{SnapshotID: snapshot.ID, ObjectRef: snapshot.ObjectRef, DeletedAt: deletedAt}, nil
}

func recordDimensionPublicationObjectDestruction(ctx context.Context, tx *sql.Tx, snapshot DimensionPublicationSnapshot, jobID, actorID ID, deletedAt time.Time) error {
	return recordDestructionReceipt(ctx, tx, DestructionReceipt{
		TenantID: snapshot.TenantID, JobID: jobID, ResourceType: "dimension_object",
		ResourceID: snapshot.ID, ActorID: actorID,
		Impact: map[string]int{"objects": 1}, SeriesMatch: snapshot.ObjectRef, DestroyedAt: deletedAt,
	})
}

func dimensionPublicationObjectGCEligibleTx(ctx context.Context, tx *sql.Tx, snapshot DimensionPublicationSnapshot, asOf time.Time) (bool, error) {
	if snapshot.Status != AddressDimensionStatusRetired || snapshot.RetiredAt == nil || snapshot.RetentionUntil == nil || asOf.Before(*snapshot.RetentionUntil) {
		return false, nil
	}
	var blockers uint64
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM dimension_snapshot_references
		WHERE tenant_id = ? AND snapshot_id = ? AND retain_until > ?
	`, snapshot.TenantID, snapshot.ID, asOf).Scan(&blockers); err != nil || blockers > 0 {
		return false, err
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM dimension_snapshot_activations AS activation
		WHERE activation.tenant_id = ? AND activation.module_key = ? AND activation.dimension_key = ?
		  AND activation.snapshot_id = ?
		  AND (
			activation.effective_from > ?
			OR activation.effective_from = (
				SELECT MAX(current_activation.effective_from)
				FROM dimension_snapshot_activations AS current_activation
				WHERE current_activation.tenant_id = activation.tenant_id
				  AND current_activation.module_key = activation.module_key
				  AND current_activation.dimension_key = activation.dimension_key
				  AND current_activation.effective_from <= ?
			)
		  )
	`, snapshot.TenantID, snapshot.ModuleKey, snapshot.DimensionKey, snapshot.ID, asOf, asOf).Scan(&blockers); err != nil || blockers > 0 {
		return false, err
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM dimension_snapshot_acks AS candidate_ack
		WHERE candidate_ack.tenant_id = ? AND candidate_ack.snapshot_id = ? AND candidate_ack.state = 'installed'
		  AND NOT EXISTS (
			SELECT 1
			FROM dimension_snapshot_acks AS later_ack
			JOIN dimension_snapshots AS later_snapshot
			  ON later_snapshot.tenant_id = later_ack.tenant_id AND later_snapshot.id = later_ack.snapshot_id
			WHERE later_ack.tenant_id = candidate_ack.tenant_id
			  AND later_ack.worker_id = candidate_ack.worker_id
			  AND later_ack.state = 'installed'
			  AND later_snapshot.module_key = ? AND later_snapshot.dimension_key = ?
			  AND (
				later_ack.installed_at > candidate_ack.installed_at
				OR (later_ack.installed_at = candidate_ack.installed_at AND later_snapshot.version > ?)
				OR (later_ack.installed_at = candidate_ack.installed_at AND later_snapshot.version = ? AND later_ack.snapshot_id > candidate_ack.snapshot_id)
			  )
		  )
	`, snapshot.TenantID, snapshot.ID, snapshot.ModuleKey, snapshot.DimensionKey, snapshot.Version, snapshot.Version).Scan(&blockers); err != nil || blockers > 0 {
		return false, err
	}
	return true, nil
}

func (p *MySQLAddressDimensionPublisher) ScheduleAddressDimensionObjectGC(ctx context.Context, tenantID, actorID, snapshotID ID, expectedRowVersion uint64, retentionUntil time.Time) (AddressDimensionSnapshot, error) {
	if p == nil || p.MySQLDimensionPublicationStore == nil {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	return p.ScheduleDimensionPublicationObjectGC(ctx, tenantID, actorID, snapshotID, expectedRowVersion, retentionUntil)
}

func (p *MySQLAddressDimensionPublisher) ListAddressDimensionGCCandidates(ctx context.Context, tenantID ID, asOf time.Time, filter AddressDimensionGCFilter) ([]AddressDimensionGCCandidate, string, error) {
	if p == nil || p.MySQLDimensionPublicationStore == nil {
		return nil, "", ErrAddressDimensionInvalid
	}
	return p.ListDimensionPublicationGCCandidates(ctx, tenantID, asOf, filter)
}

func (p *MySQLAddressDimensionPublisher) ListAllAddressDimensionGCCandidates(ctx context.Context, asOf time.Time, limit int) ([]AddressDimensionGCCandidate, error) {
	if p == nil || p.MySQLDimensionPublicationStore == nil {
		return nil, ErrAddressDimensionInvalid
	}
	return p.ListAllDimensionPublicationGCCandidates(ctx, asOf, limit)
}

func (p *MySQLAddressDimensionPublisher) DeleteAddressDimensionObject(ctx context.Context, tenantID, snapshotID, jobID, actorID ID, expectedObjectRef, expectedChecksum string, expectedRetention, asOf time.Time) (AddressDimensionObjectDeletion, error) {
	if p == nil || p.MySQLDimensionPublicationStore == nil {
		return AddressDimensionObjectDeletion{}, ErrAddressDimensionInvalid
	}
	return p.DeleteDimensionPublicationObject(ctx, tenantID, snapshotID, jobID, actorID, expectedObjectRef, expectedChecksum, expectedRetention, asOf)
}

var _ AddressDimensionGCRepository = (*MySQLAddressDimensionPublisher)(nil)
