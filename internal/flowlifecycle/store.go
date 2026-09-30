package flowlifecycle

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base32"
	"errors"
	"strings"
	"time"
)

var (
	ErrVersionConflict = errors.New("Flow retention policy row version conflict")
	ErrTransition      = errors.New("Flow retention policy transition is invalid")
)

type PolicyFilter struct {
	Status string
	Search string
	Sort   string
	Order  string
	Limit  int
	Offset int
}

type Store struct{ db *sql.DB }

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

const policyColumns = `id,policy_version,status,bootstrap_from,raw_retention_seconds,
	archive_resolution_seconds,archive_retention_seconds,late_arrival_seconds,delete_grace_seconds,
	max_partitions_per_run,raw_delete_enabled,archive_delete_enabled,require_backup_before_delete,
	auto_delete,waive_kafka_coverage,
	row_version,COALESCE(created_by,''),COALESCE(published_by,''),COALESCE(retired_by,''),created_at,published_at,retired_at`

type rowScanner interface{ Scan(...any) error }

func scanPolicy(row rowScanner) (Policy, error) {
	var policy Policy
	var publishedAt, retiredAt sql.NullTime
	err := row.Scan(&policy.ID, &policy.Version, &policy.Status, &policy.BootstrapFrom, &policy.RawRetentionSeconds,
		&policy.ArchiveResolutionSeconds, &policy.ArchiveRetentionSeconds, &policy.LateArrivalSeconds, &policy.DeleteGraceSeconds,
		&policy.MaxPartitionsPerRun, &policy.RawDeleteEnabled, &policy.ArchiveDeleteEnabled, &policy.RequireBackupBeforeDelete,
		&policy.AutoDelete, &policy.WaiveKafkaCoverage,
		&policy.RowVersion, &policy.CreatedBy, &policy.PublishedBy, &policy.RetiredBy, &policy.CreatedAt, &publishedAt, &retiredAt)
	if publishedAt.Valid {
		policy.PublishedAt = publishedAt.Time
	}
	if retiredAt.Valid {
		policy.RetiredAt = retiredAt.Time
	}
	return policy, err
}

func (store *Store) ListPolicies(ctx context.Context, filter PolicyFilter) ([]Policy, uint64, error) {
	if store == nil || store.db == nil {
		return nil, 0, errors.New("Flow lifecycle store is not initialized")
	}
	if filter.Limit <= 0 {
		filter.Limit = 25
	}
	if filter.Limit > 200 || filter.Offset < 0 || (filter.Status != "" && filter.Status != PolicyDraft && filter.Status != PolicyPublished && filter.Status != PolicyRetired) {
		return nil, 0, ErrInvalidPolicy
	}
	sortColumns := map[string]string{"version": "policy_version", "status": "status", "bootstrap_from": "bootstrap_from", "created_at": "created_at", "published_at": "published_at"}
	sortColumn := sortColumns[filter.Sort]
	if sortColumn == "" {
		sortColumn = "policy_version"
	}
	order := "DESC"
	if strings.EqualFold(filter.Order, "asc") {
		order = "ASC"
	} else if filter.Order != "" && !strings.EqualFold(filter.Order, "desc") {
		return nil, 0, ErrInvalidPolicy
	}
	where := []string{"1=1"}
	args := make([]any, 0, 5)
	if filter.Status != "" {
		where = append(where, "status=?")
		args = append(args, filter.Status)
	}
	if filter.Search = strings.TrimSpace(filter.Search); filter.Search != "" {
		where = append(where, "(id LIKE ? OR status LIKE ?)")
		like := "%" + filter.Search + "%"
		args = append(args, like, like)
	}
	clause := strings.Join(where, " AND ")
	var total uint64
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM flow_retention_policy_revisions WHERE "+clause, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	queryArgs := append(append([]any(nil), args...), filter.Limit, filter.Offset)
	rows, err := store.db.QueryContext(ctx, "SELECT "+policyColumns+" FROM flow_retention_policy_revisions WHERE "+clause+" ORDER BY "+sortColumn+" "+order+",id ASC LIMIT ? OFFSET ?", queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]Policy, 0)
	for rows.Next() {
		item, err := scanPolicy(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (store *Store) GetPolicy(ctx context.Context, id string) (Policy, error) {
	if store == nil || store.db == nil || strings.TrimSpace(id) == "" {
		return Policy{}, ErrInvalidPolicy
	}
	return scanPolicy(store.db.QueryRowContext(ctx, "SELECT "+policyColumns+" FROM flow_retention_policy_revisions WHERE id=?", id))
}

func (store *Store) CreateDraft(ctx context.Context, policy Policy, actor string) (Policy, error) {
	if store == nil || store.db == nil {
		return Policy{}, errors.New("Flow lifecycle store is not initialized")
	}
	if policy.ID == "" {
		policy.ID = newID()
	}
	policy.Version = 1
	policy.Status = PolicyDraft
	policy, err := NormalizePolicy(policy)
	if err != nil {
		return Policy{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Policy{}, err
	}
	defer tx.Rollback()
	var installationID uint8
	if err := tx.QueryRowContext(ctx, "SELECT id FROM watchdog_installation WHERE id=1 FOR UPDATE").Scan(&installationID); err != nil {
		return Policy{}, err
	}
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(policy_version),0)+1 FROM flow_retention_policy_revisions").Scan(&policy.Version); err != nil {
		return Policy{}, err
	}
	if policy.Version > uint64(^uint32(0)) {
		return Policy{}, ErrInvalidPolicy
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO flow_retention_policy_revisions
		(id,policy_version,status,bootstrap_from,raw_retention_seconds,archive_resolution_seconds,
		 archive_retention_seconds,late_arrival_seconds,delete_grace_seconds,max_partitions_per_run,
		 raw_delete_enabled,archive_delete_enabled,require_backup_before_delete,auto_delete,waive_kafka_coverage,created_by)
		VALUES (?,?,'draft',?,?,?,?,?,?,?,?,?,?,?,?,NULLIF(?,''))`,
		policy.ID, policy.Version, policy.BootstrapFrom, policy.RawRetentionSeconds, policy.ArchiveResolutionSeconds,
		policy.ArchiveRetentionSeconds, policy.LateArrivalSeconds, policy.DeleteGraceSeconds, policy.MaxPartitionsPerRun,
		policy.RawDeleteEnabled, policy.ArchiveDeleteEnabled, policy.RequireBackupBeforeDelete,
		policy.AutoDelete, policy.WaiveKafkaCoverage, strings.TrimSpace(actor))
	if err != nil {
		return Policy{}, err
	}
	if err := tx.Commit(); err != nil {
		return Policy{}, err
	}
	return store.GetPolicy(ctx, policy.ID)
}

func (store *Store) UpdateDraft(ctx context.Context, policy Policy, expected uint64) (Policy, error) {
	policy.Status = PolicyDraft
	policy, err := NormalizePolicy(policy)
	if err != nil || expected == 0 {
		return Policy{}, ErrInvalidPolicy
	}
	result, err := store.db.ExecContext(ctx, `UPDATE flow_retention_policy_revisions SET
		bootstrap_from=?,raw_retention_seconds=?,archive_resolution_seconds=?,archive_retention_seconds=?,
		late_arrival_seconds=?,delete_grace_seconds=?,max_partitions_per_run=?,raw_delete_enabled=?,
		archive_delete_enabled=?,require_backup_before_delete=?,auto_delete=?,waive_kafka_coverage=?,row_version=row_version+1
		WHERE id=? AND status='draft' AND row_version=?`, policy.BootstrapFrom, policy.RawRetentionSeconds,
		policy.ArchiveResolutionSeconds, policy.ArchiveRetentionSeconds, policy.LateArrivalSeconds,
		policy.DeleteGraceSeconds, policy.MaxPartitionsPerRun, policy.RawDeleteEnabled, policy.ArchiveDeleteEnabled, policy.RequireBackupBeforeDelete,
		policy.AutoDelete, policy.WaiveKafkaCoverage, policy.ID, expected)
	if err != nil {
		return Policy{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return Policy{}, err
		}
		return Policy{}, store.mutationError(ctx, policy.ID, expected, true)
	}
	return store.GetPolicy(ctx, policy.ID)
}

func (store *Store) DeleteDraft(ctx context.Context, id string, expected uint64) error {
	if strings.TrimSpace(id) == "" || expected == 0 {
		return ErrInvalidPolicy
	}
	result, err := store.db.ExecContext(ctx, "DELETE FROM flow_retention_policy_revisions WHERE id=? AND status='draft' AND row_version=?", id, expected)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return store.mutationError(ctx, id, expected, true)
	}
	return nil
}

func (store *Store) Publish(ctx context.Context, id, actor string, expected uint64, now time.Time) (Policy, error) {
	if strings.TrimSpace(id) == "" || strings.TrimSpace(actor) == "" || expected == 0 || now.IsZero() {
		return Policy{}, ErrInvalidPolicy
	}
	now = now.UTC().Truncate(time.Millisecond)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Policy{}, err
	}
	defer tx.Rollback()
	policy, err := scanPolicy(tx.QueryRowContext(ctx, "SELECT "+policyColumns+" FROM flow_retention_policy_revisions WHERE id=? FOR UPDATE", id))
	if err != nil {
		return Policy{}, err
	}
	if policy.Status != PolicyDraft {
		return Policy{}, ErrTransition
	}
	if policy.RowVersion != expected {
		return Policy{}, ErrVersionConflict
	}
	if _, err := tx.ExecContext(ctx, `UPDATE flow_retention_policy_revisions SET status='retired',retired_by=?,retired_at=?,row_version=row_version+1 WHERE status='published'`, actor, now); err != nil {
		return Policy{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE flow_retention_policy_revisions SET status='published',published_by=?,published_at=?,row_version=row_version+1 WHERE id=? AND status='draft' AND row_version=?`, actor, now, id, expected)
	if err != nil {
		return Policy{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return Policy{}, err
		}
		return Policy{}, ErrVersionConflict
	}
	if err := tx.Commit(); err != nil {
		return Policy{}, err
	}
	return store.GetPolicy(ctx, id)
}

func (store *Store) Retire(ctx context.Context, id, actor string, expected uint64, now time.Time) (Policy, error) {
	if strings.TrimSpace(id) == "" || strings.TrimSpace(actor) == "" || expected == 0 || now.IsZero() {
		return Policy{}, ErrInvalidPolicy
	}
	result, err := store.db.ExecContext(ctx, `UPDATE flow_retention_policy_revisions SET status='retired',retired_by=?,retired_at=?,row_version=row_version+1 WHERE id=? AND status='published' AND row_version=?`, actor, now.UTC().Truncate(time.Millisecond), id, expected)
	if err != nil {
		return Policy{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return Policy{}, err
		}
		return Policy{}, store.mutationError(ctx, id, expected, false)
	}
	return store.GetPolicy(ctx, id)
}

func (store *Store) mutationError(ctx context.Context, id string, expected uint64, draftOnly bool) error {
	policy, err := store.GetPolicy(ctx, id)
	if err != nil {
		return err
	}
	if policy.RowVersion != expected {
		return ErrVersionConflict
	}
	if (draftOnly && policy.Status != PolicyDraft) || (!draftOnly && policy.Status != PolicyPublished) {
		return ErrTransition
	}
	return ErrVersionConflict
}

var idEncoding = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

func newID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return idEncoding.EncodeToString(value[:])
}
