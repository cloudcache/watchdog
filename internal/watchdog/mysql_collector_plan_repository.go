package watchdog

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrCollectorPlanConflict          = errors.New("collector plan version conflict")
	ErrCollectorPlanInvalidTransition = errors.New("collector plan transition is invalid")
)

const collectorPlanSelectColumns = `
	id, tenant_id, collector_id, config_version, plan_schema_version, status,
	spec_json, spec_hash, COALESCE(signing_key_id, ''), signature,
	validation_json, not_before, expires_at, COALESCE(supersedes_config_version, 0),
	created_by, updated_by, row_version, created_at, updated_at, activated_at, retired_at`

type collectorPlanRow interface {
	Scan(...any) error
}

func (s *MySQLStore) CreateCollectorPlanRevision(ctx context.Context, plan CollectorPlanRevision) (CollectorPlanRevision, error) {
	if err := ValidateNewCollectorPlanRevision(plan); err != nil {
		return CollectorPlanRevision{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	defer tx.Rollback()

	var currentVersion uint64
	var schemaMin, schemaMax uint16
	var collectorStatus string
	if err := tx.QueryRowContext(ctx, `
		SELECT config_version, plan_schema_min, plan_schema_max, status
		FROM collector_agents
		WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL
		FOR UPDATE
	`, plan.CollectorID, plan.TenantID).Scan(&currentVersion, &schemaMin, &schemaMax, &collectorStatus); err != nil {
		return CollectorPlanRevision{}, err
	}
	if collectorStatus == "revoked" || collectorStatus == "deleted" || plan.ConfigVersion <= currentVersion || plan.PlanSchemaVersion < schemaMin || plan.PlanSchemaVersion > schemaMax {
		return CollectorPlanRevision{}, ErrCollectorPlanInvalidTransition
	}
	if currentVersion == 0 {
		if plan.SupersedesConfigVersion != 0 {
			return CollectorPlanRevision{}, ErrCollectorPlanInvalidTransition
		}
	} else if plan.SupersedesConfigVersion != currentVersion {
		return CollectorPlanRevision{}, ErrCollectorPlanInvalidTransition
	}

	var validation any
	if len(plan.ValidationJSON) != 0 {
		validation = string(plan.ValidationJSON)
	}
	var notBefore any
	if !plan.NotBefore.IsZero() {
		notBefore = plan.NotBefore.UTC()
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO collector_plan_revisions (
			id, tenant_id, collector_id, config_version, plan_schema_version,
			status, spec_json, spec_hash, signing_key_id, signature,
			validation_json, not_before, expires_at, supersedes_config_version,
			created_by, updated_by
		) VALUES (?, ?, ?, ?, ?, 'validated', ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), ?, ?)
	`, plan.ID, plan.TenantID, plan.CollectorID, plan.ConfigVersion, plan.PlanSchemaVersion,
		string(plan.SpecJSON), plan.SpecHash, plan.SigningKeyID, plan.Signature,
		validation, notBefore, plan.ExpiresAt.UTC(), plan.SupersedesConfigVersion,
		plan.CreatedBy, plan.CreatedBy)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	if err := insertCollectorPlanAudit(ctx, tx, plan.TenantID, plan.CreatedBy, plan.ID, "collector.plan.created", map[string]any{
		"collector_id": plan.CollectorID, "config_version": plan.ConfigVersion,
		"plan_schema_version": plan.PlanSchemaVersion, "spec_hash": plan.SpecHash,
	}); err != nil {
		return CollectorPlanRevision{}, err
	}
	created, err := getCollectorPlanRevisionTx(ctx, tx, plan.TenantID, plan.CollectorID, plan.ConfigVersion, false)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	if err := tx.Commit(); err != nil {
		return CollectorPlanRevision{}, err
	}
	return created, nil
}

func (s *MySQLStore) GetCollectorPlanRevision(ctx context.Context, tenantID, collectorID ID, configVersion uint64) (CollectorPlanRevision, error) {
	if tenantID == "" || collectorID == "" || configVersion == 0 {
		return CollectorPlanRevision{}, errors.New("collector plan tenant, collector, and version are required")
	}
	return scanCollectorPlanRevision(s.db.QueryRowContext(ctx, `
		SELECT `+collectorPlanSelectColumns+`
		FROM collector_plan_revisions
		WHERE tenant_id = ? AND collector_id = ? AND config_version = ?
	`, tenantID, collectorID, configVersion))
}

func (s *MySQLStore) GetActiveCollectorPlan(ctx context.Context, tenantID, collectorID ID) (CollectorPlanRevision, error) {
	if tenantID == "" || collectorID == "" {
		return CollectorPlanRevision{}, errors.New("collector plan tenant and collector are required")
	}
	return scanCollectorPlanRevision(s.db.QueryRowContext(ctx, `
		SELECT `+collectorPlanSelectColumns+`
		FROM collector_plan_revisions AS plan
		WHERE plan.tenant_id = ? AND plan.collector_id = ? AND plan.status = 'active'
		  AND EXISTS (
			SELECT 1 FROM collector_agents AS collector
			WHERE collector.tenant_id = plan.tenant_id AND collector.id = plan.collector_id
			  AND collector.status = 'active' AND collector.deleted_at IS NULL
		  )
	`, tenantID, collectorID))
}

func (s *MySQLStore) ActivateCollectorPlanRevision(ctx context.Context, activation CollectorPlanActivation) (CollectorPlanRevision, error) {
	if activation.TenantID == "" || activation.CollectorID == "" || activation.ConfigVersion == 0 || activation.ExpectedCollectorRowVersion == 0 || activation.ExpectedPlanRowVersion == 0 || activation.ActorID == "" {
		return CollectorPlanRevision{}, errors.New("collector plan activation input is incomplete")
	}
	activatedAt := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	defer tx.Rollback()

	var currentVersion, collectorRowVersion uint64
	var schemaMin, schemaMax uint16
	var collectorStatus string
	if err := tx.QueryRowContext(ctx, `
		SELECT config_version, plan_schema_min, plan_schema_max, status, row_version
		FROM collector_agents
		WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL
		FOR UPDATE
	`, activation.CollectorID, activation.TenantID).Scan(
		&currentVersion, &schemaMin, &schemaMax, &collectorStatus, &collectorRowVersion,
	); err != nil {
		return CollectorPlanRevision{}, err
	}
	if collectorRowVersion != activation.ExpectedCollectorRowVersion {
		return CollectorPlanRevision{}, ErrCollectorPlanConflict
	}
	if collectorStatus != "pending" && collectorStatus != "active" {
		return CollectorPlanRevision{}, ErrCollectorPlanInvalidTransition
	}

	plan, err := getCollectorPlanRevisionTx(ctx, tx, activation.TenantID, activation.CollectorID, activation.ConfigVersion, true)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	if plan.RowVersion != activation.ExpectedPlanRowVersion {
		return CollectorPlanRevision{}, ErrCollectorPlanConflict
	}
	if plan.Status != CollectorPlanValidated || plan.ConfigVersion <= currentVersion || plan.PlanSchemaVersion < schemaMin || plan.PlanSchemaVersion > schemaMax || (!plan.NotBefore.IsZero() && activatedAt.Before(plan.NotBefore)) || !activatedAt.Before(plan.ExpiresAt) {
		return CollectorPlanRevision{}, ErrCollectorPlanInvalidTransition
	}
	if currentVersion == 0 {
		if plan.SupersedesConfigVersion != 0 {
			return CollectorPlanRevision{}, ErrCollectorPlanInvalidTransition
		}
	} else if plan.SupersedesConfigVersion != currentVersion {
		return CollectorPlanRevision{}, ErrCollectorPlanInvalidTransition
	}

	retiredPlanID := ID("")
	if currentVersion != 0 {
		if err := tx.QueryRowContext(ctx, `
			SELECT id
			FROM collector_plan_revisions
			WHERE tenant_id = ? AND collector_id = ? AND config_version = ? AND status = 'active'
			FOR UPDATE
		`, activation.TenantID, activation.CollectorID, currentVersion).Scan(&retiredPlanID); err != nil {
			return CollectorPlanRevision{}, err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE collector_plan_revisions
			SET status = 'retired', retired_at = ?, updated_by = ?,
				row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
			WHERE id = ? AND tenant_id = ? AND collector_id = ?
			  AND config_version = ? AND status = 'active'
		`, activatedAt, activation.ActorID, retiredPlanID, activation.TenantID, activation.CollectorID, currentVersion)
		if err != nil {
			return CollectorPlanRevision{}, err
		}
		if err := requireOneCollectorPlanRow(result); err != nil {
			return CollectorPlanRevision{}, err
		}
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE collector_plan_revisions
		SET status = 'active', activated_at = ?, updated_by = ?,
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND tenant_id = ? AND collector_id = ?
		  AND config_version = ? AND status = 'validated' AND row_version = ?
	`, activatedAt, activation.ActorID, plan.ID, activation.TenantID, activation.CollectorID,
		activation.ConfigVersion, activation.ExpectedPlanRowVersion)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	if err := requireOneCollectorPlanRow(result); err != nil {
		return CollectorPlanRevision{}, err
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE collector_agents
		SET status = 'active', config_version = ?, plan_hash = ?, plan_expires_at = ?,
			updated_by = ?, row_version = row_version + 1,
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND tenant_id = ? AND row_version = ? AND deleted_at IS NULL
	`, plan.ConfigVersion, plan.SpecHash, plan.ExpiresAt, activation.ActorID,
		activation.CollectorID, activation.TenantID, activation.ExpectedCollectorRowVersion)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	if err := requireOneCollectorPlanRow(result); err != nil {
		return CollectorPlanRevision{}, err
	}
	if retiredPlanID != "" {
		if err := insertCollectorPlanAudit(ctx, tx, activation.TenantID, activation.ActorID, retiredPlanID, "collector.plan.retired", map[string]any{
			"collector_id": activation.CollectorID, "config_version": currentVersion,
			"superseded_by": plan.ConfigVersion, "retired_at": activatedAt,
		}); err != nil {
			return CollectorPlanRevision{}, err
		}
	}
	if err := insertCollectorPlanAudit(ctx, tx, activation.TenantID, activation.ActorID, plan.ID, "collector.plan.activated", map[string]any{
		"collector_id": activation.CollectorID, "config_version": plan.ConfigVersion,
		"previous_config_version": currentVersion, "spec_hash": plan.SpecHash,
		"activated_at": activatedAt, "expires_at": plan.ExpiresAt,
	}); err != nil {
		return CollectorPlanRevision{}, err
	}
	active, err := getCollectorPlanRevisionTx(ctx, tx, activation.TenantID, activation.CollectorID, activation.ConfigVersion, false)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	if err := tx.Commit(); err != nil {
		return CollectorPlanRevision{}, err
	}
	return active, nil
}

func (s *MySQLStore) AcknowledgeCollectorPlan(ctx context.Context, acknowledgement CollectorPlanAcknowledgement) error {
	if err := validateCollectorPlanAcknowledgement(acknowledgement); err != nil {
		return err
	}
	acknowledgedAt := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var currentVersion, acknowledgedVersion, lastGoodVersion uint64
	var specHash string
	var expiresAt time.Time
	var status string
	if err := tx.QueryRowContext(ctx, `
		SELECT config_version, acknowledged_config_version, last_good_config_version,
			plan_hash, plan_expires_at, status
		FROM collector_agents
		WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL
		FOR UPDATE
	`, acknowledgement.CollectorID, acknowledgement.TenantID).Scan(
		&currentVersion, &acknowledgedVersion, &lastGoodVersion, &specHash, &expiresAt, &status,
	); err != nil {
		return err
	}
	if status != "active" || currentVersion != acknowledgement.ConfigVersion || specHash != acknowledgement.SpecHash || !acknowledgedAt.Before(expiresAt) || acknowledgedVersion > currentVersion || lastGoodVersion > acknowledgedVersion {
		return ErrCollectorPlanInvalidTransition
	}
	var activePlanID ID
	var activeSpecHash string
	var activeExpiresAt time.Time
	if err := tx.QueryRowContext(ctx, `
		SELECT id, spec_hash, expires_at
		FROM collector_plan_revisions
		WHERE tenant_id = ? AND collector_id = ? AND config_version = ? AND status = 'active'
		FOR UPDATE
	`, acknowledgement.TenantID, acknowledgement.CollectorID, acknowledgement.ConfigVersion).Scan(
		&activePlanID, &activeSpecHash, &activeExpiresAt,
	); err != nil {
		return err
	}
	if activeSpecHash != specHash || !activeExpiresAt.Equal(expiresAt) {
		return ErrCollectorPlanInvalidTransition
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE collector_agents
		SET acknowledged_config_version = ?, last_good_config_version = ?,
			boot_id = ?, software_version = ?, observed_health = 'healthy',
			last_seen_at = IF(last_seen_at IS NULL OR last_seen_at < ?, ?, last_seen_at),
			last_error_code = NULL, last_error_detail = NULL
		WHERE id = ? AND tenant_id = ? AND config_version = ?
		  AND plan_hash = ? AND deleted_at IS NULL
	`, acknowledgement.ConfigVersion, acknowledgement.ConfigVersion,
		acknowledgement.BootID, acknowledgement.SoftwareVersion, acknowledgedAt, acknowledgedAt,
		acknowledgement.CollectorID, acknowledgement.TenantID,
		acknowledgement.ConfigVersion, acknowledgement.SpecHash)
	if err != nil {
		return err
	}
	if acknowledgedVersion < acknowledgement.ConfigVersion || lastGoodVersion < acknowledgement.ConfigVersion {
		if err := insertCollectorPlanAudit(ctx, tx, acknowledgement.TenantID, "", activePlanID, "collector.plan.acknowledged", map[string]any{
			"config_version": acknowledgement.ConfigVersion, "spec_hash": acknowledgement.SpecHash,
			"boot_id": acknowledgement.BootID, "acknowledged_at": acknowledgedAt,
		}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *MySQLStore) RecordCollectorPlanFailure(ctx context.Context, failure CollectorPlanFailure) error {
	if failure.TenantID == "" || failure.CollectorID == "" {
		return errors.New("collector plan failure identity is required")
	}
	if err := validateCollectorPlanFailureReport(failure.CollectorPlanFailureReport); err != nil {
		return err
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE collector_agents
		SET observed_health = 'degraded',
			boot_id = IF(boot_id = '', ?, boot_id),
			software_version = ?, last_seen_at = ?,
			last_error_code = ?, last_error_detail = NULLIF(?, ''),
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND tenant_id = ? AND status = 'active' AND deleted_at IS NULL
		  AND (? = 0 OR config_version = ?)
	`, failure.BootID, failure.SoftwareVersion, now,
		failure.Stage+":"+failure.Code, failure.Detail,
		failure.CollectorID, failure.TenantID,
		failure.FailedConfigVersion, failure.FailedConfigVersion)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrCollectorPlanInvalidTransition
	}
	return nil
}

func getCollectorPlanRevisionTx(ctx context.Context, tx *sql.Tx, tenantID, collectorID ID, configVersion uint64, lock bool) (CollectorPlanRevision, error) {
	query := `SELECT ` + collectorPlanSelectColumns + `
		FROM collector_plan_revisions
		WHERE tenant_id = ? AND collector_id = ? AND config_version = ?`
	if lock {
		query += ` FOR UPDATE`
	}
	return scanCollectorPlanRevision(tx.QueryRowContext(ctx, query, tenantID, collectorID, configVersion))
}

func scanCollectorPlanRevision(row collectorPlanRow) (CollectorPlanRevision, error) {
	var plan CollectorPlanRevision
	var status string
	var signature, specJSON, validationJSON []byte
	var notBefore, expiresAt, activatedAt, retiredAt sql.NullTime
	if err := row.Scan(
		&plan.ID, &plan.TenantID, &plan.CollectorID, &plan.ConfigVersion,
		&plan.PlanSchemaVersion, &status, &specJSON, &plan.SpecHash,
		&plan.SigningKeyID, &signature, &validationJSON, &notBefore, &expiresAt,
		&plan.SupersedesConfigVersion, &plan.CreatedBy, &plan.UpdatedBy,
		&plan.RowVersion, &plan.CreatedAt, &plan.UpdatedAt, &activatedAt, &retiredAt,
	); err != nil {
		return CollectorPlanRevision{}, err
	}
	plan.Status = CollectorPlanStatus(status)
	canonicalSpec, canonicalHash, err := CanonicalCollectorPlanJSON(specJSON)
	if err != nil || canonicalHash != plan.SpecHash {
		return CollectorPlanRevision{}, errors.New("stored collector plan canonical spec or hash is invalid")
	}
	plan.SpecJSON = canonicalSpec
	plan.Signature = append([]byte(nil), signature...)
	plan.ValidationJSON = append(json.RawMessage(nil), validationJSON...)
	plan.NotBefore = notBefore.Time
	plan.ExpiresAt = expiresAt.Time
	plan.ActivatedAt = activatedAt.Time
	plan.RetiredAt = retiredAt.Time
	if err := validateStoredCollectorPlanRevision(plan); err != nil {
		return CollectorPlanRevision{}, err
	}
	return plan, nil
}

func validateStoredCollectorPlanRevision(plan CollectorPlanRevision) error {
	if plan.ID == "" || plan.TenantID == "" || plan.CollectorID == "" || plan.ConfigVersion == 0 || plan.PlanSchemaVersion == 0 || plan.RowVersion == 0 || plan.CreatedBy == "" || plan.UpdatedBy == "" || plan.CreatedAt.IsZero() || plan.UpdatedAt.Before(plan.CreatedAt) || plan.ExpiresAt.IsZero() || plan.SigningKeyID == "" || len(plan.Signature) != ed25519.SignatureSize || !json.Valid(plan.SpecJSON) {
		return errors.New("stored collector plan revision is invalid")
	}
	switch plan.Status {
	case CollectorPlanValidated:
		if !plan.ActivatedAt.IsZero() || !plan.RetiredAt.IsZero() {
			return errors.New("stored validated collector plan has lifecycle timestamps")
		}
	case CollectorPlanActive:
		if plan.ActivatedAt.IsZero() || !plan.RetiredAt.IsZero() {
			return errors.New("stored active collector plan lifecycle is invalid")
		}
	case CollectorPlanRetired:
		if plan.ActivatedAt.IsZero() || plan.RetiredAt.IsZero() || plan.RetiredAt.Before(plan.ActivatedAt) {
			return errors.New("stored retired collector plan lifecycle is invalid")
		}
	default:
		return errors.New("stored collector plan status is unsupported")
	}
	return nil
}

func requireOneCollectorPlanRow(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrCollectorPlanConflict
	}
	return nil
}

func insertCollectorPlanAudit(ctx context.Context, tx *sql.Tx, tenantID, actorID, resourceID ID, action string, detail map[string]any) error {
	payload, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	createdAt := time.Now().UTC()
	var actor any
	if actorID != "" {
		actor = actorID
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO audit_logs (
			id, tenant_id, actor_id, action, resource_type, resource_id, detail_json, created_at
		) VALUES (?, ?, ?, ?, 'collector_plan', ?, ?, ?)
	`, stableID("audit", string(resourceID), action, createdAt.Format(time.RFC3339Nano)),
		tenantID, actor, action, resourceID, payload, createdAt)
	if err != nil {
		return fmt.Errorf("insert collector plan audit: %w", err)
	}
	return nil
}

var _ CollectorPlanRepository = (*MySQLStore)(nil)
var _ CollectorPlanRuntimeRepository = (*MySQLStore)(nil)
