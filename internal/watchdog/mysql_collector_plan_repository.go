package watchdog

import (
	"bytes"
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

	if err := insertCollectorPlanRevisionTx(ctx, tx, plan); err != nil {
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

// CreateNextCollectorPlanRevision is the operator creation path. The collector
// lock serializes version allocation, and the signing-key lock fences a
// concurrent rotate/revoke across signing and INSERT.
func (s *MySQLStore) CreateNextCollectorPlanRevision(ctx context.Context, request CollectorPlanCreateRequest, signer CollectorPlanSigner) (CollectorPlanRevision, error) {
	if signer == nil {
		return CollectorPlanRevision{}, ErrCollectorPlanSigningKeyUnavailable
	}
	transactionSigner, ok := signer.(collectorPlanTransactionSigner)
	if !ok {
		return CollectorPlanRevision{}, ErrCollectorPlanSigningKeyUnavailable
	}
	if err := validateCollectorPlanCreateRequest(request, time.Now().UTC()); err != nil {
		return CollectorPlanRevision{}, err
	}
	canonical, hash, err := canonicalCollectorPlanCreateSpec(request)
	if err != nil {
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
	`, request.CollectorID, request.TenantID).Scan(&currentVersion, &schemaMin, &schemaMax, &collectorStatus); err != nil {
		return CollectorPlanRevision{}, err
	}
	if collectorStatus == "revoked" || collectorStatus == "deleted" {
		return CollectorPlanRevision{}, ErrCollectorPlanInvalidTransition
	}

	planSchemaVersion := request.PlanSchemaVersion
	if request.FromConfigVersion != 0 {
		source, err := getCollectorPlanRevisionTx(ctx, tx, request.TenantID, request.CollectorID, request.FromConfigVersion, false)
		if err != nil {
			return CollectorPlanRevision{}, err
		}
		canonical = append(json.RawMessage(nil), source.SpecJSON...)
		hash = source.SpecHash
		planSchemaVersion = source.PlanSchemaVersion
	}
	if planSchemaVersion < schemaMin || planSchemaVersion > schemaMax {
		return CollectorPlanRevision{}, ErrCollectorPlanInvalidTransition
	}

	var maxVersion uint64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(config_version), 0)
		FROM collector_plan_revisions
		WHERE tenant_id = ? AND collector_id = ?
	`, request.TenantID, request.CollectorID).Scan(&maxVersion); err != nil {
		return CollectorPlanRevision{}, err
	}
	if currentVersion > maxVersion {
		maxVersion = currentVersion
	}
	if maxVersion == ^uint64(0) {
		return CollectorPlanRevision{}, ErrCollectorPlanInvalidTransition
	}
	planID, err := newManagementID()
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	plan := CollectorPlanRevision{
		ID: planID, TenantID: request.TenantID, CollectorID: request.CollectorID,
		ConfigVersion: maxVersion + 1, PlanSchemaVersion: planSchemaVersion,
		Status: CollectorPlanValidated, SpecJSON: canonical, SpecHash: hash,
		NotBefore: request.NotBefore.UTC(), ExpiresAt: request.ExpiresAt.UTC(),
		SupersedesConfigVersion: currentVersion, CreatedBy: request.ActorID, UpdatedBy: request.ActorID,
	}
	registered, err := getCollectorPlanSigningKeyTx(ctx, tx, signer.KeyID(), true)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return CollectorPlanRevision{}, ErrCollectorPlanSigningKeyUnavailable
		}
		return CollectorPlanRevision{}, err
	}
	if registered.Status != CollectorPlanSigningKeyActive || !bytes.Equal(registered.PublicKey, signer.PublicKey()) {
		return CollectorPlanRevision{}, ErrCollectorPlanSigningKeyUnavailable
	}
	plan, err = transactionSigner.signVerified(plan)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	if err := ValidateNewCollectorPlanRevision(plan); err != nil {
		return CollectorPlanRevision{}, err
	}
	if err := insertCollectorPlanRevisionTx(ctx, tx, plan); err != nil {
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

func canonicalCollectorPlanCreateSpec(request CollectorPlanCreateRequest) (json.RawMessage, string, error) {
	if request.FromConfigVersion != 0 {
		return nil, "", nil
	}
	canonical, hash, err := CanonicalCollectorPlanJSON(request.SpecJSON)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrCollectorPlanInvalidRequest, err)
	}
	return canonical, hash, nil
}

func insertCollectorPlanRevisionTx(ctx context.Context, tx *sql.Tx, plan CollectorPlanRevision) error {
	var validation any
	if len(plan.ValidationJSON) != 0 {
		validation = string(plan.ValidationJSON)
	}
	var notBefore any
	if !plan.NotBefore.IsZero() {
		notBefore = plan.NotBefore.UTC()
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO collector_plan_revisions (
			id, tenant_id, collector_id, config_version, plan_schema_version,
			status, spec_json, spec_hash, signing_key_id, signature,
			validation_json, not_before, expires_at, supersedes_config_version,
			created_by, updated_by
		) VALUES (?, ?, ?, ?, ?, 'validated', ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), ?, ?)
	`, plan.ID, plan.TenantID, plan.CollectorID, plan.ConfigVersion, plan.PlanSchemaVersion,
		string(plan.SpecJSON), plan.SpecHash, plan.SigningKeyID, plan.Signature,
		validation, notBefore, plan.ExpiresAt.UTC(), plan.SupersedesConfigVersion,
		plan.CreatedBy, plan.CreatedBy); err != nil {
		return err
	}
	return insertCollectorPlanAudit(ctx, tx, plan.TenantID, plan.CreatedBy, plan.ID, "collector.plan.created", map[string]any{
		"collector_id": plan.CollectorID, "config_version": plan.ConfigVersion,
		"plan_schema_version": plan.PlanSchemaVersion, "spec_hash": plan.SpecHash,
	})
}

func (s *MySQLStore) ListCollectorPlanRevisions(ctx context.Context, tenantID, collectorID ID, filter CollectorPlanPageFilter) ([]CollectorPlanRevision, string, error) {
	if tenantID == "" || collectorID == "" {
		return nil, "", ErrCollectorPlanInvalidRequest
	}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Limit > 200 {
		filter.Limit = 200
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `
		SELECT 1 FROM collector_agents
		WHERE tenant_id = ? AND id = ? AND deleted_at IS NULL
	`, tenantID, collectorID).Scan(&exists); err != nil {
		return nil, "", err
	}
	query := `SELECT ` + collectorPlanSelectColumns + `
		FROM collector_plan_revisions
		WHERE tenant_id = ? AND collector_id = ?`
	args := []any{tenantID, collectorID}
	if filter.BeforeVersion != 0 {
		query += ` AND config_version < ?`
		args = append(args, filter.BeforeVersion)
	}
	query += ` ORDER BY config_version DESC LIMIT ?`
	args = append(args, filter.Limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	items := make([]CollectorPlanRevision, 0, filter.Limit+1)
	for rows.Next() {
		plan, err := scanCollectorPlanRevision(rows)
		if err != nil {
			return nil, "", err
		}
		items = append(items, plan)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(items) > filter.Limit {
		items = items[:filter.Limit]
		next = encodeCollectorPlanCursor(items[len(items)-1].ConfigVersion)
	}
	return items, next, nil
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
	var currentVersion, acknowledgedVersion, lastGoodVersion, heartbeatSequence uint64
	var specHash, currentBootID string
	var expiresAt time.Time
	var status string
	if err := tx.QueryRowContext(ctx, `
		SELECT config_version, acknowledged_config_version, last_good_config_version,
			plan_hash, plan_expires_at, status, boot_id, heartbeat_sequence
		FROM collector_agents
		WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL
		FOR UPDATE
	`, acknowledgement.CollectorID, acknowledgement.TenantID).Scan(
		&currentVersion, &acknowledgedVersion, &lastGoodVersion, &specHash, &expiresAt, &status, &currentBootID, &heartbeatSequence,
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
	if heartbeatSequence > 0 && currentBootID != acknowledgement.BootID {
		return ErrCollectorHeartbeatFenced
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE collector_agents
		SET acknowledged_config_version = ?, last_good_config_version = ?,
			boot_id = ?, software_version = ?,
			observed_health = IF(heartbeat_sequence = 0, 'healthy', observed_health),
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var configVersion, heartbeatSequence uint64
	var status, bootID string
	if err := tx.QueryRowContext(ctx, `
		SELECT config_version, status, boot_id, heartbeat_sequence
		FROM collector_agents
		WHERE id = ? AND tenant_id = ? AND deleted_at IS NULL
		FOR UPDATE
	`, failure.CollectorID, failure.TenantID).Scan(&configVersion, &status, &bootID, &heartbeatSequence); err != nil {
		return err
	}
	if status != "active" || (failure.FailedConfigVersion != 0 && failure.FailedConfigVersion != configVersion) {
		return ErrCollectorPlanInvalidTransition
	}
	if heartbeatSequence > 0 && bootID != failure.BootID {
		return ErrCollectorHeartbeatFenced
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE collector_agents
		SET observed_health = 'degraded',
			boot_id = IF(boot_id = '', ?, boot_id),
			software_version = ?,
			last_seen_at = IF(last_seen_at IS NULL OR last_seen_at < ?, ?, last_seen_at),
			last_error_code = ?, last_error_detail = NULLIF(?, ''),
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND tenant_id = ? AND status = 'active' AND deleted_at IS NULL
	`, failure.BootID, failure.SoftwareVersion, now, now,
		failure.Stage+":"+failure.Code, failure.Detail,
		failure.CollectorID, failure.TenantID)
	if err != nil {
		return err
	}
	if err := requireOneCollectorPlanRow(result); err != nil {
		return ErrCollectorPlanInvalidTransition
	}
	return tx.Commit()
}

func (s *MySQLStore) RecordCollectorRuntimeHeartbeat(ctx context.Context, heartbeat CollectorRuntimeHeartbeat) error {
	if heartbeat.TenantID == "" || heartbeat.CollectorID == "" || heartbeat.ReceivedAt.IsZero() ||
		!validSHA256Hex(heartbeat.CapabilitiesHash) || !validSHA256Hex(heartbeat.ObservationHash) || !validSHA256Hex(heartbeat.PayloadHash) ||
		len(heartbeat.CapabilitiesJSON) == 0 || len(heartbeat.ObservationJSON) == 0 {
		return errors.New("collector runtime heartbeat storage input is incomplete")
	}
	if err := validateCollectorRuntimeHeartbeatReport(heartbeat.CollectorRuntimeHeartbeatReport); err != nil {
		return err
	}
	prepared, err := prepareCollectorRuntimeHeartbeat(CollectorMachineIdentity{
		TenantID: heartbeat.TenantID, CollectorID: heartbeat.CollectorID,
	}, heartbeat.CollectorRuntimeHeartbeatReport, heartbeat.ReceivedAt)
	if err != nil || !heartbeat.ReceivedAt.Equal(prepared.ReceivedAt) || heartbeat.ClockOffsetMilliseconds != prepared.ClockOffsetMilliseconds ||
		heartbeat.CapabilitiesHash != prepared.CapabilitiesHash || string(heartbeat.CapabilitiesJSON) != string(prepared.CapabilitiesJSON) ||
		heartbeat.ObservationHash != prepared.ObservationHash || string(heartbeat.ObservationJSON) != string(prepared.ObservationJSON) ||
		heartbeat.PayloadHash != prepared.PayloadHash {
		return errors.New("collector runtime heartbeat derived facts are inconsistent")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var currentVersion, lastGoodVersion, previousSequence uint64
	var currentPlanSchema uint16
	var currentSpecHash, status, previousBootID, previousPayloadHash, previousObservationHash string
	var previousObservationJSON sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT collector.config_version, collector.last_good_config_version,
			collector.plan_hash, collector.status, collector.boot_id,
			collector.heartbeat_sequence, collector.heartbeat_payload_hash,
			collector.runtime_observation_json, collector.runtime_observation_hash,
			active_plan.plan_schema_version
		FROM collector_agents AS collector
		JOIN collector_plan_revisions AS active_plan
		  ON active_plan.tenant_id = collector.tenant_id
		 AND active_plan.collector_id = collector.id
		 AND active_plan.config_version = collector.config_version
		 AND active_plan.status = 'active'
		WHERE collector.id = ? AND collector.tenant_id = ?
		  AND collector.deleted_at IS NULL
		FOR UPDATE
	`, heartbeat.CollectorID, heartbeat.TenantID).Scan(
		&currentVersion, &lastGoodVersion, &currentSpecHash, &status,
		&previousBootID, &previousSequence, &previousPayloadHash, &previousObservationJSON, &previousObservationHash, &currentPlanSchema,
	); err != nil {
		return err
	}
	if status != "active" || (heartbeat.ActiveConfigVersion != currentVersion && heartbeat.ActiveConfigVersion != lastGoodVersion) {
		return ErrCollectorHeartbeatConflict
	}
	idempotent, err := validateCollectorHeartbeatSequence(previousBootID, previousSequence, previousPayloadHash, heartbeat.BootID, heartbeat.Sequence, heartbeat.PayloadHash)
	if err != nil {
		return err
	}
	if idempotent {
		if _, err := tx.ExecContext(ctx, `
			UPDATE collector_agents
			SET last_seen_at = IF(last_seen_at IS NULL OR last_seen_at < ?, ?, last_seen_at)
			WHERE id = ? AND tenant_id = ? AND status = 'active' AND deleted_at IS NULL
		`, heartbeat.ReceivedAt.UTC(), heartbeat.ReceivedAt.UTC(), heartbeat.CollectorID, heartbeat.TenantID); err != nil {
			return err
		}
		return tx.Commit()
	}
	if previousBootID == heartbeat.BootID && previousObservationJSON.Valid {
		var previousObservation CollectorRuntimeObservation
		if err := json.Unmarshal([]byte(previousObservationJSON.String), &previousObservation); err != nil {
			return fmt.Errorf("decode stored collector runtime observation: %w", err)
		}
		_, storedObservationHash, err := marshalCollectorRuntimeValue(previousObservation)
		if err != nil || storedObservationHash != previousObservationHash {
			return errors.New("stored collector runtime observation integrity check failed")
		}
		if !collectorRuntimeCountersMonotonic(previousObservation.Counters, heartbeat.Observation.Counters) {
			return ErrCollectorHeartbeatFenced
		}
	}
	if heartbeat.ActiveSpecHash != "" {
		expectedSpecHash := currentSpecHash
		if heartbeat.ActiveConfigVersion != currentVersion {
			if err := tx.QueryRowContext(ctx, `
				SELECT spec_hash
				FROM collector_plan_revisions
				WHERE tenant_id = ? AND collector_id = ? AND config_version = ?
				  AND status IN ('active','retired')
			`, heartbeat.TenantID, heartbeat.CollectorID, heartbeat.ActiveConfigVersion).Scan(&expectedSpecHash); err != nil {
				return err
			}
		}
		if heartbeat.ActiveSpecHash != expectedSpecHash {
			return ErrCollectorHeartbeatConflict
		}
	}

	health := collectorRuntimeObservedHealth(heartbeat, currentVersion, currentPlanSchema >= heartbeat.PlanSchemaMin && currentPlanSchema <= heartbeat.PlanSchemaMax)
	sentAt := time.UnixMilli(heartbeat.SentAtUnixMilli).UTC()
	result, err := tx.ExecContext(ctx, `
		UPDATE collector_agents
		SET capabilities_json = ?, capabilities_hash = ?, capability_schema_version = ?,
			software_version = ?, agent_api_version = ?,
			plan_schema_min = ?, plan_schema_max = ?, boot_id = ?,
			runtime_schema_version = ?, heartbeat_sequence = ?,
			heartbeat_sent_at = ?, clock_offset_ms = ?,
			runtime_observation_json = ?, runtime_observation_hash = ?,
			heartbeat_payload_hash = ?,
			last_seen_at = IF(last_seen_at IS NULL OR last_seen_at < ?, ?, last_seen_at),
			observed_health = ?,
			last_error_code = IF(? = 'healthy', NULL, last_error_code),
			last_error_detail = IF(? = 'healthy', NULL, last_error_detail)
		WHERE id = ? AND tenant_id = ? AND status = 'active' AND deleted_at IS NULL
	`, string(heartbeat.CapabilitiesJSON), heartbeat.CapabilitiesHash, heartbeat.Capabilities.SchemaVersion,
		heartbeat.SoftwareVersion, heartbeat.AgentAPIVersion,
		heartbeat.PlanSchemaMin, heartbeat.PlanSchemaMax, heartbeat.BootID,
		heartbeat.SchemaVersion, heartbeat.Sequence,
		sentAt, heartbeat.ClockOffsetMilliseconds,
		string(heartbeat.ObservationJSON), heartbeat.ObservationHash,
		heartbeat.PayloadHash, heartbeat.ReceivedAt.UTC(), heartbeat.ReceivedAt.UTC(), health,
		health, health, heartbeat.CollectorID, heartbeat.TenantID)
	if err != nil {
		return err
	}
	if err := requireOneCollectorPlanRow(result); err != nil {
		return ErrCollectorHeartbeatConflict
	}
	return tx.Commit()
}

func collectorRuntimeObservedHealth(heartbeat CollectorRuntimeHeartbeat, currentConfigVersion uint64, schemaCompatible bool) string {
	observation := heartbeat.Observation
	queueFull := observation.Queues.Kafka.Capacity > 0 && observation.Queues.Kafka.Depth >= observation.Queues.Kafka.Capacity
	if !schemaCompatible || !observation.Running || !observation.PlanAccepting || !observation.ControlPlaneHealthy || !observation.KafkaHealthy ||
		queueFull {
		return "degraded"
	}
	if heartbeat.ActiveConfigVersion != currentConfigVersion {
		return "warming"
	}
	if observation.PlanUsingLKG {
		return "degraded"
	}
	return "healthy"
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
