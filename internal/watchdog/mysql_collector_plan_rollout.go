package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const collectorPlanRolloutSelectColumns = `
	id, tenant_id, module_key, rollout_schema_version, selector_json, selector_hash, spec_json, spec_hash,
	plan_schema_version, strategy_json, strategy_hash, status, expires_at,
	row_version, created_by, updated_by, created_at, updated_at, previewed_at, completed_at`

func (s *MySQLStore) CreateCollectorPlanRollout(ctx context.Context, request CollectorPlanRolloutCreateRequest) (CollectorPlanRollout, error) {
	request, err := normalizeCollectorPlanRolloutCreateRequest(request, time.Now().UTC())
	if err != nil {
		return CollectorPlanRollout{}, err
	}
	selectorJSON, selectorHash, err := canonicalCollectorPlanRolloutValue(request.Selector)
	if err != nil {
		return CollectorPlanRollout{}, ErrCollectorPlanRolloutInvalid
	}
	strategyJSON, strategyHash, err := canonicalCollectorPlanRolloutValue(request.Strategy)
	if err != nil {
		return CollectorPlanRollout{}, ErrCollectorPlanRolloutInvalid
	}
	specJSON, specHash, err := CanonicalCollectorPlanJSON(request.SpecJSON)
	if err != nil {
		return CollectorPlanRollout{}, ErrCollectorPlanRolloutInvalid
	}
	id, err := newIdentityID()
	if err != nil {
		return CollectorPlanRollout{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectorPlanRollout{}, err
	}
	defer tx.Rollback()
	if err := requireCollectorPlanRolloutActor(ctx, tx, request.TenantID, request.ActorID); err != nil {
		return CollectorPlanRollout{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO collector_plan_rollouts (
			id, tenant_id, module_key, rollout_schema_version, selector_json, selector_hash,
			spec_json, spec_hash, plan_schema_version, strategy_json,
			strategy_hash, status, expires_at, created_by, updated_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?)
	`, id, request.TenantID, request.Selector.ModuleKey, CollectorPlanRolloutSchemaVersion, string(selectorJSON), selectorHash,
		string(specJSON), specHash, request.PlanSchemaVersion, string(strategyJSON), strategyHash,
		request.ExpiresAt, request.ActorID, request.ActorID); err != nil {
		return CollectorPlanRollout{}, err
	}
	if err := insertCollectorPlanRolloutAudit(ctx, tx, request.TenantID, request.ActorID, id, "collector.plan_rollout.created", map[string]any{
		"module_key": request.Selector.ModuleKey, "selector_hash": selectorHash,
		"spec_hash": specHash, "strategy_hash": strategyHash,
		"plan_schema_version": request.PlanSchemaVersion, "expires_at": request.ExpiresAt,
	}); err != nil {
		return CollectorPlanRollout{}, err
	}
	created, err := getCollectorPlanRolloutTx(ctx, tx, request.TenantID, id, false)
	if err != nil {
		return CollectorPlanRollout{}, err
	}
	if err := tx.Commit(); err != nil {
		return CollectorPlanRollout{}, err
	}
	return created, nil
}

func (s *MySQLStore) PreviewCollectorPlanRollout(ctx context.Context, request CollectorPlanRolloutPreviewRequest) (CollectorPlanRolloutPreview, error) {
	if !validCollectorEvidenceID(request.TenantID) || !validCollectorEvidenceID(request.RolloutID) || !validCollectorEvidenceID(request.ActorID) || request.ExpectedRowVersion == 0 {
		return CollectorPlanRolloutPreview{}, ErrCollectorPlanRolloutInvalid
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectorPlanRolloutPreview{}, err
	}
	defer tx.Rollback()
	if err := requireCollectorPlanRolloutActor(ctx, tx, request.TenantID, request.ActorID); err != nil {
		return CollectorPlanRolloutPreview{}, err
	}
	rollout, err := getCollectorPlanRolloutTx(ctx, tx, request.TenantID, request.RolloutID, true)
	if err != nil {
		return CollectorPlanRolloutPreview{}, err
	}
	if rollout.RowVersion != request.ExpectedRowVersion {
		return CollectorPlanRolloutPreview{}, ErrCollectorPlanRolloutConflict
	}
	if rollout.Status != CollectorPlanRolloutDraft || !now.Before(rollout.ExpiresAt) {
		return CollectorPlanRolloutPreview{}, ErrCollectorPlanInvalidTransition
	}
	var selector CollectorPlanRolloutSelector
	var strategy CollectorPlanRolloutStrategy
	if err := json.Unmarshal(rollout.SelectorJSON, &selector); err != nil {
		return CollectorPlanRolloutPreview{}, errors.New("stored collector rollout selector is invalid")
	}
	if err := json.Unmarshal(rollout.StrategyJSON, &strategy); err != nil {
		return CollectorPlanRolloutPreview{}, errors.New("stored collector rollout strategy is invalid")
	}
	result, err := insertCollectorPlanRolloutTargets(ctx, tx, rollout, selector, strategy)
	if err != nil {
		return CollectorPlanRolloutPreview{}, err
	}
	matched, err := result.RowsAffected()
	if err != nil {
		return CollectorPlanRolloutPreview{}, err
	}
	if matched == 0 {
		return CollectorPlanRolloutPreview{}, ErrCollectorPlanRolloutEmpty
	}
	var eligible, skipped uint64
	var waveCount uint32
	if err := tx.QueryRowContext(ctx, `
		SELECT SUM(status = 'pending'), SUM(status = 'skipped'),
			COUNT(DISTINCT CASE WHEN status = 'pending' THEN wave END)
		FROM collector_plan_rollout_targets
		WHERE tenant_id = ? AND rollout_id = ?
	`, rollout.TenantID, rollout.ID).Scan(&eligible, &skipped, &waveCount); err != nil {
		return CollectorPlanRolloutPreview{}, err
	}
	update, err := tx.ExecContext(ctx, `
		UPDATE collector_plan_rollouts
		SET status = 'previewed', previewed_at = ?, updated_by = ?,
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND status = 'draft' AND row_version = ?
	`, now, request.ActorID, rollout.TenantID, rollout.ID, request.ExpectedRowVersion)
	if err != nil {
		return CollectorPlanRolloutPreview{}, err
	}
	if err := requireOneCollectorPlanRow(update); err != nil {
		return CollectorPlanRolloutPreview{}, ErrCollectorPlanRolloutConflict
	}
	if err := insertCollectorPlanRolloutAudit(ctx, tx, rollout.TenantID, request.ActorID, rollout.ID, "collector.plan_rollout.previewed", map[string]any{
		"matched_count": matched, "eligible_count": eligible, "skipped_count": skipped,
		"wave_count": waveCount, "selector_hash": rollout.SelectorHash,
	}); err != nil {
		return CollectorPlanRolloutPreview{}, err
	}
	rollout, err = getCollectorPlanRolloutTx(ctx, tx, rollout.TenantID, rollout.ID, false)
	if err != nil {
		return CollectorPlanRolloutPreview{}, err
	}
	if err := tx.Commit(); err != nil {
		return CollectorPlanRolloutPreview{}, err
	}
	return CollectorPlanRolloutPreview{
		Rollout: rollout, MatchedCount: uint64(matched), EligibleCount: eligible,
		SkippedCount: skipped, WaveCount: waveCount,
	}, nil
}

func insertCollectorPlanRolloutTargets(ctx context.Context, tx *sql.Tx, rollout CollectorPlanRollout, selector CollectorPlanRolloutSelector, strategy CollectorPlanRolloutStrategy) (sql.Result, error) {
	selectedJoin := ""
	args := []any{
		rollout.TenantID, rollout.ID,
		rollout.PlanSchemaVersion, CollectorPlanRolloutSkippedWave,
		strategy.CanaryCount, strategy.CanaryCount, strategy.WaveSize,
		rollout.PlanSchemaVersion, rollout.PlanSchemaVersion,
		rollout.PlanSchemaVersion, rollout.PlanSchemaVersion,
	}
	if len(selector.CollectorIDs) != 0 {
		collectorIDs, err := json.Marshal(selector.CollectorIDs)
		if err != nil {
			return nil, err
		}
		selectedJoin = ` JOIN JSON_TABLE(?, '$[*]' COLUMNS (
			selected_id VARCHAR(26) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci PATH '$'
		)) AS selected ON selected.selected_id = collector.id`
		args = append(args, string(collectorIDs))
	}
	args = append(args, rollout.TenantID, selector.ModuleKey, selector.Status, selector.AgentType, selector.AgentType)
	return tx.ExecContext(ctx, `
		INSERT INTO collector_plan_rollout_targets (
			tenant_id, rollout_id, collector_id, wave, config_version,
			prior_config_version, status, failure_reason
		)
		SELECT ?, ?, ranked.id,
			CASE
				WHEN NOT (? BETWEEN ranked.plan_schema_min AND ranked.plan_schema_max) THEN ?
				WHEN ranked.eligible_ordinal <= ? THEN 0
				ELSE 1 + FLOOR((ranked.eligible_ordinal - ? - 1) / ?)
			END,
			NULL, ranked.config_version,
			CASE WHEN ? BETWEEN ranked.plan_schema_min AND ranked.plan_schema_max
				THEN 'pending' ELSE 'skipped' END,
			CASE WHEN ? BETWEEN ranked.plan_schema_min AND ranked.plan_schema_max
				THEN NULL ELSE CONCAT('plan_schema_version ', ?,
					' outside collector range ', ranked.plan_schema_min, '-', ranked.plan_schema_max) END
		FROM (
			SELECT collector.id, collector.config_version,
				collector.plan_schema_min, collector.plan_schema_max,
				SUM(CASE WHEN ? BETWEEN collector.plan_schema_min AND collector.plan_schema_max
					THEN 1 ELSE 0 END) OVER (ORDER BY collector.id) AS eligible_ordinal
			FROM collector_agents AS collector`+selectedJoin+`
			WHERE collector.tenant_id = ? AND collector.module_key = ?
				AND collector.status = ? AND collector.deleted_at IS NULL
				AND (? = '' OR collector.agent_type = ?)
		) AS ranked
	`, args...)
}

func requireCollectorPlanRolloutActor(ctx context.Context, tx *sql.Tx, tenantID, actorID ID) error {
	var exists int
	return tx.QueryRowContext(ctx, `
		SELECT 1 FROM users WHERE tenant_id = ? AND id = ? AND status = 'active'
	`, tenantID, actorID).Scan(&exists)
}

func getCollectorPlanRolloutTx(ctx context.Context, tx *sql.Tx, tenantID, rolloutID ID, lock bool) (CollectorPlanRollout, error) {
	query := `SELECT ` + collectorPlanRolloutSelectColumns + `
		FROM collector_plan_rollouts WHERE tenant_id = ? AND id = ?`
	if lock {
		query += ` FOR UPDATE`
	}
	return scanCollectorPlanRollout(tx.QueryRowContext(ctx, query, tenantID, rolloutID))
}

func scanCollectorPlanRollout(row collectorPlanRow) (CollectorPlanRollout, error) {
	var rollout CollectorPlanRollout
	var selectorJSON, specJSON, strategyJSON []byte
	var status string
	var previewedAt, completedAt sql.NullTime
	if err := row.Scan(
		&rollout.ID, &rollout.TenantID, &rollout.ModuleKey, &rollout.RolloutSchemaVersion,
		&selectorJSON, &rollout.SelectorHash, &specJSON, &rollout.SpecHash,
		&rollout.PlanSchemaVersion, &strategyJSON, &rollout.StrategyHash,
		&status, &rollout.ExpiresAt, &rollout.RowVersion,
		&rollout.CreatedBy, &rollout.UpdatedBy, &rollout.CreatedAt,
		&rollout.UpdatedAt, &previewedAt, &completedAt,
	); err != nil {
		return CollectorPlanRollout{}, err
	}
	rollout.Status = CollectorPlanRolloutStatus(status)
	rollout.SelectorJSON = append(json.RawMessage(nil), selectorJSON...)
	rollout.SpecJSON = append(json.RawMessage(nil), specJSON...)
	rollout.StrategyJSON = append(json.RawMessage(nil), strategyJSON...)
	rollout.PreviewedAt = previewedAt.Time
	rollout.CompletedAt = completedAt.Time
	if err := validateStoredCollectorPlanRollout(&rollout); err != nil {
		return CollectorPlanRollout{}, err
	}
	return rollout, nil
}

func validateStoredCollectorPlanRollout(rollout *CollectorPlanRollout) error {
	if rollout == nil {
		return errors.New("stored collector plan rollout is invalid")
	}
	if !validCollectorEvidenceID(rollout.ID) || !validCollectorEvidenceID(rollout.TenantID) || !validCollectorEvidenceID(rollout.CreatedBy) || !validCollectorEvidenceID(rollout.UpdatedBy) || rollout.ModuleKey == "" || rollout.RolloutSchemaVersion != CollectorPlanRolloutSchemaVersion || rollout.PlanSchemaVersion == 0 || rollout.RowVersion == 0 || rollout.ExpiresAt.IsZero() || rollout.CreatedAt.IsZero() || rollout.UpdatedAt.Before(rollout.CreatedAt) {
		return errors.New("stored collector plan rollout is invalid")
	}
	var selector CollectorPlanRolloutSelector
	if err := json.Unmarshal(rollout.SelectorJSON, &selector); err != nil {
		return errors.New("stored collector plan rollout selector is invalid")
	}
	normalizedSelector, err := normalizeCollectorPlanRolloutSelector(selector)
	if err != nil {
		return errors.New("stored collector plan rollout selector is invalid")
	}
	canonicalSelector, selectorHash, err := canonicalCollectorPlanRolloutValue(normalizedSelector)
	if err != nil || selectorHash != rollout.SelectorHash || rollout.ModuleKey != normalizedSelector.ModuleKey {
		return errors.New("stored collector plan rollout selector integrity check failed")
	}
	rollout.SelectorJSON = canonicalSelector
	canonicalSpec, specHash, err := CanonicalCollectorPlanJSON(rollout.SpecJSON)
	if err != nil || specHash != rollout.SpecHash {
		return errors.New("stored collector plan rollout spec integrity check failed")
	}
	rollout.SpecJSON = canonicalSpec
	var strategy CollectorPlanRolloutStrategy
	if err := json.Unmarshal(rollout.StrategyJSON, &strategy); err != nil || validateCollectorPlanRolloutStrategy(strategy) != nil {
		return errors.New("stored collector plan rollout strategy is invalid")
	}
	canonicalStrategy, strategyHash, err := canonicalCollectorPlanRolloutValue(strategy)
	if err != nil || strategyHash != rollout.StrategyHash {
		return errors.New("stored collector plan rollout strategy integrity check failed")
	}
	rollout.StrategyJSON = canonicalStrategy
	switch rollout.Status {
	case CollectorPlanRolloutDraft:
		if !rollout.PreviewedAt.IsZero() || !rollout.CompletedAt.IsZero() {
			return errors.New("stored collector plan rollout lifecycle is invalid")
		}
	case CollectorPlanRolloutPreviewed:
		if rollout.PreviewedAt.IsZero() || !rollout.CompletedAt.IsZero() {
			return errors.New("stored collector plan rollout lifecycle is invalid")
		}
	case "canarying", "rolling":
		if rollout.PreviewedAt.IsZero() || !rollout.CompletedAt.IsZero() {
			return errors.New("stored collector plan rollout lifecycle is invalid")
		}
	case "paused":
		if !rollout.CompletedAt.IsZero() {
			return errors.New("stored collector plan rollout lifecycle is invalid")
		}
	case "completed", "rolled_back", "killed":
		if rollout.CompletedAt.IsZero() {
			return errors.New("stored collector plan rollout lifecycle is invalid")
		}
	default:
		return errors.New("stored collector plan rollout status is unsupported")
	}
	return nil
}

func insertCollectorPlanRolloutAudit(ctx context.Context, tx *sql.Tx, tenantID, actorID, rolloutID ID, action string, detail map[string]any) error {
	payload, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	createdAt := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO audit_logs (
			id, tenant_id, actor_id, action, resource_type, resource_id,
			detail_json, created_at
		) VALUES (?, ?, ?, ?, 'collector_plan_rollout', ?, ?, ?)
	`, stableID("audit", string(rolloutID), action, createdAt.Format(time.RFC3339Nano)),
		tenantID, actorID, action, rolloutID, payload, createdAt)
	if err != nil {
		return fmt.Errorf("insert collector plan rollout audit: %w", err)
	}
	return nil
}

var _ CollectorPlanRolloutRepository = (*MySQLStore)(nil)
