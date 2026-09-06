package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestMySQLCollectorPlanRolloutCreatePreviewAndRollback(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_PLAN_ROLLOUT_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_PLAN_ROLLOUT_MYSQL_TEST_DSN to an isolated database")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := ApplyMySQLMigrations(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"collector_plan_rollouts", "collector_plan_rollout_targets"} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s is not empty; rollout test requires an isolated database", table)
		}
	}

	ctx := context.Background()
	tenantID := ID("tenant_rollout_test_0001")
	userID := ID("user_rollout_test_000001")
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Rollout Test', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM collector_plan_rollouts WHERE tenant_id = ?", tenantID)
		_, _ = db.ExecContext(context.Background(), "DELETE FROM audit_logs WHERE tenant_id = ?", tenantID)
		_, _ = db.ExecContext(context.Background(), "DELETE FROM tenants WHERE id = ?", tenantID)
	}()
	if _, err := db.ExecContext(ctx, "INSERT INTO users (id, tenant_id, email, name, status) VALUES (?, ?, 'rollout@watchdog.local', 'Rollout Test', 'active')", userID, tenantID); err != nil {
		t.Fatal(err)
	}

	collectorIDs := []ID{
		"collector-rollout-0001", "collector-rollout-0002", "collector-rollout-0003",
		"collector-rollout-0004", "collector-rollout-0005", "collector-rollout-0006",
		"collector-rollout-snmp", "collector-rollout-wrong", "collector-rollout-pend",
	}
	for index, collectorID := range collectorIDs {
		moduleKey, agentType, status := "flow", "flow-collect", "active"
		minSchema, maxSchema := 1, 2
		switch collectorID {
		case "collector-rollout-0006":
			minSchema, maxSchema = 2, 3
		case "collector-rollout-snmp":
			moduleKey = "snmp"
		case "collector-rollout-wrong":
			agentType = "other-flow-agent"
		case "collector-rollout-pend":
			status = "pending"
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO collector_agents (
				id, tenant_id, module_key, name, agent_type, mode, status,
				observed_health, auth_type, token_hash, plan_schema_min,
				plan_schema_max, created_by, updated_by
			) VALUES (?, ?, ?, ?, ?, 'listen', ?, 'healthy', 'token',
				'test-token-hash', ?, ?, ?, ?)
		`, collectorID, tenantID, moduleKey, "rollout-test-"+string(rune('a'+index)), agentType,
			status, minSchema, maxSchema, userID, userID); err != nil {
			t.Fatalf("insert collector %s: %v", collectorID, err)
		}
	}

	service, err := NewCollectorPlanRolloutService(NewMySQLStore(db))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	base := CollectorPlanRolloutCreateRequest{
		TenantID: tenantID, ActorID: userID,
		Selector:          CollectorPlanRolloutSelector{ModuleKey: "flow", AgentType: "flow-collect"},
		SpecJSON:          []byte(`{"schema_version":1,"kafka":{"topic":"flows"}}`),
		PlanSchemaVersion: 1,
		Strategy: CollectorPlanRolloutStrategy{
			CanaryCount: 2, WaveSize: 2, MinSoakSeconds: 600, FailureBudget: 1,
		},
		ExpiresAt: now.Add(time.Hour),
	}
	created, err := service.CreateCollectorPlanRollout(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != CollectorPlanRolloutDraft || created.RowVersion != 1 || created.ModuleKey != "flow" || !validSHA256Hex(created.SelectorHash) || !validSHA256Hex(created.SpecHash) || !validSHA256Hex(created.StrategyHash) {
		t.Fatalf("created=%+v", created)
	}
	if string(created.SelectorJSON) != `{"agent_type":"flow-collect","module_key":"flow","status":"active"}` || string(created.SpecJSON) != `{"kafka":{"topic":"flows"},"schema_version":1}` || string(created.StrategyJSON) != `{"canary_count":2,"failure_budget":1,"min_soak_seconds":600,"wave_size":2}` {
		t.Fatalf("stored JSON was not canonicalized on read: selector=%s spec=%s strategy=%s", created.SelectorJSON, created.SpecJSON, created.StrategyJSON)
	}
	preview, err := service.PreviewCollectorPlanRollout(ctx, CollectorPlanRolloutPreviewRequest{
		TenantID: tenantID, RolloutID: created.ID, ActorID: userID, ExpectedRowVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if preview.Rollout.Status != CollectorPlanRolloutPreviewed || preview.Rollout.RowVersion != 2 || preview.Rollout.PreviewedAt.IsZero() || preview.MatchedCount != 6 || preview.EligibleCount != 5 || preview.SkippedCount != 1 || preview.WaveCount != 3 {
		t.Fatalf("preview=%+v", preview)
	}
	assertCollectorPlanRolloutTargets(t, db, created.ID, map[ID]uint32{
		"collector-rollout-0001": 0, "collector-rollout-0002": 0,
		"collector-rollout-0003": 1, "collector-rollout-0004": 1,
		"collector-rollout-0005": 2, "collector-rollout-0006": CollectorPlanRolloutSkippedWave,
	})
	if _, err := service.PreviewCollectorPlanRollout(ctx, CollectorPlanRolloutPreviewRequest{
		TenantID: tenantID, RolloutID: created.ID, ActorID: userID, ExpectedRowVersion: 1,
	}); !errors.Is(err, ErrCollectorPlanRolloutConflict) {
		t.Fatalf("stale preview error=%v", err)
	}

	explicit := base
	explicit.Selector.CollectorIDs = []string{"collector-rollout-0001", "collector-rollout-snmp"}
	explicitRollout, err := service.CreateCollectorPlanRollout(ctx, explicit)
	if err != nil {
		t.Fatal(err)
	}
	explicitPreview, err := service.PreviewCollectorPlanRollout(ctx, CollectorPlanRolloutPreviewRequest{
		TenantID: tenantID, RolloutID: explicitRollout.ID, ActorID: userID, ExpectedRowVersion: 1,
	})
	if err != nil || explicitPreview.MatchedCount != 1 || explicitPreview.EligibleCount != 1 {
		t.Fatalf("explicit preview=%+v err=%v", explicitPreview, err)
	}

	empty := base
	empty.Selector.ModuleKey = "missing-module"
	emptyRollout, err := service.CreateCollectorPlanRollout(ctx, empty)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.PreviewCollectorPlanRollout(ctx, CollectorPlanRolloutPreviewRequest{
		TenantID: tenantID, RolloutID: emptyRollout.ID, ActorID: userID, ExpectedRowVersion: 1,
	}); !errors.Is(err, ErrCollectorPlanRolloutEmpty) {
		t.Fatalf("empty preview error=%v", err)
	}
	var status string
	var rowVersion, targetCount uint64
	if err := db.QueryRowContext(ctx, "SELECT status, row_version FROM collector_plan_rollouts WHERE id = ?", emptyRollout.ID).Scan(&status, &rowVersion); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collector_plan_rollout_targets WHERE rollout_id = ?", emptyRollout.ID).Scan(&targetCount); err != nil {
		t.Fatal(err)
	}
	if status != "draft" || rowVersion != 1 || targetCount != 0 {
		t.Fatalf("empty preview persisted status=%s row_version=%d targets=%d", status, rowVersion, targetCount)
	}
	var revisionCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collector_plan_revisions WHERE tenant_id = ?", tenantID).Scan(&revisionCount); err != nil || revisionCount != 0 {
		t.Fatalf("plan revisions=%d err=%v", revisionCount, err)
	}
	var changedHeads int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collector_agents WHERE tenant_id = ? AND config_version <> 0", tenantID).Scan(&changedHeads); err != nil || changedHeads != 0 {
		t.Fatalf("changed collector heads=%d err=%v", changedHeads, err)
	}
	var auditCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit_logs WHERE tenant_id = ? AND resource_type = 'collector_plan_rollout'", tenantID).Scan(&auditCount); err != nil || auditCount != 5 {
		t.Fatalf("audit count=%d err=%v", auditCount, err)
	}
}

func assertCollectorPlanRolloutTargets(t *testing.T, db *sql.DB, rolloutID ID, want map[ID]uint32) {
	t.Helper()
	rows, err := db.Query(`
		SELECT collector_id, wave, status, failure_reason, config_version, prior_config_version
		FROM collector_plan_rollout_targets WHERE rollout_id = ? ORDER BY collector_id
	`, rolloutID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[ID]uint32, len(want))
	for rows.Next() {
		var collectorID ID
		var wave uint32
		var status string
		var reason sql.NullString
		var configVersion sql.NullInt64
		var priorConfigVersion uint64
		if err := rows.Scan(&collectorID, &wave, &status, &reason, &configVersion, &priorConfigVersion); err != nil {
			t.Fatal(err)
		}
		got[collectorID] = wave
		if configVersion.Valid || priorConfigVersion != 0 {
			t.Fatalf("collector=%s config=%+v prior=%d", collectorID, configVersion, priorConfigVersion)
		}
		if wave == CollectorPlanRolloutSkippedWave {
			if status != "skipped" || !reason.Valid || reason.String == "" {
				t.Fatalf("skipped collector=%s status=%s reason=%+v", collectorID, status, reason)
			}
		} else if status != "pending" || reason.Valid {
			t.Fatalf("eligible collector=%s status=%s reason=%+v", collectorID, status, reason)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("targets=%v want=%v", got, want)
	}
}
