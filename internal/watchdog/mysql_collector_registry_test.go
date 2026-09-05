package watchdog

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCollectorRegistryMigrationHasCompatibilityAndPlanSafetyContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "018_collector_registry_expand.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, fragment := range []string{
		"create table if not exists collector_agents",
		"create table if not exists collector_bindings",
		"create table if not exists collector_plan_revisions",
		"unique key uq_collector_name",
		"unique key uq_collector_active_plan",
		"check (last_good_config_version <= acknowledged_config_version)",
		"check (acknowledged_config_version <= config_version)",
		"insert into collector_agents",
		"from target_agents as legacy",
		"insert into collector_bindings",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Fatalf("collector registry migration missing %q", fragment)
		}
	}
}

func TestCollectorOwnershipEvidenceMigrationHasNormalizedSafetyContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "019_collector_ownership_evidence.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, fragment := range []string{
		"create table if not exists collector_service_principals",
		"unique key uq_collector_service_principal_ref",
		"create table if not exists collector_ownership_transfers",
		"old_revoke_plan_revision bigint unsigned not null",
		"unique key uq_collector_ownership_transfer_epoch",
		"create table if not exists collector_state_restore_receipts",
		"primary key (transfer_id, state_kind, state_identity_key)",
		"state_identity_key binary(32)",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Fatalf("collector ownership evidence migration missing %q", fragment)
		}
	}
}

func TestCollectorPrincipalOperationMigrationHasCrashRecoveryContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "020_collector_principal_operations.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, fragment := range []string{
		"grant_operation_key char(64)",
		"grant_request_hash char(64)",
		"revoke_operation_key char(64)",
		"watchdog:imported:grant:",
		"watchdog:imported:request:",
		"watchdog:imported:revoke:",
		"unique key uq_collector_principal_grant_operation",
		"unique key uq_collector_principal_revoke_operation",
		"check ((status = 'revoked') = (revoke_operation_key is not null))",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Fatalf("collector principal operation migration missing %q", fragment)
		}
	}
}

func TestCollectorRuntimeHeartbeatMigrationHasObservedStateContract(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "migration", "mysql", "021_collector_runtime_heartbeat.sql")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sqlText := strings.ToLower(string(data))
	for _, fragment := range []string{
		"runtime_schema_version smallint unsigned",
		"heartbeat_sequence bigint unsigned",
		"heartbeat_sent_at datetime(3)",
		"clock_offset_ms bigint",
		"runtime_observation_json json",
		"runtime_observation_hash char(64)",
		"heartbeat_payload_hash char(64)",
		"check (",
	} {
		if !strings.Contains(sqlText, fragment) {
			t.Fatalf("collector runtime heartbeat migration missing %q", fragment)
		}
	}
	if strings.Contains(sqlText, "row_version = row_version") || strings.Contains(sqlText, "acknowledged_config_version =") {
		t.Fatal("runtime heartbeat migration must not advance management or plan acknowledgement state")
	}
}

func TestLegacyCollectorProjectionMappings(t *testing.T) {
	tests := []struct {
		legacyStatus string
		desired      string
		health       string
	}{
		{AgentStatusPending, "pending", "unknown"},
		{AgentStatusUp, "active", "healthy"},
		{AgentStatusDown, "active", "unavailable"},
		{AgentStatusError, "active", "degraded"},
		{AgentStatusDisabled, "suspended", "unknown"},
	}
	for _, test := range tests {
		if got := legacyCollectorDesiredStatus(test.legacyStatus); got != test.desired {
			t.Errorf("desired status for %q = %q, want %q", test.legacyStatus, got, test.desired)
		}
		if got := legacyCollectorObservedHealth(test.legacyStatus); got != test.health {
			t.Errorf("observed health for %q = %q, want %q", test.legacyStatus, got, test.health)
		}
	}
	if got := legacyCollectorModuleKey(AgentTypeSNMP); got != "network" {
		t.Fatalf("SNMP module key = %q", got)
	}
	if got := legacyCollectorModuleKey(AgentTypeSystem); got != "watchdog" {
		t.Fatalf("system module key = %q", got)
	}
}

func TestMySQLLegacyAgentCollectorProjectionLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_MYSQL_TEST_DSN to run collector registry integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := ApplyMySQLMigrations(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	tenantA, tenantB := ID("tenant_collector_test_a"), ID("tenant_collector_test_b")
	targetA, targetA2 := ID("target_collector_test_a"), ID("target_collector_test_a2")
	targetB := ID("target_collector_test_b")
	agentID := ID("agent_collector_test_01")
	for _, tenant := range []ID{tenantA, tenantB} {
		if _, err := db.ExecContext(ctx, "DELETE FROM tenants WHERE id = ?", tenant); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM tenants WHERE id IN (?, ?)", tenantA, tenantB)
	}()
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Collector A', 'active'), (?, 'Collector B', 'active')", tenantA, tenantB); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES (?, ?, 'Collector Target A', 'network', '192.0.2.10', 'pending'),
		       (?, ?, 'Collector Target A2', 'network', '192.0.2.12', 'pending'),
		       (?, ?, 'Collector Target B', 'network', '192.0.2.11', 'pending')
	`, targetA, tenantA, targetA2, tenantA, targetB, tenantB); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	agent := SNMPAgentConfig{
		ID: agentID, TenantID: tenantA, TargetID: targetA, AgentType: AgentTypeSNMP,
		Mode: AgentModePull, TokenHash: "collector-test-token-hash", Status: AgentStatusPending,
	}
	if _, err := store.UpsertAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	assertCollectorProjection(t, db, agentID, tenantA, targetA, "pending", "unknown", 1)
	agent.TargetID = targetA2
	agent.Status = AgentStatusDisabled
	if _, err := store.UpsertAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	assertCollectorProjection(t, db, agentID, tenantA, targetA2, "suspended", "unknown", 1)

	otherTenant := agent
	otherTenant.TenantID = tenantB
	otherTenant.TargetID = targetB
	if _, err := store.UpsertAgent(ctx, otherTenant); err == nil || !strings.Contains(err.Error(), "another tenant") {
		t.Fatalf("cross-tenant agent takeover error = %v", err)
	}
	assertCollectorProjection(t, db, agentID, tenantA, targetA2, "suspended", "unknown", 1)

	if err := store.RecordAgentRun(ctx, AgentRunReport{
		AgentID: agentID, Status: AgentRunFailure, Seen: true, Error: "injected failure",
		StartedAt: time.Now().UTC().Add(-time.Second), EndedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	assertCollectorProjection(t, db, agentID, tenantA, targetA2, "suspended", "degraded", 1)

	if err := store.MarkAgentSeen(ctx, agentID); err != nil {
		t.Fatal(err)
	}
	assertCollectorProjection(t, db, agentID, tenantA, targetA2, "suspended", "healthy", 1)

	if err := store.DeleteAgent(ctx, tenantA, agentID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collector_agents WHERE id = ?", agentID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("collector count after delete = %d, err = %v", count, err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM target_agents WHERE id = ?", agentID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("legacy agent count after delete = %d, err = %v", count, err)
	}
}

func assertCollectorProjection(t *testing.T, db *sql.DB, agentID, tenantID, targetID ID, desired, health string, bindingCount int) {
	t.Helper()
	var gotTenant ID
	var gotDesired, gotHealth string
	if err := db.QueryRow(`
		SELECT tenant_id, status, observed_health
		FROM collector_agents WHERE id = ?
	`, agentID).Scan(&gotTenant, &gotDesired, &gotHealth); err != nil {
		t.Fatal(err)
	}
	if gotTenant != tenantID || gotDesired != desired || gotHealth != health {
		t.Fatalf("collector projection = tenant %q desired %q health %q", gotTenant, gotDesired, gotHealth)
	}
	var gotBindings int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM collector_bindings
		WHERE collector_id = ? AND tenant_id = ? AND resource_type = 'target'
	`, agentID, tenantID).Scan(&gotBindings); err != nil {
		t.Fatal(err)
	}
	if gotBindings != bindingCount {
		t.Fatalf("collector binding count = %d, want %d", gotBindings, bindingCount)
	}
	var selectedBinding int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM collector_bindings
		WHERE collector_id = ? AND tenant_id = ? AND resource_type = 'target' AND resource_id = ?
	`, agentID, tenantID, targetID).Scan(&selectedBinding); err != nil {
		t.Fatal(err)
	}
	if selectedBinding != 1 {
		t.Fatalf("collector binding for target %q = %d, want 1", targetID, selectedBinding)
	}
}

func TestLegacyCollectorProjectionRequiresTransaction(t *testing.T) {
	err := upsertCollectorCompatibilityProjection(context.Background(), nil, SNMPAgentConfig{})
	if err == nil || err.Error() != "collector compatibility projection transaction is required" {
		t.Fatalf("nil transaction error = %v", err)
	}
}
