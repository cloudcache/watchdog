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

	// PLAT-03B1: the registry is the sole credential write authority. Rotate
	// the collector credential registry-side, then prove legacy upsert /
	// heartbeat / run sync leave credentials and the management row_version
	// untouched while target_agents keeps its own legacy token.
	if _, err := db.ExecContext(ctx, `
		UPDATE collector_agents
		SET auth_type = 'mtls', token_hash = NULL, certificate_fingerprint = 'rotated-fp',
			row_version = row_version + 1, updated_by = 'user_registry_admin'
		WHERE id = ?
	`, agentID); err != nil {
		t.Fatal(err)
	}
	var rowVersionBefore uint64
	if err := db.QueryRowContext(ctx, `SELECT row_version FROM collector_agents WHERE id = ?`, agentID).Scan(&rowVersionBefore); err != nil {
		t.Fatal(err)
	}
	agent.TokenHash = "legacy-rotated-token-hash"
	if _, err := store.UpsertAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkAgentSeen(ctx, agentID); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordAgentRun(ctx, AgentRunReport{
		AgentID: agentID, Status: AgentRunSuccess, Seen: true,
		StartedAt: time.Now().UTC().Add(-time.Second), EndedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	var authType, certFP string
	var registryTokenHash sql.NullString
	var rowVersionAfter uint64
	if err := db.QueryRowContext(ctx, `
		SELECT auth_type, token_hash, COALESCE(certificate_fingerprint, ''), row_version
		FROM collector_agents WHERE id = ?
	`, agentID).Scan(&authType, &registryTokenHash, &certFP, &rowVersionAfter); err != nil {
		t.Fatal(err)
	}
	if authType != "mtls" || registryTokenHash.Valid || certFP != "rotated-fp" {
		t.Fatalf("legacy writes overwrote registry credentials: auth=%q token=%v fp=%q", authType, registryTokenHash.String, certFP)
	}
	if rowVersionAfter != rowVersionBefore {
		t.Fatalf("legacy writes moved management row_version: %d -> %d", rowVersionBefore, rowVersionAfter)
	}
	var legacyTokenHash string
	if err := db.QueryRowContext(ctx, `SELECT token_hash FROM target_agents WHERE id = ?`, agentID).Scan(&legacyTokenHash); err != nil {
		t.Fatal(err)
	}
	if legacyTokenHash != "legacy-rotated-token-hash" {
		t.Fatalf("legacy auth token hash = %q, want the rotated legacy value", legacyTokenHash)
	}

	// A registry-created collector must be invisible to legacy writes: upsert
	// under the same id is rejected (and rolls back its target_agents write),
	// heartbeat sync skips it, and legacy delete leaves the row in place.
	registryID := ID("agent_registry_owned_01")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode,
			status, observed_health, auth_type, token_hash, created_by, updated_by
		) VALUES (?, ?, 'flow', 'registry-owned', 'flow_collect', 'listen',
			'active', 'unknown', 'token', 'registry-token-hash', 'user_registry_admin', 'user_registry_admin')
	`, registryID, tenantA); err != nil {
		t.Fatal(err)
	}
	registryAgent := agent
	registryAgent.ID = registryID
	registryAgent.TokenHash = "legacy-takeover-hash"
	if _, err := store.UpsertAgent(ctx, registryAgent); err == nil || !strings.Contains(err.Error(), "collector registry") {
		t.Fatalf("legacy takeover of registry-owned collector error = %v", err)
	}
	var legacyRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM target_agents WHERE id = ?`, registryID).Scan(&legacyRows); err != nil || legacyRows != 0 {
		t.Fatalf("rejected takeover left target_agents rows = %d, err = %v", legacyRows, err)
	}
	if err := store.MarkAgentSeen(ctx, registryID); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteAgent(ctx, tenantA, registryID); err != nil {
		t.Fatal(err)
	}
	var registryHealth, registryToken string
	if err := db.QueryRowContext(ctx, `
		SELECT observed_health, COALESCE(token_hash, '') FROM collector_agents WHERE id = ?
	`, registryID).Scan(&registryHealth, &registryToken); err != nil {
		t.Fatalf("registry-owned collector row missing after legacy delete: %v", err)
	}
	if registryHealth != "unknown" || registryToken != "registry-token-hash" {
		t.Fatalf("legacy sync touched registry-owned collector: health=%q token=%q", registryHealth, registryToken)
	}

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
