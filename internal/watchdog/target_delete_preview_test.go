package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type fakeTargetDeletePreviewer struct {
	preview TargetDeletePreview
	err     error
}

func (f *fakeTargetDeletePreviewer) PreviewTargetDelete(context.Context, ID, ID) (TargetDeletePreview, error) {
	return f.preview, f.err
}

func TestTargetDeletePreviewEndpoint(t *testing.T) {
	previewer := &fakeTargetDeletePreviewer{preview: TargetDeletePreview{
		Target: Target{ID: "target-a", TenantID: "tenant-a", Name: "Core"},
		Impacts: []TargetDeleteImpact{
			{ResourceType: "network_device", Behavior: "deleted", Count: 1},
			{ResourceType: "aggregate_graph", Behavior: "detached", Count: 2},
		},
	}}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:                configureNetworkTestAuth,
		Targets:             &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a"}}},
		TargetDeletePreview: previewer,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets/target-a/delete-preview", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var got TargetDeletePreview
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Target.ID != "target-a" || len(got.Impacts) != 2 || got.Impacts[1].Behavior != "detached" {
		t.Fatalf("preview payload = %+v", got)
	}

	previewer.err = sql.ErrNoRows
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets/target-a/delete-preview", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing target status = %d", rec.Code)
	}
}

func TestTargetDeletePreviewEndpointUnavailableWithoutRepository(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    configureNetworkTestAuth,
		Targets: &fakeTargetRepository{targets: []Target{{ID: "target-a", TenantID: "tenant-a"}}},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/targets/target-a/delete-preview", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

// TestMySQLTargetDeletePreviewAndCascade seeds a full dependency fan-out and
// checks the preview counts, then deletes the target and verifies the cascade
// removes the legacy collector projection while registry-owned collectors and
// detached resources survive.
func TestMySQLTargetDeletePreviewAndCascade(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_MYSQL_TEST_DSN to run target delete integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}

	tenant := ID("tenant_target_del_test")
	targetID := ID("target_del_test_01")
	deviceID := ID("device_del_test_01")
	portID, portID2 := ID("port_del_test_01"), ID("port_del_test_02")
	agentID := ID("agent_del_test_01")
	graphID := ID("aggr_del_test_01")
	if _, err := db.ExecContext(ctx, "DELETE FROM tenants WHERE id = ?", tenant); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM tenants WHERE id = ?", tenant)
	}()
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Target Del', 'active')", tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES (?, ?, 'Del Core', 'network', '192.0.2.99', 'pending')
	`, targetID, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO network_devices (id, tenant_id, target_id, sys_name)
		VALUES (?, ?, ?, 'core-sw-del')
	`, deviceID, tenant, targetID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO network_ports (id, tenant_id, device_id, if_index, if_name)
		VALUES (?, ?, ?, 1, 'ge-0/0/1'), (?, ?, ?, 2, 'ge-0/0/2')
	`, portID, tenant, deviceID, portID2, tenant, deviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO aggregate_graphs (id, tenant_id, name) VALUES (?, ?, 'del-graph')
	`, graphID, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO aggregate_graph_ports (aggregate_graph_id, tenant_id, port_id) VALUES (?, ?, ?)
	`, graphID, tenant, portID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO metric_retention_policies (id, tenant_id, target_id, high_precision_days)
		VALUES ('retention_del_test_01', ?, ?, 30)
	`, tenant, targetID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES ('user_del_test', ?, 'del@test.local', 'Del', 'active')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO export_tasks (id, tenant_id, created_by, target_id, period_type, range_start, range_end, aggregation)
		VALUES ('export_del_test_01', ?, 'user_del_test', ?, 'day', NOW(3), NOW(3), 'avg_5m')
	`, tenant, targetID); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	if _, err := store.UpsertAgent(ctx, SNMPAgentConfig{
		ID: agentID, TenantID: tenant, TargetID: targetID, AgentType: AgentTypeSNMP,
		Mode: AgentModePull, TokenHash: "del-test-token", Status: AgentStatusPending,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordAgentRun(ctx, AgentRunReport{
		AgentID: agentID, Status: AgentRunSuccess, Seen: true,
		StartedAt: time.Now().UTC().Add(-time.Second), EndedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	registryID := ID("agent_registry_del_01")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode,
			status, observed_health, auth_type, token_hash, created_by, updated_by
		) VALUES (?, ?, 'flow', 'registry-del-owned', 'flow_collect', 'listen',
			'active', 'unknown', 'token', 'registry-del-token', 'user_registry_admin', 'user_registry_admin')
	`, registryID, tenant); err != nil {
		t.Fatal(err)
	}

	preview, err := store.PreviewTargetDelete(ctx, tenant, targetID)
	if err != nil {
		t.Fatal(err)
	}
	impacts := map[string]TargetDeleteImpact{}
	for _, impact := range preview.Impacts {
		impacts[impact.ResourceType] = impact
	}
	for resourceType, want := range map[string]struct {
		behavior string
		count    int
	}{
		"network_device":          {"deleted", 1},
		"network_port":            {"deleted", 2},
		"aggregate_graph":         {"detached", 1},
		"agent":                   {"deleted", 1},
		"collector_projection":    {"deleted", 1},
		"agent_run_history":       {"deleted", 1},
		"metric_retention_policy": {"deleted", 1},
		"export_task":             {"detached", 1},
	} {
		got, ok := impacts[resourceType]
		if !ok || got.Behavior != want.behavior || got.Count != want.count {
			t.Fatalf("impact %s = %+v, want behavior %s count %d", resourceType, got, want.behavior, want.count)
		}
	}
	if _, ok := impacts["metric_series"]; !ok {
		t.Fatal("preview missing metric_series impact")
	}
	if len(impacts["network_device"].Items) != 1 || impacts["network_device"].Items[0].Name != "core-sw-del" {
		t.Fatalf("device items = %+v", impacts["network_device"].Items)
	}

	if err := store.DeleteTarget(ctx, tenant, targetID); err != nil {
		t.Fatal(err)
	}
	for query, want := range map[string]int{
		"SELECT COUNT(*) FROM targets WHERE id = 'target_del_test_01'":                  0,
		"SELECT COUNT(*) FROM network_devices WHERE id = 'device_del_test_01'":          0,
		"SELECT COUNT(*) FROM network_ports WHERE device_id = 'device_del_test_01'":     0,
		"SELECT COUNT(*) FROM target_agents WHERE id = 'agent_del_test_01'":             0,
		"SELECT COUNT(*) FROM collector_agents WHERE id = 'agent_del_test_01'":          0,
		"SELECT COUNT(*) FROM collector_agents WHERE id = 'agent_registry_del_01'":      1,
		"SELECT COUNT(*) FROM aggregate_graphs WHERE id = 'aggr_del_test_01'":           1,
		"SELECT COUNT(*) FROM aggregate_graph_ports WHERE port_id = 'port_del_test_01'": 0,
		"SELECT COUNT(*) FROM export_tasks WHERE id = 'export_del_test_01'":             1,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if count != want {
			t.Fatalf("%s = %d, want %d", query, count, want)
		}
	}
	var exportTarget sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT target_id FROM export_tasks WHERE id = 'export_del_test_01'").Scan(&exportTarget); err != nil {
		t.Fatal(err)
	}
	if exportTarget.Valid {
		t.Fatalf("export task target_id = %q, want NULL", exportTarget.String)
	}
	if !strings.Contains(impacts["metric_series"].Detail, string(targetID)) {
		t.Fatalf("metric series detail = %q", impacts["metric_series"].Detail)
	}
}
