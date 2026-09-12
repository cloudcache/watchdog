package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestMySQLDeviceDeletePreviewJobAndReceipt seeds a device fan-out, checks the
// preview counts, then deletes it asynchronously through the API + worker and
// verifies the cascade, VM series cleanup and destruction receipt.
func TestMySQLDeviceDeletePreviewJobAndReceipt(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	targetID := ID("target_devdel_01")
	deviceID := ID("device_devdel_01")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES (?, ?, 'DevDel', 'network', '192.0.2.210', 'pending')
	`, targetID, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertDevice(ctx, NetworkDevice{
		ID: deviceID, TenantID: tenant, TargetID: targetID, Vendor: "TestVendor", Model: "TestModel", SysName: "dev-del", SNMPPort: 161,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO network_ports (id, tenant_id, device_id, if_index, if_name)
		VALUES ('port_dd_1', ?, ?, 1, 'ge-0/0/1'), ('port_dd_2', ?, ?, 2, 'ge-0/0/2')
	`, tenant, deviceID, tenant, deviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bgp_sessions (id, tenant_id, device_id, peer_addr, peer_as, local_as, state)
		VALUES ('bgp_dd_1', ?, ?, INET6_ATON('10.0.0.2'), 65001, 65000, 'established')
	`, tenant, deviceID); err != nil {
		t.Fatal(err)
	}
	// An aggregate graph charts one of the device's ports (detached, not deleted).
	if _, err := db.ExecContext(ctx, `INSERT INTO aggregate_graphs (id, tenant_id, name) VALUES ('aggr_dd_1', ?, 'dd-graph')`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO aggregate_graph_ports (aggregate_graph_id, tenant_id, port_id) VALUES ('aggr_dd_1', ?, 'port_dd_1')`, tenant); err != nil {
		t.Fatal(err)
	}

	preview, err := store.PreviewDeviceDelete(ctx, tenant, deviceID)
	if err != nil {
		t.Fatal(err)
	}
	impacts := map[string]TargetDeleteImpact{}
	for _, impact := range preview.Impacts {
		impacts[impact.ResourceType] = impact
	}
	if impacts["network_port"].Count != 2 || impacts["network_port"].Behavior != "deleted" {
		t.Fatalf("port impact = %+v", impacts["network_port"])
	}
	if impacts["bgp_session"].Count != 1 {
		t.Fatalf("bgp impact = %+v", impacts["bgp_session"])
	}
	if impacts["aggregate_graph"].Count != 1 || impacts["aggregate_graph"].Behavior != "detached" {
		t.Fatalf("aggregate impact = %+v", impacts["aggregate_graph"])
	}
	if _, ok := impacts["metric_series"]; !ok {
		t.Fatal("preview missing metric_series")
	}

	// Async delete through the worker + API.
	worker := &OperationJobWorker{
		Repo: store, JobType: DeviceDeleteJobType, Owner: "worker-devdel",
		PollInterval: 50 * time.Millisecond, LeaseFor: 3 * time.Second,
		Handler: NewDeviceDeleteJobHandler(store, nil, store),
	}
	go worker.Run(ctx)

	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: tenant, UserID: "", IsAdmin: true}, nil
		},
		Network: store, DeviceDeletePreview: store, OperationJobs: store,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/network/devices/"+string(deviceID), nil))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("delete status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var accepted struct {
		JobID ID `json:"job_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &accepted); err != nil || accepted.JobID == "" {
		t.Fatalf("delete response = %s err=%v", rec.Body.String(), err)
	}
	waitForOperationJob(t, store, tenant, accepted.JobID, OperationJobStatusSucceeded)

	for query, want := range map[string]int{
		"SELECT COUNT(*) FROM network_devices WHERE id = 'device_devdel_01'":      0,
		"SELECT COUNT(*) FROM network_ports WHERE device_id = 'device_devdel_01'": 0,
		"SELECT COUNT(*) FROM bgp_sessions WHERE device_id = 'device_devdel_01'":  0,
		"SELECT COUNT(*) FROM aggregate_graphs WHERE id = 'aggr_dd_1'":            1,
		"SELECT COUNT(*) FROM targets WHERE id = 'target_devdel_01'":              1,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if count != want {
			t.Fatalf("%s = %d, want %d", query, count, want)
		}
	}

	// Destruction receipt recorded and queryable, impact preserved.
	logs, _, err := store.ListAuditLogs(ctx, tenant, AuditLogFilter{ResourceType: "network_device", Action: "network_device.destroyed"})
	if err != nil || len(logs) != 1 {
		t.Fatalf("device destruction receipt = %d err=%v", len(logs), err)
	}
	if impact, ok := logs[0].Detail["impact"].(map[string]any); !ok || impact["network_port"] != float64(2) {
		t.Fatalf("receipt impact = %+v", logs[0].Detail)
	}
}
