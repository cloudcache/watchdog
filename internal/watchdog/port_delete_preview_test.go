package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestMySQLPortDeletePreviewJobAndReceipt seeds a port with owned rows and
// references, checks the preview, then deletes it asynchronously through the
// API + worker and verifies the cascade, detach, series cleanup and receipt.
func TestMySQLPortDeletePreviewJobAndReceipt(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	targetID := ID("target_portdel_01")
	deviceID := ID("device_portdel_01")
	portID := ID("port_portdel_01")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES (?, ?, 'PortDel', 'network', '192.0.2.211', 'pending')
	`, targetID, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertDevice(ctx, NetworkDevice{ID: deviceID, TenantID: tenant, TargetID: targetID, Vendor: "V", Model: "M", SysName: "pd", SNMPPort: 161}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPorts(ctx, []NetworkPort{{ID: portID, TenantID: tenant, DeviceID: deviceID, IfIndex: 1, IfName: "ge-0/0/1"}}); err != nil {
		t.Fatal(err)
	}
	// Owned rows (deleted with the port): an interface address and a policy.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO network_interface_addresses (id, tenant_id, device_id, port_id, if_index, address, family, prefix_length, origin)
		VALUES ('ia_pd_1', ?, ?, ?, 1, INET6_ATON('10.1.1.1'), 'ipv4', 24, 'manual')
	`, tenant, deviceID, portID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO port_policies (id, tenant_id, port_id, side_type, billing_base_bps, sample_step_seconds, correction_direction, correction_min, correction_max, enabled)
		VALUES ('pp_pd_1', ?, ?, 'provider', 0, 300, 'none', 0, 0, 1)
	`, tenant, portID); err != nil {
		t.Fatal(err)
	}
	// Referencing membership (detached): an aggregate graph.
	if _, err := db.ExecContext(ctx, `INSERT INTO aggregate_graphs (id, tenant_id, name) VALUES ('aggr_pd_1', ?, 'pd-graph')`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO aggregate_graph_ports (aggregate_graph_id, tenant_id, port_id) VALUES ('aggr_pd_1', ?, ?)`, tenant, portID); err != nil {
		t.Fatal(err)
	}

	preview, err := store.PreviewPortDelete(ctx, tenant, portID)
	if err != nil {
		t.Fatal(err)
	}
	impacts := map[string]TargetDeleteImpact{}
	for _, impact := range preview.Impacts {
		impacts[impact.ResourceType] = impact
	}
	if impacts["network_interface_address"].Count != 1 || impacts["network_interface_address"].Behavior != "deleted" {
		t.Fatalf("address impact = %+v", impacts["network_interface_address"])
	}
	if impacts["port_policy"].Count != 1 {
		t.Fatalf("policy impact = %+v", impacts["port_policy"])
	}
	if impacts["aggregate_graph"].Count != 1 || impacts["aggregate_graph"].Behavior != "detached" {
		t.Fatalf("aggregate impact = %+v", impacts["aggregate_graph"])
	}

	worker := &OperationJobWorker{
		Repo: store, JobType: PortDeleteJobType, Owner: "worker-portdel",
		PollInterval: 50 * time.Millisecond, LeaseFor: 3 * time.Second,
		Handler: NewPortDeleteJobHandler(store, nil, store),
	}
	go worker.Run(ctx)

	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: tenant, UserID: "", IsAdmin: true}, nil
		},
		Network: store, PortDeletePreview: store, OperationJobs: store,
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/network/ports/"+string(portID), nil))
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
		"SELECT COUNT(*) FROM network_ports WHERE id = 'port_portdel_01'":                    0,
		"SELECT COUNT(*) FROM network_interface_addresses WHERE port_id = 'port_portdel_01'": 0,
		"SELECT COUNT(*) FROM port_policies WHERE port_id = 'port_portdel_01'":               0,
		"SELECT COUNT(*) FROM aggregate_graph_ports WHERE port_id = 'port_portdel_01'":       0,
		"SELECT COUNT(*) FROM aggregate_graphs WHERE id = 'aggr_pd_1'":                       1,
		"SELECT COUNT(*) FROM network_devices WHERE id = 'device_portdel_01'":                1,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if count != want {
			t.Fatalf("%s = %d, want %d", query, count, want)
		}
	}

	logs, _, err := store.ListAuditLogs(ctx, tenant, AuditLogFilter{ResourceType: "network_port", Action: "network_port.destroyed"})
	if err != nil || len(logs) != 1 {
		t.Fatalf("port destruction receipt = %d err=%v", len(logs), err)
	}
	if impact, ok := logs[0].Detail["impact"].(map[string]any); !ok || impact["network_interface_address"] != float64(1) {
		t.Fatalf("receipt impact = %+v", logs[0].Detail)
	}
}
