package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
)

func TestDefaultString(t *testing.T) {
	if got := defaultString("", "pending"); got != "pending" {
		t.Fatalf("default = %s", got)
	}
	if got := defaultString("up", "pending"); got != "up" {
		t.Fatalf("explicit = %s", got)
	}
}

func TestTargetLabelsJSONRoundTrip(t *testing.T) {
	encoded, err := encodeLabelsForTest(map[string]string{"site": "shanghai", "role": "edge"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeStringMapJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded["site"] != "shanghai" || decoded["role"] != "edge" {
		t.Fatalf("labels = %#v", decoded)
	}
}

func TestMySQLCreateNetworkTargetIsAtomicAndHostUnique(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run MySQL target integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := ApplyMySQLMigrations(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	const tenantID = ID("tenant_target_check")
	_, _ = db.Exec("DELETE FROM tenants WHERE id = ?", tenantID)
	defer db.Exec("DELETE FROM tenants WHERE id = ?", tenantID)
	if _, err := db.Exec("INSERT INTO tenants (id, name, status) VALUES (?, 'Target Check', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO snmp_profiles (id, tenant_id, name, version, security_json, timeout_ms, retries)
		VALUES ('profile_target_check', ?, 'target-check', '2c', JSON_OBJECT('community', 'public'), 1000, 1)
	`, tenantID); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	target := Target{ID: "target_target_check", TenantID: tenantID, Name: "10.0.0.10", Kind: TargetKindNetwork, Host: "10.0.0.10"}
	device := NetworkDevice{ID: "device_target_check", TenantID: tenantID, TargetID: target.ID, SNMPProfileID: "profile_target_check", SNMPPort: 161}
	createdTarget, createdDevice, err := store.CreateNetworkTarget(context.Background(), target, device)
	if err != nil {
		t.Fatal(err)
	}
	if createdTarget.Host != target.Host || createdDevice.TargetID != target.ID || createdDevice.SNMPProfileID != device.SNMPProfileID {
		t.Fatalf("created target/device = %#v / %#v", createdTarget, createdDevice)
	}
	var managementIP string
	if err := db.QueryRow("SELECT INET6_NTOA(mgmt_ip) FROM targets WHERE tenant_id = ? AND id = ?", tenantID, target.ID).Scan(&managementIP); err != nil {
		t.Fatal(err)
	}
	if managementIP != target.Host {
		t.Fatalf("management IP = %q, want %q", managementIP, target.Host)
	}

	duplicate := target
	duplicate.ID = "target_duplicate_check"
	if _, err := store.CreateTarget(context.Background(), duplicate); !errors.Is(err, ErrTargetHostExists) {
		t.Fatalf("duplicate host error = %v", err)
	}

	badTarget := Target{ID: "target_rollback_check", TenantID: tenantID, Name: "10.0.0.11", Kind: TargetKindNetwork, Host: "10.0.0.11"}
	badDevice := NetworkDevice{ID: "device_rollback_check", TenantID: tenantID, TargetID: badTarget.ID, SNMPProfileID: "missing_profile", SNMPPort: 161}
	if _, _, err := store.CreateNetworkTarget(context.Background(), badTarget, badDevice); err == nil {
		t.Fatal("expected invalid profile to roll back target creation")
	}
	if _, err := store.GetTarget(context.Background(), tenantID, badTarget.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rolled back target lookup error = %v", err)
	}
}

// TestMySQLDeviceAndPortUpdatedAtPopulated proves the device/port reads scan
// updated_at so the ETag/If-Match contract has a real value to hash.
func TestMySQLDeviceAndPortUpdatedAtPopulated(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run MySQL device/port updated_at test")
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
	tenant := ID("tenant_updatedat_check")
	_, _ = db.Exec("DELETE FROM tenants WHERE id = ?", tenant)
	defer db.Exec("DELETE FROM tenants WHERE id = ?", tenant)
	if _, err := db.Exec("INSERT INTO tenants (id, name, status) VALUES (?, 'UpdatedAt', 'active')", tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES ('target_ua', ?, 'UA', 'network', '10.9.9.9', 'pending')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	if _, err := store.UpsertDevice(ctx, NetworkDevice{ID: "device_ua", TenantID: tenant, TargetID: "target_ua", SNMPPort: 161}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPorts(ctx, []NetworkPort{{ID: "port_ua", TenantID: tenant, DeviceID: "device_ua", IfIndex: 1, IfName: "ge-0/0/1"}}); err != nil {
		t.Fatal(err)
	}

	device, err := store.GetDevice(ctx, tenant, "device_ua")
	if err != nil || device.UpdatedAt.IsZero() {
		t.Fatalf("device UpdatedAt = %v, err = %v", device.UpdatedAt, err)
	}
	port, err := store.GetPort(ctx, tenant, "port_ua")
	if err != nil || port.UpdatedAt.IsZero() {
		t.Fatalf("port UpdatedAt = %v, err = %v", port.UpdatedAt, err)
	}
	// The list paths feed the same scanners; verify they populate it too.
	devices, err := store.ListDevices(ctx, tenant)
	if err != nil || len(devices) != 1 || devices[0].UpdatedAt.IsZero() {
		t.Fatalf("ListDevices UpdatedAt not populated: %+v err=%v", devices, err)
	}
	ports, err := store.ListPorts(ctx, tenant, "device_ua")
	if err != nil || len(ports) != 1 || ports[0].UpdatedAt.IsZero() {
		t.Fatalf("ListPorts UpdatedAt not populated: %+v err=%v", ports, err)
	}
}
