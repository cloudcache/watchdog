package watchdog

import (
	"context"
	"testing"
)

func TestMySQLDeviceSwitchingPages(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status) VALUES
		  ('tgt_sw_a', ?, 'Switch A', 'network', '192.0.2.20', 'up'),
		  ('tgt_sw_b', ?, 'Switch B', 'network', '192.0.2.21', 'up')
	`, tenant, tenant); err != nil {
		t.Fatal(err)
	}
	for _, device := range []NetworkDevice{
		{ID: "dev_sw_a", TenantID: tenant, TargetID: "tgt_sw_a", SysName: "sw-a", Vendor: "V", Model: "M", SNMPPort: 161},
		{ID: "dev_sw_b", TenantID: tenant, TargetID: "tgt_sw_b", SysName: "sw-b", Vendor: "V", Model: "M", SNMPPort: 161},
	} {
		if _, err := store.UpsertDevice(ctx, device); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpsertDeviceVLANs(ctx, tenant, "dev_sw_a", []DeviceVLAN{
		{VLANID: 10, Name: "users", Status: "active"},
		{VLANID: 20, Name: "servers", Status: "active"},
		{VLANID: 30, Name: "retired", Status: "suspended"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertDeviceVLANs(ctx, tenant, "dev_sw_b", []DeviceVLAN{{VLANID: 99, Name: "other", Status: "active"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertDeviceLAGGroups(ctx, tenant, "dev_sw_a", []DeviceLAGGroup{
		{AggregateIndex: 1, MACAddress: "00:11:22:33:44:01", Mode: "lacp"},
		{AggregateIndex: 2, MACAddress: "00:11:22:33:44:02", Mode: "static"},
		{AggregateIndex: 3, MACAddress: "00:11:22:33:44:03", Mode: "lacp"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertDeviceLAGGroups(ctx, tenant, "dev_sw_b", []DeviceLAGGroup{{AggregateIndex: 9, MACAddress: "00:11:22:33:44:09", Mode: "lacp"}}); err != nil {
		t.Fatal(err)
	}

	vlans, total, err := store.ListDeviceVLANsPage(ctx, tenant, "dev_sw_a", DeviceVLANQuery{Status: "active", Sort: "name", Desc: true, Limit: 1, Offset: 1})
	if err != nil || total != 2 || len(vlans) != 1 || vlans[0].Name != "servers" {
		t.Fatalf("VLAN page=%+v total=%d err=%v", vlans, total, err)
	}
	vlans, total, err = store.ListDeviceVLANsPage(ctx, tenant, "dev_sw_a", DeviceVLANQuery{Search: "30", Sort: "vlan_id", Limit: 10})
	if err != nil || total != 1 || len(vlans) != 1 || vlans[0].VLANID != 30 {
		t.Fatalf("VLAN search=%+v total=%d err=%v", vlans, total, err)
	}
	vlans, total, err = store.ListDeviceVLANsPage(ctx, tenant, "dev_sw_a", DeviceVLANQuery{Search: "%", Sort: "vlan_id", Limit: 10})
	if err != nil || total != 0 || len(vlans) != 0 {
		t.Fatalf("VLAN escaped search=%+v total=%d err=%v", vlans, total, err)
	}

	lags, total, err := store.ListDeviceLAGGroupsPage(ctx, tenant, "dev_sw_a", DeviceLAGQuery{Mode: "lacp", Sort: "aggregate_index", Desc: true, Limit: 10})
	if err != nil || total != 2 || len(lags) != 2 || lags[0].AggregateIndex != 3 || lags[1].AggregateIndex != 1 {
		t.Fatalf("LAG page=%+v total=%d err=%v", lags, total, err)
	}
	lags, total, err = store.ListDeviceLAGGroupsPage(ctx, tenant, "dev_sw_a", DeviceLAGQuery{Search: "44:02", Sort: "mac_address", Limit: 10})
	if err != nil || total != 1 || len(lags) != 1 || lags[0].AggregateIndex != 2 {
		t.Fatalf("LAG search=%+v total=%d err=%v", lags, total, err)
	}
}
