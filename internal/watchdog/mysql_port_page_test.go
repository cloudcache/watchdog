package watchdog

import (
	"context"
	"testing"
)

func TestMySQLDevicePortPages(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status) VALUES
		  ('tgt_port_a', ?, 'Port A', 'network', '192.0.2.40', 'up'),
		  ('tgt_port_b', ?, 'Port B', 'network', '192.0.2.41', 'up')
	`, tenant, tenant); err != nil {
		t.Fatal(err)
	}
	for _, device := range []NetworkDevice{
		{ID: "dev_port_a", TenantID: tenant, TargetID: "tgt_port_a", SysName: "port-a", Vendor: "V", Model: "M", SNMPPort: 161},
		{ID: "dev_port_b", TenantID: tenant, TargetID: "tgt_port_b", SysName: "port-b", Vendor: "V", Model: "M", SNMPPort: 161},
	} {
		if _, err := store.UpsertDevice(ctx, device); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpsertPorts(ctx, []NetworkPort{
		{ID: "port_page_1", TenantID: tenant, DeviceID: "dev_port_a", IfIndex: 1, IfName: "ge-0/0/0", IfAlias: "access", AdminStatus: "up", OperStatus: "up", SpeedBps: 1_000_000_000},
		{ID: "port_page_2", TenantID: tenant, DeviceID: "dev_port_a", IfIndex: 2, IfName: "xe-0/0/0", IfAlias: "transit", AdminStatus: "up", OperStatus: "down", SpeedBps: 10_000_000_000},
		{ID: "port_page_3", TenantID: tenant, DeviceID: "dev_port_a", IfIndex: 3, IfName: "et-0/0/0", IfAlias: "core", AdminStatus: "up", OperStatus: "up", SpeedBps: 100_000_000_000},
		{ID: "port_page_other", TenantID: tenant, DeviceID: "dev_port_b", IfIndex: 1, IfName: "other", AdminStatus: "up", OperStatus: "down"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceInterfaceAddresses(ctx, tenant, "dev_port_a", []NetworkInterfaceAddress{
		{ID: "addr_page_v4", PortID: "port_page_1", IfIndex: 1, Address: "192.0.2.1", Family: "ipv4", PrefixLength: 24},
		{ID: "addr_page_v6_2", PortID: "port_page_2", IfIndex: 2, Address: "2001:db8::2", Family: "ipv6", PrefixLength: 64},
		{ID: "addr_page_v6_3", PortID: "port_page_3", IfIndex: 3, Address: "2001:db8::3", Family: "ipv6", PrefixLength: 64},
	}); err != nil {
		t.Fatal(err)
	}

	ports, total, err := store.ListDevicePortsPage(ctx, tenant, "dev_port_a", NetworkPortQuery{
		AddressFamily: "ipv6", Sort: "speed", Desc: true, Limit: 1, Offset: 1,
	})
	if err != nil || total != 2 || len(ports) != 1 || ports[0].ID != "port_page_2" {
		t.Fatalf("port page=%+v total=%d err=%v", ports, total, err)
	}
	ports, total, err = store.ListDevicePortsPage(ctx, tenant, "dev_port_a", NetworkPortQuery{
		Search: "2001:db8::3", OperStatus: "up", Sort: "if_index", Limit: 10,
	})
	if err != nil || total != 1 || len(ports) != 1 || ports[0].ID != "port_page_3" {
		t.Fatalf("port address search=%+v total=%d err=%v", ports, total, err)
	}
	ports, total, err = store.ListDevicePortsPage(ctx, tenant, "dev_port_a", NetworkPortQuery{Search: "%", Sort: "if_index", Limit: 10})
	if err != nil || total != 0 || len(ports) != 0 {
		t.Fatalf("escaped port search=%+v total=%d err=%v", ports, total, err)
	}
	addresses, err := store.ListInterfaceAddressesByPorts(ctx, tenant, []ID{"port_page_2"})
	if err != nil || len(addresses) != 1 || addresses[0].Address != "2001:db8::2" {
		t.Fatalf("page addresses=%+v err=%v", addresses, err)
	}
	counts, err := store.CountDevicePorts(ctx, tenant, "dev_port_a")
	if err != nil || counts.Total != 3 || counts.Up != 2 || counts.Down != 1 {
		t.Fatalf("port counts=%+v err=%v", counts, err)
	}
}
