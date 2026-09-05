package watchdog

import (
	"context"
	"testing"
)

// TestMySQLDeviceSummaryQuery proves the server-driven summary query: search
// across device+target text, the status filter (with "down" = not up), sort
// direction, offset paging, grant pushdown, and the badge counts.
func TestMySQLDeviceSummaryQuery(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()

	seed := []struct {
		target, name, status, device, sysName, vendor, os string
	}{
		{"tgt_su_up", "Core-A", "up", "dev_su_up", "alpha", "Cisco", "IOS"},
		{"tgt_su_down", "Core-B", "down", "dev_su_down", "bravo", "Juniper", "JunOS"},
		{"tgt_su_pend", "Core-C", "pending", "dev_su_pend", "charlie", "Arista", "EOS"},
	}
	for _, s := range seed {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO targets (id, tenant_id, name, kind, host, status)
			VALUES (?, ?, ?, 'network', ?, ?)
		`, s.target, tenant, s.name, s.device+".host", s.status); err != nil {
			t.Fatalf("seed target %s: %v", s.target, err)
		}
		if _, err := store.UpsertDevice(ctx, NetworkDevice{
			ID: ID(s.device), TenantID: tenant, TargetID: ID(s.target),
			SysName: s.sysName, Vendor: s.vendor, OSName: s.os, Model: "M", SNMPPort: 161,
		}); err != nil {
			t.Fatalf("seed device %s: %v", s.device, err)
		}
	}

	ids := func(devices []NetworkDevice) []ID {
		out := make([]ID, 0, len(devices))
		for _, d := range devices {
			out = append(out, d.ID)
		}
		return out
	}
	page := func(q DeviceSummaryQuery) []ID {
		t.Helper()
		devices, err := store.ListDeviceSummaryDevicesPage(ctx, tenant, true, nil, q)
		if err != nil {
			t.Fatalf("page %+v: %v", q, err)
		}
		return ids(devices)
	}

	// Default sort is target name ascending: Core-A, Core-B, Core-C.
	if got := page(DeviceSummaryQuery{Limit: 10}); !equalIDs(got, []ID{"dev_su_up", "dev_su_down", "dev_su_pend"}) {
		t.Fatalf("default order = %v", got)
	}
	// Sort by vendor ascending: Arista, Cisco, Juniper.
	if got := page(DeviceSummaryQuery{Limit: 10, Sort: "vendor"}); !equalIDs(got, []ID{"dev_su_pend", "dev_su_up", "dev_su_down"}) {
		t.Fatalf("vendor order = %v", got)
	}
	// Descending name.
	if got := page(DeviceSummaryQuery{Limit: 10, Sort: "name", Desc: true}); !equalIDs(got, []ID{"dev_su_pend", "dev_su_down", "dev_su_up"}) {
		t.Fatalf("name desc order = %v", got)
	}
	// Offset paging over the default order.
	if got := page(DeviceSummaryQuery{Limit: 1, Offset: 1}); !equalIDs(got, []ID{"dev_su_down"}) {
		t.Fatalf("offset page = %v", got)
	}

	// Status filter: up = up only; down = not up (down + pending); pending only.
	if got := page(DeviceSummaryQuery{Limit: 10, Status: "up"}); !equalIDs(got, []ID{"dev_su_up"}) {
		t.Fatalf("status up = %v", got)
	}
	if got := page(DeviceSummaryQuery{Limit: 10, Status: "down"}); !equalIDs(got, []ID{"dev_su_down", "dev_su_pend"}) {
		t.Fatalf("status down (not up) = %v", got)
	}
	if got := page(DeviceSummaryQuery{Limit: 10, Status: "pending"}); !equalIDs(got, []ID{"dev_su_pend"}) {
		t.Fatalf("status pending = %v", got)
	}

	// Search matches across vendor / sys_name / os / target name / host,
	// case-insensitively.
	if got := page(DeviceSummaryQuery{Limit: 10, Search: "juniper"}); !equalIDs(got, []ID{"dev_su_down"}) {
		t.Fatalf("search vendor = %v", got)
	}
	if got := page(DeviceSummaryQuery{Limit: 10, Search: "alpha"}); !equalIDs(got, []ID{"dev_su_up"}) {
		t.Fatalf("search sys_name = %v", got)
	}
	if got := page(DeviceSummaryQuery{Limit: 10, Search: "eos"}); !equalIDs(got, []ID{"dev_su_pend"}) {
		t.Fatalf("search os = %v", got)
	}
	if got := page(DeviceSummaryQuery{Limit: 10, Search: "core"}); len(got) != 3 {
		t.Fatalf("search target name = %v", got)
	}

	// Grant pushdown by target_id.
	if got := page(DeviceSummaryQuery{Limit: 10}); len(got) != 3 { // sanity before scoping
		t.Fatalf("pre-scope = %v", got)
	}
	scoped, err := store.ListDeviceSummaryDevicesPage(ctx, tenant, false, []ID{"tgt_su_pend"}, DeviceSummaryQuery{Limit: 10})
	if err != nil || !equalIDs(ids(scoped), []ID{"dev_su_pend"}) {
		t.Fatalf("scoped = %v err=%v", ids(scoped), err)
	}

	// Counts: up=1, down=total-up=2, pending=1, total=3 (search-narrowed too).
	counts, err := store.CountDeviceStatuses(ctx, tenant, true, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if counts.Total != 3 || counts.Up != 1 || counts.Down != 2 || counts.Pending != 1 {
		t.Fatalf("counts = %+v", counts)
	}
	narrowed, err := store.CountDeviceStatuses(ctx, tenant, true, nil, "alpha")
	if err != nil {
		t.Fatal(err)
	}
	if narrowed.Total != 1 || narrowed.Up != 1 {
		t.Fatalf("narrowed counts = %+v", narrowed)
	}
}
