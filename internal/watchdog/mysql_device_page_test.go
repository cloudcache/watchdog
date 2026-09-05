package watchdog

import (
	"context"
	"testing"
)

// TestMySQLListDevicesPage proves the opt-in keyset page: (sys_name, id)
// ordering with an id tiebreaker on duplicate names, no overlap, full coverage,
// and the grant pushdown by target_id (allowed / admin-sees-all / empty).
func TestMySQLListDevicesPage(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()

	for _, tgt := range []string{"target_dp_a", "target_dp_b"} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO targets (id, tenant_id, name, kind, host, status)
			VALUES (?, ?, ?, 'network', ?, 'pending')
		`, tgt, tenant, tgt, "192.0.2."+tgt[len(tgt)-1:]); err != nil {
			t.Fatalf("seed target %s: %v", tgt, err)
		}
	}

	// Two devices share sys_name "dup" so the id tiebreaker is exercised.
	seed := []struct {
		id, sysName, target string
	}{
		{"dev_dp_1", "dup", "target_dp_a"},
		{"dev_dp_2", "dup", "target_dp_a"},
		{"dev_dp_3", "m", "target_dp_b"},
		{"dev_dp_4", "x", "target_dp_a"},
		{"dev_dp_5", "z", "target_dp_b"},
	}
	for _, s := range seed {
		if _, err := store.UpsertDevice(ctx, NetworkDevice{ID: ID(s.id), TenantID: tenant, TargetID: ID(s.target), SysName: s.sysName, Vendor: "V", Model: "M", SNMPPort: 161}); err != nil {
			t.Fatalf("seed %s: %v", s.id, err)
		}
	}
	wantOrder := []ID{"dev_dp_1", "dev_dp_2", "dev_dp_3", "dev_dp_4", "dev_dp_5"}

	collect := func(all bool, allowed []ID, limit int) []ID {
		t.Helper()
		var ids []ID
		cursor := ""
		for i := 0; ; i++ {
			page, next, err := store.ListDevicesPage(ctx, tenant, all, allowed, NetworkDevicePageFilter{Limit: limit, Cursor: cursor})
			if err != nil {
				t.Fatalf("page: %v", err)
			}
			for _, d := range page {
				ids = append(ids, d.ID)
			}
			if next == "" {
				break
			}
			cursor = next
			if i > 50 {
				t.Fatal("pagination did not terminate")
			}
		}
		return ids
	}

	// Admin / tenant-wide (all=true): full ordered set at limit 2.
	if got := collect(true, nil, 2); !equalIDs(got, wantOrder) {
		t.Fatalf("admin paged order = %v, want %v", got, wantOrder)
	}

	// Grant pushdown by target_id: only devices on target_dp_a, still ordered.
	gotA := collect(false, []ID{"target_dp_a"}, 2)
	if !equalIDs(gotA, []ID{"dev_dp_1", "dev_dp_2", "dev_dp_4"}) {
		t.Fatalf("target_dp_a paged = %v", gotA)
	}

	// A non-admin with no target grants sees nothing (no query row).
	empty, next, err := store.ListDevicesPage(ctx, tenant, false, nil, NetworkDevicePageFilter{Limit: 10})
	if err != nil || len(empty) != 0 || next != "" {
		t.Fatalf("empty scope = %v next=%q err=%v", empty, next, err)
	}
}
