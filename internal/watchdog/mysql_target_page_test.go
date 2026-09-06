package watchdog

import (
	"context"
	"database/sql"
	"os"
	"testing"
)

// TestMySQLListTargetsPage proves the opt-in keyset page: (name, id) ordering
// with an id tiebreaker on duplicate names, no page overlap, full coverage, and
// the grant pushdown (allowedIDs / admin-sees-all / empty = nothing).
func TestMySQLListTargetsPage(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run MySQL target pagination test")
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
	const tenant = ID("tenant_target_page")
	_, _ = db.Exec("DELETE FROM tenants WHERE id = ?", tenant)
	t.Cleanup(func() { _, _ = db.Exec("DELETE FROM tenants WHERE id = ?", tenant) })
	if _, err := db.Exec("INSERT INTO tenants (id, name, status) VALUES (?, 'Target Page', 'active')", tenant); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)

	// Two targets share the name "dup" so the id tiebreaker is exercised.
	seed := []struct {
		id, name, host string
	}{
		{"tgt_page_1", "dup", "192.0.2.1"},
		{"tgt_page_2", "dup", "192.0.2.2"},
		{"tgt_page_3", "m", "192.0.2.3"},
		{"tgt_page_4", "x", "192.0.2.4"},
		{"tgt_page_5", "z", "192.0.2.5"},
	}
	for _, s := range seed {
		if _, err := store.CreateTarget(ctx, Target{ID: ID(s.id), TenantID: tenant, Name: s.name, Kind: TargetKindSystem, Host: s.host}); err != nil {
			t.Fatalf("seed %s: %v", s.id, err)
		}
	}
	wantOrder := []ID{"tgt_page_1", "tgt_page_2", "tgt_page_3", "tgt_page_4", "tgt_page_5"}

	collect := func(all bool, allowed []ID, limit int) []ID {
		t.Helper()
		var ids []ID
		cursor := ""
		for i := 0; ; i++ {
			page, next, err := store.ListTargetsPage(ctx, tenant, all, allowed, TargetPageFilter{Limit: limit, Cursor: cursor})
			if err != nil {
				t.Fatalf("page: %v", err)
			}
			for _, tg := range page {
				ids = append(ids, tg.ID)
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

	// Admin (all=true): walk every page at limit 2, expect the full ordered set.
	got := collect(true, nil, 2)
	if !equalIDs(got, wantOrder) {
		t.Fatalf("admin paged order = %v, want %v", got, wantOrder)
	}

	// Grant pushdown (all=false): only the allowed ids come back, still ordered.
	allowed := []ID{"tgt_page_2", "tgt_page_4", "tgt_page_5"}
	gotAllowed := collect(false, allowed, 2)
	if !equalIDs(gotAllowed, []ID{"tgt_page_2", "tgt_page_4", "tgt_page_5"}) {
		t.Fatalf("allowed paged = %v", gotAllowed)
	}

	// A non-admin with no grants sees nothing (and issues no query row).
	empty, next, err := store.ListTargetsPage(ctx, tenant, false, nil, TargetPageFilter{Limit: 10})
	if err != nil || len(empty) != 0 || next != "" {
		t.Fatalf("empty scope = %v next=%q err=%v", empty, next, err)
	}

	// exclude_kind omits network targets (the Hosts view). Seed a network
	// target, then page with ExcludeKind=network and confirm it is absent while
	// the five system targets remain.
	if _, err := store.CreateTarget(ctx, Target{ID: "tgt_page_net", TenantID: tenant, Name: "aaa-net", Kind: TargetKindNetwork, Host: "198.51.100.1"}); err != nil {
		t.Fatal(err)
	}
	hosts := collect(true, nil, 100)
	if !contains(hosts, "tgt_page_net") {
		t.Fatal("network target should appear without exclude_kind")
	}
	filtered, _, err := store.ListTargetsPage(ctx, tenant, true, nil, TargetPageFilter{Limit: 100, ExcludeKind: string(TargetKindNetwork)})
	if err != nil {
		t.Fatal(err)
	}
	for _, tg := range filtered {
		if tg.ID == "tgt_page_net" {
			t.Fatal("exclude_kind=network must omit the network target")
		}
	}
	if len(filtered) != len(wantOrder) {
		t.Fatalf("exclude_kind page = %d, want %d system targets", len(filtered), len(wantOrder))
	}

	if _, err := db.Exec(`UPDATE targets SET status = CASE id WHEN 'tgt_page_1' THEN 'up' WHEN 'tgt_page_2' THEN 'down' ELSE status END WHERE tenant_id = ?`, tenant); err != nil {
		t.Fatal(err)
	}
	tablePage, total, err := store.ListTargetsTablePage(ctx, tenant, true, nil, TargetTableQuery{
		Search: "192.0.2", Status: "pending", ExcludeKind: string(TargetKindNetwork), Sort: "host", Desc: true, Limit: 2, Offset: 1,
	})
	if err != nil || total != 3 || len(tablePage) != 2 || tablePage[0].ID != "tgt_page_4" || tablePage[1].ID != "tgt_page_3" {
		t.Fatalf("server table page=%v total=%d err=%v", tablePage, total, err)
	}
	scopedPage, scopedTotal, err := store.ListTargetsTablePage(ctx, tenant, false, []ID{"tgt_page_3", "tgt_page_5"}, TargetTableQuery{
		ExcludeKind: string(TargetKindNetwork), Sort: "name", Limit: 25,
	})
	if err != nil || scopedTotal != 2 || len(scopedPage) != 2 || scopedPage[0].ID != "tgt_page_3" || scopedPage[1].ID != "tgt_page_5" {
		t.Fatalf("scoped server table page=%v total=%d err=%v", scopedPage, scopedTotal, err)
	}
}

func contains(ids []ID, want ID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func equalIDs(a, b []ID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
