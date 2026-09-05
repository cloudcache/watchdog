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
		{"tgt_page_1", "dup", "h1"},
		{"tgt_page_2", "dup", "h2"},
		{"tgt_page_3", "m", "h3"},
		{"tgt_page_4", "x", "h4"},
		{"tgt_page_5", "z", "h5"},
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
