package watchdog

import (
	"context"
	"testing"
)

// TestMySQLListAllBGPSessionsPage proves the server-driven Core (BGP) query:
// search across device/peer/AS, the state filter, sort, offset paging, grant
// pushdown by the owning device's target, and the total/established counts.
func TestMySQLListAllBGPSessionsPage(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES ('tgt_bg', ?, 'Core', 'network', '10.0.0.254', 'up')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertDevice(ctx, NetworkDevice{ID: "dev_bg", TenantID: tenant, TargetID: "tgt_bg", SysName: "edge1", Vendor: "V", Model: "M", SNMPPort: 161}); err != nil {
		t.Fatal(err)
	}
	seed := []struct {
		id, peer, state string
		as              uint64
	}{
		{"bgp_1", "10.0.0.1", "established", 65001},
		{"bgp_2", "10.0.0.2", "idle", 65002},
		{"bgp_3", "10.0.0.3", "established", 65003},
	}
	for _, s := range seed {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO bgp_sessions (id, tenant_id, device_id, peer_addr, peer_as, state)
			VALUES (?, ?, 'dev_bg', INET6_ATON(?), ?, ?)
		`, s.id, tenant, s.peer, s.as, s.state); err != nil {
			t.Fatalf("seed %s: %v", s.id, err)
		}
	}

	ids := func(sessions []BGPSession) []ID {
		out := make([]ID, 0, len(sessions))
		for _, s := range sessions {
			out = append(out, s.ID)
		}
		return out
	}
	page := func(q BGPSessionQuery) []ID {
		t.Helper()
		sessions, err := store.ListAllBGPSessionsPage(ctx, tenant, true, nil, q)
		if err != nil {
			t.Fatalf("page %+v: %v", q, err)
		}
		return ids(sessions)
	}

	// Default sort is device sys_name; all share one device so the id tiebreaker
	// orders them.
	if got := page(BGPSessionQuery{Limit: 10}); !equalIDs(got, []ID{"bgp_1", "bgp_2", "bgp_3"}) {
		t.Fatalf("default order = %v", got)
	}
	// Sort by peer AS descending.
	if got := page(BGPSessionQuery{Limit: 10, Sort: "peer_as", Desc: true}); !equalIDs(got, []ID{"bgp_3", "bgp_2", "bgp_1"}) {
		t.Fatalf("peer_as desc = %v", got)
	}
	// Offset paging.
	if got := page(BGPSessionQuery{Limit: 1, Offset: 1}); !equalIDs(got, []ID{"bgp_2"}) {
		t.Fatalf("offset page = %v", got)
	}

	// State filter.
	if got := page(BGPSessionQuery{Limit: 10, State: "established"}); !equalIDs(got, []ID{"bgp_1", "bgp_3"}) {
		t.Fatalf("state established = %v", got)
	}
	if got := page(BGPSessionQuery{Limit: 10, State: "idle"}); !equalIDs(got, []ID{"bgp_2"}) {
		t.Fatalf("state idle = %v", got)
	}

	// Search across peer address, AS number and device name.
	if got := page(BGPSessionQuery{Limit: 10, Search: "10.0.0.2"}); !equalIDs(got, []ID{"bgp_2"}) {
		t.Fatalf("search peer = %v", got)
	}
	if got := page(BGPSessionQuery{Limit: 10, Search: "65003"}); !equalIDs(got, []ID{"bgp_3"}) {
		t.Fatalf("search AS = %v", got)
	}
	if got := page(BGPSessionQuery{Limit: 10, Search: "edge1"}); len(got) != 3 {
		t.Fatalf("search device = %v", got)
	}

	// Grant pushdown by the owning device's target.
	if got := page(BGPSessionQuery{Limit: 10}); len(got) != 3 { // admin sanity
		t.Fatalf("pre-scope = %v", got)
	}
	scoped, err := store.ListAllBGPSessionsPage(ctx, tenant, false, []ID{"tgt_bg"}, BGPSessionQuery{Limit: 10})
	if err != nil || len(scoped) != 3 {
		t.Fatalf("granted scope = %d err=%v", len(scoped), err)
	}
	empty, err := store.ListAllBGPSessionsPage(ctx, tenant, false, nil, BGPSessionQuery{Limit: 10})
	if err != nil || len(empty) != 0 {
		t.Fatalf("no-grant scope = %d err=%v", len(empty), err)
	}

	// Counts: 3 total, 2 established; search narrows both.
	counts, err := store.CountBGPSessions(ctx, tenant, true, nil, "")
	if err != nil || counts.Total != 3 || counts.Established != 2 {
		t.Fatalf("counts = %+v err=%v", counts, err)
	}
	narrowed, err := store.CountBGPSessions(ctx, tenant, true, nil, "10.0.0.2")
	if err != nil || narrowed.Total != 1 || narrowed.Established != 0 {
		t.Fatalf("narrowed counts = %+v err=%v", narrowed, err)
	}
}
