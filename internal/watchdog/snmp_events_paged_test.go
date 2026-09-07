package watchdog

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestMySQLListSNMPEventsPaged proves keyset pagination (newest-first, no
// overlap) and search/severity/event_type filters over a device's events.
func TestMySQLListSNMPEventsPaged(t *testing.T) {
	db, tenant := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	device := ID("device_events_paged")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO targets (id, tenant_id, name, kind, host, status)
		VALUES ('target_ev', ?, 'Ev', 'network', '192.0.2.60', 'pending')
	`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertDevice(ctx, NetworkDevice{ID: device, TenantID: tenant, TargetID: "target_ev", Vendor: "V", Model: "M", SNMPPort: 161}); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		severity := "info"
		eventType := "link"
		if i%2 == 0 {
			severity = "critical"
			eventType = "bgp"
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO snmp_events (id, tenant_id, device_id, source, severity, event_type, message, occurred_at)
			VALUES (?, ?, ?, 'trap', ?, ?, ?, ?)
		`, fmt.Sprintf("ev_%02d", i), tenant, device, severity, eventType, fmt.Sprintf("event %d", i), base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}

	// Page 1 of 2, newest first.
	page1, cursor, err := store.ListSNMPEventsPaged(ctx, tenant, device, SNMPEventFilter{Limit: 2})
	if err != nil || len(page1) != 2 || cursor == "" {
		t.Fatalf("page1 = %d cursor=%q err=%v", len(page1), cursor, err)
	}
	if page1[0].ID != "ev_04" || page1[1].ID != "ev_03" {
		t.Fatalf("page1 order = %s,%s", page1[0].ID, page1[1].ID)
	}
	page2, cursor2, err := store.ListSNMPEventsPaged(ctx, tenant, device, SNMPEventFilter{Limit: 2, Cursor: cursor})
	if err != nil || len(page2) != 2 || page2[0].ID != "ev_02" {
		t.Fatalf("page2 = %+v cursor=%q err=%v", page2, cursor2, err)
	}
	seen := map[ID]bool{}
	for _, ev := range append(append([]SNMPEvent{}, page1...), page2...) {
		if seen[ev.ID] {
			t.Fatalf("pages overlap on %s", ev.ID)
		}
		seen[ev.ID] = true
	}

	// Severity filter: only the 3 critical events (ev_00, ev_02, ev_04).
	critical, _, err := store.ListSNMPEventsPaged(ctx, tenant, device, SNMPEventFilter{Severities: []string{"critical"}, Limit: 100})
	if err != nil || len(critical) != 3 {
		t.Fatalf("critical filter = %d err=%v", len(critical), err)
	}
	// Multi-severity is a server-side OR, while event_type composes as AND.
	allSeverities, _, err := store.ListSNMPEventsPaged(ctx, tenant, device, SNMPEventFilter{Severities: []string{"critical", "info"}, Limit: 100})
	if err != nil || len(allSeverities) != 5 {
		t.Fatalf("multi-severity filter = %d err=%v", len(allSeverities), err)
	}
	linkInfo, _, err := store.ListSNMPEventsPaged(ctx, tenant, device, SNMPEventFilter{EventType: "link", Severities: []string{"info"}, Limit: 100})
	if err != nil || len(linkInfo) != 2 {
		t.Fatalf("event_type filter = %d err=%v", len(linkInfo), err)
	}
	matched, _, err := store.ListSNMPEventsPaged(ctx, tenant, device, SNMPEventFilter{Search: "event 4", Limit: 100})
	if err != nil || len(matched) != 1 || matched[0].ID != "ev_04" {
		t.Fatalf("search filter = %+v err=%v", matched, err)
	}
	// LIKE wildcards in user input are literals, not an accidental match-all.
	wildcard, _, err := store.ListSNMPEventsPaged(ctx, tenant, device, SNMPEventFilter{Search: "%", Limit: 100})
	if err != nil || len(wildcard) != 0 {
		t.Fatalf("escaped search filter = %+v err=%v", wildcard, err)
	}

	// The VTable contract sorts and filters the full result before applying the
	// offset, and returns the filtered total independently of the page.
	tablePage, total, err := store.ListSNMPEventTable(ctx, tenant, device, SNMPEventTableQuery{
		Severities: []string{"critical"}, SortBy: "occurred_at", SortDirection: "DESC", Limit: 2, Offset: 1,
	})
	if err != nil || total != 3 || len(tablePage) != 2 || tablePage[0].ID != "ev_02" || tablePage[1].ID != "ev_00" {
		t.Fatalf("table page=%+v total=%d err=%v", tablePage, total, err)
	}
	byMessage, total, err := store.ListSNMPEventTable(ctx, tenant, device, SNMPEventTableQuery{
		SortBy: "message", SortDirection: "ASC", Limit: 2, Offset: 2,
	})
	if err != nil || total != 5 || len(byMessage) != 2 || byMessage[0].ID != "ev_02" || byMessage[1].ID != "ev_03" {
		t.Fatalf("message page=%+v total=%d err=%v", byMessage, total, err)
	}
	facets, err := store.ListSNMPEventFacets(ctx, tenant, device, SNMPEventFacetQuery{
		SNMPEventTableQuery: SNMPEventTableQuery{
			Severities: []string{"critical"}, EventTypes: []string{"link"}, SortBy: "occurred_at", SortDirection: "DESC", Limit: 10,
		},
		Field: "event_type",
	})
	if err != nil || len(facets) != 1 || facets[0].Value != "bgp" || facets[0].Count != 3 {
		t.Fatalf("facets=%+v err=%v", facets, err)
	}
	wildcardFacets, err := store.ListSNMPEventFacets(ctx, tenant, device, SNMPEventFacetQuery{
		SNMPEventTableQuery: SNMPEventTableQuery{SortBy: "occurred_at", SortDirection: "DESC", Limit: 10},
		Field:               "source", FacetSearch: "%",
	})
	if err != nil || len(wildcardFacets) != 0 {
		t.Fatalf("escaped facets=%+v err=%v", wildcardFacets, err)
	}

	for _, indexName := range []string{"idx_snmp_events_device_severity_time", "idx_snmp_events_device_type_time"} {
		var count int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.statistics
			WHERE table_schema = DATABASE() AND table_name = 'snmp_events' AND index_name = ?
		`, indexName).Scan(&count); err != nil || count == 0 {
			t.Fatalf("index %s count=%d err=%v", indexName, count, err)
		}
	}
}
