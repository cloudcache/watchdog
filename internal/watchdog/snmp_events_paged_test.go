package watchdog

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestMySQLListSNMPEventsPaged proves keyset pagination (newest-first, no
// overlap) and severity/event_type filters over a device's events.
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
	critical, _, err := store.ListSNMPEventsPaged(ctx, tenant, device, SNMPEventFilter{Severity: "critical", Limit: 100})
	if err != nil || len(critical) != 3 {
		t.Fatalf("critical filter = %d err=%v", len(critical), err)
	}
	// event_type filter composes with severity.
	linkInfo, _, err := store.ListSNMPEventsPaged(ctx, tenant, device, SNMPEventFilter{EventType: "link", Limit: 100})
	if err != nil || len(linkInfo) != 2 {
		t.Fatalf("event_type filter = %d err=%v", len(linkInfo), err)
	}
}
