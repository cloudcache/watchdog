package server

import (
	"context"
	"testing"
	"time"
)

func TestSNMPDiscoveryBackoff(t *testing.T) {
	b := &snmpDiscoveryBackoff{until: map[string]time.Time{}}
	now := time.Now()
	if !b.ready("d1", now) {
		t.Fatal("an unknown device should be ready immediately")
	}
	b.fail("d1", now)
	if b.ready("d1", now.Add(time.Minute)) {
		t.Fatal("a device in backoff should not be ready")
	}
	if !b.ready("d1", now.Add(snmpDiscoverFailBackoff+time.Second)) {
		t.Fatal("a device should be ready after the backoff window elapses")
	}
	b.fail("d1", now)
	b.ok("d1")
	if !b.ready("d1", now) {
		t.Fatal("a successful discovery should clear the backoff")
	}
}

// TestDueSNMPDiscoveryDevices proves the selection that closes the add→collect
// loop: a network device with an SNMP profile but no recipes (never discovered)
// is due, a device whose recipes are stale is due, and fresh / unprofiled /
// disabled devices are not. Opt-in via WATCHDOG_TEST_MYSQL_DSN.
func TestDueSNMPDiscoveryDevices(t *testing.T) {
	dsn := isolatedMySQLDSN(t)
	s, err := New(Config{MySQL: MySQLConfig{DSN: dsn}, Admin: AdminConfig{Username: "disc-admin", Password: "disc-password"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.db.Exec(`INSERT INTO snmp_profiles (id,name,version,security_json) VALUES ('discp','disc-profile','2c','{}')`); err != nil {
		t.Fatal(err)
	}
	devices := []struct {
		id, host, profile string
		disabled          int
	}{
		{"discA", "10.90.0.1", "discp", 0}, // profiled, never discovered → due
		{"discB", "10.90.0.2", "discp", 0}, // profiled, fresh recipe → not due
		{"discC", "10.90.0.3", "", 0},      // no profile → not due
		{"discD", "10.90.0.4", "discp", 0}, // profiled, stale recipe → due
		{"discE", "10.90.0.5", "discp", 1}, // disabled → not due
	}
	for _, d := range devices {
		var profile any
		if d.profile != "" {
			profile = d.profile
		}
		if _, err := s.db.Exec(`INSERT INTO devices (id,host,kind,snmp_profile_id,disabled) VALUES (?,?,'network',?,?)`, d.id, d.host, profile, d.disabled); err != nil {
			t.Fatal(err)
		}
	}
	insertRecipe := func(id, deviceID string, discoveredAt time.Time) {
		if _, err := s.db.Exec(`INSERT INTO snmp_collection_recipes
			(id,device_id,entity_kind,module_name,metric,value_kind,oid,numeric_oid,labels_json,options_json,discovered_at,enabled)
			VALUES (?,?,'device','system','uptime','gauge','.1.3.6.1','.1.3.6.1','{}','{}',?,1)`, id, deviceID, discoveredAt); err != nil {
			t.Fatal(err)
		}
	}
	insertRecipe("recB", "discB", time.Now().UTC())
	insertRecipe("recD", "discD", time.Now().UTC().Add(-10*time.Hour))

	threshold := time.Now().UTC().Add(-6 * time.Hour)
	due, err := s.dueSNMPDiscoveryDevices(context.Background(), threshold, 50)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, id := range due {
		got[id] = true
	}
	if !got["discA"] || !got["discD"] {
		t.Fatalf("expected never-discovered discA and stale discD to be due; got %v", due)
	}
	if got["discB"] || got["discC"] || got["discE"] {
		t.Fatalf("fresh/unprofiled/disabled devices must not be due; got %v", due)
	}
	if len(due) >= 2 && due[0] != "discA" {
		t.Fatalf("never-discovered device should sort before stale ones; got %v", due)
	}
}
