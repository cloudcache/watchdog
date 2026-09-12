package snmpch

import (
	"os"
	"strings"
	"testing"
)

func TestSNMPEventsMigrationIsClickHouseOnlyAndUnboundedByPolicyTTL(t *testing.T) {
	data, err := os.ReadFile("../../deploy/migration/clickhouse/014_snmp_events.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(data)
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS watchdog_flow.snmp_events",
		"ReplacingMergeTree(ingested_at)",
		"ORDER BY (device_id, occurred_at, id)",
		"raw_json    String",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("SNMP event migration is missing %q", required)
		}
	}
	for _, forbidden := range []string{"tenant_id", "TTL occurred_at", "ENGINE = MySQL"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("SNMP event migration retained forbidden contract %q", forbidden)
		}
	}
}
