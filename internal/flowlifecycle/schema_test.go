package flowlifecycle

import (
	"os"
	"strings"
	"testing"
)

func TestLifecycleSchemaIsSingleDomainAndFailClosed(t *testing.T) {
	data, err := os.ReadFile("../../deploy/schema/mysql/0033_flow_storage_lifecycle.sql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(data)
	for _, required := range []string{
		"flow_retention_policy_revisions", "active_singleton", "raw_delete_enabled",
		"flow_retention_partition_states", "flow_reconciliation_watermarks",
		"bootstrap_offset <= reconciled_next_offset", "reconciled_next_offset <= committed_next_offset",
		"flow_backup_restore_evidence", "restore_tested_at", "flow_deletion_receipts",
		"kafka_coverage_json", "partition_granularity = 'day'", "partition_granularity = 'month'",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("lifecycle schema is missing %q", required)
		}
	}
	if strings.Contains(schema, "tenant_id") {
		t.Fatal("single-domain lifecycle schema retained tenant_id")
	}
	for _, forbidden := range []string{"DEFAULT 365", "TTL ", "retention_raw_days"} {
		if strings.Contains(schema, forbidden) {
			t.Fatalf("lifecycle schema retained false static lifecycle contract %q", forbidden)
		}
	}
}
