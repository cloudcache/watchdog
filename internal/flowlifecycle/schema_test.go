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

func TestArchiveLifecycleSchemaAddsRotatingLateCheckEvidence(t *testing.T) {
	data, err := os.ReadFile("../../deploy/schema/mysql/0034_flow_archive_lifecycle.sql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(data)
	for _, required := range []string{"late_checked_at", "idx_flow_retention_late_check"} {
		if !strings.Contains(schema, required) {
			t.Fatalf("archive lifecycle schema is missing %q", required)
		}
	}
	if strings.Contains(schema, "tenant_id") {
		t.Fatal("archive lifecycle migration retained tenant_id")
	}
}

func TestBackupEvidenceSchemaAddsTraceAndCAS(t *testing.T) {
	data, err := os.ReadFile("../../deploy/schema/mysql/0035_flow_backup_evidence_cas.sql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(data)
	for _, required := range []string{"flow_backup_restore_evidence", "restore_test_ref", "row_version"} {
		if !strings.Contains(schema, required) {
			t.Fatalf("backup evidence migration is missing %q", required)
		}
	}
	if strings.Contains(schema, "tenant_id") {
		t.Fatal("backup evidence migration retained tenant_id")
	}
}

func TestDeletionApprovalSchemaFreezesEvidence(t *testing.T) {
	data, err := os.ReadFile("../../deploy/schema/mysql/0036_flow_deletion_approvals.sql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(data)
	for _, required := range []string{
		"flow_deletion_approvals", "kafka_coverage_json", "backup_evidence_id", "delete_approval_id", "deletion_approval_id",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("deletion approval migration is missing %q", required)
		}
	}
	if strings.Contains(schema, "tenant_id") {
		t.Fatal("deletion approval migration retained tenant_id")
	}
}

func TestRawDeleteExecutionSchemaKeepsApprovalScopedReceipts(t *testing.T) {
	data, err := os.ReadFile("../../deploy/schema/mysql/0037_flow_raw_delete_execution.sql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(data)
	for _, required := range []string{
		"deletion_approval_id CHAR(26) NOT NULL", "source_physical_record_count", "source_estimated_valid_records", "post_delete_record_count", "row_version", "uq_flow_deletion_receipt_approval",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("raw deletion execution migration is missing %q", required)
		}
	}
	if strings.Contains(schema, "tenant_id") {
		t.Fatal("raw deletion execution migration retained tenant_id")
	}
}

func TestRawDeleteBarrierSchemaFreezesWorkerACKGate(t *testing.T) {
	data, err := os.ReadFile("../../deploy/schema/mysql/0038_flow_raw_delete_barriers.sql")
	if err != nil {
		t.Fatal(err)
	}
	schema := string(data)
	for _, required := range []string{
		"flow_raw_delete_barriers", "revision", "deleted_through", "exception_days_json",
		"flow_raw_delete_barrier_acks", "worker_id", "required", "installed_at",
		"FOREIGN KEY (worker_id) REFERENCES agents(id)",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("raw-delete barrier schema is missing %q", required)
		}
	}
	if strings.Contains(schema, "tenant_id") {
		t.Fatal("raw-delete barrier schema retained tenant_id")
	}
}
