// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/cloudcache/watchdog/internal/opjob"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestFlowReclassificationCancelActivationIntegration proves the MySQL half of
// the FLOW-06B visibility boundary. Cancellation and activation lock the same
// operation/reclassification rows, so whichever commits first has an explicit,
// deterministic result. Opt-in via WATCHDOG_TEST_MYSQL_DSN.
func TestFlowReclassificationCancelActivationIntegration(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_flow_reclassification_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })

	s, err := New(Config{
		MySQL: MySQLConfig{DSN: dsn},
		Admin: AdminConfig{Username: "reclass-admin", Password: "reclass-admin-password"},
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	var adminID string
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM users WHERE username='reclass-admin'`).Scan(&adminID); err != nil {
		t.Fatalf("load admin: %v", err)
	}

	from := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Second)
	sourcePublication := seedReclassificationPublication(t, s, adminID, "snapshot-reclass-101", 101, 101, from.Add(-4*time.Hour))
	targetPublication := seedReclassificationPublication(t, s, adminID, "snapshot-reclass-102", 102, 102, from.Add(-3*time.Hour))
	evidence := flowch.ReclassificationEvidence{RecordCount: 1, RawBytes: "100", RawPackets: "2", EstimatedBytes: "1000", EstimatedPackets: "20", EstimatedValidRecords: 1}

	newRun := func(t *testing.T, suffix string, generation uint64) (flowReclassification, opjob.Job) {
		t.Helper()
		hash := strings.Repeat(suffix, 64)
		job, err := s.jobs.Enqueue(ctx, opjob.Job{
			JobType: flowReclassificationJobType, IdempotencyKey: "reclass-" + suffix,
			RequestHash: hash, ProgressTotal: 1, CreatedBy: adminID,
		})
		if err != nil {
			t.Fatalf("enqueue %s: %v", suffix, err)
		}
		run := flowReclassification{
			ID: newID(), OperationJobID: job.ID, RequestHash: hash,
			SourcePublicationID: sourcePublication, TargetPublicationID: targetPublication,
			View: "customer", WindowStart: from, WindowEnd: from.Add(time.Hour), Generation: generation,
			SourceDimensionSnapshotID: "snapshot-reclass-101", SourceClassificationVersion: 101,
			TargetDimensionSnapshotID: "snapshot-reclass-102", TargetDimensionVersion: 102,
			TargetDimensionObjectRef: "obj://snapshot-reclass-102", TargetDimensionChecksum: "sha256:" + strings.Repeat("b", 64),
			TargetClassificationVersion: 102, TargetClassificationObjectRef: "obj://classification/102", TargetClassificationChecksum: "sha256:" + strings.Repeat("c", 64),
			Expected: evidence, CreatedBy: adminID,
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO flow_reclassifications (
id,operation_job_id,request_hash,source_publication_id,target_publication_id,value_view,window_start,window_end,generation,
source_dimension_snapshot_id,source_classification_version,target_dimension_snapshot_id,target_dimension_version,target_dimension_object_ref,target_dimension_checksum,
target_classification_version,target_classification_object_ref,target_classification_checksum,authorization_json,
expected_record_count,expected_raw_bytes,expected_raw_packets,expected_estimated_bytes,expected_estimated_packets,expected_estimated_valid_records,created_by)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CAST(? AS JSON),?,?,?,?,?,?,?)`,
			run.ID, run.OperationJobID, run.RequestHash, run.SourcePublicationID, run.TargetPublicationID, run.View, run.WindowStart, run.WindowEnd, run.Generation,
			run.SourceDimensionSnapshotID, run.SourceClassificationVersion, run.TargetDimensionSnapshotID, run.TargetDimensionVersion, run.TargetDimensionObjectRef, run.TargetDimensionChecksum,
			run.TargetClassificationVersion, run.TargetClassificationObjectRef, run.TargetClassificationChecksum, `{}`,
			evidence.RecordCount, evidence.RawBytes, evidence.RawPackets, evidence.EstimatedBytes, evidence.EstimatedPackets, evidence.EstimatedValidRecords, adminID); err != nil {
			t.Fatalf("insert run %s: %v", suffix, err)
		}
		return run, job
	}

	t.Run("cancel committed first prevents activation", func(t *testing.T) {
		run, _ := newRun(t, "a", 1)
		leased, err := s.jobs.LeaseNext(ctx, flowReclassificationJobType, "test-worker-a", time.Minute)
		if err != nil {
			t.Fatalf("lease: %v", err)
		}
		if err := s.flowLifecycle.EnsureNoActiveReclassificationForDay(ctx, from); !errors.Is(err, flowlifecycle.ErrDeleteLocked) {
			t.Fatalf("raw deletion guard before cancel = %v", err)
		}
		activated, err := s.requestFlowReclassificationCancel(ctx, run.ID)
		if err != nil || activated {
			t.Fatalf("cancel = activated %v err %v", activated, err)
		}
		if err := s.activateFlowReclassification(ctx, run, leased.LeaseToken, evidence); !errors.Is(err, opjob.ErrCancelRequested) {
			t.Fatalf("activate after cancel = %v, want ErrCancelRequested", err)
		}
		stored, err := s.readFlowReclassification(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.ActivatedAt.Valid || stored.Job.Status != opjob.StatusCancelRequested {
			t.Fatalf("stored activation=%v status=%s", stored.ActivatedAt.Valid, stored.Job.Status)
		}
		if err := s.jobs.CompleteCanceled(ctx, leased.ID, leased.LeaseToken); err != nil {
			t.Fatalf("complete canceled: %v", err)
		}
		if err := s.flowLifecycle.EnsureNoActiveReclassificationForDay(ctx, from); err != nil {
			t.Fatalf("raw deletion guard after cancel = %v", err)
		}
	})

	t.Run("activation committed first rejects cancellation", func(t *testing.T) {
		run, _ := newRun(t, "d", 2)
		leased, err := s.jobs.LeaseNext(ctx, flowReclassificationJobType, "test-worker-b", time.Minute)
		if err != nil {
			t.Fatalf("lease: %v", err)
		}
		if err := s.activateFlowReclassification(ctx, run, leased.LeaseToken, evidence); err != nil {
			t.Fatalf("activate: %v", err)
		}
		activated, err := s.requestFlowReclassificationCancel(ctx, run.ID)
		if err != nil || !activated {
			t.Fatalf("cancel after activation = activated %v err %v", activated, err)
		}
		stored, err := s.readFlowReclassification(ctx, run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !stored.ActivatedAt.Valid || stored.Output == nil || !stored.Output.Equal(evidence) {
			t.Fatalf("stored activation/output = %v %+v", stored.ActivatedAt.Valid, stored.Output)
		}
		if err := s.flowLifecycle.EnsureNoActiveReclassificationForDay(ctx, from); err != nil {
			t.Fatalf("activated generation must release raw deletion guard: %v", err)
		}
	})
}

func seedReclassificationPublication(t *testing.T, s *Server, adminID, snapshotID string, version, classificationVersion uint64, effective time.Time) string {
	t.Helper()
	digest := "sha256:" + strings.Repeat("a", 64)
	if _, err := s.db.Exec(`INSERT INTO dimension_snapshots
(id,module_key,dimension_key,version,effective_from,object_ref,object_format,object_format_version,builder_version,build_job_id,checksum,draft_digest,source_manifest_version,source_manifest,bundle_schema_version,status,approval_state,created_by)
VALUES (?,'flow','address',?,? ,?,'wads',1,'reclassification-test',?,?,?,0,'[]',1,'active','approved',?)`,
		snapshotID, version, effective.Add(-time.Hour), "obj://"+snapshotID, newID(), digest, digest, adminID); err != nil {
		t.Fatalf("seed dimension snapshot: %v", err)
	}
	publicationID := newID()
	if _, err := s.db.Exec(`INSERT INTO flow_enrichment_publications
(id,pair_schema_version,classification_version,effective_from,profile_row_version,dimension_snapshot_id,dimension_version,dimension_effective_from,
dimension_object_ref,dimension_object_format,dimension_object_format_version,dimension_checksum,classification_schema_version,classification_object_ref,
classification_checksum,signature_algorithm,signing_key_id,signature,signed_at,created_by)
VALUES (?,1,?,?,1,?,?,?,?,'wads',1,?,1,?,?,'ed25519','reclassification-test',?,?,?)`,
		publicationID, classificationVersion, effective, snapshotID, version, effective.Add(-time.Hour), "obj://"+snapshotID, digest,
		"obj://classification/"+publicationID, "sha256:"+strings.Repeat("c", 64), bytes.Repeat([]byte{1}, 64), effective, adminID); err != nil {
		t.Fatalf("seed publication: %v", err)
	}
	return publicationID
}
