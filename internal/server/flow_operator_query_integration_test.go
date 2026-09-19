// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestResolveFlowOperatorQueryBinding exercises the operator-binding resolver
// against a disposable real MySQL database. It proves that queries pin the signed
// AddressSnap + classification pair timeline and fail closed until every active
// flow worker has reached the installed milestone for every required pair.
func TestResolveFlowOperatorQueryBinding(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_flow_operator_it"
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	root := t.TempDir()
	s, err := New(Config{
		MySQL: MySQLConfig{DSN: parsed.FormatDSN()},
		Admin: AdminConfig{Username: "flow-operator-admin", Password: "flow-operator-password"},
		Address: AddressConfig{
			ArtifactDir: filepath.Join(root, "artifacts"),
			SnapshotDir: filepath.Join(root, "snapshots"),
		},
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer s.Close()
	db := s.db
	ctx := context.Background()
	from := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)

	reset := func() {
		if _, err := db.Exec(`DELETE FROM flow_enrichment_publication_acks`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`DELETE FROM flow_enrichment_publication_targets`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`DELETE FROM flow_enrichment_publications`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`DELETE FROM dimension_snapshot_activations`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`DELETE FROM dimension_snapshots`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`DELETE FROM agents WHERE kind='flow_worker'`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`DELETE FROM isp_operators WHERE code LIKE 'op-test-%'`); err != nil {
			t.Fatal(err)
		}
	}
	reset()

	seedSnapshot := func(t *testing.T, id string, version int, effective time.Time) {
		t.Helper()
		digest := "sha256:" + strings.Repeat("a", 64)
		if _, err := db.Exec(`INSERT INTO dimension_snapshots
			(id,module_key,dimension_key,version,effective_from,object_ref,object_format,object_format_version,
			 builder_version,build_job_id,checksum,draft_digest,source_manifest_version,source_manifest,
			 bundle_schema_version,status,approval_state)
			VALUES (?,'flow','address',?,? ,?,'wads',1,'operator-test',?,?,?,0,'[]',1,'active','approved')`,
			id, version, effective.UTC(), "obj://"+id, newID(), digest, digest); err != nil {
			t.Fatal(err)
		}
	}
	seedPublication := func(t *testing.T, id, snapshotID string, classificationVersion uint32, effective time.Time) {
		t.Helper()
		dimensionChecksum := "sha256:" + strings.Repeat("a", 64)
		classificationChecksum := "sha256:" + strings.Repeat("b", 64)
		if _, err := db.Exec(`INSERT INTO flow_enrichment_publications
			(id,pair_schema_version,classification_version,effective_from,profile_row_version,
			 dimension_snapshot_id,dimension_version,dimension_effective_from,dimension_object_ref,
			 dimension_object_format,dimension_object_format_version,dimension_checksum,
			 classification_schema_version,classification_object_ref,classification_checksum,
			 signature_algorithm,signing_key_id,signature,signed_at)
			VALUES (?,1,?,?,1,?,1,?,?,'wads',1,?,1,?,?, 'ed25519','operator-test-key',?,?)`,
			id, classificationVersion, effective.UTC(), snapshotID, effective.Add(-time.Hour).UTC(),
			"obj://"+snapshotID, dimensionChecksum, "obj://classification/"+id, classificationChecksum,
			bytes.Repeat([]byte{1}, 64), effective.UTC()); err != nil {
			t.Fatal(err)
		}
	}
	seedWorker := func(t *testing.T) string {
		t.Helper()
		id := newID()
		if _, err := db.Exec(`INSERT INTO agents (id,kind,status,capabilities_json) VALUES (?,'flow_worker','active','[]')`, id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	seedInstalledACK := func(t *testing.T, publicationID, workerID string) {
		t.Helper()
		now := time.Now().UTC()
		if _, err := db.Exec(`INSERT INTO flow_enrichment_publication_acks
			(publication_id,worker_id,boot_id,software_version,state,attempted_at,downloaded_at,installed_at)
			VALUES (?,?,?,'v1','installed',?,?,?)`, publicationID, workerID, newID(), now, now, now); err != nil {
			t.Fatal(err)
		}
	}
	seedTarget := func(t *testing.T, publicationID, workerID string) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO flow_enrichment_publication_targets (publication_id,worker_id) VALUES (?,?)`, publicationID, workerID); err != nil {
			t.Fatal(err)
		}
	}
	markACKFailedAfterInstall := func(t *testing.T, publicationID, workerID string) {
		t.Helper()
		if _, err := db.Exec(`UPDATE flow_enrichment_publication_acks
			SET state='failed',attempted_at=?,error_code='refresh_failed',error_message='later refresh failed'
			WHERE publication_id=? AND worker_id=?`, time.Now().UTC(), publicationID, workerID); err != nil {
			t.Fatal(err)
		}
	}
	seedOperator := func(t *testing.T, code string, flowISPID int, enabled bool) string {
		t.Helper()
		id := newID()
		if _, err := db.Exec(`INSERT INTO isp_operators (id,code,name,category,flow_isp_id,asns,enabled)
			VALUES (?,?,?,'other',?,'[]',?)`, id, code, code, flowISPID, enabled); err != nil {
			t.Fatal(err)
		}
		return id
	}

	t.Run("happy path pins publication snapshot classification and isp", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-a", 42, true)
		snapshot := newID()
		publication := newID()
		seedSnapshot(t, snapshot, 1, from.Add(-48*time.Hour))
		seedPublication(t, publication, snapshot, 7, from.Add(-24*time.Hour))
		worker := seedWorker(t)
		seedTarget(t, publication, worker)
		seedInstalledACK(t, publication, worker)

		binding, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if binding.FlowISPID != 42 || binding.ExpectedWorkers != 1 ||
			len(binding.PublicationIDs) != 1 || binding.PublicationIDs[0] != publication ||
			len(binding.DimensionSnapshotIDs) != 1 || binding.DimensionSnapshotIDs[0] != snapshot ||
			len(binding.ClassificationVersions) != 1 || binding.ClassificationVersions[0] != 7 {
			t.Fatalf("binding = %+v", binding)
		}

		recorder := httptest.NewRecorder()
		ginContext, _ := gin.CreateTestContext(recorder)
		ginContext.Request = httptest.NewRequest("POST", "/api/v1/flow/query", nil).WithContext(ctx)
		selection := &flowOperatorSelection{OperatorID: op}
		filters := flowquery.Filters{}
		var filter *flowquery.FilterExpression
		if !s.applyFlowOperatorSelection(ginContext, selection, flowquery.ViewCustomer, from, to, &filters, &filter) {
			t.Fatalf("apply selection failed: status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		if selection.SchemaVersion != 1 || selection.FlowISPID != 42 ||
			len(selection.PublicationIDs) != 1 || selection.PublicationIDs[0] != publication ||
			len(filters.DimensionSnapshotIDs) != 1 || filters.DimensionSnapshotIDs[0] != snapshot ||
			len(filters.ClassificationVersions) != 1 || filters.ClassificationVersions[0] != 7 ||
			filter == nil || !flowFilterReferencesISP(filter) {
			t.Fatalf("prepared selection=%+v filters=%+v filter=%+v", selection, filters, filter)
		}

		var adminID string
		if err := db.QueryRow(`SELECT id FROM users WHERE username='flow-operator-admin'`).Scan(&adminID); err != nil {
			t.Fatal(err)
		}
		s.auditFlowQuery(ctx, adminID, "flow.query", "flow_query", "estimated_bps", selection)
		var detail string
		if err := db.QueryRow(`SELECT CAST(detail_json AS CHAR) FROM audit_logs
			WHERE actor_id=? AND action='flow.query' ORDER BY occurred_at DESC,id DESC LIMIT 1`, adminID).Scan(&detail); err != nil {
			t.Fatal(err)
		}
		var audited struct {
			OperatorSelection flowOperatorSelection `json:"operator_selection"`
		}
		if err := json.Unmarshal([]byte(detail), &audited); err != nil ||
			len(audited.OperatorSelection.PublicationIDs) != 1 || audited.OperatorSelection.PublicationIDs[0] != publication ||
			len(audited.OperatorSelection.ClassificationVersions) != 1 || audited.OperatorSelection.ClassificationVersions[0] != 7 {
			t.Fatalf("operator query audit provenance = %s", detail)
		}
	})

	t.Run("two effective pairs cover a crossed range", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-b", 43, true)
		snapshotA, snapshotB := newID(), newID()
		publicationA, publicationB := newID(), newID()
		seedSnapshot(t, snapshotA, 1, from.Add(-48*time.Hour))
		seedSnapshot(t, snapshotB, 2, from.Add(-47*time.Hour))
		seedPublication(t, publicationA, snapshotA, 8, from.Add(-time.Hour))
		seedPublication(t, publicationB, snapshotB, 9, from.Add(30*time.Minute))
		worker := seedWorker(t)
		seedTarget(t, publicationA, worker)
		seedTarget(t, publicationB, worker)
		seedInstalledACK(t, publicationA, worker)
		seedInstalledACK(t, publicationB, worker)

		binding, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if len(binding.PublicationIDs) != 2 || binding.PublicationIDs[0] != publicationA || binding.PublicationIDs[1] != publicationB {
			t.Fatalf("publication timeline = %+v", binding.PublicationIDs)
		}
		if len(binding.ClassificationVersions) != 2 || binding.ClassificationVersions[0] != 8 || binding.ClassificationVersions[1] != 9 {
			t.Fatalf("classification versions = %+v", binding.ClassificationVersions)
		}
	})

	t.Run("disabled and unknown operators are invalid", func(t *testing.T) {
		reset()
		disabled := seedOperator(t, "op-test-c", 7, false)
		if _, err := resolveFlowOperatorQueryBinding(ctx, db, disabled, from, to); !errors.Is(err, errFlowOperatorInvalid) {
			t.Fatalf("disabled operator err = %v", err)
		}
		if _, err := resolveFlowOperatorQueryBinding(ctx, db, newID(), from, to); !errors.Is(err, errFlowOperatorInvalid) {
			t.Fatalf("unknown operator err = %v", err)
		}
	})

	t.Run("coverage gap at range start is unavailable", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-d", 42, true)
		snapshot, publication := newID(), newID()
		seedSnapshot(t, snapshot, 1, from.Add(-48*time.Hour))
		seedPublication(t, publication, snapshot, 10, from.Add(30*time.Minute))
		worker := seedWorker(t)
		seedTarget(t, publication, worker)
		seedInstalledACK(t, publication, worker)
		if _, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to); !errors.Is(err, errFlowOperatorUnavailable) {
			t.Fatalf("coverage gap err = %v", err)
		}
	})

	t.Run("publication not installed on every targeted worker is unavailable", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-e", 42, true)
		snapshot, publication := newID(), newID()
		seedSnapshot(t, snapshot, 1, from.Add(-48*time.Hour))
		seedPublication(t, publication, snapshot, 11, from.Add(-time.Hour))
		installed := seedWorker(t)
		missing := seedWorker(t)
		seedTarget(t, publication, installed)
		seedTarget(t, publication, missing)
		seedInstalledACK(t, publication, installed)
		if _, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to); !errors.Is(err, errFlowOperatorUnavailable) {
			t.Fatalf("incomplete rollout err = %v", err)
		}
	})

	t.Run("installed milestone survives a later failed attempt", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-f", 42, true)
		snapshot, publication := newID(), newID()
		seedSnapshot(t, snapshot, 1, from.Add(-48*time.Hour))
		seedPublication(t, publication, snapshot, 12, from.Add(-time.Hour))
		worker := seedWorker(t)
		seedTarget(t, publication, worker)
		seedInstalledACK(t, publication, worker)
		markACKFailedAfterInstall(t, publication, worker)
		if _, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to); err != nil {
			t.Fatalf("installed milestone was lost after failed refresh: %v", err)
		}
	})

	t.Run("publication without a target worker is unavailable", func(t *testing.T) {
		reset()
		op := seedOperator(t, "op-test-g", 42, true)
		snapshot, publication := newID(), newID()
		seedSnapshot(t, snapshot, 1, from.Add(-48*time.Hour))
		seedPublication(t, publication, snapshot, 13, from.Add(-time.Hour))
		if _, err := resolveFlowOperatorQueryBinding(ctx, db, op, from, to); !errors.Is(err, errFlowOperatorUnavailable) {
			t.Fatalf("no workers err = %v", err)
		}
	})
}
