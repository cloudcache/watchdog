// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

// TestExportsGenericLifecycle exercises the DB-coupled generic /api/v1/exports/*
// surface across both job types: a cross-type list (scoped to the caller), retry
// (re-enqueues the frozen spec), and delete (cancels + drops the row). The pure
// merge/dispatch pieces are covered DB-free in exports_test.go; this asserts the
// parts that only a real opjob store can prove. Gated on WATCHDOG_TEST_MYSQL_DSN.
func TestExportsGenericLifecycle(t *testing.T) {
	dsn := isolatedMySQLDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}
	if err := EnsureRBACSeed(ctx, db); err != nil {
		t.Fatal(err)
	}

	userID := newID()
	cleanup := func() {
		_, _ = db.Exec(`DELETE FROM operation_jobs WHERE created_by=?`, userID)
		_, _ = db.Exec(`DELETE FROM users WHERE id=?`, userID)
	}
	cleanup()
	t.Cleanup(cleanup)
	if _, err := db.Exec(`INSERT INTO users (id,username,status) VALUES (?,?,'active')`, userID, "exports-"+userID); err != nil {
		t.Fatal(err)
	}

	s := &Server{db: db, jobs: opjob.NewStore(db), cfg: Config{SNMP: SNMPConfig{ExportRetention: time.Hour}}}
	owner := &principal{UserID: userID, Abilities: map[string]bool{"device.view": true}}

	snmpCheckpoint, _ := opjob.EncodePayload(snmpCSVExportPayloadSchema, snmpCSVExportPayload{
		Metric: "if_in_bps", Aggregate: "sum", FromMS: 1, ToMS: 2, StepSeconds: 300,
	})
	snmpJob, err := s.jobs.Enqueue(ctx, opjob.Job{JobType: snmpCSVExportJobType, IdempotencyKey: newID(),
		RequestHash: sha256hex(string(snmpCheckpoint)), CheckpointJSON: snmpCheckpoint, CreatedBy: userID})
	if err != nil {
		t.Fatal(err)
	}
	flowCheckpoint, _ := opjob.EncodePayload(flowExportPayloadSchema, flowExportPayload{
		Kind: flowExportKindReport, Format: "csv", View: flowquery.ViewCustomer,
	})
	flowJob, err := s.jobs.Enqueue(ctx, opjob.Job{JobType: flowExportJobType, IdempotencyKey: newID(),
		RequestHash: sha256hex(string(flowCheckpoint)), CheckpointJSON: flowCheckpoint, CreatedBy: userID})
	if err != nil {
		t.Fatal(err)
	}

	// Cross-type list, scoped to the owner: both job types appear under one page.
	list := directExportRequest(t, s.listExports, owner, http.MethodGet, "/api/v1/exports?limit=25&offset=0")
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"total":2`) ||
		!strings.Contains(list.Body.String(), snmpJob.ID) || !strings.Contains(list.Body.String(), flowJob.ID) {
		t.Fatalf("list exports: status=%d body=%s", list.Code, list.Body.String())
	}

	// Detail dispatches to the flow view (kind survives the round-trip).
	got := directExportRequest(t, s.getExport, owner, http.MethodGet, "/api/v1/exports/"+flowJob.ID)
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"kind":"report"`) {
		t.Fatalf("get flow export: status=%d body=%s", got.Code, got.Body.String())
	}

	// Retry re-enqueues a second flow job from the same frozen spec.
	retry := directExportRequest(t, s.retryExport, owner, http.MethodPost, "/api/v1/exports/"+flowJob.ID+"/retry")
	if retry.Code != http.StatusAccepted {
		t.Fatalf("retry: status=%d body=%s", retry.Code, retry.Body.String())
	}
	if n, err := s.jobs.Count(ctx, opjob.Filter{JobType: flowExportJobType, CreatedBy: userID}); err != nil || n != 2 {
		t.Fatalf("flow job count after retry = %d (err=%v), want 2", n, err)
	}

	// Delete cancels + drops the row; the job is then gone from the store and the list.
	del := directExportRequest(t, s.deleteExport, owner, http.MethodDelete, "/api/v1/exports/"+snmpJob.ID)
	if del.Code != http.StatusOK {
		t.Fatalf("delete: status=%d body=%s", del.Code, del.Body.String())
	}
	if _, err := s.jobs.Get(ctx, snmpJob.ID); err == nil {
		t.Fatalf("snmp job still present after delete")
	}
	list2 := directExportRequest(t, s.listExports, owner, http.MethodGet, "/api/v1/exports?limit=25&offset=0")
	if !strings.Contains(list2.Body.String(), `"total":2`) || strings.Contains(list2.Body.String(), snmpJob.ID) {
		t.Fatalf("list after delete: body=%s", list2.Body.String())
	}

	// Another user sees none of these jobs (created_by scoping).
	other := directExportRequest(t, s.listExports, &principal{UserID: newID()}, http.MethodGet, "/api/v1/exports")
	if !strings.Contains(other.Body.String(), `"total":0`) {
		t.Fatalf("cross-owner list: body=%s", other.Body.String())
	}
}

// directExportRequest drives a generic-export handler with a synthetic gin context,
// extracting the {id} path param from /exports/<id>[/...]. Kept local so this file
// does not depend on the parallel session's SNMP test helpers.
func directExportRequest(t *testing.T, handler gin.HandlerFunc, p *principal, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, bytes.NewReader(nil))
	c.Set(principalKey, p)
	if idx := strings.Index(target, "/exports/"); idx >= 0 {
		rest := strings.TrimPrefix(target[idx:], "/exports/")
		id := strings.SplitN(strings.SplitN(rest, "?", 2)[0], "/", 2)[0]
		c.Params = gin.Params{{Key: "id", Value: id}}
	}
	handler(c)
	return recorder
}
