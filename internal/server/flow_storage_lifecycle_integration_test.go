package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestFlowStoragePolicyLifecycleIntegration(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_flow_lifecycle_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	if err := ensureDatabase(dsn); err != nil {
		t.Fatal(err)
	}
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
	userID := newID()
	if _, err := db.Exec(`INSERT INTO users (id,username,status) VALUES (?,?,'active')`, userID, "flow-lifecycle-admin"); err != nil {
		t.Fatal(err)
	}
	s := &Server{db: db}
	admin := &principal{UserID: userID, Username: "flow-lifecycle-admin", IsAdmin: true}

	input := map[string]any{
		"bootstrap_from": "2026-01-01", "raw_retention_seconds": 86400,
		"archive_retention_seconds": 2592000, "late_arrival_seconds": 3600,
		"delete_grace_seconds": 3600, "max_partitions_per_run": 7,
		"require_backup_before_delete": true,
	}
	created := flowLifecycleRequest(t, s.createFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies", "", "", input, "")
	if created.Code != http.StatusCreated || created.Header().Get("ETag") != `"1"` {
		t.Fatalf("create: status=%d etag=%q body=%s", created.Code, created.Header().Get("ETag"), created.Body.String())
	}
	var first flowlifecycle.Policy
	if err := json.Unmarshal(created.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	input["late_arrival_seconds"] = 7200
	updated := flowLifecycleRequest(t, s.updateFlowRetentionPolicy, admin, http.MethodPatch, "/api/v1/flow/storage/policies/"+first.ID, "id", first.ID, input, `"1"`)
	if updated.Code != http.StatusOK || updated.Header().Get("ETag") != `"2"` {
		t.Fatalf("update: status=%d etag=%q body=%s", updated.Code, updated.Header().Get("ETag"), updated.Body.String())
	}
	stale := flowLifecycleRequest(t, s.updateFlowRetentionPolicy, admin, http.MethodPatch, "/api/v1/flow/storage/policies/"+first.ID, "id", first.ID, input, `"1"`)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale update: status=%d body=%s", stale.Code, stale.Body.String())
	}
	published := flowLifecycleRequest(t, s.publishFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies/"+first.ID+"/actions/publish", "id", first.ID, nil, `"2"`)
	if published.Code != http.StatusOK || !strings.Contains(published.Body.String(), `"status":"published"`) {
		t.Fatalf("publish: status=%d body=%s", published.Code, published.Body.String())
	}

	unsafeInput := cloneFlowLifecycleMap(input)
	unsafeInput["raw_delete_enabled"] = true
	unsafe := flowLifecycleRequest(t, s.createFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies", "", "", unsafeInput, "")
	if unsafe.Code != http.StatusConflict || !strings.Contains(unsafe.Body.String(), "flow_deletion_locked") {
		t.Fatalf("unsafe create: status=%d body=%s", unsafe.Code, unsafe.Body.String())
	}

	secondCreated := flowLifecycleRequest(t, s.createFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies", "", "", input, "")
	var second flowlifecycle.Policy
	if secondCreated.Code != http.StatusCreated || json.Unmarshal(secondCreated.Body.Bytes(), &second) != nil {
		t.Fatalf("second create: status=%d body=%s", secondCreated.Code, secondCreated.Body.String())
	}
	secondPublished := flowLifecycleRequest(t, s.publishFlowRetentionPolicy, admin, http.MethodPost, "/api/v1/flow/storage/policies/"+second.ID+"/actions/publish", "id", second.ID, nil, `"1"`)
	if secondPublished.Code != http.StatusOK {
		t.Fatalf("second publish: status=%d body=%s", secondPublished.Code, secondPublished.Body.String())
	}
	var activeCount, retiredCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_retention_policy_revisions WHERE status='published'`).Scan(&activeCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM flow_retention_policy_revisions WHERE status='retired'`).Scan(&retiredCount); err != nil {
		t.Fatal(err)
	}
	if activeCount != 1 || retiredCount != 1 {
		t.Fatalf("published=%d retired=%d", activeCount, retiredCount)
	}

	listed := flowLifecycleRequest(t, s.listFlowRetentionPolicies, admin, http.MethodGet, "/api/v1/flow/storage/policies?sort=version&order=desc&status=published", "", "", nil, "")
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"total":1`) || !strings.Contains(listed.Body.String(), second.ID) {
		t.Fatalf("list: status=%d body=%s", listed.Code, listed.Body.String())
	}
	for name, test := range map[string]struct {
		handler gin.HandlerFunc
		path    string
	}{
		"partitions": {s.listFlowRetentionPartitions, "/api/v1/flow/storage/partitions"},
		"watermarks": {s.listFlowReconciliationWatermarks, "/api/v1/flow/storage/watermarks"},
		"receipts":   {s.listFlowDeletionReceipts, "/api/v1/flow/storage/deletion-receipts"},
	} {
		t.Run(name, func(t *testing.T) {
			response := flowLifecycleRequest(t, test.handler, admin, http.MethodGet, test.path, "", "", nil, "")
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"items":[]`) {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	var auditCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_logs WHERE actor_id=? AND resource='flow_retention_policy'`, userID).Scan(&auditCount); err != nil || auditCount != 5 {
		t.Fatalf("audit count=%d error=%v", auditCount, err)
	}
}

func flowLifecycleRequest(t *testing.T, handler gin.HandlerFunc, principal *principal, method, target, paramName, paramValue string, body any, ifMatch string) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, bytes.NewReader(encoded))
	c.Request.Header.Set("Content-Type", "application/json")
	if ifMatch != "" {
		c.Request.Header.Set("If-Match", ifMatch)
	}
	c.Set(principalKey, principal)
	if paramName != "" {
		c.Params = gin.Params{{Key: paramName, Value: paramValue}}
	}
	handler(c)
	return recorder
}

func cloneFlowLifecycleMap(source map[string]any) map[string]any {
	copy := make(map[string]any, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}
