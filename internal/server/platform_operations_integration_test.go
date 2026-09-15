package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestPlatformRetentionAndAuditIntegration(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_platform_operations_it"
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
	userID, deviceID := newID(), newID()
	if _, err := db.Exec(`INSERT INTO users (id,username,status) VALUES (?,?,'active')`, userID, "audit-operator"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO devices (id,host,kind,sys_name) VALUES (?,?,'network',?)`, deviceID, "192.0.2.10", "edge-retention"); err != nil {
		t.Fatal(err)
	}
	s := &Server{db: db}
	principal := &principal{UserID: userID, IsAdmin: true}

	global := snmpManagementRequest(t, s.putRetentionPolicy, principal, http.MethodPut, "/api/v1/retention/policies", "", "", map[string]any{
		"HighPrecisionDays": 400, "ManualCleanupEnabled": true, "Notes": "global",
	})
	if global.Code != http.StatusOK {
		t.Fatalf("put global retention: status=%d body=%s", global.Code, global.Body.String())
	}
	var globalPolicy metricRetentionPolicy
	if err := json.Unmarshal(global.Body.Bytes(), &globalPolicy); err != nil || globalPolicy.ID == "" {
		t.Fatalf("decode global retention: value=%+v err=%v", globalPolicy, err)
	}
	device := snmpManagementRequest(t, s.putRetentionPolicy, principal, http.MethodPut, "/api/v1/retention/policies", "", "", map[string]any{
		"TargetID": deviceID, "HighPrecisionDays": 90, "ManualCleanupEnabled": false,
	})
	if device.Code != http.StatusOK {
		t.Fatalf("put device retention: status=%d body=%s", device.Code, device.Body.String())
	}
	list := snmpManagementRequest(t, s.listRetentionPolicies, principal, http.MethodGet, "/api/v1/retention/policies", "", "", nil)
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"HighPrecisionDays":400`) || !strings.Contains(list.Body.String(), deviceID) {
		t.Fatalf("list retention: status=%d body=%s", list.Code, list.Body.String())
	}

	audit := snmpManagementRequest(t, s.listAuditLogs, principal, http.MethodGet, "/api/v1/audit-logs", "", "", nil)
	if audit.Code != http.StatusOK || !strings.Contains(audit.Body.String(), `"actor_username":"audit-operator"`) {
		t.Fatalf("audit actor username: status=%d body=%s", audit.Code, audit.Body.String())
	}

	deleted := snmpManagementRequest(t, s.deleteRetentionPolicy, principal, http.MethodDelete, "/api/v1/retention/policies/"+string(globalPolicy.ID), "policy_id", string(globalPolicy.ID), nil)
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete global retention: status=%d body=%s", deleted.Code, deleted.Body.String())
	}
}
