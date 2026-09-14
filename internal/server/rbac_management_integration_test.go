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
	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestRBACManagementTransactionsAndSeedPersistence(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_rbac_management_it"
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
	if err := EnsureRBACSeed(ctx, db); err != nil {
		t.Fatal(err)
	}

	adminID := newID()
	if _, err := db.Exec(`INSERT INTO users (id,username,status) VALUES (?,?,'active')`, adminID, "rbac-admin"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO user_roles (user_id,role_id) SELECT ?,id FROM roles WHERE name='administrator'`, adminID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE watchdog_installation SET admin_bootstrapped=1 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	s := &Server{db: db}
	p := &principal{UserID: adminID, IsAdmin: true, Abilities: map[string]bool{}}

	invalidRole := rbacHandlerRequest(t, s.createRole, p, http.MethodPost, "/api/v1/roles", "", map[string]any{
		"name": "invalid-role", "permissions": []string{"not.a.real.ability"},
	})
	if invalidRole.Code != http.StatusBadRequest {
		t.Fatalf("invalid role status=%d body=%s", invalidRole.Code, invalidRole.Body.String())
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM roles WHERE name='invalid-role'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid role was not rolled back: count=%d err=%v", count, err)
	}

	invalidUser := rbacHandlerRequest(t, s.createUser, p, http.MethodPost, "/api/v1/users", "", map[string]any{
		"username": "invalid-user", "password": "long-enough-password", "roles": []string{"missing-role"},
	})
	if invalidUser.Code != http.StatusBadRequest {
		t.Fatalf("invalid user status=%d body=%s", invalidUser.Code, invalidUser.Body.String())
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE username='invalid-user'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid user was not rolled back: count=%d err=%v", count, err)
	}

	viewerID := ""
	if err := db.QueryRow(`SELECT id FROM roles WHERE name='viewer'`).Scan(&viewerID); err != nil {
		t.Fatal(err)
	}
	permissions := []string{"device.view"}
	title := "Restricted viewer"
	updated := rbacHandlerRequest(t, s.updateRole, p, http.MethodPatch, "/api/v1/roles/"+viewerID, viewerID, map[string]any{
		"title": title, "permissions": permissions,
	})
	if updated.Code != http.StatusOK {
		t.Fatalf("update viewer status=%d body=%s", updated.Code, updated.Body.String())
	}
	if _, err := db.Exec(`DELETE FROM roles WHERE name='operator'`); err != nil {
		t.Fatal(err)
	}
	if err := EnsureRBACSeed(ctx, db); err != nil {
		t.Fatal(err)
	}
	var storedTitle string
	if err := db.QueryRow(`SELECT title FROM roles WHERE id=?`, viewerID).Scan(&storedTitle); err != nil || storedTitle != title {
		t.Fatalf("editable role was overwritten by startup seed: title=%q err=%v", storedTitle, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM roles WHERE name='operator'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("deleted install-time role was recreated: count=%d err=%v", count, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM role_permissions rp JOIN permissions p ON p.id=rp.permission_id WHERE rp.role_id=?`, viewerID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("edited permission set was overwritten: count=%d err=%v", count, err)
	}

	administratorID := ""
	if err := db.QueryRow(`SELECT id FROM roles WHERE name='administrator'`).Scan(&administratorID); err != nil {
		t.Fatal(err)
	}
	protectedUpdate := rbacHandlerRequest(t, s.updateRole, p, http.MethodPatch, "/api/v1/roles/"+administratorID, administratorID, map[string]any{
		"title": "Renamed administrator",
	})
	if protectedUpdate.Code != http.StatusForbidden || !strings.Contains(protectedUpdate.Body.String(), "protected") {
		t.Fatalf("protected role update status=%d body=%s", protectedUpdate.Code, protectedUpdate.Body.String())
	}

	lastAdmin := rbacHandlerRequest(t, s.updateUser, p, http.MethodPatch, "/api/v1/users/"+adminID, adminID, map[string]any{"roles": []string{}})
	if lastAdmin.Code != http.StatusConflict || !strings.Contains(lastAdmin.Body.String(), "last_administrator") {
		t.Fatalf("last administrator removal status=%d body=%s", lastAdmin.Code, lastAdmin.Body.String())
	}
}

func rbacHandlerRequest(t *testing.T, handler gin.HandlerFunc, p *principal, method, target, id string, body any) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, bytes.NewReader(data))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(principalKey, p)
	if id != "" {
		c.Params = gin.Params{{Key: "id", Value: id}}
	}
	handler(c)
	return recorder
}
