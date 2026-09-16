package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/metricdomain"
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

	preferences := rbacHandlerRequest(t, s.putUserPreferences, p, http.MethodPut, "/api/v1/me/preferences", "", map[string]any{
		"chartTime": "6h", "layoutWidth": 1600,
	})
	if preferences.Code != http.StatusOK || !strings.Contains(preferences.Body.String(), `"row_version":1`) {
		t.Fatalf("create user preferences: status=%d body=%s", preferences.Code, preferences.Body.String())
	}
	loadedPreferences := rbacHandlerRequest(t, s.getUserPreferences, p, http.MethodGet, "/api/v1/me/preferences", "", nil)
	if loadedPreferences.Code != http.StatusOK || !strings.Contains(loadedPreferences.Body.String(), `"chartTime":"6h"`) {
		t.Fatalf("load user preferences: status=%d body=%s", loadedPreferences.Code, loadedPreferences.Body.String())
	}
	if _, version, err := s.upsertUserPreferences(ctx, adminID, []byte(`{"chartTime":"12h"}`), 1); err != nil || version != 2 {
		t.Fatalf("update user preferences: version=%d err=%v", version, err)
	}
	if _, _, err := s.upsertUserPreferences(ctx, adminID, []byte(`{"chartTime":"24h"}`), 1); !errors.Is(err, errUserPreferencesConflict) {
		t.Fatalf("stale user preferences update: err=%v", err)
	}

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

	deviceID, groupID, portID, accountID, graphID := newID(), newID(), newID(), newID(), "aggr-rbac-create"
	otherDeviceID, otherPortID := newID(), newID()
	if _, err := db.Exec(`INSERT INTO devices (id,host) VALUES (?,?)`, deviceID, "rbac-router.example.test"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO devices (id,host) VALUES (?,?)`, otherDeviceID, "other-router.example.test"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO device_groups (id,name) VALUES (?,?)`, groupID, "RBAC routers"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ports (id,device_id,if_index,if_name) VALUES (?,?,?,?)`, portID, deviceID, 7, "xe-0/0/7"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ports (id,device_id,if_index,if_name) VALUES (?,?,?,?)`, otherPortID, otherDeviceID, 8, "xe-0/0/8"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO billing_accounts (id,name) VALUES (?,?)`, accountID, "RBAC transit"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO aggregate_graphs (id,name) VALUES (?,?)`, graphID, "RBAC aggregate"); err != nil {
		t.Fatal(err)
	}
	invalidAccessUser := rbacHandlerRequest(t, s.createUser, p, http.MethodPost, "/api/v1/users", "", map[string]any{
		"username": "invalid-access", "email": "invalid-access@example.test", "password": "long-enough-password",
		"roles": []string{"viewer"}, "access": map[string]any{"device_ids": []string{"missing-device"}},
	})
	if invalidAccessUser.Code != http.StatusBadRequest {
		t.Fatalf("invalid access user status=%d body=%s", invalidAccessUser.Code, invalidAccessUser.Body.String())
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM users WHERE username='invalid-access'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("user with invalid access was not rolled back: count=%d err=%v", count, err)
	}

	createdUser := rbacHandlerRequest(t, s.createUser, p, http.MethodPost, "/api/v1/users", "", map[string]any{
		"username": "scoped-user", "email": "scoped-user@example.test", "password": "long-enough-password",
		"roles": []string{"viewer"},
		"access": map[string]any{
			"device_ids": []string{deviceID}, "device_group_ids": []string{groupID}, "port_ids": []string{portID},
			"billing_account_ids": []string{accountID}, "aggregate_graph_ids": []string{graphID},
			"metrics": []string{metricdomain.SNMPIfInBps},
		},
	})
	if createdUser.Code != http.StatusCreated {
		t.Fatalf("create user with scoped user: status=%d body=%s", createdUser.Code, createdUser.Body.String())
	}
	var created struct {
		ID string `json:"id"`
	}
	decodeJSON(t, createdUser, &created)
	for table := range map[string]string{
		"user_device_permissions": deviceID, "user_device_group_permissions": groupID,
		"user_port_permissions": portID, "user_billing_permissions": accountID,
		"user_aggregate_graph_permissions": graphID, "user_metric_permissions": metricdomain.SNMPIfInBps,
	} {
		if err := db.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE user_id=?`, created.ID).Scan(&count); err != nil || count != 1 {
			t.Fatalf("atomic create grant %s: count=%d err=%v", table, count, err)
		}
	}
	updatedAccess := rbacHandlerRequest(t, s.updateUser, p, http.MethodPatch, "/api/v1/users/"+created.ID, created.ID, map[string]any{
		"access": map[string]any{"device_ids": []string{deviceID}, "metrics": []string{metricdomain.SNMPIfOutBps}},
	})
	if updatedAccess.Code != http.StatusOK {
		t.Fatalf("update user access: status=%d body=%s", updatedAccess.Code, updatedAccess.Body.String())
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM user_device_permissions WHERE user_id=?`, created.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("updated device grant count=%d err=%v", count, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM user_port_permissions WHERE user_id=?`, created.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("updated access did not replace old grants: count=%d err=%v", count, err)
	}
	options := rbacHandlerRequest(t, s.listAccessOptions, p, http.MethodGet,
		"/api/v1/access-options?type=device&q=rbac-router&limit=10&sort=label&order=asc", "", nil)
	if options.Code != http.StatusOK || !strings.Contains(options.Body.String(), deviceID) {
		t.Fatalf("create-page access options: status=%d body=%s", options.Code, options.Body.String())
	}
	portOptions := rbacHandlerRequest(t, s.listAccessOptions, p, http.MethodGet,
		"/api/v1/access-options?type=port&device_id="+deviceID+"&limit=10&sort=label&order=asc", "", nil)
	if portOptions.Code != http.StatusOK || !strings.Contains(portOptions.Body.String(), portID) || strings.Contains(portOptions.Body.String(), otherPortID) {
		t.Fatalf("device-scoped port options: status=%d body=%s", portOptions.Code, portOptions.Body.String())
	}
	portIDs := rbacHandlerRequest(t, s.listPortAccessOptionIDs, p, http.MethodGet,
		"/api/v1/access-options/port-ids?device_id="+deviceID, "", nil)
	if portIDs.Code != http.StatusOK || !strings.Contains(portIDs.Body.String(), portID) || strings.Contains(portIDs.Body.String(), otherPortID) {
		t.Fatalf("device-scoped port ids: status=%d body=%s", portIDs.Code, portIDs.Body.String())
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
