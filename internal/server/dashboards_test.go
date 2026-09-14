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

func TestDashboardGraphIDs(t *testing.T) {
	ids, err := dashboardGraphIDs(json.RawMessage(`{"panels":[{"graph_id":" graph-a "},{"graph_id":"graph-a"},{"graph_id":"graph-b"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ids, ",") != "graph-a,graph-b" {
		t.Fatalf("ids = %v", ids)
	}
	for _, raw := range []string{`[]`, `{"panels":[{"graph_id":""}]}`, ``} {
		if _, err := dashboardGraphIDs(json.RawMessage(raw)); err == nil {
			t.Fatalf("dashboardGraphIDs(%q) unexpectedly succeeded", raw)
		}
	}
}

func TestDashboardMySQLAPI(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_dashboards_it"
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

	userID, graphID := newID(), "dashboard-graph"
	if _, err := db.Exec("INSERT INTO users (id,username,status) VALUES (?,?,'active')", userID, "dashboard-"+userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO aggregate_graphs (id,name,aggregation,value_mode,created_by) VALUES (?,'Traffic','sum','corrected',?)`, graphID, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO aggregate_graph_items (id,aggregate_graph_id,sequence,metric,direction,label) VALUES ('dashboard-item',?,0,'if_in_bps','in','Inbound')`, graphID); err != nil {
		t.Fatal(err)
	}

	s := &Server{db: db}
	p := &principal{UserID: userID, IsAdmin: true}
	created := dashboardRequest(t, s.createDashboard, p, http.MethodPost, "/api/v1/dashboards", "", map[string]any{
		"name": "Operations", "description": "core links", "layout": map[string]any{"panels": []map[string]any{{"graph_id": graphID}}},
	}, "")
	if created.Code != http.StatusCreated || created.Header().Get("ETag") != `"1"` {
		t.Fatalf("create: status=%d etag=%q body=%s", created.Code, created.Header().Get("ETag"), created.Body.String())
	}
	var dashboard dashboardRecord
	if err := json.Unmarshal(created.Body.Bytes(), &dashboard); err != nil {
		t.Fatal(err)
	}

	listed := dashboardRequest(t, s.listDashboards, p, http.MethodGet,
		"/api/v1/dashboards?limit=25&offset=0&sort=updated_at&order=desc&q=Operations", "", nil, "")
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"total":1`) || !strings.Contains(listed.Body.String(), dashboard.ID) {
		t.Fatalf("list: status=%d body=%s", listed.Code, listed.Body.String())
	}
	options := dashboardRequest(t, s.listDashboardGraphOptions, p, http.MethodGet,
		"/api/v1/dashboards/graph-options?limit=20&offset=0&q=Traffic", "", nil, "")
	if options.Code != http.StatusOK || !strings.Contains(options.Body.String(), graphID) {
		t.Fatalf("graph options: status=%d body=%s", options.Code, options.Body.String())
	}
	preview := dashboardRequest(t, s.previewDashboard, p, http.MethodGet,
		"/api/v1/dashboards/"+dashboard.ID+"/preview", dashboard.ID, nil, "")
	if preview.Code != http.StatusOK || !strings.Contains(preview.Body.String(), `"exists":true`) ||
		!strings.Contains(preview.Body.String(), "dashboard-item") {
		t.Fatalf("preview: status=%d body=%s", preview.Code, preview.Body.String())
	}

	stale := dashboardRequest(t, s.updateDashboard, p, http.MethodPatch,
		"/api/v1/dashboards/"+dashboard.ID, dashboard.ID, map[string]any{
			"name": "Operations 2", "layout": map[string]any{"panels": []map[string]any{{"graph_id": graphID}}},
		}, `"99"`)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale update: status=%d body=%s", stale.Code, stale.Body.String())
	}
	updated := dashboardRequest(t, s.updateDashboard, p, http.MethodPatch,
		"/api/v1/dashboards/"+dashboard.ID, dashboard.ID, map[string]any{
			"name": "Operations 2", "layout": map[string]any{"panels": []map[string]any{{"graph_id": graphID}}},
		}, `"1"`)
	if updated.Code != http.StatusOK || updated.Header().Get("ETag") != `"2"` {
		t.Fatalf("update: status=%d etag=%q body=%s", updated.Code, updated.Header().Get("ETag"), updated.Body.String())
	}
	deleted := dashboardRequest(t, s.deleteDashboard, p, http.MethodDelete,
		"/api/v1/dashboards/"+dashboard.ID, dashboard.ID, nil, `"2"`)
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete: status=%d body=%s", deleted.Code, deleted.Body.String())
	}
}

func dashboardRequest(t *testing.T, handler gin.HandlerFunc, p *principal, method, target, dashboardID string, body any, match string) *httptest.ResponseRecorder {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, bytes.NewReader(payload))
	c.Set(principalKey, p)
	if dashboardID != "" {
		c.Params = gin.Params{{Key: "id", Value: dashboardID}}
	}
	if match != "" {
		c.Request.Header.Set("If-Match", match)
	}
	handler(c)
	c.Writer.WriteHeaderNow()
	return recorder
}
