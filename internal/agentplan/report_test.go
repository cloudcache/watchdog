// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package agentplan

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReportRunPostsSummaryToStatusEndpoint(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotPath = req.URL.Path
		gotAuth = req.Header.Get("Authorization")
		_ = json.NewDecoder(req.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	rc := RuntimeConfig{BaseURL: srv.URL, AgentID: "flow-worker-1", HTTPClient: srv.Client()}
	if err := rc.ReportRun(context.Background(), "tok", RunReport{
		Status:  "success",
		Summary: map[string]any{"records_in": 200000, "drops": 3, "kafka_lag": 12},
	}); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/v1/agents/flow-worker-1/status" {
		t.Fatalf("path=%s", gotPath)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("auth=%s", gotAuth)
	}
	if gotBody["status"] != "success" {
		t.Fatalf("status=%v", gotBody["status"])
	}
	summary, ok := gotBody["summary"].(map[string]any)
	if !ok || summary["records_in"] == nil {
		t.Fatalf("summary not forwarded: %v", gotBody["summary"])
	}
}

func TestReportRunSurfacesUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	rc := RuntimeConfig{BaseURL: srv.URL, AgentID: "a", HTTPClient: srv.Client()}
	if err := rc.ReportRun(context.Background(), "tok", RunReport{}); err != ErrUnauthorized {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
}
