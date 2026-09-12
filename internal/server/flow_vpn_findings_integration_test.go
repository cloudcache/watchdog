// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowvpn"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestFlowVPNFindingsMaterialize drives the findings writer against a real, empty
// MySQL: a scored candidate materializes into flow_vpn_findings; a re-score of the
// same conversation-window UPSERTs in place, refreshing the score while preserving
// a human disposition. Opt-in via WATCHDOG_TEST_MYSQL_DSN.
func TestFlowVPNFindingsMaterialize(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_flow_vpn_findings_it"
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	s, err := New(Config{
		MySQL:   MySQLConfig{DSN: parsed.FormatDSN()},
		Admin:   AdminConfig{Username: "find-admin", Password: "find-password"},
		Address: AddressConfig{ArtifactDir: t.TempDir(), SnapshotDir: t.TempDir()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	sc := scoredFixture()
	sc.Score.FamilyHints = []flowvpn.ProtocolFamily{flowvpn.FamilyTrojan}
	if n, err := s.materializeVPNFindings(ctx, []flowvpn.ScoredCandidate{sc}); err != nil || n != 1 {
		t.Fatalf("materialize: n=%d err=%v", n, err)
	}

	key := findingKey(sc)
	var score uint16
	var verdict, disposition, familyHints string
	var rowVersion uint64
	read := func() {
		t.Helper()
		if err := s.db.QueryRowContext(ctx, `SELECT score, verdict, disposition, row_version, COALESCE(family_hints, '[]')
			FROM flow_vpn_findings WHERE finding_key=?`, key).Scan(&score, &verdict, &disposition, &rowVersion, &familyHints); err != nil {
			t.Fatalf("read finding: %v", err)
		}
	}
	read()
	if score != 80 || verdict != string(flowvpn.VerdictProbeCandidate) || disposition != "unreviewed" || rowVersion != 1 {
		t.Fatalf("initial finding: score=%d verdict=%s disposition=%s rv=%d", score, verdict, disposition, rowVersion)
	}
	if familyHints != `["trojan"]` {
		t.Fatalf("family hints = %s, want [\"trojan\"]", familyHints)
	}

	// A reviewer dispositions the finding.
	if _, err := s.db.ExecContext(ctx, `UPDATE flow_vpn_findings SET disposition='confirmed' WHERE finding_key=?`, key); err != nil {
		t.Fatalf("disposition: %v", err)
	}

	// A re-score of the same window materializes in place with a higher score.
	sc.Score.Score = 90
	if _, err := s.materializeVPNFindings(ctx, []flowvpn.ScoredCandidate{sc}); err != nil {
		t.Fatalf("re-materialize: %v", err)
	}
	read()
	if score != 90 {
		t.Fatalf("re-score did not refresh score: %d", score)
	}
	if disposition != "confirmed" {
		t.Fatalf("re-score clobbered disposition: %s", disposition)
	}
	if rowVersion != 2 {
		t.Fatalf("re-score did not bump row_version: %d", rowVersion)
	}
}

// TestFlowVPNFindingsAPI drives the read/triage HTTP surface against a real MySQL:
// a materialized finding is listed and fetched, and dispositioned with If-Match
// (missing If-Match -> 428, stale -> 412). Opt-in via WATCHDOG_TEST_MYSQL_DSN.
func TestFlowVPNFindingsAPI(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_flow_vpn_findings_api_it"
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	s, err := New(Config{
		MySQL:   MySQLConfig{DSN: parsed.FormatDSN()},
		Admin:   AdminConfig{Username: "find-api-admin", Password: "find-api-password"},
		Address: AddressConfig{ArtifactDir: t.TempDir(), SnapshotDir: t.TempDir()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	sc := scoredFixture()
	if _, err := s.materializeVPNFindings(ctx, []flowvpn.ScoredCandidate{sc}); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	var findingID string
	if err := s.db.QueryRowContext(ctx, "SELECT id FROM flow_vpn_findings WHERE finding_key=?", findingKey(sc)).Scan(&findingID); err != nil {
		t.Fatalf("read id: %v", err)
	}

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "find-api-admin", "password": "find-api-password",
	}, nil)
	cookies := login.Result().Cookies()
	csrf := cookieValue(cookies, csrfCookie)
	field := func(resp *httptest.ResponseRecorder, key string) any {
		t.Helper()
		var body map[string]any
		if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v (%s)", err, resp.Body.String())
		}
		return body[key]
	}

	// --- list, filtered by verdict, includes the finding ---
	list := requestJSON(t, s, http.MethodGet, "/api/v1/flow/vpn/findings?verdict=probe_candidate", nil, nil, cookies...)
	if list.Code != http.StatusOK || field(list, "total").(float64) < 1 {
		t.Fatalf("list: %d %s", list.Code, list.Body.String())
	}

	// --- get returns the finding + ETag ---
	got := requestJSON(t, s, http.MethodGet, "/api/v1/flow/vpn/findings/"+findingID, nil, nil, cookies...)
	if got.Code != http.StatusOK || got.Header().Get("ETag") == "" || field(got, "score").(float64) != 80 {
		t.Fatalf("get: %d etag=%q body=%s", got.Code, got.Header().Get("ETag"), got.Body.String())
	}
	etag := got.Header().Get("ETag")

	// --- disposition without If-Match -> 428 ---
	no428 := requestJSON(t, s, http.MethodPost, "/api/v1/flow/vpn/findings/"+findingID+"/actions/disposition",
		map[string]any{"disposition": "confirmed"}, map[string]string{"X-CSRF-Token": csrf}, cookies...)
	if no428.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match: %d", no428.Code)
	}

	// --- disposition with If-Match -> 200, disposition applied ---
	ok := requestJSON(t, s, http.MethodPost, "/api/v1/flow/vpn/findings/"+findingID+"/actions/disposition",
		map[string]any{"disposition": "confirmed", "note": "reviewed"},
		map[string]string{"X-CSRF-Token": csrf, "If-Match": etag}, cookies...)
	if ok.Code != http.StatusOK || field(ok, "disposition") != "confirmed" {
		t.Fatalf("disposition: %d %s", ok.Code, ok.Body.String())
	}

	// --- stale If-Match -> 412 ---
	stale := requestJSON(t, s, http.MethodPost, "/api/v1/flow/vpn/findings/"+findingID+"/actions/disposition",
		map[string]any{"disposition": "allowed"}, map[string]string{"X-CSRF-Token": csrf, "If-Match": etag}, cookies...)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale If-Match: %d %s", stale.Code, stale.Body.String())
	}
}
