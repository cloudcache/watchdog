package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowvpn"
	"github.com/cloudcache/watchdog/internal/opjob"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestVPNRuleSetPublicationWorkerACK exercises the complete typed adapter over
// the shared publication lifecycle: active draft -> preview -> async immutable
// object -> approval/activation -> authenticated worker install ACK.
func TestVPNRuleSetPublicationWorkerACK(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_vpn_publication_it"
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	root := t.TempDir()
	// The Flow worker authenticates with the installation shared token.
	const workerToken = "flow-worker-vpn-secret"
	s, err := New(Config{
		MySQL:   MySQLConfig{DSN: parsed.FormatDSN()},
		Admin:   AdminConfig{Username: "vpn-publish-admin", Password: "vpn-publish-password"},
		Agents:  AgentsConfig{SharedToken: workerToken},
		Address: AddressConfig{ArtifactDir: filepath.Join(root, "artifacts"), SnapshotDir: filepath.Join(root, "snapshots")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "vpn-publish-admin", "password": "vpn-publish-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login: %d %s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	csrf := cookieValue(cookies, csrfCookie)
	mutation := map[string]string{"X-CSRF-Token": csrf}

	created := requestJSON(t, s, http.MethodPost, "/api/v1/flow/vpn/rules", map[string]any{
		"name": "Published TLS tunnel", "kind": "passive", "effect": "score", "weight": 25,
		"family_hint": "trojan", "status": "active", "match": map[string]any{"remote_ports": []int{443}},
	}, mutation, cookies...)
	if created.Code != http.StatusCreated {
		t.Fatalf("create active rule: %d %s", created.Code, created.Body.String())
	}

	effective := time.Now().UTC().Truncate(time.Minute)
	policy := map[string]any{"medium_threshold": 30, "high_threshold": 60, "critical_threshold": 85, "probe_threshold": 70, "minimum_completeness": 0.8}
	preview := requestJSON(t, s, http.MethodPost, "/api/v1/flow/vpn/rule-sets/preview", map[string]any{
		"effective_from": effective, "policy": policy,
	}, mutation, cookies...)
	if preview.Code != http.StatusOK {
		t.Fatalf("preview: %d %s", preview.Code, preview.Body.String())
	}
	var previewBody struct {
		DraftDigest string `json:"draft_digest"`
		RuleCount   uint64 `json:"rule_count"`
	}
	if err := json.Unmarshal(preview.Body.Bytes(), &previewBody); err != nil || previewBody.DraftDigest == "" || previewBody.RuleCount != 1 {
		t.Fatalf("preview response: err=%v body=%s", err, preview.Body.String())
	}
	published := requestJSON(t, s, http.MethodPost, "/api/v1/flow/vpn/rule-sets/publish", map[string]any{
		"effective_from": effective, "preview_digest": previewBody.DraftDigest, "policy": policy,
	}, mutation, cookies...)
	if published.Code != http.StatusAccepted {
		t.Fatalf("publish: %d %s", published.Code, published.Body.String())
	}
	var publishBody struct {
		Job opjob.Job `json:"job"`
	}
	if err := json.Unmarshal(published.Body.Bytes(), &publishBody); err != nil || publishBody.Job.ID == "" {
		t.Fatalf("publish job: err=%v body=%s", err, published.Body.String())
	}
	waitForVPNPublishJob(t, s, publishBody.Job.ID)

	got := requestJSON(t, s, http.MethodGet, "/api/v1/flow/vpn/rule-sets/"+publishBody.Job.ID, nil, nil, cookies...)
	if got.Code != http.StatusOK || got.Header().Get("ETag") != `"1"` {
		t.Fatalf("get pending rule set: %d etag=%q body=%s", got.Code, got.Header().Get("ETag"), got.Body.String())
	}
	approved := requestJSON(t, s, http.MethodPost, "/api/v1/flow/vpn/rule-sets/"+publishBody.Job.ID+"/actions/approve", nil,
		map[string]string{"X-CSRF-Token": csrf, "If-Match": got.Header().Get("ETag")}, cookies...)
	if approved.Code != http.StatusOK || approved.Header().Get("ETag") != `"2"` {
		t.Fatalf("approve: %d etag=%q body=%s", approved.Code, approved.Header().Get("ETag"), approved.Body.String())
	}
	activated := requestJSON(t, s, http.MethodPost, "/api/v1/flow/vpn/rule-sets/"+publishBody.Job.ID+"/actions/activate", nil,
		map[string]string{"X-CSRF-Token": csrf, "If-Match": approved.Header().Get("ETag")}, cookies...)
	if activated.Code != http.StatusOK {
		t.Fatalf("activate: %d %s", activated.Code, activated.Body.String())
	}

	// The real scoring worker consumes the same activation/object contract. It
	// must install the immutable scorer and record its own installed ACK rather
	// than reading mutable flow_vpn_rules during a detection window.
	s.vpnRuleSetCatalog = flowvpn.NewRuleSetCatalog()
	s.vpnRuleSetBootID = newID()
	installed, err := s.installVPNRuleSetForEventTime(t.Context(), effective)
	if err != nil || installed.Metadata.SnapshotID != publishBody.Job.ID {
		t.Fatalf("detection worker install: metadata=%+v err=%v", installed.Metadata, err)
	}
	var detectionState string
	if err := s.db.QueryRowContext(t.Context(), `SELECT state FROM dimension_snapshot_acks WHERE snapshot_id = ? AND worker_id = ?`, publishBody.Job.ID, vpnDetectionWorkerID).Scan(&detectionState); err != nil {
		t.Fatal(err)
	}
	if detectionState != "installed" {
		t.Fatalf("detection worker ACK = %q", detectionState)
	}

	const workerID = "flow_worker_vpn_it"
	agent := requestJSON(t, s, http.MethodPost, "/api/v1/agents", map[string]any{
		"id": workerID, "name": "VPN publication worker", "kind": "flow_worker", "mode": "push",
		"api_version": "v1", "capabilities": []string{"flow.write.clickhouse/v1"},
	}, mutation, cookies...)
	if agent.Code != http.StatusCreated {
		t.Fatalf("create worker: %d %s", agent.Code, agent.Body.String())
	}
	desired := machineRequest(t, s, http.MethodGet, "/api/v1/flow-workers/"+workerID+"/vpn-rule-set", workerToken, nil)
	if desired.Code != http.StatusOK {
		t.Fatalf("fetch desired: %d %s", desired.Code, desired.Body.String())
	}
	var desiredBody struct {
		SnapshotID string `json:"snapshot_id"`
		Checksum   string `json:"checksum"`
		ObjectURL  string `json:"object_url"`
	}
	if err := json.Unmarshal(desired.Body.Bytes(), &desiredBody); err != nil || desiredBody.SnapshotID != publishBody.Job.ID {
		t.Fatalf("desired response: err=%v body=%s", err, desired.Body.String())
	}
	object := machineRequest(t, s, http.MethodGet, desiredBody.ObjectURL, workerToken, nil)
	if object.Code != http.StatusOK || object.Header().Get("X-Watchdog-Object-Checksum") != desiredBody.Checksum {
		t.Fatalf("fetch object: %d body=%s", object.Code, object.Body.String())
	}
	_, metadata, err := flowvpn.DecodeAndCompileRuleSetBundle(object.Body.Bytes(), desiredBody.Checksum)
	if err != nil || metadata.SnapshotID != publishBody.Job.ID {
		t.Fatalf("decode object: metadata=%+v err=%v", metadata, err)
	}
	for _, state := range []string{"downloaded", "installed"} {
		ack := machineRequest(t, s, http.MethodPost, "/api/v1/flow-workers/"+workerID+"/vpn-rule-sets/"+publishBody.Job.ID+"/ack", workerToken, map[string]any{
			"state": state, "boot_id": "boot-vpn-it", "software_version": "watchdog-flow-worker-it", "checksum": desiredBody.Checksum,
		})
		if ack.Code != http.StatusAccepted {
			t.Fatalf("%s ack: %d %s", state, ack.Code, ack.Body.String())
		}
	}
	consumers := requestJSON(t, s, http.MethodGet, "/api/v1/flow/vpn/rule-sets/"+publishBody.Job.ID+"/consumers", nil, nil, cookies...)
	if consumers.Code != http.StatusOK {
		t.Fatalf("consumers: %d %s", consumers.Code, consumers.Body.String())
	}

	// Fixed reports use half-open windows. A publication activated exactly at the
	// report end belongs to the next window; the following window exposes the
	// approved snapshot and both installed consumers.
	before := s.vpnRulePublicationReportPanel(t.Context(), effective)
	if before.Status != "unavailable" {
		t.Fatalf("publication leaked across report boundary: %+v", before)
	}
	panel := s.vpnRulePublicationReportPanel(t.Context(), effective.Add(time.Minute))
	if panel.Status != "ready" {
		t.Fatalf("publication panel: %+v", panel)
	}
	var publication struct {
		SnapshotID      string `json:"snapshot_id"`
		RuleCount       uint64 `json:"rule_count"`
		ObservedWorkers uint64 `json:"observed_workers"`
		ReadyWorkers    uint64 `json:"ready_workers"`
	}
	if err := json.Unmarshal(panel.Data, &publication); err != nil {
		t.Fatal(err)
	}
	if publication.SnapshotID != publishBody.Job.ID || publication.RuleCount != 1 ||
		publication.ObservedWorkers != 2 || publication.ReadyWorkers != 2 {
		t.Fatalf("publication panel data: %+v", publication)
	}
}

func waitForVPNPublishJob(t *testing.T, s *Server, id string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		job, err := s.jobs.Get(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == opjob.StatusSucceeded {
			return
		}
		if job.Status == opjob.StatusFailed {
			t.Fatalf("publish job failed: %s", job.LastErrorDetail)
		}
		if time.Now().After(deadline) {
			t.Fatalf("publish job did not finish: %s", job.Status)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
