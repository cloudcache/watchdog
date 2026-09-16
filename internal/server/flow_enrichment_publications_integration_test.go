package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowworker"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// TestFlowEnrichmentPublicationGinWorkerIntegration proves the complete
// single-domain chain against a disposable MySQL database: admin profile and
// pair publication, authenticated flow_worker pull, signature/object checks,
// downloaded+installed ACKs, rejection of a corrupt upgrade without replacing
// the durable LKG, offline restore, and stable trust on restart.
func TestFlowEnrichmentPublicationGinWorkerIntegration(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_flow_enrichment_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })

	root := t.TempDir()
	cfg := Config{
		MySQL: MySQLConfig{DSN: dsn},
		Admin: AdminConfig{Username: "flow-enrichment-admin", Password: "flow-enrichment-password"},
		AgentPlans: AgentPlansConfig{
			SigningKeyID: "flow-enrichment-key", SigningPrivateKey: filepath.Join(root, "agent-plan.pem"),
			DefaultTTL: 24 * time.Hour,
		},
		Address: AddressConfig{ArtifactDir: filepath.Join(root, "artifacts"), SnapshotDir: filepath.Join(root, "snapshots")},
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	var legacyTableCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema=DATABASE() AND table_name IN ('tenants','collector_agents')`).Scan(&legacyTableCount); err != nil {
		t.Fatalf("inspect clean KISS schema: %v", err)
	}
	if legacyTableCount != 0 {
		t.Fatalf("clean KISS schema contains %d legacy tenant/collector tables", legacyTableCount)
	}
	closed := false
	defer func() {
		if !closed {
			_ = s.Close()
		}
	}()

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{
		"username": "flow-enrichment-admin", "password": "flow-enrichment-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login: status=%d body=%s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	adminHeaders := map[string]string{"X-CSRF-Token": cookieValue(cookies, csrfCookie)}

	const workerID = "flow_worker_it"
	const workerToken = "flow-worker-it-secret"
	createdAgent := requestJSON(t, s, http.MethodPost, "/api/v1/agents", map[string]any{
		"id": workerID, "name": "Flow worker integration", "kind": "flow_worker", "mode": "push",
		"token": workerToken, "api_version": "v1", "capabilities": []string{"flow.write.clickhouse/v1"},
	}, adminHeaders, cookies...)
	if createdAgent.Code != http.StatusCreated {
		t.Fatalf("create flow worker: status=%d body=%s", createdAgent.Code, createdAgent.Body.String())
	}
	const wrongKindToken = "snmp-worker-it-secret"
	wrongKind := requestJSON(t, s, http.MethodPost, "/api/v1/agents", map[string]any{
		"id": "snmp_worker_it", "name": "SNMP worker integration", "kind": "snmp", "mode": "push",
		"token": wrongKindToken, "api_version": "v1", "capabilities": []string{"snmp.poll/v2"},
	}, adminHeaders, cookies...)
	if wrongKind.Code != http.StatusCreated {
		t.Fatalf("create wrong-kind agent: status=%d body=%s", wrongKind.Code, wrongKind.Body.String())
	}
	wrongKindPull := machineRequest(t, s, http.MethodGet, "/api/v1/flow-workers/snmp_worker_it/trust-bundle", wrongKindToken, nil)
	if wrongKindPull.Code != http.StatusUnauthorized {
		t.Fatalf("wrong-kind agent can pull Flow enrichment: status=%d body=%s", wrongKindPull.Code, wrongKindPull.Body.String())
	}

	effectiveFrom := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
	prepareFlowEnrichmentAddressSnapshot(t, s, effectiveFrom)

	profile := requestJSON(t, s, http.MethodGet, "/api/v1/flow/classification-profile", nil, nil, cookies...)
	if profile.Code != http.StatusOK || profile.Header().Get("ETag") != `"0"` {
		t.Fatalf("initial profile: status=%d etag=%q body=%s", profile.Code, profile.Header().Get("ETag"), profile.Body.String())
	}
	profileHeaders := map[string]string{"X-CSRF-Token": adminHeaders["X-CSRF-Token"], "If-Match": profile.Header().Get("ETag")}
	legacyProfile := requestJSON(t, s, http.MethodPut, "/api/v1/flow/classification-profile", map[string]any{
		"home_province": "330000", "tenant_id": "legacy-tenant",
	}, profileHeaders, cookies...)
	if legacyProfile.Code != http.StatusBadRequest {
		t.Fatalf("legacy tenant field was accepted: status=%d body=%s", legacyProfile.Code, legacyProfile.Body.String())
	}
	savedProfile := requestJSON(t, s, http.MethodPut, "/api/v1/flow/classification-profile", map[string]any{
		"home_province": "330000", "home_city": "330100", "home_isp_ids": []uint16{2, 1, 2},
		"home_asns": []uint32{4812, 4134, 4812}, "overseas_includes_hmt": true,
		"internal_policy": "count", "transit_policy": "drop",
	}, profileHeaders, cookies...)
	if savedProfile.Code != http.StatusOK || savedProfile.Header().Get("ETag") != `"1"` {
		t.Fatalf("save profile: status=%d etag=%q body=%s", savedProfile.Code, savedProfile.Header().Get("ETag"), savedProfile.Body.String())
	}
	staleProfile := requestJSON(t, s, http.MethodPut, "/api/v1/flow/classification-profile", map[string]any{
		"home_province": "330000", "internal_policy": "count", "transit_policy": "count",
	}, profileHeaders, cookies...)
	if staleProfile.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale profile update was accepted: status=%d body=%s", staleProfile.Code, staleProfile.Body.String())
	}

	published := requestJSON(t, s, http.MethodPost, "/api/v1/flow/enrichment-publications", map[string]any{
		"effective_from": effectiveFrom,
	}, adminHeaders, cookies...)
	var publication flowEnrichmentPublication
	decodeJSON(t, published, &publication)
	if published.Code != http.StatusCreated || publication.ClassificationVersion != 1 ||
		publication.DimensionSnapshotID != "address_snapshot_it" || len(publication.Signature) != 64 {
		t.Fatalf("publish pair: status=%d publication=%+v body=%s", published.Code, publication, published.Body.String())
	}
	listed := requestJSON(t, s, http.MethodGet,
		"/api/v1/flow/enrichment-publications?q=address_snapshot&dimension_snapshot_id=address_snapshot_it&signing_key_id=flow-enrichment-key&limit=25&offset=0&sort=effective_from&order=desc",
		nil, nil, cookies...)
	if listed.Code != http.StatusOK || !bytes.Contains(listed.Body.Bytes(), []byte(`"total":1`)) {
		t.Fatalf("publication VTable list: status=%d body=%s", listed.Code, listed.Body.String())
	}
	facets := requestJSON(t, s, http.MethodGet,
		"/api/v1/flow/enrichment-publications/facets?field=dimension_snapshot_id&q=address&limit=10", nil, nil, cookies...)
	if facets.Code != http.StatusOK || !bytes.Contains(facets.Body.Bytes(), []byte(`"value":"address_snapshot_it"`)) {
		t.Fatalf("publication facets: status=%d body=%s", facets.Code, facets.Body.String())
	}

	identity := flowworker.VersionWorkerIdentity{WorkerID: workerID, BootID: "boot-it", SoftwareVersion: "1.0.0"}
	httpClient := &http.Client{Transport: ginRoundTripper{handler: s.engine}}
	client, err := flowworker.NewVersionHTTPClient(flowworker.VersionHTTPClientConfig{
		BaseURL: "http://watchdog.test", AgentToken: workerToken, Identity: identity, Client: httpClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	lkgDir := filepath.Join(root, "lkg")
	lkg, err := flowworker.NewDiskVersionLKG(lkgDir)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := flowworker.NewEnrichmentVersionCatalog()
	if err != nil {
		t.Fatal(err)
	}
	syncer, err := flowworker.NewRemoteVersionSync(client, lkg, &flowplan.TrustStore{}, catalog, flowworker.VersionLoaderLimits{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := syncer.SyncOnce(context.Background(), 0)
	if err != nil || result.Installed != 1 || result.HighestVersion != 1 {
		t.Fatalf("worker sync: result=%+v err=%v", result, err)
	}
	installed, ok := catalog.ClassificationVersion(1)
	if !ok || installed.Metadata().DimensionSnapshotID != publication.DimensionSnapshotID {
		t.Fatalf("catalog did not atomically expose pair: %+v ok=%v", installed.Metadata(), ok)
	}
	var ackState string
	var downloadedAt, installedAt time.Time
	if err := s.db.QueryRow(`SELECT state,downloaded_at,installed_at FROM flow_enrichment_publication_acks WHERE publication_id=? AND worker_id=?`,
		publication.ID, workerID).Scan(&ackState, &downloadedAt, &installedAt); err != nil || ackState != "installed" || downloadedAt.IsZero() || installedAt.IsZero() {
		t.Fatalf("worker ACK did not converge: state=%q downloaded=%v installed=%v err=%v", ackState, downloadedAt, installedAt, err)
	}
	ackList := requestJSON(t, s, http.MethodGet,
		"/api/v1/flow/enrichment-publications/"+publication.ID+"/acks?q=Flow&state=installed&limit=25&offset=0&sort=attempted_at&order=desc",
		nil, nil, cookies...)
	if ackList.Code != http.StatusOK || !bytes.Contains(ackList.Body.Bytes(), []byte(`"total":1`)) {
		t.Fatalf("acknowledgement VTable list: status=%d body=%s", ackList.Code, ackList.Body.String())
	}
	ackFacets := requestJSON(t, s, http.MethodGet,
		"/api/v1/flow/enrichment-publications/"+publication.ID+"/acks/facets?field=state&q=install&limit=10",
		nil, nil, cookies...)
	if ackFacets.Code != http.StatusOK || !bytes.Contains(ackFacets.Body.Bytes(), []byte(`"value":"installed"`)) {
		t.Fatalf("acknowledgement facets: status=%d body=%s", ackFacets.Code, ackFacets.Body.String())
	}

	secondProfileHeaders := map[string]string{"X-CSRF-Token": adminHeaders["X-CSRF-Token"], "If-Match": savedProfile.Header().Get("ETag")}
	secondProfile := requestJSON(t, s, http.MethodPut, "/api/v1/flow/classification-profile", map[string]any{
		"home_province": "330000", "home_city": "330100", "home_isp_ids": []uint16{1, 2},
		"home_asns": []uint32{4134, 4812, 64500}, "overseas_includes_hmt": true,
		"internal_policy": "count", "transit_policy": "drop",
	}, secondProfileHeaders, cookies...)
	if secondProfile.Code != http.StatusOK || secondProfile.Header().Get("ETag") != `"2"` {
		t.Fatalf("save second profile: status=%d etag=%q body=%s", secondProfile.Code, secondProfile.Header().Get("ETag"), secondProfile.Body.String())
	}
	secondPublished := requestJSON(t, s, http.MethodPost, "/api/v1/flow/enrichment-publications", map[string]any{
		"effective_from": effectiveFrom.Add(time.Minute),
	}, adminHeaders, cookies...)
	var secondPublication flowEnrichmentPublication
	decodeJSON(t, secondPublished, &secondPublication)
	if secondPublished.Code != http.StatusCreated || secondPublication.ClassificationVersion != 2 {
		t.Fatalf("publish second pair: status=%d publication=%+v body=%s", secondPublished.Code, secondPublication, secondPublished.Body.String())
	}
	classificationPath, err := s.addressObjects.ResolveDimensionObject(secondPublication.ClassificationObjectRef)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(classificationPath, []byte("corrupt-publication"), 0o640); err != nil {
		t.Fatal(err)
	}
	failedResult, err := syncer.SyncOnce(context.Background(), 1)
	if err == nil || failedResult.Installed != 0 || failedResult.HighestVersion != 1 {
		t.Fatalf("corrupt upgrade replaced active version: result=%+v err=%v", failedResult, err)
	}
	if _, ok := catalog.ClassificationVersion(2); ok {
		t.Fatal("corrupt classification version became query-visible")
	}
	if _, ok := catalog.ClassificationVersion(1); !ok {
		t.Fatal("corrupt upgrade removed the active classification version")
	}
	var failedState, failedCode string
	if err := s.db.QueryRow(`SELECT state,COALESCE(error_code,'') FROM flow_enrichment_publication_acks WHERE publication_id=? AND worker_id=?`,
		secondPublication.ID, workerID).Scan(&failedState, &failedCode); err != nil || failedState != "failed" || failedCode != "verify:CLASSIFICATION_OBJECT_INVALID" {
		t.Fatalf("corrupt upgrade ACK: state=%q code=%q err=%v", failedState, failedCode, err)
	}

	restartedLKG, err := flowworker.NewDiskVersionLKG(lkgDir)
	if err != nil {
		t.Fatal(err)
	}
	restartedCatalog, _ := flowworker.NewEnrichmentVersionCatalog()
	restore, err := restartedLKG.Restore(context.Background(), &flowplan.TrustStore{}, restartedCatalog, identity, flowworker.VersionLoaderLimits{}, time.Now().UTC())
	if err != nil || restore.PublicationCount != 1 || restore.HighestVersion != 1 {
		t.Fatalf("offline LKG restore: result=%+v err=%v", restore, err)
	}

	firstTrust := append([]byte(nil), s.flowTrustBundle...)
	var trustRowVersion uint64
	if err := s.db.QueryRow(`SELECT row_version FROM settings WHERE `+"`key`"+`=?`, flowTrustSettingKey).Scan(&trustRowVersion); err != nil {
		t.Fatalf("read trust row version: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	restarted, err := New(cfg)
	if err != nil {
		t.Fatalf("restart server: %v", err)
	}
	defer restarted.Close()
	if !bytes.Equal(firstTrust, restarted.flowTrustBundle) || restarted.flowTrustGeneration != 1 {
		t.Fatalf("restart changed trust bundle at same signing key: before=%s after=%s", firstTrust, restarted.flowTrustBundle)
	}
	var restartedTrustRowVersion uint64
	if err := restarted.db.QueryRow(`SELECT row_version FROM settings WHERE `+"`key`"+`=?`, flowTrustSettingKey).Scan(&restartedTrustRowVersion); err != nil || restartedTrustRowVersion != trustRowVersion {
		t.Fatalf("restart rewrote unchanged trust bundle: before=%d after=%d err=%v", trustRowVersion, restartedTrustRowVersion, err)
	}
}

func prepareFlowEnrichmentAddressSnapshot(t *testing.T, s *Server, effectiveFrom time.Time) {
	t.Helper()
	const snapshotID = "address_snapshot_it"
	artifact := flowdimension.AddressSnapshotArtifact{
		SnapshotID: snapshotID, Version: 1, EffectiveFrom: effectiveFrom, BuilderVersion: "integration-test",
		SourceManifestSHA256: "sha256:" + string(bytes.Repeat([]byte{'a'}, 64)),
		Strings:              []string{""}, Values: []flowdimension.AddressSnapshotValue{{}},
	}
	data, err := flowdimension.EncodeAddressSnapshot(artifact, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatalf("encode WADS: %v", err)
	}
	object, err := s.addressObjects.SaveDimensionObject(context.Background(), snapshotID, data)
	if err != nil {
		t.Fatalf("save WADS: %v", err)
	}
	draft := sha256.Sum256([]byte("flow enrichment integration draft"))
	var adminID string
	if err := s.db.QueryRow(`SELECT id FROM users WHERE username='flow-enrichment-admin'`).Scan(&adminID); err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`INSERT INTO dimension_snapshots
		(id,module_key,dimension_key,version,effective_from,object_ref,object_format,object_format_version,builder_version,build_job_id,
		 checksum,draft_digest,source_manifest_version,source_manifest,source_prefix_count,bundle_schema_version,entry_count,prefix_count,
		 address_set_count,max_address_sets_per_record,status,approval_state,decided_by,decided_at,created_by)
		 VALUES (?,'flow','address',1,?,?,'wads',1,'integration-test',?,?,?,0,JSON_ARRAY(),0,?,1,0,0,0,'active','approved',?,NOW(3),?)`,
		snapshotID, effectiveFrom, object.Ref, snapshotID, object.Checksum, "sha256:"+hex.EncodeToString(draft[:]),
		flowdimension.BundleSchemaVersion, adminID, adminID)
	if err != nil {
		t.Fatalf("insert address snapshot: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO dimension_snapshot_activations
		(id,module_key,dimension_key,snapshot_id,effective_from,reason,created_by)
		VALUES ('address_activation_it','flow','address',?,?,'publish',?)`, snapshotID, effectiveFrom, adminID); err != nil {
		t.Fatalf("activate address snapshot: %v", err)
	}
}

type ginRoundTripper struct{ handler http.Handler }

func (transport ginRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	transport.handler.ServeHTTP(recorder, request)
	response := recorder.Result()
	if response.Body == nil {
		response.Body = io.NopCloser(bytes.NewReader(nil))
	}
	return response, nil
}

var _ http.RoundTripper = ginRoundTripper{}
