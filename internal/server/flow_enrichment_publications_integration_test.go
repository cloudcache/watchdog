package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
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
		"status": "active", "token": workerToken, "api_version": "v1", "capabilities": []string{"flow.write.clickhouse/v1"},
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
	bootstrap := requestJSON(t, s, http.MethodPost, "/api/v1/flow/enrichment-publications/bootstrap", map[string]any{}, adminHeaders, cookies...)
	var bootstrapPublication flowEnrichmentPublication
	decodeJSON(t, bootstrap, &bootstrapPublication)
	if bootstrap.Code != http.StatusCreated || bootstrapPublication.ClassificationVersion != 1 ||
		!bootstrapPublication.EffectiveFrom.Equal(effectiveFrom) ||
		bootstrapPublication.ClassificationSchemaVersion != uint16(flowdimension.LegacyClassificationSchemaVersion) {
		t.Fatalf("bootstrap unclassified pair: status=%d publication=%+v body=%s", bootstrap.Code, bootstrapPublication, bootstrap.Body.String())
	}
	duplicateBootstrap := requestJSON(t, s, http.MethodPost, "/api/v1/flow/enrichment-publications/bootstrap", map[string]any{}, adminHeaders, cookies...)
	if duplicateBootstrap.Code != http.StatusConflict {
		t.Fatalf("duplicate bootstrap was accepted: status=%d body=%s", duplicateBootstrap.Code, duplicateBootstrap.Body.String())
	}
	if _, err := s.db.Exec(`DELETE FROM flow_enrichment_publications WHERE id=?`, bootstrapPublication.ID); err != nil {
		t.Fatalf("remove bootstrap fixture: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO devices (id,host,kind) VALUES ('flow-device-it','192.0.2.10','network')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO flow_exporter_bindings
		(id,device_id,source_prefix,protocol,sampling_rules_json,observations_json)
		VALUES ('flow-binding-it','flow-device-it','192.0.2.10/32','sflow5',JSON_ARRAY(),JSON_OBJECT())`); err != nil {
		t.Fatal(err)
	}
	createdCustomer := requestJSON(t, s, http.MethodPost, "/api/v1/flow/customers", map[string]any{
		"name": "Integration customer",
	}, adminHeaders, cookies...)
	var customer struct {
		ID string `json:"id"`
	}
	decodeJSON(t, createdCustomer, &customer)
	if createdCustomer.Code != http.StatusCreated || customer.ID == "" {
		t.Fatalf("create Flow customer: status=%d body=%s", createdCustomer.Code, createdCustomer.Body.String())
	}
	createdBoundary := requestJSON(t, s, http.MethodPost, "/api/v1/flow/customer-bindings", map[string]any{
		"device_id": "flow-device-it", "customer_id": customer.ID,
		"source_ranges": []string{"198.51.100.0/24", "2001:db8:100::/48"},
	}, adminHeaders, cookies...)
	if createdBoundary.Code != http.StatusCreated {
		t.Fatalf("create Flow customer boundary: status=%d body=%s", createdBoundary.Code, createdBoundary.Body.String())
	}
	var savedBoundary struct {
		ID                  string `json:"id"`
		SourceRevision      string `json:"source_revision"`
		RuntimeState        string `json:"runtime_state"`
		PublicationRequired bool   `json:"publication_required"`
	}
	decodeJSON(t, createdBoundary, &savedBoundary)
	if savedBoundary.ID == "" || !flowSHA256(savedBoundary.SourceRevision) || savedBoundary.RuntimeState != "draft" || !savedBoundary.PublicationRequired {
		t.Fatalf("saved customer boundary did not expose draft publication state: %+v", savedBoundary)
	}
	var automaticBindings, automaticPublications int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM flow_worker_device_bindings WHERE device_id='flow-device-it'`).Scan(&automaticBindings); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM flow_enrichment_publications`).Scan(&automaticPublications); err != nil {
		t.Fatal(err)
	}
	if automaticBindings != 0 || automaticPublications != 0 {
		t.Fatalf("boundary draft mutated deployment state: bindings=%d publications=%d", automaticBindings, automaticPublications)
	}
	listedBoundaries := requestJSON(t, s, http.MethodGet,
		"/api/v1/flow/customer-bindings?device_id=flow-device-it&limit=25&offset=0", nil, nil, cookies...)
	var boundaryPage struct {
		Items []flowCustomerBoundary `json:"items"`
		Total int                    `json:"total"`
	}
	decodeJSON(t, listedBoundaries, &boundaryPage)
	if listedBoundaries.Code != http.StatusOK || boundaryPage.Total != 1 || len(boundaryPage.Items) != 1 ||
		len(boundaryPage.Items[0].SourceRanges) != 2 || boundaryPage.Items[0].CustomerName != "Integration customer" {
		t.Fatalf("list Flow customer boundaries: status=%d page=%+v body=%s", listedBoundaries.Code, boundaryPage, listedBoundaries.Body.String())
	}
	if _, err := s.db.Exec(`INSERT INTO address_prefixes
		(id,cidr,family,prefix_length,ip_start,ip_end,labels,source)
		VALUES
		('customer-prefix-it','10.0.0.0/8',4,8,INET6_ATON('10.0.0.0'),INET6_ATON('10.255.255.255'),JSON_OBJECT(),'manual'),
		('draft-only-prefix-it','172.16.0.0/12',4,12,INET6_ATON('172.16.0.0'),INET6_ATON('172.31.255.255'),JSON_OBJECT(),'manual')`); err != nil {
		t.Fatal(err)
	}

	profile := requestJSON(t, s, http.MethodGet, "/api/v1/flow/classification-profile", nil, nil, cookies...)
	if profile.Code != http.StatusOK || profile.Header().Get("ETag") != `"1"` {
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
		"device_profiles": []map[string]any{{"device_id": "flow-device-it", "source_prefix_ids": []string{"customer-prefix-it"}}},
	}, profileHeaders, cookies...)
	if savedProfile.Code != http.StatusOK || savedProfile.Header().Get("ETag") != `"2"` {
		t.Fatalf("save profile: status=%d etag=%q body=%s", savedProfile.Code, savedProfile.Header().Get("ETag"), savedProfile.Body.String())
	}
	if operationID := savedProfile.Header().Get("X-Watchdog-Operation-ID"); operationID != "" {
		waitForVPNPublishJob(t, s, operationID)
	}

	// A customer boundary is draft management data only. An administrator must
	// explicitly compose it with WADS and policy artifacts for selected workers.
	// The requested worker list is explicit. A placement recommendation may be
	// present, but it never replaces the deployment's authorization target.
	deploymentEffective := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
	deploymentRequest := map[string]any{
		"worker_ids": []string{workerID}, "device_ids": []string{"flow-device-it"}, "effective_from": deploymentEffective,
	}
	missingDevice := requestJSON(t, s, http.MethodPost, "/api/v1/flow/deployments/validate", map[string]any{
		"worker_ids": []string{workerID}, "device_ids": []string{"missing-device"}, "effective_from": deploymentEffective,
	}, adminHeaders, cookies...)
	if missingDevice.Code != http.StatusUnprocessableEntity {
		t.Fatalf("missing deployment device was accepted: status=%d body=%s", missingDevice.Code, missingDevice.Body.String())
	}
	validatedDeployment := requestJSON(t, s, http.MethodPost, "/api/v1/flow/deployments/validate", deploymentRequest, adminHeaders, cookies...)
	if validatedDeployment.Code != http.StatusOK || !bytes.Contains(validatedDeployment.Body.Bytes(), []byte(`"valid":true`)) {
		t.Fatalf("validate explicit worker deployment: status=%d body=%s", validatedDeployment.Code, validatedDeployment.Body.String())
	}
	queuedDeployment := requestJSON(t, s, http.MethodPost, "/api/v1/flow/deployments", deploymentRequest, adminHeaders, cookies...)
	var queued struct {
		Job struct {
			ID string `json:"id"`
		} `json:"job"`
	}
	decodeJSON(t, queuedDeployment, &queued)
	if queuedDeployment.Code != http.StatusAccepted || queued.Job.ID == "" {
		t.Fatalf("queue explicit Flow deployment: status=%d body=%s", queuedDeployment.Code, queuedDeployment.Body.String())
	}
	waitForVPNPublishJob(t, s, queued.Job.ID)

	var deploymentID, deploymentChecksum string
	var deploymentGeneration uint64
	if err := s.db.QueryRow(`SELECT id,generation,manifest_checksum FROM flow_worker_deployments WHERE worker_id=? AND state='desired'`, workerID).
		Scan(&deploymentID, &deploymentGeneration, &deploymentChecksum); err != nil {
		t.Fatalf("read desired Flow deployment: %v", err)
	}
	var artifactKinds int
	if err := s.db.QueryRow(`SELECT COUNT(DISTINCT kind) FROM flow_worker_deployment_artifacts WHERE deployment_id=?`, deploymentID).Scan(&artifactKinds); err != nil || artifactKinds != 3 {
		t.Fatalf("deployment did not compose three independent artifact kinds: count=%d err=%v", artifactKinds, err)
	}

	identity := flowworker.VersionWorkerIdentity{WorkerID: workerID, BootID: "boot-it", SoftwareVersion: "1.0.0"}
	httpClient := &http.Client{Transport: ginRoundTripper{handler: s.engine}}
	client, err := flowworker.NewVersionHTTPClient(flowworker.VersionHTTPClientConfig{
		BaseURL: "http://watchdog.test", AgentToken: workerToken, Identity: identity, Client: httpClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	deploymentLKG, err := flowworker.NewDiskVersionLKG(filepath.Join(root, "deployment-lkg"))
	if err != nil {
		t.Fatal(err)
	}
	deploymentCatalog, err := flowworker.NewEnrichmentVersionCatalog()
	if err != nil {
		t.Fatal(err)
	}
	deploymentSync, err := flowworker.NewRemoteDeploymentSync(client, deploymentLKG, &flowplan.TrustStore{}, deploymentCatalog, flowworker.DeploymentLoaderLimits{})
	if err != nil {
		t.Fatal(err)
	}
	deploymentResult, err := deploymentSync.SyncOnce(context.Background(), 0)
	if err != nil || deploymentResult.Installed != 1 || deploymentResult.HighestGeneration != deploymentGeneration {
		t.Fatalf("worker deployment sync: result=%+v err=%v", deploymentResult, err)
	}
	installedDeployment, ok := deploymentCatalog.ClassificationVersion(uint32(deploymentGeneration))
	if !ok {
		t.Fatalf("deployment generation %d was not activated", deploymentGeneration)
	}
	if direction, customerName, matched := installedDeployment.Classification.DeviceDirectionAttribution(
		"flow-device-it", netip.MustParseAddr("198.51.100.8"), netip.MustParseAddr("203.0.113.8"),
	); !matched || direction != flowdimension.DirectionOut || customerName != "Integration customer" {
		t.Fatalf("IPv4 customer boundary was not installed: direction=%v customer=%q matched=%v", direction, customerName, matched)
	}
	if direction, customerName, matched := installedDeployment.Classification.DeviceDirectionAttribution(
		"flow-device-it", netip.MustParseAddr("2001:db8:100::8"), netip.MustParseAddr("2001:db8:ffff::8"),
	); !matched || direction != flowdimension.DirectionOut || customerName != "Integration customer" {
		t.Fatalf("IPv6 customer boundary was not installed: direction=%v customer=%q matched=%v", direction, customerName, matched)
	}
	var deploymentACKState string
	var deploymentInstalledAt time.Time
	if err := s.db.QueryRow(`SELECT state,installed_at FROM flow_worker_deployment_acks WHERE deployment_id=? AND worker_id=?`, deploymentID, workerID).
		Scan(&deploymentACKState, &deploymentInstalledAt); err != nil || deploymentACKState != "installed" || deploymentInstalledAt.IsZero() {
		t.Fatalf("deployment ACK did not converge: state=%q installed=%v err=%v", deploymentACKState, deploymentInstalledAt, err)
	}
	listedDeployments := requestJSON(t, s, http.MethodGet,
		"/api/v1/flow/deployments?limit=25&offset=0&sort=created_at&order=desc", nil, nil, cookies...)
	if listedDeployments.Code != http.StatusOK ||
		!bytes.Contains(listedDeployments.Body.Bytes(), []byte(`"device_names":"192.0.2.10"`)) ||
		!bytes.Contains(listedDeployments.Body.Bytes(), []byte(`"ack_state":"installed"`)) {
		t.Fatalf("deployment list did not expose device and installed ACK: status=%d body=%s",
			listedDeployments.Code, listedDeployments.Body.String())
	}
	lateFailure := machineRequest(t, s, http.MethodPost, "/api/v1/flow-workers/"+workerID+"/deployments/"+deploymentID+"/acks", workerToken, map[string]any{
		"generation": deploymentGeneration, "manifest_checksum": deploymentChecksum, "boot_id": "boot-it",
		"software_version": "1.0.0", "state": "failed", "failure_stage": "ack", "failure_code": "LATE_ACK", "failure_message": "late failure",
	})
	if lateFailure.Code != http.StatusAccepted {
		t.Fatalf("late deployment ACK: status=%d body=%s", lateFailure.Code, lateFailure.Body.String())
	}
	var lateState string
	var lateStage, lateCode sql.NullString
	if err := s.db.QueryRow(`SELECT state,failure_stage,error_code FROM flow_worker_deployment_acks WHERE deployment_id=? AND worker_id=?`, deploymentID, workerID).
		Scan(&lateState, &lateStage, &lateCode); err != nil || lateState != "installed" || lateStage.Valid || lateCode.Valid {
		t.Fatalf("late failure regressed installed ACK: state=%q stage=%+v code=%+v err=%v", lateState, lateStage, lateCode, err)
	}

	deletedBoundary := requestJSON(t, s, http.MethodDelete,
		"/api/v1/flow/customer-bindings/"+boundaryPage.Items[0].ID, nil,
		map[string]string{"X-CSRF-Token": adminHeaders["X-CSRF-Token"], "If-Match": `"1"`}, cookies...)
	if deletedBoundary.Code != http.StatusUnprocessableEntity {
		t.Fatalf("delete Flow customer boundary: status=%d body=%s", deletedBoundary.Code, deletedBoundary.Body.String())
	}
	if _, err := s.db.Exec(`UPDATE flow_exporter_bindings SET enabled=0 WHERE device_id='flow-device-it'`); err != nil {
		t.Fatal(err)
	}
	deletedBoundary = requestJSON(t, s, http.MethodDelete,
		"/api/v1/flow/customer-bindings/"+boundaryPage.Items[0].ID, nil,
		map[string]string{"X-CSRF-Token": adminHeaders["X-CSRF-Token"], "If-Match": `"1"`}, cookies...)
	if deletedBoundary.Code != http.StatusOK {
		t.Fatalf("delete disabled Flow customer boundary: status=%d body=%s", deletedBoundary.Code, deletedBoundary.Body.String())
	}
	var clearedBoundary struct {
		SourceRevision      string `json:"source_revision"`
		RuntimeState        string `json:"runtime_state"`
		PublicationRequired bool   `json:"publication_required"`
	}
	decodeJSON(t, deletedBoundary, &clearedBoundary)
	if !flowSHA256(clearedBoundary.SourceRevision) || clearedBoundary.RuntimeState != "draft" || !clearedBoundary.PublicationRequired {
		t.Fatalf("deleted boundary did not expose a publishable tombstone: %+v", clearedBoundary)
	}

	// Deleting the last boundary must publish an explicit empty device artifact.
	// Otherwise the worker would retain the old CIDRs in its LKG indefinitely.
	clearRequest := map[string]any{
		"worker_ids": []string{workerID}, "device_ids": []string{"flow-device-it"},
		"effective_from": time.Now().UTC().Truncate(time.Minute).Add(2 * time.Minute),
	}
	clearDeployment := requestJSON(t, s, http.MethodPost, "/api/v1/flow/deployments", clearRequest, adminHeaders, cookies...)
	var clearQueued struct {
		Job struct {
			ID string `json:"id"`
		} `json:"job"`
	}
	decodeJSON(t, clearDeployment, &clearQueued)
	if clearDeployment.Code != http.StatusAccepted || clearQueued.Job.ID == "" {
		t.Fatalf("queue empty-boundary deployment: status=%d body=%s", clearDeployment.Code, clearDeployment.Body.String())
	}
	waitForVPNPublishJob(t, s, clearQueued.Job.ID)
	var clearDeploymentID string
	var clearGeneration uint64
	if err := s.db.QueryRow(`SELECT id,generation FROM flow_worker_deployments WHERE worker_id=? AND state='desired'`, workerID).
		Scan(&clearDeploymentID, &clearGeneration); err != nil || clearGeneration <= deploymentGeneration {
		t.Fatalf("read empty-boundary deployment: id=%q generation=%d err=%v", clearDeploymentID, clearGeneration, err)
	}
	clearResult, err := deploymentSync.SyncOnce(context.Background(), deploymentGeneration)
	if err != nil || clearResult.Installed != 1 || clearResult.HighestGeneration != clearGeneration {
		t.Fatalf("install empty-boundary deployment: result=%+v err=%v", clearResult, err)
	}
	clearedDeployment, ok := deploymentCatalog.ClassificationVersion(uint32(clearGeneration))
	if !ok {
		t.Fatalf("empty-boundary deployment generation %d was not activated", clearGeneration)
	}
	if direction, customerName, matched := clearedDeployment.Classification.DeviceDirectionAttribution(
		"flow-device-it", netip.MustParseAddr("198.51.100.8"), netip.MustParseAddr("203.0.113.8"),
	); matched || customerName != "" || direction != flowdimension.DirectionAmbiguous {
		t.Fatalf("deleted customer boundary remained active: direction=%v customer=%q matched=%v", direction, customerName, matched)
	}
	if _, err := s.db.Exec(`UPDATE flow_exporter_bindings SET enabled=1 WHERE device_id='flow-device-it'`); err != nil {
		t.Fatal(err)
	}
	staleProfile := requestJSON(t, s, http.MethodPut, "/api/v1/flow/classification-profile", map[string]any{
		"device_profiles": []map[string]any{{"device_id": "flow-device-it", "source_prefix_ids": []string{"customer-prefix-it"}}},
	}, profileHeaders, cookies...)
	if staleProfile.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale profile update was accepted: status=%d body=%s", staleProfile.Code, staleProfile.Body.String())
	}

	// The profile update also exercises the legacy v1 auto-publish job. Remove
	// its fixture before independently testing the v1 manual publication path;
	// the v2 deployment above is stored in separate tables and remains intact.
	if _, err := s.db.Exec(`DELETE FROM flow_enrichment_publications`); err != nil {
		t.Fatalf("reset v1 publication fixture: %v", err)
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
		"device_profiles": []map[string]any{{"device_id": "flow-device-it", "source_prefix_ids": []string{"draft-only-prefix-it"}}},
	}, secondProfileHeaders, cookies...)
	if secondProfile.Code != http.StatusOK || secondProfile.Header().Get("ETag") != `"3"` {
		t.Fatalf("save second profile: status=%d etag=%q body=%s", secondProfile.Code, secondProfile.Header().Get("ETag"), secondProfile.Body.String())
	}
	draftOnlyPublished := requestJSON(t, s, http.MethodPost, "/api/v1/flow/enrichment-publications", map[string]any{
		"effective_from": effectiveFrom.Add(time.Minute),
	}, adminHeaders, cookies...)
	if draftOnlyPublished.Code != http.StatusBadRequest || !bytes.Contains(draftOnlyPublished.Body.Bytes(), []byte("not present in the active address snapshot")) {
		t.Fatalf("draft-only prefix was published: status=%d body=%s", draftOnlyPublished.Code, draftOnlyPublished.Body.String())
	}
	thirdProfileHeaders := map[string]string{"X-CSRF-Token": adminHeaders["X-CSRF-Token"], "If-Match": secondProfile.Header().Get("ETag")}
	thirdProfile := requestJSON(t, s, http.MethodPut, "/api/v1/flow/classification-profile", map[string]any{
		"device_profiles": []map[string]any{{"device_id": "flow-device-it", "source_prefix_ids": []string{"customer-prefix-it"}}},
	}, thirdProfileHeaders, cookies...)
	if thirdProfile.Code != http.StatusOK || thirdProfile.Header().Get("ETag") != `"4"` {
		t.Fatalf("restore published profile: status=%d etag=%q body=%s", thirdProfile.Code, thirdProfile.Header().Get("ETag"), thirdProfile.Body.String())
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
	definition, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: snapshotID, Version: 1, EffectiveFrom: effectiveFrom,
		GeoNodes: []flowdimension.GeoNodeDefinition{
			{ID: "city-hangzhou-it", Kind: "city", Code: "330100", Name: "Hangzhou", ParentID: "province-zhejiang-it", Enabled: true},
			{ID: "continent-asia-it", Kind: "continent", Code: "AS", Name: "Asia", Enabled: true},
			{ID: "country-cn-it", Kind: "country", Code: "CN", Name: "China", ParentID: "continent-asia-it", Enabled: true},
			{ID: "province-zhejiang-it", Kind: "province", Code: "330000", Name: "Zhejiang", ParentID: "country-cn-it", Enabled: true},
		},
		Operators: []flowdimension.OperatorDefinition{{ID: "operator-it", FlowISPID: 1, Code: "IT", Name: "Integration Carrier", Category: "carrier", ASNs: []uint32{4134}, Enabled: true}},
		Prefixes: []flowdimension.PrefixDefinition{{ID: "customer-prefix-it", CIDR: "10.0.0.0/8", Labels: map[string]string{
			"geo.continent": "AS", "geo.continent_id": "continent-asia-it", "geo.country": "CN", "geo.country_id": "country-cn-it",
			"geo.province": "330000", "geo.province_id": "province-zhejiang-it", "geo.city": "330100", "geo.city_id": "city-hangzhou-it",
			"operator.id": "operator-it", "asn": "4134", "business": "customer",
		}}},
	}, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatalf("compile address definition: %v", err)
	}
	built, err := flowdimension.BuildAddressSnapshot(flowdimension.AddressSnapshotBuildInput{
		Definition: definition, BuilderVersion: "integration-test",
	}, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatalf("build WADS: %v", err)
	}
	object, err := s.addressObjects.SaveDimensionObject(context.Background(), snapshotID, built.Data)
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
