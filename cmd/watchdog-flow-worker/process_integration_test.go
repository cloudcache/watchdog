// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/watchdog"
	mysql "github.com/go-sql-driver/mysql"
)

const (
	workerProcessTenantID   = watchdog.ID("tenant-worker-process")
	workerProcessUserID     = watchdog.ID("user-worker-process")
	workerProcessID         = watchdog.ID("worker-process")
	workerProcessSnapshotID = watchdog.ID("snapshot-worker-process")
	workerProcessToken      = "wdc_worker_process_secret"
)

// TestFlowWorkerRealMySQLProcessLKGAndFailedUpgrade closes the remaining
// process boundary: production HTTP services use real MySQL identity and
// publication repositories, while separate worker processes install, reject a
// corrupt next publication without replacing LKG, and restart offline.
func TestFlowWorkerRealMySQLProcessLKGAndFailedUpgrade(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, cleanup := openWorkerProcessScratchMySQL(t, ctx, dsn)
	defer cleanup()
	if _, err := watchdog.ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}

	directory := t.TempDir()
	objects := watchdog.DiskDimensionObjectStore{Dir: filepath.Join(directory, "objects"), MaxBytes: 4 << 20}
	store := watchdog.NewMySQLStore(db)
	publisher := prepareWorkerProcessPublicationStore(t, ctx, db, store, objects, directory)
	base := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
	first := publishWorkerProcessVersion(t, ctx, publisher, 0, base.Add(time.Minute), nil)

	authenticator, err := watchdog.NewMySQLFlowWorkerMachineAuthenticator(db)
	if err != nil {
		t.Fatal(err)
	}
	trust, err := watchdog.NewCollectorPlanTrustBundleService(authenticator, store)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := watchdog.NewFlowEnrichmentDeliveryService(authenticator, store, objects, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(watchdog.NewAPIV1Router(watchdog.APIV1RouterConfig{
		FlowWorkerTrust: trust, FlowEnrichmentDelivery: delivery,
	}))
	defer server.Close()

	planPath, planKeyPath := writeWorkerProcessPlan(t, directory)
	tokenPath := filepath.Join(directory, "worker.token")
	if err := os.WriteFile(tokenPath, []byte(workerProcessToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	lkgDirectory := filepath.Join(directory, "lkg")
	firstOutput := runWorkerProcess(t, server.URL, planPath, planKeyPath, tokenPath, lkgDirectory)
	if !strings.Contains(firstOutput, "expected ClickHouse boundary exit") {
		t.Fatalf("first worker did not cross production startup boundary:\n%s", firstOutput)
	}
	assertWorkerProcessACK(t, ctx, db, first.ID, watchdog.FlowEnrichmentAckInstalled, "")

	second := publishWorkerProcessVersion(t, ctx, publisher, 1, base.Add(2*time.Minute), []uint32{64500})
	classificationPath, err := objects.ResolveDimensionObject(second.ClassificationObjectRef)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(classificationPath, []byte("corrupt-publication"), 0o640); err != nil {
		t.Fatal(err)
	}
	failedOutput := runWorkerProcess(t, server.URL, planPath, planKeyPath, tokenPath, lkgDirectory)
	if !strings.Contains(failedOutput, "using LKG version 1") {
		t.Fatalf("failed upgrade did not retain version 1 LKG:\n%s", failedOutput)
	}
	assertWorkerProcessACK(t, ctx, db, second.ID, watchdog.FlowEnrichmentAckFailed, "verify:CLASSIFICATION_OBJECT_INVALID")
	assertWorkerProcessACK(t, ctx, db, first.ID, watchdog.FlowEnrichmentAckInstalled, "")

	server.Close()
	offlineOutput := runWorkerProcess(t, server.URL, planPath, planKeyPath, tokenPath, lkgDirectory)
	if !strings.Contains(offlineOutput, "using LKG version 1") {
		t.Fatalf("offline restart did not restore version 1 LKG:\n%s", offlineOutput)
	}
}

// TestFlowWorkerProcessHelper runs only in a child test binary. Calling run
// exercises the production startup lifecycle while a deliberately closed
// ClickHouse endpoint gives the parent a deterministic process exit.
func TestFlowWorkerProcessHelper(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_WORKER_PROCESS_HELPER") != "1" {
		return
	}
	opt := options{
		planFiles:                  stringList{os.Getenv("WATCHDOG_FLOW_WORKER_TEST_PLAN")},
		planPublicKey:              os.Getenv("WATCHDOG_FLOW_WORKER_TEST_PLAN_KEY"),
		workerID:                   string(workerProcessID),
		controlPlaneURL:            os.Getenv("WATCHDOG_FLOW_WORKER_TEST_CONTROL_URL"),
		agentTokenFile:             os.Getenv("WATCHDOG_FLOW_WORKER_TEST_TOKEN_FILE"),
		versionLKGDir:              os.Getenv("WATCHDOG_FLOW_WORKER_TEST_LKG_DIR"),
		versionRefreshInterval:     5 * time.Second,
		controlPlaneTimeout:        5 * time.Second,
		brokers:                    "127.0.0.1:9092",
		topic:                      "watchdog.flow.process-test",
		sourceStreamID:             "process-test:raw-v1:incarnation-1",
		clientID:                   "watchdog-flow-worker-process-test",
		consumerGroup:              "watchdog-flow-worker-process-test",
		saslMechanism:              "none",
		fetchMinBytes:              1,
		fetchMaxWait:               time.Second,
		templateReplayRecords:      1,
		decoderStateTTL:            time.Minute,
		clickHouseAddress:          "127.0.0.1:1",
		clickHouseDatabase:         "watchdog_flow_process_test",
		clickHouseUser:             "default",
		clickHouseMaxConns:         1,
		clickHouseMinConns:         0,
		clickHouseDialTimeout:      100 * time.Millisecond,
		clickHouseReadTimeout:      time.Second,
		clickHouseOperationTimeout: time.Second,
		blockMaxRows:               1,
		blockMaxBytes:              1 << 20,
	}
	err := run(opt)
	if err == nil || !strings.Contains(err.Error(), "ClickHouse") {
		t.Fatalf("worker must reach the expected ClickHouse boundary, got %v", err)
	}
	t.Logf("expected ClickHouse boundary exit: %v", err)
}

func openWorkerProcessScratchMySQL(t testing.TB, ctx context.Context, dsn string) (*sql.DB, func()) {
	t.Helper()
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.DBName = ""
	config.ParseTime = true
	config.MultiStatements = true
	server, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	if err := server.PingContext(ctx); err != nil {
		server.Close()
		t.Skipf("mysql not reachable: %v", err)
	}
	schema := fmt.Sprintf("watchdog_worker_process_%d", time.Now().UnixNano())
	if _, err := server.ExecContext(ctx, "CREATE DATABASE `"+schema+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		server.Close()
		t.Fatal(err)
	}
	databaseConfig := *config
	databaseConfig.DBName = schema
	db, err := sql.Open("mysql", databaseConfig.FormatDSN())
	if err != nil {
		_, _ = server.ExecContext(context.Background(), "DROP DATABASE `"+schema+"`")
		server.Close()
		t.Fatal(err)
	}
	cleanup := func() {
		_ = db.Close()
		dropCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = server.ExecContext(dropCtx, "DROP DATABASE `"+schema+"`")
		_ = server.Close()
	}
	return db, cleanup
}

func prepareWorkerProcessPublicationStore(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	store *watchdog.MySQLStore,
	objects watchdog.DiskDimensionObjectStore,
	directory string,
) *watchdog.MySQLFlowEnrichmentPublisher {
	t.Helper()
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Worker Process', 'active')`, workerProcessTenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES (?, ?, 'worker-process@test.invalid', 'Worker Process', 'active', 'test', 'worker-process')
	`, workerProcessUserID, workerProcessTenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode, status,
			observed_health, auth_type, token_hash, created_by, updated_by
		) VALUES (?, ?, 'flow', 'worker-process', 'flow_worker', 'pull', 'active',
			'healthy', 'token', ?, ?, ?)
	`, workerProcessID, workerProcessTenantID, watchdog.NewAgentTokenHash(workerProcessToken), workerProcessUserID, workerProcessUserID); err != nil {
		t.Fatal(err)
	}

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(directory, "control-plane.key")
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(privateKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	signer, err := watchdog.LoadCollectorPlanSigner(watchdog.CollectorPlanSigningConfig{
		KeyID: "flow-worker-process-key", PrivateKeyFile: keyPath,
	}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivateCollectorPlanSigningKey(ctx, signer.KeyID(), signer.PublicKey(), time.Now().UTC().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	payloadSigner, ok := signer.(watchdog.ControlPlanePayloadSigner)
	if !ok {
		t.Fatal("collector plan signer does not support control-plane payloads")
	}

	base := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
	dimensionData, dimensionChecksum := buildWorkerProcessWADS(t, base)
	dimensionObject, err := objects.SaveDimensionObject(ctx, workerProcessTenantID, workerProcessSnapshotID, dimensionData)
	if err != nil {
		t.Fatal(err)
	}
	if dimensionObject.Checksum != dimensionChecksum {
		t.Fatal("saved AddressSnap checksum changed")
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dimension_snapshots (
			id, tenant_id, module_key, dimension_key, version, effective_from,
			object_ref, object_format, object_format_version, builder_version, build_job_id,
			checksum, draft_digest, source_manifest_version, source_manifest,
			bundle_schema_version, entry_count, status, approval_state,
			decided_by, decided_at, signature_algorithm, signing_key_id, signature, signed_at, created_by
		) VALUES (?, ?, 'flow', 'address', 1, ?, ?, 'wads', 1, 'process-test',
			'build-worker-process', ?, ?, 0, JSON_ARRAY(), 1, 1, 'active', 'approved',
			?, ?, 'ed25519', 'address-process-key', ?, ?, ?)
	`, workerProcessSnapshotID, workerProcessTenantID, base, dimensionObject.Ref,
		dimensionObject.Checksum, dimensionObject.Checksum, workerProcessUserID, base,
		bytes.Repeat([]byte{1}, ed25519.SignatureSize), base, workerProcessUserID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_activations (
			id, tenant_id, module_key, dimension_key, snapshot_id, effective_from, reason, created_by
		) VALUES ('activate-worker-process', ?, 'flow', 'address', ?, ?, 'publish', ?)
	`, workerProcessTenantID, workerProcessSnapshotID, base, workerProcessUserID); err != nil {
		t.Fatal(err)
	}
	publisher, err := watchdog.NewMySQLFlowEnrichmentPublisher(store, objects, payloadSigner)
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}

func buildWorkerProcessWADS(t testing.TB, effectiveFrom time.Time) ([]byte, string) {
	t.Helper()
	definition, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion,
		SnapshotID:    string(workerProcessSnapshotID), TenantID: string(workerProcessTenantID),
		Version: 1, EffectiveFrom: effectiveFrom,
		Prefixes: []flowdimension.PrefixDefinition{{
			ID: "local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local"},
		}},
	}, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	built, err := flowdimension.BuildAddressSnapshot(flowdimension.AddressSnapshotBuildInput{
		Definition: definition, BuilderVersion: "process-test",
	}, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	return built.Data, built.ChecksumSHA256
}

func publishWorkerProcessVersion(
	t testing.TB,
	ctx context.Context,
	publisher *watchdog.MySQLFlowEnrichmentPublisher,
	expectedProfileVersion uint64,
	effectiveFrom time.Time,
	homeASNs []uint32,
) watchdog.FlowEnrichmentPublication {
	t.Helper()
	profile, err := publisher.PutClassificationProfile(ctx, workerProcessTenantID, workerProcessUserID, expectedProfileVersion, watchdog.FlowClassificationProfileDraft{
		HomeASNs: homeASNs, InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	publication, err := publisher.Publish(ctx, workerProcessTenantID, workerProcessUserID, watchdog.FlowEnrichmentPublishRequest{EffectiveFrom: effectiveFrom})
	if err != nil {
		t.Fatal(err)
	}
	if publication.ProfileRowVersion != profile.RowVersion {
		t.Fatalf("publication profile version=%d want %d", publication.ProfileRowVersion, profile.RowVersion)
	}
	return publication
}

func writeWorkerProcessPlan(t testing.TB, directory string) (string, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	planData, err := json.Marshal(flowplan.Plan{
		SchemaVersion: 2, Revision: 1, CollectorID: "collector-process",
		NotBefore: time.Now().UTC().Add(-time.Hour), ExpiresAt: time.Now().UTC().Add(time.Hour),
		Sources: []flowplan.SourceBinding{{
			Protocol: flowplan.ProtocolNetFlow5, SourcePrefix: "192.0.2.0/24",
			TenantID: string(workerProcessTenantID), ExporterID: "exporter-process", TargetID: "target-process",
			OwnershipEpoch: 1, SamplingMode: flowplan.SamplingModePreScaled, Enabled: true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	envelopeData, err := json.Marshal(struct {
		SchemaVersion uint32 `json:"schema_version"`
		Payload       string `json:"payload"`
		Signature     string `json:"signature"`
	}{
		SchemaVersion: 1,
		Payload:       base64.StdEncoding.EncodeToString(planData),
		Signature:     base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, planData)),
	})
	if err != nil {
		t.Fatal(err)
	}
	planPath := filepath.Join(directory, "bootstrap-plan.json")
	keyPath := filepath.Join(directory, "bootstrap-plan.pub")
	if err := os.WriteFile(planPath, envelopeData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, []byte(base64.StdEncoding.EncodeToString(publicKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	return planPath, keyPath
}

func runWorkerProcess(t testing.TB, controlURL, planPath, planKeyPath, tokenPath, lkgDirectory string) string {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestFlowWorkerProcessHelper$", "-test.v")
	command.Env = append(os.Environ(),
		"WATCHDOG_FLOW_WORKER_PROCESS_HELPER=1",
		"WATCHDOG_FLOW_WORKER_TEST_CONTROL_URL="+controlURL,
		"WATCHDOG_FLOW_WORKER_TEST_PLAN="+planPath,
		"WATCHDOG_FLOW_WORKER_TEST_PLAN_KEY="+planKeyPath,
		"WATCHDOG_FLOW_WORKER_TEST_TOKEN_FILE="+tokenPath,
		"WATCHDOG_FLOW_WORKER_TEST_LKG_DIR="+lkgDirectory,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("worker child process: %v\n%s", err, output)
	}
	return string(output)
}

func assertWorkerProcessACK(t testing.TB, ctx context.Context, db *sql.DB, publicationID watchdog.ID, state, errorCode string) {
	t.Helper()
	var gotState, gotErrorCode string
	var downloaded, installed sql.NullTime
	if err := db.QueryRowContext(ctx, `
		SELECT state, COALESCE(error_code, ''), downloaded_at, installed_at
		FROM flow_enrichment_publication_acks
		WHERE tenant_id = ? AND publication_id = ? AND worker_id = ?
	`, workerProcessTenantID, publicationID, workerProcessID).Scan(&gotState, &gotErrorCode, &downloaded, &installed); err != nil {
		t.Fatal(err)
	}
	if gotState != state || gotErrorCode != errorCode {
		t.Fatalf("publication %s ACK=%s/%s want %s/%s", publicationID, gotState, gotErrorCode, state, errorCode)
	}
	if state == watchdog.FlowEnrichmentAckInstalled && (!downloaded.Valid || !installed.Valid) {
		t.Fatalf("publication %s installed ACK lacks milestones", publicationID)
	}
}
