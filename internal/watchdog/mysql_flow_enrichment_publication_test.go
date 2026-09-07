package watchdog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

type recordingDimensionObjectStore struct {
	DiskDimensionObjectStore
	saved []DimensionObject
}

func (s *recordingDimensionObjectStore) SaveDimensionObject(ctx context.Context, tenantID, snapshotID ID, data []byte) (DimensionObject, error) {
	object, err := s.DiskDimensionObjectStore.SaveDimensionObject(ctx, tenantID, snapshotID, data)
	if err == nil {
		s.saved = append(s.saved, object)
	}
	return object, err
}

func TestFlowEnrichmentPublicationMigrationLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	server, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.PingContext(ctx); err != nil {
		t.Skipf("mysql not reachable: %v", err)
	}

	schema := "watchdog_enrichment_pub_" + randomSchemaSuffix(t)
	createScratchSchema(ctx, t, server, schema)
	db := openScratchSchema(t, dsn, schema)
	defer db.Close()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	migrations, err := EmbeddedMySQLMigrations()
	if err != nil {
		t.Fatal(err)
	}
	var migration MySQLMigration
	for _, candidate := range migrations {
		if candidate.Version == "059" {
			migration = candidate
			break
		}
	}
	if migration.Version == "" {
		t.Fatal("migration 059 not found")
	}
	for replay := 0; replay < 2; replay++ {
		for index, statement := range SplitSQLStatements(migration.SQL) {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				t.Fatalf("replay 059 pass %d statement %d: %v", replay+1, index+1, err)
			}
		}
	}

	const (
		tenantID      = "tenant_enrichment_pub"
		workerID      = "worker_enrichment_pub"
		collectorID   = "collect_enrichment_pub"
		snapshotID    = "snapshot_enrichment_pub"
		publicationID = "publish_enrichment_pub"
		actorID       = "actor_enrichment_pub"
	)
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Flow Enrichment', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode, status,
			observed_health, auth_type, token_hash, created_by, updated_by
		) VALUES
			(?, ?, 'flow', 'worker', 'flow_worker', 'pull', 'active', 'healthy', 'token', 'worker-token', ?, ?),
			(?, ?, 'flow', 'collector', 'flow_collect', 'listen', 'active', 'healthy', 'token', 'collector-token', ?, ?)
	`, workerID, tenantID, actorID, actorID, collectorID, tenantID, actorID, actorID); err != nil {
		t.Fatal(err)
	}
	checksum := "sha256:" + strings.Repeat("a", 64)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dimension_snapshots (
			id, tenant_id, module_key, dimension_key, version, effective_from,
			object_ref, object_format, object_format_version, builder_version, build_job_id,
			checksum, draft_digest, source_manifest_version, source_manifest,
			bundle_schema_version, entry_count, approval_state
		) VALUES (?, ?, 'flow', 'address', 7, '2026-09-07 08:00:00.000',
			'dimension-snapshots/tenant_enrichment_pub/snapshot_enrichment_pub/bundle.wads',
			'wads', 1, 'watchdog-test', 'build_enrichment_pub', ?, ?, 0, JSON_ARRAY(), 1, 1, 'approved')
	`, snapshotID, tenantID, checksum, checksum); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_classification_profiles (
			tenant_id, home_province, home_city, home_isp_ids, home_asns,
			overseas_includes_hmt, internal_policy, transit_policy,
			definition_digest, created_by, updated_by
		) VALUES (?, 'CN-11', '1101', JSON_ARRAY(1, 2), JSON_ARRAY(4134, 4837),
			FALSE, 'count', 'drop', ?, ?, ?)
	`, tenantID, strings.Repeat("b", 64), actorID, actorID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_enrichment_publications (
			id, tenant_id, classification_version, effective_from, profile_row_version,
			dimension_snapshot_id, dimension_version, dimension_effective_from,
			dimension_object_ref, dimension_object_format, dimension_object_format_version,
			dimension_checksum, classification_schema_version, classification_object_ref,
			classification_checksum, signature_algorithm, signing_key_id, signature,
			signed_at, created_by
		) VALUES (?, ?, 1, '2026-09-07 09:00:00.000', 1,
			?, 7, '2026-09-07 08:00:00.000',
			'dimension-snapshots/tenant_enrichment_pub/snapshot_enrichment_pub/bundle.wads',
			'wads', 1, ?, 1,
			'dimension-snapshots/tenant_enrichment_pub/publish_enrichment_pub/classification.json',
			?, 'ed25519', 'test-key', REPEAT('s', 64), '2026-09-07 08:59:00.000', ?)
	`, publicationID, tenantID, snapshotID, checksum, checksum, actorID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_enrichment_publication_acks (
			tenant_id, publication_id, worker_id, boot_id, software_version,
			state, attempted_at, downloaded_at, installed_at
		) VALUES (?, ?, ?, 'boot-1', 'test', 'installed',
			'2026-09-07 09:01:00.000', '2026-09-07 09:01:00.000', '2026-09-07 09:01:01.000')
	`, tenantID, publicationID, workerID); err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_enrichment_publication_acks (
			tenant_id, publication_id, worker_id, boot_id, software_version,
			state, attempted_at, downloaded_at, installed_at
		) VALUES (?, ?, ?, 'boot-2', 'test', 'installed', NOW(3), NOW(3), NOW(3))
	`, tenantID, publicationID, collectorID); err != nil {
		t.Fatal(err)
	}
	// The FK deliberately proves registry ownership only. The repository and
	// machine authenticator enforce agent_type=flow_worker at the API boundary.

	if _, err := db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID); err != nil {
		t.Fatalf("tenant cascade: %v", err)
	}
	for _, table := range []string{
		"flow_classification_profiles",
		"flow_enrichment_publications",
		"flow_enrichment_publication_acks",
	} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE tenant_id = ?", tenantID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s retained %d rows after tenant delete", table, count)
		}
	}
}

func TestMySQLFlowEnrichmentPublisherBuildsSignedImmutablePair(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	server, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.PingContext(ctx); err != nil {
		t.Skipf("mysql not reachable: %v", err)
	}

	schema := "watchdog_enrichment_repo_" + randomSchemaSuffix(t)
	createScratchSchema(ctx, t, server, schema)
	db := openScratchSchema(t, dsn, schema)
	defer db.Close()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	const (
		tenantID   = ID("tenant_enrichment_repo")
		actorID    = ID("actor_enrichment_repo")
		snapshotID = ID("snapshot_enrich_repo")
	)
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Flow Enrichment Repo', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES (?, ?, 'enrichment-repo@test.invalid', 'Flow Enrichment', 'active', 'test', 'enrichment-repo')
	`, actorID, tenantID); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	signer := newCollectorPlanSignerForTest(t, store, "enrichment-repo-key")
	base := time.Date(2026, 9, 7, 8, 0, 0, 0, time.UTC)
	if _, err := store.ActivateCollectorPlanSigningKey(ctx, signer.KeyID(), signer.PublicKey(), base.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	payloadSigner, ok := signer.(ControlPlanePayloadSigner)
	if !ok {
		t.Fatal("test signer does not implement ControlPlanePayloadSigner")
	}
	objects := &recordingDimensionObjectStore{DiskDimensionObjectStore: DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 1 << 20}}
	dimensionObject, err := objects.SaveDimensionObject(ctx, tenantID, snapshotID, []byte("WADS-test-address-snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	decidedAt := base.Add(-30 * time.Minute)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dimension_snapshots (
			id, tenant_id, module_key, dimension_key, version, effective_from,
			object_ref, object_format, object_format_version, builder_version, build_job_id,
			checksum, draft_digest, source_manifest_version, source_manifest,
			bundle_schema_version, entry_count, status, approval_state,
			decided_by, decided_at, signature_algorithm, signing_key_id, signature, signed_at, created_by
		) VALUES (?, ?, 'flow', 'address', 7, ?, ?, 'wads', 1, 'watchdog-test',
			'build_enrich_repo', ?, ?, 0, JSON_ARRAY(), 1, 1, 'active', 'approved',
			?, ?, 'ed25519', 'address-approval-key', ?, ?, ?)
	`, snapshotID, tenantID, base, dimensionObject.Ref, dimensionObject.Checksum,
		"sha256:"+strings.Repeat("d", 64), actorID, decidedAt, bytes.Repeat([]byte{1}, 64), decidedAt, actorID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dimension_snapshot_activations (
			id, tenant_id, module_key, dimension_key, snapshot_id, effective_from, reason, created_by
		) VALUES ('activate_enrich_repo', ?, 'flow', 'address', ?, ?, 'publish', ?)
	`, tenantID, snapshotID, base, actorID); err != nil {
		t.Fatal(err)
	}
	publisher, err := NewMySQLFlowEnrichmentPublisher(store, objects, payloadSigner)
	if err != nil {
		t.Fatal(err)
	}
	publisher.now = func() time.Time { return base.Add(30 * time.Minute) }
	profile, err := publisher.PutClassificationProfile(ctx, tenantID, actorID, 0, FlowClassificationProfileDraft{
		HomeProvince: "110000", HomeCity: "110100",
		HomeISPIDs: []uint16{9, 3, 9}, HomeASNs: []uint32{4837, 4134, 4837},
		OverseasIncludesHMT: true, InternalPolicy: flowdimension.RecordPolicyCount,
		TransitPolicy: flowdimension.RecordPolicyDrop,
	})
	if err != nil {
		t.Fatal(err)
	}
	if profile.RowVersion != 1 || len(profile.Draft.HomeISPIDs) != 2 || profile.Draft.HomeISPIDs[0] != 3 ||
		len(profile.Draft.HomeASNs) != 2 || profile.Draft.HomeASNs[0] != 4134 || len(profile.DefinitionDigest) != 64 {
		t.Fatalf("canonical profile = %+v", profile)
	}
	if _, err := publisher.PutClassificationProfile(ctx, tenantID, actorID, 9, profile.Draft); !errors.Is(err, ErrFlowEnrichmentConflict) {
		t.Fatalf("stale profile CAS error = %v", err)
	}

	effectiveFrom := base.Add(time.Hour)
	publication, err := publisher.Publish(ctx, tenantID, actorID, FlowEnrichmentPublishRequest{EffectiveFrom: effectiveFrom})
	if err != nil {
		t.Fatal(err)
	}
	if publication.PairSchemaVersion != 1 || publication.ClassificationVersion != 1 || publication.DimensionSnapshotID != snapshotID ||
		publication.DimensionChecksum != dimensionObject.Checksum || publication.ProfileRowVersion != profile.RowVersion || len(publication.Signature) != 64 {
		t.Fatalf("publication = %+v", publication)
	}
	classificationPath, err := objects.ResolveDimensionObject(publication.ClassificationObjectRef)
	if err != nil {
		t.Fatal(err)
	}
	classificationData, err := os.ReadFile(classificationPath)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := flowdimension.DecodeAndCompileClassificationBundle(classificationData, publication.ClassificationChecksum, flowdimension.ClassificationCompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if metadata := compiled.Metadata(); metadata.Version != 1 || metadata.DimensionSnapshotID != string(snapshotID) || !metadata.EffectiveFrom.Equal(effectiveFrom) {
		t.Fatalf("classification metadata = %+v", metadata)
	}
	trustPublication, err := store.GetCollectorPlanTrustBundle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	trust := &flowplan.TrustStore{}
	if err := trust.Install(trustPublication.BundleJSON); err != nil {
		t.Fatal(err)
	}
	envelopeData, err := flowworker.MarshalSignedEnrichmentVersionPublication(publication.SignedEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := flowworker.VerifySignedEnrichmentVersionPublication(envelopeData, trust, base.Add(31*time.Minute)); err != nil {
		t.Fatal(err)
	}
	const (
		workerID    = ID("worker_enrichment_repo")
		collectorID = ID("collect_enrichment_repo")
	)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode, status,
			observed_health, auth_type, token_hash, created_by, updated_by
		) VALUES
			(?, ?, 'flow', 'worker', 'flow_worker', 'pull', 'active', 'healthy', 'token', 'unused', ?, ?),
			(?, ?, 'flow', 'collector', 'flow_collect', 'listen', 'active', 'healthy', 'token', 'unused', ?, ?)
	`, workerID, tenantID, actorID, actorID, collectorID, tenantID, actorID, actorID); err != nil {
		t.Fatal(err)
	}
	page, err := store.ListFlowEnrichmentPublications(ctx, tenantID, 0, 1)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != publication.ID || page.HasMore {
		t.Fatalf("publication page=%+v err=%v", page, err)
	}
	acknowledgement := FlowEnrichmentAcknowledgement{
		TenantID: tenantID, PublicationID: publication.ID, WorkerID: workerID,
		FlowEnrichmentAcknowledgementReport: FlowEnrichmentAcknowledgementReport{
			State: FlowEnrichmentAckDownloaded, BootID: "boot-download", SoftwareVersion: "1.0.0",
			DimensionSnapshotID: publication.DimensionSnapshotID, DimensionVersion: publication.DimensionVersion,
			DimensionChecksum: publication.DimensionChecksum, ClassificationVersion: publication.ClassificationVersion,
			ClassificationChecksum: publication.ClassificationChecksum,
		},
		AttemptedAt: base.Add(32 * time.Minute),
	}
	if err := store.RecordFlowEnrichmentAcknowledgement(ctx, acknowledgement); err != nil {
		t.Fatal(err)
	}
	acknowledgement.State = FlowEnrichmentAckFailed
	acknowledgement.BootID = "boot-failed"
	acknowledgement.FailureStage = "verify"
	acknowledgement.FailureCode = "CHECKSUM_MISMATCH"
	acknowledgement.FailureMessage = "downloaded object did not match the signed checksum"
	acknowledgement.AttemptedAt = base.Add(33 * time.Minute)
	if err := store.RecordFlowEnrichmentAcknowledgement(ctx, acknowledgement); err != nil {
		t.Fatal(err)
	}
	acknowledgement.State = FlowEnrichmentAckInstalled
	acknowledgement.BootID = "boot-installed"
	acknowledgement.FailureStage, acknowledgement.FailureCode, acknowledgement.FailureMessage = "", "", ""
	acknowledgement.AttemptedAt = base.Add(34 * time.Minute)
	if err := store.RecordFlowEnrichmentAcknowledgement(ctx, acknowledgement); err != nil {
		t.Fatal(err)
	}
	acknowledgement.State = FlowEnrichmentAckFailed
	acknowledgement.FailureStage = "activate"
	acknowledgement.FailureCode = "RUNTIME_SWAP_FAILED"
	acknowledgement.FailureMessage = "runtime rejected the prepared catalog"
	acknowledgement.AttemptedAt = base.Add(35 * time.Minute)
	if err := store.RecordFlowEnrichmentAcknowledgement(ctx, acknowledgement); err != nil {
		t.Fatal(err)
	}
	var ackState, ackBootID, ackErrorCode string
	var downloadedAt, installedAt time.Time
	var ackRowVersion uint64
	if err := db.QueryRowContext(ctx, `
		SELECT state, boot_id, error_code, downloaded_at, installed_at, row_version
		FROM flow_enrichment_publication_acks
		WHERE tenant_id = ? AND publication_id = ? AND worker_id = ?
	`, tenantID, publication.ID, workerID).Scan(&ackState, &ackBootID, &ackErrorCode, &downloadedAt, &installedAt, &ackRowVersion); err != nil {
		t.Fatal(err)
	}
	if ackState != FlowEnrichmentAckFailed || ackBootID != "boot-installed" || ackErrorCode != "activate:RUNTIME_SWAP_FAILED" ||
		!downloadedAt.Equal(base.Add(32*time.Minute)) || !installedAt.Equal(base.Add(34*time.Minute)) || ackRowVersion != 4 {
		t.Fatalf("ack state=%s boot=%s code=%s downloaded=%s installed=%s version=%d", ackState, ackBootID, ackErrorCode, downloadedAt, installedAt, ackRowVersion)
	}
	operator, err := store.CreateISPOperator(ctx, ISPOperator{
		TenantID: tenantID, Code: "operator-query", Name: "Operator Query", Category: "carrier", ASNs: []uint32{4134}, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := store.ResolveFlowOperatorQueryBinding(ctx, tenantID, operator.ID, effectiveFrom, effectiveFrom.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if binding.OperatorID != operator.ID || binding.FlowISPID != operator.FlowISPID || binding.ExpectedWorkers != 1 ||
		len(binding.PublicationIDs) != 1 || binding.PublicationIDs[0] != publication.ID ||
		len(binding.DimensionSnapshotIDs) != 1 || binding.DimensionSnapshotIDs[0] != string(snapshotID) ||
		len(binding.ClassificationVersions) != 1 || binding.ClassificationVersions[0] != publication.ClassificationVersion {
		t.Fatalf("operator query binding = %+v", binding)
	}
	const pendingWorkerID = ID("pending_enrichment_repo")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode, status,
			observed_health, auth_type, token_hash, created_by, updated_by
		) VALUES (?, ?, 'flow', 'pending-worker', 'flow_worker', 'pull', 'active', 'unknown', 'token', 'unused', ?, ?)
	`, pendingWorkerID, tenantID, actorID, actorID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveFlowOperatorQueryBinding(ctx, tenantID, operator.ID, effectiveFrom, effectiveFrom.Add(time.Hour)); !errors.Is(err, ErrFlowOperatorQueryUnavailable) {
		t.Fatalf("missing active-worker ACK error = %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE collector_agents SET status = 'suspended' WHERE tenant_id = ? AND id = ?`, tenantID, pendingWorkerID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveFlowOperatorQueryBinding(ctx, tenantID, operator.ID, effectiveFrom.Add(-time.Minute), effectiveFrom.Add(time.Hour)); !errors.Is(err, ErrFlowOperatorQueryUnavailable) {
		t.Fatalf("range before first publication error = %v", err)
	}
	wrongPair := acknowledgement
	wrongPair.DimensionChecksum = "sha256:" + strings.Repeat("f", 64)
	if err := store.RecordFlowEnrichmentAcknowledgement(ctx, wrongPair); !errors.Is(err, ErrFlowEnrichmentAckConflict) {
		t.Fatalf("wrong pair acknowledgement error=%v", err)
	}
	collectorAck := acknowledgement
	collectorAck.WorkerID = collectorID
	if err := store.RecordFlowEnrichmentAcknowledgement(ctx, collectorAck); !errors.Is(err, ErrCollectorMachineUnauthorized) {
		t.Fatalf("collector identity acknowledgement error=%v", err)
	}
	if _, err := publisher.Publish(ctx, tenantID, actorID, FlowEnrichmentPublishRequest{EffectiveFrom: effectiveFrom}); !errors.Is(err, ErrFlowEnrichmentConflict) {
		t.Fatalf("duplicate effective-time error = %v", err)
	}
	if _, err := store.RevokeCollectorPlanSigningKey(ctx, signer.KeyID(), "test failed publication cleanup", base.Add(40*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(ctx, tenantID, actorID, FlowEnrichmentPublishRequest{EffectiveFrom: effectiveFrom.Add(time.Minute)}); !errors.Is(err, ErrCollectorPlanSigningKeyUnavailable) {
		t.Fatalf("revoked signer publication error = %v", err)
	}
	if len(objects.saved) != 3 {
		t.Fatalf("saved objects = %d, want dimension + committed classification + failed classification", len(objects.saved))
	}
	if _, err := objects.ResolveDimensionObject(objects.saved[2].Ref); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed publication object was not cleaned up: %v", err)
	}
}
