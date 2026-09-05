package watchdog

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCanonicalCollectorPlanJSON(t *testing.T) {
	canonical, hash, err := CanonicalCollectorPlanJSON([]byte(` { "z": 2, "a": {"b":true} } `))
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != `{"a":{"b":true},"z":2}` || !validSHA256Hex(hash) {
		t.Fatalf("canonical=%s hash=%q", canonical, hash)
	}
	canonicalAgain, hashAgain, err := CanonicalCollectorPlanJSON(canonical)
	if err != nil || !bytes.Equal(canonicalAgain, canonical) || hashAgain != hash {
		t.Fatalf("canonical round trip=%s hash=%q err=%v", canonicalAgain, hashAgain, err)
	}
	for _, invalid := range [][]byte{
		nil,
		[]byte(`[]`),
		[]byte(`{"a":1}{"b":2}`),
		[]byte(`{"a":`),
	} {
		if _, _, err := CanonicalCollectorPlanJSON(invalid); err == nil {
			t.Fatalf("invalid plan JSON accepted: %q", invalid)
		}
	}
}

func TestValidateNewCollectorPlanRevisionRejectsNonCanonicalOrUnsigned(t *testing.T) {
	base := time.Unix(2_000_000, 0).UTC()
	canonical, hash, err := CanonicalCollectorPlanJSON([]byte(`{"schema_version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	plan := CollectorPlanRevision{
		ID: "plan_validation_test_01", TenantID: "tenant_plan_test_001",
		CollectorID: "collector_plan_test_01", ConfigVersion: 1, PlanSchemaVersion: 1,
		Status: CollectorPlanValidated, SpecJSON: canonical, SpecHash: hash,
		SigningKeyID: "test-key",
		NotBefore:    base, ExpiresAt: base.Add(time.Hour), CreatedBy: "user_plan_test_00001",
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := CollectorPlanSigningPayload(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.Signature = ed25519.Sign(privateKey, payload)
	unverified := plan
	if err := ValidateNewCollectorPlanRevision(unverified); err == nil || !strings.Contains(err.Error(), "not cryptographically verified") {
		t.Fatalf("unverified signed plan error=%v", err)
	}
	plan, err = VerifyCollectorPlanRevisionSignature(plan, publicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateNewCollectorPlanRevision(plan); err != nil {
		t.Fatal(err)
	}
	mutated := plan
	mutated.SpecJSON, mutated.SpecHash, err = CanonicalCollectorPlanJSON([]byte(`{"schema_version":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateNewCollectorPlanRevision(mutated); err == nil || !strings.Contains(err.Error(), "not cryptographically verified") {
		t.Fatalf("post-verification plan mutation error=%v", err)
	}
	plan.SpecJSON = []byte(`{ "schema_version": 1 }`)
	if err := ValidateNewCollectorPlanRevision(plan); err == nil || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("non-canonical plan error=%v", err)
	}
	plan.SpecJSON = canonical
	plan.Signature = nil
	if err := ValidateNewCollectorPlanRevision(plan); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("unsigned plan error=%v", err)
	}
	plan = unverified
	plan.Signature[0] ^= 0xff
	if _, err := VerifyCollectorPlanRevisionSignature(plan, publicKey); err == nil || !strings.Contains(err.Error(), "verification failed") {
		t.Fatalf("tampered signature error=%v", err)
	}
}

func TestMySQLCollectorPlanLifecycle(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_COLLECTOR_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_COLLECTOR_MYSQL_TEST_DSN to run collector plan integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := ApplyMySQLMigrations(context.Background(), db); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	tenantID := ID("tenant_plan_lifecycle_01")
	userID := ID("user_plan_lifecycle_0001")
	targetID := ID("target_plan_lifecycle_01")
	collectorID := ID("collector_plan_lifecycle")
	if _, err := db.ExecContext(ctx, "DELETE FROM tenants WHERE id = ?", tenantID); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = db.ExecContext(context.Background(), "DELETE FROM tenants WHERE id = ?", tenantID) }()
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Collector Plan Test', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO users (id, tenant_id, email, name, status) VALUES (?, ?, 'collector-plan@watchdog.local', 'Collector Plan', 'active')", userID, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO targets (id, tenant_id, name, kind, host, status) VALUES (?, ?, 'Plan Target', 'network', '192.0.2.20', 'pending')", targetID, tenantID); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	if _, err := store.UpsertAgent(ctx, SNMPAgentConfig{
		ID: collectorID, TenantID: tenantID, TargetID: targetID, AgentType: AgentTypeSNMP,
		Mode: AgentModePull, TokenHash: "collector-plan-token", Status: AgentStatusPending,
	}); err != nil {
		t.Fatal(err)
	}

	base := time.Now().UTC().Truncate(time.Millisecond)
	plan1 := collectorPlanFixture(t, "collector_plan_rev_00001", tenantID, collectorID, userID, 1, 0, base)
	created1, err := store.CreateCollectorPlanRevision(ctx, plan1)
	if err != nil {
		t.Fatal(err)
	}
	if created1.Status != CollectorPlanValidated || created1.RowVersion != 1 {
		t.Fatalf("created plan 1=%+v", created1)
	}
	active1, err := store.ActivateCollectorPlanRevision(ctx, CollectorPlanActivation{
		TenantID: tenantID, CollectorID: collectorID, ConfigVersion: 1,
		ExpectedCollectorRowVersion: 1, ExpectedPlanRowVersion: created1.RowVersion,
		ActorID: userID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if active1.Status != CollectorPlanActive || active1.RowVersion != 2 || active1.ActivatedAt.IsZero() {
		t.Fatalf("active plan 1=%+v", active1)
	}
	assertCollectorPlanHead(t, db, collectorID, 1, 0, 0, active1.SpecHash, "active", 2)

	ack1 := CollectorPlanAcknowledgement{
		TenantID: tenantID, CollectorID: collectorID, ConfigVersion: 1,
		SpecHash: active1.SpecHash, BootID: "boot-plan-1", SoftwareVersion: "1.0.0",
	}
	if err := store.AcknowledgeCollectorPlan(ctx, ack1); err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeCollectorPlan(ctx, ack1); err != nil {
		t.Fatalf("idempotent plan acknowledgement: %v", err)
	}
	assertCollectorPlanHead(t, db, collectorID, 1, 1, 1, active1.SpecHash, "active", 2)

	plan2 := collectorPlanFixture(t, "collector_plan_rev_00002", tenantID, collectorID, userID, 2, 1, base.Add(2*time.Second))
	created2, err := store.CreateCollectorPlanRevision(ctx, plan2)
	if err != nil {
		t.Fatal(err)
	}
	active2, err := store.ActivateCollectorPlanRevision(ctx, CollectorPlanActivation{
		TenantID: tenantID, CollectorID: collectorID, ConfigVersion: 2,
		ExpectedCollectorRowVersion: 2, ExpectedPlanRowVersion: created2.RowVersion,
		ActorID: userID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if active2.Status != CollectorPlanActive || active2.RowVersion != 2 {
		t.Fatalf("active plan 2=%+v", active2)
	}
	retired1, err := store.GetCollectorPlanRevision(ctx, tenantID, collectorID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if retired1.Status != CollectorPlanRetired || retired1.RowVersion != 3 || !retired1.RetiredAt.Equal(active2.ActivatedAt) {
		t.Fatalf("retired plan 1=%+v", retired1)
	}
	assertCollectorPlanHead(t, db, collectorID, 2, 1, 1, active2.SpecHash, "active", 3)
	delivered2, err := store.GetActiveCollectorPlan(ctx, tenantID, collectorID)
	if err != nil {
		t.Fatal(err)
	}
	if delivered2.ID != active2.ID || delivered2.ConfigVersion != 2 || delivered2.SpecHash != active2.SpecHash {
		t.Fatalf("delivered active plan=%+v", delivered2)
	}
	if err := store.RecordCollectorPlanFailure(ctx, CollectorPlanFailure{
		TenantID: tenantID, CollectorID: collectorID,
		CollectorPlanFailureReport: CollectorPlanFailureReport{
			FailedConfigVersion: 2, BootID: "boot-plan-2", SoftwareVersion: "1.1.0",
			Stage: "dependency", Code: "KAFKA_CONTRACT", Detail: "topic contract unavailable",
		},
	}); err != nil {
		t.Fatal(err)
	}
	var observedHealth, bootID, softwareVersion, lastErrorCode, lastErrorDetail string
	var lastSeenAt sql.NullTime
	var collectorRowVersion uint64
	if err := db.QueryRowContext(ctx, `
		SELECT observed_health, boot_id, software_version, COALESCE(last_error_code, ''),
			COALESCE(last_error_detail, ''), last_seen_at, row_version
		FROM collector_agents WHERE tenant_id = ? AND id = ?
	`, tenantID, collectorID).Scan(
		&observedHealth, &bootID, &softwareVersion, &lastErrorCode,
		&lastErrorDetail, &lastSeenAt, &collectorRowVersion,
	); err != nil {
		t.Fatal(err)
	}
	if observedHealth != "degraded" || bootID != "boot-plan-1" || softwareVersion != "1.1.0" || lastErrorCode != "dependency:KAFKA_CONTRACT" || lastErrorDetail != "topic contract unavailable" || !lastSeenAt.Valid || collectorRowVersion != 3 {
		t.Fatalf("collector failure state health=%q boot=%q software=%q code=%q detail=%q seen=%v row=%d", observedHealth, bootID, softwareVersion, lastErrorCode, lastErrorDetail, lastSeenAt, collectorRowVersion)
	}
	runtimeReport := collectorRuntimeHeartbeatFixture()
	runtimeReport.BootID = bootID
	runtimeReport.ActiveConfigVersion = 1
	runtimeReport.ActiveSpecHash = retired1.SpecHash
	runtimeHeartbeat, err := prepareCollectorRuntimeHeartbeat(CollectorMachineIdentity{TenantID: tenantID, CollectorID: collectorID}, runtimeReport, base.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCollectorRuntimeHeartbeat(ctx, runtimeHeartbeat); err != nil {
		t.Fatal(err)
	}
	replayedHeartbeat, err := prepareCollectorRuntimeHeartbeat(CollectorMachineIdentity{TenantID: tenantID, CollectorID: collectorID}, runtimeReport, base.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCollectorRuntimeHeartbeat(ctx, replayedHeartbeat); err != nil {
		t.Fatalf("exact runtime heartbeat replay was not idempotent: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT last_seen_at FROM collector_agents WHERE tenant_id = ? AND id = ?", tenantID, collectorID).Scan(&lastSeenAt); err != nil {
		t.Fatal(err)
	}
	if !lastSeenAt.Valid || lastSeenAt.Time.UnixMilli() != replayedHeartbeat.ReceivedAt.UnixMilli() {
		t.Fatalf("exact replay did not refresh server-observed liveness: %v", lastSeenAt)
	}
	conflictingReport := runtimeReport
	conflictingReport.Observation.Counters.ReceivedDatagrams++
	conflictingHeartbeat, err := prepareCollectorRuntimeHeartbeat(CollectorMachineIdentity{TenantID: tenantID, CollectorID: collectorID}, conflictingReport, base.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCollectorRuntimeHeartbeat(ctx, conflictingHeartbeat); !errors.Is(err, ErrCollectorHeartbeatFenced) {
		t.Fatalf("same sequence conflicting heartbeat error=%v", err)
	}
	runtimeReport.BootID = "boot-plan-2"
	runtimeReport.Sequence = 1
	runtimeReport.ActiveConfigVersion = 2
	runtimeReport.ActiveSpecHash = active2.SpecHash
	runtimeHeartbeat, err = prepareCollectorRuntimeHeartbeat(CollectorMachineIdentity{TenantID: tenantID, CollectorID: collectorID}, runtimeReport, base.Add(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCollectorRuntimeHeartbeat(ctx, runtimeHeartbeat); err != nil {
		t.Fatal(err)
	}
	var runtimeSchema uint16
	var heartbeatSequence uint64
	var heartbeatSentAt time.Time
	var clockOffset int64
	var capabilitiesHash, observationHash, heartbeatPayloadHash string
	var capabilitiesJSON, observationJSON []byte
	if err := db.QueryRowContext(ctx, `
		SELECT observed_health, boot_id, runtime_schema_version, heartbeat_sequence,
			heartbeat_sent_at, clock_offset_ms, capabilities_json, capabilities_hash,
			runtime_observation_json, runtime_observation_hash, heartbeat_payload_hash,
			COALESCE(last_error_code, ''), COALESCE(last_error_detail, ''), row_version
		FROM collector_agents WHERE tenant_id = ? AND id = ?
	`, tenantID, collectorID).Scan(
		&observedHealth, &bootID, &runtimeSchema, &heartbeatSequence,
		&heartbeatSentAt, &clockOffset, &capabilitiesJSON, &capabilitiesHash,
		&observationJSON, &observationHash, &heartbeatPayloadHash,
		&lastErrorCode, &lastErrorDetail, &collectorRowVersion,
	); err != nil {
		t.Fatal(err)
	}
	if observedHealth != "healthy" || bootID != "boot-plan-2" || runtimeSchema != 1 || heartbeatSequence != 1 || heartbeatSentAt.UnixMilli() != runtimeReport.SentAtUnixMilli || clockOffset != runtimeHeartbeat.ClockOffsetMilliseconds || len(capabilitiesJSON) == 0 || len(observationJSON) == 0 || !validSHA256Hex(capabilitiesHash) || !validSHA256Hex(observationHash) || !validSHA256Hex(heartbeatPayloadHash) || lastErrorCode != "" || lastErrorDetail != "" || collectorRowVersion != 3 {
		t.Fatalf("collector runtime state health=%q boot=%q schema=%d seq=%d sent=%s offset=%d capabilities=%s/%s observation=%s/%s payload=%s errors=%q/%q row=%d", observedHealth, bootID, runtimeSchema, heartbeatSequence, heartbeatSentAt, clockOffset, capabilitiesJSON, capabilitiesHash, observationJSON, observationHash, heartbeatPayloadHash, lastErrorCode, lastErrorDetail, collectorRowVersion)
	}
	counterRollbackReport := runtimeReport
	counterRollbackReport.Sequence = 2
	counterRollbackReport.Observation.Counters.ReceivedDatagrams--
	counterRollbackHeartbeat, err := prepareCollectorRuntimeHeartbeat(CollectorMachineIdentity{TenantID: tenantID, CollectorID: collectorID}, counterRollbackReport, base.Add(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCollectorRuntimeHeartbeat(ctx, counterRollbackHeartbeat); !errors.Is(err, ErrCollectorHeartbeatFenced) {
		t.Fatalf("same-boot counter rollback error=%v", err)
	}
	degradedReport := runtimeReport
	degradedReport.Sequence = 2
	degradedReport.Observation.ControlPlaneHealthy = false
	degradedHeartbeat, err := prepareCollectorRuntimeHeartbeat(CollectorMachineIdentity{TenantID: tenantID, CollectorID: collectorID}, degradedReport, base.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCollectorRuntimeHeartbeat(ctx, degradedHeartbeat); err != nil {
		t.Fatalf("degraded runtime heartbeat failed: %v", err)
	}
	staleBootAck := CollectorPlanAcknowledgement{
		TenantID: tenantID, CollectorID: collectorID, ConfigVersion: 2,
		SpecHash: active2.SpecHash, BootID: "boot-plan-1", SoftwareVersion: "1.0.0",
	}
	if err := store.AcknowledgeCollectorPlan(ctx, staleBootAck); !errors.Is(err, ErrCollectorHeartbeatFenced) {
		t.Fatalf("previous boot reclaimed plan acknowledgement stream: %v", err)
	}
	currentBootAck := staleBootAck
	currentBootAck.BootID = "boot-plan-2"
	currentBootAck.SoftwareVersion = "1.1.0"
	if err := store.AcknowledgeCollectorPlan(ctx, currentBootAck); err != nil {
		t.Fatalf("current boot acknowledgement failed: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT observed_health FROM collector_agents WHERE tenant_id = ? AND id = ?", tenantID, collectorID).Scan(&observedHealth); err != nil {
		t.Fatal(err)
	}
	if observedHealth != "degraded" {
		t.Fatalf("plan acknowledgement masked runtime health: %q", observedHealth)
	}
	if err := store.RecordCollectorPlanFailure(ctx, CollectorPlanFailure{
		TenantID: tenantID, CollectorID: collectorID,
		CollectorPlanFailureReport: CollectorPlanFailureReport{
			FailedConfigVersion: 2, BootID: "boot-plan-1", SoftwareVersion: "1.0.0",
			Stage: "activate", Code: "STALE_PROCESS", Detail: "stale process failure",
		},
	}); !errors.Is(err, ErrCollectorHeartbeatFenced) {
		t.Fatalf("previous boot reported plan failure after takeover: %v", err)
	}
	staleBootReport := runtimeReport
	staleBootReport.BootID = "boot-plan-1"
	staleBootReport.Sequence = 2
	staleBootHeartbeat, err := prepareCollectorRuntimeHeartbeat(CollectorMachineIdentity{TenantID: tenantID, CollectorID: collectorID}, staleBootReport, base.Add(9*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCollectorRuntimeHeartbeat(ctx, staleBootHeartbeat); !errors.Is(err, ErrCollectorHeartbeatFenced) {
		t.Fatalf("previous boot reclaimed heartbeat stream: %v", err)
	}
	if err := store.RecordCollectorPlanFailure(ctx, CollectorPlanFailure{
		TenantID: tenantID, CollectorID: collectorID,
		CollectorPlanFailureReport: CollectorPlanFailureReport{
			FailedConfigVersion: 1, BootID: "boot-stale", Stage: "verify", Code: "SIGNATURE_INVALID",
		},
	}); !errors.Is(err, ErrCollectorPlanInvalidTransition) {
		t.Fatalf("stale plan failure error=%v", err)
	}
	wrongAck := ack1
	wrongAck.ConfigVersion = 2
	if err := store.AcknowledgeCollectorPlan(ctx, wrongAck); !errors.Is(err, ErrCollectorPlanInvalidTransition) {
		t.Fatalf("wrong plan hash acknowledgement error=%v", err)
	}

	if _, err := store.ActivateCollectorPlanRevision(ctx, CollectorPlanActivation{
		TenantID: tenantID, CollectorID: collectorID, ConfigVersion: 1,
		ExpectedCollectorRowVersion: 3, ExpectedPlanRowVersion: retired1.RowVersion,
		ActorID: userID,
	}); !errors.Is(err, ErrCollectorPlanInvalidTransition) {
		t.Fatalf("retired plan reactivation error=%v", err)
	}
	plan3 := collectorPlanFixture(t, "collector_plan_rev_00003", tenantID, collectorID, userID, 3, 2, base.Add(3*time.Second))
	created3, err := store.CreateCollectorPlanRevision(ctx, plan3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivateCollectorPlanRevision(ctx, CollectorPlanActivation{
		TenantID: tenantID, CollectorID: collectorID, ConfigVersion: 3,
		ExpectedCollectorRowVersion: 2, ExpectedPlanRowVersion: created3.RowVersion,
		ActorID: userID,
	}); !errors.Is(err, ErrCollectorPlanConflict) {
		t.Fatalf("stale collector row version error=%v", err)
	}
	var activeCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM collector_plan_revisions WHERE collector_id = ? AND status = 'active'", collectorID).Scan(&activeCount); err != nil || activeCount != 1 {
		t.Fatalf("active plan count=%d err=%v", activeCount, err)
	}
	var auditCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit_logs WHERE tenant_id = ? AND resource_type = 'collector_plan'", tenantID).Scan(&auditCount); err != nil || auditCount != 7 {
		t.Fatalf("collector plan audit count=%d err=%v", auditCount, err)
	}
	if _, err := store.GetCollectorPlanRevision(ctx, "tenant_other_plan_001", collectorID, 2); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("cross-tenant plan read error=%v", err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE collector_plan_revisions SET signature = 'x' WHERE id = ?", created3.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetCollectorPlanRevision(ctx, tenantID, collectorID, 3); err == nil || !strings.Contains(err.Error(), "stored collector plan revision is invalid") {
		t.Fatalf("tampered stored signature error=%v", err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE collector_plan_revisions SET spec_json = '{\"tampered\":true}' WHERE id = ?", active2.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetCollectorPlanRevision(ctx, tenantID, collectorID, 2); err == nil || !strings.Contains(err.Error(), "canonical spec or hash") {
		t.Fatalf("tampered stored plan error=%v", err)
	}
}

func collectorPlanFixture(t *testing.T, id string, tenantID, collectorID, userID ID, version, supersedes uint64, at time.Time) CollectorPlanRevision {
	t.Helper()
	canonical, hash, err := CanonicalCollectorPlanJSON([]byte(`{"collector_id":"` + string(collectorID) + `","revision":` + strconv.FormatUint(version, 10) + `,"schema_version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	plan := CollectorPlanRevision{
		ID: ID(id), TenantID: tenantID, CollectorID: collectorID,
		ConfigVersion: version, PlanSchemaVersion: 1, Status: CollectorPlanValidated,
		SpecJSON: canonical, SpecHash: hash, SigningKeyID: "collector-plan-test-key",
		ValidationJSON: []byte(`{"valid":true}`),
		NotBefore:      at.Add(-time.Minute), ExpiresAt: at.Add(time.Hour),
		SupersedesConfigVersion: supersedes, CreatedBy: userID,
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := CollectorPlanSigningPayload(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.Signature = ed25519.Sign(privateKey, payload)
	verified, err := VerifyCollectorPlanRevisionSignature(plan, publicKey)
	if err != nil {
		t.Fatal(err)
	}
	return verified
}

func assertCollectorPlanHead(t *testing.T, db *sql.DB, collectorID ID, config, acknowledged, lastGood uint64, hash, status string, rowVersion uint64) {
	t.Helper()
	var gotConfig, gotAcknowledged, gotLastGood, gotRowVersion uint64
	var gotHash, gotStatus string
	if err := db.QueryRow(`
		SELECT config_version, acknowledged_config_version, last_good_config_version,
			plan_hash, status, row_version
		FROM collector_agents WHERE id = ?
	`, collectorID).Scan(&gotConfig, &gotAcknowledged, &gotLastGood, &gotHash, &gotStatus, &gotRowVersion); err != nil {
		t.Fatal(err)
	}
	if gotConfig != config || gotAcknowledged != acknowledged || gotLastGood != lastGood || gotHash != hash || gotStatus != status || gotRowVersion != rowVersion {
		t.Fatalf("collector plan head=(%d,%d,%d,%q,%q,%d)", gotConfig, gotAcknowledged, gotLastGood, gotHash, gotStatus, gotRowVersion)
	}
}
