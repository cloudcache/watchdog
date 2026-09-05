package watchdog

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
)

type collectorPlanRuntimeRepositoryStub struct {
	plan             CollectorPlanRevision
	fetchTenantID    ID
	fetchCollectorID ID
	ack              CollectorPlanAcknowledgement
	failure          CollectorPlanFailure
	heartbeat        CollectorRuntimeHeartbeat
	fetchErr         error
	ackErr           error
	failureErr       error
	heartbeatErr     error
}

func (r *collectorPlanRuntimeRepositoryStub) GetActiveCollectorPlan(_ context.Context, tenantID, collectorID ID) (CollectorPlanRevision, error) {
	r.fetchTenantID, r.fetchCollectorID = tenantID, collectorID
	return r.plan, r.fetchErr
}

func (r *collectorPlanRuntimeRepositoryStub) AcknowledgeCollectorPlan(_ context.Context, acknowledgement CollectorPlanAcknowledgement) error {
	r.ack = acknowledgement
	return r.ackErr
}

func (r *collectorPlanRuntimeRepositoryStub) RecordCollectorPlanFailure(_ context.Context, failure CollectorPlanFailure) error {
	r.failure = failure
	return r.failureErr
}

func (r *collectorPlanRuntimeRepositoryStub) RecordCollectorRuntimeHeartbeat(_ context.Context, heartbeat CollectorRuntimeHeartbeat) error {
	r.heartbeat = heartbeat
	return r.heartbeatErr
}

func TestCollectorPlanDeliveryUsesAuthenticatedIdentityAndStoredSignature(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	plan, publicKey := collectorPlanDeliveryFixture(t, now)
	authenticator := &collectorEvidenceServiceAuthenticator{identity: CollectorMachineIdentity{
		TenantID: plan.TenantID, CollectorID: plan.CollectorID,
	}}
	repository := &collectorPlanRuntimeRepositoryStub{plan: plan}
	service, err := NewCollectorPlanDeliveryService(authenticator, repository)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return now }
	credential := CollectorMachineCredential{Token: "collector-token"}
	delivery, err := service.Fetch(context.Background(), plan.CollectorID, credential)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.ETag != collectorPlanETag(plan.ConfigVersion, plan.SpecHash) || delivery.ConfigVersion != plan.ConfigVersion || delivery.SpecHash != plan.SpecHash {
		t.Fatalf("unexpected delivery metadata: %+v", delivery)
	}
	if repository.fetchTenantID != plan.TenantID || repository.fetchCollectorID != plan.CollectorID {
		t.Fatalf("fetch identity was not injected: tenant=%q collector=%q", repository.fetchTenantID, repository.fetchCollectorID)
	}
	registry, err := flowplan.VerifySignedPlan(delivery.Envelope, []byte(base64.StdEncoding.EncodeToString(publicKey)), now)
	if err != nil {
		t.Fatal(err)
	}
	if registry.Plan().CollectorID != string(plan.CollectorID) || registry.Plan().Revision != plan.ConfigVersion {
		t.Fatalf("unexpected delivered registry: %+v", registry.Plan())
	}

	err = service.Acknowledge(context.Background(), plan.CollectorID, credential, CollectorPlanAcknowledgement{
		TenantID: "injected-tenant", CollectorID: "injected-collector",
		ConfigVersion: plan.ConfigVersion, SpecHash: plan.SpecHash, BootID: "boot-a", SoftwareVersion: "1.0.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if repository.ack.TenantID != plan.TenantID || repository.ack.CollectorID != plan.CollectorID {
		t.Fatalf("acknowledgement identity was not injected: %+v", repository.ack)
	}

	err = service.ReportFailure(context.Background(), plan.CollectorID, credential, CollectorPlanFailureReport{
		FailedConfigVersion: plan.ConfigVersion, BootID: "boot-b", SoftwareVersion: "1.0.1",
		Stage: "dependency", Code: "KAFKA_CONTRACT", Detail: "topic contract unavailable",
	})
	if err != nil {
		t.Fatal(err)
	}
	if repository.failure.TenantID != plan.TenantID || repository.failure.CollectorID != plan.CollectorID || repository.failure.Stage != "dependency" {
		t.Fatalf("failure identity was not injected: %+v", repository.failure)
	}
}

func TestCollectorPlanAcknowledgementRejectsInvalidEvidenceBeforeRepositoryWrite(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	plan, _ := collectorPlanDeliveryFixture(t, now)
	repository := &collectorPlanRuntimeRepositoryStub{plan: plan}
	service, err := NewCollectorPlanDeliveryService(&collectorEvidenceServiceAuthenticator{identity: CollectorMachineIdentity{
		TenantID: plan.TenantID, CollectorID: plan.CollectorID,
	}}, repository)
	if err != nil {
		t.Fatal(err)
	}
	err = service.Acknowledge(context.Background(), plan.CollectorID, CollectorMachineCredential{Token: "token"}, CollectorPlanAcknowledgement{
		ConfigVersion: plan.ConfigVersion, SpecHash: plan.SpecHash, BootID: "boot id with spaces",
	})
	if err == nil || repository.ack.ConfigVersion != 0 {
		t.Fatalf("invalid acknowledgement error=%v repository_ack=%+v", err, repository.ack)
	}
}

func TestCollectorPlanDeliveryRejectsExpiredOrMismatchedIdentity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	plan, _ := collectorPlanDeliveryFixture(t, now)
	repository := &collectorPlanRuntimeRepositoryStub{plan: plan}
	authenticator := &collectorEvidenceServiceAuthenticator{identity: CollectorMachineIdentity{TenantID: plan.TenantID, CollectorID: plan.CollectorID}}
	service, err := NewCollectorPlanDeliveryService(authenticator, repository)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return plan.ExpiresAt }
	if _, err := service.Fetch(context.Background(), plan.CollectorID, CollectorMachineCredential{Token: "token"}); !errors.Is(err, ErrCollectorPlanUnavailable) {
		t.Fatalf("expired plan error=%v", err)
	}
	authenticator.identity.CollectorID = "other-collector"
	if _, err := service.Fetch(context.Background(), plan.CollectorID, CollectorMachineCredential{Token: "token"}); !errors.Is(err, ErrCollectorMachineUnauthorized) {
		t.Fatalf("mismatched identity error=%v", err)
	}
}

func TestCollectorPlanDeliveryRuntimeHeartbeatUsesAuthenticatedIdentityAndServerTime(t *testing.T) {
	receivedAt := time.UnixMilli(2_000_000_000_250).UTC()
	repository := &collectorPlanRuntimeRepositoryStub{}
	service, err := NewCollectorPlanDeliveryService(&collectorEvidenceServiceAuthenticator{identity: CollectorMachineIdentity{
		TenantID: "tenant-runtime", CollectorID: "collector-runtime", BootID: "stored-boot",
	}}, repository)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return receivedAt }
	report := collectorRuntimeHeartbeatFixture()
	if err := service.ReportRuntime(context.Background(), "collector-runtime", CollectorMachineCredential{Token: "secret"}, report); err != nil {
		t.Fatal(err)
	}
	heartbeat := repository.heartbeat
	if heartbeat.TenantID != "tenant-runtime" || heartbeat.CollectorID != "collector-runtime" || !heartbeat.ReceivedAt.Equal(receivedAt) || heartbeat.ClockOffsetMilliseconds != 250 {
		t.Fatalf("heartbeat server facts=%+v", heartbeat)
	}
	if !validSHA256Hex(heartbeat.CapabilitiesHash) || !validSHA256Hex(heartbeat.ObservationHash) || !validSHA256Hex(heartbeat.PayloadHash) || len(heartbeat.CapabilitiesJSON) == 0 || len(heartbeat.ObservationJSON) == 0 {
		t.Fatalf("heartbeat canonical payload=%+v", heartbeat)
	}
	report.Observation.Queues.Kafka.Depth = report.Observation.Queues.Kafka.Capacity + 1
	if err := service.ReportRuntime(context.Background(), "collector-runtime", CollectorMachineCredential{Token: "secret"}, report); err == nil {
		t.Fatal("invalid runtime heartbeat was accepted")
	}
}

func collectorRuntimeHeartbeatFixture() CollectorRuntimeHeartbeatReport {
	return CollectorRuntimeHeartbeatReport{
		SchemaVersion: 1, Sequence: 1, SentAtUnixMilli: 2_000_000_000_000,
		BootID: "boot-runtime", SoftwareVersion: "1.2.3", AgentAPIVersion: 1,
		PlanSchemaMin: 1, PlanSchemaMax: 1, ActiveConfigVersion: 7,
		ActiveSpecHash: strings.Repeat("a", 64),
		Capabilities: CollectorRuntimeCapabilities{
			SchemaVersion: 1, Protocols: []string{"ipfix", "netflow5", "netflow9", "sflow5"},
			PlanEnvelopeVersions: []uint16{2},
		},
		Observation: CollectorRuntimeObservation{
			Running: true, UptimeSeconds: 10, PlanAccepting: true,
			ControlPlaneHealthy: true, KafkaHealthy: true,
			Queues:   CollectorRuntimeQueues{Kafka: CollectorQueueObservation{Depth: 2, Capacity: 20}},
			Counters: CollectorRuntimeCounters{ReceivedDatagrams: 100, RejectedSources: 1, UDPKernelDrops: 2, KafkaRecords: 99, KafkaBytes: 4096, PublishFailures: 4},
		},
	}
}

func TestCollectorRuntimeHeartbeatSequenceAndHealthTransitions(t *testing.T) {
	hashA := strings.Repeat("a", 64)
	hashB := strings.Repeat("b", 64)
	for _, test := range []struct {
		name                                   string
		previousBoot, boot, previousHash, hash string
		previousSequence, sequence             uint64
		idempotent, conflict                   bool
	}{
		{name: "next", previousBoot: "boot-a", boot: "boot-a", previousHash: hashA, hash: hashB, previousSequence: 1, sequence: 2},
		{name: "exact replay", previousBoot: "boot-a", boot: "boot-a", previousHash: hashA, hash: hashA, previousSequence: 1, sequence: 1, idempotent: true},
		{name: "same sequence different payload", previousBoot: "boot-a", boot: "boot-a", previousHash: hashA, hash: hashB, previousSequence: 1, sequence: 1, conflict: true},
		{name: "sequence rollback", previousBoot: "boot-a", boot: "boot-a", previousHash: hashA, hash: hashA, previousSequence: 2, sequence: 1, conflict: true},
		{name: "new boot starts at one", previousBoot: "boot-a", boot: "boot-b", previousHash: hashA, hash: hashB, previousSequence: 9, sequence: 1},
		{name: "new boot skips first", previousBoot: "boot-a", boot: "boot-b", previousHash: hashA, hash: hashB, previousSequence: 9, sequence: 2, conflict: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			idempotent, err := validateCollectorHeartbeatSequence(test.previousBoot, test.previousSequence, test.previousHash, test.boot, test.sequence, test.hash)
			if idempotent != test.idempotent || errors.Is(err, ErrCollectorHeartbeatFenced) != test.conflict || (err != nil) != test.conflict {
				t.Fatalf("idempotent=%v conflict=%v err=%v", idempotent, test.conflict, err)
			}
		})
	}
	previousCounters := CollectorRuntimeCounters{ReceivedDatagrams: 10, RejectedSources: 2, UDPKernelDrops: 1}
	currentCounters := previousCounters
	currentCounters.ReceivedDatagrams++
	if !collectorRuntimeCountersMonotonic(previousCounters, currentCounters) {
		t.Fatal("increasing runtime counters were rejected")
	}
	currentCounters.RejectedSources--
	if collectorRuntimeCountersMonotonic(previousCounters, currentCounters) {
		t.Fatal("runtime counter rollback was accepted")
	}
	heartbeat, err := prepareCollectorRuntimeHeartbeat(CollectorMachineIdentity{TenantID: "tenant", CollectorID: "collector"}, collectorRuntimeHeartbeatFixture(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got := collectorRuntimeObservedHealth(heartbeat, heartbeat.ActiveConfigVersion, true); got != "healthy" {
		t.Fatalf("healthy observation=%q", got)
	}
	if got := collectorRuntimeObservedHealth(heartbeat, heartbeat.ActiveConfigVersion+1, true); got != "warming" {
		t.Fatalf("LKG observation=%q", got)
	}
	heartbeat.Observation.Queues.Kafka.Depth = heartbeat.Observation.Queues.Kafka.Capacity
	if got := collectorRuntimeObservedHealth(heartbeat, heartbeat.ActiveConfigVersion+1, true); got != "degraded" {
		t.Fatalf("unhealthy LKG observation=%q", got)
	}
	if got := collectorRuntimeObservedHealth(heartbeat, heartbeat.ActiveConfigVersion, true); got != "degraded" {
		t.Fatalf("full Kafka queue observation=%q", got)
	}
	heartbeat.Observation.Queues.Kafka.Depth = 0
	if got := collectorRuntimeObservedHealth(heartbeat, heartbeat.ActiveConfigVersion, false); got != "degraded" {
		t.Fatalf("incompatible schema observation=%q", got)
	}
	heartbeat.Observation.ControlPlaneHealthy = false
	if got := collectorRuntimeObservedHealth(heartbeat, heartbeat.ActiveConfigVersion, true); got != "degraded" {
		t.Fatalf("unhealthy control plane observation=%q", got)
	}
}

func TestCollectorPlanFailureReportUsesBoundedFixedTaxonomy(t *testing.T) {
	valid := CollectorPlanFailureReport{BootID: "boot-a", Stage: "verify", Code: "SIGNATURE_INVALID", Detail: "signature verification failed"}
	if err := validateCollectorPlanFailureReport(valid); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*CollectorPlanFailureReport){
		func(report *CollectorPlanFailureReport) { report.Stage = "other" },
		func(report *CollectorPlanFailureReport) { report.Code = "dynamic error" },
		func(report *CollectorPlanFailureReport) { report.Detail = "line one\nline two" },
	} {
		report := valid
		mutate(&report)
		if err := validateCollectorPlanFailureReport(report); err == nil {
			t.Fatalf("invalid failure report was accepted: %+v", report)
		}
	}
}

func collectorPlanDeliveryFixture(t *testing.T, now time.Time) (CollectorPlanRevision, ed25519.PublicKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	flowPlan := flowplan.Plan{
		SchemaVersion: 1, Revision: 7, CollectorID: "collector-plan-test",
		NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
	}
	raw, err := json.Marshal(flowPlan)
	if err != nil {
		t.Fatal(err)
	}
	canonical, hash, err := CanonicalCollectorPlanJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	plan := CollectorPlanRevision{
		ID: "plan-delivery-test", TenantID: "tenant-plan-test", CollectorID: ID(flowPlan.CollectorID),
		ConfigVersion: flowPlan.Revision, PlanSchemaVersion: uint16(flowPlan.SchemaVersion),
		Status: CollectorPlanValidated, SpecJSON: canonical, SpecHash: hash,
		SigningKeyID: "key-test", NotBefore: flowPlan.NotBefore, ExpiresAt: flowPlan.ExpiresAt,
		CreatedBy: "user-plan-test",
	}
	signingPayload, err := CollectorPlanSigningPayload(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.Signature = ed25519.Sign(privateKey, signingPayload)
	plan.Status = CollectorPlanActive
	plan.ActivatedAt = now
	return plan, publicKey
}
