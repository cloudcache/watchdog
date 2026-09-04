package watchdog

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

type collectorPlanRuntimeRepositoryStub struct {
	plan             CollectorPlanRevision
	fetchTenantID    ID
	fetchCollectorID ID
	ack              CollectorPlanAcknowledgement
	failure          CollectorPlanFailure
	fetchErr         error
	ackErr           error
	failureErr       error
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
	registry, err := flowcollect.VerifySignedPlan(delivery.Envelope, []byte(base64.StdEncoding.EncodeToString(publicKey)), now)
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
	partitionMap := make([]uint32, flowcollect.VirtualShardCount)
	for index := range partitionMap {
		partitionMap[index] = uint32(index % 8)
	}
	flowPlan := flowcollect.Plan{
		SchemaVersion: 1, Revision: 7, CollectorID: "collector-plan-test",
		NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
		PartitionMapVersion: 1, PartitionMap: partitionMap,
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
