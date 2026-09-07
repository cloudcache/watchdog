package watchdog

import (
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowworker"
)

type flowEnrichmentDeliveryRepositoryStub struct {
	page            FlowEnrichmentPublicationPage
	publication     FlowEnrichmentPublication
	acknowledgement FlowEnrichmentAcknowledgement
	tenantID        ID
	afterVersion    uint32
	limit           int
	err             error
}

func (r *flowEnrichmentDeliveryRepositoryStub) ListFlowEnrichmentPublications(_ context.Context, tenantID ID, afterVersion uint32, limit int) (FlowEnrichmentPublicationPage, error) {
	r.tenantID, r.afterVersion, r.limit = tenantID, afterVersion, limit
	return r.page, r.err
}

func (r *flowEnrichmentDeliveryRepositoryStub) GetFlowEnrichmentPublication(_ context.Context, tenantID, _ ID) (FlowEnrichmentPublication, error) {
	r.tenantID = tenantID
	return r.publication, r.err
}

func (r *flowEnrichmentDeliveryRepositoryStub) RecordFlowEnrichmentAcknowledgement(_ context.Context, acknowledgement FlowEnrichmentAcknowledgement) error {
	r.acknowledgement = acknowledgement
	return r.err
}

func TestFlowEnrichmentDeliveryScopesAndEncodesDesiredPublications(t *testing.T) {
	publication := flowEnrichmentDeliveryPublicationFixture()
	repository := &flowEnrichmentDeliveryRepositoryStub{page: FlowEnrichmentPublicationPage{Items: []FlowEnrichmentPublication{publication}, HasMore: true}}
	authenticator := &collectorEvidenceServiceAuthenticator{identity: CollectorMachineIdentity{TenantID: publication.TenantID, CollectorID: "worker-a"}}
	service, err := NewFlowEnrichmentDeliveryService(authenticator, repository, DiskDimensionObjectStore{Dir: t.TempDir()}, 0)
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.FetchDesired(context.Background(), "worker-a", CollectorMachineCredential{Token: "token"}, 6, 10)
	if err != nil {
		t.Fatal(err)
	}
	if repository.tenantID != publication.TenantID || repository.afterVersion != 6 || repository.limit != 10 || len(page.Items) != 1 || page.NextVersion != 7 || !page.HasMore {
		t.Fatalf("repository=%+v page=%+v", repository, page)
	}
	if _, err := flowworker.MarshalSignedEnrichmentVersionPublication(publication.SignedEnvelope()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(page.Items[0].Envelope), `"publication_id":"publication-a"`) {
		t.Fatalf("envelope=%s", page.Items[0].Envelope)
	}
	authenticator.identity.CollectorID = "another-worker"
	if _, err := service.FetchDesired(context.Background(), "worker-a", CollectorMachineCredential{Token: "token"}, 0, 20); !errors.Is(err, ErrCollectorMachineUnauthorized) {
		t.Fatalf("mismatched machine identity error=%v", err)
	}
}

func TestFlowEnrichmentDeliveryResolvesOnlyPublicationOwnedObjects(t *testing.T) {
	ctx := context.Background()
	publication := flowEnrichmentDeliveryPublicationFixture()
	objects := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 1 << 20}
	dimension, err := objects.SaveDimensionObject(ctx, publication.TenantID, publication.DimensionSnapshotID, []byte("WADS-object"))
	if err != nil {
		t.Fatal(err)
	}
	classification, err := objects.SaveDimensionObject(ctx, publication.TenantID, publication.ID, []byte(`{"schema_version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	publication.DimensionObjectRef, publication.DimensionChecksum = dimension.Ref, dimension.Checksum
	publication.ClassificationObjectRef, publication.ClassificationChecksum = classification.Ref, classification.Checksum
	repository := &flowEnrichmentDeliveryRepositoryStub{publication: publication}
	service, _ := NewFlowEnrichmentDeliveryService(&collectorEvidenceServiceAuthenticator{identity: CollectorMachineIdentity{TenantID: publication.TenantID, CollectorID: "worker-a"}}, repository, objects, 1<<20)
	delivery, err := service.ResolveObject(ctx, "worker-a", CollectorMachineCredential{Token: "token"}, publication.ID, "dimension")
	if err != nil || delivery.Path == "" || delivery.Size != int64(len("WADS-object")) || delivery.Checksum != dimension.Checksum {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	if _, err := service.ResolveObject(ctx, "worker-a", CollectorMachineCredential{Token: "token"}, publication.ID, "arbitrary"); !errors.Is(err, ErrFlowEnrichmentDeliveryInvalid) {
		t.Fatalf("arbitrary kind error=%v", err)
	}
	if err := os.Remove(delivery.Path); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ResolveObject(ctx, "worker-a", CollectorMachineCredential{Token: "token"}, publication.ID, "dimension"); !errors.Is(err, ErrFlowEnrichmentObjectMissing) {
		t.Fatalf("missing object error=%v", err)
	}
}

func TestFlowEnrichmentDeliveryAcknowledgementDerivesMachineScope(t *testing.T) {
	publication := flowEnrichmentDeliveryPublicationFixture()
	repository := &flowEnrichmentDeliveryRepositoryStub{}
	service, _ := NewFlowEnrichmentDeliveryService(&collectorEvidenceServiceAuthenticator{identity: CollectorMachineIdentity{TenantID: publication.TenantID, CollectorID: "worker-a"}}, repository, DiskDimensionObjectStore{Dir: t.TempDir()}, 0)
	attemptedAt := time.Date(2026, 9, 7, 10, 0, 0, 123000000, time.UTC)
	service.now = func() time.Time { return attemptedAt }
	report := flowEnrichmentAcknowledgementFixture()
	if err := service.Acknowledge(context.Background(), "worker-a", CollectorMachineCredential{Token: "token"}, publication.ID, report); err != nil {
		t.Fatal(err)
	}
	ack := repository.acknowledgement
	if ack.TenantID != publication.TenantID || ack.WorkerID != "worker-a" || ack.PublicationID != publication.ID || !ack.AttemptedAt.Equal(attemptedAt) {
		t.Fatalf("acknowledgement=%+v", ack)
	}
	report.State = FlowEnrichmentAckFailed
	if err := service.Acknowledge(context.Background(), "worker-a", CollectorMachineCredential{Token: "token"}, publication.ID, report); !errors.Is(err, ErrFlowEnrichmentDeliveryInvalid) {
		t.Fatalf("incomplete failure error=%v", err)
	}
}

func flowEnrichmentDeliveryPublicationFixture() FlowEnrichmentPublication {
	effective := time.Date(2026, 9, 7, 9, 0, 0, 0, time.UTC)
	return FlowEnrichmentPublication{
		ID: "publication-a", TenantID: "tenant-a", PairSchemaVersion: 1,
		ClassificationVersion: 7, EffectiveFrom: effective, ProfileRowVersion: 2,
		DimensionSnapshotID: "snapshot-a", DimensionVersion: 4, DimensionEffectiveFrom: effective.Add(-time.Hour),
		DimensionObjectRef:    "dimension-snapshots/tenant-a/snapshot-a/address-snapshot.wads",
		DimensionObjectFormat: "wads", DimensionObjectFormatVersion: 1, DimensionChecksum: "sha256:" + strings.Repeat("a", 64),
		ClassificationSchemaVersion: 1,
		ClassificationObjectRef:     "dimension-snapshots/tenant-a/publication-a/bundle.json",
		ClassificationChecksum:      "sha256:" + strings.Repeat("b", 64),
		SignatureAlgorithm:          flowworker.EnrichmentVersionSignatureAlgorithm, SigningKeyID: "signing-key",
		Signature: make([]byte, ed25519.SignatureSize), SignedAt: effective.Add(-time.Minute),
	}
}

func flowEnrichmentAcknowledgementFixture() FlowEnrichmentAcknowledgementReport {
	publication := flowEnrichmentDeliveryPublicationFixture()
	return FlowEnrichmentAcknowledgementReport{
		State: FlowEnrichmentAckInstalled, BootID: "boot-a", SoftwareVersion: "1.2.3",
		DimensionSnapshotID: publication.DimensionSnapshotID, DimensionVersion: publication.DimensionVersion,
		DimensionChecksum: publication.DimensionChecksum, ClassificationVersion: publication.ClassificationVersion,
		ClassificationChecksum: publication.ClassificationChecksum,
	}
}
