package watchdog

import (
	"context"
	"errors"
	"testing"
)

type collectorEvidenceServiceAuthenticator struct {
	identity   CollectorMachineIdentity
	credential CollectorMachineCredential
	collector  ID
	err        error
}

func (a *collectorEvidenceServiceAuthenticator) AuthenticateCollector(_ context.Context, collectorID ID, credential CollectorMachineCredential) (CollectorMachineIdentity, error) {
	a.collector, a.credential = collectorID, credential
	return a.identity, a.err
}

type collectorEvidenceServiceRecorder struct {
	drain CollectorDrainReceipt
}

func (r *collectorEvidenceServiceRecorder) RecordCollectorDrain(_ context.Context, receipt CollectorDrainReceipt) error {
	r.drain = receipt
	return nil
}

func TestCollectorEvidenceServiceInjectsAuthenticatedIdentity(t *testing.T) {
	authenticator := &collectorEvidenceServiceAuthenticator{identity: CollectorMachineIdentity{
		TenantID: "tenant-a", CollectorID: "collector-a", BootID: "boot-a",
	}}
	recorder := &collectorEvidenceServiceRecorder{}
	service, err := NewCollectorEvidenceService(authenticator, recorder)
	if err != nil {
		t.Fatal(err)
	}
	credential := CollectorMachineCredential{Token: "secret-token"}
	if err := service.RecordDrain(context.Background(), "collector-a", credential, CollectorDrainReport{
		TransferID: "transfer-a", AppliedConfigVersion: 12, ReceiptNonce: "drain-1",
	}); err != nil {
		t.Fatal(err)
	}
	if authenticator.collector != "collector-a" || authenticator.credential.Token != "secret-token" || recorder.drain.TenantID != "tenant-a" || recorder.drain.AuthenticatedCollectorID != "collector-a" || recorder.drain.BootID != "boot-a" {
		t.Fatalf("authenticator=%+v drain=%+v", authenticator, recorder.drain)
	}
}

func TestCollectorEvidenceServiceRejectsMismatchedAuthenticatedIdentity(t *testing.T) {
	authenticator := &collectorEvidenceServiceAuthenticator{identity: CollectorMachineIdentity{
		TenantID: "tenant-a", CollectorID: "collector-b", BootID: "boot-b",
	}}
	service, err := NewCollectorEvidenceService(authenticator, &collectorEvidenceServiceRecorder{})
	if err != nil {
		t.Fatal(err)
	}
	err = service.RecordDrain(context.Background(), "collector-a", CollectorMachineCredential{Token: "secret-token"}, CollectorDrainReport{
		TransferID: "transfer-a", AppliedConfigVersion: 12, ReceiptNonce: "drain-1",
	})
	if !errors.Is(err, ErrCollectorMachineUnauthorized) {
		t.Fatalf("error=%v", err)
	}
}
