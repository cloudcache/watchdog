package watchdog

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"
)

type collectorPlanManagementRepositoryStub struct {
	createRequest CollectorPlanCreateRequest
	createSigner  CollectorPlanSigner
	createCalls   int
}

func (r *collectorPlanManagementRepositoryStub) CreateNextCollectorPlanRevision(_ context.Context, request CollectorPlanCreateRequest, signer CollectorPlanSigner) (CollectorPlanRevision, error) {
	r.createRequest, r.createSigner = request, signer
	r.createCalls++
	return CollectorPlanRevision{ConfigVersion: 1}, nil
}
func (*collectorPlanManagementRepositoryStub) CreateCollectorPlanRevision(context.Context, CollectorPlanRevision) (CollectorPlanRevision, error) {
	return CollectorPlanRevision{}, nil
}
func (*collectorPlanManagementRepositoryStub) ListCollectorPlanRevisions(context.Context, ID, ID, CollectorPlanPageFilter) ([]CollectorPlanRevision, string, error) {
	return nil, "", nil
}
func (*collectorPlanManagementRepositoryStub) GetCollectorPlanRevision(context.Context, ID, ID, uint64) (CollectorPlanRevision, error) {
	return CollectorPlanRevision{}, nil
}
func (*collectorPlanManagementRepositoryStub) ActivateCollectorPlanRevision(context.Context, CollectorPlanActivation) (CollectorPlanRevision, error) {
	return CollectorPlanRevision{}, nil
}
func (*collectorPlanManagementRepositoryStub) AcknowledgeCollectorPlan(context.Context, CollectorPlanAcknowledgement) error {
	return nil
}

type collectorPlanSignerStub struct{}

func (collectorPlanSignerStub) KeyID() string { return "key-a" }
func (collectorPlanSignerStub) PublicKey() ed25519.PublicKey {
	return make(ed25519.PublicKey, ed25519.PublicKeySize)
}
func (collectorPlanSignerStub) Sign(context.Context, CollectorPlanRevision) (CollectorPlanRevision, error) {
	return CollectorPlanRevision{}, errors.New("not used by service stub")
}

func TestCollectorPlanManagementServiceRequiresSignerAndValidInput(t *testing.T) {
	repository := &collectorPlanManagementRepositoryStub{}
	service, err := NewCollectorPlanManagementService(repository, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	request := CollectorPlanCreateRequest{
		TenantID: "tenant-a", CollectorID: "collector-a", ActorID: "user-a",
		PlanSchemaVersion: 1, SpecJSON: []byte(`{"a":1}`), ExpiresAt: now.Add(time.Hour),
	}
	if _, err := service.CreateCollectorPlanRevision(context.Background(), request); !errors.Is(err, ErrCollectorPlanSigningKeyUnavailable) {
		t.Fatalf("missing signer error=%v", err)
	}
	service, err = NewCollectorPlanManagementService(repository, collectorPlanSignerStub{})
	if err != nil {
		t.Fatal(err)
	}
	invalid := request
	invalid.FromConfigVersion = 1
	if _, err := service.CreateCollectorPlanRevision(context.Background(), invalid); !errors.Is(err, ErrCollectorPlanInvalidRequest) || repository.createCalls != 0 {
		t.Fatalf("invalid request err=%v calls=%d", err, repository.createCalls)
	}
	created, err := service.CreateCollectorPlanRevision(context.Background(), request)
	if err != nil || created.ConfigVersion != 1 || repository.createCalls != 1 || repository.createSigner == nil || repository.createRequest.ActorID != "user-a" {
		t.Fatalf("created=%+v err=%v repository=%+v", created, err, repository)
	}
}

func TestCollectorPlanCursorRoundTripAndRejectsMalformed(t *testing.T) {
	for _, version := range []uint64{1, 42, ^uint64(0)} {
		decoded, err := decodeCollectorPlanCursor(encodeCollectorPlanCursor(version))
		if err != nil || decoded != version {
			t.Fatalf("version=%d decoded=%d err=%v", version, decoded, err)
		}
	}
	for _, cursor := range []string{"", "%%%", "MA", "LTE"} {
		if _, err := decodeCollectorPlanCursor(cursor); !errors.Is(err, ErrCollectorPlanInvalidRequest) {
			t.Fatalf("cursor=%q err=%v", cursor, err)
		}
	}
}
