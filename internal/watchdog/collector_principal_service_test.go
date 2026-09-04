package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"
)

type collectorPrincipalServiceRepository struct {
	byID                  map[ID]CollectorServicePrincipal
	byGrantOperation      map[string]ID
	createCalls           int
	revokeCalls           int
	createErrorAfterWrite error
	revokeErrorAfterWrite error
}

func newCollectorPrincipalServiceRepository() *collectorPrincipalServiceRepository {
	return &collectorPrincipalServiceRepository{byID: map[ID]CollectorServicePrincipal{}, byGrantOperation: map[string]ID{}}
}

func (r *collectorPrincipalServiceRepository) CreateCollectorServicePrincipal(_ context.Context, grant CollectorServicePrincipalGrant) error {
	r.createCalls++
	now := time.Unix(1_000_000, 0).UTC()
	principal := CollectorServicePrincipal{
		ID: grant.ID, TenantID: grant.TenantID, CollectorID: grant.CollectorID,
		ServiceType: grant.ServiceType, PrincipalRef: grant.PrincipalRef,
		CredentialSecretRef: grant.CredentialSecretRef, Provider: grant.Provider,
		GrantOperationKey: grant.GrantOperationKey, GrantRequestHash: grant.GrantRequestHash,
		Status: "active", ACLPropagationDelay: grant.ACLPropagationDelay,
		RowVersion: 1, CreatedAt: now, UpdatedAt: now,
	}
	r.byID[principal.ID] = principal
	r.byGrantOperation[principal.GrantOperationKey] = principal.ID
	return r.createErrorAfterWrite
}

func (r *collectorPrincipalServiceRepository) RevokeCollectorServicePrincipal(_ context.Context, revocation CollectorPrincipalRevocation) error {
	r.revokeCalls++
	principal, ok := r.byID[revocation.PrincipalID]
	if !ok {
		return sql.ErrNoRows
	}
	principal.Status = "revoked"
	principal.RevokeOperationKey = revocation.OperationKey
	principal.RowVersion++
	principal.UpdatedAt = principal.UpdatedAt.Add(time.Second)
	r.byID[principal.ID] = principal
	return r.revokeErrorAfterWrite
}

func (r *collectorPrincipalServiceRepository) GetCollectorServicePrincipal(_ context.Context, tenantID, collectorID, principalID ID) (CollectorServicePrincipal, error) {
	principal, ok := r.byID[principalID]
	if !ok || principal.TenantID != tenantID || principal.CollectorID != collectorID {
		return CollectorServicePrincipal{}, sql.ErrNoRows
	}
	return principal, nil
}

func (r *collectorPrincipalServiceRepository) GetCollectorServicePrincipalByGrantOperation(_ context.Context, tenantID ID, operationKey string) (CollectorServicePrincipal, error) {
	id, ok := r.byGrantOperation[operationKey]
	if !ok {
		return CollectorServicePrincipal{}, sql.ErrNoRows
	}
	principal := r.byID[id]
	if principal.TenantID != tenantID {
		return CollectorServicePrincipal{}, sql.ErrNoRows
	}
	return principal, nil
}

type collectorPrincipalServiceProvider struct {
	name         string
	grants       map[string]CollectorPrincipalGrantProviderResult
	revocations  map[string]CollectorPrincipalRevokeProviderResult
	grantCalls   int
	revokeCalls  int
	grantError   error
	revokeError  error
	invalidGrant bool
}

func newCollectorPrincipalServiceProvider(name string) *collectorPrincipalServiceProvider {
	return &collectorPrincipalServiceProvider{
		name: name, grants: map[string]CollectorPrincipalGrantProviderResult{},
		revocations: map[string]CollectorPrincipalRevokeProviderResult{},
	}
}

func (p *collectorPrincipalServiceProvider) LookupGrant(_ context.Context, operationKey string) (CollectorPrincipalGrantProviderResult, bool, error) {
	result, ok := p.grants[operationKey]
	return result, ok, nil
}

func (p *collectorPrincipalServiceProvider) Grant(_ context.Context, request CollectorPrincipalGrantProviderRequest) (CollectorPrincipalGrantProviderResult, error) {
	p.grantCalls++
	result := CollectorPrincipalGrantProviderResult{
		OperationKey: request.OperationKey, RequestHash: request.RequestHash, Provider: p.name,
		PrincipalRef:        "User:" + string(request.PrincipalID),
		CredentialSecretRef: "secret://collectors/" + string(request.PrincipalID),
		ReceiptRef:          "provider://grants/" + request.OperationKey,
		Receipt:             []byte("provider grant receipt"),
	}
	if p.invalidGrant {
		result.RequestHash = "invalid"
	}
	p.grants[request.OperationKey] = result
	return result, p.grantError
}

func (p *collectorPrincipalServiceProvider) LookupRevoke(_ context.Context, operationKey string) (CollectorPrincipalRevokeProviderResult, bool, error) {
	result, ok := p.revocations[operationKey]
	return result, ok, nil
}

func (p *collectorPrincipalServiceProvider) RevokeWrite(_ context.Context, request CollectorPrincipalRevokeProviderRequest) (CollectorPrincipalRevokeProviderResult, error) {
	p.revokeCalls++
	result := CollectorPrincipalRevokeProviderResult{
		OperationKey: request.OperationKey, RequestHash: request.RequestHash,
		Provider: p.name, PrincipalRef: request.PrincipalRef,
		ReceiptRef: "provider://revocations/" + request.OperationKey,
		Receipt:    []byte("provider revoke receipt"),
	}
	p.revocations[request.OperationKey] = result
	return result, p.revokeError
}

func TestCollectorPrincipalServiceGrantIsProviderAndDatabaseIdempotent(t *testing.T) {
	repository := newCollectorPrincipalServiceRepository()
	provider := newCollectorPrincipalServiceProvider("kafka-admin")
	service, err := NewCollectorPrincipalService(repository, map[string]CollectorPrincipalProvider{"kafka-admin": provider})
	if err != nil {
		t.Fatal(err)
	}
	request := CollectorPrincipalGrantRequest{
		Provider: "kafka-admin", IdempotencyKey: "grant-request-1",
		ACLPropagationDelay: 2 * time.Second,
	}
	principal, err := service.Grant(context.Background(), "tenant-a", "collector-a", "user-a", request)
	if err != nil {
		t.Fatal(err)
	}
	if matched, _ := regexp.MatchString(`^p_[0-9a-f]{24}$`, string(principal.ID)); !matched || principal.Status != "active" || principal.Provider != "kafka-admin" || provider.grantCalls != 1 || repository.createCalls != 1 {
		t.Fatalf("principal=%+v provider calls=%d create calls=%d", principal, provider.grantCalls, repository.createCalls)
	}
	replayed, err := service.Grant(context.Background(), "tenant-a", "collector-a", "user-a", request)
	if err != nil || replayed.ID != principal.ID || provider.grantCalls != 1 || repository.createCalls != 1 {
		t.Fatalf("replayed=%+v err=%v provider calls=%d create calls=%d", replayed, err, provider.grantCalls, repository.createCalls)
	}
	request.ACLPropagationDelay = 3 * time.Second
	if _, err := service.Grant(context.Background(), "tenant-a", "collector-a", "user-a", request); !errors.Is(err, ErrCollectorEvidenceConflict) {
		t.Fatalf("idempotency conflict error=%v", err)
	}
}

func TestCollectorPrincipalServiceRecoversAmbiguousGrantAndDatabaseCommit(t *testing.T) {
	for _, test := range []struct {
		name          string
		providerError error
		databaseError error
	}{
		{name: "provider response lost", providerError: errors.New("provider timeout after apply")},
		{name: "database response lost", databaseError: errors.New("database connection lost after commit")},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := newCollectorPrincipalServiceRepository()
			repository.createErrorAfterWrite = test.databaseError
			provider := newCollectorPrincipalServiceProvider("kafka-admin")
			provider.grantError = test.providerError
			service, err := NewCollectorPrincipalService(repository, map[string]CollectorPrincipalProvider{"kafka-admin": provider})
			if err != nil {
				t.Fatal(err)
			}
			principal, err := service.Grant(context.Background(), "tenant-a", "collector-a", "user-a", CollectorPrincipalGrantRequest{
				Provider: "kafka-admin", IdempotencyKey: "grant-request-1", ACLPropagationDelay: time.Second,
			})
			if err != nil || principal.Status != "active" || provider.grantCalls != 1 || repository.createCalls != 1 {
				t.Fatalf("principal=%+v err=%v provider calls=%d create calls=%d", principal, err, provider.grantCalls, repository.createCalls)
			}
		})
	}
}

func TestCollectorPrincipalServiceRejectsUnboundProviderResult(t *testing.T) {
	repository := newCollectorPrincipalServiceRepository()
	provider := newCollectorPrincipalServiceProvider("kafka-admin")
	provider.invalidGrant = true
	service, err := NewCollectorPrincipalService(repository, map[string]CollectorPrincipalProvider{"kafka-admin": provider})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Grant(context.Background(), "tenant-a", "collector-a", "user-a", CollectorPrincipalGrantRequest{
		Provider: "kafka-admin", IdempotencyKey: "grant-request-1", ACLPropagationDelay: time.Second,
	})
	if !errors.Is(err, ErrCollectorPrincipalProviderResultInvalid) || repository.createCalls != 0 {
		t.Fatalf("error=%v create calls=%d", err, repository.createCalls)
	}
}

func TestCollectorPrincipalServiceRevokeIsCrashRecoverable(t *testing.T) {
	repository := newCollectorPrincipalServiceRepository()
	provider := newCollectorPrincipalServiceProvider("kafka-admin")
	service, err := NewCollectorPrincipalService(repository, map[string]CollectorPrincipalProvider{"kafka-admin": provider})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := service.Grant(context.Background(), "tenant-a", "collector-a", "user-a", CollectorPrincipalGrantRequest{
		Provider: "kafka-admin", IdempotencyKey: "grant-request-1", ACLPropagationDelay: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	provider.revokeError = errors.New("provider timeout after apply")
	repository.revokeErrorAfterWrite = errors.New("database connection lost after commit")
	revoked, err := service.RevokeWrite(context.Background(), "tenant-a", "collector-a", principal.ID, "user-a", principal.RowVersion)
	if err != nil || revoked.Status != "revoked" || revoked.RowVersion != 2 || provider.revokeCalls != 1 || repository.revokeCalls != 1 {
		t.Fatalf("revoked=%+v err=%v provider calls=%d repo calls=%d", revoked, err, provider.revokeCalls, repository.revokeCalls)
	}
	replayed, err := service.RevokeWrite(context.Background(), "tenant-a", "collector-a", principal.ID, "user-a", principal.RowVersion)
	if err != nil || replayed.RevokeOperationKey != revoked.RevokeOperationKey || provider.revokeCalls != 1 || repository.revokeCalls != 1 {
		t.Fatalf("replayed=%+v err=%v provider calls=%d repo calls=%d", replayed, err, provider.revokeCalls, repository.revokeCalls)
	}
	if _, err := service.RevokeWrite(context.Background(), "tenant-a", "collector-a", principal.ID, "user-a", revoked.RowVersion); !errors.Is(err, ErrCollectorEvidenceConflict) {
		t.Fatalf("different revocation version error=%v", err)
	}
}
