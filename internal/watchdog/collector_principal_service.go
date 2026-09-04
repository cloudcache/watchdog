package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrCollectorPrincipalProviderNotConfigured = errors.New("collector principal provider is not configured")
	ErrCollectorPrincipalProviderResultInvalid = errors.New("collector principal provider result is invalid")
	ErrCollectorPrincipalVersionConflict       = errors.New("collector principal row version conflict")
)

type CollectorPrincipalGrantProviderRequest struct {
	OperationKey        string
	RequestHash         string
	PrincipalID         ID
	TenantID            ID
	CollectorID         ID
	ServiceType         string
	ACLPropagationDelay time.Duration
}

type CollectorPrincipalRevokeProviderRequest struct {
	OperationKey string
	RequestHash  string
	TenantID     ID
	CollectorID  ID
	PrincipalID  ID
	ServiceType  string
	PrincipalRef string
}

type CollectorPrincipalGrantProviderResult struct {
	OperationKey        string
	RequestHash         string
	Provider            string
	PrincipalRef        string
	CredentialSecretRef string
	ReceiptRef          string
	Receipt             []byte
}

type CollectorPrincipalRevokeProviderResult struct {
	OperationKey string
	RequestHash  string
	Provider     string
	PrincipalRef string
	ReceiptRef   string
	Receipt      []byte
}

// CollectorPrincipalProvider must make Grant and RevokeWrite idempotent by
// OperationKey and retain a queryable result for longer than the control-plane
// retry window. Lookup is always called before a side effect and again after an
// ambiguous failure.
type CollectorPrincipalProvider interface {
	LookupGrant(context.Context, string) (CollectorPrincipalGrantProviderResult, bool, error)
	Grant(context.Context, CollectorPrincipalGrantProviderRequest) (CollectorPrincipalGrantProviderResult, error)
	LookupRevoke(context.Context, string) (CollectorPrincipalRevokeProviderResult, bool, error)
	RevokeWrite(context.Context, CollectorPrincipalRevokeProviderRequest) (CollectorPrincipalRevokeProviderResult, error)
}

type CollectorPrincipalGrantRequest struct {
	Provider            string
	IdempotencyKey      string
	ACLPropagationDelay time.Duration
}

type CollectorPrincipalService struct {
	repository CollectorPrincipalOperationRepository
	providers  map[string]CollectorPrincipalProvider
}

type CollectorPrincipalController interface {
	Grant(context.Context, ID, ID, ID, CollectorPrincipalGrantRequest) (CollectorServicePrincipal, error)
	RevokeWrite(context.Context, ID, ID, ID, ID, uint64) (CollectorServicePrincipal, error)
}

func NewCollectorPrincipalService(repository CollectorPrincipalOperationRepository, providers map[string]CollectorPrincipalProvider) (*CollectorPrincipalService, error) {
	if repository == nil || len(providers) == 0 {
		return nil, errors.New("collector principal repository and providers are required")
	}
	cloned := make(map[string]CollectorPrincipalProvider, len(providers))
	for name, provider := range providers {
		if strings.TrimSpace(name) == "" || len(name) > 64 || !isPrintableASCII(name) || provider == nil {
			return nil, errors.New("collector principal provider registry is invalid")
		}
		cloned[name] = provider
	}
	return &CollectorPrincipalService{repository: repository, providers: cloned}, nil
}

func (s *CollectorPrincipalService) Grant(ctx context.Context, tenantID, collectorID, actorID ID, request CollectorPrincipalGrantRequest) (CollectorServicePrincipal, error) {
	if s == nil || s.repository == nil || ctx == nil || !validCollectorEvidenceID(tenantID) || !validCollectorEvidenceID(collectorID) || !validCollectorEvidenceID(actorID) || request.IdempotencyKey == "" || len(request.IdempotencyKey) > 128 || !isPrintableASCII(request.IdempotencyKey) || !validEvidenceDuration(request.ACLPropagationDelay) {
		return CollectorServicePrincipal{}, errors.New("collector principal grant request is incomplete")
	}
	provider, ok := s.providers[request.Provider]
	if !ok {
		return CollectorServicePrincipal{}, ErrCollectorPrincipalProviderNotConfigured
	}
	operationKey, err := hashCollectorReceipt(struct {
		Type           string `json:"type"`
		TenantID       ID     `json:"tenant_id"`
		IdempotencyKey string `json:"idempotency_key"`
	}{"collector_principal_grant", tenantID, request.IdempotencyKey})
	if err != nil {
		return CollectorServicePrincipal{}, err
	}
	requestHash, err := hashCollectorReceipt(struct {
		CollectorID           ID     `json:"collector_id"`
		Provider              string `json:"provider"`
		ServiceType           string `json:"service_type"`
		ACLPropagationDelayMS int64  `json:"acl_propagation_delay_ms"`
	}{collectorID, request.Provider, "kafka", request.ACLPropagationDelay.Milliseconds()})
	if err != nil {
		return CollectorServicePrincipal{}, err
	}
	if existing, err := s.repository.GetCollectorServicePrincipalByGrantOperation(ctx, tenantID, operationKey); err == nil {
		if matchesCollectorPrincipalGrant(existing, collectorID, request.Provider, requestHash) {
			return existing, nil
		}
		return CollectorServicePrincipal{}, ErrCollectorEvidenceConflict
	} else if !errors.Is(err, sql.ErrNoRows) {
		return CollectorServicePrincipal{}, err
	}

	principalID := collectorPrincipalIDFromOperationKey(operationKey)
	providerRequest := CollectorPrincipalGrantProviderRequest{
		OperationKey: operationKey, RequestHash: requestHash, PrincipalID: principalID,
		TenantID: tenantID, CollectorID: collectorID, ServiceType: "kafka",
		ACLPropagationDelay: request.ACLPropagationDelay,
	}
	result, found, err := provider.LookupGrant(ctx, operationKey)
	if err != nil {
		return CollectorServicePrincipal{}, fmt.Errorf("lookup collector principal grant: %w", err)
	}
	if !found {
		result, err = provider.Grant(ctx, providerRequest)
		if err != nil {
			recovered, recoveredFound, lookupErr := provider.LookupGrant(ctx, operationKey)
			if lookupErr != nil {
				return CollectorServicePrincipal{}, errors.Join(err, fmt.Errorf("recover collector principal grant: %w", lookupErr))
			}
			if !recoveredFound {
				return CollectorServicePrincipal{}, err
			}
			result = recovered
		}
	}
	if err := validateCollectorPrincipalGrantProviderResult(result, request.Provider, operationKey, requestHash); err != nil {
		return CollectorServicePrincipal{}, err
	}
	grant := CollectorServicePrincipalGrant{
		ID: principalID, TenantID: tenantID, CollectorID: collectorID,
		ServiceType: "kafka", PrincipalRef: result.PrincipalRef,
		CredentialSecretRef: result.CredentialSecretRef, Provider: result.Provider,
		GrantOperationKey: operationKey, GrantRequestHash: requestHash,
		GrantReceiptRef: result.ReceiptRef, GrantReceipt: result.Receipt,
		ACLPropagationDelay: request.ACLPropagationDelay, ActorID: actorID,
	}
	if err := s.repository.CreateCollectorServicePrincipal(ctx, grant); err != nil {
		if existing, lookupErr := s.repository.GetCollectorServicePrincipalByGrantOperation(ctx, tenantID, operationKey); lookupErr == nil && matchesCollectorPrincipalGrant(existing, collectorID, request.Provider, requestHash) {
			return existing, nil
		}
		return CollectorServicePrincipal{}, err
	}
	return s.repository.GetCollectorServicePrincipalByGrantOperation(ctx, tenantID, operationKey)
}

func collectorPrincipalIDFromOperationKey(operationKey string) ID {
	digest := sha256.Sum256([]byte("watchdog.collector-principal.v1\x00" + operationKey))
	return ID("p_" + hex.EncodeToString(digest[:])[:24])
}

func (s *CollectorPrincipalService) RevokeWrite(ctx context.Context, tenantID, collectorID, principalID, actorID ID, expectedRowVersion uint64) (CollectorServicePrincipal, error) {
	if s == nil || s.repository == nil || ctx == nil || !validCollectorEvidenceID(tenantID) || !validCollectorEvidenceID(collectorID) || !validCollectorEvidenceID(principalID) || !validCollectorEvidenceID(actorID) || expectedRowVersion == 0 {
		return CollectorServicePrincipal{}, errors.New("collector principal revocation request is incomplete")
	}
	principal, err := s.repository.GetCollectorServicePrincipal(ctx, tenantID, collectorID, principalID)
	if err != nil {
		return CollectorServicePrincipal{}, err
	}
	provider, ok := s.providers[principal.Provider]
	if !ok {
		return CollectorServicePrincipal{}, ErrCollectorPrincipalProviderNotConfigured
	}
	operationKey, err := hashCollectorReceipt(struct {
		Type               string `json:"type"`
		TenantID           ID     `json:"tenant_id"`
		PrincipalID        ID     `json:"principal_id"`
		ExpectedRowVersion uint64 `json:"expected_row_version"`
	}{"collector_principal_revoke_write", tenantID, principalID, expectedRowVersion})
	if err != nil {
		return CollectorServicePrincipal{}, err
	}
	if principal.Status == "revoked" {
		if principal.RevokeOperationKey == operationKey {
			return principal, nil
		}
		return CollectorServicePrincipal{}, ErrCollectorEvidenceConflict
	}
	if principal.Status != "active" || principal.RowVersion != expectedRowVersion {
		return CollectorServicePrincipal{}, ErrCollectorPrincipalVersionConflict
	}
	requestHash, err := hashCollectorReceipt(struct {
		CollectorID  ID     `json:"collector_id"`
		Provider     string `json:"provider"`
		ServiceType  string `json:"service_type"`
		PrincipalRef string `json:"principal_ref"`
	}{collectorID, principal.Provider, principal.ServiceType, principal.PrincipalRef})
	if err != nil {
		return CollectorServicePrincipal{}, err
	}
	providerRequest := CollectorPrincipalRevokeProviderRequest{
		OperationKey: operationKey, RequestHash: requestHash, TenantID: tenantID,
		CollectorID: collectorID, PrincipalID: principalID,
		ServiceType: principal.ServiceType, PrincipalRef: principal.PrincipalRef,
	}
	result, found, err := provider.LookupRevoke(ctx, operationKey)
	if err != nil {
		return CollectorServicePrincipal{}, fmt.Errorf("lookup collector principal revocation: %w", err)
	}
	if !found {
		result, err = provider.RevokeWrite(ctx, providerRequest)
		if err != nil {
			recovered, recoveredFound, lookupErr := provider.LookupRevoke(ctx, operationKey)
			if lookupErr != nil {
				return CollectorServicePrincipal{}, errors.Join(err, fmt.Errorf("recover collector principal revocation: %w", lookupErr))
			}
			if !recoveredFound {
				return CollectorServicePrincipal{}, err
			}
			result = recovered
		}
	}
	if err := validateCollectorPrincipalRevokeProviderResult(result, principal.Provider, principal.PrincipalRef, operationKey, requestHash); err != nil {
		return CollectorServicePrincipal{}, err
	}
	revocation := CollectorPrincipalRevocation{
		TenantID: tenantID, PrincipalID: principalID, ExpectedRowVersion: expectedRowVersion,
		Provider: result.Provider, OperationKey: operationKey,
		RevokeReceiptRef: result.ReceiptRef, RevokeReceipt: result.Receipt, ActorID: actorID,
	}
	if err := s.repository.RevokeCollectorServicePrincipal(ctx, revocation); err != nil {
		if current, lookupErr := s.repository.GetCollectorServicePrincipal(ctx, tenantID, collectorID, principalID); lookupErr == nil && current.Status == "revoked" && current.RevokeOperationKey == operationKey {
			return current, nil
		}
		return CollectorServicePrincipal{}, err
	}
	return s.repository.GetCollectorServicePrincipal(ctx, tenantID, collectorID, principalID)
}

func matchesCollectorPrincipalGrant(principal CollectorServicePrincipal, collectorID ID, provider, requestHash string) bool {
	return principal.CollectorID == collectorID && principal.Provider == provider && principal.ServiceType == "kafka" && principal.GrantRequestHash == requestHash
}

func validateCollectorPrincipalGrantProviderResult(result CollectorPrincipalGrantProviderResult, provider, operationKey, requestHash string) error {
	if result.Provider != provider || result.OperationKey != operationKey || result.RequestHash != requestHash || result.PrincipalRef == "" || len(result.PrincipalRef) > 190 || !isPrintableASCII(result.PrincipalRef) || result.CredentialSecretRef == "" || len(result.CredentialSecretRef) > 255 || !isPrintableASCII(result.CredentialSecretRef) || result.ReceiptRef == "" || len(result.ReceiptRef) > 512 || !isPrintableASCII(result.ReceiptRef) || len(result.Receipt) == 0 || len(result.Receipt) > collectorEvidenceMaxReceiptBytes {
		return ErrCollectorPrincipalProviderResultInvalid
	}
	return nil
}

func validateCollectorPrincipalRevokeProviderResult(result CollectorPrincipalRevokeProviderResult, provider, principalRef, operationKey, requestHash string) error {
	if result.Provider != provider || result.OperationKey != operationKey || result.RequestHash != requestHash || result.PrincipalRef != principalRef || result.ReceiptRef == "" || len(result.ReceiptRef) > 512 || !isPrintableASCII(result.ReceiptRef) || len(result.Receipt) == 0 || len(result.Receipt) > collectorEvidenceMaxReceiptBytes {
		return ErrCollectorPrincipalProviderResultInvalid
	}
	return nil
}

var _ CollectorPrincipalController = (*CollectorPrincipalService)(nil)
