package watchdog

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type collectorPrincipalAPIController struct {
	principal       CollectorServicePrincipal
	grantRequest    CollectorPrincipalGrantRequest
	tenantID        ID
	collectorID     ID
	principalID     ID
	actorID         ID
	expectedVersion uint64
	grantCalls      int
	revokeCalls     int
	err             error
}

func (c *collectorPrincipalAPIController) Grant(_ context.Context, tenantID, collectorID, actorID ID, request CollectorPrincipalGrantRequest) (CollectorServicePrincipal, error) {
	c.tenantID, c.collectorID, c.actorID, c.grantRequest = tenantID, collectorID, actorID, request
	c.grantCalls++
	return c.principal, c.err
}

func (c *collectorPrincipalAPIController) RevokeWrite(_ context.Context, tenantID, collectorID, principalID, actorID ID, expectedVersion uint64) (CollectorServicePrincipal, error) {
	c.tenantID, c.collectorID, c.principalID, c.actorID, c.expectedVersion = tenantID, collectorID, principalID, actorID, expectedVersion
	c.revokeCalls++
	return c.principal, c.err
}

func TestCollectorPrincipalAPIGrantHidesProviderEvidence(t *testing.T) {
	controller := &collectorPrincipalAPIController{principal: collectorPrincipalAPIRecord("active", 1)}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), CollectorPrincipals: controller})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/service-principals", strings.NewReader(`{"provider":"kafka-admin","acl_propagation_delay_ms":2000}`))
	req.Header.Set("Idempotency-Key", "grant-request-1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated || rec.Header().Get("ETag") != `"1"` || controller.grantCalls != 1 {
		t.Fatalf("status=%d etag=%q calls=%d body=%s", rec.Code, rec.Header().Get("ETag"), controller.grantCalls, rec.Body.String())
	}
	if controller.tenantID != "tenant-a" || controller.collectorID != "collector-a" || controller.actorID != "user-a" || controller.grantRequest.Provider != "kafka-admin" || controller.grantRequest.IdempotencyKey != "grant-request-1" || controller.grantRequest.ACLPropagationDelay != 2*time.Second {
		t.Fatalf("controller=%+v", controller)
	}
	for _, secret := range []string{"credential_secret_ref", "grant_operation_key", "grant_request_hash", "receipt"} {
		if strings.Contains(rec.Body.String(), secret) {
			t.Fatalf("response leaked %s: %s", secret, rec.Body.String())
		}
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/service-principals", strings.NewReader(`{"provider":"kafka-admin","acl_propagation_delay_ms":2000,"grant_receipt":"forbidden"}`))
	req.Header.Set("Idempotency-Key", "grant-request-2")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || controller.grantCalls != 1 {
		t.Fatalf("receipt injection status=%d calls=%d body=%s", rec.Code, controller.grantCalls, rec.Body.String())
	}
}

func TestCollectorPrincipalAPIRevokeRequiresPermissionAndIfMatch(t *testing.T) {
	controller := &collectorPrincipalAPIController{principal: collectorPrincipalAPIRecord("revoked", 2)}
	path := "/api/v1/collectors/collector-a/service-principals/principal-a/actions/revoke-write"
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(false), CollectorPrincipals: controller})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != http.StatusForbidden || controller.revokeCalls != 0 {
		t.Fatalf("permission status=%d calls=%d", rec.Code, controller.revokeCalls)
	}

	router = NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), CollectorPrincipals: controller})
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	if rec.Code != http.StatusPreconditionRequired || controller.revokeCalls != 0 {
		t.Fatalf("missing If-Match status=%d calls=%d", rec.Code, controller.revokeCalls)
	}

	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
	req.Header.Set("If-Match", `"1"`)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("ETag") != `"2"` || controller.revokeCalls != 1 || controller.principalID != "principal-a" || controller.expectedVersion != 1 {
		t.Fatalf("status=%d etag=%q controller=%+v body=%s", rec.Code, rec.Header().Get("ETag"), controller, rec.Body.String())
	}
}

func TestCollectorPrincipalAPIMapsVersionConflict(t *testing.T) {
	controller := &collectorPrincipalAPIController{err: ErrCollectorPrincipalVersionConflict}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: collectorPrincipalAPIAuth(true), CollectorPrincipals: controller})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/service-principals/principal-a/actions/revoke-write", nil)
	req.Header.Set("If-Match", `"1"`)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	controller.err = errors.New("provider secret details")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "provider secret details") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func collectorPrincipalAPIAuth(admin bool) AuthContextAdapter {
	return func(*http.Request) (AuthContext, error) {
		return AuthContext{TenantID: "tenant-a", UserID: "user-a", IsAdmin: admin}, nil
	}
}

func collectorPrincipalAPIRecord(status string, rowVersion uint64) CollectorServicePrincipal {
	now := time.Unix(1_000_000, 0).UTC()
	return CollectorServicePrincipal{
		ID: "principal-a", TenantID: "tenant-a", CollectorID: "collector-a",
		ServiceType: "kafka", PrincipalRef: "User:collector-a", CredentialSecretRef: "secret://collector-a",
		Provider: "kafka-admin", GrantOperationKey: strings.Repeat("a", 64), GrantRequestHash: strings.Repeat("b", 64),
		Status: status, ACLPropagationDelay: 2 * time.Second, RowVersion: rowVersion,
		CreatedAt: now, UpdatedAt: now,
	}
}
