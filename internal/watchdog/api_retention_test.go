package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeRetentionRepository struct {
	policies []MetricRetentionPolicy
	deleted  ID
}

func (r *fakeRetentionRepository) ListRetentionPolicies(context.Context, ID) ([]MetricRetentionPolicy, error) {
	return r.policies, nil
}

func (r *fakeRetentionRepository) UpsertRetentionPolicy(_ context.Context, policy MetricRetentionPolicy) (MetricRetentionPolicy, error) {
	if policy.ID == "" {
		policy.ID = "retention-a"
	}
	r.policies = append(r.policies, policy)
	return policy, nil
}

func (r *fakeRetentionRepository) DeleteRetentionPolicy(_ context.Context, _ ID, policyID ID) error {
	r.deleted = policyID
	return nil
}

func TestAPIRetentionPolicyPutListDelete(t *testing.T) {
	repo := &fakeRetentionRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), Retention: repo})

	put := httptest.NewRecorder()
	router.ServeHTTP(put, httptest.NewRequest(http.MethodPut, "/api/v1/retention/policies", strings.NewReader(`{"TargetID":"target-a","HighPrecisionDays":400,"ManualCleanupEnabled":true,"Notes":"manual cleanup only"}`)))
	if put.Code != http.StatusOK {
		t.Fatalf("put status = %d, body = %s", put.Code, put.Body.String())
	}
	if len(repo.policies) != 1 || repo.policies[0].TenantID != "tenant-a" || repo.policies[0].HighPrecisionDays != 400 {
		t.Fatalf("policies = %#v", repo.policies)
	}

	list := httptest.NewRecorder()
	router.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/v1/retention/policies", nil))
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), "target-a") {
		t.Fatalf("list status = %d, body = %s", list.Code, list.Body.String())
	}

	del := httptest.NewRecorder()
	router.ServeHTTP(del, httptest.NewRequest(http.MethodDelete, "/api/v1/retention/policies/retention-a", nil))
	if del.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, body = %s", del.Code, del.Body.String())
	}
	if repo.deleted != "retention-a" {
		t.Fatalf("deleted = %s", repo.deleted)
	}
}
