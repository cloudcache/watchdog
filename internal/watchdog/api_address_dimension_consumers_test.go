package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeAddressDimensionConsumerAPI struct {
	AddressDimensionPublisher
	AddressDimensionLifecycle
	AddressDimensionConsumerStatusReader
	snapshot   AddressDimensionSnapshot
	activation AddressDimensionActivation
	summary    AddressDimensionConsumerSummary
	items      []AddressDimensionConsumerStatus
	next       string
	filter     AddressDimensionConsumerFilter
}

func (api *fakeAddressDimensionConsumerAPI) GetAddressDimensionSnapshot(_ context.Context, tenantID, snapshotID ID) (AddressDimensionSnapshot, error) {
	if tenantID != api.snapshot.TenantID || snapshotID != api.snapshot.ID {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	return api.snapshot, nil
}

func (api *fakeAddressDimensionConsumerAPI) GetAddressDimensionActivationAt(_ context.Context, tenantID ID, at time.Time) (AddressDimensionActivation, error) {
	if tenantID != api.snapshot.TenantID || at.IsZero() {
		return AddressDimensionActivation{}, ErrAddressDimensionInvalid
	}
	return api.activation, nil
}

func (api *fakeAddressDimensionConsumerAPI) GetAddressDimensionConsumerSummary(_ context.Context, tenantID, snapshotID ID) (AddressDimensionConsumerSummary, error) {
	if tenantID != api.snapshot.TenantID || snapshotID != api.snapshot.ID {
		return AddressDimensionConsumerSummary{}, ErrAddressDimensionInvalid
	}
	return api.summary, nil
}

func (api *fakeAddressDimensionConsumerAPI) ListAddressDimensionConsumers(_ context.Context, tenantID, snapshotID ID, filter AddressDimensionConsumerFilter) ([]AddressDimensionConsumerStatus, string, error) {
	if tenantID != api.snapshot.TenantID || snapshotID != api.snapshot.ID {
		return nil, "", ErrAddressDimensionInvalid
	}
	api.filter = filter
	return api.items, api.next, nil
}

func TestAddressDimensionConsumerStatusAPISeparatesReadinessAndDrift(t *testing.T) {
	at := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	api := &fakeAddressDimensionConsumerAPI{
		snapshot:   AddressDimensionSnapshot{ID: "snapshot-a", TenantID: "tenant-dimension", Version: 7},
		activation: AddressDimensionActivation{ID: "activation-a", TenantID: "tenant-dimension", SnapshotID: "snapshot-a", EffectiveFrom: at},
		summary: AddressDimensionConsumerSummary{
			SnapshotID: "snapshot-a", Version: 7, Scope: AddressDimensionConsumerScopeObserved,
			Queryability: AddressDimensionQueryabilityPartial, Observed: 2, Ready: 1, Ahead: 1,
		},
		items: []AddressDimensionConsumerStatus{{
			WorkerID: "worker-b", TargetState: AddressDimensionConsumerUnreported,
			LatestInstalledSnapshot: "snapshot-b", LatestInstalledVersion: 8, Drift: AddressDimensionDriftAhead,
		}},
		next: "next-consumer-page",
	}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: addressDimensionTestAuth, AddressDimensions: api, DimensionLifecycle: api, DimensionConsumers: api,
	})

	statusResponse := httptest.NewRecorder()
	router.ServeHTTP(statusResponse, httptest.NewRequest(http.MethodGet, "/api/v1/dimensions/address/status?at=2026-09-08T00:00:00Z", nil))
	if statusResponse.Code != http.StatusOK {
		t.Fatalf("status = %d %s", statusResponse.Code, statusResponse.Body.String())
	}
	var status AddressDimensionRuntimeStatus
	if err := json.Unmarshal(statusResponse.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Activation.ID != "activation-a" || status.Snapshot.ID != "snapshot-a" || status.Consumers.Queryability != AddressDimensionQueryabilityPartial || status.Consumers.Scope != AddressDimensionConsumerScopeObserved {
		t.Fatalf("runtime status = %#v", status)
	}

	listResponse := httptest.NewRecorder()
	router.ServeHTTP(listResponse, httptest.NewRequest(http.MethodGet, "/api/v1/dimensions/address/versions/snapshot-a/consumers?q=worker&state=unreported&drift=ahead&limit=25", nil))
	if listResponse.Code != http.StatusOK || api.filter.Query != "worker" || api.filter.State != AddressDimensionConsumerUnreported || api.filter.Drift != AddressDimensionDriftAhead || api.filter.Limit != 25 {
		t.Fatalf("list = %d %s filter=%#v", listResponse.Code, listResponse.Body.String(), api.filter)
	}
	var listBody struct {
		Items      []AddressDimensionConsumerStatus `json:"items"`
		Summary    AddressDimensionConsumerSummary  `json:"summary"`
		NextCursor string                           `json:"next_cursor"`
	}
	if err := json.Unmarshal(listResponse.Body.Bytes(), &listBody); err != nil {
		t.Fatal(err)
	}
	if len(listBody.Items) != 1 || listBody.Summary.Observed != 2 || listBody.NextCursor != "next-consumer-page" {
		t.Fatalf("consumer list body = %#v", listBody)
	}
}

func TestAddressDimensionConsumerStatusAPIRejectsBadParameters(t *testing.T) {
	api := &fakeAddressDimensionConsumerAPI{
		snapshot: AddressDimensionSnapshot{ID: "snapshot-a", TenantID: "tenant-dimension", Version: 1},
	}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: addressDimensionTestAuth, AddressDimensions: api, DimensionLifecycle: api, DimensionConsumers: api,
	})
	for _, path := range []string{
		"/api/v1/dimensions/address/status?at=not-a-time",
		"/api/v1/dimensions/address/versions/snapshot-a/consumers?limit=0",
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d %s", path, response.Code, response.Body.String())
		}
	}
}
