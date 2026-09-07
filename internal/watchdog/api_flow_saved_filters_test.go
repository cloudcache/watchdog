package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

type fakeFlowSavedFilterRepository struct {
	items       map[ID]FlowSavedFilter
	listQuery   FlowSavedFilterListQuery
	updateError error
	deleteError error
}

func (repo *fakeFlowSavedFilterRepository) ListFlowSavedFilterOwners(_ context.Context, tenantID, viewerUserID ID, search string, limit int) ([]FlowSavedFilterOwnerFacet, error) {
	return []FlowSavedFilterOwnerFacet{{OwnerUserID: viewerUserID, OwnerName: search, Count: int64(limit)}}, nil
}

func newFakeFlowSavedFilterRepository() *fakeFlowSavedFilterRepository {
	return &fakeFlowSavedFilterRepository{items: make(map[ID]FlowSavedFilter)}
}

func (repo *fakeFlowSavedFilterRepository) ListFlowSavedFilters(_ context.Context, tenantID ID, query FlowSavedFilterListQuery) ([]FlowSavedFilter, int64, error) {
	repo.listQuery = query
	items := make([]FlowSavedFilter, 0)
	for _, item := range repo.items {
		if item.TenantID == tenantID && (item.ShareScope == FlowSavedFilterTenant || item.OwnerUserID == query.ViewerUserID) {
			items = append(items, item)
		}
	}
	return items, int64(len(items)), nil
}

func (repo *fakeFlowSavedFilterRepository) GetFlowSavedFilter(_ context.Context, tenantID, viewerUserID, filterID ID) (FlowSavedFilter, error) {
	item, ok := repo.items[filterID]
	if !ok || item.TenantID != tenantID || (item.ShareScope != FlowSavedFilterTenant && item.OwnerUserID != viewerUserID) {
		return FlowSavedFilter{}, sql.ErrNoRows
	}
	return item, nil
}

func (repo *fakeFlowSavedFilterRepository) CreateFlowSavedFilter(_ context.Context, item FlowSavedFilter) (FlowSavedFilter, error) {
	item.ID = ID("saved_" + string(rune('a'+len(repo.items))))
	item.RowVersion = 1
	repo.items[item.ID] = item
	return item, nil
}

func (repo *fakeFlowSavedFilterRepository) UpdateFlowSavedFilter(_ context.Context, item FlowSavedFilter, expected uint64) (FlowSavedFilter, error) {
	if repo.updateError != nil {
		return FlowSavedFilter{}, repo.updateError
	}
	item.RowVersion = expected + 1
	repo.items[item.ID] = item
	return item, nil
}

func (repo *fakeFlowSavedFilterRepository) DeleteFlowSavedFilter(_ context.Context, _ ID, filterID ID, _ uint64) error {
	if repo.deleteError != nil {
		return repo.deleteError
	}
	delete(repo.items, filterID)
	return nil
}

func flowSavedFilterTestAuth(configure bool) AuthContextAdapter {
	return func(request *http.Request) (AuthContext, error) {
		userID := ID(request.Header.Get("X-Test-User"))
		if userID == "" {
			userID = "user-a"
		}
		actions := []Action{ActionViewCustomer}
		if configure {
			actions = append(actions, ActionConfigure)
		}
		return AuthContext{
			TenantID: "tenant-a", UserID: userID,
			Grants: []Permission{{TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: userID,
				ResourceType: ResourceTenant, ResourceID: "tenant-a", Actions: actions}},
		}, nil
	}
}

func savedFilterBody(name, scope string) string {
	return `{"name":"` + name + `","description":"edge","share_scope":"` + scope + `","filter":` +
		`{"op":"predicate","field":"asn","operator":"eq","values":["AS4134"]}}`
}

func TestFlowSavedFilterAPIPrivateLifecycleAndAudit(t *testing.T) {
	repo := newFakeFlowSavedFilterRepository()
	audit := &recordingAuditRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: flowSavedFilterTestAuth(false), FlowSavedFilters: repo, Audit: audit})

	created := httptest.NewRecorder()
	router.ServeHTTP(created, httptest.NewRequest(http.MethodPost, "/api/v1/flow/filters", strings.NewReader(savedFilterBody("China Telecom", "private"))))
	if created.Code != http.StatusCreated || !strings.Contains(created.Body.String(), `"values":["4134"]`) || created.Header().Get("ETag") != `"1"` {
		t.Fatalf("create status=%d etag=%q body=%s", created.Code, created.Header().Get("ETag"), created.Body.String())
	}

	list := httptest.NewRecorder()
	router.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/v1/flow/filters?q=china&scope=private&owner_id=user-a&sort=name&order=asc&limit=10&offset=0", nil))
	if list.Code != http.StatusOK || repo.listQuery.ViewerUserID != "user-a" || repo.listQuery.ShareScope != "private" || repo.listQuery.SortBy != "name" {
		t.Fatalf("list status=%d query=%+v body=%s", list.Code, repo.listQuery, list.Body.String())
	}

	owners := httptest.NewRecorder()
	router.ServeHTTP(owners, httptest.NewRequest(http.MethodGet, "/api/v1/flow/filters/facets/owners?q=alice&limit=10", nil))
	if owners.Code != http.StatusOK || !strings.Contains(owners.Body.String(), `"owner_name":"alice"`) || !strings.Contains(owners.Body.String(), `"count":10`) {
		t.Fatalf("owners status=%d body=%s", owners.Code, owners.Body.String())
	}

	missing := httptest.NewRecorder()
	router.ServeHTTP(missing, httptest.NewRequest(http.MethodPatch, "/api/v1/flow/filters/saved_a", strings.NewReader(savedFilterBody("Updated", "private"))))
	if missing.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match status=%d body=%s", missing.Code, missing.Body.String())
	}

	updated := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPatch, "/api/v1/flow/filters/saved_a", strings.NewReader(savedFilterBody("Updated", "private")))
	request.Header.Set("If-Match", `"1"`)
	router.ServeHTTP(updated, request)
	if updated.Code != http.StatusOK || updated.Header().Get("ETag") != `"2"` {
		t.Fatalf("update status=%d etag=%q body=%s", updated.Code, updated.Header().Get("ETag"), updated.Body.String())
	}

	deleted := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodDelete, "/api/v1/flow/filters/saved_a", nil)
	request.Header.Set("If-Match", `"2"`)
	router.ServeHTTP(deleted, request)
	if deleted.Code != http.StatusNoContent || len(audit.logs) != 3 {
		t.Fatalf("delete status=%d audit=%+v", deleted.Code, audit.logs)
	}
}

func TestFlowSavedFilterAPIOwnershipSharingAndValidation(t *testing.T) {
	repo := newFakeFlowSavedFilterRepository()
	repo.items["private-a"] = FlowSavedFilter{ID: "private-a", TenantID: "tenant-a", OwnerUserID: "user-a", ShareScope: "private", RowVersion: 1}
	repo.items["shared-a"] = FlowSavedFilter{ID: "shared-a", TenantID: "tenant-a", OwnerUserID: "user-a", ShareScope: "tenant", RowVersion: 1}
	viewer := NewAPIV1Router(APIV1RouterConfig{Auth: flowSavedFilterTestAuth(false), FlowSavedFilters: repo})

	for name, body := range map[string]string{
		"shared without configure": savedFilterBody("Shared", "tenant"),
		"mutable resource":         `{"name":"Device","share_scope":"private","filter":{"op":"predicate","field":"device","operator":"eq","values":["device-a"]}}`,
	} {
		response := httptest.NewRecorder()
		viewer.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/flow/filters", strings.NewReader(body)))
		if name == "shared without configure" && response.Code != http.StatusForbidden {
			t.Fatalf("%s status=%d body=%s", name, response.Code, response.Body.String())
		}
		if name == "mutable resource" && response.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d body=%s", name, response.Code, response.Body.String())
		}
	}

	otherPrivate := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/flow/filters/private-a", nil)
	request.Header.Set("X-Test-User", "user-b")
	viewer.ServeHTTP(otherPrivate, request)
	if otherPrivate.Code != http.StatusNotFound {
		t.Fatalf("other private status=%d body=%s", otherPrivate.Code, otherPrivate.Body.String())
	}

	sharedEdit := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPatch, "/api/v1/flow/filters/shared-a", strings.NewReader(savedFilterBody("Shared", "tenant")))
	request.Header.Set("X-Test-User", "user-b")
	request.Header.Set("If-Match", `"1"`)
	viewer.ServeHTTP(sharedEdit, request)
	if sharedEdit.Code != http.StatusForbidden {
		t.Fatalf("shared edit status=%d body=%s", sharedEdit.Code, sharedEdit.Body.String())
	}

	configure := NewAPIV1Router(APIV1RouterConfig{Auth: flowSavedFilterTestAuth(true), FlowSavedFilters: repo})
	allowed := httptest.NewRecorder()
	configure.ServeHTTP(allowed, httptest.NewRequest(http.MethodPost, "/api/v1/flow/filters", strings.NewReader(savedFilterBody("Shared", "tenant"))))
	if allowed.Code != http.StatusCreated {
		t.Fatalf("configured create status=%d body=%s", allowed.Code, allowed.Body.String())
	}

	repo.updateError = ErrFlowSavedFilterVersionConflict
	stale := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPatch, "/api/v1/flow/filters/shared-a", strings.NewReader(savedFilterBody("Shared", "tenant")))
	request.Header.Set("If-Match", `"1"`)
	configure.ServeHTTP(stale, request)
	if stale.Code != http.StatusPreconditionFailed {
		t.Fatalf("stale status=%d body=%s", stale.Code, stale.Body.String())
	}
}

func TestNormalizeFlowSavedFilterCanonicalizesAndBounds(t *testing.T) {
	item, err := normalizeFlowSavedFilter(FlowSavedFilter{
		Name: " Test ", ShareScope: "PRIVATE",
		Filter: flowquery.FilterExpression{Op: flowquery.FilterPredicate, Field: "src_ip", Operator: flowquery.FilterEqual, Values: []string{"203.0.113.9/24"}},
	})
	if err != nil || item.Name != "Test" || item.ShareScope != "private" || item.Filter.Values[0] != "203.0.113.0/24" {
		t.Fatalf("item=%+v err=%v", item, err)
	}
	item.Filter.Field = "target"
	item.Filter.Values = []string{"target-a"}
	if _, err := normalizeFlowSavedFilter(item); !errors.Is(err, ErrFlowSavedFilterInvalid) {
		t.Fatalf("resource filter err=%v", err)
	}
}
