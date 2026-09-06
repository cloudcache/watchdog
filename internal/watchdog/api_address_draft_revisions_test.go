package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeAddressDraftRevisionRepository struct {
	AddressSetRepository
	filter          AddressDraftRevisionListFilter
	previewTenant   ID
	previewActor    ID
	previewOps      []AddressPrefixBatchOperation
	applyTenant     ID
	applyActor      ID
	applyRevisionID ID
	applyVersion    uint64
}

func (r *fakeAddressDraftRevisionRepository) PrepareAddressPrefixRevision(_ context.Context, tenantID, actorID ID, operations []AddressPrefixBatchOperation) (AddressDraftRevision, error) {
	r.previewTenant, r.previewActor, r.previewOps = tenantID, actorID, operations
	return AddressDraftRevision{ID: "revision-a", TenantID: tenantID, Scope: "prefix", Status: "prepared", RowVersion: 1}, nil
}

func (r *fakeAddressDraftRevisionRepository) ListAddressDraftRevisions(_ context.Context, tenantID ID, filter AddressDraftRevisionListFilter) ([]AddressDraftRevision, string, int, error) {
	r.filter = filter
	return []AddressDraftRevision{{ID: "revision-a", TenantID: tenantID, Status: "prepared"}}, "next", 3, nil
}

func (r *fakeAddressDraftRevisionRepository) GetAddressDraftRevision(_ context.Context, tenantID, revisionID ID) (AddressDraftRevision, error) {
	return AddressDraftRevision{
		ID: revisionID, TenantID: tenantID, Status: "prepared", RowVersion: 1,
		Preview: AddressPrefixBatchPreview{Changes: []AddressPrefixBatchChange{
			{Action: "delete", CIDR: "192.0.2.0/24", PrefixID: "prefix-a"},
			{Action: "create", CIDR: "198.51.100.0/24", PrefixID: "prefix-b"},
		}},
	}, nil
}

func (r *fakeAddressDraftRevisionRepository) ApplyAddressDraftRevision(_ context.Context, tenantID, actorID, revisionID ID, expectedVersion uint64) (AddressDraftRevision, error) {
	r.applyTenant, r.applyActor, r.applyRevisionID, r.applyVersion = tenantID, actorID, revisionID, expectedVersion
	return AddressDraftRevision{ID: revisionID, TenantID: tenantID, Status: "applied", RowVersion: expectedVersion + 1}, nil
}

func TestAddressDraftRevisionAPIListPreviewGetAndApply(t *testing.T) {
	repo := &fakeAddressDraftRevisionRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressSets: repo})

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/address-draft-revisions?status=prepared&limit=25&cursor=cursor-a", nil))
	if response.Code != http.StatusOK || repo.filter.Status != "prepared" || repo.filter.Limit != 25 || repo.filter.Cursor != "cursor-a" {
		t.Fatalf("list status=%d filter=%#v body=%s", response.Code, repo.filter, response.Body.String())
	}
	var page struct {
		Items      []AddressDraftRevision `json:"items"`
		NextCursor string                 `json:"next_cursor"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil || len(page.Items) != 1 || page.NextCursor != "next" {
		t.Fatalf("page=%#v err=%v", page, err)
	}

	body := `{"operations":[{"action":"create","cidr":"203.0.113.7/24"}]}`
	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/address-draft-revisions/preview", strings.NewReader(body)))
	if response.Code != http.StatusCreated || response.Header().Get("ETag") != `"1"` || repo.previewTenant != "tenant-address-api" || repo.previewActor == "" || len(repo.previewOps) != 1 {
		t.Fatalf("preview status=%d tenant=%q actor=%q ops=%#v body=%s", response.Code, repo.previewTenant, repo.previewActor, repo.previewOps, response.Body.String())
	}

	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/address-draft-revisions/revision-a", nil))
	if response.Code != http.StatusOK || response.Header().Get("ETag") != `"1"` {
		t.Fatalf("get status=%d etag=%q body=%s", response.Code, response.Header().Get("ETag"), response.Body.String())
	}

	request := httptest.NewRequest(http.MethodPost, "/api/v1/address-draft-revisions/revision-a/apply", nil)
	request.Header.Set("If-Match", `"1"`)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("ETag") != `"2"` || repo.applyTenant != "tenant-address-api" || repo.applyActor == "" || repo.applyRevisionID != "revision-a" || repo.applyVersion != 1 {
		t.Fatalf("apply status=%d tenant=%q actor=%q revision=%q version=%d body=%s", response.Code, repo.applyTenant, repo.applyActor, repo.applyRevisionID, repo.applyVersion, response.Body.String())
	}
}

func TestAddressDraftRevisionAPIRejectsUnknownFieldsAndMissingIfMatch(t *testing.T) {
	repo := &fakeAddressDraftRevisionRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressSets: repo})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/address-draft-revisions/preview", strings.NewReader(`{"operations":[],"unknown":true}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d body=%s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/address-draft-revisions/revision-a/apply", nil))
	if response.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAddressDraftRevisionServerTables(t *testing.T) {
	repo := &fakeAddressDraftRevisionRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressSets: repo})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/address-draft-revisions?q=revision&status=prepared&sort=operations&order=desc&limit=25&offset=50", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	if filter := repo.filter; !filter.TableMode || filter.Search != "revision" || filter.Status != "prepared" || filter.Sort != "operations" || !filter.Desc || filter.Limit != 25 || filter.Offset != 50 {
		t.Fatalf("filter=%#v", filter)
	}
	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/address-draft-revisions/revision-a/changes?q=198.51&action=create&sort=cidr&order=desc&limit=25&offset=0", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("changes status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Items []addressDraftRevisionChangeItem `json:"items"`
		Total int                              `json:"total"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.Total != 1 || len(result.Items) != 1 || result.Items[0].Index != 2 {
		t.Fatalf("changes=%#v err=%v", result, err)
	}
}
