package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeAddressTaxonomyRepository struct {
	AddressTaxonomyRepository
	geoFilter       AddressTaxonomyListFilter
	createdGeo      GeoDictionaryNode
	updatedGeo      GeoDictionaryNode
	expectedVersion uint64
}

func (r *fakeAddressTaxonomyRepository) ListGeoDictionary(_ context.Context, _ ID, filter AddressTaxonomyListFilter) ([]GeoDictionaryNode, string, error) {
	r.geoFilter = filter
	return []GeoDictionaryNode{{ID: "geo-a", Kind: GeoKindCountry, Code: "CN", Name: "China", RowVersion: 1}}, "next", nil
}

func (r *fakeAddressTaxonomyRepository) GetGeoDictionary(_ context.Context, tenantID, id ID) (GeoDictionaryNode, error) {
	return GeoDictionaryNode{ID: id, TenantID: tenantID, Kind: GeoKindCountry, Code: "CN", Name: "China", Enabled: true, RowVersion: 3}, nil
}

func (r *fakeAddressTaxonomyRepository) CreateGeoDictionary(_ context.Context, item GeoDictionaryNode) (GeoDictionaryNode, error) {
	r.createdGeo = item
	item.ID = "geo-created"
	item.RowVersion = 1
	return item, nil
}

func (r *fakeAddressTaxonomyRepository) UpdateGeoDictionary(_ context.Context, item GeoDictionaryNode, expectedVersion uint64) (GeoDictionaryNode, error) {
	r.updatedGeo = item
	r.expectedVersion = expectedVersion
	item.RowVersion = expectedVersion + 1
	return item, nil
}

func TestAddressTaxonomyGeoListParsesFilters(t *testing.T) {
	repo := &fakeAddressTaxonomyRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressTaxonomy: repo})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/geo/dictionary?q=china&kind=country&parent_id=geo-parent&enabled=true&limit=25&cursor=cursor-a", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	filter := repo.geoFilter
	if filter.Search != "china" || filter.Kind != "country" || filter.ParentID != "geo-parent" || filter.Enabled == nil || !*filter.Enabled || filter.Limit != 25 || filter.Cursor != "cursor-a" {
		t.Fatalf("filter = %#v", filter)
	}
	var result struct {
		Items      []GeoDictionaryNode `json:"items"`
		NextCursor string              `json:"next_cursor"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || len(result.Items) != 1 || result.NextCursor != "next" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestAddressTaxonomyGeoCreateDefaultsEnabledAndRejectsUnknownFields(t *testing.T) {
	repo := &fakeAddressTaxonomyRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressTaxonomy: repo})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/geo/dictionary", strings.NewReader(`{"kind":"country","code":"CN","name":"China"}`))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Header().Get("ETag") != `"1"` || !repo.createdGeo.Enabled || repo.createdGeo.TenantID != "tenant-address-api" {
		t.Fatalf("status=%d etag=%q created=%#v body=%s", response.Code, response.Header().Get("ETag"), repo.createdGeo, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/api/v1/geo/dictionary", strings.NewReader(`{"kind":"country","code":"CN","name":"China","unexpected":true}`))
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAddressTaxonomyGeoPatchRequiresAndUsesETag(t *testing.T) {
	repo := &fakeAddressTaxonomyRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressTaxonomy: repo})
	request := httptest.NewRequest(http.MethodPatch, "/api/v1/geo/dictionary/geo-a", strings.NewReader(`{"name":"China mainland","enabled":false}`))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPatch, "/api/v1/geo/dictionary/geo-a", strings.NewReader(`{"name":"China mainland","enabled":false}`))
	request.Header.Set("If-Match", `"3"`)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("ETag") != `"4"` || repo.expectedVersion != 3 || repo.updatedGeo.Name != "China mainland" || repo.updatedGeo.Enabled {
		t.Fatalf("status=%d etag=%q expected=%d updated=%#v body=%s", response.Code, response.Header().Get("ETag"), repo.expectedVersion, repo.updatedGeo, response.Body.String())
	}
}
