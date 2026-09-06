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
	operatorFilter  AddressTaxonomyListFilter
	lineFilter      AddressTaxonomyListFilter
	createdGeo      GeoDictionaryNode
	updatedGeo      GeoDictionaryNode
	expectedVersion uint64
}

func (r *fakeAddressTaxonomyRepository) ListISPOperators(_ context.Context, tenantID ID, filter AddressTaxonomyListFilter) ([]ISPOperator, string, int, error) {
	r.operatorFilter = filter
	return []ISPOperator{{
		ID: "operator-a", TenantID: tenantID, FlowISPID: 7, Code: "telecom", Name: "Telecom",
		Category: "carrier", ASNs: []uint32{4134}, Enabled: true, RowVersion: 1,
	}}, "", 1, nil
}

func (r *fakeAddressTaxonomyRepository) ListGeoDictionary(_ context.Context, _ ID, filter AddressTaxonomyListFilter) ([]GeoDictionaryNode, string, int, error) {
	r.geoFilter = filter
	return []GeoDictionaryNode{{ID: "geo-a", Kind: GeoKindCountry, Code: "CN", Name: "China", RowVersion: 1}}, "next", 3, nil
}

func (r *fakeAddressTaxonomyRepository) ListGeoLines(_ context.Context, tenantID ID, filter AddressTaxonomyListFilter) ([]GeoLine, string, int, error) {
	r.lineFilter = filter
	return []GeoLine{{ID: "line-a", TenantID: tenantID, Code: "cn", Name: "China", Enabled: true, RowVersion: 1}}, "", 2, nil
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
		Total      int                 `json:"total"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || len(result.Items) != 1 || result.NextCursor != "next" || result.Total != 3 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestAddressTaxonomyListsParseServerTableContract(t *testing.T) {
	repo := &fakeAddressTaxonomyRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressTaxonomy: repo})

	for _, tc := range []struct {
		path   string
		filter *AddressTaxonomyListFilter
	}{
		{"/api/v1/geo/dictionary?q=china&kind=country&enabled=true&sort=kind&order=desc&limit=25&offset=50", &repo.geoFilter},
		{"/api/v1/network/operators?q=telecom&enabled=false&sort=flow_isp_id&order=asc&limit=50&offset=100", &repo.operatorFilter},
		{"/api/v1/geo/lines?q=china&parent_id=line-root&enabled=true&sort=parent&order=desc&limit=100&offset=200", &repo.lineFilter},
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", tc.path, response.Code, response.Body.String())
		}
		filter := *tc.filter
		if !filter.TableMode || filter.Sort == "" || filter.Limit < 25 || filter.Offset < 50 || filter.Cursor != "" {
			t.Fatalf("path=%s filter=%#v", tc.path, filter)
		}
		if !strings.Contains(response.Body.String(), `"total":`) || !strings.Contains(response.Body.String(), `"offset":`) {
			t.Fatalf("path=%s response=%s", tc.path, response.Body.String())
		}
	}
}

func TestAddressTaxonomyListsRejectInvalidServerQueries(t *testing.T) {
	repo := &fakeAddressTaxonomyRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressTaxonomy: repo})
	for _, path := range []string{
		"/api/v1/geo/dictionary?unknown=1",
		"/api/v1/geo/dictionary?kind=building",
		"/api/v1/geo/dictionary?enabled=1",
		"/api/v1/geo/dictionary?sort=parent",
		"/api/v1/geo/dictionary?order=sideways",
		"/api/v1/geo/dictionary?limit=501",
		"/api/v1/geo/dictionary?offset=-1",
		"/api/v1/geo/dictionary?cursor=abc&sort=name",
		"/api/v1/network/operators?kind=country",
		"/api/v1/network/operators?parent_id=geo-a",
		"/api/v1/network/operators?sort=kind",
		"/api/v1/geo/lines?kind=country",
		"/api/v1/geo/lines?sort=category",
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("path=%s status=%d body=%s", path, response.Code, response.Body.String())
		}
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

func TestAddressTaxonomyOperatorListExposesStableFlowISPID(t *testing.T) {
	repo := &fakeAddressTaxonomyRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressTaxonomy: repo})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/network/operators?limit=25", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Items []ISPOperator `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || len(result.Items) != 1 || result.Items[0].FlowISPID != 7 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}
