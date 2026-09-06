package watchdog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeAddressSetRepository struct {
	AddressSetRepository
	prefixFilter    AddressPrefixListFilter
	createdPrefix   AddressPrefix
	updatedPrefix   AddressPrefix
	prefixVersion   uint64
	setFilter       AddressSetListFilter
	createdSet      AddressSet
	updatedSet      AddressSet
	expectedVersion uint64
	deletedVersion  uint64
	prefixTotal     int
	setTotal        int
}

func (r *fakeAddressSetRepository) ListAddressPrefixesPage(_ context.Context, _ ID, filter AddressPrefixListFilter) ([]AddressPrefix, string, int, error) {
	r.prefixFilter = filter
	return []AddressPrefix{{ID: "prefix-a", CIDR: "192.0.2.0/24", Family: 4, RowVersion: 1}}, "next-prefix", r.prefixTotal, nil
}

func (r *fakeAddressSetRepository) GetAddressPrefix(_ context.Context, tenantID ID, prefixID string) (AddressPrefix, error) {
	return AddressPrefix{ID: prefixID, TenantID: tenantID, CIDR: "192.0.2.0/24", Labels: map[string]string{"type": "customer"}, Source: "manual", RowVersion: 2}, nil
}

func (r *fakeAddressSetRepository) UpsertAddressPrefix(_ context.Context, prefix AddressPrefix) (AddressPrefix, error) {
	r.createdPrefix = prefix
	prefix.RowVersion = 1
	return prefix, nil
}

func (r *fakeAddressSetRepository) UpdateAddressPrefix(_ context.Context, prefix AddressPrefix, expectedVersion uint64) (AddressPrefix, error) {
	r.updatedPrefix = prefix
	r.prefixVersion = expectedVersion
	prefix.RowVersion = expectedVersion + 1
	return prefix, nil
}

func (r *fakeAddressSetRepository) ListAddressSetsPage(_ context.Context, _ ID, filter AddressSetListFilter) ([]AddressSet, string, int, error) {
	r.setFilter = filter
	return []AddressSet{{ID: "set-a", Name: "Customer", RowVersion: 1}}, "next", r.setTotal, nil
}

func (r *fakeAddressSetRepository) GetAddressSet(_ context.Context, tenantID ID, setID string) (AddressSet, error) {
	return AddressSet{
		ID: setID, TenantID: tenantID, Name: "Customer", Selector: map[string]any{"labels": map[string]any{"type": "customer"}},
		MatchDirection: "both", Enabled: true, RowVersion: 3,
	}, nil
}

func (r *fakeAddressSetRepository) UpsertAddressSet(_ context.Context, set AddressSet) (AddressSet, error) {
	r.createdSet = set
	set.RowVersion = 1
	return set, nil
}

func (r *fakeAddressSetRepository) UpdateAddressSet(_ context.Context, set AddressSet, expectedVersion uint64) (AddressSet, error) {
	r.updatedSet = set
	r.expectedVersion = expectedVersion
	set.RowVersion = expectedVersion + 1
	return set, nil
}

func (r *fakeAddressSetRepository) DeleteAddressSetVersion(_ context.Context, _ ID, _ string, expectedVersion uint64) error {
	r.deletedVersion = expectedVersion
	return nil
}

func TestAddressSetListParsesServerSideFilters(t *testing.T) {
	repo := &fakeAddressSetRepository{setTotal: 3}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressSets: repo})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/address-sets?q=customer&match_direction=in&enabled=false&limit=25&cursor=cursor-a", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	filter := repo.setFilter
	if filter.Search != "customer" || filter.MatchDirection != "in" || filter.Enabled == nil || *filter.Enabled || filter.Limit != 25 || filter.Cursor != "cursor-a" {
		t.Fatalf("filter = %#v", filter)
	}
	var result struct {
		Items      []AddressSet `json:"items"`
		NextCursor string       `json:"next_cursor"`
		Total      int          `json:"total"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || len(result.Items) != 1 || result.NextCursor != "next" || result.Total != 3 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestAddressPrefixListParsesServerSideFilters(t *testing.T) {
	repo := &fakeAddressSetRepository{prefixTotal: 4}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressSets: repo})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/address-prefixes?q=customer&family=4&source=manual&geo_leaf_id=geo-a&operator_id=operator-a&asn=4134&limit=25&cursor=cursor-a", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	filter := repo.prefixFilter
	if filter.Search != "customer" || filter.Family != 4 || filter.Source != "manual" || filter.GeoLeafID != "geo-a" || filter.OperatorID != "operator-a" || filter.ASN == nil || *filter.ASN != 4134 || filter.Limit != 25 || filter.Cursor != "cursor-a" {
		t.Fatalf("filter = %#v", filter)
	}
	var result struct {
		Items      []AddressPrefix `json:"items"`
		NextCursor string          `json:"next_cursor"`
		Total      int             `json:"total"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || len(result.Items) != 1 || result.NextCursor != "next-prefix" || result.Total != 4 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestAddressListsParseServerTableContract(t *testing.T) {
	repo := &fakeAddressSetRepository{prefixTotal: 8, setTotal: 9}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressSets: repo})

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/api/v1/address-prefixes?q=customer&family=6&source=vendor&sort=prefix_length&order=desc&limit=25&offset=50", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("prefix status=%d body=%s", response.Code, response.Body.String())
	}
	if filter := repo.prefixFilter; !filter.TableMode || filter.Search != "customer" || filter.Family != 6 || filter.Source != "vendor" ||
		filter.Sort != "prefix_length" || !filter.Desc || filter.Limit != 25 || filter.Offset != 50 || filter.Cursor != "" {
		t.Fatalf("prefix table filter=%#v", filter)
	}
	if !strings.Contains(response.Body.String(), `"total":8`) || !strings.Contains(response.Body.String(), `"offset":50`) {
		t.Fatalf("prefix response=%s", response.Body.String())
	}

	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet,
		"/api/v1/address-sets?q=customer&match_direction=out&enabled=true&sort=enabled&order=asc&limit=50&offset=100", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("set status=%d body=%s", response.Code, response.Body.String())
	}
	if filter := repo.setFilter; !filter.TableMode || filter.Search != "customer" || filter.MatchDirection != "out" || filter.Enabled == nil ||
		!*filter.Enabled || filter.Sort != "enabled" || filter.Desc || filter.Limit != 50 || filter.Offset != 100 || filter.Cursor != "" {
		t.Fatalf("set table filter=%#v", filter)
	}
	if !strings.Contains(response.Body.String(), `"total":9`) || !strings.Contains(response.Body.String(), `"offset":100`) {
		t.Fatalf("set response=%s", response.Body.String())
	}
}

func TestAddressListsRejectInvalidServerTableQueries(t *testing.T) {
	repo := &fakeAddressSetRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressSets: repo})
	queries := []string{
		"/api/v1/address-prefixes?unknown=1",
		"/api/v1/address-prefixes?sort=labels",
		"/api/v1/address-prefixes?order=sideways",
		"/api/v1/address-prefixes?limit=501",
		"/api/v1/address-prefixes?offset=-1",
		"/api/v1/address-prefixes?cursor=abc&sort=cidr",
		"/api/v1/address-sets?unknown=1",
		"/api/v1/address-sets?match_direction=sideways",
		"/api/v1/address-sets?enabled=maybe",
		"/api/v1/address-sets?sort=selector",
		"/api/v1/address-sets?order=sideways",
		"/api/v1/address-sets?cursor=abc&offset=1",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, query, nil))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestAddressPrefixCreateAndPatchUseTenantAndETag(t *testing.T) {
	repo := &fakeAddressSetRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressSets: repo})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/address-prefixes", strings.NewReader(`{"cidr":"192.0.2.7/24","labels":{"type":"customer"}}`))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Header().Get("ETag") != `"1"` || repo.createdPrefix.TenantID != "tenant-address-api" || repo.createdPrefix.Source != "manual" {
		t.Fatalf("status=%d etag=%q created=%#v body=%s", response.Code, response.Header().Get("ETag"), repo.createdPrefix, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPatch, "/api/v1/address-prefixes/prefix-a", strings.NewReader(`{"source":"customer","asn":4134}`))
	request.Header.Set("If-Match", `"2"`)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("ETag") != `"3"` || repo.prefixVersion != 2 || repo.updatedPrefix.Source != "customer" || repo.updatedPrefix.ASN == nil || *repo.updatedPrefix.ASN != 4134 {
		t.Fatalf("status=%d etag=%q version=%d updated=%#v body=%s", response.Code, response.Header().Get("ETag"), repo.prefixVersion, repo.updatedPrefix, response.Body.String())
	}
}

func TestAddressSetCreatePreservesDisabledAndRejectsUnknownFields(t *testing.T) {
	repo := &fakeAddressSetRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressSets: repo})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/address-sets", strings.NewReader(`{"name":"Disabled set","explicit_members":["192.0.2.7/24"],"enabled":false}`))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Header().Get("ETag") != `"1"` || repo.createdSet.Enabled || repo.createdSet.TenantID != "tenant-address-api" {
		t.Fatalf("status=%d etag=%q created=%#v body=%s", response.Code, response.Header().Get("ETag"), repo.createdSet, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPost, "/api/v1/address-sets", strings.NewReader(`{"name":"Bad","unexpected":true}`))
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAddressSetPatchAndDeleteRequireAndUseETag(t *testing.T) {
	repo := &fakeAddressSetRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressSets: repo})
	request := httptest.NewRequest(http.MethodPatch, "/api/v1/address-sets/set-a", strings.NewReader(`{"enabled":false}`))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodPatch, "/api/v1/address-sets/set-a", strings.NewReader(`{"enabled":false}`))
	request.Header.Set("If-Match", `"3"`)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("ETag") != `"4"` || repo.expectedVersion != 3 || repo.updatedSet.Enabled {
		t.Fatalf("status=%d etag=%q expected=%d updated=%#v body=%s", response.Code, response.Header().Get("ETag"), repo.expectedVersion, repo.updatedSet, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodDelete, "/api/v1/address-sets/set-a", nil)
	request.Header.Set("If-Match", `"4"`)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || repo.deletedVersion != 4 {
		t.Fatalf("delete status=%d version=%d body=%s", response.Code, repo.deletedVersion, response.Body.String())
	}
}
