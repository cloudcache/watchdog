package watchdog

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type flowVPNFindingRepositoryStub struct {
	listFilter  VPNFindingListFilter
	facetFilter VPNFindingFacetFilter
	item        VPNFinding
	updated     bool
	actor       ID
	expected    uint64
}

func (s *flowVPNFindingRepositoryStub) ListVPNFindingsPage(_ context.Context, _ ID, filter VPNFindingListFilter) ([]VPNFinding, int64, error) {
	s.listFilter = filter
	return []VPNFinding{s.item}, 1, nil
}

func (s *flowVPNFindingRepositoryStub) ListVPNFindingFacets(_ context.Context, _ ID, filter VPNFindingFacetFilter) ([]VPNFindingFacet, error) {
	s.facetFilter = filter
	return []VPNFindingFacet{{Value: "high", Count: 3}}, nil
}

func (s *flowVPNFindingRepositoryStub) GetVPNFinding(_ context.Context, _ ID, findingID ID) (VPNFinding, error) {
	if findingID != s.item.ID {
		return VPNFinding{}, sql.ErrNoRows
	}
	return s.item, nil
}

func (s *flowVPNFindingRepositoryStub) UpdateVPNFindingDisposition(_ context.Context, _ ID, findingID ID, disposition, note string, actor ID, expected uint64) (VPNFinding, error) {
	if findingID != s.item.ID {
		return VPNFinding{}, sql.ErrNoRows
	}
	s.updated, s.actor, s.expected = disposition == "confirmed" && note == "reviewed", actor, expected
	s.item.Disposition, s.item.DispositionNote, s.item.RowVersion = disposition, note, expected+1
	return s.item, nil
}

func TestFlowVPNFindingListAndFacetsAreTenantScopedAndTyped(t *testing.T) {
	repo := &flowVPNFindingRepositoryStub{item: VPNFinding{ID: "finding-a", RowVersion: 4}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: flowVPNTestAuth(ActionVPNView), FlowVPNFindings: repo})

	list := httptest.NewRecorder()
	router.ServeHTTP(list, httptest.NewRequest(http.MethodGet,
		"/api/v1/flow/vpn/findings?from=2026-09-06T00:00:00Z&to=2026-09-07T00:00:00Z&q=192.0.2&filter.risk_level=high&filter.risk_level=critical&sort_by=score&sort_direction=asc&limit=50&offset=100", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", list.Code, list.Body.String())
	}
	if repo.listFilter.Search != "192.0.2" || repo.listFilter.SortBy != "score" || repo.listFilter.SortDirection != "ASC" ||
		repo.listFilter.Limit != 50 || repo.listFilter.Offset != 100 || len(repo.listFilter.ColumnFilters["risk_level"]) != 2 {
		t.Fatalf("list filter=%+v", repo.listFilter)
	}

	facets := httptest.NewRecorder()
	router.ServeHTTP(facets, httptest.NewRequest(http.MethodGet,
		"/api/v1/flow/vpn/findings/facets?field=risk_level&q=hi&search=192.0.2&filter.disposition=unreviewed&limit=20", nil))
	if facets.Code != http.StatusOK || repo.facetFilter.Field != "risk_level" || repo.facetFilter.Search != "hi" ||
		repo.facetFilter.Query != "192.0.2" || repo.facetFilter.Limit != 20 || repo.facetFilter.ColumnFilters["disposition"][0] != "unreviewed" {
		t.Fatalf("facet status=%d filter=%+v body=%s", facets.Code, repo.facetFilter, facets.Body.String())
	}
}

func TestFlowVPNFindingGetDispositionPermissionsAndETag(t *testing.T) {
	repo := &flowVPNFindingRepositoryStub{item: VPNFinding{ID: "finding-a", RowVersion: 4}}
	viewRouter := NewAPIV1Router(APIV1RouterConfig{Auth: flowVPNTestAuth(ActionVPNView), FlowVPNFindings: repo})
	get := httptest.NewRecorder()
	viewRouter.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/flow/vpn/findings/finding-a", nil))
	if get.Code != http.StatusOK || get.Header().Get("ETag") != `"4"` {
		t.Fatalf("get status=%d etag=%q body=%s", get.Code, get.Header().Get("ETag"), get.Body.String())
	}

	denied := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/flow/vpn/findings/finding-a/actions/disposition", strings.NewReader(`{"disposition":"confirmed","note":"reviewed"}`))
	request.Header.Set("If-Match", `"4"`)
	viewRouter.ServeHTTP(denied, request)
	if denied.Code != http.StatusForbidden || repo.updated {
		t.Fatalf("denied status=%d updated=%v", denied.Code, repo.updated)
	}

	audit := &recordingAuditRepository{}
	triageRouter := NewAPIV1Router(APIV1RouterConfig{Auth: flowVPNTestAuth(ActionVPNTriage), FlowVPNFindings: repo, Audit: audit})
	missing := httptest.NewRecorder()
	triageRouter.ServeHTTP(missing, httptest.NewRequest(http.MethodPost, "/api/v1/flow/vpn/findings/finding-a/actions/disposition", strings.NewReader(`{"disposition":"confirmed"}`)))
	if missing.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match status=%d body=%s", missing.Code, missing.Body.String())
	}

	ok := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/v1/flow/vpn/findings/finding-a/actions/disposition", strings.NewReader(`{"disposition":"confirmed","note":"reviewed"}`))
	request.Header.Set("If-Match", `"4"`)
	triageRouter.ServeHTTP(ok, request)
	if ok.Code != http.StatusOK || ok.Header().Get("ETag") != `"5"` || !repo.updated || repo.actor != "user-vpn" || repo.expected != 4 {
		t.Fatalf("status=%d etag=%q updated=%v actor=%q expected=%d body=%s", ok.Code, ok.Header().Get("ETag"), repo.updated, repo.actor, repo.expected, ok.Body.String())
	}
	if len(audit.logs) != 1 || audit.logs[0].Action != "flow.vpn_finding.disposition_updated" || audit.logs[0].ResourceID != "finding-a" {
		t.Fatalf("audit=%+v", audit.logs)
	}
}

func TestFlowVPNFindingQueryRejectsUnknownAndInvalidFilters(t *testing.T) {
	repo := &flowVPNFindingRepositoryStub{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: flowVPNTestAuth(ActionVPNView), FlowVPNFindings: repo})
	for _, path := range []string{
		"/api/v1/flow/vpn/findings?tenant_id=other",
		"/api/v1/flow/vpn/findings?filter.risk_level=dangerous",
		"/api/v1/flow/vpn/findings?filter.remote_ip=not-an-ip",
		"/api/v1/flow/vpn/findings?from=2026-09-01T00:00:00Z",
		"/api/v1/flow/vpn/findings/facets?field=secret_column",
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("path=%s status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
}

func TestFlowVPNPermissionActionsAreAccepted(t *testing.T) {
	for _, action := range []Action{ActionVPNView, ActionVPNTriage, ActionVPNProbe} {
		if normalized, err := normalizePermissionActions([]Action{action}); err != nil || len(normalized) != 1 || normalized[0] != action {
			t.Fatalf("action=%q normalized=%v err=%v", action, normalized, err)
		}
	}
}

func flowVPNTestAuth(action Action) AuthContextAdapter {
	return func(*http.Request) (AuthContext, error) {
		return AuthContext{
			TenantID: "tenant-vpn", UserID: "user-vpn",
			Grants: []Permission{{
				TenantID: "tenant-vpn", SubjectType: SubjectUser, SubjectID: "user-vpn",
				ResourceType: ResourceTenant, ResourceID: "tenant-vpn", Actions: []Action{action},
			}},
		}, nil
	}
}
