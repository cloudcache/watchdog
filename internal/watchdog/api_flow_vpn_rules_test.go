package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowvpn"
)

type vpnRuleRepositoryStub struct {
	item      VPNRule
	filter    VPNRuleListFilter
	expected  uint64
	created   bool
	updated   bool
	deleted   bool
	lastActor ID
}

func (s *vpnRuleRepositoryStub) ListVPNRules(_ context.Context, _ ID, filter VPNRuleListFilter) ([]VPNRule, int64, error) {
	s.filter = filter
	return []VPNRule{s.item}, 1, nil
}

func (s *vpnRuleRepositoryStub) GetVPNRule(_ context.Context, _ ID, id ID) (VPNRule, error) {
	if id != s.item.ID {
		return VPNRule{}, sql.ErrNoRows
	}
	return s.item, nil
}

func (s *vpnRuleRepositoryStub) CreateVPNRule(_ context.Context, item VPNRule) (VPNRule, error) {
	s.created, s.item = true, item
	s.item.RowVersion = 1
	return s.item, nil
}

func (s *vpnRuleRepositoryStub) UpdateVPNRule(_ context.Context, item VPNRule, expected uint64) (VPNRule, error) {
	s.updated, s.expected, s.item = true, expected, item
	s.item.RowVersion = expected + 1
	return s.item, nil
}

func (s *vpnRuleRepositoryStub) DeleteVPNRule(_ context.Context, _ ID, id, actor ID, expected uint64) error {
	if id != s.item.ID {
		return sql.ErrNoRows
	}
	s.deleted, s.expected, s.lastActor = true, expected, actor
	return nil
}

func TestFlowVPNRuleCRUDUsesTypedCanonicalInputETagAndAudit(t *testing.T) {
	repo := &vpnRuleRepositoryStub{item: VPNRule{ID: "rule-vpn-a", TenantID: "tenant-vpn", RowVersion: 4, CreatedBy: "creator"}}
	audit := &recordingAuditRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: flowVPNTestAuth(ActionVPNView, ActionConfigureAdjustment), FlowVPNRules: repo, Audit: audit,
	})

	list := httptest.NewRecorder()
	router.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/v1/flow/vpn/rules?q=edge&kind=passive&effect=score&status=draft&sort=priority&order=asc&limit=50&offset=100", nil))
	if list.Code != http.StatusOK || repo.filter.Search != "edge" || repo.filter.SortBy != "priority" || repo.filter.Descending || repo.filter.Limit != 50 || repo.filter.Offset != 100 {
		t.Fatalf("list status=%d filter=%+v body=%s", list.Code, repo.filter, list.Body.String())
	}

	create := httptest.NewRecorder()
	createBody := `{"name":" Risk rule ","kind":"passive","match":{"remote_ports":[443,443,8443],"remote_countries":["US"]},"effect":"score","weight":25,"priority":9,"status":"draft"}`
	router.ServeHTTP(create, httptest.NewRequest(http.MethodPost, "/api/v1/flow/vpn/rules", strings.NewReader(createBody)))
	if create.Code != http.StatusCreated || create.Header().Get("ETag") != `"1"` || !repo.created || repo.item.Name != "Risk rule" || len(repo.item.Match.RemotePorts) != 2 {
		t.Fatalf("create status=%d item=%+v body=%s", create.Code, repo.item, create.Body.String())
	}
	createdID := repo.item.ID
	repo.item.CreatedBy = "creator"

	patch := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPatch, "/api/v1/flow/vpn/rules/"+string(createdID), strings.NewReader(`{"name":"Allow edge","kind":"intelligence","match":{"remote_asns":[64512]},"effect":"allow","weight":0,"priority":10,"status":"active"}`))
	request.Header.Set("If-Match", `"1"`)
	router.ServeHTTP(patch, request)
	if patch.Code != http.StatusOK || patch.Header().Get("ETag") != `"2"` || !repo.updated || repo.expected != 1 || repo.item.CreatedBy != "creator" {
		t.Fatalf("patch status=%d expected=%d item=%+v body=%s", patch.Code, repo.expected, repo.item, patch.Body.String())
	}

	remove := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodDelete, "/api/v1/flow/vpn/rules/"+string(createdID), nil)
	request.Header.Set("If-Match", `"2"`)
	router.ServeHTTP(remove, request)
	if remove.Code != http.StatusNoContent || !repo.deleted || repo.expected != 2 || repo.lastActor != "user-vpn" {
		t.Fatalf("delete status=%d deleted=%v expected=%d actor=%q", remove.Code, repo.deleted, repo.expected, repo.lastActor)
	}
	if len(audit.logs) != 3 || audit.logs[0].Action != "flow.vpn_rule.created" || audit.logs[1].Action != "flow.vpn_rule.updated" || audit.logs[2].Action != "flow.vpn_rule.deleted" {
		t.Fatalf("audit=%+v", audit.logs)
	}
}

func TestFlowVPNRuleRejectsUnknownInvalidAndMissingPermission(t *testing.T) {
	valid := `{"name":"Risk","kind":"passive","match":{"remote_ports":[443]},"effect":"score","weight":25,"priority":9,"status":"draft"}`
	for name, testCase := range map[string]struct {
		auth       AuthContextAdapter
		method     string
		path       string
		body       string
		wantStatus int
	}{
		"permission":      {flowVPNTestAuth(ActionVPNView), http.MethodPost, "/api/v1/flow/vpn/rules", valid, http.StatusForbidden},
		"unknown":         {flowVPNTestAuth(ActionConfigureAdjustment), http.MethodPost, "/api/v1/flow/vpn/rules", strings.TrimSuffix(valid, "}") + `,"tenant_id":"other"}`, http.StatusBadRequest},
		"empty match":     {flowVPNTestAuth(ActionConfigureAdjustment), http.MethodPost, "/api/v1/flow/vpn/rules", `{"name":"Risk","kind":"passive","match":{},"effect":"score","weight":25,"status":"draft"}`, http.StatusBadRequest},
		"terminal weight": {flowVPNTestAuth(ActionConfigureAdjustment), http.MethodPost, "/api/v1/flow/vpn/rules", `{"name":"Risk","kind":"passive","match":{"remote_ports":[443]},"effect":"allow","weight":1,"status":"draft"}`, http.StatusBadRequest},
		"unknown filter":  {flowVPNTestAuth(ActionVPNView), http.MethodGet, "/api/v1/flow/vpn/rules?tenant_id=other", "", http.StatusBadRequest},
	} {
		t.Run(name, func(t *testing.T) {
			repo := &vpnRuleRepositoryStub{}
			router := NewAPIV1Router(APIV1RouterConfig{Auth: testCase.auth, FlowVPNRules: repo})
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(testCase.method, testCase.path, strings.NewReader(testCase.body)))
			if response.Code != testCase.wantStatus || repo.created || repo.updated || repo.deleted {
				t.Fatalf("status=%d mutated=%v/%v/%v body=%s", response.Code, repo.created, repo.updated, repo.deleted, response.Body.String())
			}
		})
	}
}

func TestVPNRuleMatchJSONUsesStableSnakeCaseFields(t *testing.T) {
	data, err := json.Marshal(flowvpn.Match{RemotePorts: []uint16{443}, MinDurationMS: pointer(uint64(1000))})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"remote_ports":[443],"min_duration_ms":1000}` {
		t.Fatalf("match JSON=%s", data)
	}
}

func pointer[T any](value T) *T { return &value }
