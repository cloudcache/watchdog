package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/cloudcache/watchdog/internal/flowvpn"
)

type flowVPNRuleAPI struct {
	repo  VPNRuleRepository
	audit AuditRepository
}

type flowVPNRuleInput struct {
	Name     string             `json:"name"`
	Kind     string             `json:"kind"`
	Match    flowvpn.Match      `json:"match"`
	Effect   flowvpn.RuleEffect `json:"effect"`
	Weight   uint16             `json:"weight"`
	Priority uint16             `json:"priority"`
	Status   string             `json:"status"`
}

func registerFlowVPNRuleRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo VPNRuleRepository, audit AuditRepository) {
	api := flowVPNRuleAPI{repo: repo, audit: audit}
	view := RequirePermission(ActionVPNView, TenantResource)
	configure := RequirePermission(ActionConfigureAdjustment, TenantResource)
	mux.Handle("GET /api/v1/flow/vpn/rules", auth(view(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/flow/vpn/rules", auth(configure(http.HandlerFunc(api.create))))
	mux.Handle("GET /api/v1/flow/vpn/rules/{rule_id}", auth(view(http.HandlerFunc(api.get))))
	mux.Handle("PATCH /api/v1/flow/vpn/rules/{rule_id}", auth(configure(http.HandlerFunc(api.patch))))
	mux.Handle("DELETE /api/v1/flow/vpn/rules/{rule_id}", auth(configure(http.HandlerFunc(api.delete))))
}

func (api flowVPNRuleAPI) list(w http.ResponseWriter, r *http.Request) {
	filter, err := parseVPNRuleListFilter(r)
	if err != nil {
		writeVPNRuleError(w, err)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	items, total, err := api.repo.ListVPNRules(r.Context(), auth.TenantID, filter)
	if err != nil {
		writeVPNRuleError(w, err)
		return
	}
	if items == nil {
		items = []VPNRule{}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items, "total": total, "limit": filter.Limit, "offset": filter.Offset})
}

func (api flowVPNRuleAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetVPNRule(r.Context(), auth.TenantID, ID(r.PathValue("rule_id")))
	if err != nil {
		writeVPNRuleError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

func (api flowVPNRuleAPI) create(w http.ResponseWriter, r *http.Request) {
	input, ok := decodeVPNRuleInput(w, r)
	if !ok {
		return
	}
	auth, _ := AuthFromContext(r.Context())
	ruleID, err := newManagementID()
	if err != nil {
		writeVPNRuleError(w, err)
		return
	}
	item, err := normalizeVPNRule(vpnRuleFromInput(ruleID, auth, input))
	if err != nil {
		writeVPNRuleError(w, err)
		return
	}
	created, err := api.repo.CreateVPNRule(r.Context(), item)
	if err != nil {
		writeVPNRuleError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "flow.vpn_rule.created", created, map[string]any{"status": created.Status})
	w.Header().Set("ETag", quotedRowVersion(created.RowVersion))
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api flowVPNRuleAPI) patch(w http.ResponseWriter, r *http.Request) {
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	input, ok := decodeVPNRuleInput(w, r)
	if !ok {
		return
	}
	auth, _ := AuthFromContext(r.Context())
	ruleID := ID(r.PathValue("rule_id"))
	existing, err := api.repo.GetVPNRule(r.Context(), auth.TenantID, ruleID)
	if err != nil {
		writeVPNRuleError(w, err)
		return
	}
	item := vpnRuleFromInput(ruleID, auth, input)
	item.CreatedBy = existing.CreatedBy
	item, err = normalizeVPNRule(item)
	if err != nil {
		writeVPNRuleError(w, err)
		return
	}
	updated, err := api.repo.UpdateVPNRule(r.Context(), item, expected)
	if err != nil {
		writeVPNRuleError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "flow.vpn_rule.updated", updated, map[string]any{
		"previous_row_version": expected, "row_version": updated.RowVersion,
		"previous_status": existing.Status, "status": updated.Status,
	})
	w.Header().Set("ETag", quotedRowVersion(updated.RowVersion))
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api flowVPNRuleAPI) delete(w http.ResponseWriter, r *http.Request) {
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	auth, _ := AuthFromContext(r.Context())
	ruleID := ID(r.PathValue("rule_id"))
	if err := api.repo.DeleteVPNRule(r.Context(), auth.TenantID, ruleID, auth.UserID, expected); err != nil {
		writeVPNRuleError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "flow.vpn_rule.deleted", VPNRule{ID: ruleID}, map[string]any{"row_version": expected})
	w.WriteHeader(http.StatusNoContent)
}

func vpnRuleFromInput(id ID, auth AuthContext, input flowVPNRuleInput) VPNRule {
	return VPNRule{
		ID: id, TenantID: auth.TenantID, Name: input.Name, Kind: input.Kind, Match: input.Match,
		Effect: input.Effect, Weight: input.Weight, Priority: input.Priority, Status: input.Status,
		CreatedBy: auth.UserID, UpdatedBy: auth.UserID,
	}
}

func decodeVPNRuleInput(w http.ResponseWriter, r *http.Request) (flowVPNRuleInput, bool) {
	var input flowVPNRuleInput
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return input, false
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return input, false
	}
	return input, true
}

func parseVPNRuleListFilter(r *http.Request) (VPNRuleListFilter, error) {
	values := r.URL.Query()
	allowed := map[string]struct{}{"q": {}, "kind": {}, "effect": {}, "status": {}, "sort": {}, "order": {}, "limit": {}, "offset": {}}
	for key := range values {
		if _, ok := allowed[key]; !ok {
			return VPNRuleListFilter{}, fmt.Errorf("%w: unknown query parameter %q", ErrVPNRuleInvalid, key)
		}
	}
	filter := VPNRuleListFilter{Search: values.Get("q"), Kind: values.Get("kind"), Effect: values.Get("effect"), Status: values.Get("status"), SortBy: values.Get("sort")}
	order := strings.ToLower(strings.TrimSpace(values.Get("order")))
	if order != "" && order != "asc" && order != "desc" {
		return filter, fmt.Errorf("%w: order must be asc or desc", ErrVPNRuleInvalid)
	}
	filter.Descending = order == "desc"
	var err error
	if filter.Limit, err = parseVPNRuleInteger(values.Get("limit"), 25, 1, 200); err != nil {
		return filter, err
	}
	if filter.Offset, err = parseVPNRuleInteger(values.Get("offset"), 0, 0, 100_000); err != nil {
		return filter, err
	}
	return normalizeVPNRuleListFilter(filter)
}

func parseVPNRuleInteger(raw string, fallback, minimum, maximum int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%w: value must be between %d and %d", ErrVPNRuleInvalid, minimum, maximum)
	}
	return value, nil
}

func writeVPNRuleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "VPN rule not found", nil)
	case errors.Is(err, ErrVPNRuleInvalid):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
	case errors.Is(err, ErrVPNRuleNameConflict):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, err.Error(), nil)
	case errors.Is(err, ErrVPNRuleVersionConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), err.Error(), nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorServiceUnavailable, "VPN rule storage is unavailable", nil)
	}
}

func (api flowVPNRuleAPI) recordAudit(ctx context.Context, auth AuthContext, action string, item VPNRule, detail map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID: auth.TenantID, ActorID: auth.UserID, Action: action,
		ResourceType: ResourceVPNRule, ResourceID: item.ID, Detail: detail,
	})
}
