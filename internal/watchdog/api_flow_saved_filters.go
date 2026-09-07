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

	"github.com/cloudcache/watchdog/internal/flowquery"
)

type flowSavedFilterAPI struct {
	repo  FlowSavedFilterRepository
	audit AuditRepository
}

type flowSavedFilterInput struct {
	Name        string                     `json:"name"`
	Description string                     `json:"description"`
	ShareScope  string                     `json:"share_scope"`
	Filter      flowquery.FilterExpression `json:"filter"`
}

func registerFlowSavedFilterRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo FlowSavedFilterRepository, audit AuditRepository) {
	api := flowSavedFilterAPI{repo: repo, audit: audit}
	viewCustomer := RequirePermission(ActionViewCustomer, TenantResource)
	mux.Handle("GET /api/v1/flow/filters", auth(viewCustomer(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/flow/filters", auth(viewCustomer(http.HandlerFunc(api.create))))
	mux.Handle("GET /api/v1/flow/filters/facets/owners", auth(viewCustomer(http.HandlerFunc(api.listOwners))))
	mux.Handle("GET /api/v1/flow/filters/{filter_id}", auth(viewCustomer(http.HandlerFunc(api.get))))
	mux.Handle("PATCH /api/v1/flow/filters/{filter_id}", auth(viewCustomer(http.HandlerFunc(api.patch))))
	mux.Handle("DELETE /api/v1/flow/filters/{filter_id}", auth(viewCustomer(http.HandlerFunc(api.delete))))
}

func (api flowSavedFilterAPI) listOwners(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	values := r.URL.Query()
	for key := range values {
		if key != "q" && key != "limit" {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, fmt.Sprintf("unknown query parameter %q", key), nil)
			return
		}
	}
	limit, err := parseFlowSavedFilterInteger(values.Get("limit"), 50, 1, 200)
	if err != nil || len(strings.TrimSpace(values.Get("q"))) > 255 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid owner facet query", nil)
		return
	}
	items, err := api.repo.ListFlowSavedFilterOwners(r.Context(), auth.TenantID, auth.UserID, values.Get("q"), limit)
	if err != nil {
		api.writeError(w, err)
		return
	}
	if items == nil {
		items = []FlowSavedFilterOwnerFacet{}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (api flowSavedFilterAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	query, err := parseFlowSavedFilterListQuery(r, auth)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	items, total, err := api.repo.ListFlowSavedFilters(r.Context(), auth.TenantID, query)
	if err != nil {
		api.writeError(w, err)
		return
	}
	if items == nil {
		items = []FlowSavedFilter{}
	}
	for index := range items {
		items[index].CanEdit = canEditFlowSavedFilter(auth, items[index])
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{
		"items": items, "total": total, "limit": query.Limit, "offset": query.Offset,
		"meta": map[string]any{"can_share": canConfigureTenant(auth)},
	})
}

func (api flowSavedFilterAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetFlowSavedFilter(r.Context(), auth.TenantID, auth.UserID, ID(r.PathValue("filter_id")))
	if err != nil {
		api.writeError(w, err)
		return
	}
	item.CanEdit = canEditFlowSavedFilter(auth, item)
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

func (api flowSavedFilterAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	input, ok := decodeFlowSavedFilterInput(w, r)
	if !ok {
		return
	}
	item, err := normalizeFlowSavedFilter(FlowSavedFilter{
		TenantID: auth.TenantID, OwnerUserID: auth.UserID,
		Name: input.Name, Description: input.Description, ShareScope: input.ShareScope, Filter: input.Filter,
	})
	if err != nil {
		api.writeError(w, err)
		return
	}
	if item.ShareScope == FlowSavedFilterTenant && !canConfigureTenant(auth) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Tenant-shared filters require configure permission", nil)
		return
	}
	created, err := api.repo.CreateFlowSavedFilter(r.Context(), item)
	if err != nil {
		api.writeError(w, err)
		return
	}
	created.CanEdit = true
	api.recordAudit(r.Context(), auth, "flow_saved_filter.created", created, map[string]any{"share_scope": created.ShareScope})
	w.Header().Set("ETag", quotedRowVersion(created.RowVersion))
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api flowSavedFilterAPI) patch(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filterID := ID(r.PathValue("filter_id"))
	existing, err := api.repo.GetFlowSavedFilter(r.Context(), auth.TenantID, auth.UserID, filterID)
	if err != nil {
		api.writeError(w, err)
		return
	}
	if !canEditFlowSavedFilter(auth, existing) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Saved Flow filter cannot be changed by this user", nil)
		return
	}
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	input, ok := decodeFlowSavedFilterInput(w, r)
	if !ok {
		return
	}
	item, err := normalizeFlowSavedFilter(FlowSavedFilter{
		ID: filterID, TenantID: auth.TenantID, OwnerUserID: existing.OwnerUserID,
		Name: input.Name, Description: input.Description, ShareScope: input.ShareScope, Filter: input.Filter,
	})
	if err != nil {
		api.writeError(w, err)
		return
	}
	if item.ShareScope == FlowSavedFilterTenant && !canConfigureTenant(auth) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Tenant-shared filters require configure permission", nil)
		return
	}
	updated, err := api.repo.UpdateFlowSavedFilter(r.Context(), item, expected)
	if err != nil {
		api.writeError(w, err)
		return
	}
	updated.CanEdit = canEditFlowSavedFilter(auth, updated)
	api.recordAudit(r.Context(), auth, "flow_saved_filter.updated", updated, map[string]any{
		"previous_row_version": expected, "row_version": updated.RowVersion,
		"previous_share_scope": existing.ShareScope, "share_scope": updated.ShareScope,
	})
	w.Header().Set("ETag", quotedRowVersion(updated.RowVersion))
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api flowSavedFilterAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filterID := ID(r.PathValue("filter_id"))
	existing, err := api.repo.GetFlowSavedFilter(r.Context(), auth.TenantID, auth.UserID, filterID)
	if err != nil {
		api.writeError(w, err)
		return
	}
	if !canEditFlowSavedFilter(auth, existing) {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, "Saved Flow filter cannot be deleted by this user", nil)
		return
	}
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	if err := api.repo.DeleteFlowSavedFilter(r.Context(), auth.TenantID, filterID, expected); err != nil {
		api.writeError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "flow_saved_filter.deleted", existing, map[string]any{
		"row_version": expected, "share_scope": existing.ShareScope,
	})
	w.WriteHeader(http.StatusNoContent)
}

func decodeFlowSavedFilterInput(w http.ResponseWriter, r *http.Request) (flowSavedFilterInput, bool) {
	var input flowSavedFilterInput
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

func parseFlowSavedFilterListQuery(r *http.Request, auth AuthContext) (FlowSavedFilterListQuery, error) {
	values := r.URL.Query()
	allowed := map[string]struct{}{"q": {}, "scope": {}, "owner_id": {}, "sort": {}, "order": {}, "limit": {}, "offset": {}}
	for key := range values {
		if _, ok := allowed[key]; !ok {
			return FlowSavedFilterListQuery{}, fmt.Errorf("unknown query parameter %q", key)
		}
	}
	query := FlowSavedFilterListQuery{
		ViewerUserID: auth.UserID, Search: values.Get("q"), ShareScope: values.Get("scope"),
		OwnerUserID: ID(strings.TrimSpace(values.Get("owner_id"))), SortBy: values.Get("sort"),
	}
	order := strings.ToLower(strings.TrimSpace(values.Get("order")))
	if order != "" && order != "asc" && order != "desc" {
		return query, errors.New("order must be asc or desc")
	}
	query.Descending = order == "desc"
	var err error
	if query.Limit, err = parseFlowSavedFilterInteger(values.Get("limit"), 25, 1, 200); err != nil {
		return query, err
	}
	if query.Offset, err = parseFlowSavedFilterInteger(values.Get("offset"), 0, 0, 100_000); err != nil {
		return query, err
	}
	return normalizeFlowSavedFilterListQuery(query)
}

func parseFlowSavedFilterInteger(raw string, fallback, minimum, maximum int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("value must be between %d and %d", minimum, maximum)
	}
	return value, nil
}

func canConfigureTenant(auth AuthContext) bool {
	if auth.IsAdmin {
		return true
	}
	return HasPermission(AccessRequest{
		TenantID: auth.TenantID, UserID: auth.UserID, RoleIDs: auth.RoleIDs, Action: ActionConfigure,
		Resource: ResourceRef{Type: ResourceTenant, ID: auth.TenantID},
	}, auth.Grants)
}

func canEditFlowSavedFilter(auth AuthContext, item FlowSavedFilter) bool {
	if item.ShareScope == FlowSavedFilterTenant {
		return canConfigureTenant(auth)
	}
	return item.OwnerUserID != "" && item.OwnerUserID == auth.UserID
}

func (api flowSavedFilterAPI) writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Saved Flow filter not found", nil)
	case errors.Is(err, ErrFlowSavedFilterVersionConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), err.Error(), nil)
	case errors.Is(err, ErrFlowSavedFilterInvalid):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorServiceUnavailable, "Saved Flow filter storage is unavailable", nil)
	}
}

func (api flowSavedFilterAPI) recordAudit(ctx context.Context, auth AuthContext, action string, item FlowSavedFilter, detail map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID: auth.TenantID, ActorID: auth.UserID, Action: action,
		ResourceType: ResourceType("flow_saved_filter"), ResourceID: item.ID, Detail: detail,
	})
}
