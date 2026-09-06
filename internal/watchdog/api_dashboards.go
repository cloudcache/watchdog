package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type dashboardAPI struct {
	repo  DashboardRepository
	audit AuditRepository
}

func registerDashboardRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo DashboardRepository, audit AuditRepository) {
	api := dashboardAPI{repo: repo, audit: audit}
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	viewTenant := RequirePermission(ActionView, TenantResource)
	mux.Handle("GET /api/v1/dashboards", auth(viewTenant(http.HandlerFunc(api.list))))
	mux.Handle("GET /api/v1/dashboards/graph-options", auth(viewTenant(http.HandlerFunc(api.listGraphOptions))))
	mux.Handle("POST /api/v1/dashboards", auth(configureTenant(http.HandlerFunc(api.create))))
	mux.Handle("POST /api/v1/dashboards/actions/preview", auth(viewTenant(http.HandlerFunc(api.previewDraft))))
	mux.Handle("GET /api/v1/dashboards/{dashboard_id}", auth(viewTenant(http.HandlerFunc(api.get))))
	mux.Handle("GET /api/v1/dashboards/{dashboard_id}/preview", auth(viewTenant(http.HandlerFunc(api.previewSaved))))
	mux.Handle("PATCH /api/v1/dashboards/{dashboard_id}", auth(configureTenant(http.HandlerFunc(api.patch))))
	mux.Handle("DELETE /api/v1/dashboards/{dashboard_id}", auth(configureTenant(http.HandlerFunc(api.delete))))
}

func (api dashboardAPI) listGraphOptions(w http.ResponseWriter, r *http.Request) {
	repo, ok := api.repo.(DashboardGraphOptionRepository)
	if !ok {
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Dashboard graph lookup is unavailable", nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	filter, ok := parseDashboardGraphOptionListFilter(w, r)
	if !ok {
		return
	}
	graphs, total, err := repo.ListDashboardGraphOptions(r.Context(), auth.TenantID, filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": graphs, "total": total, "limit": filter.Limit, "offset": filter.Offset})
}

func (api dashboardAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filter, ok := parseDashboardListFilter(w, r)
	if !ok {
		return
	}
	dashboards, total, err := api.repo.ListDashboards(r.Context(), auth.TenantID, filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": dashboards, "total": total, "limit": filter.Limit, "offset": filter.Offset})
}

func (api dashboardAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	dashboard, err := api.repo.GetDashboard(r.Context(), auth.TenantID, ID(r.PathValue("dashboard_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Dashboard not found", nil)
		return
	}
	setDashboardETag(w, dashboard.Version)
	WriteAPIJSON(w, http.StatusOK, dashboard)
}

func (api dashboardAPI) create(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	dashboard, err := decodeDashboardRequest(w, r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	dashboard.TenantID = auth.TenantID
	dashboard.OwnerID = auth.UserID
	created, err := api.repo.CreateDashboard(r.Context(), dashboard)
	if err != nil {
		api.writeWriteError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "dashboard.created", created.ID, map[string]any{"version": created.Version})
	setDashboardETag(w, created.Version)
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api dashboardAPI) patch(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	dashboardID := ID(r.PathValue("dashboard_id"))
	existing, err := api.repo.GetDashboard(r.Context(), auth.TenantID, dashboardID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Dashboard not found", nil)
		return
	}
	expectedVersion, ok := dashboardIfMatch(w, r, existing.Version)
	if !ok {
		return
	}
	dashboard, err := decodeDashboardRequest(w, r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	dashboard.ID = dashboardID
	dashboard.TenantID = auth.TenantID
	dashboard.OwnerID = existing.OwnerID
	updated, err := api.repo.UpdateDashboard(r.Context(), dashboard, expectedVersion)
	if err != nil {
		api.writeWriteError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "dashboard.updated", updated.ID, map[string]any{
		"previous_version": expectedVersion, "version": updated.Version,
	})
	setDashboardETag(w, updated.Version)
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api dashboardAPI) delete(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	dashboardID := ID(r.PathValue("dashboard_id"))
	existing, err := api.repo.GetDashboard(r.Context(), auth.TenantID, dashboardID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Dashboard not found", nil)
		return
	}
	expectedVersion, ok := dashboardIfMatch(w, r, existing.Version)
	if !ok {
		return
	}
	if err := api.repo.DeleteDashboard(r.Context(), auth.TenantID, dashboardID, expectedVersion); err != nil {
		api.writeWriteError(w, err)
		return
	}
	api.recordAudit(r.Context(), auth, "dashboard.deleted", dashboardID, map[string]any{"version": expectedVersion})
	w.WriteHeader(http.StatusNoContent)
}

func (api dashboardAPI) previewDraft(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	dashboard, err := decodeDashboardRequest(w, r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	dashboard.TenantID = auth.TenantID
	dashboard.OwnerID = auth.UserID
	api.writePreview(w, r, dashboard)
}

func (api dashboardAPI) previewSaved(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	dashboard, err := api.repo.GetDashboard(r.Context(), auth.TenantID, ID(r.PathValue("dashboard_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Dashboard not found", nil)
		return
	}
	setDashboardETag(w, dashboard.Version)
	api.writePreview(w, r, dashboard)
}

func (api dashboardAPI) writePreview(w http.ResponseWriter, r *http.Request, dashboard Dashboard) {
	graphIDs, err := dashboardGraphIDs(dashboard.Layout)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	references, err := api.repo.ResolveDashboardGraphReferences(r.Context(), dashboard.TenantID, graphIDs)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	missing := make([]ID, 0)
	for _, reference := range references {
		if !reference.Exists {
			missing = append(missing, reference.GraphID)
		}
	}
	WriteAPIJSON(w, http.StatusOK, DashboardPreview{
		Dashboard: dashboard, References: references, MissingGraphIDs: missing,
	})
}

// writeWriteError maps repository write failures to the right status: a name
// clash is a 409 conflict, a missing row on update is a 404, everything else a
// 400.
func (api dashboardAPI) writeWriteError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrDashboardNameConflict):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, err.Error(), nil)
	case errors.Is(err, ErrDashboardVersionConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), err.Error(), nil)
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Dashboard not found", nil)
	default:
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
	}
}

func decodeDashboardRequest(w http.ResponseWriter, r *http.Request) (Dashboard, error) {
	defer r.Body.Close()
	var dashboard Dashboard
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&dashboard); err != nil {
		return Dashboard{}, err
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		return Dashboard{}, err
	}
	return normalizeDashboard(dashboard)
}

func ensureDashboardJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain one JSON object")
		}
		return err
	}
	return nil
}

func parseDashboardListFilter(w http.ResponseWriter, r *http.Request) (DashboardListFilter, bool) {
	query := r.URL.Query()
	filter := DashboardListFilter{
		Search: query.Get("q"), OwnerID: ID(strings.TrimSpace(query.Get("owner_id"))),
		Sort: query.Get("sort"), Desc: strings.EqualFold(strings.TrimSpace(query.Get("order")), "desc"),
	}
	if order := strings.TrimSpace(query.Get("order")); order != "" && !strings.EqualFold(order, "asc") && !strings.EqualFold(order, "desc") {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "order must be asc or desc", nil)
		return filter, false
	}
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return filter, false
		}
		filter.Limit = value
	}
	if raw := strings.TrimSpace(query.Get("offset")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "offset must be a non-negative integer", nil)
			return filter, false
		}
		filter.Offset = value
	}
	filter, err := normalizeDashboardListFilter(filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return filter, false
	}
	return filter, true
}

func parseDashboardGraphOptionListFilter(w http.ResponseWriter, r *http.Request) (DashboardGraphOptionListFilter, bool) {
	query := r.URL.Query()
	filter := DashboardGraphOptionListFilter{Search: query.Get("q")}
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return filter, false
		}
		filter.Limit = value
	}
	if raw := strings.TrimSpace(query.Get("offset")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "offset must be a non-negative integer", nil)
			return filter, false
		}
		filter.Offset = value
	}
	filter, err := normalizeDashboardGraphOptionListFilter(filter)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return filter, false
	}
	return filter, true
}

func dashboardIfMatch(w http.ResponseWriter, r *http.Request, current uint32) (uint32, bool) {
	raw := strings.TrimSpace(r.Header.Get("If-Match"))
	if raw == "" || raw == "*" {
		return current, true
	}
	if len(raw) < 3 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "If-Match must be a quoted dashboard version", nil)
		return 0, false
	}
	version, err := strconv.ParseUint(raw[1:len(raw)-1], 10, 32)
	if err != nil || version == 0 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "If-Match must be a quoted dashboard version", nil)
		return 0, false
	}
	if uint32(version) != current {
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"),
			"The dashboard changed since it was read", map[string]any{"current_etag": quotedRowVersion(uint64(current))})
		return 0, false
	}
	return uint32(version), true
}

func setDashboardETag(w http.ResponseWriter, version uint32) {
	w.Header().Set("ETag", quotedRowVersion(uint64(version)))
}

func (api dashboardAPI) recordAudit(ctx context.Context, auth AuthContext, action string, dashboardID ID, detail map[string]any) {
	if api.audit == nil {
		return
	}
	_ = api.audit.CreateAuditLog(ctx, AuditLog{
		TenantID: auth.TenantID, ActorID: auth.UserID, Action: action,
		ResourceType: "dashboard", ResourceID: dashboardID, Detail: detail,
	})
}
