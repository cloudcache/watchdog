package watchdog

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type flowVPNManagementAPI struct {
	repo    VPNFindingRepository
	exports ExportRepository
	audit   AuditRepository
}

func registerFlowVPNManagementRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo VPNFindingRepository, exports ExportRepository, audit AuditRepository) {
	api := flowVPNManagementAPI{repo: repo, exports: exports, audit: audit}
	view := RequirePermission(ActionVPNView, TenantResource)
	triage := RequirePermission(ActionVPNTriage, TenantResource)
	mux.Handle("GET /api/v1/flow/vpn/findings", auth(view(http.HandlerFunc(api.list))))
	mux.Handle("GET /api/v1/flow/vpn/findings/facets", auth(view(http.HandlerFunc(api.facets))))
	mux.Handle("GET /api/v1/flow/vpn/findings/{finding_id}", auth(view(http.HandlerFunc(api.get))))
	mux.Handle("POST /api/v1/flow/vpn/findings/{finding_id}/actions/disposition", auth(triage(http.HandlerFunc(api.disposition))))
	if exports != nil {
		export := RequirePermission(ActionVPNExport, TenantResource)
		mux.Handle("POST /api/v1/flow/vpn/findings/exports", auth(view(export(http.HandlerFunc(api.createExport)))))
	}
}

type flowVPNFindingExportCreateRequest struct {
	From             time.Time           `json:"from"`
	To               time.Time           `json:"to"`
	Search           string              `json:"search,omitempty"`
	ColumnFilters    map[string][]string `json:"column_filters,omitempty"`
	SortBy           string              `json:"sort_by,omitempty"`
	SortDirection    string              `json:"sort_direction,omitempty"`
	Limit            uint32              `json:"limit,omitempty"`
	Format           ExportFormat        `json:"format"`
	RetentionSeconds uint32              `json:"retention_seconds,omitempty"`
}

func (api flowVPNManagementAPI) list(w http.ResponseWriter, r *http.Request) {
	filter, err := parseVPNFindingListFilter(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	items, total, err := api.repo.ListVPNFindingsPage(r.Context(), auth.TenantID, filter)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorServiceUnavailable, "VPN finding query failed", nil)
		return
	}
	if items == nil {
		items = []VPNFinding{}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{
		"items": items, "total": total, "limit": filter.Limit, "offset": filter.Offset,
		"meta": map[string]any{"sort": filter.SortBy + ":" + strings.ToLower(filter.SortDirection) + ",id:" + strings.ToLower(filter.SortDirection)},
	})
}

func (api flowVPNManagementAPI) facets(w http.ResponseWriter, r *http.Request) {
	filter, err := parseVPNFindingFacetFilter(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	items, err := api.repo.ListVPNFindingFacets(r.Context(), auth.TenantID, filter)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorServiceUnavailable, "VPN finding facet query failed", nil)
		return
	}
	if items == nil {
		items = []VPNFindingFacet{}
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items), "field": filter.Field})
}

func (api flowVPNManagementAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetVPNFinding(r.Context(), auth.TenantID, ID(r.PathValue("finding_id")))
	if err != nil {
		writeVPNFindingError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

func (api flowVPNManagementAPI) createExport(w http.ResponseWriter, r *http.Request) {
	var input flowVPNFindingExportCreateRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	task, err := prepareVPNFindingExportTask(auth, input)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	task.ID, err = newExportTaskID()
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorServiceUnavailable, "VPN finding export ID generation failed", nil)
		return
	}
	created, err := api.exports.CreateExportTask(r.Context(), task)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if api.audit != nil {
		_ = api.audit.CreateAuditLog(r.Context(), AuditLog{
			TenantID: auth.TenantID, ActorID: auth.UserID, Action: "flow.vpn_findings.export_created",
			ResourceType: ResourceExportTask, ResourceID: created.ID,
			Detail: map[string]any{
				"dataset_key": created.DatasetKey, "format": created.Format,
				"query_hash": created.QueryHash, "operation_job_id": created.OperationJobID,
			},
		})
	}
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api flowVPNManagementAPI) disposition(w http.ResponseWriter, r *http.Request) {
	if strings.TrimSpace(r.Header.Get("If-Match")) == "" {
		WriteAPIError(w, http.StatusPreconditionRequired, APIErrorInvalidRequest, "If-Match with the current quoted row_version is required", nil)
		return
	}
	expectedVersion, err := parseCollectorPrincipalIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	var input struct {
		Disposition string `json:"disposition"`
		Note        string `json:"note"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := ensureDashboardJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := validateVPNFindingDisposition(input.Disposition, input.Note); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	auth, _ := AuthFromContext(r.Context())
	findingID := ID(r.PathValue("finding_id"))
	item, err := api.repo.UpdateVPNFindingDisposition(r.Context(), auth.TenantID, findingID, input.Disposition, input.Note, auth.UserID, expectedVersion)
	if err != nil {
		writeVPNFindingError(w, err)
		return
	}
	if api.audit != nil {
		_ = api.audit.CreateAuditLog(r.Context(), AuditLog{
			TenantID: auth.TenantID, ActorID: auth.UserID, Action: "flow.vpn_finding.disposition_updated",
			ResourceType: ResourceVPNFinding, ResourceID: findingID,
			Detail: map[string]any{"disposition": item.Disposition, "row_version": item.RowVersion},
		})
	}
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

func parseVPNFindingListFilter(r *http.Request) (VPNFindingListFilter, error) {
	query := r.URL.Query()
	if err := rejectUnknownVPNFindingQuery(query, false); err != nil {
		return VPNFindingListFilter{}, err
	}
	filter := VPNFindingListFilter{
		Search: strings.TrimSpace(query.Get("q")), SortBy: strings.TrimSpace(query.Get("sort_by")),
		SortDirection: strings.TrimSpace(query.Get("sort_direction")), ColumnFilters: vpnFindingColumnFiltersFromQuery(query),
	}
	var err error
	if filter.Limit, err = parseVPNFindingInteger(query.Get("limit"), 25); err != nil {
		return VPNFindingListFilter{}, err
	}
	if filter.Offset, err = parseVPNFindingInteger(query.Get("offset"), 0); err != nil {
		return VPNFindingListFilter{}, err
	}
	if filter.From, filter.To, err = parseVPNFindingTimeRange(query.Get("from"), query.Get("to")); err != nil {
		return VPNFindingListFilter{}, err
	}
	if err := validateVPNFindingListFilter(&filter); err != nil {
		return VPNFindingListFilter{}, err
	}
	return filter, nil
}

func parseVPNFindingFacetFilter(r *http.Request) (VPNFindingFacetFilter, error) {
	query := r.URL.Query()
	if err := rejectUnknownVPNFindingQuery(query, true); err != nil {
		return VPNFindingFacetFilter{}, err
	}
	filter := VPNFindingFacetFilter{
		Field: strings.TrimSpace(query.Get("field")), Search: strings.TrimSpace(query.Get("q")),
		Query:         strings.TrimSpace(query.Get("search")),
		ColumnFilters: vpnFindingColumnFiltersFromQuery(query),
	}
	var err error
	if filter.Limit, err = parseVPNFindingInteger(query.Get("limit"), 50); err != nil {
		return VPNFindingFacetFilter{}, err
	}
	if filter.From, filter.To, err = parseVPNFindingTimeRange(query.Get("from"), query.Get("to")); err != nil {
		return VPNFindingFacetFilter{}, err
	}
	if err := validateVPNFindingFacetFilter(&filter); err != nil {
		return VPNFindingFacetFilter{}, err
	}
	return filter, nil
}

func rejectUnknownVPNFindingQuery(query map[string][]string, facet bool) error {
	allowed := map[string]struct{}{"q": {}, "limit": {}, "from": {}, "to": {}}
	if facet {
		allowed["field"] = struct{}{}
		allowed["search"] = struct{}{}
	} else {
		allowed["offset"] = struct{}{}
		allowed["sort_by"] = struct{}{}
		allowed["sort_direction"] = struct{}{}
	}
	for key := range query {
		if _, ok := allowed[key]; ok || strings.HasPrefix(key, "filter.") {
			continue
		}
		return errors.New("unknown VPN finding query parameter " + strconv.Quote(key))
	}
	return nil
}

func vpnFindingColumnFiltersFromQuery(query map[string][]string) map[string][]string {
	filters := make(map[string][]string)
	for key, values := range query {
		if field := strings.TrimPrefix(key, "filter."); field != key {
			filters[field] = append([]string(nil), values...)
		}
	}
	return filters
}

func parseVPNFindingInteger(raw string, defaultValue int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New("VPN finding limit and offset must be integers")
	}
	return value, nil
}

func parseVPNFindingTimeRange(rawFrom, rawTo string) (time.Time, time.Time, error) {
	if strings.TrimSpace(rawFrom) == "" && strings.TrimSpace(rawTo) == "" {
		return time.Time{}, time.Time{}, nil
	}
	from, err := time.Parse(time.RFC3339Nano, rawFrom)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("VPN finding from must be RFC3339")
	}
	to, err := time.Parse(time.RFC3339Nano, rawTo)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("VPN finding to must be RFC3339")
	}
	return from.UTC(), to.UTC(), nil
}

func writeVPNFindingError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "VPN finding not found", nil)
	case errors.Is(err, ErrVPNFindingConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), err.Error(), nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorServiceUnavailable, "VPN finding service failed", nil)
	}
}
