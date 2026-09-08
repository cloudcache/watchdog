package watchdog

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

type addressTaxonomyAPI struct {
	repo AddressTaxonomyRepository
}

func registerAddressTaxonomyRoutes(mux *http.ServeMux, viewAuth, adminAuth func(http.Handler) http.Handler, repo AddressTaxonomyRepository) {
	api := addressTaxonomyAPI{repo: repo}
	view := RequirePermission(ActionView, TenantResource)
	configure := RequirePermission(ActionConfigure, TenantResource)
	mux.Handle("GET /api/v1/geo/dictionary", viewAuth(view(http.HandlerFunc(api.listGeo))))
	mux.Handle("POST /api/v1/geo/dictionary", adminAuth(configure(http.HandlerFunc(api.createGeo))))
	mux.Handle("GET /api/v1/geo/dictionary/{id}", viewAuth(view(http.HandlerFunc(api.getGeo))))
	mux.Handle("PATCH /api/v1/geo/dictionary/{id}", adminAuth(configure(http.HandlerFunc(api.updateGeo))))
	mux.Handle("DELETE /api/v1/geo/dictionary/{id}", adminAuth(configure(http.HandlerFunc(api.deleteGeo))))
	mux.Handle("GET /api/v1/network/operators", viewAuth(view(http.HandlerFunc(api.listOperators))))
	mux.Handle("POST /api/v1/network/operators", adminAuth(configure(http.HandlerFunc(api.createOperator))))
	mux.Handle("GET /api/v1/network/operators/{id}", viewAuth(view(http.HandlerFunc(api.getOperator))))
	mux.Handle("PATCH /api/v1/network/operators/{id}", adminAuth(configure(http.HandlerFunc(api.updateOperator))))
	mux.Handle("DELETE /api/v1/network/operators/{id}", adminAuth(configure(http.HandlerFunc(api.deleteOperator))))
	mux.Handle("GET /api/v1/geo/lines", viewAuth(view(http.HandlerFunc(api.listLines))))
	mux.Handle("POST /api/v1/geo/lines", adminAuth(configure(http.HandlerFunc(api.createLine))))
	mux.Handle("GET /api/v1/geo/lines/{id}", viewAuth(view(http.HandlerFunc(api.getLine))))
	mux.Handle("PATCH /api/v1/geo/lines/{id}", adminAuth(configure(http.HandlerFunc(api.updateLine))))
	mux.Handle("DELETE /api/v1/geo/lines/{id}", adminAuth(configure(http.HandlerFunc(api.deleteLine))))
}

type geoDictionaryInput struct {
	Kind      *string `json:"kind,omitempty"`
	Code      *string `json:"code,omitempty"`
	ParentID  *ID     `json:"parent_id,omitempty"`
	Name      *string `json:"name,omitempty"`
	ShortName *string `json:"short_name,omitempty"`
	SortOrder *int    `json:"sort_order,omitempty"`
	Enabled   *bool   `json:"enabled,omitempty"`
}

type ispOperatorInput struct {
	Code      *string   `json:"code,omitempty"`
	Name      *string   `json:"name,omitempty"`
	ShortName *string   `json:"short_name,omitempty"`
	Category  *string   `json:"category,omitempty"`
	ASNs      *[]uint32 `json:"asns,omitempty"`
	SortOrder *int      `json:"sort_order,omitempty"`
	Enabled   *bool     `json:"enabled,omitempty"`
}

type geoLineInput struct {
	ParentID     *ID              `json:"parent_id,omitempty"`
	Code         *string          `json:"code,omitempty"`
	Name         *string          `json:"name,omitempty"`
	Description  *string          `json:"description,omitempty"`
	GeoSelector  *GeoLineSelector `json:"geo_selector,omitempty"`
	OperatorID   *ID              `json:"operator_id,omitempty"`
	AddressSetID *string          `json:"address_set_id,omitempty"`
	SortOrder    *int             `json:"sort_order,omitempty"`
	Enabled      *bool            `json:"enabled,omitempty"`
}

func (api addressTaxonomyAPI) listGeo(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filter, ok := parseAddressTaxonomyListFilter(w, r, true, true, geoDictionarySortColumns)
	if !ok {
		return
	}
	items, cursor, total, err := api.repo.ListGeoDictionary(r.Context(), auth.TenantID, filter)
	writeAddressTaxonomyList(w, items, cursor, total, filter, err)
}

func (api addressTaxonomyAPI) getGeo(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetGeoDictionary(r.Context(), auth.TenantID, ID(r.PathValue("id")))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

func (api addressTaxonomyAPI) createGeo(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var input geoDictionaryInput
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	item := applyGeoDictionaryInput(GeoDictionaryNode{TenantID: auth.TenantID, Enabled: true}, input)
	created, err := api.repo.CreateGeoDictionary(r.Context(), item)
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(created.RowVersion))
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api addressTaxonomyAPI) updateGeo(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetGeoDictionary(r.Context(), auth.TenantID, ID(r.PathValue("id")))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	var input geoDictionaryInput
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	updated, err := api.repo.UpdateGeoDictionary(r.Context(), applyGeoDictionaryInput(item, input), expected)
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(updated.RowVersion))
	WriteAPIJSON(w, http.StatusOK, updated)
}

func applyGeoDictionaryInput(item GeoDictionaryNode, input geoDictionaryInput) GeoDictionaryNode {
	if input.Kind != nil {
		item.Kind = *input.Kind
	}
	if input.Code != nil {
		item.Code = *input.Code
	}
	if input.ParentID != nil {
		item.ParentID = *input.ParentID
	}
	if input.Name != nil {
		item.Name = *input.Name
	}
	if input.ShortName != nil {
		item.ShortName = *input.ShortName
	}
	if input.SortOrder != nil {
		item.SortOrder = *input.SortOrder
	}
	if input.Enabled != nil {
		item.Enabled = *input.Enabled
	}
	return item
}

func (api addressTaxonomyAPI) deleteGeo(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	if err := api.repo.DeleteGeoDictionary(r.Context(), auth.TenantID, ID(r.PathValue("id")), expected); err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (api addressTaxonomyAPI) listOperators(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filter, ok := parseAddressTaxonomyListFilter(w, r, false, false, ispOperatorSortColumns)
	if !ok {
		return
	}
	items, cursor, total, err := api.repo.ListISPOperators(r.Context(), auth.TenantID, filter)
	writeAddressTaxonomyList(w, items, cursor, total, filter, err)
}

func (api addressTaxonomyAPI) getOperator(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetISPOperator(r.Context(), auth.TenantID, ID(r.PathValue("id")))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

func (api addressTaxonomyAPI) createOperator(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var input ispOperatorInput
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	created, err := api.repo.CreateISPOperator(r.Context(), applyISPOperatorInput(ISPOperator{TenantID: auth.TenantID, Enabled: true}, input))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(created.RowVersion))
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api addressTaxonomyAPI) updateOperator(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetISPOperator(r.Context(), auth.TenantID, ID(r.PathValue("id")))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	var input ispOperatorInput
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	updated, err := api.repo.UpdateISPOperator(r.Context(), applyISPOperatorInput(item, input), expected)
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(updated.RowVersion))
	WriteAPIJSON(w, http.StatusOK, updated)
}

func applyISPOperatorInput(item ISPOperator, input ispOperatorInput) ISPOperator {
	if input.Code != nil {
		item.Code = *input.Code
	}
	if input.Name != nil {
		item.Name = *input.Name
	}
	if input.ShortName != nil {
		item.ShortName = *input.ShortName
	}
	if input.Category != nil {
		item.Category = *input.Category
	}
	if input.ASNs != nil {
		item.ASNs = append([]uint32(nil), (*input.ASNs)...)
	}
	if input.SortOrder != nil {
		item.SortOrder = *input.SortOrder
	}
	if input.Enabled != nil {
		item.Enabled = *input.Enabled
	}
	return item
}

func (api addressTaxonomyAPI) deleteOperator(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	if err := api.repo.DeleteISPOperator(r.Context(), auth.TenantID, ID(r.PathValue("id")), expected); err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (api addressTaxonomyAPI) listLines(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filter, ok := parseAddressTaxonomyListFilter(w, r, false, true, geoLineSortColumns)
	if !ok {
		return
	}
	items, cursor, total, err := api.repo.ListGeoLines(r.Context(), auth.TenantID, filter)
	writeAddressTaxonomyList(w, items, cursor, total, filter, err)
}

func (api addressTaxonomyAPI) getLine(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetGeoLine(r.Context(), auth.TenantID, ID(r.PathValue("id")))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(item.RowVersion))
	WriteAPIJSON(w, http.StatusOK, item)
}

func (api addressTaxonomyAPI) createLine(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var input geoLineInput
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	created, err := api.repo.CreateGeoLine(r.Context(), applyGeoLineInput(GeoLine{TenantID: auth.TenantID, Enabled: true}, input))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(created.RowVersion))
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api addressTaxonomyAPI) updateLine(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	item, err := api.repo.GetGeoLine(r.Context(), auth.TenantID, ID(r.PathValue("id")))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	var input geoLineInput
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	updated, err := api.repo.UpdateGeoLine(r.Context(), applyGeoLineInput(item, input), expected)
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(updated.RowVersion))
	WriteAPIJSON(w, http.StatusOK, updated)
}

func applyGeoLineInput(item GeoLine, input geoLineInput) GeoLine {
	if input.ParentID != nil {
		item.ParentID = *input.ParentID
	}
	if input.Code != nil {
		item.Code = *input.Code
	}
	if input.Name != nil {
		item.Name = *input.Name
	}
	if input.Description != nil {
		item.Description = *input.Description
	}
	if input.GeoSelector != nil {
		item.GeoSelector = *input.GeoSelector
	}
	if input.OperatorID != nil {
		item.OperatorID = *input.OperatorID
	}
	if input.AddressSetID != nil {
		item.AddressSetID = *input.AddressSetID
	}
	if input.SortOrder != nil {
		item.SortOrder = *input.SortOrder
	}
	if input.Enabled != nil {
		item.Enabled = *input.Enabled
	}
	return item
}

func (api addressTaxonomyAPI) deleteLine(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	if err := api.repo.DeleteGeoLine(r.Context(), auth.TenantID, ID(r.PathValue("id")), expected); err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseAddressTaxonomyListFilter(w http.ResponseWriter, r *http.Request, allowKind, allowParent bool, sortColumns map[string]string) (AddressTaxonomyListFilter, bool) {
	query := r.URL.Query()
	for key := range query {
		switch key {
		case "q", "kind", "parent_id", "enabled", "sort", "order", "limit", "offset", "cursor":
		default:
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "unsupported query parameter: "+key, nil)
			return AddressTaxonomyListFilter{}, false
		}
	}
	filter := AddressTaxonomyListFilter{
		Search: strings.TrimSpace(query.Get("q")), Cursor: strings.TrimSpace(query.Get("cursor")), Sort: strings.TrimSpace(query.Get("sort")),
	}
	if len(filter.Search) > 255 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "q must be at most 255 characters", nil)
		return filter, false
	}
	filter.TableMode = query.Has("sort") || query.Has("order") || query.Has("offset")
	if filter.Cursor != "" && filter.TableMode {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "cursor cannot be combined with table query parameters", nil)
		return filter, false
	}
	if _, ok := sortColumns[filter.Sort]; !ok {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid taxonomy sort", nil)
		return filter, false
	}
	order := strings.ToLower(strings.TrimSpace(query.Get("order")))
	if order != "" && order != "asc" && order != "desc" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "order must be asc or desc", nil)
		return filter, false
	}
	filter.Desc = order == "desc"
	if allowKind {
		filter.Kind = strings.ToLower(strings.TrimSpace(query.Get("kind")))
		if filter.Kind != "" && !validGeoKind(filter.Kind) {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid geography kind", nil)
			return filter, false
		}
	} else if query.Has("kind") {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "kind is not supported by this list", nil)
		return filter, false
	}
	if allowParent {
		filter.ParentID = ID(strings.TrimSpace(query.Get("parent_id")))
	} else if query.Has("parent_id") {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "parent_id is not supported by this list", nil)
		return filter, false
	}
	if raw := strings.TrimSpace(query.Get("enabled")); raw != "" {
		raw = strings.ToLower(raw)
		if raw != "true" && raw != "false" {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "enabled must be true or false", nil)
			return filter, false
		}
		value := raw == "true"
		filter.Enabled = &value
	}
	var err error
	filter.Limit, err = parseAgentPageInteger(query.Get("limit"), 100, 1, 500)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be between 1 and 500", nil)
		return filter, false
	}
	filter.Offset, err = parseAgentPageInteger(query.Get("offset"), 0, 0, int(^uint(0)>>1))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "offset must be zero or greater", nil)
		return filter, false
	}
	return filter, true
}

func decodeAddressTaxonomyInput(w http.ResponseWriter, r *http.Request, destination any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return false
	}
	if err := ensureAddressJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return false
	}
	return true
}

func requireAddressTaxonomyIfMatch(w http.ResponseWriter, r *http.Request) (uint64, bool) {
	if strings.TrimSpace(r.Header.Get("If-Match")) == "" {
		WriteAPIError(w, http.StatusPreconditionRequired, APIErrorCode("precondition_required"), "If-Match is required", nil)
		return 0, false
	}
	return parseUserPreferencesIfMatch(w, r)
}

func writeAddressTaxonomyList[T any](w http.ResponseWriter, items []T, cursor string, total int, filter AddressTaxonomyListFilter, err error) {
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	if items == nil {
		items = []T{}
	}
	response := map[string]any{"items": items, "total": total}
	if filter.TableMode {
		response["limit"] = filter.Limit
		response["offset"] = filter.Offset
	}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func writeAddressTaxonomyError(w http.ResponseWriter, err error) {
	var mysqlErr *mysqldriver.MySQLError
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Address taxonomy item not found", nil)
	case errors.Is(err, ErrAddressTaxonomyInvalid):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
	case errors.Is(err, ErrAddressTaxonomyConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), "Address taxonomy item changed since it was read", nil)
	case errors.Is(err, ErrAddressTaxonomyCycle), errors.Is(err, ErrAddressTaxonomyInUse):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, err.Error(), nil)
	case errors.As(err, &mysqlErr) && (mysqlErr.Number == 1062 || mysqlErr.Number == 1451):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Address taxonomy item conflicts with an existing or referenced item", nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
	}
}
