package watchdog

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

type addressSetAPI struct {
	repo AddressSetRepository
}

func registerAddressSetRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo AddressSetRepository) {
	api := addressSetAPI{repo: repo}
	viewTenant := RequirePermission(ActionView, TenantResource)
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	mux.Handle("GET /api/v1/address-prefixes", auth(viewTenant(http.HandlerFunc(api.listPrefixes))))
	mux.Handle("POST /api/v1/address-prefixes", auth(configureTenant(http.HandlerFunc(api.upsertPrefix))))
	mux.Handle("GET /api/v1/address-prefixes/{prefix_id}", auth(viewTenant(http.HandlerFunc(api.getPrefix))))
	mux.Handle("PATCH /api/v1/address-prefixes/{prefix_id}", auth(configureTenant(http.HandlerFunc(api.updatePrefix))))
	mux.Handle("DELETE /api/v1/address-prefixes/{prefix_id}", auth(configureTenant(http.HandlerFunc(api.deletePrefix))))
	mux.Handle("POST /api/v1/address-prefixes/actions/merge-preview", auth(viewTenant(http.HandlerFunc(api.mergePreviewPrefixes))))
	mux.Handle("GET /api/v1/address-sets", auth(viewTenant(http.HandlerFunc(api.listSets))))
	mux.Handle("POST /api/v1/address-sets", auth(configureTenant(http.HandlerFunc(api.createSet))))
	mux.Handle("GET /api/v1/address-sets/{set_id}", auth(viewTenant(http.HandlerFunc(api.getSet))))
	mux.Handle("PATCH /api/v1/address-sets/{set_id}", auth(configureTenant(http.HandlerFunc(api.updateSet))))
	mux.Handle("DELETE /api/v1/address-sets/{set_id}", auth(configureTenant(http.HandlerFunc(api.deleteSet))))
	mux.Handle("POST /api/v1/address-sets/actions/preview", auth(configureTenant(http.HandlerFunc(api.previewOperation))))
	registerAddressDraftRevisionRoutes(mux, auth, repo)
}

func (api addressSetAPI) previewOperation(w http.ResponseWriter, r *http.Request) {
	var request AddressSetOperationRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := ensureAddressJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	preview, err := PreviewAddressSetOperation(request)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, preview)
}

type addressPrefixMergeInput struct {
	CIDR       string            `json:"cidr"`
	GeoLeafID  ID                `json:"geo_leaf_id"`
	OperatorID ID                `json:"operator_id"`
	ASN        *uint32           `json:"asn"`
	Labels     map[string]string `json:"labels"`
	Source     string            `json:"source"`
}

type addressPrefixMergeRequest struct {
	Prefixes []addressPrefixMergeInput `json:"prefixes"`
}

// mergePreviewPrefixes computes an attribution-preserving merge plan for a set
// of prefixes (see PreviewAddressPrefixMerge). It is a pure computation over
// the submitted prefixes — no persistence — so a read grant is sufficient; the
// apply is ordinary create/delete under the configure grant.
func (api addressSetAPI) mergePreviewPrefixes(w http.ResponseWriter, r *http.Request) {
	var request addressPrefixMergeRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := ensureAddressJSONEOF(decoder); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if len(request.Prefixes) < 2 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "at least two prefixes are required to merge", nil)
		return
	}
	if len(request.Prefixes) > MaxAddressOperationInputs {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, fmt.Sprintf("merge accepts at most %d prefixes", MaxAddressOperationInputs), nil)
		return
	}
	prefixes := make([]AddressPrefix, 0, len(request.Prefixes))
	for _, input := range request.Prefixes {
		prefixes = append(prefixes, AddressPrefix{
			CIDR: input.CIDR, GeoLeafID: input.GeoLeafID, OperatorID: input.OperatorID,
			ASN: input.ASN, Labels: input.Labels, Source: input.Source,
		})
	}
	preview, err := PreviewAddressPrefixMerge(prefixes)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, preview)
}

func ensureAddressJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("request body must contain exactly one JSON value")
	}
	return err
}

func (api addressSetAPI) listPrefixes(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	query := r.URL.Query()
	for key := range query {
		switch key {
		case "q", "family", "source", "geo_leaf_id", "operator_id", "asn", "sort", "order", "limit", "offset", "cursor":
		default:
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "unsupported query parameter: "+key, nil)
			return
		}
	}
	filter := AddressPrefixListFilter{
		Search: strings.TrimSpace(query.Get("q")), Source: strings.TrimSpace(query.Get("source")), GeoLeafID: ID(strings.TrimSpace(query.Get("geo_leaf_id"))),
		OperatorID: ID(strings.TrimSpace(query.Get("operator_id"))), Cursor: strings.TrimSpace(query.Get("cursor")), Sort: strings.TrimSpace(query.Get("sort")),
	}
	if len(filter.Search) > 255 || len(filter.Source) > 32 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "address prefix search or source is too long", nil)
		return
	}
	filter.TableMode = query.Has("sort") || query.Has("order") || query.Has("offset")
	if filter.Cursor != "" && filter.TableMode {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "cursor cannot be combined with table query parameters", nil)
		return
	}
	if _, ok := addressPrefixSortColumns[filter.Sort]; !ok {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid address prefix sort", nil)
		return
	}
	order := strings.ToLower(strings.TrimSpace(query.Get("order")))
	if order != "" && order != "asc" && order != "desc" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "order must be asc or desc", nil)
		return
	}
	filter.Desc = order == "desc"
	if raw := strings.TrimSpace(query.Get("family")); raw != "" {
		family, err := strconv.ParseUint(raw, 10, 8)
		if err != nil || (family != 4 && family != 6) {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "family must be 4 or 6", nil)
			return
		}
		filter.Family = uint8(family)
	}
	if raw := strings.TrimSpace(query.Get("asn")); raw != "" {
		asn, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "asn must be an unsigned 32-bit integer", nil)
			return
		}
		value := uint32(asn)
		filter.ASN = &value
	}
	var err error
	filter.Limit, err = parseAgentPageInteger(query.Get("limit"), 100, 1, 500)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be between 1 and 500", nil)
		return
	}
	filter.Offset, err = parseAgentPageInteger(query.Get("offset"), 0, 0, int(^uint(0)>>1))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "offset must be zero or greater", nil)
		return
	}
	prefixes, cursor, total, err := api.repo.ListAddressPrefixesPage(r.Context(), auth.TenantID, filter)
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	if prefixes == nil {
		prefixes = []AddressPrefix{}
	}
	response := map[string]any{"items": prefixes, "total": total}
	if filter.TableMode {
		response["limit"] = filter.Limit
		response["offset"] = filter.Offset
	}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

type addressPrefixInput struct {
	CIDR       *string            `json:"cidr,omitempty"`
	Labels     *map[string]string `json:"labels,omitempty"`
	GeoLeafID  *ID                `json:"geo_leaf_id,omitempty"`
	OperatorID *ID                `json:"operator_id,omitempty"`
	ASN        *uint32            `json:"asn,omitempty"`
	Source     *string            `json:"source,omitempty"`
}

func (api addressSetAPI) upsertPrefix(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var input addressPrefixInput
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	if input.CIDR == nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "cidr is required", nil)
		return
	}
	prefix := applyAddressPrefixInput(AddressPrefix{ID: uuid.New().String(), TenantID: auth.TenantID, Labels: map[string]string{}, Source: "manual"}, input)
	saved, err := api.repo.UpsertAddressPrefix(r.Context(), prefix)
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(saved.RowVersion))
	WriteAPIJSON(w, http.StatusCreated, saved)
}

func (api addressSetAPI) getPrefix(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	prefix, err := api.repo.GetAddressPrefix(r.Context(), auth.TenantID, r.PathValue("prefix_id"))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(prefix.RowVersion))
	WriteAPIJSON(w, http.StatusOK, prefix)
}

func (api addressSetAPI) updatePrefix(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	prefix, err := api.repo.GetAddressPrefix(r.Context(), auth.TenantID, r.PathValue("prefix_id"))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	var input addressPrefixInput
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	updated, err := api.repo.UpdateAddressPrefix(r.Context(), applyAddressPrefixInput(prefix, input), expected)
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(updated.RowVersion))
	WriteAPIJSON(w, http.StatusOK, updated)
}

func applyAddressPrefixInput(prefix AddressPrefix, input addressPrefixInput) AddressPrefix {
	if input.CIDR != nil {
		prefix.CIDR = *input.CIDR
	}
	if input.Labels != nil {
		prefix.Labels = *input.Labels
	}
	if input.GeoLeafID != nil {
		prefix.GeoLeafID = *input.GeoLeafID
	}
	if input.OperatorID != nil {
		prefix.OperatorID = *input.OperatorID
	}
	if input.ASN != nil {
		if *input.ASN == 0 {
			prefix.ASN = nil
		} else {
			value := *input.ASN
			prefix.ASN = &value
		}
	}
	if input.Source != nil {
		prefix.Source = *input.Source
	}
	return prefix
}

func (api addressSetAPI) deletePrefix(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	if err := api.repo.DeleteAddressPrefixVersion(r.Context(), auth.TenantID, r.PathValue("prefix_id"), expected); err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (api addressSetAPI) listSets(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	query := r.URL.Query()
	for key := range query {
		switch key {
		case "q", "match_direction", "enabled", "sort", "order", "limit", "offset", "cursor":
		default:
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "unsupported query parameter: "+key, nil)
			return
		}
	}
	filter := AddressSetListFilter{
		Search: strings.TrimSpace(query.Get("q")), MatchDirection: strings.ToLower(strings.TrimSpace(query.Get("match_direction"))),
		Cursor: strings.TrimSpace(query.Get("cursor")), Sort: strings.TrimSpace(query.Get("sort")),
	}
	if len(filter.Search) > 255 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "q must be at most 255 characters", nil)
		return
	}
	if filter.MatchDirection != "" && filter.MatchDirection != "in" && filter.MatchDirection != "out" && filter.MatchDirection != "both" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "match_direction must be in, out, or both", nil)
		return
	}
	filter.TableMode = query.Has("sort") || query.Has("order") || query.Has("offset")
	if filter.Cursor != "" && filter.TableMode {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "cursor cannot be combined with table query parameters", nil)
		return
	}
	if _, ok := addressSetSortColumns[filter.Sort]; !ok {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid address set sort", nil)
		return
	}
	order := strings.ToLower(strings.TrimSpace(query.Get("order")))
	if order != "" && order != "asc" && order != "desc" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "order must be asc or desc", nil)
		return
	}
	filter.Desc = order == "desc"
	if raw := strings.TrimSpace(query.Get("enabled")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "enabled must be true or false", nil)
			return
		}
		filter.Enabled = &enabled
	}
	var err error
	filter.Limit, err = parseAgentPageInteger(query.Get("limit"), 100, 1, 500)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be between 1 and 500", nil)
		return
	}
	filter.Offset, err = parseAgentPageInteger(query.Get("offset"), 0, 0, int(^uint(0)>>1))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "offset must be zero or greater", nil)
		return
	}
	sets, cursor, total, err := api.repo.ListAddressSetsPage(r.Context(), auth.TenantID, filter)
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	if sets == nil {
		sets = []AddressSet{}
	}
	response := map[string]any{"items": sets, "total": total}
	if filter.TableMode {
		response["limit"] = filter.Limit
		response["offset"] = filter.Offset
	}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api addressSetAPI) getSet(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	set, err := api.repo.GetAddressSet(r.Context(), auth.TenantID, r.PathValue("set_id"))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(set.RowVersion))
	WriteAPIJSON(w, http.StatusOK, set)
}

type addressSetInput struct {
	Name                   *string         `json:"name,omitempty"`
	Description            *string         `json:"description,omitempty"`
	Selector               *map[string]any `json:"selector,omitempty"`
	ExplicitMembers        *[]string       `json:"explicit_members,omitempty"`
	ExplicitExcludeMembers *[]string       `json:"explicit_exclude_members,omitempty"`
	IncludeSetIDs          *[]string       `json:"include_set_ids,omitempty"`
	ExcludeSetIDs          *[]string       `json:"exclude_set_ids,omitempty"`
	MatchDirection         *string         `json:"match_direction,omitempty"`
	Enabled                *bool           `json:"enabled,omitempty"`
}

func (api addressSetAPI) createSet(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var input addressSetInput
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	if input.Name == nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "name is required", nil)
		return
	}
	set := applyAddressSetInput(AddressSet{
		ID: uuid.New().String(), TenantID: auth.TenantID, Selector: map[string]any{}, MatchDirection: "both", Enabled: true,
	}, input)
	saved, err := api.repo.UpsertAddressSet(r.Context(), set)
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(saved.RowVersion))
	WriteAPIJSON(w, http.StatusCreated, saved)
}

func (api addressSetAPI) updateSet(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	set, err := api.repo.GetAddressSet(r.Context(), auth.TenantID, r.PathValue("set_id"))
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	var input addressSetInput
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	saved, err := api.repo.UpdateAddressSet(r.Context(), applyAddressSetInput(set, input), expected)
	if err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(saved.RowVersion))
	WriteAPIJSON(w, http.StatusOK, saved)
}

func applyAddressSetInput(set AddressSet, input addressSetInput) AddressSet {
	if input.Name != nil {
		set.Name = *input.Name
	}
	if input.Description != nil {
		set.Description = *input.Description
	}
	if input.Selector != nil {
		set.Selector = *input.Selector
	}
	if input.ExplicitMembers != nil {
		set.ExplicitMembers = append([]string(nil), (*input.ExplicitMembers)...)
	}
	if input.ExplicitExcludeMembers != nil {
		set.ExplicitExcludeMembers = append([]string(nil), (*input.ExplicitExcludeMembers)...)
	}
	if input.IncludeSetIDs != nil {
		set.IncludeSetIDs = append([]string(nil), (*input.IncludeSetIDs)...)
	}
	if input.ExcludeSetIDs != nil {
		set.ExcludeSetIDs = append([]string(nil), (*input.ExcludeSetIDs)...)
	}
	if input.MatchDirection != nil {
		set.MatchDirection = *input.MatchDirection
	}
	if input.Enabled != nil {
		set.Enabled = *input.Enabled
	}
	return set
}

func (api addressSetAPI) deleteSet(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	expected, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	if err := api.repo.DeleteAddressSetVersion(r.Context(), auth.TenantID, r.PathValue("set_id"), expected); err != nil {
		writeAddressTaxonomyError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
