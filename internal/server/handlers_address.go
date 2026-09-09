package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
)

// registerAddressRoutes wires the KISS-05 editable address library CRUD/list over
// internal/address.Store (the single-domain de-tenanted engine) — converged from the
// earlier raw-JSON server handlers so import/publish and editable CRUD share one
// implementation. Reads need address.view; writes need address.manage. Contracts
// (strict params, cursor/table pagination, If-Match/428/ETag, merge/set previews)
// are faithful ports of internal/watchdog/api_address_sets.go + api_address_taxonomy.go.
func (s *Server) registerAddressRoutes(auth *gin.RouterGroup) {
	view := s.requirePermission("address.view")
	manage := s.requirePermission("address.manage")

	geo := auth.Group("/geo/dictionary")
	geo.GET("", view, s.listGeographies)
	geo.POST("", manage, s.createGeography)
	geo.GET("/:id", view, s.getGeography)
	geo.PATCH("/:id", manage, s.updateGeography)
	geo.DELETE("/:id", manage, s.deleteGeography)

	ops := auth.Group("/network/operators")
	ops.GET("", view, s.listOperators)
	ops.POST("", manage, s.createOperator)
	ops.GET("/:id", view, s.getOperator)
	ops.PATCH("/:id", manage, s.updateOperator)
	ops.DELETE("/:id", manage, s.deleteOperator)

	prefixes := auth.Group("/address-prefixes")
	prefixes.GET("", view, s.listAddressPrefixes)
	prefixes.POST("", manage, s.createAddressPrefix)
	prefixes.POST("/actions/merge-preview", manage, s.mergePreviewAddressPrefixes)
	prefixes.GET("/:id", view, s.getAddressPrefix)
	prefixes.PATCH("/:id", manage, s.updateAddressPrefix)
	prefixes.DELETE("/:id", manage, s.deleteAddressPrefix)

	sets := auth.Group("/address-sets")
	sets.GET("", view, s.listAddressSets)
	sets.POST("", manage, s.createAddressSet)
	sets.POST("/actions/preview", view, s.previewAddressSetOperation) // set math: union/intersection/difference/cover/normalize
	sets.GET("/:id", view, s.getAddressSet)
	sets.PATCH("/:id", manage, s.updateAddressSet)
	sets.DELETE("/:id", manage, s.deleteAddressSet)

	lines := auth.Group("/geo/lines")
	lines.GET("", view, s.listGeoLines)
	lines.POST("", manage, s.createGeoLine)
	lines.GET("/:id", view, s.getGeoLine)
	lines.PATCH("/:id", manage, s.updateGeoLine)
	lines.DELETE("/:id", manage, s.deleteGeoLine)

	s.registerAddressDraftRoutes(auth)
}

// writeAddressError maps the internal/address CRUD errors to HTTP, faithfully
// mirroring internal/watchdog.writeAddressTaxonomyError.
func writeAddressError(c *gin.Context, err error) {
	var mysqlErr *mysql.MySQLError
	switch {
	case errors.Is(err, address.ErrAddressTaxonomyInvalid):
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, address.ErrAddressTaxonomyConflict):
		fail(c, http.StatusPreconditionFailed, "version_conflict", "address item changed since it was read")
	case errors.Is(err, address.ErrAddressTaxonomyCycle), errors.Is(err, address.ErrAddressTaxonomyInUse):
		fail(c, http.StatusConflict, "conflict", err.Error())
	case errors.As(err, &mysqlErr) && (mysqlErr.Number == 1062 || mysqlErr.Number == 1451):
		fail(c, http.StatusConflict, "conflict", "address item conflicts with an existing or referenced item")
	default:
		writeSQLError(c, err)
	}
}

// requireAddressIfMatch enforces the write-time optimistic-concurrency contract:
// a missing If-Match is 428 (Precondition Required); an unparseable one is 412.
func requireAddressIfMatch(c *gin.Context) (uint64, bool) {
	expected, supplied, err := ifMatch(c)
	if !supplied {
		fail(c, http.StatusPreconditionRequired, "precondition_required", "If-Match is required")
		return 0, false
	}
	if err != nil {
		fail(c, http.StatusPreconditionFailed, "version_conflict", "If-Match must be a row version")
		return 0, false
	}
	return expected, true
}

// addressDecodeStrict decodes exactly one JSON value with unknown-field rejection.
func addressDecodeStrict(c *gin.Context, dest any, maxBytes int64) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dest); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		fail(c, http.StatusBadRequest, "invalid_request", "request body must contain exactly one JSON value")
		return false
	}
	return true
}

func addressListParam(c *gin.Context, allowed ...string) (map[string]bool, bool) {
	permitted := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		permitted[key] = true
	}
	for key := range c.Request.URL.Query() {
		if !permitted[key] {
			fail(c, http.StatusBadRequest, "invalid_request", "unsupported query parameter: "+key)
			return nil, false
		}
	}
	return permitted, true
}

func addressListPageParams(c *gin.Context) (limit, offset int, tableMode bool, cursor string, ok bool) {
	cursor = strings.TrimSpace(c.Query("cursor"))
	tableMode = c.Query("sort") != "" || c.Query("order") != "" || c.Query("offset") != ""
	if cursor != "" && tableMode {
		fail(c, http.StatusBadRequest, "invalid_request", "cursor cannot be combined with table query parameters")
		return 0, 0, false, "", false
	}
	order := strings.ToLower(strings.TrimSpace(c.Query("order")))
	if order != "" && order != "asc" && order != "desc" {
		fail(c, http.StatusBadRequest, "invalid_request", "order must be asc or desc")
		return 0, 0, false, "", false
	}
	limit, offset = pageParams(c)
	return limit, offset, tableMode, cursor, true
}

func addressListResponse[T any](c *gin.Context, items []T, cursor string, total int, tableMode bool, limit, offset int) {
	if items == nil {
		items = []T{}
	}
	response := gin.H{"items": items, "total": total}
	if tableMode {
		response["limit"] = limit
		response["offset"] = offset
	}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	c.JSON(http.StatusOK, response)
}

// -------------------- address_prefixes --------------------

func (s *Server) listAddressPrefixes(c *gin.Context) {
	if _, ok := addressListParam(c, "q", "family", "source", "geo_leaf_id", "operator_id", "asn", "sort", "order", "limit", "offset", "cursor"); !ok {
		return
	}
	limit, offset, tableMode, cursor, ok := addressListPageParams(c)
	if !ok {
		return
	}
	filter := address.AddressPrefixListFilter{
		Search: strings.TrimSpace(c.Query("q")), Source: strings.TrimSpace(c.Query("source")),
		GeoLeafID: strings.TrimSpace(c.Query("geo_leaf_id")), OperatorID: strings.TrimSpace(c.Query("operator_id")),
		Cursor: cursor, Sort: strings.TrimSpace(c.Query("sort")), Desc: sortDirection(c) == "DESC" && c.Query("order") != "",
		Limit: limit, Offset: offset, TableMode: tableMode,
	}
	if raw := strings.TrimSpace(c.Query("family")); raw != "" {
		family, err := strconv.ParseUint(raw, 10, 8)
		if err != nil || (family != 4 && family != 6) {
			fail(c, http.StatusBadRequest, "invalid_request", "family must be 4 or 6")
			return
		}
		filter.Family = uint8(family)
	}
	if raw := strings.TrimSpace(c.Query("asn")); raw != "" {
		asn, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "asn must be an unsigned 32-bit integer")
			return
		}
		value := uint32(asn)
		filter.ASN = &value
	}
	items, next, total, err := s.addressStore.ListAddressPrefixesPage(c.Request.Context(), filter)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	addressListResponse(c, items, next, total, tableMode, limit, offset)
}

type addressPrefixInput struct {
	CIDR       *string            `json:"cidr,omitempty"`
	Labels     *map[string]string `json:"labels,omitempty"`
	GeoLeafID  *string            `json:"geo_leaf_id,omitempty"`
	OperatorID *string            `json:"operator_id,omitempty"`
	ASN        *uint32            `json:"asn,omitempty"`
	Source     *string            `json:"source,omitempty"`
}

func applyAddressPrefixInput(prefix address.AddressPrefix, input addressPrefixInput) address.AddressPrefix {
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

func (s *Server) createAddressPrefix(c *gin.Context) {
	var input addressPrefixInput
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	if input.CIDR == nil {
		fail(c, http.StatusBadRequest, "invalid_request", "cidr is required")
		return
	}
	prefix := applyAddressPrefixInput(address.AddressPrefix{ID: address.NewSetID(), Labels: map[string]string{}, Source: "manual"}, input)
	saved, err := s.addressStore.UpsertAddressPrefix(c.Request.Context(), prefix)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "prefix.create", "prefix", saved.ID)
	c.Header("ETag", etag(saved.RowVersion))
	c.JSON(http.StatusCreated, saved)
}

func (s *Server) getAddressPrefix(c *gin.Context) {
	prefix, err := s.addressStore.GetAddressPrefix(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	c.Header("ETag", etag(prefix.RowVersion))
	c.JSON(http.StatusOK, prefix)
}

func (s *Server) updateAddressPrefix(c *gin.Context) {
	prefix, err := s.addressStore.GetAddressPrefix(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	var input addressPrefixInput
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	updated, err := s.addressStore.UpdateAddressPrefix(c.Request.Context(), applyAddressPrefixInput(prefix, input), expected)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "prefix.update", "prefix", updated.ID)
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated)
}

func (s *Server) deleteAddressPrefix(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	if err := s.addressStore.DeleteAddressPrefixVersion(c.Request.Context(), c.Param("id"), expected); err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "prefix.delete", "prefix", c.Param("id"))
	c.Status(http.StatusNoContent)
}

type addressPrefixMergeInput struct {
	CIDR       string            `json:"cidr"`
	GeoLeafID  string            `json:"geo_leaf_id"`
	OperatorID string            `json:"operator_id"`
	ASN        *uint32           `json:"asn"`
	Labels     map[string]string `json:"labels"`
	Source     string            `json:"source"`
}

func (s *Server) mergePreviewAddressPrefixes(c *gin.Context) {
	var request struct {
		Prefixes []addressPrefixMergeInput `json:"prefixes"`
	}
	if !addressDecodeStrict(c, &request, 8<<20) {
		return
	}
	if len(request.Prefixes) < 2 {
		fail(c, http.StatusBadRequest, "invalid_request", "at least two prefixes are required to merge")
		return
	}
	if len(request.Prefixes) > address.MaxAddressOperationInputs {
		fail(c, http.StatusBadRequest, "invalid_request", "too many prefixes to merge")
		return
	}
	prefixes := make([]address.AddressPrefix, 0, len(request.Prefixes))
	for _, input := range request.Prefixes {
		prefixes = append(prefixes, address.AddressPrefix{
			CIDR: input.CIDR, GeoLeafID: input.GeoLeafID, OperatorID: input.OperatorID,
			ASN: input.ASN, Labels: input.Labels, Source: input.Source,
		})
	}
	preview, err := address.PreviewAddressPrefixMerge(prefixes)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	c.JSON(http.StatusOK, preview)
}

// -------------------- address_sets --------------------

func (s *Server) listAddressSets(c *gin.Context) {
	if _, ok := addressListParam(c, "q", "match_direction", "enabled", "sort", "order", "limit", "offset", "cursor"); !ok {
		return
	}
	limit, offset, tableMode, cursor, ok := addressListPageParams(c)
	if !ok {
		return
	}
	filter := address.AddressSetListFilter{
		Search: strings.TrimSpace(c.Query("q")), MatchDirection: strings.ToLower(strings.TrimSpace(c.Query("match_direction"))),
		Cursor: cursor, Sort: strings.TrimSpace(c.Query("sort")), Desc: sortDirection(c) == "DESC" && c.Query("order") != "",
		Limit: limit, Offset: offset, TableMode: tableMode,
	}
	if filter.MatchDirection != "" && filter.MatchDirection != "in" && filter.MatchDirection != "out" && filter.MatchDirection != "both" {
		fail(c, http.StatusBadRequest, "invalid_request", "match_direction must be in, out, or both")
		return
	}
	if raw := strings.TrimSpace(c.Query("enabled")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "enabled must be true or false")
			return
		}
		filter.Enabled = &enabled
	}
	items, next, total, err := s.addressStore.ListAddressSetsPage(c.Request.Context(), filter)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	addressListResponse(c, items, next, total, tableMode, limit, offset)
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

func applyAddressSetInput(set address.AddressSet, input addressSetInput) address.AddressSet {
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

func (s *Server) createAddressSet(c *gin.Context) {
	var input addressSetInput
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	if input.Name == nil {
		fail(c, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}
	set := applyAddressSetInput(address.AddressSet{ID: address.NewSetID(), Selector: map[string]any{}, MatchDirection: "both", Enabled: true}, input)
	saved, err := s.addressStore.UpsertAddressSet(c.Request.Context(), set)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "address_set.create", "address_set", saved.ID)
	c.Header("ETag", etag(saved.RowVersion))
	c.JSON(http.StatusCreated, saved)
}

func (s *Server) getAddressSet(c *gin.Context) {
	set, err := s.addressStore.GetAddressSet(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	c.Header("ETag", etag(set.RowVersion))
	c.JSON(http.StatusOK, set)
}

func (s *Server) updateAddressSet(c *gin.Context) {
	set, err := s.addressStore.GetAddressSet(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	var input addressSetInput
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	saved, err := s.addressStore.UpdateAddressSet(c.Request.Context(), applyAddressSetInput(set, input), expected)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "address_set.update", "address_set", saved.ID)
	c.Header("ETag", etag(saved.RowVersion))
	c.JSON(http.StatusOK, saved)
}

func (s *Server) deleteAddressSet(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	if err := s.addressStore.DeleteAddressSetVersion(c.Request.Context(), c.Param("id"), expected); err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "address_set.delete", "address_set", c.Param("id"))
	c.Status(http.StatusNoContent)
}

// previewAddressSetOperation computes union/intersection/difference/cover/normalize
// over CIDR lists, reusing the internal/address set-math engine (no DB).
func (s *Server) previewAddressSetOperation(c *gin.Context) {
	var req address.AddressSetOperationRequest
	if !addressDecodeStrict(c, &req, 4<<20) {
		return
	}
	preview, err := address.PreviewAddressSetOperation(req)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	c.JSON(http.StatusOK, preview)
}

// -------------------- geo_dict (geographies) --------------------

func (s *Server) parseTaxonomyFilter(c *gin.Context, allowed ...string) (address.AddressTaxonomyListFilter, bool) {
	base := append([]string{"q", "enabled", "sort", "order", "limit", "offset", "cursor"}, allowed...)
	if _, ok := addressListParam(c, base...); !ok {
		return address.AddressTaxonomyListFilter{}, false
	}
	limit, offset, tableMode, cursor, ok := addressListPageParams(c)
	if !ok {
		return address.AddressTaxonomyListFilter{}, false
	}
	filter := address.AddressTaxonomyListFilter{
		Search: strings.TrimSpace(c.Query("q")), Kind: strings.TrimSpace(c.Query("kind")),
		ParentID: strings.TrimSpace(c.Query("parent_id")), Cursor: cursor, Sort: strings.TrimSpace(c.Query("sort")),
		Desc: sortDirection(c) == "DESC" && c.Query("order") != "", Limit: limit, Offset: offset, TableMode: tableMode,
	}
	if raw := strings.TrimSpace(c.Query("enabled")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "enabled must be true or false")
			return address.AddressTaxonomyListFilter{}, false
		}
		filter.Enabled = &enabled
	}
	return filter, true
}

func (s *Server) listGeographies(c *gin.Context) {
	filter, ok := s.parseTaxonomyFilter(c, "kind", "parent_id")
	if !ok {
		return
	}
	items, next, total, err := s.addressStore.ListGeoDictionary(c.Request.Context(), filter)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	addressListResponse(c, items, next, total, filter.TableMode, filter.Limit, filter.Offset)
}

type geoDictionaryInput struct {
	Kind      *string `json:"kind,omitempty"`
	Code      *string `json:"code,omitempty"`
	ParentID  *string `json:"parent_id,omitempty"`
	Name      *string `json:"name,omitempty"`
	ShortName *string `json:"short_name,omitempty"`
	SortOrder *int    `json:"sort_order,omitempty"`
	Enabled   *bool   `json:"enabled,omitempty"`
}

func applyGeoDictionaryInput(node address.GeoDictionaryNode, input geoDictionaryInput) address.GeoDictionaryNode {
	if input.Kind != nil {
		node.Kind = *input.Kind
	}
	if input.Code != nil {
		node.Code = *input.Code
	}
	if input.ParentID != nil {
		node.ParentID = *input.ParentID
	}
	if input.Name != nil {
		node.Name = *input.Name
	}
	if input.ShortName != nil {
		node.ShortName = *input.ShortName
	}
	if input.SortOrder != nil {
		node.SortOrder = *input.SortOrder
	}
	if input.Enabled != nil {
		node.Enabled = *input.Enabled
	}
	return node
}

func (s *Server) createGeography(c *gin.Context) {
	var input geoDictionaryInput
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	if input.Kind == nil || input.Code == nil || input.Name == nil {
		fail(c, http.StatusBadRequest, "invalid_request", "kind, code and name are required")
		return
	}
	saved, err := s.addressStore.CreateGeoDictionary(c.Request.Context(), applyGeoDictionaryInput(address.GeoDictionaryNode{Enabled: true}, input))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "geography.create", "geography", saved.ID)
	c.Header("ETag", etag(saved.RowVersion))
	c.JSON(http.StatusCreated, saved)
}

func (s *Server) getGeography(c *gin.Context) {
	node, err := s.addressStore.GetGeoDictionary(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	c.Header("ETag", etag(node.RowVersion))
	c.JSON(http.StatusOK, node)
}

func (s *Server) updateGeography(c *gin.Context) {
	node, err := s.addressStore.GetGeoDictionary(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	var input geoDictionaryInput
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	saved, err := s.addressStore.UpdateGeoDictionary(c.Request.Context(), applyGeoDictionaryInput(node, input), expected)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "geography.update", "geography", saved.ID)
	c.Header("ETag", etag(saved.RowVersion))
	c.JSON(http.StatusOK, saved)
}

func (s *Server) deleteGeography(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	if err := s.addressStore.DeleteGeoDictionary(c.Request.Context(), c.Param("id"), expected); err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "geography.delete", "geography", c.Param("id"))
	c.Status(http.StatusNoContent)
}

// -------------------- isp_operators (operators) --------------------

func (s *Server) listOperators(c *gin.Context) {
	filter, ok := s.parseTaxonomyFilter(c)
	if !ok {
		return
	}
	items, next, total, err := s.addressStore.ListISPOperators(c.Request.Context(), filter)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	addressListResponse(c, items, next, total, filter.TableMode, filter.Limit, filter.Offset)
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

func applyISPOperatorInput(operator address.ISPOperator, input ispOperatorInput) address.ISPOperator {
	if input.Code != nil {
		operator.Code = *input.Code
	}
	if input.Name != nil {
		operator.Name = *input.Name
	}
	if input.ShortName != nil {
		operator.ShortName = *input.ShortName
	}
	if input.Category != nil {
		operator.Category = *input.Category
	}
	if input.ASNs != nil {
		operator.ASNs = append([]uint32(nil), (*input.ASNs)...)
	}
	if input.SortOrder != nil {
		operator.SortOrder = *input.SortOrder
	}
	if input.Enabled != nil {
		operator.Enabled = *input.Enabled
	}
	return operator
}

func (s *Server) createOperator(c *gin.Context) {
	var input ispOperatorInput
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	if input.Code == nil || input.Name == nil {
		fail(c, http.StatusBadRequest, "invalid_request", "code and name are required")
		return
	}
	saved, err := s.addressStore.CreateISPOperator(c.Request.Context(), applyISPOperatorInput(address.ISPOperator{Enabled: true}, input))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "operator.create", "operator", saved.ID)
	c.Header("ETag", etag(saved.RowVersion))
	c.JSON(http.StatusCreated, saved)
}

func (s *Server) getOperator(c *gin.Context) {
	operator, err := s.addressStore.GetISPOperator(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	c.Header("ETag", etag(operator.RowVersion))
	c.JSON(http.StatusOK, operator)
}

func (s *Server) updateOperator(c *gin.Context) {
	operator, err := s.addressStore.GetISPOperator(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	var input ispOperatorInput
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	saved, err := s.addressStore.UpdateISPOperator(c.Request.Context(), applyISPOperatorInput(operator, input), expected)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "operator.update", "operator", saved.ID)
	c.Header("ETag", etag(saved.RowVersion))
	c.JSON(http.StatusOK, saved)
}

func (s *Server) deleteOperator(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	if err := s.addressStore.DeleteISPOperator(c.Request.Context(), c.Param("id"), expected); err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "operator.delete", "operator", c.Param("id"))
	c.Status(http.StatusNoContent)
}

// -------------------- geo_lines (线路) --------------------

func (s *Server) listGeoLines(c *gin.Context) {
	filter, ok := s.parseTaxonomyFilter(c, "parent_id")
	if !ok {
		return
	}
	items, next, total, err := s.addressStore.ListGeoLines(c.Request.Context(), filter)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	addressListResponse(c, items, next, total, filter.TableMode, filter.Limit, filter.Offset)
}

type geoLineInput struct {
	ParentID     *string                  `json:"parent_id,omitempty"`
	Code         *string                  `json:"code,omitempty"`
	Name         *string                  `json:"name,omitempty"`
	Description  *string                  `json:"description,omitempty"`
	GeoSelector  *address.GeoLineSelector `json:"geo_selector,omitempty"`
	OperatorID   *string                  `json:"operator_id,omitempty"`
	AddressSetID *string                  `json:"address_set_id,omitempty"`
	SortOrder    *int                     `json:"sort_order,omitempty"`
	Enabled      *bool                    `json:"enabled,omitempty"`
}

func applyGeoLineInput(line address.GeoLine, input geoLineInput) address.GeoLine {
	if input.ParentID != nil {
		line.ParentID = *input.ParentID
	}
	if input.Code != nil {
		line.Code = *input.Code
	}
	if input.Name != nil {
		line.Name = *input.Name
	}
	if input.Description != nil {
		line.Description = *input.Description
	}
	if input.GeoSelector != nil {
		line.GeoSelector = *input.GeoSelector
	}
	if input.OperatorID != nil {
		line.OperatorID = *input.OperatorID
	}
	if input.AddressSetID != nil {
		line.AddressSetID = *input.AddressSetID
	}
	if input.SortOrder != nil {
		line.SortOrder = *input.SortOrder
	}
	if input.Enabled != nil {
		line.Enabled = *input.Enabled
	}
	return line
}

func (s *Server) createGeoLine(c *gin.Context) {
	var input geoLineInput
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	if input.Code == nil || input.Name == nil {
		fail(c, http.StatusBadRequest, "invalid_request", "code and name are required")
		return
	}
	saved, err := s.addressStore.CreateGeoLine(c.Request.Context(), applyGeoLineInput(address.GeoLine{Enabled: true}, input))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "geo_line.create", "geo_line", saved.ID)
	c.Header("ETag", etag(saved.RowVersion))
	c.JSON(http.StatusCreated, saved)
}

func (s *Server) getGeoLine(c *gin.Context) {
	line, err := s.addressStore.GetGeoLine(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	c.Header("ETag", etag(line.RowVersion))
	c.JSON(http.StatusOK, line)
}

func (s *Server) updateGeoLine(c *gin.Context) {
	line, err := s.addressStore.GetGeoLine(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressError(c, err)
		return
	}
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	var input geoLineInput
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	saved, err := s.addressStore.UpdateGeoLine(c.Request.Context(), applyGeoLineInput(line, input), expected)
	if err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "geo_line.update", "geo_line", saved.ID)
	c.Header("ETag", etag(saved.RowVersion))
	c.JSON(http.StatusOK, saved)
}

func (s *Server) deleteGeoLine(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	if err := s.addressStore.DeleteGeoLine(c.Request.Context(), c.Param("id"), expected); err != nil {
		writeAddressError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "geo_line.delete", "geo_line", c.Param("id"))
	c.Status(http.StatusNoContent)
}
