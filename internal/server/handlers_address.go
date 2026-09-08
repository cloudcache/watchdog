package server

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

// registerAddressRoutes wires the KISS-05 address library CRUD/list under the
// authenticated group. Reads need address.view; writes need address.manage.
// Paths match what the existing UI calls (/geo/dictionary, /network/operators,
// /address-prefixes, /address-sets). Import (Phase 2) and publish (Phase 3) follow.
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
	prefixes.GET("/:id", view, s.getAddressPrefix)
	prefixes.PATCH("/:id", manage, s.updateAddressPrefix)
	prefixes.DELETE("/:id", manage, s.deleteAddressPrefix)

	sets := auth.Group("/address-sets")
	sets.GET("", view, s.listAddressSets)
	sets.POST("", manage, s.createAddressSet)
	sets.GET("/:id", view, s.getAddressSet)
	sets.PATCH("/:id", manage, s.updateAddressSet)
	sets.DELETE("/:id", manage, s.deleteAddressSet)
}

// rawOrNull returns a JSON column value or the given default when empty/null.
func rawOr(v json.RawMessage, def string) json.RawMessage {
	if len(v) == 0 || string(v) == "null" {
		return json.RawMessage(def)
	}
	return v
}

func (s *Server) deleteByID(c *gin.Context, table, resource string) {
	res, err := s.db.ExecContext(c.Request.Context(), "DELETE FROM "+table+" WHERE id = ?", c.Param("id"))
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		fail(c, http.StatusNotFound, "not_found", resource+" not found")
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, resource+".delete", resource, c.Param("id"))
	c.Status(http.StatusNoContent)
}

// -------------------- geo_dict (geographies) --------------------

func (s *Server) listGeographies(c *gin.Context) {
	rows, err := s.db.QueryContext(c.Request.Context(), `
		SELECT id, kind, code, COALESCE(parent_id,''), name, COALESCE(short_name,''), sort_order, enabled, row_version
		FROM geo_dict ORDER BY kind, sort_order, code`)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var id, kind, code, parent, name, short string
		var sortOrder int
		var enabled bool
		var rv uint64
		if err := rows.Scan(&id, &kind, &code, &parent, &name, &short, &sortOrder, &enabled, &rv); err != nil {
			fail(c, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		items = append(items, gin.H{"id": id, "kind": kind, "code": code, "parent_id": parent,
			"name": name, "short_name": short, "sort_order": sortOrder, "enabled": enabled, "row_version": rv})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) createGeography(c *gin.Context) {
	var req struct {
		Kind, Code, Name, ShortName, ParentID string
		SortOrder                             int
		Enabled                               *bool
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Kind == "" || req.Code == "" || req.Name == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "kind, code and name are required")
		return
	}
	id := newID()
	if _, err := s.db.ExecContext(c.Request.Context(), `
		INSERT INTO geo_dict (id, kind, code, parent_id, name, short_name, sort_order, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id, req.Kind, req.Code, nullIfEmpty(req.ParentID), req.Name, nullIfEmpty(req.ShortName), req.SortOrder, boolOrTrue(req.Enabled)); err != nil {
		fail(c, http.StatusConflict, "conflict", "a geography with this kind+code already exists")
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "geography.create", "geography", id)
	c.JSON(http.StatusCreated, gin.H{"id": id})
}

func (s *Server) getGeography(c *gin.Context) {
	var kind, code, parent, name, short string
	var sortOrder int
	var enabled bool
	var rv uint64
	err := s.db.QueryRowContext(c.Request.Context(), `
		SELECT kind, code, COALESCE(parent_id,''), name, COALESCE(short_name,''), sort_order, enabled, row_version
		FROM geo_dict WHERE id = ?`, c.Param("id")).Scan(&kind, &code, &parent, &name, &short, &sortOrder, &enabled, &rv)
	if err == sql.ErrNoRows {
		fail(c, http.StatusNotFound, "not_found", "geography not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": c.Param("id"), "kind": kind, "code": code, "parent_id": parent,
		"name": name, "short_name": short, "sort_order": sortOrder, "enabled": enabled, "row_version": rv})
}

func (s *Server) updateGeography(c *gin.Context) {
	var req struct {
		Name, ShortName *string
		SortOrder       *int
		Enabled         *bool
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	ctx := c.Request.Context()
	id := c.Param("id")
	if req.Name != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE geo_dict SET name=?, row_version=row_version+1 WHERE id=?`, *req.Name, id)
	}
	if req.ShortName != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE geo_dict SET short_name=?, row_version=row_version+1 WHERE id=?`, nullIfEmpty(*req.ShortName), id)
	}
	if req.SortOrder != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE geo_dict SET sort_order=?, row_version=row_version+1 WHERE id=?`, *req.SortOrder, id)
	}
	if req.Enabled != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE geo_dict SET enabled=?, row_version=row_version+1 WHERE id=?`, *req.Enabled, id)
	}
	s.audit(ctx, currentPrincipal(c).UserID, "geography.update", "geography", id)
	s.getGeography(c)
}

func (s *Server) deleteGeography(c *gin.Context) { s.deleteByID(c, "geo_dict", "geography") }

// -------------------- isp_operators (operators) --------------------

func (s *Server) listOperators(c *gin.Context) {
	rows, err := s.db.QueryContext(c.Request.Context(), `
		SELECT id, code, name, COALESCE(short_name,''), category, flow_isp_id, asns, sort_order, enabled, row_version
		FROM isp_operators ORDER BY sort_order, name`)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var id, code, name, short, category string
		var flowISPID, sortOrder int
		var enabled bool
		var rv uint64
		var asns json.RawMessage
		if err := rows.Scan(&id, &code, &name, &short, &category, &flowISPID, &asns, &sortOrder, &enabled, &rv); err != nil {
			fail(c, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		items = append(items, gin.H{"id": id, "code": code, "name": name, "short_name": short,
			"category": category, "flow_isp_id": flowISPID, "asns": rawOr(asns, "[]"),
			"sort_order": sortOrder, "enabled": enabled, "row_version": rv})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) createOperator(c *gin.Context) {
	var req struct {
		Code, Name, ShortName, Category string
		FlowISPID                       int             `json:"flow_isp_id"`
		ASNs                            json.RawMessage `json:"asns"`
		SortOrder                       int             `json:"sort_order"`
		Enabled                         *bool
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Code == "" || req.Name == "" || req.FlowISPID <= 0 {
		fail(c, http.StatusBadRequest, "invalid_request", "code, name and flow_isp_id are required")
		return
	}
	if req.Category == "" {
		req.Category = "other"
	}
	id := newID()
	if _, err := s.db.ExecContext(c.Request.Context(), `
		INSERT INTO isp_operators (id, code, name, short_name, category, flow_isp_id, asns, sort_order, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, req.Code, req.Name, nullIfEmpty(req.ShortName), req.Category, req.FlowISPID,
		string(rawOr(req.ASNs, "[]")), req.SortOrder, boolOrTrue(req.Enabled)); err != nil {
		fail(c, http.StatusConflict, "conflict", "operator code or flow_isp_id already exists")
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "operator.create", "operator", id)
	c.JSON(http.StatusCreated, gin.H{"id": id})
}

func (s *Server) getOperator(c *gin.Context) {
	var code, name, short, category string
	var flowISPID, sortOrder int
	var enabled bool
	var rv uint64
	var asns json.RawMessage
	err := s.db.QueryRowContext(c.Request.Context(), `
		SELECT code, name, COALESCE(short_name,''), category, flow_isp_id, asns, sort_order, enabled, row_version
		FROM isp_operators WHERE id = ?`, c.Param("id")).
		Scan(&code, &name, &short, &category, &flowISPID, &asns, &sortOrder, &enabled, &rv)
	if err == sql.ErrNoRows {
		fail(c, http.StatusNotFound, "not_found", "operator not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": c.Param("id"), "code": code, "name": name, "short_name": short,
		"category": category, "flow_isp_id": flowISPID, "asns": rawOr(asns, "[]"),
		"sort_order": sortOrder, "enabled": enabled, "row_version": rv})
}

func (s *Server) updateOperator(c *gin.Context) {
	var req struct {
		Name, ShortName, Category *string
		ASNs                      json.RawMessage `json:"asns"`
		SortOrder                 *int            `json:"sort_order"`
		Enabled                   *bool
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	ctx := c.Request.Context()
	id := c.Param("id")
	if req.Name != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE isp_operators SET name=?, row_version=row_version+1 WHERE id=?`, *req.Name, id)
	}
	if req.ShortName != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE isp_operators SET short_name=?, row_version=row_version+1 WHERE id=?`, nullIfEmpty(*req.ShortName), id)
	}
	if req.Category != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE isp_operators SET category=?, row_version=row_version+1 WHERE id=?`, *req.Category, id)
	}
	if len(req.ASNs) > 0 {
		_, _ = s.db.ExecContext(ctx, `UPDATE isp_operators SET asns=?, row_version=row_version+1 WHERE id=?`, string(req.ASNs), id)
	}
	if req.SortOrder != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE isp_operators SET sort_order=?, row_version=row_version+1 WHERE id=?`, *req.SortOrder, id)
	}
	if req.Enabled != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE isp_operators SET enabled=?, row_version=row_version+1 WHERE id=?`, *req.Enabled, id)
	}
	s.audit(ctx, currentPrincipal(c).UserID, "operator.update", "operator", id)
	s.getOperator(c)
}

func (s *Server) deleteOperator(c *gin.Context) { s.deleteByID(c, "isp_operators", "operator") }

// -------------------- address_prefixes --------------------

func (s *Server) listAddressPrefixes(c *gin.Context) {
	rows, err := s.db.QueryContext(c.Request.Context(), `
		SELECT id, cidr, COALESCE(family,0), COALESCE(prefix_length,0), labels,
		       COALESCE(geo_leaf_id,''), COALESCE(operator_id,''), COALESCE(asn,0), source, row_version
		FROM address_prefixes ORDER BY cidr LIMIT 1000`)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var id, cidr, geo, op, source string
		var family, prefixLen int
		var asn uint64
		var rv uint64
		var labels json.RawMessage
		if err := rows.Scan(&id, &cidr, &family, &prefixLen, &labels, &geo, &op, &asn, &source, &rv); err != nil {
			fail(c, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		items = append(items, gin.H{"id": id, "cidr": cidr, "family": family, "prefix_length": prefixLen,
			"labels": rawOr(labels, "{}"), "geo_leaf_id": geo, "operator_id": op, "asn": asn,
			"source": source, "row_version": rv})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) createAddressPrefix(c *gin.Context) {
	var req struct {
		CIDR       string          `json:"cidr"`
		Labels     json.RawMessage `json:"labels"`
		GeoLeafID  string          `json:"geo_leaf_id"`
		OperatorID string          `json:"operator_id"`
		ASN        *uint64         `json:"asn"`
		Source     string          `json:"source"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.CIDR == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "cidr is required")
		return
	}
	if req.Source == "" {
		req.Source = "manual"
	}
	id := newID()
	if _, err := s.db.ExecContext(c.Request.Context(), `
		INSERT INTO address_prefixes (id, cidr, labels, geo_leaf_id, operator_id, asn, source)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, req.CIDR, string(rawOr(req.Labels, "{}")), nullIfEmpty(req.GeoLeafID), nullIfEmpty(req.OperatorID), req.ASN, req.Source); err != nil {
		fail(c, http.StatusConflict, "conflict", "a prefix with this cidr already exists")
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "prefix.create", "prefix", id)
	c.JSON(http.StatusCreated, gin.H{"id": id})
}

func (s *Server) getAddressPrefix(c *gin.Context) {
	var cidr, geo, op, source string
	var family, prefixLen int
	var asn, rv uint64
	var labels json.RawMessage
	err := s.db.QueryRowContext(c.Request.Context(), `
		SELECT cidr, COALESCE(family,0), COALESCE(prefix_length,0), labels,
		       COALESCE(geo_leaf_id,''), COALESCE(operator_id,''), COALESCE(asn,0), source, row_version
		FROM address_prefixes WHERE id = ?`, c.Param("id")).
		Scan(&cidr, &family, &prefixLen, &labels, &geo, &op, &asn, &source, &rv)
	if err == sql.ErrNoRows {
		fail(c, http.StatusNotFound, "not_found", "prefix not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": c.Param("id"), "cidr": cidr, "family": family, "prefix_length": prefixLen,
		"labels": rawOr(labels, "{}"), "geo_leaf_id": geo, "operator_id": op, "asn": asn, "source": source, "row_version": rv})
}

func (s *Server) updateAddressPrefix(c *gin.Context) {
	var req struct {
		Labels     json.RawMessage `json:"labels"`
		GeoLeafID  *string         `json:"geo_leaf_id"`
		OperatorID *string         `json:"operator_id"`
		ASN        *uint64         `json:"asn"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	ctx := c.Request.Context()
	id := c.Param("id")
	if len(req.Labels) > 0 {
		_, _ = s.db.ExecContext(ctx, `UPDATE address_prefixes SET labels=?, row_version=row_version+1 WHERE id=?`, string(req.Labels), id)
	}
	if req.GeoLeafID != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE address_prefixes SET geo_leaf_id=?, row_version=row_version+1 WHERE id=?`, nullIfEmpty(*req.GeoLeafID), id)
	}
	if req.OperatorID != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE address_prefixes SET operator_id=?, row_version=row_version+1 WHERE id=?`, nullIfEmpty(*req.OperatorID), id)
	}
	if req.ASN != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE address_prefixes SET asn=?, row_version=row_version+1 WHERE id=?`, *req.ASN, id)
	}
	s.audit(ctx, currentPrincipal(c).UserID, "prefix.update", "prefix", id)
	s.getAddressPrefix(c)
}

func (s *Server) deleteAddressPrefix(c *gin.Context) { s.deleteByID(c, "address_prefixes", "prefix") }

// -------------------- address_sets --------------------

func (s *Server) listAddressSets(c *gin.Context) {
	rows, err := s.db.QueryContext(c.Request.Context(), `
		SELECT id, name, COALESCE(description,''), selector, explicit_members, explicit_exclude_members,
		       include_set_ids, exclude_set_ids, match_direction, enabled, row_version
		FROM address_sets ORDER BY name`)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var id, name, desc, dir string
		var enabled bool
		var rv uint64
		var selector, members, exclude, incl, excl json.RawMessage
		if err := rows.Scan(&id, &name, &desc, &selector, &members, &exclude, &incl, &excl, &dir, &enabled, &rv); err != nil {
			fail(c, http.StatusInternalServerError, "internal", err.Error())
			return
		}
		items = append(items, gin.H{"id": id, "name": name, "description": desc,
			"selector": rawOr(selector, "{}"), "explicit_members": rawOr(members, "[]"),
			"explicit_exclude_members": rawOr(exclude, "[]"), "include_set_ids": rawOr(incl, "[]"),
			"exclude_set_ids": rawOr(excl, "[]"), "match_direction": dir, "enabled": enabled, "row_version": rv})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) createAddressSet(c *gin.Context) {
	var req struct {
		Name                   string          `json:"name"`
		Description            string          `json:"description"`
		Selector               json.RawMessage `json:"selector"`
		ExplicitMembers        json.RawMessage `json:"explicit_members"`
		ExplicitExcludeMembers json.RawMessage `json:"explicit_exclude_members"`
		IncludeSetIDs          json.RawMessage `json:"include_set_ids"`
		ExcludeSetIDs          json.RawMessage `json:"exclude_set_ids"`
		MatchDirection         string          `json:"match_direction"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}
	if req.MatchDirection == "" {
		req.MatchDirection = "both"
	}
	id := newID()
	if _, err := s.db.ExecContext(c.Request.Context(), `
		INSERT INTO address_sets (id, name, description, selector, explicit_members, explicit_exclude_members,
		                          include_set_ids, exclude_set_ids, match_direction)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, req.Name, nullIfEmpty(req.Description), string(rawOr(req.Selector, "{}")),
		string(rawOr(req.ExplicitMembers, "[]")), string(rawOr(req.ExplicitExcludeMembers, "[]")),
		string(rawOr(req.IncludeSetIDs, "[]")), string(rawOr(req.ExcludeSetIDs, "[]")), req.MatchDirection); err != nil {
		fail(c, http.StatusConflict, "conflict", "an address set with this name already exists")
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "address_set.create", "address_set", id)
	c.JSON(http.StatusCreated, gin.H{"id": id})
}

func (s *Server) getAddressSet(c *gin.Context) {
	var name, desc, dir string
	var enabled bool
	var rv uint64
	var selector, members, exclude, incl, excl json.RawMessage
	err := s.db.QueryRowContext(c.Request.Context(), `
		SELECT name, COALESCE(description,''), selector, explicit_members, explicit_exclude_members,
		       include_set_ids, exclude_set_ids, match_direction, enabled, row_version
		FROM address_sets WHERE id = ?`, c.Param("id")).
		Scan(&name, &desc, &selector, &members, &exclude, &incl, &excl, &dir, &enabled, &rv)
	if err == sql.ErrNoRows {
		fail(c, http.StatusNotFound, "not_found", "address set not found")
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": c.Param("id"), "name": name, "description": desc,
		"selector": rawOr(selector, "{}"), "explicit_members": rawOr(members, "[]"),
		"explicit_exclude_members": rawOr(exclude, "[]"), "include_set_ids": rawOr(incl, "[]"),
		"exclude_set_ids": rawOr(excl, "[]"), "match_direction": dir, "enabled": enabled, "row_version": rv})
}

func (s *Server) updateAddressSet(c *gin.Context) {
	var req struct {
		Name            *string         `json:"name"`
		Description     *string         `json:"description"`
		Selector        json.RawMessage `json:"selector"`
		ExplicitMembers json.RawMessage `json:"explicit_members"`
		MatchDirection  *string         `json:"match_direction"`
		Enabled         *bool           `json:"enabled"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	ctx := c.Request.Context()
	id := c.Param("id")
	if req.Name != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE address_sets SET name=?, row_version=row_version+1 WHERE id=?`, *req.Name, id)
	}
	if req.Description != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE address_sets SET description=?, row_version=row_version+1 WHERE id=?`, nullIfEmpty(*req.Description), id)
	}
	if len(req.Selector) > 0 {
		_, _ = s.db.ExecContext(ctx, `UPDATE address_sets SET selector=?, row_version=row_version+1 WHERE id=?`, string(req.Selector), id)
	}
	if len(req.ExplicitMembers) > 0 {
		_, _ = s.db.ExecContext(ctx, `UPDATE address_sets SET explicit_members=?, row_version=row_version+1 WHERE id=?`, string(req.ExplicitMembers), id)
	}
	if req.MatchDirection != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE address_sets SET match_direction=?, row_version=row_version+1 WHERE id=?`, *req.MatchDirection, id)
	}
	if req.Enabled != nil {
		_, _ = s.db.ExecContext(ctx, `UPDATE address_sets SET enabled=?, row_version=row_version+1 WHERE id=?`, *req.Enabled, id)
	}
	s.audit(ctx, currentPrincipal(c).UserID, "address_set.update", "address_set", id)
	s.getAddressSet(c)
}

func (s *Server) deleteAddressSet(c *gin.Context) { s.deleteByID(c, "address_sets", "address_set") }

func boolOrTrue(b *bool) bool {
	if b == nil {
		return true
	}
	return *b
}

// nullIfEmpty maps an empty string to SQL NULL so nullable FK columns stay clean.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
