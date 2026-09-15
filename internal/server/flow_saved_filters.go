// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// Saved Flow filters are reusable canonical typed-filter ASTs (KISS-06 phase-1c,
// de-tenanted port of the hub flow_saved_filters domain). They deliberately
// exclude query time, dimensions and resource selectors: those stay explicit on
// each query and are authorized at execution time, so a saved filter can never
// widen a caller's resource scope. In a single-domain install, share_scope is
// either private or shared (visible to every user allowed to view Flow).
const (
	flowSavedFilterPrivate = "private"
	flowSavedFilterShared  = "shared"

	flowSavedFilterSchemaVersion  = uint16(1)
	flowSavedFilterNameMax        = 190
	flowSavedFilterDescriptionMax = 1024
)

var errFlowSavedFilterInvalid = errors.New("saved Flow filter is invalid")

type flowSavedFilter struct {
	ID                  string                     `json:"id"`
	OwnerUserID         string                     `json:"owner_user_id,omitempty"`
	OwnerName           string                     `json:"owner_name,omitempty"`
	Name                string                     `json:"name"`
	Description         string                     `json:"description"`
	ShareScope          string                     `json:"share_scope"`
	FilterSchemaVersion uint16                     `json:"filter_schema_version"`
	Filter              flowquery.FilterExpression `json:"filter"`
	RowVersion          uint64                     `json:"row_version"`
	CanEdit             bool                       `json:"can_edit"`
	CreatedAt           time.Time                  `json:"created_at"`
	UpdatedAt           time.Time                  `json:"updated_at"`
}

type flowSavedFilterInput struct {
	Name        string                     `json:"name"`
	Description string                     `json:"description"`
	ShareScope  string                     `json:"share_scope"`
	Filter      flowquery.FilterExpression `json:"filter"`
}

type flowSavedFilterOwnerFacet struct {
	OwnerUserID string `json:"owner_user_id"`
	OwnerName   string `json:"owner_name"`
	Count       int64  `json:"count"`
}

// normalizeFlowSavedFilter validates and canonicalizes a saved filter. It rejects
// resource fields (target/device/exporter): those must use the authorized query
// selector and cannot be baked into a stored, shareable AST.
func normalizeFlowSavedFilter(item flowSavedFilter) (flowSavedFilter, error) {
	item.Name = strings.TrimSpace(item.Name)
	item.Description = strings.TrimSpace(item.Description)
	item.ShareScope = strings.ToLower(strings.TrimSpace(item.ShareScope))
	if item.Name == "" || len(item.Name) > flowSavedFilterNameMax {
		return flowSavedFilter{}, fmt.Errorf("%w: name must contain 1..%d characters", errFlowSavedFilterInvalid, flowSavedFilterNameMax)
	}
	if len(item.Description) > flowSavedFilterDescriptionMax {
		return flowSavedFilter{}, fmt.Errorf("%w: description exceeds %d characters", errFlowSavedFilterInvalid, flowSavedFilterDescriptionMax)
	}
	if item.ShareScope == "" {
		item.ShareScope = flowSavedFilterPrivate
	}
	if item.ShareScope != flowSavedFilterPrivate && item.ShareScope != flowSavedFilterShared {
		return flowSavedFilter{}, fmt.Errorf("%w: share_scope must be private or shared", errFlowSavedFilterInvalid)
	}
	canonical, err := flowquery.CanonicalFilter(item.Filter)
	if err != nil {
		return flowSavedFilter{}, fmt.Errorf("%w: %v", errFlowSavedFilterInvalid, err)
	}
	fields, err := flowquery.FilterFields(canonical)
	if err != nil {
		return flowSavedFilter{}, fmt.Errorf("%w: %v", errFlowSavedFilterInvalid, err)
	}
	for _, field := range fields {
		switch field {
		case "target", "device", "exporter":
			return flowSavedFilter{}, fmt.Errorf("%w: resource field %q must use the authorized query selector and cannot be saved", errFlowSavedFilterInvalid, field)
		}
	}
	item.Filter = canonical
	item.FilterSchemaVersion = flowSavedFilterSchemaVersion
	return item, nil
}

// canShareFlowFilter reports whether the caller may publish an install-wide
// (shared) filter. This is the de-tenanted mapping of the hub's tenant-configure
// gate: flow management (or admin) rather than any flow viewer.
func canShareFlowFilter(p *principal) bool {
	return p != nil && (p.IsAdmin || p.can("flow.device.manage"))
}

// canEditFlowSavedFilter reports whether the caller may change or delete a saved
// filter: shared filters require the share permission; private filters are
// owner-only.
func canEditFlowSavedFilter(p *principal, item flowSavedFilter) bool {
	if item.ShareScope == flowSavedFilterShared {
		return canShareFlowFilter(p)
	}
	return p != nil && item.OwnerUserID != "" && item.OwnerUserID == p.UserID
}

const flowSavedFilterColumns = `f.id, COALESCE(f.owner_user_id, ''), COALESCE(u.username, ''),
	f.name, f.description, f.share_scope, f.filter_schema_version, f.filter_json,
	f.row_version, f.created_at, f.updated_at`

func scanFlowSavedFilter(row rowScanner) (flowSavedFilter, error) {
	var item flowSavedFilter
	var filterJSON []byte
	if err := row.Scan(
		&item.ID, &item.OwnerUserID, &item.OwnerName, &item.Name, &item.Description,
		&item.ShareScope, &item.FilterSchemaVersion, &filterJSON, &item.RowVersion, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return flowSavedFilter{}, err
	}
	if item.FilterSchemaVersion != flowSavedFilterSchemaVersion {
		return flowSavedFilter{}, errors.New("unsupported saved Flow filter schema version")
	}
	if err := json.Unmarshal(filterJSON, &item.Filter); err != nil {
		return flowSavedFilter{}, err
	}
	canonical, err := flowquery.CanonicalFilter(item.Filter)
	if err != nil {
		return flowSavedFilter{}, err
	}
	if !reflect.DeepEqual(canonical, item.Filter) {
		return flowSavedFilter{}, errors.New("saved Flow filter contains a non-canonical AST")
	}
	return item, nil
}

// readFlowSavedFilter loads a single saved filter the viewer may see: a shared
// filter or the viewer's own private filter.
func (s *Server) readFlowSavedFilter(ctx context.Context, id, viewerID string) (flowSavedFilter, error) {
	return scanFlowSavedFilter(s.db.QueryRowContext(ctx, `SELECT `+flowSavedFilterColumns+`
		FROM flow_saved_filters f LEFT JOIN users u ON u.id = f.owner_user_id
		WHERE f.id = ? AND f.deleted_at IS NULL AND (f.share_scope = 'shared' OR f.owner_user_id = ?)`, id, viewerID))
}

func (s *Server) registerFlowSavedFilterRoutes(auth *gin.RouterGroup, view gin.HandlerFunc) {
	filters := auth.Group("/flow/filters", view)
	filters.GET("", s.listFlowSavedFilters)
	filters.POST("", s.createFlowSavedFilter)
	filters.GET("/facets/owners", s.listFlowSavedFilterOwners)
	filters.GET("/:filter_id", s.getFlowSavedFilter)
	filters.PATCH("/:filter_id", s.patchFlowSavedFilter)
	filters.DELETE("/:filter_id", s.deleteFlowSavedFilter)
}

func (s *Server) listFlowSavedFilters(c *gin.Context) {
	page, ok := parseInventoryPage(c, []string{"scope", "owner_id"}, map[string]string{
		"name": "f.name", "share_scope": "f.share_scope", "owner_user_id": "f.owner_user_id",
		"created_at": "f.created_at", "updated_at": "f.updated_at",
	}, "updated_at")
	if !ok {
		return
	}
	p := currentPrincipal(c)
	where := []string{"f.deleted_at IS NULL", "(f.share_scope = 'shared' OR f.owner_user_id = ?)"}
	args := []any{p.UserID}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, "(f.name LIKE ? OR f.description LIKE ?)")
		args = append(args, like, like)
	}
	if scope := strings.TrimSpace(c.Query("scope")); scope != "" {
		if scope != flowSavedFilterPrivate && scope != flowSavedFilterShared {
			fail(c, http.StatusBadRequest, "invalid_filter", "scope must be private or shared")
			return
		}
		where = append(where, "f.share_scope = ?")
		args = append(args, scope)
	}
	if owner := strings.TrimSpace(c.Query("owner_id")); owner != "" {
		if len(owner) > 26 {
			fail(c, http.StatusBadRequest, "invalid_filter", "owner_id must not exceed 26 characters")
			return
		}
		where = append(where, "f.owner_user_id = ?")
		args = append(args, owner)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*) FROM flow_saved_filters f`+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	query := `SELECT ` + flowSavedFilterColumns + ` FROM flow_saved_filters f LEFT JOIN users u ON u.id = f.owner_user_id` + clause +
		fmt.Sprintf(" ORDER BY %s %s, f.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order)
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), query, queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]flowSavedFilter, 0, page.Limit)
	for rows.Next() {
		item, err := scanFlowSavedFilter(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		item.CanEdit = canEditFlowSavedFilter(p, item)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"items": items, "total": total, "limit": page.Limit, "offset": page.Offset,
		"meta": gin.H{"can_share": canShareFlowFilter(p)},
	})
}

func (s *Server) getFlowSavedFilter(c *gin.Context) {
	p := currentPrincipal(c)
	item, err := s.readFlowSavedFilter(c.Request.Context(), c.Param("filter_id"), p.UserID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	item.CanEdit = canEditFlowSavedFilter(p, item)
	c.Header("ETag", etag(item.RowVersion))
	c.JSON(http.StatusOK, item)
}

func (s *Server) createFlowSavedFilter(c *gin.Context) {
	var input flowSavedFilterInput
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	p := currentPrincipal(c)
	item, err := normalizeFlowSavedFilter(flowSavedFilter{
		OwnerUserID: p.UserID, Name: input.Name, Description: input.Description,
		ShareScope: input.ShareScope, Filter: input.Filter,
	})
	if err != nil {
		writeFlowSavedFilterError(c, err)
		return
	}
	if item.ShareScope == flowSavedFilterShared && !canShareFlowFilter(p) {
		fail(c, http.StatusForbidden, "forbidden", "sharing a saved filter requires flow management permission")
		return
	}
	filterJSON, err := json.Marshal(item.Filter)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", "encode filter")
		return
	}
	id := newID()
	if _, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO flow_saved_filters
		(id, owner_user_id, name, description, share_scope, filter_schema_version, filter_json)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, p.UserID, item.Name, item.Description, item.ShareScope, item.FilterSchemaVersion, filterJSON,
	); err != nil {
		writeSQLError(c, err)
		return
	}
	created, err := s.readFlowSavedFilter(c.Request.Context(), id, p.UserID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	created.CanEdit = true
	s.audit(c.Request.Context(), p.UserID, "flow.saved_filter.create", "flow_saved_filter", id)
	c.Header("ETag", etag(created.RowVersion))
	c.Header("Location", "/api/v1/flow/filters/"+id)
	c.JSON(http.StatusCreated, created)
}

func (s *Server) patchFlowSavedFilter(c *gin.Context) {
	p := currentPrincipal(c)
	filterID := c.Param("filter_id")
	existing, err := s.readFlowSavedFilter(c.Request.Context(), filterID, p.UserID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !canEditFlowSavedFilter(p, existing) {
		fail(c, http.StatusForbidden, "forbidden", "saved Flow filter cannot be changed by this user")
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != existing.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	var input flowSavedFilterInput
	if !addressDecodeStrict(c, &input, 256<<10) {
		return
	}
	item, err := normalizeFlowSavedFilter(flowSavedFilter{
		ID: filterID, OwnerUserID: existing.OwnerUserID, Name: input.Name,
		Description: input.Description, ShareScope: input.ShareScope, Filter: input.Filter,
	})
	if err != nil {
		writeFlowSavedFilterError(c, err)
		return
	}
	if item.ShareScope == flowSavedFilterShared && !canShareFlowFilter(p) {
		fail(c, http.StatusForbidden, "forbidden", "sharing a saved filter requires flow management permission")
		return
	}
	filterJSON, err := json.Marshal(item.Filter)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", "encode filter")
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `UPDATE flow_saved_filters
		SET name = ?, description = ?, share_scope = ?, filter_schema_version = ?, filter_json = ?,
		    row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND row_version = ? AND deleted_at IS NULL`,
		item.Name, item.Description, item.ShareScope, item.FilterSchemaVersion, filterJSON,
		filterID, existing.RowVersion,
	)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	updated, err := s.readFlowSavedFilter(c.Request.Context(), filterID, p.UserID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	updated.CanEdit = canEditFlowSavedFilter(p, updated)
	s.audit(c.Request.Context(), p.UserID, "flow.saved_filter.update", "flow_saved_filter", filterID)
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated)
}

func (s *Server) deleteFlowSavedFilter(c *gin.Context) {
	p := currentPrincipal(c)
	filterID := c.Param("filter_id")
	existing, err := s.readFlowSavedFilter(c.Request.Context(), filterID, p.UserID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !canEditFlowSavedFilter(p, existing) {
		fail(c, http.StatusForbidden, "forbidden", "saved Flow filter cannot be deleted by this user")
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != existing.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `UPDATE flow_saved_filters
		SET deleted_at = CURRENT_TIMESTAMP(3), row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND row_version = ? AND deleted_at IS NULL`, filterID, existing.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	s.audit(c.Request.Context(), p.UserID, "flow.saved_filter.delete", "flow_saved_filter", filterID)
	c.Status(http.StatusNoContent)
}

// listFlowSavedFilterOwners returns the distinct owners of filters the viewer can
// see, for the client's "filter by owner" facet.
func (s *Server) listFlowSavedFilterOwners(c *gin.Context) {
	for key := range c.Request.URL.Query() {
		if key != "q" && key != "limit" {
			fail(c, http.StatusBadRequest, "invalid_filter", "unsupported query parameter: "+key)
			return
		}
	}
	limit := 50
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 200 {
			fail(c, http.StatusBadRequest, "invalid_filter", "limit must be between 1 and 200")
			return
		}
		limit = value
	}
	search := strings.TrimSpace(c.Query("q"))
	if len(search) > 255 {
		fail(c, http.StatusBadRequest, "invalid_filter", "q must not exceed 255 characters")
		return
	}
	p := currentPrincipal(c)
	where := []string{"f.deleted_at IS NULL", "(f.share_scope = 'shared' OR f.owner_user_id = ?)"}
	args := []any{p.UserID}
	if search != "" {
		like := "%" + escapeLike(search) + "%"
		where = append(where, "(u.username LIKE ? OR u.email LIKE ?)")
		args = append(args, like, like)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT COALESCE(f.owner_user_id, ''), COALESCE(u.username, ''), COUNT(*)
		FROM flow_saved_filters f LEFT JOIN users u ON u.id = f.owner_user_id
		WHERE `+strings.Join(where, " AND ")+`
		GROUP BY f.owner_user_id, u.username
		ORDER BY COALESCE(u.username, ''), COALESCE(f.owner_user_id, '')
		LIMIT ?`, args...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]flowSavedFilterOwnerFacet, 0)
	for rows.Next() {
		var item flowSavedFilterOwnerFacet
		if err := rows.Scan(&item.OwnerUserID, &item.OwnerName, &item.Count); err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

// writeFlowSavedFilterError maps a validation error to 400 and defers everything
// else (including sql.ErrNoRows and version conflicts) to writeSQLError.
func writeFlowSavedFilterError(c *gin.Context, err error) {
	if errors.Is(err, errFlowSavedFilterInvalid) {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	writeSQLError(c, err)
}
