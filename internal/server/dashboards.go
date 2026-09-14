package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	dashboardMaxPanels = 200
	dashboardMaxLayout = 256 << 10
)

type dashboardRecord struct {
	ID          string          `json:"id"`
	OwnerID     string          `json:"owner_id,omitempty"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Layout      json.RawMessage `json:"layout"`
	Version     uint64          `json:"version"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type dashboardLayout struct {
	Panels []dashboardPanel `json:"panels"`
}

type dashboardPanel struct {
	GraphID string `json:"graph_id"`
}

type dashboardGraphReference struct {
	GraphID string                `json:"graph_id"`
	Exists  bool                  `json:"exists"`
	Graph   *aggregateGraphRecord `json:"graph,omitempty"`
	Series  []aggregateGraphItem  `json:"series"`
}

type dashboardPreview struct {
	Dashboard       dashboardRecord           `json:"dashboard"`
	References      []dashboardGraphReference `json:"references"`
	MissingGraphIDs []string                  `json:"missing_graph_ids"`
}

const dashboardSelect = `SELECT id,COALESCE(owner_id,''),name,COALESCE(description,''),layout_json,row_version,created_at,updated_at FROM dashboards`

func scanDashboard(row scanner) (dashboardRecord, error) {
	var record dashboardRecord
	var layout []byte
	err := row.Scan(&record.ID, &record.OwnerID, &record.Name, &record.Description, &layout,
		&record.Version, &record.CreatedAt, &record.UpdatedAt)
	record.Layout = json.RawMessage(layout)
	return record, err
}

func (s *Server) listDashboards(c *gin.Context) {
	page, ok := parseInventoryPage(c, []string{"owner_id"}, map[string]string{
		"name": "name", "owner_id": "owner_id", "version": "row_version",
		"created_at": "created_at", "updated_at": "updated_at",
	}, "name")
	if !ok {
		return
	}
	where, args := []string{"1=1"}, []any{}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + q + "%"
		where = append(where, "(name LIKE ? OR COALESCE(description,'') LIKE ?)")
		args = append(args, like, like)
	}
	if ownerID := strings.TrimSpace(c.Query("owner_id")); ownerID != "" {
		where = append(where, "owner_id=?")
		args = append(args, ownerID)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM dashboards"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), dashboardSelect+clause+
		" ORDER BY "+page.Sort+" "+page.Order+",id "+page.Order+" LIMIT ? OFFSET ?", queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]dashboardRecord, 0, page.Limit)
	for rows.Next() {
		record, err := scanDashboard(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, record)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) getDashboard(c *gin.Context) {
	record, err := s.readDashboard(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(record.Version))
	c.JSON(http.StatusOK, record)
}

func (s *Server) createDashboard(c *gin.Context) {
	record, ok := decodeDashboard(c)
	if !ok {
		return
	}
	record.ID = newID()
	record.OwnerID = currentPrincipal(c).UserID
	_, err := s.db.ExecContext(c.Request.Context(), `
		INSERT INTO dashboards (id,owner_id,name,description,layout_json)
		VALUES (?,?,?,?,?)`, record.ID, record.OwnerID, record.Name, record.Description, string(record.Layout))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	created, err := s.readDashboard(c, record.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), record.OwnerID, "dashboard.created", "dashboard", created.ID)
	c.Header("ETag", etag(created.Version))
	c.Header("Location", "/api/v1/dashboards/"+created.ID)
	c.JSON(http.StatusCreated, created)
}

func (s *Server) updateDashboard(c *gin.Context) {
	current, err := s.readDashboard(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !dashboardIfMatch(c, current.Version) {
		return
	}
	request, ok := decodeDashboard(c)
	if !ok {
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `
		UPDATE dashboards
		SET name=?,description=?,layout_json=?,row_version=row_version+1
		WHERE id=? AND row_version=?`, request.Name, request.Description, string(request.Layout), current.ID, current.Version)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	updated, err := s.readDashboard(c, current.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "dashboard.updated", "dashboard", updated.ID)
	c.Header("ETag", etag(updated.Version))
	c.JSON(http.StatusOK, updated)
}

func (s *Server) deleteDashboard(c *gin.Context) {
	current, err := s.readDashboard(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if !dashboardIfMatch(c, current.Version) {
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), "DELETE FROM dashboards WHERE id=? AND row_version=?", current.ID, current.Version)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "dashboard.deleted", "dashboard", current.ID)
	c.Status(http.StatusNoContent)
}

func (s *Server) previewDashboardDraft(c *gin.Context) {
	record, ok := decodeDashboard(c)
	if !ok {
		return
	}
	record.OwnerID = currentPrincipal(c).UserID
	s.writeDashboardPreview(c, record)
}

func (s *Server) previewDashboard(c *gin.Context) {
	record, err := s.readDashboard(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(record.Version))
	s.writeDashboardPreview(c, record)
}

func (s *Server) writeDashboardPreview(c *gin.Context, record dashboardRecord) {
	graphIDs, err := dashboardGraphIDs(record.Layout)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	references, err := s.resolveDashboardGraphs(c, graphIDs)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	missing := make([]string, 0)
	for _, reference := range references {
		if !reference.Exists {
			missing = append(missing, reference.GraphID)
		}
	}
	c.JSON(http.StatusOK, dashboardPreview{Dashboard: record, References: references, MissingGraphIDs: missing})
}

func (s *Server) listDashboardGraphOptions(c *gin.Context) {
	page, ok := parseInventoryPage(c, nil, map[string]string{"name": "g.name"}, "name")
	if !ok {
		return
	}
	where, args := []string{"1=1"}, []any{}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + q + "%"
		where = append(where, "(g.name LIKE ? OR COALESCE(g.description,'') LIKE ?)")
		args = append(args, like, like)
	}
	where, args = appendDashboardGraphScope(where, args, currentPrincipal(c))
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM aggregate_graphs g"+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), aggregateGraphSelect+" g"+clause+
		" ORDER BY "+page.Sort+" "+page.Order+",g.id "+page.Order+" LIMIT ? OFFSET ?", queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]aggregateGraphRecord, 0, page.Limit)
	for rows.Next() {
		graph, err := scanAggregateGraph(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, graph)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) readDashboard(c *gin.Context, id string) (dashboardRecord, error) {
	return scanDashboard(s.db.QueryRowContext(c.Request.Context(), dashboardSelect+" WHERE id=?", id))
}

func decodeDashboard(c *gin.Context) (dashboardRecord, bool) {
	var record dashboardRecord
	decoder := json.NewDecoder(io.LimitReader(c.Request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid dashboard body: "+err.Error())
		return dashboardRecord{}, false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		fail(c, http.StatusBadRequest, "invalid_request", "dashboard body must contain one JSON object")
		return dashboardRecord{}, false
	}
	record.Name = strings.TrimSpace(record.Name)
	if record.Name == "" || len(record.Name) > 190 {
		fail(c, http.StatusBadRequest, "invalid_request", "name is required and must not exceed 190 characters")
		return dashboardRecord{}, false
	}
	if len(record.Description) > 4096 {
		fail(c, http.StatusBadRequest, "invalid_request", "description must not exceed 4096 characters")
		return dashboardRecord{}, false
	}
	if _, err := dashboardGraphIDs(record.Layout); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return dashboardRecord{}, false
	}
	return record, true
}

func dashboardGraphIDs(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, errors.New("dashboard layout is required")
	}
	if len(raw) > dashboardMaxLayout {
		return nil, errors.New("dashboard layout exceeds 262144 bytes")
	}
	if !strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		return nil, errors.New("dashboard layout must be a JSON object")
	}
	var layout dashboardLayout
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	if err := decoder.Decode(&layout); err != nil {
		return nil, errors.New("dashboard layout is not valid JSON")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("dashboard layout must contain one JSON object")
	}
	if len(layout.Panels) > dashboardMaxPanels {
		return nil, errors.New("dashboard has more than 200 panels")
	}
	ids := make([]string, 0, len(layout.Panels))
	seen := make(map[string]bool, len(layout.Panels))
	for index, panel := range layout.Panels {
		id := strings.TrimSpace(panel.GraphID)
		if id == "" {
			return nil, errors.New("dashboard panel " + strconv.Itoa(index) + " is missing graph_id")
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func dashboardIfMatch(c *gin.Context, current uint64) bool {
	if strings.TrimSpace(c.GetHeader("If-Match")) == "*" {
		return true
	}
	expected, supplied, err := ifMatch(c)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return false
	}
	if supplied && expected != current {
		writeSQLError(c, errVersionConflict)
		return false
	}
	return true
}

func appendDashboardGraphScope(where []string, args []any, principal *principal) ([]string, []any) {
	return appendAggregateGraphScopeSQL(where, args, principal)
}

func (s *Server) resolveDashboardGraphs(c *gin.Context, graphIDs []string) ([]dashboardGraphReference, error) {
	if len(graphIDs) == 0 {
		return []dashboardGraphReference{}, nil
	}
	where := []string{"g.id IN (" + placeholders(len(graphIDs)) + ")"}
	args := stringsToAny(graphIDs)
	where, args = appendDashboardGraphScope(where, args, currentPrincipal(c))
	rows, err := s.db.QueryContext(c.Request.Context(), aggregateGraphSelect+" g WHERE "+strings.Join(where, " AND "), args...)
	if err != nil {
		return nil, err
	}
	graphs := make(map[string]aggregateGraphRecord, len(graphIDs))
	for rows.Next() {
		graph, err := scanAggregateGraph(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		graphs[graph.ID] = graph
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	series := make(map[string][]aggregateGraphItem, len(graphs))
	visibleIDs := make([]string, 0, len(graphs))
	for id := range graphs {
		visibleIDs = append(visibleIDs, id)
	}
	if len(visibleIDs) > 0 {
		rows, err = s.db.QueryContext(c.Request.Context(), `
			SELECT id,aggregate_graph_id,sequence,metric,direction,label,graph_type,total,created_at
			FROM aggregate_graph_items WHERE aggregate_graph_id IN (`+placeholders(len(visibleIDs))+`)
			ORDER BY aggregate_graph_id,sequence,id`, stringsToAny(visibleIDs)...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var item aggregateGraphItem
			if err := rows.Scan(&item.ID, &item.GraphID, &item.Sequence, &item.Metric, &item.Direction, &item.Label, &item.GraphType, &item.Total, &item.CreatedAt); err != nil {
				return nil, err
			}
			series[item.GraphID] = append(series[item.GraphID], item)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	references := make([]dashboardGraphReference, 0, len(graphIDs))
	for _, id := range graphIDs {
		reference := dashboardGraphReference{GraphID: id, Series: []aggregateGraphItem{}}
		if graph, exists := graphs[id]; exists {
			graphCopy := graph
			reference.Exists = true
			reference.Graph = &graphCopy
			if items := series[id]; items != nil {
				reference.Series = items
			}
		}
		references = append(references, reference)
	}
	return references, nil
}
