package server

import (
	"database/sql"
	"errors"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/gin-gonic/gin"
)

// aggregateGraphRecord is the de-tenanted form of the established saved graph
// contract. MySQL owns only this definition and its memberships; samples stay
// in ClickHouse and are queried through snmpch.
type aggregateGraphRecord struct {
	ID, Name, Aggregation, ValueMode, Unit, Description string
	RowVersion                                          uint64
	CreatedAt, UpdatedAt                                time.Time
}

type aggregateGraphDevice struct {
	ID, Name string
}

type aggregateGraphView struct {
	ID, Name, Aggregation, ValueMode, Unit, Description string
	RowVersion                                          uint64
	CreatedAt, UpdatedAt                                time.Time
	PortCount                                           int
	Metrics                                             []string
	Devices                                             []aggregateGraphDevice
}

type aggregateGraphMutation struct {
	ID, Name, Aggregation, ValueMode, Unit, Description string
}

type aggregateGraphItem struct {
	ID, GraphID, Metric, Direction, Label, GraphType string
	Sequence                                         uint32
	Total                                            bool
	CreatedAt                                        time.Time
}

type aggregateGraphPort struct {
	AggregateGraphID, PortID string
	CreatedAt                time.Time
}

// aggregateGraphDataPoint preserves the historical /data response while its
// samples are now read from ClickHouse instead of a management-store snapshot.
type aggregateGraphDataPoint struct {
	ID, GraphID, ItemID string
	Timestamp           time.Time
	Value               float64
	CreatedAt           time.Time
}

var (
	errAggregateGraphValueMode    = errors.New("unsupported aggregate graph value mode")
	errAggregateGraphRawForbidden = errors.New("raw aggregate graph values require administrator permission")
)

const aggregateGraphSelect = `SELECT id,name,aggregation,value_mode,unit,COALESCE(description,''),row_version,created_at,updated_at FROM aggregate_graphs`

func scanAggregateGraph(row scanner) (aggregateGraphRecord, error) {
	var graph aggregateGraphRecord
	err := row.Scan(&graph.ID, &graph.Name, &graph.Aggregation, &graph.ValueMode, &graph.Unit, &graph.Description, &graph.RowVersion, &graph.CreatedAt, &graph.UpdatedAt)
	return graph, err
}

func (s *Server) readAggregateGraph(c *gin.Context, id string) (aggregateGraphRecord, error) {
	return scanAggregateGraph(s.db.QueryRowContext(c.Request.Context(), aggregateGraphSelect+` WHERE id=?`, id))
}

func (s *Server) listAggregateGraphs(c *gin.Context) {
	page, ok := parseInventoryPage(c, []string{"aggregation", "value_mode", "device_id", "metric"}, map[string]string{
		"name": "g.name", "created_at": "g.created_at", "updated_at": "g.updated_at",
	}, "name")
	if !ok {
		return
	}
	where, args := []string{"1=1"}, []any{}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + q + "%"
		where = append(where, `(g.name LIKE ? OR COALESCE(g.description,'') LIKE ? OR EXISTS (
			SELECT 1 FROM aggregate_graph_items qi WHERE qi.aggregate_graph_id=g.id AND (qi.metric LIKE ? OR qi.label LIKE ?)
		) OR EXISTS (
			SELECT 1 FROM aggregate_graph_ports qp JOIN ports qport ON qport.id=qp.port_id JOIN devices qd ON qd.id=qport.device_id
			WHERE qp.aggregate_graph_id=g.id AND (qd.host LIKE ? OR qd.display_name LIKE ? OR qd.sys_name LIKE ? OR qport.if_name LIKE ? OR qport.if_alias LIKE ?)
		))`)
		args = append(args, like, like, like, like, like, like, like, like, like)
	}
	for _, filter := range []struct{ param, column string }{{"aggregation", "g.aggregation"}, {"value_mode", "g.value_mode"}} {
		if value := strings.TrimSpace(c.Query(filter.param)); value != "" {
			where = append(where, filter.column+"=?")
			args = append(args, value)
		}
	}
	if value := strings.TrimSpace(c.Query("device_id")); value != "" {
		where = append(where, `EXISTS (SELECT 1 FROM aggregate_graph_ports fp JOIN ports fport ON fport.id=fp.port_id WHERE fp.aggregate_graph_id=g.id AND fport.device_id=?)`)
		args = append(args, value)
	}
	if value := strings.TrimSpace(c.Query("metric")); value != "" {
		where = append(where, `EXISTS (SELECT 1 FROM aggregate_graph_items fi WHERE fi.aggregate_graph_id=g.id AND fi.metric=?)`)
		args = append(args, value)
	}
	where, args = appendAggregateGraphScopeSQL(where, args, currentPrincipal(c))
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*) FROM aggregate_graphs g`+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), aggregateGraphSelect+` g`+clause+` ORDER BY `+page.Sort+` `+page.Order+`,g.id LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	graphs := make([]aggregateGraphRecord, 0, page.Limit)
	for rows.Next() {
		graph, err := scanAggregateGraph(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		graphs = append(graphs, graph)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	items, err := s.aggregateGraphListViews(c, graphs)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) aggregateGraphListViews(c *gin.Context, graphs []aggregateGraphRecord) ([]aggregateGraphView, error) {
	views := make([]aggregateGraphView, len(graphs))
	positions := make(map[string]int, len(graphs))
	ids := make([]string, 0, len(graphs))
	for index, graph := range graphs {
		views[index] = aggregateGraphView{
			ID: graph.ID, Name: graph.Name, Aggregation: graph.Aggregation, ValueMode: graph.ValueMode,
			Unit: graph.Unit, Description: graph.Description, RowVersion: graph.RowVersion,
			CreatedAt: graph.CreatedAt, UpdatedAt: graph.UpdatedAt,
			Metrics: []string{}, Devices: []aggregateGraphDevice{},
		}
		positions[graph.ID] = index
		ids = append(ids, graph.ID)
	}
	if len(ids) == 0 {
		return views, nil
	}
	params := placeholders(len(ids))
	args := stringsToAny(ids)
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT aggregate_graph_id,metric FROM aggregate_graph_items WHERE aggregate_graph_id IN (`+params+`) ORDER BY aggregate_graph_id,sequence`, args...)
	if err != nil {
		return nil, err
	}
	metricSeen := map[string]map[string]bool{}
	for rows.Next() {
		var graphID, metric string
		if err := rows.Scan(&graphID, &metric); err != nil {
			rows.Close()
			return nil, err
		}
		if metricSeen[graphID] == nil {
			metricSeen[graphID] = map[string]bool{}
		}
		if !metricSeen[graphID][metric] {
			metricSeen[graphID][metric] = true
			views[positions[graphID]].Metrics = append(views[positions[graphID]].Metrics, metric)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	rows, err = s.db.QueryContext(c.Request.Context(), `SELECT gp.aggregate_graph_id,p.device_id,COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host),COUNT(*)
		FROM aggregate_graph_ports gp JOIN ports p ON p.id=gp.port_id JOIN devices d ON d.id=p.device_id
		WHERE gp.aggregate_graph_id IN (`+params+`) GROUP BY gp.aggregate_graph_id,p.device_id,d.display_name,d.sys_name,d.host ORDER BY gp.aggregate_graph_id,3`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var graphID, deviceID, name string
		var count int
		if err := rows.Scan(&graphID, &deviceID, &name, &count); err != nil {
			rows.Close()
			return nil, err
		}
		index := positions[graphID]
		views[index].PortCount += count
		views[index].Devices = append(views[index].Devices, aggregateGraphDevice{ID: deviceID, Name: name})
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return views, nil
}

func stringsToAny(values []string) []any {
	args := make([]any, len(values))
	for index := range values {
		args[index] = values[index]
	}
	return args
}

func (s *Server) getAggregateGraph(c *gin.Context) {
	graph, ok := s.loadAggregateGraph(c)
	if !ok {
		return
	}
	c.Header("ETag", etag(graph.RowVersion))
	c.JSON(http.StatusOK, graph)
}

func (s *Server) createAggregateGraph(c *gin.Context) {
	var request aggregateGraphMutation
	if err := c.ShouldBindJSON(&request); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	request = normalizeAggregateGraphMutation(request)
	if message := validateAggregateGraphMutation(request); message != "" {
		fail(c, http.StatusBadRequest, "invalid_request", message)
		return
	}
	if request.ID == "" {
		request.ID = newID()
	}
	if request.ValueMode != "corrected" && !aggregateGraphAdmin(c) {
		fail(c, http.StatusForbidden, "forbidden", "raw aggregate graph values require administrator permission")
		return
	}
	_, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO aggregate_graphs (id,name,aggregation,value_mode,unit,description,created_by,updated_by) VALUES (?,?,?,?,?,?,?,?)`,
		request.ID, request.Name, request.Aggregation, request.ValueMode, request.Unit, request.Description, principalUserID(c), principalUserID(c))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	graph, err := s.readAggregateGraph(c, request.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(graph.RowVersion))
	c.Header("Location", "/api/v1/aggregate-graphs/"+graph.ID)
	c.JSON(http.StatusCreated, graph)
}

func (s *Server) updateAggregateGraph(c *gin.Context) {
	old, ok := s.loadAggregateGraph(c)
	if !ok {
		return
	}
	var request aggregateGraphMutation
	if err := c.ShouldBindJSON(&request); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	request.ID = old.ID
	request = normalizeAggregateGraphMutation(request)
	if message := validateAggregateGraphMutation(request); message != "" {
		fail(c, http.StatusBadRequest, "invalid_request", message)
		return
	}
	if request.ValueMode != "corrected" && !aggregateGraphAdmin(c) {
		fail(c, http.StatusForbidden, "forbidden", "raw aggregate graph values require administrator permission")
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != old.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `UPDATE aggregate_graphs SET name=?,aggregation=?,value_mode=?,unit=?,description=?,updated_by=?,row_version=row_version+1 WHERE id=? AND row_version=?`,
		request.Name, request.Aggregation, request.ValueMode, request.Unit, request.Description, principalUserID(c), old.ID, old.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	updated, err := s.readAggregateGraph(c, old.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(updated.RowVersion))
	c.JSON(http.StatusOK, updated)
}

func (s *Server) deleteAggregateGraph(c *gin.Context) {
	graph, ok := s.loadAggregateGraph(c)
	if !ok {
		return
	}
	if expected, supplied, err := ifMatch(c); err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	} else if supplied && expected != graph.RowVersion {
		writeSQLError(c, errVersionConflict)
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `DELETE FROM aggregate_graphs WHERE id=? AND row_version=?`, graph.ID, graph.RowVersion)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	c.Status(http.StatusNoContent)
}

func normalizeAggregateGraphMutation(request aggregateGraphMutation) aggregateGraphMutation {
	request.ID = strings.TrimSpace(request.ID)
	request.Name = strings.TrimSpace(request.Name)
	request.Aggregation = strings.ToLower(strings.TrimSpace(request.Aggregation))
	request.ValueMode = strings.ToLower(strings.TrimSpace(request.ValueMode))
	request.Unit = strings.TrimSpace(request.Unit)
	request.Description = strings.TrimSpace(request.Description)
	if request.Aggregation == "" {
		request.Aggregation = "sum"
	}
	if request.ValueMode == "" {
		request.ValueMode = "corrected"
	}
	return request
}

func validateAggregateGraphMutation(request aggregateGraphMutation) string {
	if request.ID != "" && len(request.ID) > 64 {
		return "id must not exceed 64 characters"
	}
	if request.Name == "" || len(request.Name) > 190 {
		return "name is required and must not exceed 190 characters"
	}
	if !validSNMPAggregateMethod(request.Aggregation) {
		return "aggregation must be sum, avg, min, max or count"
	}
	if request.ValueMode != "corrected" && request.ValueMode != "raw" && request.ValueMode != "both" {
		return "value mode must be corrected, raw or both"
	}
	if len(request.Unit) > 64 || len(request.Description) > 8192 {
		return "unit or description is too long"
	}
	return ""
}

func aggregateGraphAdmin(c *gin.Context) bool {
	p := currentPrincipal(c)
	return p != nil && p.IsAdmin
}

func (s *Server) loadAggregateGraph(c *gin.Context) (aggregateGraphRecord, bool) {
	graph, err := s.readAggregateGraph(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return aggregateGraphRecord{}, false
	}
	if err := s.requireAggregateGraphAccess(c, graph.ID); err != nil {
		writeRBACResourceError(c, err)
		return aggregateGraphRecord{}, false
	}
	return graph, true
}

func (s *Server) requireAggregateGraphAccess(c *gin.Context, graphID string) error {
	p := currentPrincipal(c)
	direct, err := s.directlyGrantedAggregateGraph(c.Request.Context(), p, graphID)
	if err != nil || direct {
		return err
	}
	if err := s.requireAggregateGraphMetrics(c.Request.Context(), p, graphID); err != nil {
		return err
	}
	ports, err := s.readAggregateGraphPorts(c, graphID)
	if err != nil || len(ports) == 0 {
		return err
	}
	ids := make([]string, 0, len(ports))
	for _, port := range ports {
		ids = append(ids, port.PortID)
	}
	_, err = s.resolveSNMPScopes(c.Request.Context(), currentPrincipal(c), nil, ids)
	return err
}

func (s *Server) listAggregateGraphItems(c *gin.Context) {
	graph, ok := s.loadAggregateGraph(c)
	if !ok {
		return
	}
	items, err := s.readAggregateGraphItems(c, graph.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": len(items)})
}

func (s *Server) readAggregateGraphItems(c *gin.Context, graphID string) ([]aggregateGraphItem, error) {
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT id,aggregate_graph_id,sequence,metric,direction,label,graph_type,total,created_at FROM aggregate_graph_items WHERE aggregate_graph_id=? ORDER BY sequence,id`, graphID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []aggregateGraphItem{}
	for rows.Next() {
		var item aggregateGraphItem
		if err := rows.Scan(&item.ID, &item.GraphID, &item.Sequence, &item.Metric, &item.Direction, &item.Label, &item.GraphType, &item.Total, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Server) replaceAggregateGraphItems(c *gin.Context) {
	graph, ok := s.loadAggregateGraph(c)
	if !ok {
		return
	}
	var body struct {
		Items []aggregateGraphItem `json:"items"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	if len(body.Items) == 0 || len(body.Items) > 64 {
		fail(c, http.StatusBadRequest, "invalid_request", "one to 64 metrics are required")
		return
	}
	seen := map[string]bool{}
	for index := range body.Items {
		item := &body.Items[index]
		item.ID = strings.TrimSpace(item.ID)
		if item.ID == "" {
			item.ID = newID()
		}
		item.Metric = strings.TrimSpace(item.Metric)
		item.Direction = strings.ToLower(strings.TrimSpace(item.Direction))
		item.GraphType = strings.ToLower(strings.TrimSpace(item.GraphType))
		item.Label = strings.TrimSpace(item.Label)
		if item.Direction == "" {
			item.Direction = "other"
		}
		if item.GraphType == "" {
			item.GraphType = "line"
		}
		if len(item.ID) > 64 || !isSNMPMetric(item.Metric) || !validAggregateItemDirection(item.Direction) || !validAggregateGraphType(item.GraphType) || len(item.Label) > 190 || seen[item.ID] {
			fail(c, http.StatusBadRequest, "invalid_request", "invalid or duplicate aggregate graph item")
			return
		}
		if !s.requireMetricAccess(c, item.Metric) {
			return
		}
		seen[item.ID] = true
		item.GraphID = graph.ID
		item.Sequence = uint32(index)
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(c.Request.Context(), `DELETE FROM aggregate_graph_items WHERE aggregate_graph_id=?`, graph.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	for _, item := range body.Items {
		if _, err := tx.ExecContext(c.Request.Context(), `INSERT INTO aggregate_graph_items (id,aggregate_graph_id,sequence,metric,direction,label,graph_type,total) VALUES (?,?,?,?,?,?,?,?)`,
			item.ID, graph.ID, item.Sequence, item.Metric, item.Direction, item.Label, item.GraphType, item.Total); err != nil {
			writeSQLError(c, err)
			return
		}
	}
	if _, err := tx.ExecContext(c.Request.Context(), `UPDATE aggregate_graphs SET updated_by=?,row_version=row_version+1 WHERE id=?`, principalUserID(c), graph.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func validAggregateItemDirection(value string) bool {
	return value == "in" || value == "out" || value == "other"
}

func validAggregateGraphType(value string) bool {
	return value == "line" || value == "area" || value == "stack"
}

func (s *Server) listAggregateGraphPorts(c *gin.Context) {
	graph, ok := s.loadAggregateGraph(c)
	if !ok {
		return
	}
	ports, err := s.readAggregateGraphPorts(c, graph.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": ports, "total": len(ports)})
}

func (s *Server) readAggregateGraphPorts(c *gin.Context, graphID string) ([]aggregateGraphPort, error) {
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT aggregate_graph_id,port_id,created_at FROM aggregate_graph_ports WHERE aggregate_graph_id=? ORDER BY port_id`, graphID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ports := []aggregateGraphPort{}
	for rows.Next() {
		var port aggregateGraphPort
		if err := rows.Scan(&port.AggregateGraphID, &port.PortID, &port.CreatedAt); err != nil {
			return nil, err
		}
		ports = append(ports, port)
	}
	return ports, rows.Err()
}

func (s *Server) replaceAggregateGraphPorts(c *gin.Context) {
	graph, ok := s.loadAggregateGraph(c)
	if !ok {
		return
	}
	var body struct {
		Ports []aggregateGraphPort `json:"ports"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	if len(body.Ports) == 0 || len(body.Ports) > 1000 {
		fail(c, http.StatusBadRequest, "invalid_request", "one to 1000 ports are required")
		return
	}
	ids, seen := make([]string, 0, len(body.Ports)), map[string]bool{}
	for _, port := range body.Ports {
		id := strings.TrimSpace(port.PortID)
		if id == "" || len(id) > 64 || seen[id] {
			fail(c, http.StatusBadRequest, "invalid_request", "invalid or duplicate port id")
			return
		}
		seen[id] = true
		ids = append(ids, id)
	}
	if _, err := s.resolveSNMPScopes(c.Request.Context(), currentPrincipal(c), nil, ids); err != nil {
		writeSNMPScopeError(c, err)
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(c.Request.Context(), `DELETE FROM aggregate_graph_ports WHERE aggregate_graph_id=?`, graph.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(c.Request.Context(), `INSERT INTO aggregate_graph_ports (aggregate_graph_id,port_id) VALUES (?,?)`, graph.ID, id); err != nil {
			writeSQLError(c, err)
			return
		}
	}
	if _, err := tx.ExecContext(c.Request.Context(), `UPDATE aggregate_graphs SET updated_by=?,row_version=row_version+1 WHERE id=?`, principalUserID(c), graph.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (s *Server) aggregateGraphSeries(c *gin.Context) {
	graph, ok := s.loadAggregateGraph(c)
	if !ok {
		return
	}
	maxRows, valid := parseSNMPRowBudget(c.Query("max_data_points"), 250000)
	if !valid {
		fail(c, http.StatusBadRequest, "invalid_range", "max_data_points must be 1..250000")
		return
	}
	from, to, step, ok := aggregateGraphWindow(c, int(maxRows))
	if !ok {
		return
	}
	response, _, err := s.queryAggregateGraph(c, graph, from, to, step, maxRows)
	if err != nil {
		writeAggregateGraphQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, response)
}

func (s *Server) aggregateGraphSummary(c *gin.Context) {
	graph, ok := s.loadAggregateGraph(c)
	if !ok {
		return
	}
	// Historical summary was computed from the single stored corrected series,
	// even when the interactive /series view allowed an admin raw comparison.
	query := c.Request.URL.Query()
	query.Set("value_mode", "corrected")
	c.Request.URL.RawQuery = query.Encode()
	from, to, step, ok := aggregateGraphWindow(c, 600)
	if !ok {
		return
	}
	_, samples, err := s.queryAggregateGraph(c, graph, from, to, step, 250000)
	if err != nil {
		writeAggregateGraphQueryError(c, err)
		return
	}
	c.JSON(http.StatusOK, summarizeAggregateGraph(samples, step, graph.Unit))
}

func (s *Server) aggregateGraphData(c *gin.Context) {
	graph, ok := s.loadAggregateGraph(c)
	if !ok {
		return
	}
	// Historical /data persisted one corrected value per graph item and bucket.
	// Keep that DTO cardinality; raw/both remains an interactive /series option.
	query := c.Request.URL.Query()
	query.Set("value_mode", "corrected")
	c.Request.URL.RawQuery = query.Encode()
	from, to, step, ok := aggregateGraphWindow(c, 600)
	if !ok {
		return
	}
	response, _, err := s.queryAggregateGraph(c, graph, from, to, step, 250000)
	if err != nil {
		writeAggregateGraphQueryError(c, err)
		return
	}
	items := make([]aggregateGraphDataPoint, 0)
	for _, series := range response.Data.Result {
		itemID := series.Metric["item_id"]
		if itemID == "" {
			// The synthetic Total series was never an aggregate_graph_data row.
			continue
		}
		for _, value := range series.Values {
			timestamp := value.Time.UTC()
			items = append(items, aggregateGraphDataPoint{
				ID:      graph.ID + ":" + itemID + ":" + strconv.FormatInt(timestamp.UnixMilli(), 10),
				GraphID: graph.ID, ItemID: itemID, Timestamp: timestamp, Value: value.Value, CreatedAt: timestamp,
			})
		}
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func aggregateGraphWindow(c *gin.Context, maxDataPoints int) (time.Time, time.Time, time.Duration, bool) {
	var from, to time.Time
	var step time.Duration
	if c.Query("start") != "" || c.Query("end") != "" {
		var err error
		from, err = time.Parse(time.RFC3339, c.Query("start"))
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_range", "invalid start")
			return time.Time{}, time.Time{}, 0, false
		}
		to, err = time.Parse(time.RFC3339, c.Query("end"))
		if err != nil || !to.After(from) || to.Sub(from) > 400*24*time.Hour {
			fail(c, http.StatusBadRequest, "invalid_range", "invalid end or range")
			return time.Time{}, time.Time{}, 0, false
		}
	} else {
		var ok bool
		from, to, step, ok = parseMetricWindow(c)
		if !ok {
			return time.Time{}, time.Time{}, 0, false
		}
	}
	if strings.TrimSpace(c.Query("step")) == "" {
		step = watchdog.AutoQueryStep(to.Sub(from), maxDataPoints)
	}
	return from.UTC(), to.UTC(), step, true
}

func (s *Server) queryAggregateGraph(c *gin.Context, graph aggregateGraphRecord, from, to time.Time, step time.Duration, maxRows uint32) (metricRangeResponse, []snmpch.Point, error) {
	if s.snmpMetrics == nil {
		return metricRangeResponse{}, nil, errors.New("SNMP ClickHouse store is not configured")
	}
	items, err := s.readAggregateGraphItems(c, graph.ID)
	if err != nil {
		return metricRangeResponse{}, nil, err
	}
	ports, err := s.readAggregateGraphPorts(c, graph.ID)
	if err != nil {
		return metricRangeResponse{}, nil, err
	}
	ids := make([]string, 0, len(ports))
	for _, port := range ports {
		ids = append(ids, port.PortID)
	}
	scopes, err := s.resolveSNMPScopes(c.Request.Context(), currentPrincipal(c), nil, ids)
	if err != nil {
		return metricRangeResponse{}, nil, err
	}
	modes, err := aggregateGraphValueModes(c, graph)
	if err != nil {
		return metricRangeResponse{}, nil, err
	}
	groups, err := s.aggregateGraphScopeGroups(c, scopes, c.Request.URL.Query().Has("split_side_type"))
	if err != nil {
		return metricRangeResponse{}, nil, err
	}
	response := metricRangeResponse{Status: "success"}
	response.Data.ResultType = "matrix"
	combined, include := map[time.Time]float64{}, false
	totalSeries := map[string]map[time.Time]float64{}
	hasTotal := false
	for _, item := range items {
		for _, group := range groups {
			series, err := s.snmpMetrics.QueryScopes(c.Request.Context(), snmpch.AggregateRequest{
				Scopes: group.Scopes, Metric: item.Metric, Method: graph.Aggregation, From: from, To: to, Step: step,
				MaxRows: snmpScopedQueryRowBudget(maxRows, group.Scopes),
			})
			if err != nil {
				return metricRangeResponse{}, nil, err
			}
			policies := map[string]watchdog.PortPolicy{}
			if item.Metric == snmpch.MetricIfInBPS || item.Metric == snmpch.MetricIfOutBPS {
				policies, err = s.readPortPoliciesContext(c.Request.Context(), snmpSeriesPortIDs(series))
				if err != nil {
					return metricRangeResponse{}, nil, err
				}
			}
			label := item.Label
			if label == "" {
				label = item.Direction
			}
			if label == "" {
				label = item.Metric
			}
			if group.SideType != "" {
				label += " · " + group.SideType
			}
			for _, mode := range modes {
				view := correctedSNMPSeries(series, policies, mode == "corrected" && (item.Metric == snmpch.MetricIfInBPS || item.Metric == snmpch.MetricIfOutBPS))
				points := aggregateSNMPSeries(view, graph.Aggregation)
				values := metricValues(points)
				if mode == "corrected" {
					for _, point := range points {
						combined[point.Time] += point.Value
					}
				}
				include = include || len(points) > 0
				labels := map[string]string{
					"__name__": item.Metric, "item_id": item.ID, "label": label,
					"direction": item.Direction, "value_mode": mode,
				}
				if group.SideType != "" {
					labels["side_type"] = group.SideType
				}
				response.Data.Result = append(response.Data.Result, metricRangeQueryItem{Metric: labels, Values: values})
				if item.Total {
					hasTotal = true
					key := mode + "\x00" + group.SideType
					if totalSeries[key] == nil {
						totalSeries[key] = map[time.Time]float64{}
					}
					for _, point := range points {
						totalSeries[key][point.Time] += point.Value
					}
				}
			}
		}
	}
	if hasTotal {
		for key, totals := range totalSeries {
			mode, sideType, _ := strings.Cut(key, "\x00")
			values := make([]metricValue, 0, len(totals))
			for _, point := range sortedAggregatePoints(totals) {
				values = append(values, metricValue{Time: point.Time, Value: point.Value})
			}
			label := "Total"
			if sideType != "" {
				label += " · " + sideType
			}
			labels := map[string]string{"label": label, "value_mode": mode}
			if sideType != "" {
				labels["side_type"] = sideType
			}
			response.Data.Result = append(response.Data.Result, metricRangeQueryItem{Metric: labels, Values: values})
		}
	}
	if !include {
		return response, []snmpch.Point{}, nil
	}
	return response, sortedAggregatePoints(combined), nil
}

func aggregateGraphValueModes(c *gin.Context, graph aggregateGraphRecord) ([]string, error) {
	mode := strings.ToLower(strings.TrimSpace(c.Query("value_mode")))
	if mode == "" {
		mode = graph.ValueMode
	}
	if mode == "" {
		mode = "corrected"
	}
	if mode != "corrected" && mode != "raw" && mode != "both" {
		return nil, errAggregateGraphValueMode
	}
	if mode != "corrected" && !aggregateGraphAdmin(c) {
		return nil, errAggregateGraphRawForbidden
	}
	if mode == "both" {
		return []string{"corrected", "raw"}, nil
	}
	return []string{mode}, nil
}

type aggregateGraphScopeGroup struct {
	SideType string
	Scopes   []snmpch.Scope
}

func (s *Server) aggregateGraphScopeGroups(c *gin.Context, scopes []snmpch.Scope, split bool) ([]aggregateGraphScopeGroup, error) {
	if !split {
		return []aggregateGraphScopeGroup{{Scopes: scopes}}, nil
	}
	portIDs := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		if scope.PortID != "" {
			portIDs = append(portIDs, scope.PortID)
		}
	}
	if len(portIDs) == 0 {
		return []aggregateGraphScopeGroup{{SideType: "unknown", Scopes: scopes}}, nil
	}
	policies, err := s.readPortPoliciesContext(c.Request.Context(), portIDs)
	if err != nil {
		return nil, err
	}
	sideByPort := make(map[string]string, len(portIDs))
	for portID, policy := range policies {
		sideByPort[portID] = string(policy.SideType)
	}
	groups := map[string][]snmpch.Scope{}
	for _, scope := range scopes {
		sideType := sideByPort[scope.PortID]
		if sideType == "" {
			sideType = "customer"
		}
		groups[sideType] = append(groups[sideType], scope)
	}
	result := make([]aggregateGraphScopeGroup, 0, 2)
	for _, sideType := range []string{"provider", "customer"} {
		if len(groups[sideType]) > 0 {
			result = append(result, aggregateGraphScopeGroup{SideType: sideType, Scopes: groups[sideType]})
		}
	}
	return result, nil
}

func sortedAggregatePoints(values map[time.Time]float64) []snmpch.Point {
	points := make([]snmpch.Point, 0, len(values))
	for observedAt, value := range values {
		points = append(points, snmpch.Point{Time: observedAt, Value: value})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Time.Before(points[j].Time) })
	return points
}

func summarizeAggregateGraph(points []snmpch.Point, step time.Duration, unit string) gin.H {
	if len(points) == 0 {
		return gin.H{"samples": 0, "p95": 0, "peak": 0, "average": 0, "total_bytes": 0}
	}
	values := make([]float64, len(points))
	var sum, peak float64
	for index, point := range points {
		values[index] = point.Value
		sum += point.Value
		if index == 0 || point.Value > peak {
			peak = point.Value
		}
	}
	sort.Float64s(values)
	position := int(math.Ceil(float64(len(values))*0.95)) - 1
	if position < 0 {
		position = 0
	}
	var totalBytes uint64
	if strings.EqualFold(unit, "bps") {
		total := sum * step.Seconds() / 8
		if total > 0 && total < math.MaxUint64 {
			totalBytes = uint64(math.Round(total))
		}
	}
	return gin.H{"samples": len(points), "p95": values[position], "peak": peak, "average": sum / float64(len(points)), "total_bytes": totalBytes}
}

func writeAggregateGraphQueryError(c *gin.Context, err error) {
	if errors.Is(err, errAggregateGraphValueMode) {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if errors.Is(err, errAggregateGraphRawForbidden) {
		fail(c, http.StatusForbidden, "forbidden", err.Error())
		return
	}
	if errors.Is(err, errSNMPScopeForbidden) || errors.Is(err, errSNMPScopeNotFound) || errors.Is(err, sql.ErrNoRows) {
		writeSNMPScopeError(c, err)
		return
	}
	if strings.Contains(err.Error(), "ClickHouse") {
		fail(c, http.StatusServiceUnavailable, "clickhouse_query_failed", err.Error())
		return
	}
	writeSQLError(c, err)
}
