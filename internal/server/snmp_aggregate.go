package server

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/gin-gonic/gin"
)

var (
	errSNMPScopeForbidden = errors.New("SNMP scope is outside the caller's resource grants")
	errSNMPScopeNotFound  = errors.New("SNMP scope does not exist")
)

func (s *Server) aggregateMetrics(c *gin.Context) {
	allowed := map[string]bool{
		"device_ids": true, "target_ids": true, "port_ids": true,
		"metric": true, "aggregate": true, "time_mode": true, "window": true,
		"start": true, "end": true, "step": true, "max_data_points": true,
		"value_mode": true, "traffic_view": true, "func": true,
	}
	for name := range c.Request.URL.Query() {
		if !allowed[name] {
			fail(c, http.StatusBadRequest, "invalid_filter", "unsupported query parameter: "+name)
			return
		}
	}
	metric := strings.TrimSpace(c.Query("metric"))
	if !isSNMPMetric(metric) {
		fail(c, http.StatusBadRequest, "invalid_metric", "unsupported SNMP metric")
		return
	}
	method := strings.TrimSpace(c.DefaultQuery("aggregate", "sum"))
	if !validSNMPAggregateMethod(method) {
		fail(c, http.StatusBadRequest, "invalid_aggregate", "aggregate must be sum, avg, min, max or count")
		return
	}
	deviceIDs, err := parseSNMPIDs(c.Query("device_ids"), c.Query("target_ids"))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_resource", err.Error())
		return
	}
	portIDs, err := parseSNMPIDs(c.Query("port_ids"))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_resource", err.Error())
		return
	}
	scopes, err := s.resolveSNMPScopes(c.Request.Context(), currentPrincipal(c), deviceIDs, portIDs)
	if err != nil {
		writeSNMPScopeError(c, err)
		return
	}
	from, to, step, ok := parseMetricWindow(c)
	if !ok {
		return
	}
	maxRows, ok := parseSNMPRowBudget(c.Query("max_data_points"), 250000)
	if !ok {
		fail(c, http.StatusBadRequest, "invalid_range", "max_data_points must be 1..250000")
		return
	}
	if s.snmpMetrics == nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_unavailable", "SNMP ClickHouse store is not configured")
		return
	}
	result, err := s.snmpMetrics.Aggregate(c.Request.Context(), snmpch.AggregateRequest{
		Scopes: scopes, Metric: metric, Method: method, From: from, To: to, Step: step, MaxRows: maxRows,
	})
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_query_failed", err.Error())
		return
	}
	values := make([]metricValue, 0, len(result.Points))
	for _, point := range result.Points {
		values = append(values, metricValue{Time: point.Time, Value: point.Value})
	}
	response := metricRangeResponse{Status: "success"}
	response.Data.ResultType = "matrix"
	response.Data.Result = []metricRangeQueryItem{{
		Metric: map[string]string{"__name__": metric, "aggregate": method, "value_mode": "raw"},
		Values: values,
	}}
	c.JSON(http.StatusOK, response)
}

func parseSNMPIDs(values ...string) ([]string, error) {
	seen := make(map[string]struct{})
	for _, value := range values {
		for _, item := range strings.Split(value, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			if len(item) > 64 {
				return nil, errors.New("resource ID exceeds 64 characters")
			}
			seen[item] = struct{}{}
			if len(seen) > 1000 {
				return nil, errors.New("at most 1000 device/port IDs are allowed")
			}
		}
	}
	result := make([]string, 0, len(seen))
	for item := range seen {
		result = append(result, item)
	}
	sort.Strings(result)
	return result, nil
}

func parseSNMPRowBudget(raw string, fallback uint32) (uint32, bool) {
	if strings.TrimSpace(raw) == "" {
		return fallback, true
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	return uint32(value), err == nil && value > 0 && value <= 250000
}

func validSNMPAggregateMethod(method string) bool {
	switch method {
	case "sum", "avg", "min", "max", "count":
		return true
	}
	return false
}

// resolveSNMPScopes is shared by synchronous aggregate and async export. It
// resolves child ports to their device root and applies current grants before
// a ClickHouse query is built; the job worker calls it again so revocation wins.
func (s *Server) resolveSNMPScopes(ctx context.Context, p *principal, deviceIDs, portIDs []string) ([]snmpch.Scope, error) {
	if p == nil {
		return nil, errSNMPScopeForbidden
	}
	if len(deviceIDs)+len(portIDs) == 0 || len(deviceIDs)+len(portIDs) > 1000 {
		return nil, errors.New("one to 1000 device or port IDs are required")
	}
	scopes := make([]snmpch.Scope, 0, len(deviceIDs)+len(portIDs))
	if len(deviceIDs) > 0 {
		query := `SELECT d.id FROM devices d WHERE d.id IN (` + placeholders(len(deviceIDs)) + `)`
		args := make([]any, 0, len(deviceIDs)+2)
		for _, id := range deviceIDs {
			args = append(args, id)
		}
		if !p.can("device.viewAll") {
			query += ` AND EXISTS(
				SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=d.id
				UNION ALL
				SELECT 1 FROM user_device_group_permissions ug
				JOIN device_group_members gm ON gm.device_group_id=ug.device_group_id
				WHERE ug.user_id=? AND gm.device_id=d.id
			)`
			args = append(args, p.UserID, p.UserID)
		}
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			scopes = append(scopes, snmpch.Scope{DeviceID: id})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if len(scopes) != len(deviceIDs) {
			return nil, errSNMPScopeForbidden
		}
	}
	if len(portIDs) > 0 {
		query := `SELECT p.id,p.device_id FROM ports p WHERE p.id IN (` + placeholders(len(portIDs)) + `)`
		args := make([]any, 0, len(portIDs)+3)
		for _, id := range portIDs {
			args = append(args, id)
		}
		if !p.can("device.viewAll") && !p.can("port.viewAll") {
			query += ` AND EXISTS(
				SELECT 1 FROM user_port_permissions upp WHERE upp.user_id=? AND upp.port_id=p.id
				UNION ALL
				SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=p.device_id
				UNION ALL
				SELECT 1 FROM user_device_group_permissions ug
				JOIN device_group_members gm ON gm.device_group_id=ug.device_group_id
				WHERE ug.user_id=? AND gm.device_id=p.device_id
			)`
			args = append(args, p.UserID, p.UserID, p.UserID)
		}
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		portScopes := make([]snmpch.Scope, 0, len(portIDs))
		for rows.Next() {
			var scope snmpch.Scope
			if err := rows.Scan(&scope.PortID, &scope.DeviceID); err != nil {
				rows.Close()
				return nil, err
			}
			portScopes = append(portScopes, scope)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if len(portScopes) != len(portIDs) {
			return nil, errSNMPScopeForbidden
		}
		scopes = append(scopes, portScopes...)
	}
	return scopes, nil
}

func writeSNMPScopeError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errSNMPScopeForbidden):
		fail(c, http.StatusForbidden, "forbidden", err.Error())
	case errors.Is(err, errSNMPScopeNotFound), errors.Is(err, sql.ErrNoRows):
		fail(c, http.StatusNotFound, "not_found", "SNMP device or port not found")
	default:
		writeSQLError(c, err)
	}
}
