package server

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/gin-gonic/gin"
)

func (s *Server) metricCatalog(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"items": watchdog.MetricCatalog})
}

// metricRangeResponse intentionally keeps the JSON contract consumed by the
// existing charts while keeping the active API independent of the retired
// metrics transport types.
type metricRangeResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string                 `json:"resultType"`
		Result     []metricRangeQueryItem `json:"result"`
	} `json:"data"`
}

type metricRangeQueryItem struct {
	Metric map[string]string `json:"metric"`
	Values []metricValue     `json:"values"`
}

type metricValue struct {
	Time  time.Time
	Value float64
}

func (s *Server) queryMetrics(c *gin.Context) {
	allowed := map[string]bool{
		"target_id": true, "device_id": true, "port_id": true, "metric": true,
		"time_mode": true, "window": true, "start": true, "end": true, "step": true,
		"max_data_points": true, "value_mode": true, "traffic_view": true,
		"per_port": true, "collection_step": true, "func": true,
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
	deviceID := strings.TrimSpace(c.Query("device_id"))
	targetID := strings.TrimSpace(c.Query("target_id"))
	portID := strings.TrimSpace(c.Query("port_id"))
	if deviceID == "" {
		deviceID = targetID
	}
	if targetID != "" && deviceID != targetID {
		fail(c, http.StatusBadRequest, "invalid_resource", "target_id and device_id must refer to the same device")
		return
	}
	if portID != "" {
		port, err := s.readPort(c, portID)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		if deviceID != "" && port.DeviceID != deviceID {
			fail(c, http.StatusBadRequest, "invalid_resource", "port does not belong to device")
			return
		}
		deviceID = port.DeviceID
		if _, ok := s.portScope(c, deviceID, portID); !ok {
			return
		}
	}
	if deviceID == "" {
		fail(c, http.StatusBadRequest, "invalid_resource", "target_id, device_id or port_id is required")
		return
	}
	if !s.requireDeviceAccess(c, deviceID) {
		return
	}
	from, to, step, ok := parseMetricWindow(c)
	if !ok {
		return
	}
	maxRows := uint32(250000)
	if raw := strings.TrimSpace(c.Query("max_data_points")); raw != "" {
		n, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || n == 0 || n > 250000 {
			fail(c, http.StatusBadRequest, "invalid_range", "max_data_points must be 1..250000")
			return
		}
		maxRows = uint32(n)
		if c.Query("per_port") == "1" {
			maxRows = 250000
		}
	}
	if s.snmpMetrics == nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_unavailable", "SNMP ClickHouse store is not configured")
		return
	}
	series, err := s.snmpMetrics.Query(c.Request.Context(), snmpch.QueryRequest{DeviceID: deviceID, EntityID: portID, Metric: metric, From: from, To: to, Step: step, MaxRows: maxRows})
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_query_failed", err.Error())
		return
	}
	response := metricRangeResponse{Status: "success"}
	response.Data.ResultType = "matrix"
	for _, item := range series {
		labels := map[string]string{"__name__": metric, "device_id": item.DeviceID, "entity_type": item.EntityKind, "entity_id": item.EntityID}
		if item.EntityKind == "port" {
			labels["port_id"] = item.EntityID
		}
		values := make([]metricValue, 0, len(item.Points))
		for _, point := range item.Points {
			values = append(values, metricValue{Time: point.Time, Value: point.Value})
		}
		response.Data.Result = append(response.Data.Result, metricRangeQueryItem{Metric: labels, Values: values})
	}
	c.JSON(http.StatusOK, response)
}

func isSNMPMetric(metric string) bool {
	for _, definition := range watchdog.MetricCatalog {
		if definition.Name == metric && (strings.HasPrefix(definition.Family, "snmp_") || definition.Family == "bgp") {
			return true
		}
	}
	return false
}

func parseMetricWindow(c *gin.Context) (time.Time, time.Time, time.Duration, bool) {
	now := time.Now().UTC()
	mode := strings.TrimSpace(c.Query("time_mode"))
	if mode == "" {
		mode = "fixed"
	}
	var from, to time.Time
	switch mode {
	case "realtime":
		to = now
		from = now.Add(-10 * time.Minute)
	case "fixed":
		duration, ok := metricWindowDuration(c.Query("window"))
		if !ok {
			fail(c, http.StatusBadRequest, "invalid_range", "unsupported fixed window")
			return from, to, 0, false
		}
		to = now
		from = now.Add(-duration)
	case "custom":
		var err error
		from, err = time.Parse(time.RFC3339, c.Query("start"))
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_range", "invalid start")
			return from, to, 0, false
		}
		to, err = time.Parse(time.RFC3339, c.Query("end"))
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_range", "invalid end")
			return from, to, 0, false
		}
	default:
		fail(c, http.StatusBadRequest, "invalid_range", "unsupported time_mode")
		return from, to, 0, false
	}
	if !to.After(from) || to.Sub(from) > 400*24*time.Hour {
		fail(c, http.StatusBadRequest, "invalid_range", "range must be positive and at most 400 days")
		return from, to, 0, false
	}
	step := watchdog.AutoQueryStep(to.Sub(from), 1200)
	if raw := strings.TrimSpace(c.Query("step")); raw != "" {
		seconds, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || seconds == 0 {
			fail(c, http.StatusBadRequest, "invalid_range", "step must be positive seconds")
			return from, to, 0, false
		}
		step = time.Duration(seconds) * time.Second
	}
	if step < time.Second || step > 24*time.Hour {
		fail(c, http.StatusBadRequest, "invalid_range", "step must be between 1 second and 24 hours")
		return from, to, 0, false
	}
	return from.UTC(), to.UTC(), step, true
}

func metricWindowDuration(raw string) (time.Duration, bool) {
	switch raw {
	case "", "1h":
		return time.Hour, true
	case "5m":
		return 5 * time.Minute, true
	case "10m":
		return 10 * time.Minute, true
	case "15m":
		return 15 * time.Minute, true
	case "30m":
		return 30 * time.Minute, true
	case "24h":
		return 24 * time.Hour, true
	case "7d":
		return 7 * 24 * time.Hour, true
	case "30d":
		return 30 * 24 * time.Hour, true
	}
	return 0, false
}
