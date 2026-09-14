package server

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/gin-gonic/gin"
)

var (
	vmSelectorPattern = regexp.MustCompile(`^\s*([A-Za-z_:][A-Za-z0-9_:]*)(?:\{(.*)\})?\s*$`)
	vmMatcherPattern  = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"([^"\\]*)"$`)
)

type vmQuerySelector struct {
	Metric, DeviceID, PortID string
}

func (s *Server) vmQueryMetrics(c *gin.Context) {
	principal := currentPrincipal(c)
	if principal == nil || !principal.IsAdmin {
		fail(c, http.StatusForbidden, "forbidden", "admin permission required")
		return
	}
	selector, err := parseVMQuerySelector(c.Query("query"))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if !isSNMPMetric(selector.Metric) {
		fail(c, http.StatusBadRequest, "invalid_metric", "unsupported SNMP metric")
		return
	}
	if !s.requireMetricAccess(c, selector.Metric) {
		return
	}
	if selector.PortID != "" {
		port, err := s.readPort(c, selector.PortID)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		if selector.DeviceID != "" && selector.DeviceID != port.DeviceID {
			fail(c, http.StatusBadRequest, "invalid_resource", "port does not belong to device")
			return
		}
		selector.DeviceID = port.DeviceID
	}
	if selector.DeviceID == "" {
		fail(c, http.StatusBadRequest, "invalid_resource", "query must select device_id, target_id or port_id")
		return
	}
	if !s.requireDeviceAccess(c, selector.DeviceID) {
		return
	}
	if s.snmpMetrics == nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_unavailable", "SNMP ClickHouse store is not configured")
		return
	}
	from := parseVMQueryTime(c.Query("start"), time.Now().UTC().Add(-time.Hour))
	to := parseVMQueryTime(c.Query("end"), time.Now().UTC())
	if !to.After(from) || to.Sub(from) > 400*24*time.Hour {
		fail(c, http.StatusBadRequest, "invalid_range", "range must be positive and at most 400 days")
		return
	}
	step := time.Minute
	if raw := strings.TrimSpace(c.Query("step")); raw != "" {
		seconds, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || seconds == 0 {
			fail(c, http.StatusBadRequest, "invalid_range", "step must be positive seconds")
			return
		}
		step = time.Duration(seconds) * time.Second
	}
	if step < time.Second || step > 24*time.Hour {
		fail(c, http.StatusBadRequest, "invalid_range", "step must be between 1 second and 24 hours")
		return
	}
	series, err := s.snmpMetrics.Query(c.Request.Context(), snmpch.QueryRequest{
		DeviceID: selector.DeviceID, EntityID: selector.PortID, Metric: selector.Metric,
		From: from, To: to, Step: step, MaxRows: 250000,
	})
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_query_failed", err.Error())
		return
	}
	response := metricRangeResponse{Status: "success"}
	response.Data.ResultType = "matrix"
	for _, item := range series {
		labels := map[string]string{
			"__name__": selector.Metric, "target_id": item.DeviceID, "device_id": item.DeviceID,
			"entity_type": item.EntityKind, "entity_id": item.EntityID,
		}
		if item.EntityKind == "port" {
			labels["port_id"] = item.EntityID
		}
		values := make([]metricValue, 0, len(item.Points))
		for _, point := range item.Points {
			values = append(values, metricValue{Time: point.Time, Value: point.Value})
		}
		response.Data.Result = append(response.Data.Result, metricRangeQueryItem{Metric: labels, Values: values})
	}
	s.audit(c.Request.Context(), principal.UserID, "metrics.vmquery.sensitive_viewed", "dataset", "clickhouse.snmp_samples")
	c.JSON(http.StatusOK, response)
}

func parseVMQuerySelector(raw string) (vmQuerySelector, error) {
	if strings.TrimSpace(raw) == "" {
		return vmQuerySelector{}, errors.New("query is required")
	}
	match := vmSelectorPattern.FindStringSubmatch(raw)
	if match == nil {
		return vmQuerySelector{}, errors.New("unsupported vmquery expression; use a metric selector with exact label matchers")
	}
	selector := vmQuerySelector{Metric: match[1]}
	if strings.TrimSpace(match[2]) == "" {
		return selector, nil
	}
	seen := map[string]bool{}
	for _, rawMatcher := range strings.Split(match[2], ",") {
		matcher := vmMatcherPattern.FindStringSubmatch(strings.TrimSpace(rawMatcher))
		if matcher == nil {
			return vmQuerySelector{}, errors.New("vmquery supports exact label matchers only")
		}
		key, value := matcher[1], matcher[2]
		if seen[key] {
			return vmQuerySelector{}, errors.New("duplicate vmquery label matcher")
		}
		seen[key] = true
		switch key {
		case "device_id", "target_id":
			if selector.DeviceID != "" && selector.DeviceID != value {
				return vmQuerySelector{}, errors.New("target_id and device_id must refer to the same device")
			}
			selector.DeviceID = value
		case "port_id", "entity_id":
			selector.PortID = value
		case "tenant_id", "entity_type":
			// Historical labels are accepted for compatibility. tenant_id is no
			// longer a storage or authorization dimension; entity_type is derived.
		default:
			return vmQuerySelector{}, errors.New("unsupported vmquery label: " + key)
		}
	}
	return selector, nil
}

func parseVMQueryTime(raw string, fallback time.Time) time.Time {
	value, err := time.Parse(time.RFC3339, strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return value.UTC()
}
