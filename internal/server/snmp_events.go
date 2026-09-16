package server

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/snmpdomain"
	"github.com/gin-gonic/gin"
)

func (s *Server) listDeviceEvents(c *gin.Context) {
	deviceID := c.Param("id")
	if _, ok := s.loadScopedDevice(c, deviceID); !ok {
		return
	}
	if s.snmpMetrics == nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_unavailable", "SNMP ClickHouse store is not configured")
		return
	}
	query, err := parseSNMPEventTableQuery(c.Request.URL.Query())
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_filter", err.Error())
		return
	}
	query.DeviceID = deviceID
	items, total, err := s.snmpMetrics.QueryEvents(c.Request.Context(), query)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_query_failed", err.Error())
		return
	}
	result := make([]snmpdomain.Event, 0, len(items))
	for _, item := range items {
		result = append(result, snmpdomain.Event{
			ID: item.ID, DeviceID: item.DeviceID,
			EntityType: snmpdomain.EntityType(item.EntityType), EntityID: item.EntityID,
			Source: item.Source, Severity: item.Severity, EventType: item.EventType,
			Message: item.Message, Raw: item.Raw, OccurredAt: item.OccurredAt, CreatedAt: item.IngestedAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"items": result, "total": total, "limit": query.Limit, "offset": query.Offset,
		"meta": gin.H{"sort": strings.ToLower(query.SortBy + ":" + query.SortDirection)},
	})
}

func (s *Server) listDeviceEventFacets(c *gin.Context) {
	deviceID := c.Param("id")
	if _, ok := s.loadScopedDevice(c, deviceID); !ok {
		return
	}
	if s.snmpMetrics == nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_unavailable", "SNMP ClickHouse store is not configured")
		return
	}
	query, err := parseSNMPEventFacetQuery(c.Request.URL.Query())
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_filter", err.Error())
		return
	}
	query.DeviceID = deviceID
	items, err := s.snmpMetrics.QueryEventFacets(c.Request.Context(), query)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_query_failed", err.Error())
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": len(items), "field": query.Field})
}

func parseSNMPEventTableQuery(values url.Values) (snmpch.EventQuery, error) {
	allowed := map[string]bool{
		"q": true, "sort": true, "order": true, "limit": true, "offset": true,
		"filter.severity": true, "filter.event_type": true, "filter.source": true,
	}
	for key := range values {
		if !allowed[key] {
			return snmpch.EventQuery{}, fmt.Errorf("unknown query parameter %q", key)
		}
	}
	query := snmpch.EventQuery{
		Search: strings.TrimSpace(values.Get("q")), SortBy: strings.TrimSpace(values.Get("sort")),
		SortDirection: strings.ToUpper(strings.TrimSpace(values.Get("order"))), Limit: 25,
	}
	if query.SortBy == "" {
		query.SortBy = "occurred_at"
	}
	if query.SortDirection == "" {
		query.SortDirection = "DESC"
	}
	var err error
	if query.Limit, err = parseBoundedInteger(values.Get("limit"), 25, 1, 100); err != nil {
		return snmpch.EventQuery{}, errors.New("limit must be between 1 and 100")
	}
	if query.Offset, err = parseBoundedInteger(values.Get("offset"), 0, 0, 100000); err != nil {
		return snmpch.EventQuery{}, errors.New("offset must be between 0 and 100000")
	}
	if query.Severities, err = parseSNMPEventValues(values.Get("filter.severity"), "filter.severity"); err != nil {
		return snmpch.EventQuery{}, err
	}
	if query.EventTypes, err = parseSNMPEventValues(values.Get("filter.event_type"), "filter.event_type"); err != nil {
		return snmpch.EventQuery{}, err
	}
	if query.Sources, err = parseSNMPEventValues(values.Get("filter.source"), "filter.source"); err != nil {
		return snmpch.EventQuery{}, err
	}
	if len(query.Search) > 256 {
		return snmpch.EventQuery{}, errors.New("q must not exceed 256 characters")
	}
	return query, nil
}

func parseSNMPEventFacetQuery(values url.Values) (snmpch.EventFacetQuery, error) {
	allowed := map[string]bool{
		"field": true, "q": true, "search": true, "limit": true,
		"filter.severity": true, "filter.event_type": true, "filter.source": true,
	}
	for key := range values {
		if !allowed[key] {
			return snmpch.EventFacetQuery{}, fmt.Errorf("unknown query parameter %q", key)
		}
	}
	base := url.Values{
		"q": {values.Get("search")}, "limit": {values.Get("limit")},
		"sort": {"occurred_at"}, "order": {"desc"}, "offset": {"0"},
	}
	for _, key := range []string{"filter.severity", "filter.event_type", "filter.source"} {
		if value := values.Get(key); value != "" {
			base.Set(key, value)
		}
	}
	query, err := parseSNMPEventTableQuery(base)
	if err != nil {
		return snmpch.EventFacetQuery{}, err
	}
	facet := snmpch.EventFacetQuery{EventQuery: query, Field: strings.TrimSpace(values.Get("field")), FacetSearch: strings.TrimSpace(values.Get("q"))}
	if facet.Field != "severity" && facet.Field != "event_type" && facet.Field != "source" {
		return snmpch.EventFacetQuery{}, errors.New("field must be severity, event_type or source")
	}
	if len(facet.FacetSearch) > 96 {
		return snmpch.EventFacetQuery{}, errors.New("q must not exceed 96 characters")
	}
	return facet, nil
}

func parseSNMPEventValues(raw, field string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	seen := make(map[string]bool)
	result := make([]string, 0, 4)
	for _, rawValue := range strings.Split(raw, ",") {
		value := strings.TrimSpace(rawValue)
		if value == "" || len(value) > 96 {
			return nil, fmt.Errorf("%s values must contain 1..96 characters", field)
		}
		if seen[value] {
			continue
		}
		seen[value] = true
		result = append(result, value)
		if len(result) > 8 {
			return nil, fmt.Errorf("%s accepts at most 8 values", field)
		}
	}
	return result, nil
}

func parseBoundedInteger(raw string, fallback, minimum, maximum int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, errors.New("integer is outside the allowed range")
	}
	return value, nil
}
