// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/netip"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// FlowGeoConfig points the runtime at a flow-geo-v2 bundle directory (usually the
// exporter's `current` symlink). An empty path leaves the routes available but
// returns service_unavailable until a bundle is configured.
type FlowGeoConfig struct {
	Path            string   `yaml:"path"`
	HistoricalPaths []string `yaml:"historical_paths"`
}

var (
	ErrFlowGeoNotLoaded       = errors.New("Geo bundle is not loaded")
	ErrFlowGeoVersionNotFound = errors.New("Geo bundle version was not found")
	ErrFlowGeoNodeNotFound    = errors.New("Geo parent node was not found")
)

// flowGeoService wraps the Flow data-plane's authoritative flowdimension.GeoCatalog
// for the v2 /flow/geo API, so validation, hierarchy and version semantics have a
// single implementation (ported verbatim from the retired hub adapter, KISS-06).
// A failed reload keeps the previously loaded index serving; the last error is
// surfaced through Status.
type flowGeoService struct {
	Path    string
	catalog *flowdimension.GeoCatalog
	lastErr atomic.Pointer[string]
}

func newFlowGeoService(path string) *flowGeoService {
	return &flowGeoService{Path: path, catalog: flowdimension.NewGeoCatalog()}
}

// startFlowGeo constructs the geo service and best-effort loads the configured
// bundle. A failed active-bundle load is non-fatal (the routes stay up and the
// error is surfaced through Status); a configured historical bundle that fails
// to load is fatal, since historical result labels would then be incomplete.
func (s *Server) startFlowGeo() error {
	geo := newFlowGeoService(s.cfg.Flow.Geo.Path)
	if s.cfg.Flow.Geo.Path != "" {
		if err := geo.Reload(); err != nil {
			log.Printf("watchdog flow geo bundle load failed (serving without geo until reload): %v", err)
		} else {
			for _, path := range s.cfg.Flow.Geo.HistoricalPaths {
				if err := geo.LoadHistorical(path); err != nil {
					return fmt.Errorf("load historical Flow Geo bundle %q: %w", path, err)
				}
			}
			status := geo.Status()
			log.Printf("watchdog flow geo bundle loaded version=%s v4=%d v6=%d", status.Version, status.RowsV4, status.RowsV6)
		}
	}
	s.flowGeo = geo
	return nil
}

// Reload re-reads the configured bundle into the catalog. The path is never
// request-supplied.
func (s *flowGeoService) Reload() error {
	if _, err := s.catalog.Reload(s.Path, flowdimension.GeoLoadLimits{}); err != nil {
		message := err.Error()
		s.lastErr.Store(&message)
		return err
	}
	s.lastErr.Store(nil)
	return nil
}

// LoadHistorical installs a validated bundle for historical result labels without
// switching the active lookup version.
func (s *flowGeoService) LoadHistorical(path string) error {
	if s == nil || s.catalog == nil {
		return ErrFlowGeoNotLoaded
	}
	if _, err := s.catalog.LoadHistorical(path, flowdimension.GeoLoadLimits{}); err != nil {
		return err
	}
	return nil
}

// Lookup resolves an address against the active index. found is false when no
// index is loaded or the address is not covered.
func (s *flowGeoService) Lookup(addr netip.Addr) (flowdimension.GeoInfo, bool) {
	if s == nil {
		return flowdimension.GeoInfo{}, false
	}
	index, ok := s.catalog.Active()
	if !ok || index == nil {
		return flowdimension.GeoInfo{}, false
	}
	return index.Lookup(addr)
}

// FlowGeoStatus reports whether an index is loaded and its provenance.
type FlowGeoStatus struct {
	Path        string    `json:"path"`
	Loaded      bool      `json:"loaded"`
	LastError   string    `json:"last_error,omitempty"`
	Version     string    `json:"version,omitempty"`
	GeneratedAt time.Time `json:"generated_at,omitzero"`
	RowsV4      uint64    `json:"rows_v4"`
	RowsV6      uint64    `json:"rows_v6"`
	Operators   uint64    `json:"operators"`
}

type FlowGeoLabel struct {
	Code       string            `json:"code"`
	Name       string            `json:"name"`
	Kind       string            `json:"kind"`
	ParentID   string            `json:"parent_id,omitempty"`
	Path       []FlowGeoPathNode `json:"path"`
	Breadcrumb []string          `json:"breadcrumb"`
	Additive   bool              `json:"additive"`
	Version    string            `json:"version"`
}

type FlowGeoPathNode struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Kind string `json:"kind"`
}

type FlowGeoCatalogItem struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Kind     string            `json:"kind"`
	ParentID string            `json:"parent_id,omitempty"`
	Path     []FlowGeoPathNode `json:"path"`
	Additive bool              `json:"additive"`
}

type FlowGeoCatalogResult struct {
	Version       string               `json:"version"`
	EffectiveFrom time.Time            `json:"effective_from"`
	Level         string               `json:"level"`
	ParentID      string               `json:"parent_id,omitempty"`
	Items         []FlowGeoCatalogItem `json:"items"`
	Total         int                  `json:"total"`
}

// Label resolves display metadata from the immutable dictionary version that
// classified the Flow row. It never falls back to the active version because
// doing so would silently relabel historical traffic after a publication.
func (s *flowGeoService) Label(version, code string) (FlowGeoLabel, bool) {
	if s == nil || s.catalog == nil || version == "" || code == "" {
		return FlowGeoLabel{}, false
	}
	index, ok := s.catalog.Get(version)
	if !ok || index == nil {
		return FlowGeoLabel{}, false
	}
	entries, ok := index.GeoBreadcrumb(code)
	if !ok || len(entries) == 0 {
		return FlowGeoLabel{}, false
	}
	label := flowGeoLabel(version, entries)
	return label, true
}

// Catalog lists one exact hierarchy level from one immutable publication. An
// empty version selects the active publication; it never merges versions.
func (s *flowGeoService) Catalog(version, level, parentID string, maximum int) (FlowGeoCatalogResult, error) {
	if s == nil || s.catalog == nil {
		return FlowGeoCatalogResult{}, ErrFlowGeoNotLoaded
	}
	var index *flowdimension.GeoIndex
	var ok bool
	if version == "" {
		index, ok = s.catalog.Active()
	} else {
		index, ok = s.catalog.Get(version)
		if !ok {
			return FlowGeoCatalogResult{}, fmt.Errorf("%w: %s", ErrFlowGeoVersionNotFound, version)
		}
	}
	if !ok || index == nil {
		return FlowGeoCatalogResult{}, ErrFlowGeoNotLoaded
	}
	nodes, err := index.GeoNodes(level, parentID, maximum)
	if err != nil {
		if parentID != "" {
			if _, exists := index.GeoNode(parentID); !exists {
				return FlowGeoCatalogResult{}, fmt.Errorf("%w: %s", ErrFlowGeoNodeNotFound, parentID)
			}
		}
		return FlowGeoCatalogResult{}, err
	}
	metadata := index.Metadata()
	result := FlowGeoCatalogResult{
		Version: metadata.Version, EffectiveFrom: metadata.EffectiveFrom, Level: level, ParentID: parentID,
		Items: make([]FlowGeoCatalogItem, 0, len(nodes)), Total: len(nodes),
	}
	for _, node := range nodes {
		breadcrumb, exists := index.GeoBreadcrumb(node.Code)
		if !exists {
			return FlowGeoCatalogResult{}, fmt.Errorf("Geo path is unavailable for %s", node.Code)
		}
		label := flowGeoLabel(metadata.Version, breadcrumb)
		result.Items = append(result.Items, FlowGeoCatalogItem{
			ID: label.Code, Name: label.Name, Kind: label.Kind, ParentID: label.ParentID,
			Path: label.Path, Additive: true,
		})
	}
	return result, nil
}

func flowGeoLabel(version string, entries []flowdimension.GeoDictionaryEntry) FlowGeoLabel {
	path := make([]FlowGeoPathNode, 0, len(entries))
	breadcrumb := make([]string, 0, len(entries))
	for _, entry := range entries {
		path = append(path, FlowGeoPathNode{ID: entry.Code, Name: entry.Name, Kind: entry.Kind})
		breadcrumb = append(breadcrumb, entry.Name)
	}
	current := entries[len(entries)-1]
	return FlowGeoLabel{
		Code: current.Code, Name: current.Name, Kind: current.Kind, ParentID: current.ParentCode,
		Path: path, Breadcrumb: breadcrumb, Additive: true, Version: version,
	}
}

func (s *flowGeoService) Status() FlowGeoStatus {
	status := FlowGeoStatus{Path: s.Path}
	if message := s.lastErr.Load(); message != nil {
		status.LastError = *message
	}
	index, ok := s.catalog.Active()
	if !ok || index == nil {
		return status
	}
	metadata := index.Metadata()
	status.Loaded = true
	status.Version = metadata.Version
	status.GeneratedAt = metadata.GeneratedAt
	status.RowsV4 = metadata.IPv4Rows
	status.RowsV6 = metadata.IPv6Rows
	status.Operators = metadata.OperatorRows
	return status
}

// registerFlowGeoRoutes wires the IP-library lookup API (flow-module-design.md
// §11.1). Reload never accepts a request-supplied path: it re-reads the
// configured bundle only, so it is gated on flow management rather than view.
func (s *Server) registerFlowGeoRoutes(auth *gin.RouterGroup, view gin.HandlerFunc) {
	geo := auth.Group("/flow/geo")
	geo.GET("/catalog", view, s.flowGeoCatalog)
	geo.GET("/lookup", view, s.flowGeoLookup)
	geo.GET("/status", view, s.flowGeoStatus)
	geo.POST("/reload", s.requirePermission("flow.device.manage"), s.flowGeoReload)
}

// flowGeoCatalog lists one hierarchy level of one geo publication.
func (s *Server) flowGeoCatalog(c *gin.Context) {
	if s.flowGeo == nil {
		fail(c, http.StatusServiceUnavailable, "service_unavailable", "Geo bundle is not loaded")
		return
	}
	query := c.Request.URL.Query()
	for key, values := range query {
		if key != "level" && key != "parent" && key != "version" && key != "limit" {
			fail(c, http.StatusBadRequest, "invalid_request", "unsupported query parameter: "+key)
			return
		}
		if len(values) != 1 {
			fail(c, http.StatusBadRequest, "invalid_request", "query parameter must appear once: "+key)
			return
		}
	}
	limit := 500
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 5000 {
			fail(c, http.StatusBadRequest, "invalid_request", "limit must be 1..5000")
			return
		}
		limit = parsed
	}
	catalog, err := s.flowGeo.Catalog(query.Get("version"), query.Get("level"), query.Get("parent"), limit)
	if err != nil {
		status, code := http.StatusBadRequest, "invalid_request"
		if errors.Is(err, ErrFlowGeoNotLoaded) {
			status, code = http.StatusServiceUnavailable, "service_unavailable"
		} else if errors.Is(err, ErrFlowGeoVersionNotFound) || errors.Is(err, ErrFlowGeoNodeNotFound) {
			status, code = http.StatusNotFound, "not_found"
		}
		fail(c, status, code, err.Error())
		return
	}
	c.JSON(http.StatusOK, catalog)
}

// flowGeoLookup resolves a single IP against the active geo index.
func (s *Server) flowGeoLookup(c *gin.Context) {
	if s.flowGeo == nil {
		fail(c, http.StatusServiceUnavailable, "service_unavailable", "Geo bundle is not loaded")
		return
	}
	addr, err := netip.ParseAddr(c.Query("ip"))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "a valid ip parameter is required")
		return
	}
	if !s.flowGeo.Status().Loaded {
		fail(c, http.StatusServiceUnavailable, "service_unavailable", "Geo bundle is not loaded")
		return
	}
	info, found := s.flowGeo.Lookup(addr)
	c.JSON(http.StatusOK, gin.H{"ip": addr.String(), "found": found, "geo": info})
}

// flowGeoStatus reports the active bundle's provenance and load state.
func (s *Server) flowGeoStatus(c *gin.Context) {
	if s.flowGeo == nil {
		c.JSON(http.StatusOK, FlowGeoStatus{})
		return
	}
	c.JSON(http.StatusOK, s.flowGeo.Status())
}

// flowGeoReload re-reads the server-configured bundle. The path is never
// request-supplied.
func (s *Server) flowGeoReload(c *gin.Context) {
	if s.flowGeo == nil {
		fail(c, http.StatusServiceUnavailable, "service_unavailable", "Geo bundle is not loaded")
		return
	}
	if err := s.flowGeo.Reload(); err != nil {
		fail(c, http.StatusServiceUnavailable, "service_unavailable", err.Error())
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow.geo.reload", "flow_geo", s.flowGeo.Path)
	c.JSON(http.StatusOK, s.flowGeo.Status())
}

// flowOverseasGeoLabels resolves human labels for the geo codes in an overseas
// result, keyed "<geo_version>:<geo_value>", using the immutable version that
// classified each row. An unloaded geo service yields an empty map.
func (s *Server) flowOverseasGeoLabels(result flowquery.OverseasResult) map[string]FlowGeoLabel {
	labels := make(map[string]FlowGeoLabel)
	if s.flowGeo == nil {
		return labels
	}
	for _, point := range result.Points {
		if point.Kind != flowquery.OverseasRowGeo || point.GeoValue == "" || point.Other {
			continue
		}
		key := point.GeoVersion + ":" + point.GeoValue
		if _, exists := labels[key]; exists {
			continue
		}
		if label, ok := s.flowGeo.Label(point.GeoVersion, point.GeoValue); ok {
			labels[key] = label
		}
	}
	return labels
}
