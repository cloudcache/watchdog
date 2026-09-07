package watchdog

import (
	"errors"
	"fmt"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

var (
	ErrFlowGeoNotLoaded       = errors.New("Geo bundle is not loaded")
	ErrFlowGeoVersionNotFound = errors.New("Geo bundle version was not found")
	ErrFlowGeoNodeNotFound    = errors.New("Geo parent node was not found")
)

// PLAT-04D: the hub no longer maintains its own flow-geo-v1 loader. Geo lookups
// delegate to the Flow data-plane's authoritative flowdimension.GeoCatalog, so
// validation, hierarchy and version semantics have a single implementation.
// This file is a thin API-facing adapter over that catalog.

// FlowGeoService wraps a flowdimension.GeoCatalog for the hub's /flow/geo API.
// A failed reload keeps the previously loaded index serving; the last error is
// surfaced through Status.
type FlowGeoService struct {
	Path    string
	catalog *flowdimension.GeoCatalog
	lastErr atomic.Pointer[string]
}

func NewFlowGeoService(path string) *FlowGeoService {
	return &FlowGeoService{Path: path, catalog: flowdimension.NewGeoCatalog()}
}

// Reload re-reads the configured bundle into the catalog. The path is never
// request-supplied.
func (s *FlowGeoService) Reload() error {
	if _, err := s.catalog.Reload(s.Path, flowdimension.GeoLoadLimits{}); err != nil {
		message := err.Error()
		s.lastErr.Store(&message)
		return err
	}
	s.lastErr.Store(nil)
	return nil
}

// LoadHistorical installs a validated bundle for historical result labels
// without switching the active lookup version.
func (s *FlowGeoService) LoadHistorical(path string) error {
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
func (s *FlowGeoService) Lookup(addr netip.Addr) (flowdimension.GeoInfo, bool) {
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
func (s *FlowGeoService) Label(version, code string) (FlowGeoLabel, bool) {
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

// Catalog lists one exact hierarchy level from one immutable publication.
// Empty version selects the active publication; it never merges versions.
func (s *FlowGeoService) Catalog(version, level, parentID string, maximum int) (FlowGeoCatalogResult, error) {
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

func (s *FlowGeoService) Status() FlowGeoStatus {
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
