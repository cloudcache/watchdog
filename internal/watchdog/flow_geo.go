package watchdog

import (
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
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
