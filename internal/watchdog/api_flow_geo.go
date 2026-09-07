package watchdog

import (
	"errors"
	"net/http"
	"net/netip"
	"strconv"
)

// FlowGeoConfig points the runtime at a flow-geo-v2 bundle directory (usually
// the exporter's `current` symlink). Empty path leaves the routes available but
// returns service_unavailable until a bundle is configured.
type FlowGeoConfig struct {
	Path            string   `yaml:"path"`
	HistoricalPaths []string `yaml:"historical_paths"`
}

// IP-library lookup API (flow-module-design.md §11.1). Reload never accepts a
// request-supplied path: it re-reads the configured bundle only.
func registerFlowGeoRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, service *FlowGeoService) {
	admin := RequirePermission(ActionAdmin, TenantResource)
	mux.Handle("GET /api/v1/flow/geo/catalog", auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		for key, values := range query {
			if key != "level" && key != "parent" && key != "version" && key != "limit" {
				WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "unsupported query parameter: "+key, nil)
				return
			}
			if len(values) != 1 {
				WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "query parameter must appear once: "+key, nil)
				return
			}
		}
		limit := 500
		if raw := query.Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > 5000 {
				WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be 1..5000", nil)
				return
			}
			limit = parsed
		}
		catalog, err := service.Catalog(query.Get("version"), query.Get("level"), query.Get("parent"), limit)
		if err != nil {
			status := http.StatusBadRequest
			code := APIErrorInvalidRequest
			if errors.Is(err, ErrFlowGeoNotLoaded) {
				status, code = http.StatusServiceUnavailable, APIErrorServiceUnavailable
			} else if errors.Is(err, ErrFlowGeoVersionNotFound) || errors.Is(err, ErrFlowGeoNodeNotFound) {
				status, code = http.StatusNotFound, APIErrorNotFound
			}
			WriteAPIError(w, status, code, err.Error(), nil)
			return
		}
		WriteAPIJSON(w, http.StatusOK, catalog)
	})))
	mux.Handle("GET /api/v1/flow/geo/lookup", auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.Query().Get("ip")
		addr, err := netip.ParseAddr(raw)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "a valid ip parameter is required", nil)
			return
		}
		if !service.Status().Loaded {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Geo bundle is not loaded", nil)
			return
		}
		info, found := service.Lookup(addr)
		WriteAPIJSON(w, http.StatusOK, map[string]any{
			"ip":    addr.String(),
			"found": found,
			"geo":   info,
		})
	})))
	mux.Handle("GET /api/v1/flow/geo/status", auth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		WriteAPIJSON(w, http.StatusOK, service.Status())
	})))
	mux.Handle("POST /api/v1/flow/geo/reload", auth(admin(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := service.Reload(); err != nil {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, err.Error(), nil)
			return
		}
		WriteAPIJSON(w, http.StatusOK, service.Status())
	}))))
}
