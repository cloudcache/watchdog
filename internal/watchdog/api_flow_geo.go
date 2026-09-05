package watchdog

import (
	"net/http"
	"net/netip"
)

// FlowGeoConfig points the runtime at a flow-geo-v1 bundle directory (usually
// the exporter's `current` symlink). Empty path disables the service.
type FlowGeoConfig struct {
	Path string `yaml:"path"`
}

// IP-library lookup API (flow-module-design.md §11.1). Reload never accepts a
// request-supplied path: it re-reads the configured bundle only.
func registerFlowGeoRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, service *FlowGeoService) {
	admin := RequirePermission(ActionAdmin, TenantResource)
	mux.Handle("GET /api/v1/flow/geo/lookup", auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.Query().Get("ip")
		addr, err := netip.ParseAddr(raw)
		if err != nil {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "a valid ip parameter is required", nil)
			return
		}
		index := service.Index()
		if index == nil {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Geo bundle is not loaded", nil)
			return
		}
		info, found := index.Lookup(addr)
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
