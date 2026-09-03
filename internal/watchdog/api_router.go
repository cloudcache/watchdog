package watchdog

import (
	"context"
	"net/http"
)

type APIV1RouterConfig struct {
	Auth            AuthContextAdapter
	TenantDiscovery AuthContextAdapter
	Targets         TargetRepository
	Agents          AgentRepository
	Network         NetworkRepository
	Exports         ExportRepository
	ExportFiles     ExportFileReader
	Billing         BillingRepository
	AggregateGraphs AggregateGraphRepository
	Permissions     PermissionRepository
	Retention       RetentionRepository
	SNMP            SNMPRepository
	Metrics         MetricsService
	SNMPDiscovery   SNMPDeviceDiscoverer
	SNMPCollector   SNMPCollectorRepository
	SeriesCleaner   SeriesCleaner
	DiscoveryJobs   DiscoveryJobRepository
	TrapDispatcher  func(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error)
	Audit           AuditRepository
	AddressSets     AddressSetRepository
	Tenants         TenantRepository
	Readiness       func(context.Context) error
}

type SNMPDeviceDiscoverer interface {
	Discover(ctx context.Context, req SNMPDiscoveryEngineRequest) (SNMPCollectorDiscoveryResult, error)
}

func NewAPIV1Router(cfg APIV1RouterConfig) http.Handler {
	mux := http.NewServeMux()
	auth := AuthMiddleware(cfg.Auth)
	tenantDiscovery := auth
	if cfg.TenantDiscovery != nil {
		tenantDiscovery = AuthMiddleware(cfg.TenantDiscovery)
	}
	mux.Handle("GET /api/v1/health", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		WriteAPIJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}))
	mux.Handle("GET /api/v1/health/live", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		WriteAPIJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}))
	mux.Handle("GET /api/v1/health/ready", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg.Readiness == nil {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Readiness check is not configured", nil)
			return
		}
		if err := cfg.Readiness(r.Context()); err != nil {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Platform backend is not ready", nil)
			return
		}
		WriteAPIJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}))
	mux.Handle("GET /api/v1/me", auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _ := AuthFromContext(r.Context())
		WriteAPIJSON(w, http.StatusOK, user)
	})))
	listTenants := tenantDiscovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, _ := AuthFromContext(r.Context())
		items := identity.AvailableTenants
		if items == nil && cfg.Tenants != nil {
			var err error
			items, err = cfg.Tenants.ListTenantsForUser(r.Context(), identity.UserID)
			if err != nil {
				WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Tenant membership is unavailable", nil)
				return
			}
		}
		if items == nil {
			items = []Tenant{}
		}
		WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.Handle("GET /api/v1/tenants", listTenants)
	mux.Handle("GET /api/v1/me/tenants", listTenants)
	if cfg.Targets != nil {
		registerTargetRoutes(mux, auth, cfg.Targets, cfg.SeriesCleaner, cfg.Network, cfg.DiscoveryJobs)
	}
	if cfg.Network != nil {
		registerNetworkRoutes(mux, auth, cfg.Network, cfg.Targets, cfg.Agents, cfg.SNMP, cfg.SNMPDiscovery, cfg.SNMPCollector, cfg.SeriesCleaner, cfg.DiscoveryJobs, cfg.Audit)
		registerPortRoutes(mux, auth, cfg.Network)
		registerBGPRoutes(mux, auth, cfg.Network)
		registerGraphRoutes(mux, auth, cfg.Network)
	}
	if cfg.Exports != nil {
		registerExportRoutes(mux, auth, cfg.Exports, cfg.ExportFiles, cfg.Network)
	}
	if cfg.Billing != nil {
		registerBillingRoutes(mux, auth, cfg.Billing, cfg.Network, cfg.Metrics)
	}
	if cfg.AggregateGraphs != nil {
		registerAggregateGraphRoutes(mux, auth, cfg.AggregateGraphs, cfg.Network, cfg.Metrics)
	}
	if cfg.Permissions != nil {
		registerPermissionRoutes(mux, auth, cfg.Permissions)
	}
	if cfg.Retention != nil {
		registerRetentionRoutes(mux, auth, cfg.Retention)
	}
	if cfg.SNMP != nil {
		registerSNMPRoutes(mux, auth, cfg.SNMP)
	}
	if cfg.Network != nil && cfg.SNMPCollector != nil && cfg.TrapDispatcher != nil {
		registerTrapRoutes(mux, auth, cfg.Network, cfg.SNMPCollector, cfg.TrapDispatcher, cfg.DiscoveryJobs)
	}
	if cfg.Agents != nil {
		registerAgentRegistryRoutes(mux, auth, cfg.Agents)
		registerAgentRoutes(mux, AgentPlanService{
			Agents:  cfg.Agents,
			Targets: cfg.Targets,
			Network: cfg.Network,
			SNMP:    cfg.SNMP,
			Metrics: cfg.Metrics,
		})
	}
	registerHistoricalRoutes(mux, auth)
	registerMetricsRoutes(mux, auth, cfg.Metrics, cfg.Network)
	if cfg.AddressSets != nil {
		registerAddressSetRoutes(mux, auth, cfg.AddressSets)
	}
	return RequestIDMiddleware(withJSONAPINotFound(mux))
}

func withJSONAPINotFound(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		if pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
			probe := r.Clone(r.Context())
			probe.Method = method
			if _, candidate := mux.Handler(probe); candidate != "" {
				mux.ServeHTTP(w, r)
				return
			}
		}
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "API route not found", nil)
	})
}
