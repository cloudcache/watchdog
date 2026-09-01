package watchdog

import (
	"context"
	"net/http"
)

type APIV1RouterConfig struct {
	Auth            AuthContextAdapter
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
}

type SNMPDeviceDiscoverer interface {
	Discover(ctx context.Context, req SNMPDiscoveryEngineRequest) (SNMPCollectorDiscoveryResult, error)
}

func NewAPIV1Router(cfg APIV1RouterConfig) http.Handler {
	mux := http.NewServeMux()
	auth := AuthMiddleware(cfg.Auth)
	mux.Handle("GET /api/v1/health", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		WriteAPIJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}))
	mux.Handle("GET /api/v1/me", auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _ := AuthFromContext(r.Context())
		WriteAPIJSON(w, http.StatusOK, user)
	})))
	mux.Handle("GET /api/v1/tenants", auth(RequirePermission(ActionView, TenantResource)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		WriteAPIJSON(w, http.StatusOK, map[string]any{"items": []Tenant{}})
	}))))
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
	return mux
}
