package watchdog

import (
	"context"
	"net/http"
	"time"
)

type APIV1RouterConfig struct {
	Auth                   AuthContextAdapter
	TenantDiscovery        AuthContextAdapter
	Targets                TargetRepository
	Agents                 AgentRepository
	Network                NetworkRepository
	Exports                ExportRepository
	ExportFiles            ExportFileReader
	ExportMetric           string
	ExportCollectionStep   time.Duration
	Billing                BillingRepository
	AggregateGraphs        AggregateGraphRepository
	Dashboards             DashboardRepository
	Permissions            PermissionRepository
	IdentityAdmin          IdentityAdminRepository
	Idempotency            IdempotencyRepository
	TargetDeletePreview    TargetDeletePreviewRepository
	DeviceDeletePreview    DeviceDeletePreviewRepository
	PortDeletePreview      PortDeletePreviewRepository
	CollectorDeletePreview CollectorDeletePreviewRepository
	UserPreferences        UserPreferencesRepository
	NotificationChannels   NotificationChannelRepository
	QuietHours             QuietHoursRepository
	AlertsHistory          AlertHistoryRepository
	CollectorCredentials   CollectorCredentialRepository
	CollectorEnrollment    CollectorEnrollmentRepository
	OperationJobs          OperationJobRepository
	OperationJobSchedules  OperationJobScheduleRepository
	QueryGateway           *QueryGateway
	FlowRecords            flowDetailRunner
	FlowRecordNow          func() time.Time
	FlowOverseas           flowOverseasRunner
	FlowOverseasNow        func() time.Time
	FlowStorageQuery       FlowStorageArchiveBoundaryRepository
	FlowVPNFindings        VPNFindingRepository
	FlowStorage            FlowStorageLifecycleRepository
	QueryPolicies          QueryDatasetPolicyRepository
	AuditLogs              AuditLogReader
	Registries             *PlatformRegistries
	TenantModules          TenantModuleRepository
	FlowGeo                *FlowGeoService
	Retention              RetentionRepository
	SNMP                   SNMPRepository
	Metrics                MetricsService
	SNMPDiscovery          SNMPDeviceDiscoverer
	SNMPCollector          SNMPCollectorRepository
	SeriesCleaner          SeriesCleaner
	DiscoveryJobs          DiscoveryJobRepository
	TrapDispatcher         func(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error)
	Audit                  AuditRepository
	AddressSets            AddressSetRepository
	AddressTaxonomy        AddressTaxonomyRepository
	AddressImports         AddressImportRepository
	AddressArtifacts       AddressArtifactStore
	AddressImportMaxBytes  int64
	AddressDimensions      AddressDimensionPublisher
	DimensionLifecycle     AddressDimensionLifecycle
	DimensionConsumers     AddressDimensionConsumerStatusReader
	DimensionGC            AddressDimensionGCRepository
	DimensionKeys          AddressDimensionPublicKeyResolver
	Tenants                TenantRepository
	Readiness              func(context.Context) error
	RuntimeHealth          func() PlatformRuntimeHealth
	RuntimeMetrics         func() []byte
	CollectorEvidence      CollectorEvidenceController
	CollectorPrincipals    CollectorPrincipalController
	CollectorPlans         CollectorPlanDeliveryController
	CollectorPlanTrust     CollectorPlanTrustBundleController
	PlanManagement         CollectorPlanManagementController
	PlanRollouts           CollectorPlanRolloutController
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
	mux.Handle("GET /api/v1/health/runtime", auth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if cfg.RuntimeHealth == nil {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Runtime health is not configured", nil)
			return
		}
		WriteAPIJSON(w, http.StatusOK, cfg.RuntimeHealth())
	})))
	mux.Handle("GET /api/v1/health/runtime/metrics", auth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if cfg.RuntimeMetrics == nil {
			WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Runtime metrics are not configured", nil)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(cfg.RuntimeMetrics())
	})))
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
		registerTargetRoutes(mux, auth, cfg.Targets, cfg.SeriesCleaner, cfg.Network, cfg.DiscoveryJobs, cfg.SNMP, cfg.TargetDeletePreview, cfg.OperationJobs)
	}
	if cfg.Network != nil {
		registerNetworkRoutes(mux, auth, cfg.Network, cfg.Targets, cfg.Agents, cfg.SNMP, cfg.SNMPDiscovery, cfg.SNMPCollector, cfg.SeriesCleaner, cfg.DiscoveryJobs, cfg.Audit, cfg.DeviceDeletePreview, cfg.OperationJobs)
		registerPortRoutes(mux, auth, cfg.Network, cfg.PortDeletePreview, cfg.OperationJobs, cfg.SeriesCleaner)
		registerBGPRoutes(mux, auth, cfg.Network)
		registerGraphRoutes(mux, auth, cfg.Network)
	}
	if cfg.Exports != nil {
		registerExportRoutes(mux, auth, cfg.Exports, cfg.ExportFiles, cfg.Network, cfg.Audit, cfg.OperationJobs, cfg.QueryGateway, cfg.ExportMetric, cfg.ExportCollectionStep)
	}
	if cfg.Billing != nil {
		registerBillingRoutes(mux, auth, cfg.Billing, cfg.Network, cfg.Metrics)
	}
	if cfg.AggregateGraphs != nil {
		registerAggregateGraphRoutes(mux, auth, cfg.AggregateGraphs, cfg.Network, cfg.Metrics)
	}
	if cfg.Dashboards != nil {
		registerDashboardRoutes(mux, auth, cfg.Dashboards, cfg.Audit)
	}
	if cfg.Permissions != nil {
		registerPermissionRoutes(mux, auth, cfg.Permissions)
	}
	if cfg.UserPreferences != nil {
		registerUserPreferencesRoutes(mux, auth, cfg.UserPreferences)
	}
	if cfg.NotificationChannels != nil {
		registerNotificationChannelRoutes(mux, auth, cfg.NotificationChannels)
	}
	if cfg.QuietHours != nil {
		registerQuietHoursRoutes(mux, auth, cfg.QuietHours)
	}
	if cfg.AlertsHistory != nil {
		registerAlertsHistoryRoutes(mux, auth, cfg.AlertsHistory)
	}
	if cfg.IdentityAdmin != nil {
		registerIdentityAdminRoutes(mux, auth, cfg.IdentityAdmin, cfg.Audit, cfg.Idempotency)
	}
	if cfg.Registries != nil {
		registerModuleRoutes(mux, auth, cfg.Registries, cfg.TenantModules, cfg.Audit)
	}
	if cfg.FlowGeo != nil {
		registerFlowGeoRoutes(mux, auth, cfg.FlowGeo)
	}
	if cfg.Retention != nil {
		registerRetentionRoutes(mux, auth, cfg.Retention)
	}
	if cfg.CollectorEvidence != nil {
		registerCollectorEvidenceRoutes(mux, cfg.CollectorEvidence)
	}
	if cfg.CollectorPlans != nil {
		registerCollectorPlanDeliveryRoutes(mux, cfg.CollectorPlans)
	}
	if cfg.CollectorPlanTrust != nil {
		registerCollectorPlanTrustRoutes(mux, cfg.CollectorPlanTrust)
	}
	if cfg.PlanManagement != nil {
		registerCollectorPlanManagementRoutes(mux, auth, cfg.PlanManagement)
	}
	if cfg.PlanRollouts != nil {
		registerCollectorPlanRolloutRoutes(mux, auth, cfg.PlanRollouts)
	}
	if cfg.CollectorPrincipals != nil {
		registerCollectorPrincipalRoutes(mux, auth, cfg.CollectorPrincipals)
	}
	if cfg.CollectorCredentials != nil {
		registerCollectorCredentialRoutes(mux, auth, cfg.CollectorCredentials, cfg.Audit)
	}
	if cfg.CollectorEnrollment != nil {
		registerCollectorEnrollmentRoutes(mux, auth, cfg.CollectorEnrollment, cfg.Audit)
	}
	if cfg.CollectorDeletePreview != nil {
		registerCollectorDeleteRoutes(mux, auth, cfg.CollectorDeletePreview, cfg.OperationJobs)
	}
	if cfg.OperationJobs != nil {
		registerOperationJobRoutes(mux, auth, cfg.OperationJobs)
	}
	if cfg.OperationJobSchedules != nil {
		registerOperationJobScheduleRoutes(mux, auth, cfg.OperationJobSchedules, cfg.Audit)
	}
	if cfg.QueryGateway != nil {
		registerQueryGatewayRoutes(mux, auth, cfg.QueryGateway, cfg.Audit)
		registerFlowFilterRoutes(mux, auth)
	}
	if cfg.FlowRecords != nil {
		registerFlowRecordRoutes(mux, auth, cfg.FlowRecords, cfg.Network, cfg.Audit, cfg.FlowRecordNow)
	}
	if cfg.FlowOverseas != nil {
		registerFlowOverseasRoutes(mux, auth, cfg.FlowOverseas, cfg.Network, cfg.FlowStorageQuery, cfg.Audit, cfg.FlowGeo, cfg.FlowOverseasNow)
	}
	if cfg.FlowVPNFindings != nil {
		registerFlowVPNManagementRoutes(mux, auth, cfg.FlowVPNFindings, cfg.Audit)
	}
	if cfg.FlowStorage != nil {
		registerFlowStorageLifecycleRoutes(mux, auth, cfg.FlowStorage, cfg.Audit)
	}
	if cfg.QueryPolicies != nil && cfg.Registries != nil {
		registerQueryDatasetPolicyRoutes(mux, auth, cfg.QueryPolicies, cfg.Registries, cfg.Audit)
	}
	if cfg.AuditLogs != nil {
		registerAuditLogRoutes(mux, auth, cfg.AuditLogs)
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
	registerMetricsRoutes(mux, auth, cfg.Metrics, cfg.Network, cfg.QueryGateway, cfg.Audit)
	if cfg.AddressSets != nil {
		registerAddressSetRoutes(mux, auth, cfg.AddressSets)
	}
	if cfg.AddressTaxonomy != nil {
		registerAddressTaxonomyRoutes(mux, auth, cfg.AddressTaxonomy)
	}
	if cfg.AddressImports != nil {
		registerAddressImportRoutes(mux, auth, cfg.AddressImports, cfg.AddressArtifacts, cfg.OperationJobs, cfg.AddressImportMaxBytes)
	}
	if cfg.AddressDimensions != nil {
		registerAddressDimensionRoutes(mux, auth, cfg.AddressDimensions, cfg.DimensionLifecycle, cfg.DimensionKeys, cfg.OperationJobs)
	}
	if cfg.AddressDimensions != nil && cfg.DimensionLifecycle != nil && cfg.DimensionConsumers != nil {
		registerAddressDimensionConsumerRoutes(mux, auth, cfg.AddressDimensions, cfg.DimensionLifecycle, cfg.DimensionConsumers)
	}
	if cfg.DimensionGC != nil {
		registerAddressDimensionGCRoutes(mux, auth, cfg.DimensionGC)
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
