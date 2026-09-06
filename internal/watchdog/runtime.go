package watchdog

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowmetrics"
)

type collectorPrincipalRuntimeProvider interface {
	CloseIdleConnections()
	Health() CollectorPrincipalProviderRuntimeHealth
	PrometheusText() []byte
}

type flowRollupRuntimeMetrics interface {
	PrometheusText() []byte
}

type PlatformRuntimeHealth struct {
	CollectorPrincipalProvider CollectorPrincipalProviderRuntimeStatus `json:"collector_principal_provider"`
}

type BackendRuntime struct {
	Config             BackendConfig
	Store              *MySQLStore
	Registries         *PlatformRegistries
	FlowGeo            *FlowGeoService
	MetricsClient      VictoriaMetricsClient
	ExportStore        DiskExportStore
	AddressArtifacts   DiskAddressArtifactStore
	DimensionObjects   DiskDimensionObjectStore
	AddressDimensions  *MySQLAddressDimensionPublisher
	DimensionKeys      AddressDimensionPublicKeyResolver
	ExportWorker       ExportWorker
	SNMPCollector      SNMPPollRunner
	SNMPDiscovery      SNMPDiscoveryEngine
	AggregateRollup    AggregateGraphRollup
	DiscoveryScheduler DiscoveryScheduler
	CollectorEvidence  CollectorEvidenceController
	CollectorPlans     CollectorPlanDeliveryController
	FlowRollupRunner   FlowBucketRollupRunner
	FlowRollupService  *FlowRollupService
	MetricProviders    *RuntimeMetricsRegistry
	QueryProviders     *QueryProviderRegistry
	QueryGateway       *QueryGateway

	CollectorPrincipals        CollectorPrincipalController
	collectorPrincipalProvider collectorPrincipalRuntimeProvider
	flowRollupNative           interface{ Close() }
	flowRollupMetrics          flowRollupRuntimeMetrics
	metricsScrapeHandler       http.Handler

	trapDispatcherFn  func(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error)
	backgroundMu      sync.Mutex
	backgroundStarted bool
	backgroundClosed  bool
	closeOnce         sync.Once
	closeError        error
}

func NewBackendRuntime(ctx context.Context, cfg BackendConfig) (*BackendRuntime, error) {
	if err := ConfigureSNMPMIBRegistry(cfg.SNMP); err != nil {
		log.Printf("watchdog snmp mib registry: %v", err)
	}
	store, err := OpenMySQLStore(ctx, cfg.MySQL)
	if err != nil {
		return nil, err
	}
	migrationResult, err := ApplyMySQLMigrations(ctx, store.db)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	if len(migrationResult.Applied) > 0 {
		log.Printf("watchdog mysql migrations applied=%v current=%s", migrationResult.Applied, migrationResult.CurrentVersion)
	}
	metricsClient := VictoriaMetricsClient{BaseURL: cfg.VictoriaMetrics.BaseURL}
	exportStore := DiskExportStore{Dir: cfg.Export.Dir}
	addressArtifacts := DiskAddressArtifactStore{Dir: cfg.AddressLibrary.Dir, MaxBytes: cfg.AddressLibrary.MaxUploadBytes}
	dimensionObjects := DiskDimensionObjectStore{Dir: cfg.AddressLibrary.Dir, MaxBytes: 64 << 20}
	addressDimensions, err := NewMySQLAddressDimensionPublisher(store, dimensionObjects)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	addressDimensionKeys, err := LoadAddressDimensionPublicKeyResolver(cfg.AddressLibrary.TrustedKeys)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("load address dimension trusted keys: %w", err)
	}
	collectorAuthenticator, err := NewMySQLCollectorMachineAuthenticator(store.db)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	collectorEvidence, err := NewCollectorEvidenceService(collectorAuthenticator, store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	collectorPlans, err := NewCollectorPlanDeliveryService(collectorAuthenticator, store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	var flowGeo *FlowGeoService
	if cfg.FlowGeo.Path != "" {
		flowGeo = NewFlowGeoService(cfg.FlowGeo.Path)
		if err := flowGeo.Reload(); err != nil {
			log.Printf("watchdog flow geo bundle load failed (serving without geo until reload): %v", err)
		} else {
			status := flowGeo.Status()
			log.Printf("watchdog flow geo bundle loaded version=%s v4=%d v6=%d", status.Version, status.RowsV4, status.RowsV6)
		}
	}
	registries, err := NewBuiltinPlatformRegistries()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("register builtin platform modules: %w", err)
	}
	runtime := &BackendRuntime{
		Config:            cfg,
		Store:             store,
		Registries:        registries,
		FlowGeo:           flowGeo,
		MetricsClient:     metricsClient,
		ExportStore:       exportStore,
		AddressArtifacts:  addressArtifacts,
		DimensionObjects:  dimensionObjects,
		AddressDimensions: addressDimensions,
		DimensionKeys:     addressDimensionKeys,
		CollectorEvidence: collectorEvidence,
		CollectorPlans:    collectorPlans,
		MetricProviders:   NewRuntimeMetricsRegistry(),
		QueryProviders:    NewQueryProviderRegistry(),
	}
	if cfg.QueryGateway.Enabled {
		if err := runtime.QueryProviders.Register(QueryProviderRegistration{
			Kind: DatasetProviderVM, Provider: VictoriaMetricsQueryProvider{Client: metricsClient, Network: store},
			Enabled: cfg.QueryGateway.VictoriaMetricsEnabled, MaxConcurrent: uint32(cfg.QueryGateway.VictoriaMetricsConcurrent),
		}); err != nil {
			_ = runtime.Close()
			return nil, fmt.Errorf("initialize VictoriaMetrics query provider: %w", err)
		}
		runtime.QueryGateway, err = NewQueryGateway(registries, store, store, runtime.QueryProviders)
		if err != nil {
			_ = runtime.Close()
			return nil, fmt.Errorf("initialize query gateway: %w", err)
		}
	}
	if err := runtime.MetricProviders.Register("collector_principal_provider", runtimeMetricsProviderFunc(runtime.collectorPrincipalMetrics)); err != nil {
		_ = runtime.Close()
		return nil, err
	}
	if cfg.CollectorPrincipalProvider.Enabled {
		provider, err := NewRemoteCollectorPrincipalProvider(cfg.CollectorPrincipalProvider)
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("initialize collector principal provider: %w", err)
		}
		principalService, err := NewCollectorPrincipalService(store, map[string]CollectorPrincipalProvider{
			cfg.CollectorPrincipalProvider.Name: provider,
		})
		if err != nil {
			if closer, ok := provider.(interface{ CloseIdleConnections() }); ok {
				closer.CloseIdleConnections()
			}
			_ = store.Close()
			return nil, fmt.Errorf("initialize collector principal service: %w", err)
		}
		runtime.CollectorPrincipals = principalService
		runtime.collectorPrincipalProvider, _ = provider.(collectorPrincipalRuntimeProvider)
	}
	var exportData ExportDataProvider = VictoriaMetricsExportDataProvider{
		Client: metricsClient, Metric: cfg.Export.Metric, Network: store, CollectionStep: cfg.SNMPCollector.Interval,
	}
	if runtime.QueryGateway != nil {
		exportData = QueryGatewayExportDataProvider{Gateway: runtime.QueryGateway}
	}
	runtime.ExportWorker = ExportWorker{
		Repo:    store,
		Data:    exportData,
		Writer:  exportStore,
		Network: store,
		CompletenessPolicy: func(task ExportTask) CompletenessPolicy {
			return CompletenessPolicy{
				Start:           task.RangeStart,
				End:             task.RangeEnd,
				CollectionStep:  ExportQueryStep(task, cfg.SNMPCollector.Interval),
				MaxMissingRatio: 0.3,
			}
		},
	}
	discoveryEngine, err := NewSNMPDiscoveryEngineFromRepository(ctx, store, NewGoSNMPCollectorQueryEngine(), DefaultSNMPCollectorModuleRegistry())
	if err != nil {
		if runtime.collectorPrincipalProvider != nil {
			runtime.collectorPrincipalProvider.CloseIdleConnections()
		}
		_ = store.Close()
		return nil, err
	}
	runtime.SNMPCollector = SNMPPollRunner{
		Collector: store,
		Network:   store,
		Targets:   store,
		SNMP:      store,
		Poller: SNMPPoller{
			Query:  NewGoSNMPCollectorQueryEngine(),
			Writer: VictoriaMetricsSNMPRawWriter{Client: metricsClient},
		},
	}
	runtime.SNMPDiscovery = discoveryEngine
	runtime.AggregateRollup = AggregateGraphRollup{
		Graphs:  store,
		Network: store,
		Metrics: MetricsService{Client: metricsClient},
	}
	runtime.DiscoveryScheduler = DiscoveryScheduler{
		Jobs:      store,
		Network:   store,
		Targets:   store,
		SNMP:      store,
		Discovery: discoveryEngine,
		Collector: store,
	}
	runtime.trapDispatcherFn = runtime.buildTrapDispatcher()
	if cfg.FlowRollup.Enabled {
		native, runner, service, err := newFlowRollupRuntime(ctx, store, cfg.FlowRollup)
		if err != nil {
			if runtime.collectorPrincipalProvider != nil {
				runtime.collectorPrincipalProvider.CloseIdleConnections()
			}
			_ = store.Close()
			return nil, fmt.Errorf("initialize flow rollup: %w", err)
		}
		runtime.flowRollupNative = native
		runtime.FlowRollupRunner = runner
		runtime.FlowRollupService = service
		runtime.flowRollupMetrics, err = flowmetrics.NewRollup(runner.Stats)
		if err != nil {
			native.Close()
			_ = store.Close()
			return nil, fmt.Errorf("initialize flow rollup metrics: %w", err)
		}
		if err := runtime.MetricProviders.Register("flow_rollup", runtime.flowRollupMetrics); err != nil {
			_ = runtime.Close()
			return nil, err
		}
	}
	runtime.metricsScrapeHandler, err = NewMetricsScrapeHandler(cfg.MetricsScrape, runtime.RuntimeMetrics)
	if err != nil {
		_ = runtime.Close()
		return nil, fmt.Errorf("initialize metrics scrape endpoint: %w", err)
	}
	return runtime, nil
}

func (r *BackendRuntime) Router(auth AuthContextAdapter, tenantDiscovery ...AuthContextAdapter) http.Handler {
	tenantDiscoveryAuth := auth
	if len(tenantDiscovery) > 0 && tenantDiscovery[0] != nil {
		tenantDiscoveryAuth = tenantDiscovery[0]
	}
	return NewAPIV1Router(APIV1RouterConfig{
		Auth:                   auth,
		TenantDiscovery:        tenantDiscoveryAuth,
		Targets:                r.Store,
		Agents:                 r.Store,
		Network:                r.Store,
		Exports:                r.Store,
		ExportFiles:            r.ExportStore,
		ExportMetric:           r.Config.Export.Metric,
		ExportCollectionStep:   r.Config.SNMPCollector.Interval,
		Billing:                r.Store,
		AggregateGraphs:        r.Store,
		Dashboards:             r.Store,
		Permissions:            r.Store,
		IdentityAdmin:          r.Store,
		Idempotency:            r.Store,
		TargetDeletePreview:    r.Store,
		DeviceDeletePreview:    r.Store,
		PortDeletePreview:      r.Store,
		CollectorDeletePreview: r.Store,
		UserPreferences:        r.Store,
		NotificationChannels:   r.Store,
		QuietHours:             r.Store,
		AlertsHistory:          r.Store,
		CollectorCredentials:   r.Store,
		CollectorEnrollment:    r.Store,
		Registries:             r.Registries,
		TenantModules:          r.Store,
		FlowGeo:                r.FlowGeo,
		Retention:              r.Store,
		SNMP:                   r.Store,
		SNMPDiscovery:          r.SNMPDiscovery,
		SNMPCollector:          r.Store,
		SeriesCleaner:          r.MetricsClient,
		DiscoveryJobs:          r.Store,
		TrapDispatcher:         r.trapDispatcherFn,
		Audit:                  r.Store,
		AddressSets:            r.Store,
		AddressTaxonomy:        r.Store,
		AddressImports:         r.Store,
		AddressArtifacts:       r.AddressArtifacts,
		AddressImportMaxBytes:  r.Config.AddressLibrary.MaxUploadBytes,
		AddressDimensions:      r.AddressDimensions,
		DimensionLifecycle:     r.AddressDimensions,
		DimensionConsumers:     r.AddressDimensions,
		DimensionKeys:          r.DimensionKeys,
		OperationJobs:          r.Store,
		OperationJobSchedules:  r.Store,
		QueryGateway:           r.QueryGateway,
		QueryPolicies:          r.Store,
		Tenants:                r.Store,
		Readiness:              r.Ready,
		RuntimeHealth:          r.Health,
		RuntimeMetrics:         r.RuntimeMetrics,
		CollectorEvidence:      r.CollectorEvidence,
		CollectorPrincipals:    r.CollectorPrincipals,
		CollectorPlans:         r.CollectorPlans,
		Metrics: MetricsService{
			Client:   r.MetricsClient,
			Importer: r.MetricsClient,
		},
	})
}

func (r *BackendRuntime) Health() PlatformRuntimeHealth {
	providerStatus := CollectorPrincipalProviderRuntimeStatus{Enabled: r != nil && r.Config.CollectorPrincipalProvider.Enabled}
	if r != nil {
		if r.collectorPrincipalProvider != nil {
			providerStatus.Health = r.collectorPrincipalProvider.Health()
		}
	}
	return PlatformRuntimeHealth{CollectorPrincipalProvider: providerStatus}
}

func (r *BackendRuntime) RuntimeMetrics() []byte {
	if r != nil && r.MetricProviders != nil {
		return r.MetricProviders.PrometheusText()
	}
	// Compatibility for small unit fixtures that construct BackendRuntime
	// directly instead of through NewBackendRuntime.
	metrics := r.collectorPrincipalMetrics()
	if r != nil && r.flowRollupMetrics != nil {
		metrics = append(metrics, r.flowRollupMetrics.PrometheusText()...)
	}
	return metrics
}

func (r *BackendRuntime) collectorPrincipalMetrics() []byte {
	metrics := make([]byte, 0, 2048)
	if r == nil || !r.Config.CollectorPrincipalProvider.Enabled {
		metrics = append(metrics, "# TYPE watchdog_collector_principal_provider_enabled gauge\nwatchdog_collector_principal_provider_enabled 0\n"...)
	} else {
		metrics = append(metrics, "# TYPE watchdog_collector_principal_provider_enabled gauge\nwatchdog_collector_principal_provider_enabled 1\n"...)
		if r.collectorPrincipalProvider == nil {
			metrics = append(metrics, "# TYPE watchdog_collector_principal_provider_accepting_requests gauge\nwatchdog_collector_principal_provider_accepting_requests 0\n"...)
		} else {
			metrics = append(metrics, r.collectorPrincipalProvider.PrometheusText()...)
		}
	}
	return metrics
}

// MetricsScrapeHandler is nil when machine scraping is disabled. Production
// hub mounts a non-nil handler at /metrics on its existing listener.
func (r *BackendRuntime) MetricsScrapeHandler() http.Handler {
	if r == nil {
		return nil
	}
	return r.metricsScrapeHandler
}

func (r *BackendRuntime) RunExportWorker(ctx context.Context) error {
	return r.ExportWorker.RunLoop(ctx, ExportWorkerLoopConfig{
		Interval:       r.Config.Export.WorkerInterval,
		BatchSize:      r.Config.Export.WorkerBatch,
		RunImmediately: true,
	})
}

func (r *BackendRuntime) RunSNMPCollectorPoll(ctx context.Context, tenantID ID, limit int) (SNMPPollRunnerResult, error) {
	return r.SNMPCollector.RunDue(ctx, tenantID, limit)
}

func (r *BackendRuntime) RunSNMPCollectorScheduler(ctx context.Context, tenantID ID, interval time.Duration, limit int) error {
	if interval <= 0 {
		interval = time.Minute
	}
	if limit <= 0 {
		limit = 500
	}
	run := func() {
		result, err := r.RunSNMPCollectorPoll(ctx, tenantID, limit)
		if err != nil {
			log.Printf("watchdog snmp collector poll tenant=%s failed: %v", tenantID, err)
			return
		}
		log.Printf("watchdog snmp collector poll tenant=%s recipes=%d devices=%d samples=%d failed=%d", tenantID, result.RecipeCount, result.DeviceCount, result.SampleCount, result.FailedCount)
	}
	run()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			run()
		}
	}
}

func (r *BackendRuntime) RunAggregateGraphRollup(ctx context.Context) error {
	return r.AggregateRollup.RunLoop(ctx, AggregateGraphRollupLoopConfig{
		Interval:       r.Config.AggregateGraph.RollupInterval,
		RunImmediately: true,
	})
}

// StartBackground owns the lifecycle gate for embedded control-plane workers.
// Standalone collectors keep their own process lifecycle.
func (r *BackendRuntime) StartBackground(ctx context.Context) error {
	if r == nil || ctx == nil {
		return errors.New("watchdog backend runtime and context are required")
	}
	r.backgroundMu.Lock()
	defer r.backgroundMu.Unlock()
	if r.backgroundStarted {
		return errors.New("watchdog backend background services are already started")
	}
	if r.backgroundClosed {
		return errors.New("watchdog backend runtime is closed")
	}
	r.backgroundStarted = true
	if r.Store != nil {
		owner, _ := os.Hostname()
		registry := NewOperationJobHandlerRegistry()
		if err := registry.Register(OperationJobRegistration{
			JobType:     TargetDeleteJobType,
			Handler:     NewTargetDeleteJobHandler(r.Store, r.MetricsClient, r.Store),
			Concurrency: 2,
		}); err != nil {
			return err
		}
		if err := registry.Register(OperationJobRegistration{
			JobType:     DeviceDeleteJobType,
			Handler:     NewDeviceDeleteJobHandler(r.Store, r.MetricsClient, r.Store),
			Concurrency: 2,
		}); err != nil {
			return err
		}
		if err := registry.Register(OperationJobRegistration{
			JobType:     PortDeleteJobType,
			Handler:     NewPortDeleteJobHandler(r.Store, r.MetricsClient, r.Store),
			Concurrency: 2,
		}); err != nil {
			return err
		}
		if err := registry.Register(OperationJobRegistration{
			JobType:     CollectorDeleteJobType,
			Handler:     NewCollectorDeleteJobHandler(r.Store, r.Store),
			Concurrency: 1,
		}); err != nil {
			return err
		}
		if err := registry.Register(OperationJobRegistration{
			JobType:     AddressImportJobType,
			Handler:     NewAddressImportJobHandler(r.Store, r.AddressArtifacts, r.Config.AddressLibrary.ImportBatchSize),
			Concurrency: r.Config.AddressLibrary.WorkerConcurrency,
			LeaseFor:    2 * time.Minute,
			MaxAttempts: 5,
			RetryBase:   30 * time.Second,
		}); err != nil {
			return err
		}
		if err := registry.Register(OperationJobRegistration{
			JobType: AddressDimensionPublishJob, Handler: NewAddressDimensionPublishJobHandler(r.AddressDimensions),
			Concurrency: 1, LeaseFor: 5 * time.Minute, MaxAttempts: 3, RetryBase: 30 * time.Second,
		}); err != nil {
			return err
		}
		if r.QueryGateway != nil {
			if err := registry.Register(OperationJobRegistration{
				JobType: ExportExecutionJobType,
				Handler: NewExportExecutionJobHandler(r.Store, r.ExportWorker, ExportExecutionJobDependencies{
					Authorization: r.Store,
					Network:       r.Store,
				}),
				Concurrency: r.Config.Export.WorkerConcurrency,
				LeaseFor:    5 * time.Minute,
				MaxAttempts: 5,
				RetryBase:   30 * time.Second,
			}); err != nil {
				return err
			}
		}
		if err := registry.Register(OperationJobRegistration{
			JobType:     ExportDeleteJobType,
			Handler:     NewExportDeleteJobHandler(r.Store, r.ExportStore, r.Store),
			Concurrency: r.Config.Export.WorkerConcurrency, LeaseFor: 5 * time.Minute,
			MaxAttempts: 5, RetryBase: 30 * time.Second,
		}); err != nil {
			return err
		}
		if r.FlowRollupRunner != nil {
			if err := registry.Register(OperationJobRegistration{
				JobType: FlowRollupJobType, Handler: NewFlowRollupJobHandler(r.FlowRollupRunner),
				Concurrency: r.Config.FlowRollup.WorkerConcurrency, LeaseFor: r.Config.FlowRollup.LeaseFor,
				MaxAttempts: r.Config.FlowRollup.MaxAttempts, RetryBase: r.Config.FlowRollup.RetryBase,
			}); err != nil {
				return err
			}
		}
		StartOperationJobScheduler(ctx, r.Store, registry, owner, nil)
		go (OperationJobScheduleDispatcher{
			Repository: r.Store,
			Registry:   registry,
			Logf:       log.Printf,
		}).Run(ctx)
		if r.FlowRollupService != nil {
			r.FlowRollupService.Logf = log.Printf
			go r.FlowRollupService.Run(ctx)
		}
		NewStoreMaintenance(r.Store, nil).Start(ctx)
	}
	return nil
}

func (r *BackendRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.backgroundMu.Lock()
		r.backgroundClosed = true
		r.backgroundMu.Unlock()
		if r.collectorPrincipalProvider != nil {
			r.collectorPrincipalProvider.CloseIdleConnections()
		}
		if r.flowRollupNative != nil {
			r.flowRollupNative.Close()
		}
		if r.QueryProviders != nil {
			r.closeError = errors.Join(r.closeError, r.QueryProviders.Close())
		}
		if r.Store != nil {
			r.closeError = errors.Join(r.closeError, r.Store.Close())
		}
	})
	return r.closeError
}

func (r *BackendRuntime) Ready(ctx context.Context) error {
	if r == nil || r.Store == nil || r.Store.db == nil {
		return errors.New("mysql runtime is not initialized")
	}
	if err := r.Store.db.PingContext(ctx); err != nil {
		return err
	}
	if err := CheckMySQLSchemaCurrent(ctx, r.Store.db); err != nil {
		return err
	}
	if r.QueryProviders != nil {
		if err := r.QueryProviders.Ready(ctx); err != nil {
			return fmt.Errorf("query provider is not ready: %w", err)
		}
	}
	return nil
}

func (r *BackendRuntime) buildTrapDispatcher() func(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error) {
	dispatcher := NewSNMPTrapDispatcher(nil)
	return func(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error) {
		ports, _ := r.Store.ListPorts(ctx, device.TenantID, device.ID)
		portMap := make(map[uint64]NetworkPort, len(ports))
		portRecipeIDs := make(map[ID][]ID, len(ports))
		for _, port := range ports {
			portMap[port.IfIndex] = port
		}
		sessions, _ := r.Store.ListBGPSessions(ctx, device.TenantID, device.ID)
		bgpMap := make(map[string]BGPSession, len(sessions))
		bgpRecipeIDs := make(map[ID][]ID, len(sessions))
		for _, session := range sessions {
			bgpMap[session.PeerAddr] = session
		}
		handlers := DefaultSNMPTrapHandlers(portMap, portRecipeIDs, bgpMap, bgpRecipeIDs)
		dispatcher.Handlers = handlers
		return dispatcher.Dispatch(ctx, device, trap)
	}
}
