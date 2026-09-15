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

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowmetrics"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/flowstream"
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
	Config              BackendConfig
	Store               *MySQLStore
	Registries          *PlatformRegistries
	FlowGeo             *FlowGeoService
	MetricsClient       VictoriaMetricsClient
	ExportStore         DiskExportStore
	AddressArtifacts    DiskAddressArtifactStore
	DimensionObjects    DiskDimensionObjectStore
	AddressDimensions   *MySQLAddressDimensionPublisher
	DimensionKeys       AddressDimensionPublicKeyResolver
	ExportWorker        ExportWorker
	SNMPCollector       SNMPPollRunner
	SNMPDiscovery       SNMPDiscoveryEngine
	AggregateRollup     AggregateGraphRollup
	DiscoveryScheduler  DiscoveryScheduler
	CollectorEvidence   CollectorEvidenceController
	CollectorPlans      CollectorPlanDeliveryController
	CollectorPlanTrust  CollectorPlanTrustBundleController
	CollectorPlanSigner CollectorPlanSigner
	PlanManagement      CollectorPlanManagementController
	PlanRollouts        CollectorPlanRolloutController
	FlowRollupRunner    FlowBucketRollupRunner
	FlowRollupService   *FlowRollupService
	FlowStorageRunner   FlowStorageDayRunner
	FlowStorageService  *FlowStorageLifecycleService
	FlowReconciliation  flowReconciliationScanner
	MetricProviders     *RuntimeMetricsRegistry
	QueryProviders      *QueryProviderRegistry
	QueryGateway        *QueryGateway
	FlowRecords         flowDetailRunner
	FlowOverseas        flowOverseasRunner

	FlowWorkerTrust        CollectorPlanTrustBundleController
	FlowEnrichmentDelivery FlowEnrichmentDeliveryController

	CollectorPrincipals        CollectorPrincipalController
	collectorPrincipalProvider collectorPrincipalRuntimeProvider
	flowClickHouseNative       *flowch.NativeInserter
	flowReconciliationOffsets  *flowstream.CommittedOffsetReader
	flowReconciliationMetrics  *flowmetrics.Reconciliation
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
	dimensionObjects := DiskDimensionObjectStore{Dir: cfg.AddressLibrary.Dir, MaxBytes: cfg.AddressLibrary.MaxSnapshotBytes}
	addressDimensions, err := NewMySQLAddressDimensionPublisher(store, dimensionObjects,
		WithAddressDimensionObjectRetention(cfg.AddressLibrary.ObjectRetention))
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
	collectorPlanTrust, err := NewCollectorPlanTrustBundleService(collectorAuthenticator, store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	flowWorkerAuthenticator, err := NewMySQLFlowWorkerMachineAuthenticator(store.db)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	flowWorkerTrust, err := NewCollectorPlanTrustBundleService(flowWorkerAuthenticator, store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	flowEnrichmentDelivery, err := NewFlowEnrichmentDeliveryService(flowWorkerAuthenticator, store, dimensionObjects, int64(cfg.AddressLibrary.MaxSnapshotBytes))
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	var collectorPlanSigner CollectorPlanSigner
	if cfg.CollectorPlanSigning.KeyID != "" {
		collectorPlanSigner, err = LoadCollectorPlanSigner(cfg.CollectorPlanSigning, store)
		if err != nil {
			_ = store.Close()
			return nil, err
		}
		if _, err := store.ActivateCollectorPlanSigningKey(ctx, collectorPlanSigner.KeyID(), collectorPlanSigner.PublicKey(), time.Now()); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("activate collector plan signing key: %w", err)
		}
	}
	collectorPlanManagement, err := NewCollectorPlanManagementService(store, collectorPlanSigner)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	collectorPlanRollouts, err := NewCollectorPlanRolloutService(store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	flowGeo := NewFlowGeoService(cfg.FlowGeo.Path)
	if cfg.FlowGeo.Path != "" {
		if err := flowGeo.Reload(); err != nil {
			log.Printf("watchdog flow geo bundle load failed (serving without geo until reload): %v", err)
		} else {
			for _, path := range cfg.FlowGeo.HistoricalPaths {
				if err := flowGeo.LoadHistorical(path); err != nil {
					_ = store.Close()
					return nil, fmt.Errorf("load historical Flow Geo bundle %q: %w", path, err)
				}
			}
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
		Config:              cfg,
		Store:               store,
		Registries:          registries,
		FlowGeo:             flowGeo,
		MetricsClient:       metricsClient,
		ExportStore:         exportStore,
		AddressArtifacts:    addressArtifacts,
		DimensionObjects:    dimensionObjects,
		AddressDimensions:   addressDimensions,
		DimensionKeys:       addressDimensionKeys,
		CollectorEvidence:   collectorEvidence,
		CollectorPlans:      collectorPlans,
		CollectorPlanTrust:  collectorPlanTrust,
		CollectorPlanSigner: collectorPlanSigner,
		PlanManagement:      collectorPlanManagement,
		PlanRollouts:        collectorPlanRollouts,
		MetricProviders:     NewRuntimeMetricsRegistry(),
		QueryProviders:      NewQueryProviderRegistry(),

		FlowWorkerTrust:        flowWorkerTrust,
		FlowEnrichmentDelivery: flowEnrichmentDelivery,
	}
	if cfg.FlowRollup.Enabled || cfg.FlowStorage.Enabled || cfg.FlowReconciliation.Enabled || (cfg.QueryGateway.Enabled && cfg.QueryGateway.ClickHouseEnabled) {
		runtime.flowClickHouseNative, err = newFlowClickHouseNative(ctx, cfg.FlowRollup)
		if err != nil {
			_ = runtime.Close()
			return nil, fmt.Errorf("initialize Flow ClickHouse connection: %w", err)
		}
	}
	if cfg.QueryGateway.Enabled {
		if err := runtime.QueryProviders.Register(QueryProviderRegistration{
			Kind: DatasetProviderVM, Provider: VictoriaMetricsQueryProvider{Client: metricsClient, Network: store},
			Enabled: cfg.QueryGateway.VictoriaMetricsEnabled, MaxConcurrent: uint32(cfg.QueryGateway.VictoriaMetricsConcurrent),
		}); err != nil {
			_ = runtime.Close()
			return nil, fmt.Errorf("initialize VictoriaMetrics query provider: %w", err)
		}
		if cfg.QueryGateway.ClickHouseEnabled {
			runner, runnerErr := flowquery.NewRunner(runtime.flowClickHouseNative)
			if runnerErr != nil {
				_ = runtime.Close()
				return nil, fmt.Errorf("initialize Flow query runner: %w", runnerErr)
			}
			jointRunner, runnerErr := flowquery.NewJointRunner(runtime.flowClickHouseNative)
			if runnerErr != nil {
				_ = runtime.Close()
				return nil, fmt.Errorf("initialize Flow joint-query runner: %w", runnerErr)
			}
			addressSetRunner, runnerErr := flowquery.NewAddressSetRunner(runtime.flowClickHouseNative)
			if runnerErr != nil {
				_ = runtime.Close()
				return nil, fmt.Errorf("initialize Flow address-set query runner: %w", runnerErr)
			}
			detailRunner, runnerErr := flowquery.NewDetailRunner(runtime.flowClickHouseNative)
			if runnerErr != nil {
				_ = runtime.Close()
				return nil, fmt.Errorf("initialize Flow detail-query runner: %w", runnerErr)
			}
			runtime.FlowRecords = detailRunner
			overseasRunner, runnerErr := flowquery.NewOverseasRunner(runtime.flowClickHouseNative)
			if runnerErr != nil {
				_ = runtime.Close()
				return nil, fmt.Errorf("initialize Flow overseas-query runner: %w", runnerErr)
			}
			runtime.FlowOverseas = overseasRunner
			provider := ClickHouseFlowQueryProvider{
				Runner: runner, JointRunner: jointRunner, AddressSetRunner: addressSetRunner,
				OverseasRunner: overseasRunner, VPNFindings: store, Readiness: runtime.flowClickHouseNative,
				Network: store, FlowGeo: flowGeo, OperatorBindings: store,
			}
			if cfg.FlowStorage.Enabled {
				provider.StorageLifecycle = store
			}
			if err := runtime.QueryProviders.Register(QueryProviderRegistration{
				Kind:     DatasetProviderClickHouse,
				Provider: provider,
				Enabled:  true, MaxConcurrent: uint32(cfg.QueryGateway.ClickHouseConcurrent),
			}); err != nil {
				_ = runtime.Close()
				return nil, fmt.Errorf("initialize Flow ClickHouse query provider: %w", err)
			}
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
	legacyExportData := VictoriaMetricsExportDataProvider{
		Client: metricsClient, Metric: cfg.Export.Metric, Network: store, CollectionStep: cfg.SNMPCollector.Interval,
	}
	var exportData ExportDataProvider = QueryGatewayExportDataProvider{
		Gateway: runtime.QueryGateway, FlowRecords: runtime.FlowRecords, VPNFindings: store, Fallback: legacyExportData,
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
	var sharedRollupRunner *flowch.RollupRunner
	if cfg.FlowRollup.Enabled {
		runner, service, err := newFlowRollupRuntime(store, cfg.FlowRollup, runtime.flowClickHouseNative)
		if err != nil {
			_ = runtime.Close()
			return nil, fmt.Errorf("initialize flow rollup: %w", err)
		}
		runtime.FlowRollupRunner = runner
		runtime.FlowRollupService = service
		sharedRollupRunner = runner
		runtime.flowRollupMetrics, err = flowmetrics.NewRollup(runner.Stats)
		if err != nil {
			_ = runtime.Close()
			return nil, fmt.Errorf("initialize flow rollup metrics: %w", err)
		}
		if err := runtime.MetricProviders.Register("flow_rollup", runtime.flowRollupMetrics); err != nil {
			_ = runtime.Close()
			return nil, err
		}
	}
	if cfg.FlowStorage.Enabled {
		if sharedRollupRunner == nil {
			sharedRollupRunner, err = flowch.NewRollupRunner(runtime.flowClickHouseNative)
			if err != nil {
				_ = runtime.Close()
				return nil, fmt.Errorf("initialize Flow storage downsample runner: %w", err)
			}
		}
		runtime.FlowStorageRunner = sharedRollupRunner
		runtime.FlowStorageService = &FlowStorageLifecycleService{
			Store: store, Interval: cfg.FlowStorage.ScanInterval,
			MaxPoliciesPerScan:   cfg.FlowStorage.MaxPoliciesPerScan,
			MaxPartitionsPerScan: cfg.FlowStorage.MaxPartitionsPerScan,
		}
	}
	if cfg.FlowReconciliation.Enabled {
		runtime.flowReconciliationOffsets, runtime.FlowReconciliation, runtime.flowReconciliationMetrics, err =
			newFlowReconciliationRuntime(ctx, cfg.FlowReconciliation, runtime.flowClickHouseNative)
		if err != nil {
			_ = runtime.Close()
			return nil, fmt.Errorf("initialize Flow ingest reconciliation: %w", err)
		}
		if err := runtime.MetricProviders.Register("flow_ingest_reconciliation", runtime.flowReconciliationMetrics); err != nil {
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
	config := APIV1RouterConfig{
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
		Permissions:            r.Store,
		Idempotency:            r.Store,
		TargetDeletePreview:    r.Store,
		DeviceDeletePreview:    r.Store,
		PortDeletePreview:      r.Store,
		CollectorDeletePreview: r.Store,
		UserPreferences:        r.Store,
		CollectorCredentials:   r.Store,
		CollectorEnrollment:    r.Store,
		Registries:             r.Registries,
		TenantModules:          r.Store,
		FlowGeo:                r.FlowGeo,
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
		AddressLibraryOwner:    r.Config.AddressLibrary.OwnerTenantID,
		AddressDimensions:      r.AddressDimensions,
		DimensionLifecycle:     r.AddressDimensions,
		DimensionConsumers:     r.AddressDimensions,
		DimensionGC:            r.AddressDimensions,
		DimensionKeys:          r.DimensionKeys,
		OperationJobs:          r.Store,
		OperationJobSchedules:  r.Store,
		QueryGateway:           r.QueryGateway,
		FlowRecords:            r.FlowRecords,
		FlowOverseas:           r.FlowOverseas,
		FlowVPNFindings:        r.Store,
		FlowVPNRules:           r.Store,
		FlowStorage:            r.Store,
		FlowSavedFilters:       r.Store,
		QueryPolicies:          r.Store,
		Tenants:                r.Store,
		Readiness:              r.Ready,
		RuntimeHealth:          r.Health,
		RuntimeMetrics:         r.RuntimeMetrics,
		CollectorEvidence:      r.CollectorEvidence,
		CollectorPrincipals:    r.CollectorPrincipals,
		CollectorPlans:         r.CollectorPlans,
		CollectorPlanTrust:     r.CollectorPlanTrust,
		FlowWorkerTrust:        r.FlowWorkerTrust,
		FlowEnrichmentDelivery: r.FlowEnrichmentDelivery,
		PlanManagement:         r.PlanManagement,
		PlanRollouts:           r.PlanRollouts,
		Metrics: MetricsService{
			Client:   r.MetricsClient,
			Importer: r.MetricsClient,
		},
	}
	if r.Config.FlowStorage.Enabled {
		config.FlowStorageQuery = r.Store
	}
	return NewAPIV1Router(config)
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
		if err := registry.Register(OperationJobRegistration{
			JobType: AddressSnapshotBuildJob, Handler: NewAddressSnapshotBuildJobHandler(r.AddressDimensions),
			Concurrency: 1, LeaseFor: 10 * time.Minute, MaxAttempts: 5, RetryBase: 30 * time.Second,
		}); err != nil {
			return err
		}
		if err := registry.Register(OperationJobRegistration{
			JobType:     AddressDimensionObjectGCJob,
			Handler:     NewAddressDimensionObjectGCJobHandler(r.AddressDimensions, nil),
			Concurrency: 1, LeaseFor: 30 * time.Second, MaxAttempts: AddressDimensionObjectGCMaxAttempts, RetryBase: 30 * time.Second,
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
			// A terminally failed bucket is a permanent gap once the scheduled
			// watermark advances past it: surface it so the silent hole alarms.
			var onTerminal func(OperationJob, string)
			if recorder, ok := r.FlowRollupRunner.(interface{ RecordTerminalFailure(bool) }); ok {
				onTerminal = func(_ OperationJob, code string) {
					recorder.RecordTerminalFailure(code != OperationJobCodeTerminal)
				}
			}
			if err := registry.Register(OperationJobRegistration{
				JobType: FlowRollupJobType, Handler: NewFlowRollupJobHandler(r.FlowRollupRunner),
				Concurrency: r.Config.FlowRollup.WorkerConcurrency, LeaseFor: r.Config.FlowRollup.LeaseFor,
				MaxAttempts: r.Config.FlowRollup.MaxAttempts, RetryBase: r.Config.FlowRollup.RetryBase,
				OnTerminalFailure: onTerminal,
			}); err != nil {
				return err
			}
		}
		if r.FlowStorageRunner != nil {
			if err := registry.Register(OperationJobRegistration{
				JobType:     FlowStorageDownsampleJobType,
				Handler:     NewFlowStorageDownsampleJobHandler(r.Store, r.FlowStorageRunner),
				Concurrency: r.Config.FlowStorage.WorkerConcurrency, LeaseFor: r.Config.FlowStorage.LeaseFor,
				MaxAttempts: r.Config.FlowStorage.MaxAttempts, RetryBase: r.Config.FlowStorage.RetryBase,
			}); err != nil {
				return err
			}
		}
		if r.FlowReconciliation != nil {
			if err := registry.Register(OperationJobRegistration{
				JobType:     FlowReconciliationJobType,
				Handler:     NewFlowReconciliationJobHandler(r.flowReconciliationOffsets, r.FlowReconciliation, r.Store, r.flowReconciliationMetrics),
				Concurrency: 1, LeaseFor: r.Config.FlowReconciliation.LeaseFor,
				MaxAttempts: r.Config.FlowReconciliation.MaxAttempts, RetryBase: r.Config.FlowReconciliation.RetryBase,
			}); err != nil {
				return err
			}
			if err := ensureFlowReconciliationSchedule(ctx, r.Store, r.Config.FlowReconciliation); err != nil {
				return fmt.Errorf("ensure Flow reconciliation schedule: %w", err)
			}
		} else if err := r.Store.DisableSystemOperationJobSchedulesExcept(ctx, FlowReconciliationJobType, ""); err != nil {
			return fmt.Errorf("disable Flow reconciliation schedules: %w", err)
		}
		StartOperationJobScheduler(ctx, r.Store, registry, owner, nil)
		go (AddressDimensionGCProducer{
			Repository: r.AddressDimensions, Jobs: r.Store,
			Interval: r.Config.AddressLibrary.ObjectGCInterval,
			Batch:    r.Config.AddressLibrary.ObjectGCBatch, Logf: log.Printf,
		}).Run(ctx)
		go (OperationJobScheduleDispatcher{
			Repository: r.Store,
			Registry:   registry,
			Logf:       log.Printf,
		}).Run(ctx)
		if r.FlowRollupService != nil {
			r.FlowRollupService.Logf = log.Printf
			go r.FlowRollupService.Run(ctx)
		}
		if r.FlowStorageService != nil {
			r.FlowStorageService.Logf = log.Printf
			go r.FlowStorageService.Run(ctx)
		}
		NewStoreMaintenance(r.Store, nil).Start(ctx)
		NewCollectorPlanTrustMaintenance(r.Store, nil).Start(ctx)
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
		if r.flowReconciliationOffsets != nil {
			r.flowReconciliationOffsets.Close()
		}
		if r.flowClickHouseNative != nil {
			r.flowClickHouseNative.Close()
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
