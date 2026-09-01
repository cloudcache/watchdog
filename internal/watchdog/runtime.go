package watchdog

import (
	"context"
	"log"
	"net/http"
	"time"
)

type BackendRuntime struct {
	Config             BackendConfig
	Store              *MySQLStore
	MetricsClient      VictoriaMetricsClient
	ExportStore        DiskCSVExportStore
	ExportWorker       ExportWorker
	SNMPCollector      SNMPPollRunner
	SNMPDiscovery      SNMPDiscoveryEngine
	AggregateRollup    AggregateGraphRollup
	DiscoveryScheduler DiscoveryScheduler
	trapDispatcherFn   func(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error)
}

func NewBackendRuntime(ctx context.Context, cfg BackendConfig) (*BackendRuntime, error) {
	if err := ConfigureSNMPMIBRegistry(cfg.SNMP); err != nil {
		log.Printf("watchdog snmp mib registry: %v", err)
	}
	store, err := OpenMySQLStore(ctx, cfg.MySQL)
	if err != nil {
		return nil, err
	}
	metricsClient := VictoriaMetricsClient{BaseURL: cfg.VictoriaMetrics.BaseURL}
	exportStore := DiskCSVExportStore{Dir: cfg.Export.Dir}
	runtime := &BackendRuntime{
		Config:        cfg,
		Store:         store,
		MetricsClient: metricsClient,
		ExportStore:   exportStore,
	}
	runtime.ExportWorker = ExportWorker{
		Repo: store,
		Data: VictoriaMetricsExportDataProvider{
			Client:         metricsClient,
			Metric:         cfg.Export.Metric,
			Network:        store,
			CollectionStep: cfg.SNMPCollector.Interval,
		},
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
	return runtime, nil
}

func (r *BackendRuntime) Router(auth AuthContextAdapter) http.Handler {
	return NewAPIV1Router(APIV1RouterConfig{
		Auth:            auth,
		Targets:         r.Store,
		Agents:          r.Store,
		Network:         r.Store,
		Exports:         r.Store,
		ExportFiles:     r.ExportStore,
		Billing:         r.Store,
		AggregateGraphs: r.Store,
		Permissions:     r.Store,
		Retention:       r.Store,
		SNMP:            r.Store,
		SNMPDiscovery:   r.SNMPDiscovery,
		SNMPCollector:   r.Store,
		SeriesCleaner:   r.MetricsClient,
		DiscoveryJobs:   r.Store,
		TrapDispatcher:  r.trapDispatcherFn,
		Audit:           r.Store,
		AddressSets:     r.Store,
		Metrics: MetricsService{
			Client:   r.MetricsClient,
			Importer: r.MetricsClient,
		},
	})
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

func (r *BackendRuntime) Close() error {
	if r == nil || r.Store == nil {
		return nil
	}
	return r.Store.Close()
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
