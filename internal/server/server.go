// Package server is the KISS watchdog-server: a pure Go (Gin) HTTP service backed by
// MySQL (management authority) and ClickHouse (time-series + log/alert). It replaces the
// removed legacy hub. A fresh instance serves the installer without mutating an empty
// schema; an explicit install initializes MySQL and the first administrator. ClickHouse
// is initialized when reachable but does not prevent the management plane from starting.
package server

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/address"
	"github.com/cloudcache/watchdog/internal/agentplan"
	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowlifecycle"
	"github.com/cloudcache/watchdog/internal/flowvpn"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
	tushandler "github.com/tus/tusd/v2/pkg/handler"
)

// Server holds the runtime dependencies.
type Server struct {
	cfg           Config
	db            *sql.DB
	engine        *gin.Engine
	snmpDiscovery snmpDiscoveryRunner
	installMu     sync.Mutex
	installed     atomic.Bool
	runtimeReady  atomic.Bool
	clickHouseErr atomic.Pointer[string]

	addressStore         *address.Store
	addressPublisher     *address.Publisher
	vpnRuleSetPublisher  *address.Publisher
	addressObjects       address.DiskDimensionObjectStore
	addressArtifacts     address.DiskArtifactStore
	addressTus           *tushandler.UnroutedHandler
	jobs                 *opjob.Store
	billingStore         *billing.Store
	billingService       *billing.Service
	clickHouse           *flowch.NativeInserter
	clickHouseBatch      *flowch.NativeInserter
	snmpMetrics          *snmpch.Store
	flowQuery            *flowQueryService
	flowGeo              *flowGeoService
	workerCancel         context.CancelFunc
	clickHousePoolCancel context.CancelFunc
	snmpExportCancel     context.CancelFunc
	snmpDiscoverCancel   context.CancelFunc
	billingCancel        context.CancelFunc

	vpnCandidateMaterializer   *flowch.VPNCandidateMaterializer
	vpnCandidateRunner         *flowvpn.CandidateRunner
	vpnRuleSetCatalog          *flowvpn.RuleSetCatalog
	vpnRuleSetBootID           string
	vpnDetectCancel            context.CancelFunc
	flowExportCancel           context.CancelFunc
	flowReconciliationCancel   context.CancelFunc
	flowArchiveCancel          context.CancelFunc
	flowLifecycle              *flowlifecycle.Store
	flowRollup                 *flowch.RollupRunner
	flowDeleteEvidence         flowlifecycle.RawDayEvidenceReader
	flowArchiveDeleteEvidence  flowlifecycle.ArchiveMonthEvidenceReader
	flowReclassificationRunner *flowch.ReclassificationRunner
	flowReclassificationCancel context.CancelFunc
	flowReportReferenceMu      sync.Mutex
	flowReportReferenceCache   *flowReportReferenceCatalog

	agentPlanSigner     agentplan.Signer
	agentPlanPublic     ed25519.PublicKey
	agentPlanCancel     context.CancelFunc
	flowTrustBundle     []byte
	flowTrustChecksum   string
	flowTrustGeneration uint64
}

// New opens the configured MySQL database and builds the router. A fresh
// database remains uninitialized unless an explicit unattended password was
// configured; this lets the frontend redirect to the one-time installer.
func New(cfg Config) (*Server, error) {
	if err := ensureDatabase(cfg.MySQL.DSN); err != nil {
		return nil, fmt.Errorf("ensure database: %w", err)
	}
	db, err := sql.Open("mysql", cfg.MySQL.DSN)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	discovery, err := newSNMPDiscoveryRunner(cfg.SNMP)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure snmp discovery: %w", err)
	}
	s := &Server{cfg: cfg, db: db, snmpDiscovery: discovery}
	status, err := GetInstallStatus(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("read install status: %w", err)
	}
	s.installed.Store(status.Installed)
	if !status.Installed && strings.TrimSpace(cfg.Admin.Password) != "" {
		request := installRequest{Username: cfg.Admin.Username, Password: cfg.Admin.Password}
		if err := s.completeFreshInstall(ctx, request); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("unattended install: %w", err)
		}
	} else if status.Installed {
		if err := s.prepareRuntime(ctx); err != nil {
			s.stopRuntime()
			_ = db.Close()
			return nil, err
		}
		s.runtimeReady.Store(true)
	}
	s.engine = s.newRouter()
	return s, nil
}

func (s *Server) prepareRuntime(ctx context.Context) error {
	if err := ApplyMySQLSchema(ctx, s.db, schema.MySQL); err != nil {
		return fmt.Errorf("apply MySQL schema: %w", err)
	}
	if err := EnsureRBACSeed(ctx, s.db); err != nil {
		return fmt.Errorf("seed RBAC: %w", err)
	}
	if err := s.ensureBuiltinMIBModules(ctx); err != nil {
		return fmt.Errorf("seed built-in MIB modules: %w", err)
	}
	// operation_jobs is a management-plane dependency shared by address, agent,
	// SNMP and Flow workers. Create it before any worker checks the store.
	s.jobs = opjob.NewStore(s.db)
	s.flowLifecycle = flowlifecycle.NewStore(s.db)
	if err := s.startAgentPlans(); err != nil {
		return fmt.Errorf("start agent plans: %w", err)
	}
	if err := s.startAddressLibrary(); err != nil {
		return fmt.Errorf("start address library: %w", err)
	}
	if err := s.startFlowEnrichment(ctx); err != nil {
		return fmt.Errorf("start Flow enrichment publication: %w", err)
	}
	if err := s.startFlowGeo(); err != nil {
		return fmt.Errorf("start flow geo: %w", err)
	}

	// ClickHouse is the sole telemetry authority, but it is not an auth/device
	// dependency. A configuration, authentication or availability failure keeps
	// the management plane running and is exposed by health and query 503s. A
	// restart retries schema application and connection after the operator fixes
	// the dependency.
	if err := s.prepareClickHouse(ctx); err != nil {
		s.setClickHouseError(err)
		if strings.TrimSpace(s.cfg.ClickHouse.Address) != "" || strings.TrimSpace(s.cfg.ClickHouse.Database) != "" {
			log.Printf("watchdog ClickHouse unavailable; management plane remains available: %v", err)
		}
	} else {
		s.setClickHouseError(nil)
	}
	if err := s.startFlowReconciliation(); err != nil {
		log.Printf("watchdog Flow reconciliation unavailable; management plane remains available: %v", err)
	}
	if err := s.startFlowArchive(); err != nil {
		log.Printf("watchdog Flow archive lifecycle unavailable; management plane remains available: %v", err)
	}
	if err := s.startFlowQuery(); err != nil {
		return fmt.Errorf("start flow query: %w", err)
	}
	if err := s.startFlowReclassification(); err != nil {
		return fmt.Errorf("start flow historical reclassification: %w", err)
	}
	if err := s.startVPNDetection(); err != nil {
		return fmt.Errorf("start VPN detection: %w", err)
	}
	if err := s.startFlowExports(); err != nil {
		return fmt.Errorf("start flow exports: %w", err)
	}
	if err := s.startSNMPExports(); err != nil {
		return fmt.Errorf("start SNMP exports: %w", err)
	}
	s.startSNMPDiscoveryReconcile()
	if err := s.startBilling(); err != nil {
		return fmt.Errorf("start billing: %w", err)
	}
	return nil
}

func (s *Server) prepareClickHouse(ctx context.Context) error {
	address := strings.TrimSpace(s.cfg.ClickHouse.Address)
	database := strings.TrimSpace(s.cfg.ClickHouse.Database)
	if address == "" && database == "" {
		return errors.New("ClickHouse is not configured")
	}
	if address == "" || database == "" {
		return errors.New("ClickHouse configuration requires both address and database")
	}
	if err := s.applyClickHouseSchema(ctx); err != nil {
		return err
	}
	if err := s.startClickHouse(ctx); err != nil {
		return fmt.Errorf("start ClickHouse: %w", err)
	}
	return nil
}

func (s *Server) setClickHouseError(err error) {
	if err == nil {
		s.clickHouseErr.Store(nil)
		return
	}
	message := err.Error()
	s.clickHouseErr.Store(&message)
}

func (s *Server) clickHouseError() string {
	if message := s.clickHouseErr.Load(); message != nil {
		return *message
	}
	return ""
}

func (s *Server) startBilling() error {
	s.billingStore = billing.NewStoreWithLimits(s.db, billing.Limits{
		MaxAccountPorts: s.cfg.Billing.MaxAccountPorts, MaxPageSize: s.cfg.Billing.MaxPageSize, MaxPeriodDuration: s.cfg.Billing.MaxPeriodDuration,
		MaxExportRows: s.cfg.Billing.MaxExportRows, MaxPublicationRefs: s.cfg.Billing.MaxPublicationRefs,
		MaxPublicationBytes: s.cfg.Billing.MaxPublicationBytes,
	})
	if s.snmpMetrics == nil || s.clickHouse == nil {
		return nil
	}
	service, err := billing.NewService(s.billingStore, s.snmpMetrics, s.clickHouse)
	if err != nil {
		return err
	}
	s.billingService = service
	s.startBillingJobs()
	return nil
}

// startAddressLibrary wires the de-tenanted address publish chain (internal/address)
// and starts the async snapshot-build worker on the opjob engine.
func (s *Server) startAddressLibrary() error {
	s.addressStore = address.NewStore(s.db)
	if s.jobs == nil {
		s.jobs = opjob.NewStore(s.db)
	}
	s.addressObjects = address.DiskDimensionObjectStore{Dir: s.cfg.Address.SnapshotDir}
	s.addressArtifacts = address.DiskArtifactStore{Dir: s.cfg.Address.ArtifactDir, MaxBytes: s.cfg.Address.MaxUploadBytes}
	publisher, err := address.NewPublisher(s.addressStore, s.addressObjects)
	if err != nil {
		return err
	}
	s.addressPublisher = publisher

	workerCtx, cancel := context.WithCancel(context.Background())
	s.workerCancel = cancel
	importWorker := &opjob.Worker{
		Repo: s.jobs, JobType: address.AddressImportJobType, Owner: "watchdog-server/address-import",
		Handler: address.NewAddressImportJobHandler(s.addressStore, s.addressArtifacts, 0),
	}
	go importWorker.Run(workerCtx)
	buildWorker := &opjob.Worker{
		Repo: s.jobs, JobType: address.AddressSnapshotBuildJob, Owner: "watchdog-server/address",
		Handler: address.NewAddressSnapshotBuildJobHandler(publisher),
	}
	go buildWorker.Run(workerCtx)
	enrichmentWorker := &opjob.Worker{
		Repo: s.jobs, JobType: flowEnrichmentPublishJobType, Owner: "watchdog-server/flow-enrichment",
		Handler: s.flowEnrichmentPublishHandler(),
	}
	go enrichmentWorker.Run(workerCtx)
	vpnPublisher, err := address.NewScopedPublisher(s.addressStore, s.addressObjects, address.VPNRuleSetPublicationScope)
	if err != nil {
		return err
	}
	s.vpnRuleSetPublisher = vpnPublisher
	vpnWorker := &opjob.Worker{
		Repo: s.jobs, JobType: vpnRuleSetPublishJobType, Owner: "watchdog-server/vpn-rule-set",
		Handler: s.vpnRuleSetPublishHandler(),
	}
	go vpnWorker.Run(workerCtx)

	tus, err := s.newAddressImportTusHandler()
	if err != nil {
		return fmt.Errorf("build address import tus handler: %w", err)
	}
	s.addressTus = tus
	return nil
}

// Run starts the HTTP listener (blocking).
func (s *Server) Run() error {
	// ReadHeaderTimeout bounds slow-header (slowloris) clients and IdleTimeout
	// reclaims idle keep-alive connections. ReadTimeout/WriteTimeout are left
	// unset on purpose: flow reports run up to the report deadline (~120s) and
	// address imports stream large tus uploads, both of which a fixed
	// whole-request timeout would truncate.
	server := &http.Server{
		Addr:              s.cfg.Server.Listen,
		Handler:           s.engine,
		ReadHeaderTimeout: 20 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return server.ListenAndServe()
}

// DB exposes the connection pool for the domain packages wired in later work packages.
func (s *Server) DB() *sql.DB { return s.db }

// Close stops background workers and releases the database pool.
func (s *Server) Close() error {
	s.stopRuntime()
	return s.db.Close()
}

func (s *Server) stopRuntime() {
	if s.agentPlanCancel != nil {
		s.agentPlanCancel()
		s.agentPlanCancel = nil
	}
	if s.workerCancel != nil {
		s.workerCancel()
		s.workerCancel = nil
	}
	if s.snmpExportCancel != nil {
		s.snmpExportCancel()
		s.snmpExportCancel = nil
	}
	if s.snmpDiscoverCancel != nil {
		s.snmpDiscoverCancel()
		s.snmpDiscoverCancel = nil
	}
	if s.billingCancel != nil {
		s.billingCancel()
		s.billingCancel = nil
	}
	if s.vpnDetectCancel != nil {
		s.vpnDetectCancel()
		s.vpnDetectCancel = nil
	}
	if s.flowExportCancel != nil {
		s.flowExportCancel()
		s.flowExportCancel = nil
	}
	if s.flowReconciliationCancel != nil {
		s.flowReconciliationCancel()
		s.flowReconciliationCancel = nil
	}
	if s.flowArchiveCancel != nil {
		s.flowArchiveCancel()
		s.flowArchiveCancel = nil
	}
	if s.flowReclassificationCancel != nil {
		s.flowReclassificationCancel()
		s.flowReclassificationCancel = nil
	}
	if s.clickHousePoolCancel != nil {
		s.clickHousePoolCancel()
		s.clickHousePoolCancel = nil
	}
	if s.clickHouse != nil {
		s.clickHouse.Close()
		s.clickHouse = nil
	}
	if s.clickHouseBatch != nil {
		s.clickHouseBatch.Close()
		s.clickHouseBatch = nil
	}
	s.snmpMetrics = nil
	s.flowQuery = nil
	s.flowLifecycle = nil
	s.flowRollup = nil
	s.flowDeleteEvidence = nil
	s.flowArchiveDeleteEvidence = nil
	s.flowReclassificationRunner = nil
	s.runtimeReady.Store(false)
}

func (s *Server) startClickHouse(ctx context.Context) error {
	// Direct unit tests may construct Config without optional external services.
	// Loaded production configuration always supplies the ClickHouse endpoint.
	if strings.TrimSpace(s.cfg.ClickHouse.Address) == "" || strings.TrimSpace(s.cfg.ClickHouse.Database) == "" {
		return nil
	}
	password, err := clickHousePassword(s.cfg.ClickHouse.PasswordFile)
	if err != nil {
		return err
	}
	tlsConfig, err := s.cfg.ClickHouse.TLS.ClientConfig()
	if err != nil {
		return fmt.Errorf("ClickHouse TLS: %w", err)
	}
	maxConns, minConns, batchConns := clickHousePoolSizes(s.cfg.ClickHouse)
	newPool := func(clientName string, conns int32, operationTimeout time.Duration) (*flowch.NativeInserter, error) {
		return flowch.NewNativeInserter(ctx, flowch.NativeConfig{
			Address: s.cfg.ClickHouse.Address, Database: s.cfg.ClickHouse.Database,
			User: s.cfg.ClickHouse.Username, Password: password,
			ClientName: clientName, OperationTimeout: operationTimeout,
			MaxConns: conns, MinConns: minConns, TLS: tlsConfig,
		})
	}
	// Interactive pool serves flow queries, SNMP reads and billing; the batch pool
	// serves the background rollup/reclassification/reconciliation/VPN jobs, so a
	// long job cannot occupy every connection and stall user queries behind an
	// Acquire wait hidden inside the operation timeout.
	native, err := newPool("watchdog-server", maxConns, s.cfg.ClickHouse.OperationTimeout)
	if err != nil {
		return err
	}
	batch, err := newPool("watchdog-server-batch", batchConns, s.cfg.ClickHouse.BatchOperationTimeout)
	if err != nil {
		native.Close()
		return err
	}
	store, err := snmpch.NewWithQueryLimits(native, snmpQueryLimits(s.cfg.SNMP))
	if err != nil {
		native.Close()
		batch.Close()
		return err
	}
	if err := store.Ready(ctx); err != nil {
		native.Close()
		batch.Close()
		return fmt.Errorf("SNMP schema is not migrated: %w", err)
	}
	s.clickHouse = native
	s.clickHouseBatch = batch
	s.snmpMetrics = store
	monitorCtx, cancel := context.WithCancel(context.Background())
	s.clickHousePoolCancel = cancel
	go s.monitorClickHousePools(monitorCtx, native, batch)
	log.Printf("ClickHouse pools ready: interactive=%d batch=%d min=%d operation_timeout=%s batch_operation_timeout=%s tls=%t",
		maxConns, batchConns, minConns, s.cfg.ClickHouse.OperationTimeout, s.cfg.ClickHouse.BatchOperationTimeout, tlsConfig != nil)
	return nil
}

// clickHousePoolSizes normalizes configured pool sizes, falling back to
// backward-compatible defaults when a field is unset.
func clickHousePoolSizes(cfg ClickHouseConfig) (maxConns, minConns, batchConns int32) {
	maxConns, minConns, batchConns = cfg.MaxConns, cfg.MinConns, cfg.BatchMaxConns
	if maxConns <= 0 {
		maxConns = 8
	}
	if minConns < 0 {
		minConns = 0
	}
	if minConns > maxConns {
		minConns = maxConns
	}
	if batchConns <= 0 {
		batchConns = 1
	}
	return maxConns, minConns, batchConns
}

// monitorClickHousePools logs a warning when a pool has accumulated Acquire
// waits since the last check, surfacing the pool pressure that is otherwise
// hidden inside the per-operation timeout. Stat is mutex-protected, so it is
// safe to call while the pool is closing.
func (s *Server) monitorClickHousePools(ctx context.Context, interactive, batch *flowch.NativeInserter) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	var lastInteractive, lastBatch int64
	report := func(name string, inserter *flowch.NativeInserter, last *int64) {
		if inserter == nil {
			return
		}
		stat, ok := inserter.Stat()
		if !ok || stat.EmptyAcquires <= *last {
			return
		}
		log.Printf("ClickHouse pool %s pressured: acquired=%d/%d idle=%d empty_acquires=%d wait=%s",
			name, stat.Acquired, stat.Max, stat.Idle, stat.EmptyAcquires, stat.EmptyAcquireWait)
		*last = stat.EmptyAcquires
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			report("interactive", interactive, &lastInteractive)
			report("batch", batch, &lastBatch)
		}
	}
}

// ensureDatabase creates the target schema if it does not exist, so the server can
// bootstrap from an empty MySQL instance ("空库启动") without a manual CREATE DATABASE.
func ensureDatabase(dsn string) error {
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	if parsed.DBName == "" {
		return nil
	}
	name := parsed.DBName
	if !validDatabaseName(name) {
		return fmt.Errorf("database name %q contains unsupported characters", name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	err = target.PingContext(ctx)
	_ = target.Close()
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1049 {
		return err
	}

	parsed.DBName = ""
	admin, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		return err
	}
	defer admin.Close()
	_, err = admin.ExecContext(ctx,
		"CREATE DATABASE IF NOT EXISTS `"+name+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
	return err
}

func validDatabaseName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '$' {
			return false
		}
	}
	return true
}
