// Package server is the KISS watchdog-server: a pure Go (Gin) HTTP service backed by
// MySQL (management authority) and ClickHouse (time-series + log/alert). It replaces the
// removed legacy hub. A fresh instance serves the installer without mutating an empty
// schema; an explicit install initializes MySQL, ClickHouse, and the first administrator.
package server

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/address"
	"github.com/cloudcache/watchdog/internal/agentplan"
	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/cloudcache/watchdog/internal/flowch"
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

	addressStore     *address.Store
	addressPublisher *address.Publisher
	addressObjects   address.DiskDimensionObjectStore
	addressArtifacts address.DiskArtifactStore
	addressTus       *tushandler.UnroutedHandler
	jobs             *opjob.Store
	billingStore     *billing.Store
	billingService   *billing.Service
	clickHouse       *flowch.NativeInserter
	snmpMetrics      *snmpch.Store
	flowQuery        *flowQueryService
	flowGeo          *flowGeoService
	workerCancel     context.CancelFunc
	snmpExportCancel context.CancelFunc
	billingCancel    context.CancelFunc

	vpnCandidateMaterializer *flowch.VPNCandidateMaterializer
	vpnCandidateRunner       *flowvpn.CandidateRunner
	vpnDetectCancel          context.CancelFunc
	flowExportCancel         context.CancelFunc

	agentPlanSigner agentplan.Signer
	agentPlanPublic ed25519.PublicKey
	agentPlanCancel context.CancelFunc
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
	if err := s.applyClickHouseSchema(ctx); err != nil {
		return err
	}
	if err := s.startClickHouse(ctx); err != nil {
		return fmt.Errorf("start ClickHouse: %w", err)
	}
	if err := s.startFlowQuery(); err != nil {
		return fmt.Errorf("start flow query: %w", err)
	}
	if err := s.startFlowGeo(); err != nil {
		return fmt.Errorf("start flow geo: %w", err)
	}
	if err := s.startVPNDetection(); err != nil {
		return fmt.Errorf("start VPN detection: %w", err)
	}
	if err := s.startFlowExports(); err != nil {
		return fmt.Errorf("start flow exports: %w", err)
	}
	if err := s.startAgentPlans(); err != nil {
		return fmt.Errorf("start agent plans: %w", err)
	}
	if err := s.startAddressLibrary(); err != nil {
		return fmt.Errorf("start address library: %w", err)
	}
	if err := s.startSNMPExports(); err != nil {
		return fmt.Errorf("start SNMP exports: %w", err)
	}
	if err := s.startBilling(); err != nil {
		return fmt.Errorf("start billing: %w", err)
	}
	return nil
}

func (s *Server) startBilling() error {
	s.billingStore = billing.NewStore(s.db)
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
	s.jobs = opjob.NewStore(s.db)
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

	tus, err := s.newAddressImportTusHandler()
	if err != nil {
		return fmt.Errorf("build address import tus handler: %w", err)
	}
	s.addressTus = tus
	return nil
}

// Run starts the HTTP listener (blocking).
func (s *Server) Run() error {
	return s.engine.Run(s.cfg.Server.Listen)
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
	if s.clickHouse != nil {
		s.clickHouse.Close()
		s.clickHouse = nil
	}
	s.snmpMetrics = nil
	s.flowQuery = nil
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
	native, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{
		Address: s.cfg.ClickHouse.Address, Database: s.cfg.ClickHouse.Database,
		User: s.cfg.ClickHouse.Username, Password: password,
		ClientName: "watchdog-server", MaxConns: 8, MinConns: 1,
	})
	if err != nil {
		return err
	}
	store, err := snmpch.New(native)
	if err != nil {
		native.Close()
		return err
	}
	if err := store.Ready(ctx); err != nil {
		native.Close()
		return fmt.Errorf("SNMP schema is not migrated: %w", err)
	}
	s.clickHouse = native
	s.snmpMetrics = store
	return nil
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
