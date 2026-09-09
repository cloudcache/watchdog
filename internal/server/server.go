// Package server is the KISS watchdog-server: a pure Go (Gin) HTTP service backed by
// MySQL (management authority) and ClickHouse (time-series + log/alert). It replaces the
// removed legacy hub. On startup it applies the embedded v2 MySQL baseline and records
// a traceable install status, then serves the domain API (docs/watchdog-kiss-architecture.md).
package server

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/address"
	"github.com/cloudcache/watchdog/internal/agentplan"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
)

// Server holds the runtime dependencies.
type Server struct {
	cfg           Config
	db            *sql.DB
	engine        *gin.Engine
	snmpDiscovery snmpDiscoveryRunner

	addressStore     *address.Store
	addressPublisher *address.Publisher
	addressKeys      address.AddressDimensionPublicKeyResolver
	addressObjects   address.DiskDimensionObjectStore
	addressArtifacts address.DiskArtifactStore
	jobs             *opjob.Store
	clickHouse       *flowch.NativeInserter
	snmpMetrics      *snmpch.Store
	workerCancel     context.CancelFunc
	snmpExportCancel context.CancelFunc

	agentPlanSigner agentplan.Signer
	agentPlanPublic ed25519.PublicKey
	agentPlanCancel context.CancelFunc
}

// New opens MySQL, applies the v2 baseline, and builds the router.
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	if err := EnsureRBACSeed(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("seed rbac: %w", err)
	}
	if err := s.EnsureFirstAdmin(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("bootstrap admin: %w", err)
	}
	if err := s.startClickHouse(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("start ClickHouse: %w", err)
	}
	if err := s.startAgentPlans(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("start agent plans: %w", err)
	}
	if err := s.startAddressLibrary(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("start address library: %w", err)
	}
	if err := s.startSNMPExports(); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("start SNMP exports: %w", err)
	}
	s.engine = s.newRouter()
	return s, nil
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

	trustedKeys := make([]address.AddressDimensionTrustedKeyConfig, 0, len(s.cfg.Address.TrustedKeys))
	for _, key := range s.cfg.Address.TrustedKeys {
		trustedKeys = append(trustedKeys, address.AddressDimensionTrustedKeyConfig{KeyID: key.KeyID, PublicKeyFile: key.PublicKeyFile})
	}
	resolver, err := address.LoadAddressDimensionPublicKeyResolver(trustedKeys)
	if err != nil {
		return fmt.Errorf("load address dimension trusted keys: %w", err)
	}
	s.addressKeys = resolver

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
	if s.agentPlanCancel != nil {
		s.agentPlanCancel()
	}
	if s.workerCancel != nil {
		s.workerCancel()
	}
	if s.snmpExportCancel != nil {
		s.snmpExportCancel()
	}
	if s.clickHouse != nil {
		s.clickHouse.Close()
	}
	return s.db.Close()
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
