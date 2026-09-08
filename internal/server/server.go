// Package server is the KISS watchdog-server: a pure Go (Gin) HTTP service backed by
// MySQL (management authority) and ClickHouse (time-series + log/alert). It replaces the
// removed PocketBase hub. On startup it applies the embedded v2 MySQL baseline and records
// a traceable install status, then serves the domain API (docs/watchdog-kiss-architecture.md).
package server

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

// Server holds the runtime dependencies.
type Server struct {
	cfg    Config
	db     *sql.DB
	engine *gin.Engine
}

// New opens MySQL, applies the v2 baseline, and builds the router.
func New(cfg Config) (*Server, error) {
	db, err := sql.Open("mysql", cfg.MySQLDSN)
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
	s := &Server{cfg: cfg, db: db}
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
	s.engine = s.newRouter()
	return s, nil
}

// Run starts the HTTP listener (blocking).
func (s *Server) Run() error {
	return s.engine.Run(s.cfg.ListenAddr)
}

// DB exposes the connection pool for the domain packages wired in later work packages.
func (s *Server) DB() *sql.DB { return s.db }

// Close releases the database pool.
func (s *Server) Close() error { return s.db.Close() }
