// Command watchdog-server is the KISS single-domain HTTP service (Gin + MySQL + ClickHouse),
// replacing the removed legacy hub. It applies the v2 baseline on startup and serves only the
// domain API; the frontend runs as a separate process.
package main

import (
	"flag"
	"log"

	"github.com/cloudcache/watchdog/internal/server"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog server YAML configuration")
	flag.Parse()
	cfg, err := server.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("watchdog-server: configuration failed: %v", err)
	}
	srv, err := server.New(cfg)
	if err != nil {
		log.Fatalf("watchdog-server: startup failed: %v", err)
	}
	defer srv.Close()
	log.Printf("watchdog-server listening on %s (mysql applied, install status ready)", cfg.Server.Listen)
	if err := srv.Run(); err != nil {
		log.Fatalf("watchdog-server: %v", err)
	}
}
