// Command watchdog-server is the KISS single-domain HTTP service (Gin + MySQL + ClickHouse),
// replacing the removed PocketBase hub. It applies the v2 baseline on startup and serves the
// domain API and (optionally) the built UI.
package main

import (
	"log"

	"github.com/cloudcache/watchdog/internal/server"
)

func main() {
	cfg := server.LoadConfig()
	srv, err := server.New(cfg)
	if err != nil {
		log.Fatalf("watchdog-server: startup failed: %v", err)
	}
	defer srv.Close()
	log.Printf("watchdog-server listening on %s (mysql applied, install status ready)", cfg.ListenAddr)
	if err := srv.Run(); err != nil {
		log.Fatalf("watchdog-server: %v", err)
	}
}
