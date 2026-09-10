// Command watchdog-server runs the management API. The frontend is a separate
// process and reaches this service through its configured API_URL.
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
	log.Printf("watchdog-server listening on %s", cfg.Server.Listen)
	if err := srv.Run(); err != nil {
		log.Fatalf("watchdog-server: %v", err)
	}
}
