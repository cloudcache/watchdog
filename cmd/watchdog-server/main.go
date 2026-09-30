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
	checkConfig := flag.Bool("check-config", false, "load and validate the configuration, then exit without starting")
	flag.Parse()
	cfg, err := server.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("watchdog-server: configuration failed: %v", err)
	}
	if *checkConfig {
		log.Printf("watchdog-server: configuration is valid")
		return
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
