package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/henrygd/beszel/internal/watchdog"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := watchdog.LoadBackendConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	runtime, err := watchdog.NewBackendRuntime(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer runtime.Close()
	tenantID := watchdog.ID(getenv("WATCHDOG_DEV_TENANT_ID", string(cfg.SNMPCollector.TenantID)))
	userID := watchdog.ID(getenv("WATCHDOG_DEV_USER_ID", "user_dev"))
	if getenv("WATCHDOG_DEV_SNMP_COLLECTOR", "1") != "0" {
		resolvedSNMPConfig := cfg.SNMPCollector
		resolvedSNMPConfig.TenantID = tenantID
		if err := watchdog.ValidateSNMPCollectorConfig(resolvedSNMPConfig); err != nil {
			log.Fatal(err)
		}
		go func() {
			if err := runtime.RunSNMPCollectorScheduler(ctx, tenantID, cfg.SNMPCollector.Interval, cfg.SNMPCollector.PollLimit); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("watchdog snmp collector stopped: %v", err)
			}
		}()
		go func() {
			if err := runtime.DiscoveryScheduler.RunLoop(ctx, cfg.SNMPCollector.DiscoveryInterval, cfg.SNMPCollector.DiscoveryBatch); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("watchdog discovery scheduler stopped: %v", err)
			}
		}()
		go func() {
			if err := runtime.RunExportWorker(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("watchdog export worker stopped: %v", err)
			}
		}()
	}
	if getenv("WATCHDOG_DEV_AGGREGATE_ROLLUP", "1") != "0" {
		go func() {
			if err := runtime.RunAggregateGraphRollup(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("watchdog aggregate rollup stopped: %v", err)
			}
		}()
	}
	addr := getenv("WATCHDOG_DEV_ADDR", "127.0.0.1:8091")
	server := &http.Server{
		Addr: addr,
		Handler: runtime.Router(func(*http.Request) (watchdog.AuthContext, error) {
			return watchdog.AuthContext{
				TenantID: tenantID,
				UserID:   userID,
				IsAdmin:  true,
			}, nil
		}),
	}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	log.Printf("watchdog dev API listening on http://%s", addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
