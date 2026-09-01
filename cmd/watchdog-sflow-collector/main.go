package main

import (
	"context"
	"flag"
	"log"
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
	store, err := watchdog.OpenMySQLStore(ctx, cfg.MySQL)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	tenantID := cfg.SFlowCollector.TenantID
	if tenantID == "" {
		tenantID = "tenant_dev"
	}
	vlogsURL := cfg.SFlowCollector.VLogsURL
	if vlogsURL == "" {
		vlogsURL = cfg.VictoriaLogs.BaseURL
	}

	collector := &watchdog.SFlowCollector{
		ListenAddr:           cfg.SFlowCollector.Listen,
		Network:              store,
		AddressSets:          store,
		VMClient:             watchdog.VictoriaMetricsClient{BaseURL: cfg.VictoriaMetrics.BaseURL},
		VLogsURL:             vlogsURL,
		TenantID:             tenantID,
		AggInterval:          cfg.SFlowCollector.AggInterval,
		PrefixSyncInterval:   cfg.SFlowCollector.PrefixSyncInterval,
	}

	log.Printf("starting sflow collector: listen=%s tenant=%s vm=%s vlogs=%s",
		cfg.SFlowCollector.Listen, tenantID, cfg.VictoriaMetrics.BaseURL, vlogsURL)

	if err := collector.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
