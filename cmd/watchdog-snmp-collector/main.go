package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/henrygd/beszel/internal/watchdog"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	tenantID := flag.String("tenant", "tenant_dev", "tenant id")
	deviceID := flag.String("device", "", "network device id")
	discover := flag.Bool("discover", true, "run collector discovery and import recipes")
	poll := flag.Bool("poll", true, "run due recipe polling")
	loop := flag.Bool("loop", false, "keep polling due recipes until interrupted")
	interval := flag.Duration("interval", time.Minute, "poll loop interval")
	limit := flag.Int("limit", 500, "maximum due recipes to poll")
	flag.Parse()

	if *deviceID == "" {
		log.Fatal("device id is required")
	}
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

	tenant := watchdog.ID(*tenantID)
	device, err := runtime.Store.GetDevice(ctx, tenant, watchdog.ID(*deviceID))
	if err != nil {
		log.Fatal(err)
	}
	target, err := runtime.Store.GetTarget(ctx, tenant, device.TargetID)
	if err != nil {
		log.Fatal(err)
	}
	profile, err := runtime.Store.GetSNMPProfile(ctx, tenant, device.SNMPProfileID)
	if err != nil {
		log.Fatal(err)
	}
	profile = watchdog.ApplyDeviceSNMPOverrides(profile, device)

	if *discover {
		query := watchdog.NewGoSNMPCollectorQueryEngine()
		engine, err := watchdog.NewSNMPDiscoveryEngineFromRepository(ctx, runtime.Store, query, watchdog.DefaultSNMPCollectorModuleRegistry())
		if err != nil {
			log.Fatal(err)
		}
		discovery, err := engine.Discover(ctx, watchdog.SNMPDiscoveryEngineRequest{
			TenantID: tenant,
			TargetID: device.TargetID,
			Target: watchdog.SNMPCollectorTarget{
				Host: target.Host,
				Port: device.SNMPPort,
			},
			Device:  device,
			Profile: profile,
		})
		if err != nil {
			log.Fatal(err)
		}
		report, err := watchdog.ImportSNMPCollectorDiscoveryResult(ctx, runtime.Store, runtime.Store, tenant, device, discovery)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("discovery imported: device=%s ports=%d sensors=%d physical=%d bgp=%d vlans=%d lags=%d recipes=%d events=%d modules=%d\n",
			report.Device.ID, report.Ports, report.Sensors, report.PhysicalEntities, report.BGPSessions, report.VLANs, report.LAGs, report.Recipes, report.Events, report.DeviceModules)
	}
	if *poll {
		result, err := runtime.RunSNMPCollectorPoll(ctx, tenant, *limit)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("poll completed: recipes=%d devices=%d samples=%d failed=%d\n", result.RecipeCount, result.DeviceCount, result.SampleCount, result.FailedCount)
	}
	if *loop {
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result, err := runtime.RunSNMPCollectorPoll(ctx, tenant, *limit)
				if err != nil {
					log.Printf("poll failed: %v", err)
					continue
				}
				fmt.Printf("poll completed: recipes=%d devices=%d samples=%d failed=%d\n", result.RecipeCount, result.DeviceCount, result.SampleCount, result.FailedCount)
			}
		}
	}
}
