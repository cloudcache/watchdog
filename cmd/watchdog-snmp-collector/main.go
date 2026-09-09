package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cloudcache/watchdog/internal/agentplan"
	"github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/google/uuid"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	tenantID := flag.String("tenant", "", "tenant id (overrides config)")
	deviceID := flag.String("device", "", "network device id")
	discover := flag.Bool("discover", true, "run collector discovery and import recipes")
	poll := flag.Bool("poll", true, "run due recipe polling")
	loop := flag.Bool("loop", false, "keep polling due recipes until interrupted")
	interval := flag.Duration("interval", 0, "poll loop interval (overrides config)")
	limit := flag.Int("limit", 0, "maximum due recipes to poll (overrides config)")
	controlPlaneURL := flag.String("control-plane-url", "", "Watchdog API base URL for agent management")
	agentID := flag.String("agent-id", "watchdog-snmp-collector", "registered SNMP agent ID")
	agentTokenFile := flag.String("agent-token-file", "", "file containing the SNMP agent machine token")
	agentEnrollmentFile := flag.String("agent-enrollment-token-file", "", "one-time enrollment token file")
	agentPlanPublicKey := flag.String("agent-plan-public-key", "", "agent plan Ed25519 public key file")
	agentPlanLKG := flag.String("agent-plan-lkg", "", "durable agent plan LKG file")
	agentPlanCheck := flag.Bool("agent-plan-check", false, "register/sync/apply the agent plan, then exit")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *discover && strings.TrimSpace(*deviceID) == "" && !*loop && !*agentPlanCheck {
		log.Fatal("device id is required")
	}
	if !*discover && !*poll {
		log.Fatal("at least one of discover or poll must be enabled")
	}
	if *interval < 0 || *limit < 0 {
		log.Fatal("interval and limit must not be negative")
	}
	if *agentPlanCheck {
		resolvedInterval, resolvedLimit := time.Minute, 100
		agentRuntime := snmpAgentRuntime(*controlPlaneURL, *agentID, *agentTokenFile, *agentEnrollmentFile, *agentPlanPublicKey, *agentPlanLKG, uuid.NewString())
		if !agentRuntime.Enabled() {
			log.Fatal("agent plan public key, LKG, and control-plane URL are required")
		}
		result, _, err := agentRuntime.Sync(ctx, func(_ context.Context, spec agentplan.Spec) error {
			return applySNMPAgentPlan(&resolvedInterval, &resolvedLimit, spec)
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("SNMP agent plan applied: version=%d source=%s", result.Envelope.Metadata.PlanVersion, result.Source)
		if result.AckError != nil {
			log.Fatal(result.AckError)
		}
		return
	}
	cfg, err := watchdog.LoadBackendConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	resolvedTenantID := cfg.SNMPCollector.TenantID
	if strings.TrimSpace(*tenantID) != "" {
		resolvedTenantID = watchdog.ID(strings.TrimSpace(*tenantID))
	}
	resolvedSNMPConfig := cfg.SNMPCollector
	resolvedSNMPConfig.TenantID = resolvedTenantID
	if err := watchdog.ValidateSNMPCollectorConfig(resolvedSNMPConfig); err != nil {
		log.Fatal(err)
	}
	resolvedInterval := cfg.SNMPCollector.Interval
	if *interval > 0 {
		resolvedInterval = *interval
	}
	resolvedLimit := cfg.SNMPCollector.PollLimit
	if *limit > 0 {
		resolvedLimit = *limit
	}
	agentRuntime := snmpAgentRuntime(*controlPlaneURL, *agentID, *agentTokenFile, *agentEnrollmentFile, *agentPlanPublicKey, *agentPlanLKG, uuid.NewString())
	if agentRuntime.Enabled() {
		result, token, err := agentRuntime.Sync(ctx, func(_ context.Context, spec agentplan.Spec) error {
			return applySNMPAgentPlan(&resolvedInterval, &resolvedLimit, spec)
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("SNMP agent plan applied: version=%d source=%s", result.Envelope.Metadata.PlanVersion, result.Source)
		go agentRuntime.RunHeartbeats(ctx, token, 30*time.Second, func(err error) {
			log.Printf("SNMP agent heartbeat: %v", err)
			if errors.Is(err, agentplan.ErrUnauthorized) {
				stop()
			}
		})
	}
	runtime, err := watchdog.NewBackendRuntime(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer runtime.Close()

	tenant := resolvedTenantID

	if *discover && !*loop {
		device, err := runtime.Store.GetDevice(ctx, tenant, watchdog.ID(strings.TrimSpace(*deviceID)))
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
		fmt.Printf("discovery imported: device=%s ports=%d addresses=%d sensors=%d physical=%d bgp=%d vlans=%d lags=%d recipes=%d events=%d modules=%d\n",
			report.Device.ID, report.Ports, report.InterfaceAddresses, report.Sensors, report.PhysicalEntities, report.BGPSessions, report.VLANs, report.LAGs, report.Recipes, report.Events, report.DeviceModules)
	}
	if *poll && !*loop {
		result, err := runtime.RunSNMPCollectorPoll(ctx, tenant, resolvedLimit)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("poll completed: recipes=%d devices=%d samples=%d failed=%d\n", result.RecipeCount, result.DeviceCount, result.SampleCount, result.FailedCount)
	}
	if *loop {
		if *discover {
			if strings.TrimSpace(*deviceID) != "" {
				if err := runtime.Store.EnqueueDiscoveryJob(ctx, tenant, watchdog.ID(strings.TrimSpace(*deviceID)), "collector_start"); err != nil {
					log.Fatal(err)
				}
			}
			go func() {
				if err := runtime.DiscoveryScheduler.RunLoop(ctx, tenant, cfg.SNMPCollector.DiscoveryInterval, cfg.SNMPCollector.DiscoveryBatch); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("discovery scheduler stopped: %v", err)
				}
			}()
		}
		if *poll {
			if err := runtime.RunSNMPCollectorScheduler(ctx, tenant, resolvedInterval, resolvedLimit); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("poll scheduler stopped: %v", err)
			}
			return
		}
		<-ctx.Done()
	}
}

type snmpAgentPlanConfig struct {
	IntervalSeconds *int64 `json:"interval_seconds"`
	PollLimit       *int   `json:"poll_limit"`
}

func applySNMPAgentPlan(interval *time.Duration, limit *int, spec agentplan.Spec) error {
	var config snmpAgentPlanConfig
	if err := agentplan.DecodeConfig(spec, &config); err != nil {
		return err
	}
	if config.IntervalSeconds != nil {
		if *config.IntervalSeconds < 5 || *config.IntervalSeconds > 3600 {
			return errors.New("agent plan SNMP interval must be 5..3600 seconds")
		}
		*interval = time.Duration(*config.IntervalSeconds) * time.Second
	}
	if config.PollLimit != nil {
		if *config.PollLimit < 1 || *config.PollLimit > 10000 {
			return errors.New("agent plan SNMP poll_limit must be 1..10000")
		}
		*limit = *config.PollLimit
	}
	return nil
}

func snmpAgentRuntime(baseURL, agentID, tokenFile, enrollmentFile, publicKey, lkg, bootID string) agentplan.RuntimeConfig {
	if strings.TrimSpace(publicKey) == "" && strings.TrimSpace(lkg) == "" && strings.TrimSpace(enrollmentFile) == "" {
		return agentplan.RuntimeConfig{}
	}
	return agentplan.RuntimeConfig{
		BaseURL: baseURL, AgentID: strings.TrimSpace(agentID), Name: strings.TrimSpace(agentID), Kind: "snmp",
		Role: "snmp", Mode: "push", SoftwareVersion: "watchdog-snmp-collector-v1", APIVersion: "v1",
		Capabilities: []string{"snmp.poll/v2"}, TokenFile: tokenFile, EnrollmentFile: enrollmentFile,
		PublicKeyFile: publicKey, LKGFile: lkg, BootID: bootID,
	}
}
