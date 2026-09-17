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
	"github.com/cloudcache/watchdog/internal/server"
	"github.com/google/uuid"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
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
	agentLifecycle := make(chan error, 1)
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
	cfg, err := server.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	resolvedInterval := cfg.SNMP.PollInterval
	if resolvedInterval <= 0 {
		resolvedInterval = time.Minute
	}
	if *interval > 0 {
		resolvedInterval = *interval
	}
	resolvedLimit := cfg.SNMP.PollLimit
	if resolvedLimit <= 0 {
		resolvedLimit = 500
	}
	if *limit > 0 {
		resolvedLimit = *limit
	}
	agentRuntime := snmpAgentRuntime(*controlPlaneURL, *agentID, *agentTokenFile, *agentEnrollmentFile, *agentPlanPublicKey, *agentPlanLKG, uuid.NewString())
	if agentRuntime.Enabled() {
		result, token, err := agentRuntime.Sync(ctx, func(_ context.Context, spec agentplan.Spec) error {
			return applySNMPAgentPlan(&resolvedInterval, &resolvedLimit, spec)
		})
		if errors.Is(err, agentplan.ErrUnauthorized) {
			log.Printf("SNMP agent credential is revoked; stopping without restart")
			return
		}
		if err != nil && !errors.Is(err, agentplan.ErrNoPlan) {
			log.Fatal(err)
		}
		appliedPlanVersion := result.Envelope.Metadata.PlanVersion
		if errors.Is(err, agentplan.ErrNoPlan) {
			log.Printf("SNMP agent registered without a desired plan; using bootstrap values until a plan is published")
		} else {
			log.Printf("SNMP agent plan applied: version=%d source=%s", appliedPlanVersion, result.Source)
			if result.AckError != nil {
				log.Fatal(result.AckError)
			}
		}
		go agentRuntime.RunHeartbeats(ctx, token, 30*time.Second, appliedPlanVersion, func(err error) {
			log.Printf("SNMP agent heartbeat: %v", err)
			if errors.Is(err, agentplan.ErrUnauthorized) || errors.Is(err, agentplan.ErrPlanChanged) {
				select {
				case agentLifecycle <- err:
				default:
				}
				stop()
			}
		})
	}
	runtime, err := server.OpenSNMPCollectorRuntime(ctx, cfg, *agentID)
	if err != nil {
		log.Fatal(err)
	}
	defer runtime.Close()

	if *discover && !*loop {
		if err := runtime.DiscoverDevice(ctx, strings.TrimSpace(*deviceID)); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("discovery imported: device=%s\n", strings.TrimSpace(*deviceID))
	}
	if *poll && !*loop {
		result, err := runtime.RunDue(ctx, resolvedLimit)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("poll completed: recipes=%d devices=%d samples=%d failed=%d\n", result.RecipeCount, result.DeviceCount, result.SampleCount, result.FailedCount)
	}
	if *loop {
		if *discover && strings.TrimSpace(*deviceID) != "" {
			if err := runtime.DiscoverDevice(ctx, strings.TrimSpace(*deviceID)); err != nil {
				log.Fatal(err)
			}
		}
		if *poll {
			if err := runtime.RunLoop(ctx, resolvedInterval, resolvedLimit); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("poll scheduler stopped: %v", err)
			}
		} else {
			<-ctx.Done()
		}
	}
	select {
	case lifecycleErr := <-agentLifecycle:
		if errors.Is(lifecycleErr, agentplan.ErrPlanChanged) {
			_ = runtime.Close()
			log.Fatal(lifecycleErr)
		}
	default:
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
