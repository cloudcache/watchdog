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
	"github.com/cloudcache/watchdog/internal/snmpdomain"
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
	agentTokenFile := flag.String("agent-token-file", "", "file containing the installation-wide shared agent token")
	// Accepted but ignored so environment files written by the pre-shared-token
	// activate-agent.sh do not stop the upgraded binary at flag parsing.
	deprecatedEnrollmentFile := flag.String("agent-enrollment-token-file", "", "deprecated and ignored; agents authenticate with the shared token in -agent-token-file")
	agentPlanPublicKey := flag.String("agent-plan-public-key", "", "agent plan Ed25519 public key file")
	agentPlanLKG := flag.String("agent-plan-lkg", "", "durable agent plan LKG file")
	agentPlanCheck := flag.Bool("agent-plan-check", false, "register/sync/apply the agent plan, then exit")
	flag.Parse()
	if strings.TrimSpace(*deprecatedEnrollmentFile) != "" {
		log.Printf("-agent-enrollment-token-file is deprecated and ignored: agents authenticate with the installation shared token in -agent-token-file; re-run deploy/systemd/activate-agent.sh to refresh the environment file")
	}

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
		agentRuntime := snmpAgentRuntime(*controlPlaneURL, *agentID, *agentTokenFile, *agentPlanPublicKey, *agentPlanLKG, uuid.NewString())
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
	var agentToken string
	var agentPlanVersion uint64
	agentRuntime := snmpAgentRuntime(*controlPlaneURL, *agentID, *agentTokenFile, *agentPlanPublicKey, *agentPlanLKG, uuid.NewString())
	if agentRuntime.Enabled() {
		result, token, err := agentRuntime.Sync(ctx, func(_ context.Context, spec agentplan.Spec) error {
			return applySNMPAgentPlan(&resolvedInterval, &resolvedLimit, spec)
		})
		if errors.Is(err, agentplan.ErrUnauthorized) {
			log.Printf("SNMP agent token was rejected (agent revoked, or the token does not match the server's agents.shared_token); stopping without restart")
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
		agentToken = token
		agentPlanVersion = appliedPlanVersion
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

	// reportPoll records one SNMP poll pass as an agent run so the registry and
	// UI reflect real collection workload (recipes, devices, samples, failures),
	// not liveness alone. It is a no-op unless the agent runtime is configured.
	reportPoll := func(res snmpdomain.PollRunnerResult, runErr error, ended time.Time) {
		if !agentRuntime.Enabled() || agentToken == "" {
			return
		}
		report := agentplan.RunReport{
			EndedAt: ended,
			Summary: map[string]any{
				"plan_version": agentPlanVersion,
				"recipes":      res.RecipeCount,
				"devices":      res.DeviceCount,
				"samples":      res.SampleCount,
				"failed":       res.FailedCount,
			},
		}
		switch {
		case runErr != nil:
			report.Status = "failure"
			report.Error = runErr.Error()
		case res.FailedCount > 0:
			report.Status = "failure"
			report.Error = fmt.Sprintf("%d device polls failed", res.FailedCount)
		}
		reportCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := agentRuntime.ReportRun(reportCtx, agentToken, report); err != nil {
			log.Printf("SNMP agent run report: %v", err)
		}
	}

	if *discover && !*loop {
		if err := runtime.DiscoverDevice(ctx, strings.TrimSpace(*deviceID)); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("discovery imported: device=%s\n", strings.TrimSpace(*deviceID))
	}
	if *poll && !*loop {
		result, err := runtime.RunDue(ctx, resolvedLimit)
		reportPoll(result, err, time.Now().UTC())
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
			// Report passes from one goroutine, in pass order, without blocking the
			// next poll tick: a slow control plane drops reports instead of delaying
			// collection, and the server never receives an older pass after a newer
			// one. Each report carries the time its pass ended, not the send time.
			type passReport struct {
				result snmpdomain.PollRunnerResult
				err    error
				ended  time.Time
			}
			reports := make(chan passReport, 16)
			go func() {
				for r := range reports {
					reportPoll(r.result, r.err, r.ended)
				}
			}()
			if err := runtime.RunLoop(ctx, resolvedInterval, resolvedLimit, func(res snmpdomain.PollRunnerResult, cycleErr error) {
				select {
				case reports <- passReport{result: res, err: cycleErr, ended: time.Now().UTC()}:
				default:
					log.Printf("SNMP agent run report queue is full; dropping one pass report")
				}
			}); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("poll scheduler stopped: %v", err)
			}
			close(reports)
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

func snmpAgentRuntime(baseURL, agentID, tokenFile, publicKey, lkg, bootID string) agentplan.RuntimeConfig {
	if strings.TrimSpace(publicKey) == "" && strings.TrimSpace(lkg) == "" && strings.TrimSpace(tokenFile) == "" {
		return agentplan.RuntimeConfig{}
	}
	return agentplan.RuntimeConfig{
		BaseURL: baseURL, AgentID: strings.TrimSpace(agentID), Name: strings.TrimSpace(agentID), Kind: "snmp",
		Role: "snmp", Mode: "push", SoftwareVersion: "watchdog-snmp-collector-v1", APIVersion: "v1",
		Capabilities:  []string{"snmp.poll/v2"}, TokenFile: tokenFile,
		PublicKeyFile: publicKey, LKGFile: lkg, BootID: bootID,
	}
}
