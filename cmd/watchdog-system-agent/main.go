package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cloudcache/watchdog/internal/agentplan"
	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	netio "github.com/shirou/gopsutil/v4/net"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	hubURL := flag.String("hub-url", "", "Watchdog Hub URL")
	agentID := flag.String("agent-id", "", "Watchdog system agent ID")
	agentToken := flag.String("agent-token", "", "Watchdog system agent token")
	agentTokenFile := flag.String("agent-token-file", "", "file containing the system agent machine token")
	agentPlanPublicKey := flag.String("agent-plan-public-key", "", "agent plan Ed25519 public key file")
	agentPlanLKG := flag.String("agent-plan-lkg", "", "durable agent plan LKG file")
	agentPlanCheck := flag.Bool("agent-plan-check", false, "register/sync/apply the agent plan, then exit")
	interval := flag.Duration("interval", 0, "collection interval")
	rootPath := flag.String("root-path", "/", "root filesystem path for disk usage")
	once := flag.Bool("once", false, "collect once and exit")
	flag.Parse()
	if *interval < 0 {
		log.Fatal("interval must not be negative")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	agentLifecycle := make(chan error, 1)
	agentCfg, err := loadAgentConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	if strings.TrimSpace(*hubURL) != "" {
		agentCfg.HubURL = *hubURL
	}
	if strings.TrimSpace(*agentID) != "" {
		agentCfg.AgentID = *agentID
	}
	if *agentToken != "" {
		agentCfg.Token = *agentToken
	}
	if *interval > 0 {
		agentCfg.Interval = *interval
	}
	agentRuntime := systemAgentRuntime(agentCfg.HubURL, agentCfg.AgentID, agentCfg.Token, *agentTokenFile, *agentPlanPublicKey, *agentPlanLKG, uuid.NewString())
	if agentRuntime.Enabled() {
		result, token, err := agentRuntime.Sync(ctx, func(_ context.Context, spec agentplan.Spec) error {
			return applySystemAgentPlan(&agentCfg.Interval, rootPath, spec)
		})
		if errors.Is(err, agentplan.ErrUnauthorized) && !*agentPlanCheck {
			log.Printf("system agent credential is revoked; stopping without restart")
			return
		}
		if err != nil && (!errors.Is(err, agentplan.ErrNoPlan) || *agentPlanCheck) {
			log.Fatal(err)
		}
		agentCfg.Token = token
		appliedPlanVersion := result.Envelope.Metadata.PlanVersion
		if errors.Is(err, agentplan.ErrNoPlan) {
			log.Printf("system agent registered without a desired plan; using bootstrap values until a plan is published")
		} else {
			log.Printf("system agent plan applied: version=%d source=%s", appliedPlanVersion, result.Source)
			if result.AckError != nil {
				log.Fatal(result.AckError)
			}
			if *agentPlanCheck {
				return
			}
		}
		go agentRuntime.RunHeartbeats(ctx, token, 30*time.Second, appliedPlanVersion, func(err error) {
			log.Printf("system agent registry heartbeat: %v", err)
			if errors.Is(err, agentplan.ErrUnauthorized) || errors.Is(err, agentplan.ErrPlanChanged) {
				select {
				case agentLifecycle <- err:
				default:
				}
				stop()
			}
		})
	} else if *agentPlanCheck {
		log.Fatal("agent plan public key, LKG, and hub URL are required")
	}
	agentCfg, err = normalizeAndValidateAgentConfig(agentCfg)
	if err != nil {
		log.Fatal(err)
	}

	client := systemAgentClient{
		HubURL:     agentCfg.HubURL,
		AgentID:    agentCfg.AgentID,
		AgentToken: agentCfg.Token,
	}
	collector := &localSystemCollector{rootPath: *rootPath}
	if *once {
		if _, err := collectOnce(ctx, client, collector); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := run(ctx, client, collector, agentCfg.Interval); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
	select {
	case lifecycleErr := <-agentLifecycle:
		if errors.Is(lifecycleErr, agentplan.ErrPlanChanged) {
			log.Fatal(lifecycleErr)
		}
	default:
	}
}

type systemAgentPlanConfig struct {
	IntervalSeconds *int64  `json:"interval_seconds"`
	RootPath        *string `json:"root_path"`
}

func applySystemAgentPlan(interval *time.Duration, rootPath *string, spec agentplan.Spec) error {
	var config systemAgentPlanConfig
	if err := agentplan.DecodeConfig(spec, &config); err != nil {
		return err
	}
	if config.IntervalSeconds != nil {
		if *config.IntervalSeconds < 5 || *config.IntervalSeconds > 3600 {
			return errors.New("agent plan system interval must be 5..3600 seconds")
		}
		*interval = time.Duration(*config.IntervalSeconds) * time.Second
	}
	if config.RootPath != nil {
		value := strings.TrimSpace(*config.RootPath)
		if value == "" || !strings.HasPrefix(value, "/") {
			return errors.New("agent plan system root_path must be absolute")
		}
		*rootPath = value
	}
	return nil
}

func systemAgentRuntime(baseURL, agentID, token, tokenFile, publicKey, lkg, bootID string) agentplan.RuntimeConfig {
	if strings.TrimSpace(publicKey) == "" && strings.TrimSpace(lkg) == "" && strings.TrimSpace(tokenFile) == "" && strings.TrimSpace(token) == "" {
		return agentplan.RuntimeConfig{}
	}
	return agentplan.RuntimeConfig{
		BaseURL: baseURL, AgentID: strings.TrimSpace(agentID), Name: strings.TrimSpace(agentID), Kind: "system",
		Role: "system", Mode: "push", SoftwareVersion: "watchdog-system-agent-v1", APIVersion: "v1",
		Capabilities:  []string{"system.samples/v1"}, Token: token, TokenFile: tokenFile,
		PublicKeyFile: publicKey, LKGFile: lkg, BootID: bootID,
	}
}

func run(ctx context.Context, client systemAgentClient, collector *localSystemCollector, interval time.Duration) error {
	planInterval, err := collectOnce(ctx, client, collector)
	if err != nil {
		log.Printf("watchdog system agent collection failed: %v", err)
	}
	if interval <= 0 {
		interval = planInterval
	}
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := collectOnce(ctx, client, collector); err != nil {
				log.Printf("watchdog system agent collection failed: %v", err)
			}
		}
	}
}

func collectOnce(ctx context.Context, client systemAgentClient, collector *localSystemCollector) (time.Duration, error) {
	startedAt := time.Now().UTC()
	plan, err := client.FetchPlan(ctx)
	if err != nil {
		_ = client.ReportError(ctx, agentRunReport{Error: err.Error(), StartedAt: startedAt, EndedAt: time.Now().UTC()})
		return 0, err
	}
	if err := client.Heartbeat(ctx); err != nil {
		_ = client.ReportError(ctx, agentRunReport{Error: err.Error(), StartedAt: startedAt, EndedAt: time.Now().UTC()})
		return plan.Interval, err
	}
	batch, err := collector.Collect(ctx, plan)
	if err != nil {
		_ = client.ReportError(ctx, agentRunReport{Error: err.Error(), StartedAt: startedAt, EndedAt: time.Now().UTC()})
		return plan.Interval, err
	}
	if err := client.PushSamples(ctx, batch); err != nil {
		_ = client.ReportError(ctx, agentRunReport{Error: err.Error(), StartedAt: startedAt, EndedAt: time.Now().UTC()})
		return plan.Interval, err
	}
	return plan.Interval, client.ReportStatus(ctx, agentRunReport{StartedAt: startedAt, EndedAt: time.Now().UTC()})
}

type localSystemCollector struct {
	rootPath string
	prevNet  systemNetCounters
}

type systemNetCounters struct {
	receivedBytes uint64
	sentBytes     uint64
	sampledAt     time.Time
}

func (c *localSystemCollector) Collect(ctx context.Context, plan systemAgentPlan) (systemSampleBatch, error) {
	now := time.Now().UTC()
	cpuValues, err := cpu.PercentWithContext(ctx, 200*time.Millisecond, false)
	if err != nil {
		return systemSampleBatch{}, err
	}
	memory, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return systemSampleBatch{}, err
	}
	rootPath := c.rootPath
	if rootPath == "" {
		rootPath = "/"
	}
	diskUsage, err := disk.UsageWithContext(ctx, rootPath)
	if err != nil {
		return systemSampleBatch{}, err
	}
	netCounters, err := c.readNetworkCounters(ctx, now)
	if err != nil {
		return systemSampleBatch{}, err
	}
	netInBps, netOutBps := c.networkRates(netCounters)
	c.prevNet = netCounters

	cpuPercent := 0.0
	if len(cpuValues) > 0 {
		cpuPercent = cpuValues[0]
	}
	return systemSampleBatch{
		TenantID:  plan.Agent.TenantID,
		TargetID:  plan.Agent.TargetID,
		SampledAt: now,
		System: systemResourceSample{
			CPUPercent:    cpuPercent,
			MemoryPercent: memory.UsedPercent,
			DiskPercent:   diskUsage.UsedPercent,
			NetInBps:      netInBps,
			NetOutBps:     netOutBps,
		},
	}, nil
}

func (c *localSystemCollector) readNetworkCounters(ctx context.Context, sampledAt time.Time) (systemNetCounters, error) {
	items, err := netio.IOCountersWithContext(ctx, false)
	if err != nil {
		return systemNetCounters{}, err
	}
	counters := systemNetCounters{sampledAt: sampledAt}
	for _, item := range items {
		counters.receivedBytes += item.BytesRecv
		counters.sentBytes += item.BytesSent
	}
	return counters, nil
}

func (c *localSystemCollector) networkRates(current systemNetCounters) (float64, float64) {
	if c.prevNet.sampledAt.IsZero() || !current.sampledAt.After(c.prevNet.sampledAt) {
		return 0, 0
	}
	elapsedSeconds := current.sampledAt.Sub(c.prevNet.sampledAt).Seconds()
	if elapsedSeconds <= 0 {
		return 0, 0
	}
	inDelta := counterDelta(current.receivedBytes, c.prevNet.receivedBytes)
	outDelta := counterDelta(current.sentBytes, c.prevNet.sentBytes)
	return float64(inDelta*8) / elapsedSeconds, float64(outDelta*8) / elapsedSeconds
}

func counterDelta(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}
