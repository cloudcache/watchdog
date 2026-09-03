package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cloudcache/watchdog/internal/watchdog"
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
	interval := flag.Duration("interval", 0, "collection interval")
	rootPath := flag.String("root-path", "/", "root filesystem path for disk usage")
	once := flag.Bool("once", false, "collect once and exit")
	flag.Parse()
	if *interval < 0 {
		log.Fatal("interval must not be negative")
	}

	cfg, err := watchdog.LoadWatchdogConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	agentCfg := cfg.Agent
	if strings.TrimSpace(*hubURL) != "" {
		agentCfg.HubURL = *hubURL
	}
	if strings.TrimSpace(*agentID) != "" {
		agentCfg.AgentID = watchdog.ID(*agentID)
	}
	if *agentToken != "" {
		agentCfg.Token = *agentToken
	}
	if *interval > 0 {
		agentCfg.Interval = *interval
	}
	agentCfg, err = watchdog.NormalizeAndValidateAgentClientConfig(agentCfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := watchdog.SystemAgentClient{
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
}

func run(ctx context.Context, client watchdog.SystemAgentClient, collector *localSystemCollector, interval time.Duration) error {
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

func collectOnce(ctx context.Context, client watchdog.SystemAgentClient, collector *localSystemCollector) (time.Duration, error) {
	startedAt := time.Now().UTC()
	plan, err := client.FetchPlan(ctx)
	if err != nil {
		_ = client.ReportError(ctx, watchdog.AgentRunReport{Error: err.Error(), StartedAt: startedAt, EndedAt: time.Now().UTC()})
		return 0, err
	}
	if err := client.Heartbeat(ctx); err != nil {
		_ = client.ReportError(ctx, watchdog.AgentRunReport{Error: err.Error(), StartedAt: startedAt, EndedAt: time.Now().UTC()})
		return plan.Interval, err
	}
	batch, err := collector.Collect(ctx, plan)
	if err != nil {
		_ = client.ReportError(ctx, watchdog.AgentRunReport{Error: err.Error(), StartedAt: startedAt, EndedAt: time.Now().UTC()})
		return plan.Interval, err
	}
	if err := client.PushSamples(ctx, batch); err != nil {
		_ = client.ReportError(ctx, watchdog.AgentRunReport{Error: err.Error(), StartedAt: startedAt, EndedAt: time.Now().UTC()})
		return plan.Interval, err
	}
	return plan.Interval, client.ReportStatus(ctx, watchdog.AgentRunReport{StartedAt: startedAt, EndedAt: time.Now().UTC()})
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

func (c *localSystemCollector) Collect(ctx context.Context, plan watchdog.SystemAgentPlan) (watchdog.SystemSampleBatch, error) {
	now := time.Now().UTC()
	cpuValues, err := cpu.PercentWithContext(ctx, 200*time.Millisecond, false)
	if err != nil {
		return watchdog.SystemSampleBatch{}, err
	}
	memory, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return watchdog.SystemSampleBatch{}, err
	}
	rootPath := c.rootPath
	if rootPath == "" {
		rootPath = "/"
	}
	diskUsage, err := disk.UsageWithContext(ctx, rootPath)
	if err != nil {
		return watchdog.SystemSampleBatch{}, err
	}
	netCounters, err := c.readNetworkCounters(ctx, now)
	if err != nil {
		return watchdog.SystemSampleBatch{}, err
	}
	netInBps, netOutBps := c.networkRates(netCounters)
	c.prevNet = netCounters

	cpuPercent := 0.0
	if len(cpuValues) > 0 {
		cpuPercent = cpuValues[0]
	}
	return watchdog.SystemSampleBatch{
		TenantID:  plan.Agent.TenantID,
		TargetID:  plan.Agent.TargetID,
		SampledAt: now,
		System: watchdog.SystemResourceSample{
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
