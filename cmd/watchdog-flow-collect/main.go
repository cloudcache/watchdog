package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
	"github.com/cloudcache/watchdog/internal/watchdog"
)

func main() {
	configPath := flag.String("config", "", "path to watchdog YAML config")
	check := flag.Bool("check", false, "validate configuration and signed collector plan, then exit")
	flag.Parse()

	cfg, err := watchdog.LoadWatchdogConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := cfg.FlowCollect.ValidateRuntime(); err != nil {
		log.Fatal(err)
	}
	registry, err := flowcollect.LoadSignedPlan(cfg.FlowCollect.PlanFile, cfg.FlowCollect.PlanPublicKeyFile, time.Now())
	if err != nil {
		log.Fatal(err)
	}
	plan := registry.Plan()
	if *check {
		log.Printf("flow-collect configuration valid: collector=%s plan_revision=%d sources=%d", plan.CollectorID, plan.Revision, len(plan.Sources))
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	wal, err := flowcollect.OpenWAL(filepath.Join(cfg.FlowCollect.StateDir, "wal"), plan.CollectorID, cfg.FlowCollect.WAL)
	if err != nil {
		log.Fatal(err)
	}
	defer wal.Close()
	decoder, err := flowcollect.NewDecoderWithStateTTL(cfg.FlowCollect.DecoderStateTTL)
	if err != nil {
		log.Fatal(err)
	}
	defer decoder.Close()
	stateStore, err := flowcollect.OpenCollectStateStore(filepath.Join(cfg.FlowCollect.StateDir, "collect-state"), plan.CollectorID, decoder)
	if err != nil {
		log.Fatal(err)
	}
	publisher, err := flowcollect.NewKafkaPublisher(cfg.FlowCollect.Kafka, cfg.FlowCollect.NormalizedBatch, plan.CollectorID)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	runner := &flowcollect.Runner{Config: cfg.FlowCollect, Registry: registry, WAL: wal, Decoder: decoder, State: stateStore, Publisher: publisher, OnError: func(err error) { log.Printf("flow record deferred: %v", err) }}
	log.Printf("starting flow-collect: collector=%s plan_revision=%d sflow=%s netflow=%s sockets=%d workers=%d", plan.CollectorID, plan.Revision, cfg.FlowCollect.SFlowListen, cfg.FlowCollect.NetFlowListen, cfg.FlowCollect.SocketCount, cfg.FlowCollect.DecodeWorkers)
	if err := runner.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
