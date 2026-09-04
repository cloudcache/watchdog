package main

import (
	"context"
	"errors"
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
	metrics := &flowcollect.Metrics{}
	restoreStartedAt := time.Now()
	decoder, err := flowcollect.NewDecoderWithStateTTL(cfg.FlowCollect.DecoderStateTTL)
	if err != nil {
		log.Fatal(err)
	}
	defer decoder.Close()
	stateReader, err := flowcollect.NewKafkaCollectStateReader(cfg.FlowCollect.Kafka, plan.CollectorID)
	if err != nil {
		log.Fatal(err)
	}
	remoteStates, readErr := stateReader.Read(ctx, registry)
	closeErr := stateReader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		log.Fatal(err)
	}
	registry, err = flowcollect.CompilePlan(plan, time.Now())
	if err != nil {
		log.Fatalf("flow plan is no longer active after collect-state restore: %v", err)
	}
	stateStore, err := flowcollect.OpenCollectStateStoreWithRemote(filepath.Join(cfg.FlowCollect.StateDir, "collect-state"), plan.CollectorID, registry, decoder, remoteStates)
	if err != nil {
		log.Fatal(err)
	}
	metrics.CollectStateRestoreCandidates.Store(int64(len(remoteStates)))
	metrics.CollectStateRestored.Store(int64(stateStore.RestoredCount()))
	metrics.CollectStateRestoreNanos.Store(time.Since(restoreStartedAt).Nanoseconds())
	log.Printf("restored collect-state snapshot: collector=%s remote_candidates=%d restored=%d duration=%s", plan.CollectorID, len(remoteStates), stateStore.RestoredCount(), time.Since(restoreStartedAt))
	wal, err := flowcollect.OpenWAL(filepath.Join(cfg.FlowCollect.StateDir, "wal"), plan.CollectorID, cfg.FlowCollect.WAL)
	if err != nil {
		log.Fatal(err)
	}
	defer wal.Close()
	quality := flowcollect.NewQualityTracker(cfg.FlowCollect.Quality, metrics)
	qualityState, err := flowcollect.OpenQualityStateStore(filepath.Join(cfg.FlowCollect.StateDir, "quality-state"), plan.CollectorID, cfg.FlowCollect.Quality, quality, wal, metrics)
	if err != nil {
		log.Fatal(err)
	}
	defer qualityState.Close()
	publisher, err := flowcollect.NewKafkaPublisher(cfg.FlowCollect.Kafka, cfg.FlowCollect.NormalizedBatch, plan.CollectorID)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	runtimeState := flowcollect.NewRuntimeState()
	observability, err := flowcollect.NewObservabilityServer(cfg.FlowCollect.Observability, metrics, wal, qualityState, runtimeState)
	if err != nil {
		log.Fatal(err)
	}
	runner := &flowcollect.Runner{Config: cfg.FlowCollect, Registry: registry, WAL: wal, Decoder: decoder, State: stateStore, Publisher: publisher, Metrics: metrics, Quality: quality, QualityState: qualityState, Runtime: runtimeState, OnError: func(err error) { log.Printf("flow record deferred: %v", err) }}
	log.Printf("starting flow-collect: collector=%s plan_revision=%d sflow=%s netflow=%s metrics=%s sockets=%d workers=%d", plan.CollectorID, plan.Revision, cfg.FlowCollect.SFlowListen, cfg.FlowCollect.NetFlowListen, cfg.FlowCollect.Observability.Listen, cfg.FlowCollect.SocketCount, cfg.FlowCollect.DecodeWorkers)
	runCtx, cancel := context.WithCancel(ctx)
	result := make(chan error, 2)
	go func() { result <- observability.Run(runCtx) }()
	go func() { result <- runner.Run(runCtx) }()
	first := <-result
	cancel()
	second := <-result
	if err := errors.Join(first, second); err != nil {
		log.Fatal(err)
	}
}
