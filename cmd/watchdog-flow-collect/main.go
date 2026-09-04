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
	if *check {
		registry, err := flowcollect.LoadSignedPlan(cfg.FlowCollect.PlanFile, cfg.FlowCollect.PlanPublicKeyFile, time.Now())
		if err != nil {
			log.Fatal(err)
		}
		plan := registry.Plan()
		log.Printf("flow-collect configuration valid: collector=%s plan_revision=%d sources=%d", plan.CollectorID, plan.Revision, len(plan.Sources))
		return
	}
	planHistory, err := flowcollect.OpenPlanHistory(cfg.FlowCollect.PlanFile, cfg.FlowCollect.PlanPublicKeyFile, filepath.Join(cfg.FlowCollect.StateDir, "plan-history"), cfg.FlowCollect.PlanHistoryMaxEntries, time.Now())
	if err != nil {
		log.Fatal(err)
	}
	registry := planHistory.Active()
	plan := registry.Plan()
	if planHistory.UsedLKG() {
		log.Printf("active plan unavailable; using signed LKG: collector=%s plan_revision=%d expires_at=%s", plan.CollectorID, plan.Revision, plan.ExpiresAt.Format(time.RFC3339))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	metrics := &flowcollect.Metrics{}
	wal, err := flowcollect.OpenWAL(filepath.Join(cfg.FlowCollect.StateDir, "wal"), plan.CollectorID, cfg.FlowCollect.WAL)
	if err != nil {
		log.Fatal(err)
	}
	defer wal.Close()
	pendingPlanRevisions, err := wal.PendingRegistryVersions()
	if err != nil {
		log.Fatal(err)
	}
	prunedPlans, err := planHistory.Prune(pendingPlanRevisions)
	if err != nil {
		log.Fatal(err)
	}
	metrics.PlanHistoryEntries.Store(int64(len(planHistory.Revisions())))
	metrics.PlanHistoryPruned.Store(uint64(prunedPlans))
	if err := flowcollect.VerifyKafkaTopicContracts(cfg.FlowCollect, planHistory, plan.CollectorID); err != nil {
		log.Fatalf("Kafka flow topic contract verification failed: %v", err)
	}
	log.Printf("verified Kafka flow topic contracts: collector=%s normalized=%s collect_state=%s decode_dlq=%s quarantine=%s", plan.CollectorID, cfg.FlowCollect.Kafka.NormalizedTopic, cfg.FlowCollect.Kafka.CollectStateTopic, cfg.FlowCollect.Kafka.DecodeDLQTopic, cfg.FlowCollect.Kafka.QuarantineTopic)
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
	remoteStates, readErr := stateReader.ReadWithHistory(ctx, registry, planHistory)
	closeErr := stateReader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		log.Fatal(err)
	}
	registry, err = planHistory.RevalidateActive(time.Now())
	if err != nil {
		log.Fatalf("flow plan is no longer active after collect-state restore: %v", err)
	}
	stateStore, err := flowcollect.OpenCollectStateStoreWithRecovery(filepath.Join(cfg.FlowCollect.StateDir, "collect-state"), plan.CollectorID, registry, planHistory, decoder, remoteStates)
	if err != nil {
		log.Fatal(err)
	}
	metrics.CollectStateRestoreCandidates.Store(int64(len(remoteStates)))
	metrics.CollectStateRestored.Store(int64(stateStore.RestoredCount()))
	metrics.CollectStateRestoreNanos.Store(time.Since(restoreStartedAt).Nanoseconds())
	log.Printf("restored collect-state snapshot: collector=%s remote_candidates=%d restored=%d duration=%s", plan.CollectorID, len(remoteStates), stateStore.RestoredCount(), time.Since(restoreStartedAt))
	attempts, err := flowcollect.OpenAttemptStore(filepath.Join(cfg.FlowCollect.StateDir, "attempt-state"), plan.CollectorID, cfg.FlowCollect.Diagnostics, wal, metrics)
	if err != nil {
		log.Fatal(err)
	}
	defer attempts.Close()
	log.Printf("restored replay-attempt state: collector=%s pending=%d", plan.CollectorID, attempts.RestoredCount())
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
	observability, err := flowcollect.NewObservabilityServer(cfg.FlowCollect.Observability, metrics, wal, attempts, qualityState, runtimeState)
	if err != nil {
		log.Fatal(err)
	}
	runner := &flowcollect.Runner{Config: cfg.FlowCollect, Registry: registry, Plans: planHistory, WAL: wal, Decoder: decoder, State: stateStore, Attempts: attempts, Publisher: publisher, Metrics: metrics, Quality: quality, QualityState: qualityState, Runtime: runtimeState, OnError: func(err error) { log.Printf("flow record deferred: %v", err) }}
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
