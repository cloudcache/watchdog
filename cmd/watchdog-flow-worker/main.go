// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cloudcache/watchdog/internal/agentplan"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowmetrics"
	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowtombstone"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/google/uuid"
)

const maxPublicationBytes = 256 << 10

type stringList []string

func (values *stringList) String() string { return strings.Join(*values, ",") }
func (values *stringList) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("path cannot be empty")
	}
	*values = append(*values, value)
	return nil
}

type options struct {
	planFiles, versionPublications, geoBundles                    stringList
	planPublicKey, workerID                                       string
	controlPlaneURL, agentControlPlaneURL, agentTokenFile         string
	versionLKGDir                                                 string
	agentPlanPublicKey, agentPlanLKG                              string
	controlPlaneCAFile, controlPlaneCertFile, controlPlaneKeyFile string
	controlPlaneServerName                                        string
	controlPlaneTimeout, versionRefreshInterval                   time.Duration

	brokers, topic, sourceStreamID, clientID, consumerGroup                     string
	kafkaCAFile, kafkaCertFile, kafkaKeyFile                                    string
	kafkaServerName, saslMechanism                                              string
	saslUsername, saslPasswordFile                                              string
	kafkaTLS                                                                    bool
	fetchMinBytes, fetchMaxBytes, fetchMaxPartitionBytes, templateReplayRecords int64
	partitionBatchRecords                                                       int
	fetchMaxWait, decoderStateTTL                                               time.Duration

	clickHouseAddress, clickHouseDatabase                                    string
	clickHouseUser, clickHousePasswordFile                                   string
	clickHouseCAFile, clickHouseCertFile                                     string
	clickHouseKeyFile, clickHouseServerName                                  string
	clickHouseTLS                                                            bool
	clickHouseMaxConns, clickHouseMinConns                                   int
	clickHouseDialTimeout, clickHouseReadTimeout, clickHouseOperationTimeout time.Duration
	blockMaxRows, blockMaxBytes                                              int
	metricsListen                                                            string
	check, agentPlanCheck                                                    bool
}

func main() {
	var opt options
	flag.Var(&opt.planFiles, "bootstrap-plan", "signed plan file; repeat for every registry revision retained in Kafka")
	flag.StringVar(&opt.planPublicKey, "plan-public-key", "", "Ed25519 plan public key file")
	flag.Var(&opt.versionPublications, "bootstrap-version-publication", "dimension/classification publication JSON; repeat for retained event-time versions")
	flag.Var(&opt.geoBundles, "geo-bundle", "legacy JSON dimension Geo bundle directory; optional for WADS-only bootstrap, repeat oldest to newest")
	flag.StringVar(&opt.workerID, "worker-id", "watchdog-flow-worker", "stable worker instance identity")
	flag.StringVar(&opt.controlPlaneURL, "control-plane-url", "", "Watchdog API base URL for signed enrichment publications")
	flag.StringVar(&opt.agentControlPlaneURL, "agent-control-plane-url", "", "Watchdog API base URL for agent registration, plan, ACK, and health")
	flag.StringVar(&opt.agentTokenFile, "agent-token-file", "", "file containing the installation-wide shared agent token")
	// Accepted but ignored so environment files written by the pre-shared-token
	// activate-agent.sh do not stop the upgraded binary at flag parsing.
	deprecatedEnrollmentFile := flag.String("agent-enrollment-token-file", "", "deprecated and ignored; agents authenticate with the shared token in -agent-token-file")
	flag.StringVar(&opt.agentPlanPublicKey, "agent-plan-public-key", "", "agent plan Ed25519 public key file")
	flag.StringVar(&opt.agentPlanLKG, "agent-plan-lkg", "", "durable agent plan LKG file")
	flag.BoolVar(&opt.agentPlanCheck, "agent-plan-check", false, "register/sync/apply the agent plan, then exit")
	flag.StringVar(&opt.versionLKGDir, "version-lkg-dir", "", "durable directory for signed enrichment publication LKG")
	flag.DurationVar(&opt.versionRefreshInterval, "version-refresh-interval", time.Minute, "signed enrichment publication refresh interval")
	flag.DurationVar(&opt.controlPlaneTimeout, "control-plane-timeout", 2*time.Minute, "timeout for one control-plane request")
	flag.StringVar(&opt.controlPlaneCAFile, "control-plane-tls-ca", "", "control-plane TLS CA file")
	flag.StringVar(&opt.controlPlaneCertFile, "control-plane-tls-cert", "", "control-plane mTLS client certificate file")
	flag.StringVar(&opt.controlPlaneKeyFile, "control-plane-tls-key", "", "control-plane mTLS client key file")
	flag.StringVar(&opt.controlPlaneServerName, "control-plane-tls-server-name", "", "control-plane TLS server name")

	flag.StringVar(&opt.brokers, "kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	flag.StringVar(&opt.topic, "kafka-topic", "watchdog.flow.raw", "RawFlow Kafka topic base; schema suffix is automatic")
	flag.StringVar(&opt.sourceStreamID, "source-stream-id", "", "stable Kafka cluster/topic incarnation ID (required; never reuse after topic recreation)")
	flag.StringVar(&opt.clientID, "kafka-client-id", "watchdog-flow-worker", "Kafka client ID")
	flag.StringVar(&opt.consumerGroup, "kafka-consumer-group", "watchdog-flow-worker-v1", "Kafka consumer group")
	flag.Int64Var(&opt.fetchMinBytes, "kafka-fetch-min-bytes", 1_000_000, "minimum Kafka fetch bytes")
	flag.Int64Var(&opt.fetchMaxBytes, "kafka-fetch-max-bytes", 64<<20, "maximum bytes buffered from one Kafka broker fetch")
	flag.Int64Var(&opt.fetchMaxPartitionBytes, "kafka-fetch-max-partition-bytes", 16<<20, "maximum bytes fetched from one Kafka partition")
	flag.DurationVar(&opt.fetchMaxWait, "kafka-fetch-max-wait", time.Second, "maximum Kafka fetch wait")
	flag.IntVar(&opt.partitionBatchRecords, "kafka-partition-batch-records", 4_096, "maximum Kafka messages in one durable ClickHouse handler batch")
	flag.Int64Var(&opt.templateReplayRecords, "kafka-template-replay-records", 0, "NetFlow/IPFIX records replayed before each committed partition offset to rebuild templates; 0 disables replay for sFlow")
	flag.BoolVar(&opt.kafkaTLS, "kafka-tls", false, "enable Kafka TLS")
	flag.StringVar(&opt.kafkaCAFile, "kafka-tls-ca", "", "Kafka TLS CA file")
	flag.StringVar(&opt.kafkaCertFile, "kafka-tls-cert", "", "Kafka TLS client certificate file")
	flag.StringVar(&opt.kafkaKeyFile, "kafka-tls-key", "", "Kafka TLS client key file")
	flag.StringVar(&opt.kafkaServerName, "kafka-tls-server-name", "", "Kafka TLS server name")
	flag.StringVar(&opt.saslMechanism, "kafka-sasl-mechanism", "none", "none, plain, scram-sha-256 or scram-sha-512")
	flag.StringVar(&opt.saslUsername, "kafka-sasl-username", "", "Kafka SASL username")
	flag.StringVar(&opt.saslPasswordFile, "kafka-sasl-password-file", "", "file containing the Kafka SASL password")
	flag.DurationVar(&opt.decoderStateTTL, "decoder-state-ttl", 30*time.Minute, "GoFlow2 template and sampler state TTL")

	flag.StringVar(&opt.clickHouseAddress, "clickhouse-address", "127.0.0.1:9000", "ClickHouse native TCP address")
	flag.StringVar(&opt.clickHouseDatabase, "clickhouse-database", "watchdog_flow", "ClickHouse database")
	flag.StringVar(&opt.clickHouseUser, "clickhouse-user", "default", "ClickHouse user")
	flag.StringVar(&opt.clickHousePasswordFile, "clickhouse-password-file", "", "file containing the ClickHouse password")
	flag.IntVar(&opt.clickHouseMaxConns, "clickhouse-max-conns", 8, "maximum ClickHouse native connections")
	flag.IntVar(&opt.clickHouseMinConns, "clickhouse-min-conns", 1, "minimum ClickHouse native connections")
	flag.DurationVar(&opt.clickHouseDialTimeout, "clickhouse-dial-timeout", 3*time.Second, "ClickHouse connection timeout")
	flag.DurationVar(&opt.clickHouseReadTimeout, "clickhouse-read-timeout", 30*time.Second, "ClickHouse packet polling interval")
	flag.DurationVar(&opt.clickHouseOperationTimeout, "clickhouse-operation-timeout", 2*time.Minute, "maximum duration of one ClickHouse operation")
	flag.BoolVar(&opt.clickHouseTLS, "clickhouse-tls", false, "enable ClickHouse TLS")
	flag.StringVar(&opt.clickHouseCAFile, "clickhouse-tls-ca", "", "ClickHouse TLS CA file")
	flag.StringVar(&opt.clickHouseCertFile, "clickhouse-tls-cert", "", "ClickHouse TLS client certificate file")
	flag.StringVar(&opt.clickHouseKeyFile, "clickhouse-tls-key", "", "ClickHouse TLS client key file")
	flag.StringVar(&opt.clickHouseServerName, "clickhouse-tls-server-name", "", "ClickHouse TLS server name")
	flag.IntVar(&opt.blockMaxRows, "clickhouse-block-max-rows", 50_000, "maximum records per ClickHouse block")
	flag.IntVar(&opt.blockMaxBytes, "clickhouse-block-max-bytes", 64<<20, "maximum approximate bytes per ClickHouse block")
	flag.StringVar(&opt.metricsListen, "metrics-listen", "127.0.0.1:9091", "Prometheus metrics listen address; empty disables it")
	flag.BoolVar(&opt.check, "check", false, "validate configuration and bootstrap artifacts without connecting, then exit")
	flag.Parse()
	if strings.TrimSpace(*deprecatedEnrollmentFile) != "" {
		log.Printf("-agent-enrollment-token-file is deprecated and ignored: agents authenticate with the installation shared token in -agent-token-file; re-run deploy/systemd/activate-agent.sh to refresh the environment file")
	}

	if err := run(opt); err != nil {
		log.Fatal(err)
	}
}

func run(opt options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	agentLifecycle := make(chan error, 1)
	identity := flowworker.VersionWorkerIdentity{
		WorkerID: strings.TrimSpace(opt.workerID), BootID: uuid.NewString(), SoftwareVersion: "watchdog-flow-worker-v1",
	}
	var agentToken string
	var agentPlanVersion uint64
	agentRuntime := flowWorkerAgentRuntime(opt, identity.BootID)
	if agentRuntime.Enabled() {
		result, token, err := agentRuntime.Sync(ctx, func(_ context.Context, spec agentplan.Spec) error {
			return applyFlowWorkerAgentPlan(&opt, spec)
		})
		if errors.Is(err, agentplan.ErrUnauthorized) && !opt.agentPlanCheck {
			log.Printf("flow-worker agent token was rejected (agent revoked, or the token does not match the server's agents.shared_token); stopping without restart")
			return nil
		}
		if err != nil && (!errors.Is(err, agentplan.ErrNoPlan) || opt.agentPlanCheck) {
			return err
		}
		appliedPlanVersion := result.Envelope.Metadata.PlanVersion
		if errors.Is(err, agentplan.ErrNoPlan) {
			log.Printf("flow-worker registered without a desired agent plan; using bootstrap values until a plan is published")
		} else {
			log.Printf("flow-worker agent plan applied: version=%d source=%s", appliedPlanVersion, result.Source)
			if result.AckError != nil {
				return result.AckError
			}
			if opt.agentPlanCheck {
				return nil
			}
		}
		agentToken = token
		agentPlanVersion = appliedPlanVersion
		go agentRuntime.RunHeartbeats(ctx, token, 30*time.Second, appliedPlanVersion, func(err error) {
			log.Printf("flow-worker agent heartbeat: %v", err)
			if errors.Is(err, agentplan.ErrUnauthorized) || errors.Is(err, agentplan.ErrPlanChanged) {
				select {
				case agentLifecycle <- err:
				default:
				}
				stop()
			}
		})
	} else if opt.agentPlanCheck {
		return errors.New("agent plan public key, LKG, and control-plane URL are required")
	}
	if err := flowmetrics.ValidateListenAddress(opt.metricsListen); err != nil {
		return err
	}
	consumerConfig, err := buildConsumerConfig(opt)
	if err != nil {
		return err
	}
	clickHouseConfig, err := buildClickHouseConfig(opt)
	if err != nil {
		return err
	}
	plans, versions, geo, versionSync, versionCursor, err := loadBootstrap(ctx, opt, identity)
	if err != nil {
		return err
	}
	if opt.decoderStateTTL < time.Minute || opt.decoderStateTTL > 24*time.Hour {
		return errors.New("decoder state TTL must be 1m..24h")
	}
	if !flowworker.ValidSourceStreamID(opt.sourceStreamID) {
		return errors.New("source stream ID is required and must contain only letters, digits, dot, underscore, colon, or dash")
	}
	if opt.blockMaxRows < 1 || opt.blockMaxRows > 1_000_000 || opt.blockMaxBytes < 1 || opt.blockMaxBytes > 1<<30 {
		return errors.New("ClickHouse block limits are invalid")
	}
	if opt.check {
		log.Printf("flow-worker configuration valid: plans=%d enrichment_versions=%d legacy_geo_bundles=%d remote_versions=%t group=%s", len(opt.planFiles), len(opt.versionPublications), len(opt.geoBundles), versionSync != nil, consumerConfig.ConsumerGroup)
		return nil
	}
	var barrierGuard *flowtombstone.Guard
	var barrierSync func(context.Context) error
	if versionSync != nil {
		barrierGuard, barrierSync, err = loadRawDeleteBarrier(ctx, opt, identity)
		if err != nil {
			return err
		}
	}

	enricher, err := flowworker.NewEnricherWithVersionCatalog(versions, geo, flowworker.EnrichmentLimits{})
	if err != nil {
		return err
	}
	native, err := flowch.NewNativeInserter(ctx, clickHouseConfig)
	if err != nil {
		return err
	}
	defer native.Close()
	readyCtx, cancelReady := context.WithTimeout(ctx, 10*time.Second)
	err = native.Ready(readyCtx)
	cancelReady()
	if err != nil {
		return fmt.Errorf("verify Flow Storage V2 ClickHouse schema: %w", err)
	}
	writer, err := flowch.NewWriter(native, flowch.WriterConfig{Limits: flowch.BatchLimits{MaxRows: opt.blockMaxRows, MaxApproxBytes: opt.blockMaxBytes}})
	if err != nil {
		return err
	}
	var pipeline *flowch.Pipeline
	if barrierGuard != nil {
		pipeline, err = flowch.NewGuardedPipeline(enricher, writer, barrierGuard)
	} else {
		pipeline, err = flowch.NewPipeline(enricher, writer)
	}
	if err != nil {
		return err
	}
	processor, err := flowworker.NewBatchProcessorForStream(opt.decoderStateTTL, opt.sourceStreamID, plans.Resolve, pipeline.Handle, nil)
	if err != nil {
		return err
	}
	defer processor.Close()
	consumer, err := flowstream.NewConsumer(consumerConfig, func(err error) { log.Printf("flow-worker Kafka lifecycle: %v", err) })
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := consumer.Close(closeCtx); err != nil {
			log.Printf("close flow-worker Kafka consumer: %v", err)
		}
	}()
	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	err = consumer.Ping(pingCtx)
	cancelPing()
	if err != nil {
		return fmt.Errorf("connect RawFlow Kafka: %w", err)
	}
	runtimeMetrics, err := flowmetrics.NewWorker(consumer.Stats, processor.Stats, pipeline.Stats, writer.Stats)
	if err != nil {
		return err
	}
	metricsServer, err := startMetricsServer(opt.metricsListen, runtimeMetrics.Handler())
	if err != nil {
		return fmt.Errorf("start flow-worker metrics: %w", err)
	}
	defer shutdownMetricsServer(metricsServer, "flow-worker")
	var metricsErrCh <-chan error
	if metricsServer != nil {
		result := make(chan error, 1)
		go func() { result <- metricsServer.Wait() }()
		metricsErrCh = result
		log.Printf("flow-worker metrics listening: address=%s", metricsServer.Address())
	}
	log.Printf("flow-worker started: plans=%d enrichment_versions=%d legacy_geo_bundles=%d remote_versions=%t group=%s", len(opt.planFiles), len(opt.versionPublications), len(opt.geoBundles), versionSync != nil, consumerConfig.ConsumerGroup)
	runCtx, cancelRun := context.WithCancel(ctx)
	var versionSyncDone <-chan struct{}
	if versionSync != nil {
		done := make(chan struct{})
		versionSyncDone = done
		go runVersionSyncLoop(runCtx, versionSync, versionCursor, barrierSync, opt.versionRefreshInterval, done)
	}
	if agentRuntime.Enabled() && agentToken != "" {
		previousProcessor := processor.Stats()
		previousConsumer := consumer.Stats()
		windowStarted := time.Now()
		go agentRuntime.RunReports(runCtx, agentToken, time.Minute, func() agentplan.RunReport {
			pstats := processor.Stats()
			cstats := consumer.Stats()
			now := time.Now()
			report := agentplan.RunReport{
				StartedAt:  windowStarted,
				EndedAt:    now,
				DurationMS: uint64(now.Sub(windowStarted).Milliseconds()),
				Summary: map[string]any{
					"plan_version":        agentPlanVersion,
					"datagrams":           flowCounterDelta(pstats.Datagrams, previousProcessor.Datagrams),
					"records":             flowCounterDelta(pstats.Records, previousProcessor.Records),
					"counter_records":     flowCounterDelta(pstats.CounterRecords, previousProcessor.CounterRecords),
					"template_missing":    flowCounterDelta(pstats.TemplateMissing, previousProcessor.TemplateMissing),
					"rejected":            flowCounterDelta(pstats.Rejected, previousProcessor.Rejected),
					"retryable_errors":    flowCounterDelta(pstats.RetryableErrors, previousProcessor.RetryableErrors),
					"kafka_records":       flowCounterDelta(cstats.Records, previousConsumer.Records),
					"kafka_bytes":         flowCounterDelta(cstats.Bytes, previousConsumer.Bytes),
					"kafka_lag_records":   cstats.LagRecords,
					"kafka_errors":        flowCounterDelta(cstats.Errors, previousConsumer.Errors),
					"kafka_data_loss":     flowCounterDelta(cstats.DataLoss, previousConsumer.DataLoss),
					"assigned_partitions": cstats.AssignedPartitions,
				},
			}
			previousProcessor = pstats
			previousConsumer = cstats
			windowStarted = now
			return report
		}, func(err error) {
			log.Printf("flow-worker agent run report: %v", err)
		})
	}
	consumerErrCh := make(chan error, 1)
	go func() { consumerErrCh <- consumer.RunPartitionBatches(runCtx, processor.HandleRecords) }()
	select {
	case err = <-consumerErrCh:
	case metricsErr := <-metricsErrCh:
		cancelRun()
		err = errors.Join(fmt.Errorf("flow-worker metrics server: %w", metricsErr), <-consumerErrCh)
	case <-ctx.Done():
		cancelRun()
		err = <-consumerErrCh
	}
	cancelRun()
	if versionSyncDone != nil {
		<-versionSyncDone
	}
	stats := processor.Stats()
	log.Printf("flow-worker stopped: datagrams=%d records=%d template_missing=%d rejected=%d retryable_errors=%d", stats.Datagrams, stats.Records, stats.TemplateMissing, stats.Rejected, stats.RetryableErrors)
	select {
	case lifecycleErr := <-agentLifecycle:
		if errors.Is(lifecycleErr, agentplan.ErrPlanChanged) {
			return errors.Join(err, lifecycleErr)
		}
		return err
	default:
	}
	return err
}

func flowCounterDelta(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}

type flowWorkerPlanConfig struct {
	FetchMinBytes          *int64 `json:"kafka_fetch_min_bytes"`
	FetchMaxBytes          *int64 `json:"kafka_fetch_max_bytes"`
	FetchMaxPartitionBytes *int64 `json:"kafka_fetch_max_partition_bytes"`
	FetchMaxWaitMS         *int64 `json:"kafka_fetch_max_wait_ms"`
	PartitionBatchRecords  *int   `json:"kafka_partition_batch_records"`
	BlockMaxRows           *int   `json:"clickhouse_block_max_rows"`
	BlockMaxBytes          *int   `json:"clickhouse_block_max_bytes"`
}

func applyFlowWorkerAgentPlan(opt *options, spec agentplan.Spec) error {
	defaults := flowstream.DefaultConsumerConfig()
	var config flowWorkerPlanConfig
	if err := agentplan.DecodeConfig(spec, &config); err != nil {
		return err
	}
	if config.FetchMinBytes != nil {
		opt.fetchMinBytes = *config.FetchMinBytes
	}
	if config.FetchMaxBytes != nil {
		opt.fetchMaxBytes = *config.FetchMaxBytes
	}
	if config.FetchMaxPartitionBytes != nil {
		opt.fetchMaxPartitionBytes = *config.FetchMaxPartitionBytes
	}
	if opt.fetchMaxPartitionBytes == 0 {
		opt.fetchMaxPartitionBytes = int64(defaults.FetchMaxPartitionBytes)
	}
	if opt.fetchMaxBytes == 0 {
		opt.fetchMaxBytes = int64(defaults.FetchMaxBytes)
	}
	if config.FetchMaxWaitMS != nil {
		opt.fetchMaxWait = time.Duration(*config.FetchMaxWaitMS) * time.Millisecond
	}
	if config.PartitionBatchRecords != nil {
		opt.partitionBatchRecords = *config.PartitionBatchRecords
	}
	if opt.partitionBatchRecords == 0 {
		opt.partitionBatchRecords = flowstream.DefaultConsumerConfig().PartitionBatchRecords
	}
	if config.BlockMaxRows != nil {
		opt.blockMaxRows = *config.BlockMaxRows
	}
	if config.BlockMaxBytes != nil {
		opt.blockMaxBytes = *config.BlockMaxBytes
	}
	if opt.fetchMinBytes < 1 || opt.fetchMaxPartitionBytes < opt.fetchMinBytes || opt.fetchMaxBytes < opt.fetchMaxPartitionBytes || opt.fetchMaxBytes > 1<<30 || opt.fetchMaxPartitionBytes > 1<<30 || opt.fetchMaxWait <= 0 || opt.fetchMaxWait > 5*time.Minute || opt.partitionBatchRecords < 1 || opt.partitionBatchRecords > 1_000_000 {
		return errors.New("agent plan flow-worker Kafka fetch limits are invalid")
	}
	if opt.blockMaxRows < 1 || opt.blockMaxRows > 1_000_000 || opt.blockMaxBytes < 1 || opt.blockMaxBytes > 1<<30 {
		return errors.New("agent plan flow-worker ClickHouse block limits are invalid")
	}
	return nil
}

func flowWorkerAgentRuntime(opt options, bootID string) agentplan.RuntimeConfig {
	if strings.TrimSpace(opt.agentPlanPublicKey) == "" && strings.TrimSpace(opt.agentPlanLKG) == "" && strings.TrimSpace(opt.agentTokenFile) == "" && !opt.agentPlanCheck {
		return agentplan.RuntimeConfig{}
	}
	agentControlPlaneURL := strings.TrimSpace(opt.agentControlPlaneURL)
	if agentControlPlaneURL == "" {
		// Backward compatibility for workers that used the original shared
		// control-plane URL for both agent lifecycle and enrichment delivery.
		agentControlPlaneURL = strings.TrimSpace(opt.controlPlaneURL)
	}
	return agentplan.RuntimeConfig{
		BaseURL: agentControlPlaneURL, AgentID: strings.TrimSpace(opt.workerID), Name: strings.TrimSpace(opt.workerID),
		Kind: "flow_worker", Role: "flow_worker", Mode: "push", SoftwareVersion: "watchdog-flow-worker-v1",
		APIVersion: "v1", Capabilities: []string{"flow.write.clickhouse/v1"}, TokenFile: opt.agentTokenFile,
		PublicKeyFile: opt.agentPlanPublicKey,
		LKGFile:       opt.agentPlanLKG, BootID: bootID,
	}
}

func startMetricsServer(address string, handler http.Handler) (*flowmetrics.Server, error) {
	if strings.TrimSpace(address) == "" {
		return nil, nil
	}
	return flowmetrics.StartServer(address, handler)
}

func shutdownMetricsServer(server *flowmetrics.Server, processName string) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("shutdown %s metrics: %v", processName, err)
	}
}

func buildConsumerConfig(opt options) (flowstream.ConsumerConfig, error) {
	config := flowstream.DefaultConsumerConfig()
	config.Kafka.Brokers = splitNonEmpty(opt.brokers)
	config.Kafka.Topic = strings.TrimSpace(opt.topic)
	config.Kafka.ClientID = strings.TrimSpace(opt.clientID)
	config.ConsumerGroup = strings.TrimSpace(opt.consumerGroup)
	if opt.fetchMinBytes < 1 || opt.fetchMinBytes > int64(^uint32(0)>>1) {
		return flowstream.ConsumerConfig{}, errors.New("Kafka fetch minimum bytes are invalid")
	}
	config.FetchMinBytes = int32(opt.fetchMinBytes)
	if opt.fetchMaxBytes != 0 {
		if opt.fetchMaxBytes < 1 || opt.fetchMaxBytes > 1<<30 {
			return flowstream.ConsumerConfig{}, errors.New("Kafka fetch maximum bytes are invalid")
		}
		config.FetchMaxBytes = int32(opt.fetchMaxBytes)
	}
	if opt.fetchMaxPartitionBytes != 0 {
		if opt.fetchMaxPartitionBytes < 1 || opt.fetchMaxPartitionBytes > 1<<30 {
			return flowstream.ConsumerConfig{}, errors.New("Kafka fetch maximum partition bytes are invalid")
		}
		config.FetchMaxPartitionBytes = int32(opt.fetchMaxPartitionBytes)
	}
	config.FetchMaxWait = opt.fetchMaxWait
	if opt.partitionBatchRecords != 0 {
		config.PartitionBatchRecords = opt.partitionBatchRecords
	}
	config.TemplateReplayRecords = opt.templateReplayRecords
	config.Kafka.TLS = flowstream.TLSConfig{
		Enabled: opt.kafkaTLS, CAFile: strings.TrimSpace(opt.kafkaCAFile), CertFile: strings.TrimSpace(opt.kafkaCertFile),
		KeyFile: strings.TrimSpace(opt.kafkaKeyFile), ServerName: strings.TrimSpace(opt.kafkaServerName),
	}
	config.Kafka.SASL = flowstream.SASLConfig{Mechanism: flowstream.SASLMechanism(strings.TrimSpace(opt.saslMechanism)), Username: strings.TrimSpace(opt.saslUsername)}
	if opt.saslPasswordFile != "" {
		password, err := flowstream.ReadSecretFile(opt.saslPasswordFile)
		if err != nil {
			return flowstream.ConsumerConfig{}, fmt.Errorf("Kafka SASL password: %w", err)
		}
		config.Kafka.SASL.Password = password
	}
	if err := config.Validate(); err != nil {
		return flowstream.ConsumerConfig{}, err
	}
	return config, nil
}

func buildClickHouseConfig(opt options) (flowch.NativeConfig, error) {
	tlsConfig, err := (flowstream.TLSConfig{
		Enabled: opt.clickHouseTLS, CAFile: strings.TrimSpace(opt.clickHouseCAFile), CertFile: strings.TrimSpace(opt.clickHouseCertFile),
		KeyFile: strings.TrimSpace(opt.clickHouseKeyFile), ServerName: strings.TrimSpace(opt.clickHouseServerName),
	}).ClientConfig()
	if err != nil {
		return flowch.NativeConfig{}, fmt.Errorf("ClickHouse TLS: %w", err)
	}
	password := ""
	if opt.clickHousePasswordFile != "" {
		password, err = flowstream.ReadSecretFile(opt.clickHousePasswordFile)
		if err != nil {
			return flowch.NativeConfig{}, fmt.Errorf("ClickHouse password: %w", err)
		}
	}
	if opt.clickHouseMaxConns < 1 || opt.clickHouseMaxConns > 1_024 || opt.clickHouseMinConns < 0 || opt.clickHouseMinConns > opt.clickHouseMaxConns {
		return flowch.NativeConfig{}, errors.New("ClickHouse connection limits are invalid")
	}
	if strings.TrimSpace(opt.clickHouseAddress) == "" || strings.TrimSpace(opt.clickHouseDatabase) == "" || strings.TrimSpace(opt.clickHouseUser) == "" || opt.clickHouseDialTimeout <= 0 || opt.clickHouseReadTimeout <= 0 || opt.clickHouseOperationTimeout <= 0 {
		return flowch.NativeConfig{}, errors.New("ClickHouse address, database, user, and positive timeouts are required")
	}
	return flowch.NativeConfig{
		Address: strings.TrimSpace(opt.clickHouseAddress), Database: strings.TrimSpace(opt.clickHouseDatabase), User: strings.TrimSpace(opt.clickHouseUser), Password: password,
		ClientName:  "watchdog-flow-worker",
		DialTimeout: opt.clickHouseDialTimeout, ReadTimeout: opt.clickHouseReadTimeout, OperationTimeout: opt.clickHouseOperationTimeout,
		MaxConns: int32(opt.clickHouseMaxConns), MinConns: int32(opt.clickHouseMinConns), TLS: tlsConfig,
	}, nil
}

func loadBootstrap(ctx context.Context, opt options, identity flowworker.VersionWorkerIdentity) (*flowplan.Catalog, *flowworker.EnrichmentVersionCatalog, *flowdimension.GeoCatalog, flowworker.RemoteVersionSynchronizer, uint32, error) {
	if len(opt.planFiles) == 0 || strings.TrimSpace(opt.planPublicKey) == "" {
		return nil, nil, nil, nil, 0, errors.New("at least one bootstrap plan plus the plan public key are required")
	}
	plans, err := flowplan.NewCatalog()
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}
	for _, path := range opt.planFiles {
		registry, err := flowplan.LoadHistoricalSignedPlan(path, opt.planPublicKey)
		if err != nil {
			return nil, nil, nil, nil, 0, fmt.Errorf("load bootstrap plan %s: %w", filepath.Base(path), err)
		}
		if err := plans.Install(registry); err != nil {
			return nil, nil, nil, nil, 0, fmt.Errorf("install bootstrap plan %s: %w", filepath.Base(path), err)
		}
	}

	if strings.TrimSpace(opt.controlPlaneURL) != "" {
		if len(opt.versionPublications) != 0 || len(opt.geoBundles) != 0 {
			return nil, nil, nil, nil, 0, errors.New("remote enrichment versions cannot be combined with bootstrap version or Geo files")
		}
		versions, syncer, cursor, err := loadRemoteVersions(ctx, opt, identity)
		if err != nil {
			return nil, nil, nil, nil, 0, err
		}
		return plans, versions, nil, syncer, cursor, nil
	}
	if hasRemoteVersionOptions(opt) {
		return nil, nil, nil, nil, 0, errors.New("control-plane URL is required when remote enrichment options are configured")
	}
	if len(opt.versionPublications) == 0 {
		return nil, nil, nil, nil, 0, errors.New("at least one bootstrap version publication is required when remote enrichment is disabled")
	}
	versions, err := flowworker.NewEnrichmentVersionCatalog()
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}
	loader, err := flowworker.NewVersionLoader(fileObjectSource{}, discardAcknowledgement{}, versions, identity, flowworker.VersionLoaderLimits{})
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}
	needsLegacyGeo := false
	for _, path := range opt.versionPublications {
		publication, err := loadPublication(path)
		if err != nil {
			return nil, nil, nil, nil, 0, err
		}
		if err := loader.Install(ctx, publication); err != nil {
			return nil, nil, nil, nil, 0, fmt.Errorf("install bootstrap version %s: %w", filepath.Base(path), err)
		}
		if publication.Dimension.ObjectFormat != flowworker.VersionObjectFormatWADS {
			needsLegacyGeo = true
		}
	}

	if len(opt.geoBundles) == 0 {
		if needsLegacyGeo {
			return nil, nil, nil, nil, 0, errors.New("a Geo bundle is required while a legacy JSON dimension publication is retained")
		}
		return plans, versions, nil, nil, 0, nil
	}
	geo := flowdimension.NewGeoCatalog()
	activeGeo := opt.geoBundles[len(opt.geoBundles)-1]
	if _, err := geo.Reload(activeGeo, flowdimension.GeoLoadLimits{}); err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("load active Geo bundle %s: %w", filepath.Base(activeGeo), err)
	}
	for _, path := range opt.geoBundles[:len(opt.geoBundles)-1] {
		if _, err := geo.LoadHistorical(path, flowdimension.GeoLoadLimits{}); err != nil {
			return nil, nil, nil, nil, 0, fmt.Errorf("load historical Geo bundle %s: %w", filepath.Base(path), err)
		}
	}
	return plans, versions, geo, nil, 0, nil
}

func loadRemoteVersions(ctx context.Context, opt options, identity flowworker.VersionWorkerIdentity) (*flowworker.EnrichmentVersionCatalog, flowworker.RemoteVersionSynchronizer, uint32, error) {
	if strings.TrimSpace(opt.versionLKGDir) == "" || opt.versionRefreshInterval < 5*time.Second || opt.versionRefreshInterval > time.Hour {
		return nil, nil, 0, errors.New("remote enrichment requires an LKG directory and refresh interval of 5s..1h")
	}
	client, err := buildVersionHTTPClient(opt, identity)
	if err != nil {
		return nil, nil, 0, err
	}
	lkg, err := flowworker.NewDiskVersionLKG(opt.versionLKGDir)
	if err != nil {
		return nil, nil, 0, err
	}
	trust := &flowplan.TrustStore{}
	versions, err := flowworker.NewEnrichmentVersionCatalog()
	if err != nil {
		return nil, nil, 0, err
	}
	restored := flowworker.VersionLKGRestoreResult{}
	restoredDeployments := flowworker.DeploymentLKGRestoreResult{}
	if _, err := lkg.LoadTrustBundle(); err == nil {
		restored, err = lkg.Restore(ctx, trust, versions, identity, flowworker.VersionLoaderLimits{}, time.Now().UTC())
		if err != nil && !errors.Is(err, flowworker.ErrNoVersionLKG) {
			return nil, nil, 0, fmt.Errorf("restore enrichment LKG: %w", err)
		}
		restoredDeployments, err = lkg.RestoreDeployments(ctx, trust, versions, flowworker.DeploymentLoaderLimits{}, time.Now().UTC())
		if err != nil && !errors.Is(err, flowworker.ErrNoVersionLKG) {
			return nil, nil, 0, fmt.Errorf("restore worker deployment LKG: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, 0, fmt.Errorf("read enrichment LKG trust bundle: %w", err)
	}
	legacySync, err := flowworker.NewRemoteVersionSync(client, lkg, trust, versions, flowworker.VersionLoaderLimits{})
	if err != nil {
		return nil, nil, 0, err
	}
	deploymentSync, err := flowworker.NewRemoteDeploymentSync(client, lkg, trust, versions, flowworker.DeploymentLoaderLimits{})
	if err != nil {
		return nil, nil, 0, err
	}
	syncer, err := flowworker.NewDualRemoteVersionSync(legacySync, deploymentSync, restoredDeployments.DeploymentCount != 0)
	if err != nil {
		return nil, nil, 0, err
	}
	cursor := restored.HighestVersion
	if restoredDeployments.HighestGeneration > uint64(cursor) {
		if restoredDeployments.HighestGeneration > uint64(^uint32(0)) {
			return nil, nil, 0, errors.New("restored worker deployment exceeds the compatible version range")
		}
		cursor = uint32(restoredDeployments.HighestGeneration)
	}
	if opt.check {
		return versions, syncer, cursor, nil
	}
	result, syncErr := syncer.SyncOnce(ctx, cursor)
	if syncErr != nil {
		if restored.PublicationCount == 0 && restoredDeployments.DeploymentCount == 0 {
			return nil, nil, 0, fmt.Errorf("initial enrichment sync without LKG: %w", syncErr)
		}
		log.Printf("flow-worker enrichment sync unavailable; using LKG version %d: %v", cursor, syncErr)
		return versions, syncer, cursor, nil
	}
	if result.HighestVersion == 0 {
		return nil, nil, 0, errors.New("control plane returned no enrichment version and no LKG is installed")
	}
	return versions, syncer, result.HighestVersion, nil
}

func buildVersionHTTPClient(opt options, identity flowworker.VersionWorkerIdentity) (*flowworker.VersionHTTPClient, error) {
	endpoint, err := url.Parse(strings.TrimSpace(opt.controlPlaneURL))
	if err != nil || endpoint == nil || endpoint.Host == "" {
		return nil, errors.New("control-plane URL is invalid")
	}
	if opt.controlPlaneTimeout < 5*time.Second || opt.controlPlaneTimeout > 10*time.Minute {
		return nil, errors.New("control-plane timeout must be 5s..10m")
	}
	certificateConfigured := strings.TrimSpace(opt.controlPlaneCertFile) != "" || strings.TrimSpace(opt.controlPlaneKeyFile) != ""
	if (strings.TrimSpace(opt.controlPlaneCertFile) == "") != (strings.TrimSpace(opt.controlPlaneKeyFile) == "") {
		return nil, errors.New("control-plane TLS certificate and key must be configured together")
	}
	tlsConfigured := strings.TrimSpace(opt.controlPlaneCAFile) != "" || certificateConfigured || strings.TrimSpace(opt.controlPlaneServerName) != ""
	if endpoint.Scheme != "https" && tlsConfigured {
		return nil, errors.New("control-plane TLS options require an https URL")
	}
	if endpoint.Scheme == "http" && !isLoopbackControlPlane(endpoint.Hostname()) {
		return nil, errors.New("cleartext control-plane URL is allowed only on loopback")
	}
	token := ""
	if strings.TrimSpace(opt.agentTokenFile) != "" {
		token, err = flowstream.ReadSecretFile(opt.agentTokenFile)
		if err != nil {
			return nil, fmt.Errorf("flow worker agent token: %w", err)
		}
	}
	if (token != "") == certificateConfigured {
		return nil, errors.New("configure exactly one of flow worker agent token or mTLS certificate")
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if endpoint.Scheme == "https" {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("load control-plane system CA pool: %w", err)
		}
		if pool == nil {
			pool = x509.NewCertPool()
		}
		if strings.TrimSpace(opt.controlPlaneCAFile) != "" {
			pem, err := os.ReadFile(opt.controlPlaneCAFile)
			if err != nil {
				return nil, fmt.Errorf("read control-plane TLS CA: %w", err)
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, errors.New("control-plane TLS CA contains no certificate")
			}
		}
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: strings.TrimSpace(opt.controlPlaneServerName)}
		if certificateConfigured {
			certificate, err := tls.LoadX509KeyPair(opt.controlPlaneCertFile, opt.controlPlaneKeyFile)
			if err != nil {
				return nil, fmt.Errorf("load control-plane TLS client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{certificate}
		}
		transport.TLSClientConfig = tlsConfig
	}
	return flowworker.NewVersionHTTPClient(flowworker.VersionHTTPClientConfig{
		BaseURL: strings.TrimSpace(opt.controlPlaneURL), AgentToken: token, MutualTLS: certificateConfigured,
		Identity: identity, Client: &http.Client{Transport: transport, Timeout: opt.controlPlaneTimeout},
	})
}

func hasRemoteVersionOptions(opt options) bool {
	return strings.TrimSpace(opt.versionLKGDir) != "" ||
		strings.TrimSpace(opt.controlPlaneCAFile) != "" || strings.TrimSpace(opt.controlPlaneCertFile) != "" ||
		strings.TrimSpace(opt.controlPlaneKeyFile) != "" || strings.TrimSpace(opt.controlPlaneServerName) != ""
}

func isLoopbackControlPlane(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

func runVersionSyncLoop(ctx context.Context, syncer flowworker.RemoteVersionSynchronizer, cursor uint32, barrierSync func(context.Context) error, interval time.Duration, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if barrierSync != nil {
				if err := barrierSync(ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("flow-worker raw-delete barrier sync failed; retaining LKG: %v", err)
				}
			}
			result, err := syncer.SyncOnce(ctx, cursor)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Printf("flow-worker enrichment sync failed; retaining version %d: %v", cursor, err)
				}
				continue
			}
			if result.HighestVersion > cursor {
				log.Printf("flow-worker enrichment version advanced: from=%d to=%d count=%d", cursor, result.HighestVersion, result.Installed)
				cursor = result.HighestVersion
			}
		}
	}
}

func loadRawDeleteBarrier(ctx context.Context, opt options, identity flowworker.VersionWorkerIdentity) (*flowtombstone.Guard, func(context.Context) error, error) {
	client, err := buildVersionHTTPClient(opt, identity)
	if err != nil {
		return nil, nil, err
	}
	path := filepath.Join(opt.versionLKGDir, "raw-delete-barrier.json")
	guard, err := flowtombstone.NewGuard(nil)
	if err != nil {
		return nil, nil, err
	}
	if restored, loadErr := flowtombstone.LoadFile(path); loadErr == nil {
		if err := guard.Install(restored); err != nil {
			return nil, nil, fmt.Errorf("restore raw-delete barrier LKG: %w", err)
		}
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("read raw-delete barrier LKG: %w", loadErr)
	}
	syncOnce := func(syncCtx context.Context) error {
		barrier, found, fetchErr := client.FetchRawDeleteBarrier(syncCtx)
		if fetchErr != nil {
			return fetchErr
		}
		if !found {
			if _, installed := guard.Current(); installed {
				return errors.New("control plane omitted an installed raw-delete barrier")
			}
			return nil
		}
		if err := flowtombstone.SaveFile(path, barrier); err != nil {
			return fmt.Errorf("persist raw-delete barrier LKG: %w", err)
		}
		if err := guard.Install(barrier); err != nil {
			return fmt.Errorf("install raw-delete barrier: %w", err)
		}
		if err := client.AcknowledgeRawDeleteBarrier(syncCtx, barrier); err != nil {
			return err
		}
		return nil
	}
	if err := syncOnce(ctx); err != nil {
		if _, installed := guard.Current(); !installed {
			return nil, nil, fmt.Errorf("initial raw-delete barrier sync without LKG: %w", err)
		}
		log.Printf("flow-worker raw-delete barrier acknowledgement unavailable; using installed LKG and retrying: %v", err)
	}
	return guard, syncOnce, nil
}

type fileObjectSource struct{}

func (fileObjectSource) Fetch(_ context.Context, objectRef string, maxBytes int) ([]byte, error) {
	return readBoundedFile(objectRef, int64(maxBytes))
}

type discardAcknowledgement struct{}

func (discardAcknowledgement) Acknowledge(context.Context, flowworker.EnrichmentVersionAcknowledgement) error {
	return nil
}

func loadPublication(path string) (flowworker.EnrichmentVersionPublication, error) {
	data, err := readBoundedFile(path, maxPublicationBytes)
	if err != nil {
		return flowworker.EnrichmentVersionPublication{}, fmt.Errorf("read version publication %s: %w", filepath.Base(path), err)
	}
	var publication flowworker.EnrichmentVersionPublication
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&publication); err != nil {
		return flowworker.EnrichmentVersionPublication{}, fmt.Errorf("decode version publication %s: %w", filepath.Base(path), err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return flowworker.EnrichmentVersionPublication{}, fmt.Errorf("version publication %s must contain one JSON document", filepath.Base(path))
	}
	base := filepath.Dir(path)
	if !filepath.IsAbs(publication.Dimension.ObjectRef) {
		publication.Dimension.ObjectRef = filepath.Join(base, publication.Dimension.ObjectRef)
	}
	if !filepath.IsAbs(publication.Classification.ObjectRef) {
		publication.Classification.ObjectRef = filepath.Join(base, publication.Classification.ObjectRef)
	}
	return publication, nil
}

func readBoundedFile(path string, maxBytes int64) ([]byte, error) {
	if strings.TrimSpace(path) == "" || maxBytes < 1 {
		return nil, errors.New("file path and positive byte limit are required")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(io.LimitReader(file, maxBytes+1), 64<<10)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("file size must be 1..%d bytes", maxBytes)
	}
	return data, nil
}

func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			result = append(result, part)
		}
	}
	return result
}
