// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowmetrics"
	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowworker"
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
	planFiles, versionPublications, geoBundles stringList
	planPublicKey, workerID                    string

	brokers, topic, clientID, consumerGroup  string
	kafkaCAFile, kafkaCertFile, kafkaKeyFile string
	kafkaServerName, saslMechanism           string
	saslUsername, saslPasswordFile           string
	kafkaTLS                                 bool
	fetchMinBytes, templateReplayRecords     int64
	fetchMaxWait, decoderStateTTL            time.Duration

	clickHouseAddress, clickHouseDatabase        string
	clickHouseUser, clickHousePasswordFile       string
	clickHouseCAFile, clickHouseCertFile         string
	clickHouseKeyFile, clickHouseServerName      string
	clickHouseTLS                                bool
	clickHouseMaxConns, clickHouseMinConns       int
	clickHouseDialTimeout, clickHouseReadTimeout time.Duration
	blockMaxRows, blockMaxBytes                  int
	metricsListen                                string
	check                                        bool
}

func main() {
	var opt options
	flag.Var(&opt.planFiles, "bootstrap-plan", "signed plan file; repeat for every registry revision retained in Kafka")
	flag.StringVar(&opt.planPublicKey, "plan-public-key", "", "Ed25519 plan public key file")
	flag.Var(&opt.versionPublications, "bootstrap-version-publication", "dimension/classification publication JSON; repeat for retained event-time versions")
	flag.Var(&opt.geoBundles, "geo-bundle", "flow-geo-v1 or flow-geo-v2 bundle directory; repeat for retained event-time versions")
	flag.StringVar(&opt.workerID, "worker-id", "watchdog-flow-worker", "stable worker instance identity")

	flag.StringVar(&opt.brokers, "kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	flag.StringVar(&opt.topic, "kafka-topic", "watchdog.flow.raw", "RawFlow Kafka topic base; schema suffix is automatic")
	flag.StringVar(&opt.clientID, "kafka-client-id", "watchdog-flow-worker", "Kafka client ID")
	flag.StringVar(&opt.consumerGroup, "kafka-consumer-group", "watchdog-flow-worker-v1", "Kafka consumer group")
	flag.Int64Var(&opt.fetchMinBytes, "kafka-fetch-min-bytes", 1_000_000, "minimum Kafka fetch bytes")
	flag.DurationVar(&opt.fetchMaxWait, "kafka-fetch-max-wait", time.Second, "maximum Kafka fetch wait")
	flag.Int64Var(&opt.templateReplayRecords, "kafka-template-replay-records", 1_000_000, "records replayed before each committed partition offset to rebuild templates")
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
	flag.DurationVar(&opt.clickHouseReadTimeout, "clickhouse-read-timeout", 30*time.Second, "ClickHouse packet read timeout")
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

	if err := run(opt); err != nil {
		log.Fatal(err)
	}
}

func run(opt options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
	plans, versions, geo, err := loadBootstrap(ctx, opt)
	if err != nil {
		return err
	}
	if opt.decoderStateTTL < time.Minute || opt.decoderStateTTL > 24*time.Hour {
		return errors.New("decoder state TTL must be 1m..24h")
	}
	if opt.blockMaxRows < 1 || opt.blockMaxRows > 1_000_000 || opt.blockMaxBytes < 1 || opt.blockMaxBytes > 1<<30 {
		return errors.New("ClickHouse block limits are invalid")
	}
	if opt.check {
		log.Printf("flow-worker configuration valid: plans=%d versions=%d geo_versions=%d group=%s", len(opt.planFiles), len(opt.versionPublications), len(opt.geoBundles), consumerConfig.ConsumerGroup)
		return nil
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
	writer, err := flowch.NewWriter(native, flowch.WriterConfig{Limits: flowch.BatchLimits{MaxRows: opt.blockMaxRows, MaxApproxBytes: opt.blockMaxBytes}})
	if err != nil {
		return err
	}
	pipeline, err := flowch.NewPipeline(enricher, writer)
	if err != nil {
		return err
	}
	processor, err := flowworker.NewBatchProcessor(opt.decoderStateTTL, plans.Resolve, pipeline.Handle, nil)
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
	log.Printf("flow-worker started: plans=%d versions=%d geo_versions=%d group=%s", len(opt.planFiles), len(opt.versionPublications), len(opt.geoBundles), consumerConfig.ConsumerGroup)
	runCtx, cancelRun := context.WithCancel(ctx)
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
	stats := processor.Stats()
	log.Printf("flow-worker stopped: datagrams=%d records=%d template_missing=%d rejected=%d retryable_errors=%d", stats.Datagrams, stats.Records, stats.TemplateMissing, stats.Rejected, stats.RetryableErrors)
	return err
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
	config.FetchMaxWait = opt.fetchMaxWait
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
	if strings.TrimSpace(opt.clickHouseAddress) == "" || strings.TrimSpace(opt.clickHouseDatabase) == "" || strings.TrimSpace(opt.clickHouseUser) == "" || opt.clickHouseDialTimeout <= 0 || opt.clickHouseReadTimeout <= 0 {
		return flowch.NativeConfig{}, errors.New("ClickHouse address, database, user, and positive timeouts are required")
	}
	return flowch.NativeConfig{
		Address: strings.TrimSpace(opt.clickHouseAddress), Database: strings.TrimSpace(opt.clickHouseDatabase), User: strings.TrimSpace(opt.clickHouseUser), Password: password,
		ClientName:  "watchdog-flow-worker",
		DialTimeout: opt.clickHouseDialTimeout, ReadTimeout: opt.clickHouseReadTimeout, MaxConns: int32(opt.clickHouseMaxConns), MinConns: int32(opt.clickHouseMinConns), TLS: tlsConfig,
	}, nil
}

func loadBootstrap(ctx context.Context, opt options) (*flowplan.Catalog, *flowworker.EnrichmentVersionCatalog, *flowdimension.GeoCatalog, error) {
	if len(opt.planFiles) == 0 || strings.TrimSpace(opt.planPublicKey) == "" || len(opt.versionPublications) == 0 || len(opt.geoBundles) == 0 {
		return nil, nil, nil, errors.New("at least one bootstrap plan, version publication, and Geo bundle plus the plan public key are required")
	}
	plans, err := flowplan.NewCatalog()
	if err != nil {
		return nil, nil, nil, err
	}
	for _, path := range opt.planFiles {
		registry, err := flowplan.LoadHistoricalSignedPlan(path, opt.planPublicKey)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("load bootstrap plan %s: %w", filepath.Base(path), err)
		}
		if err := plans.Install(registry); err != nil {
			return nil, nil, nil, fmt.Errorf("install bootstrap plan %s: %w", filepath.Base(path), err)
		}
	}

	versions, err := flowworker.NewEnrichmentVersionCatalog()
	if err != nil {
		return nil, nil, nil, err
	}
	loader, err := flowworker.NewVersionLoader(fileObjectSource{}, discardAcknowledgement{}, versions, flowworker.VersionWorkerIdentity{
		WorkerID: strings.TrimSpace(opt.workerID), BootID: fmt.Sprintf("bootstrap-%d", os.Getpid()), SoftwareVersion: "watchdog-flow-worker-v1",
	}, flowworker.VersionLoaderLimits{})
	if err != nil {
		return nil, nil, nil, err
	}
	for _, path := range opt.versionPublications {
		publication, err := loadPublication(path)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := loader.Install(ctx, publication); err != nil {
			return nil, nil, nil, fmt.Errorf("install bootstrap version %s: %w", filepath.Base(path), err)
		}
	}

	geo := flowdimension.NewGeoCatalog()
	for _, path := range opt.geoBundles {
		if _, err := geo.LoadHistorical(path, flowdimension.GeoLoadLimits{}); err != nil {
			return nil, nil, nil, fmt.Errorf("load Geo bundle %s: %w", filepath.Base(path), err)
		}
	}
	return plans, versions, geo, nil
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
