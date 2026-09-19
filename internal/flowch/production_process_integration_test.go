// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

// TestProductionCollectorAndWorkerFourProtocolEndToEnd builds and starts the
// two production commands. It owns an isolated Kafka topic, ClickHouse
// database, and temporary VictoriaMetrics promscrape process.
func TestProductionCollectorAndWorkerFourProtocolEndToEnd(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_PRODUCTION_PROCESS_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_PRODUCTION_PROCESS_INTEGRATION=1 to run")
	}
	t.Setenv("WATCHDOG_FLOW_CLICKHOUSE_DATA_INTEGRATION", "1")
	database := fmt.Sprintf("watchdog_flow_it_process_%d", time.Now().UnixNano())
	ctx, native := openDataIntegrationClickHouse(t, database)

	kafkaAddress, kafkaContainer := startProductionKafka(t)
	brokers := []string{kafkaAddress}
	topicBase := fmt.Sprintf("watchdog.flow.process.%d", time.Now().UnixNano())
	topic := fmt.Sprintf("%s-v%d", topicBase, flowstream.SchemaVersion)
	admin := newCorpusKafkaAdmin(t, ctx, brokers)
	createCorpusTopic(t, ctx, admin, topic, 4)
	t.Cleanup(func() { deleteCorpusTopic(t, admin, topic) })

	root := productionRepositoryRoot(t)
	artifacts := writeProductionBootstrap(t)
	collectorBinary := buildProductionCommand(t, root, "watchdog-flow-collect")
	workerBinary := buildProductionCommand(t, root, "watchdog-flow-worker")
	collectorMetricsPort := reserveTCPPort(t)
	workerMetricsPort := reserveTCPPort(t)
	collectorMetrics := "127.0.0.1:" + collectorMetricsPort
	workerMetrics := "127.0.0.1:" + workerMetricsPort
	sflowPort := reserveUDPPort(t)
	netflowPort := reserveUDPPort(t)
	sflowAddress := "127.0.0.1:" + sflowPort
	netflowAddress := "127.0.0.1:" + netflowPort
	passwordFile := os.Getenv("WATCHDOG_CLICKHOUSE_PASSWORD_FILE")
	if passwordFile == "" {
		t.Fatal("WATCHDOG_CLICKHOUSE_PASSWORD_FILE is required")
	}
	clickHouseAddress := os.Getenv("WATCHDOG_CLICKHOUSE_ADDRESS")
	if clickHouseAddress == "" {
		clickHouseAddress = "127.0.0.1:9000"
	}
	clickHouseProxy := newSilentResponseProxy(t, clickHouseAddress)
	t.Cleanup(clickHouseProxy.Close)
	consumerGroup := "watchdog-flow-process-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	worker := startProductionProcess(t, root, workerBinary, productionWorkerArguments(
		artifacts, brokers, topicBase, consumerGroup, "worker-a", clickHouseProxy.Address(), database, passwordFile, workerMetricsPort,
	)...)
	waitForProcessMetric(t, ctx, worker, workerMetrics, "watchdog_flow_worker_kafka_records_total", 0)
	waitForProcessMetric(t, ctx, worker, workerMetrics, "watchdog_flow_worker_kafka_assigned_partitions", 4)

	collector := startProductionProcess(t, root, collectorBinary,
		"-plan", artifacts.plan,
		"-plan-public-key", artifacts.publicKey,
		"-kafka-brokers", strings.Join(brokers, ","),
		"-kafka-topic", topicBase,
		"-sflow-listen", "0.0.0.0:"+sflowPort,
		"-netflow-listen", "0.0.0.0:"+netflowPort,
		"-metrics-listen", "0.0.0.0:"+collectorMetricsPort,
		"-receive-buffer-bytes", strconv.Itoa(1<<20),
		"-max-datagram-bytes", "4096",
	)
	waitForProcessMetric(t, ctx, collector, collectorMetrics, "watchdog_flow_collector_kafka_records_total", 0)
	waitForUDPListeners(t, ctx, collector, sflowAddress, netflowAddress)
	netflowSender := dialUDPSender(t, "", netflowAddress)
	defer netflowSender.Close()
	unknownSource := nonLoopbackIPv4(t)
	sendUDPPayloadsFrom(t, unknownSource, net.JoinHostPort(unknownSource, netflowPort), corpusFixturePayload(t, "netflow", "nfv5.pcap"))
	waitForProcessMetric(t, ctx, collector, collectorMetrics, "watchdog_flow_collector_datagrams_rejected_total", 1)
	writeUDPPayloads(t, netflowSender, make([]byte, 4097))
	waitForProcessMetric(t, ctx, collector, collectorMetrics, "watchdog_flow_collector_datagrams_oversize_total", 1)
	assertProcessMetric(t, collectorMetrics, "watchdog_flow_collector_kafka_records_total", 0)

	pauseProductionContainer(t, kafkaContainer)
	writeUDPPayloads(t, netflowSender, corpusFixturePayload(t, "netflow", "template.pcap"))
	waitForProcessMetric(t, ctx, collector, collectorMetrics, "watchdog_flow_collector_kafka_buffered_records", 1)
	assertProcessMetric(t, collectorMetrics, "watchdog_flow_collector_kafka_records_total", 0)
	unpauseProductionContainer(t, kafkaContainer)
	waitForProcessMetric(t, ctx, collector, collectorMetrics, "watchdog_flow_collector_kafka_records_total", 1)
	waitForProcessMetric(t, ctx, worker, workerMetrics, "watchdog_flow_worker_kafka_records_total", 1)
	waitForKafkaOffsets(t, kafkaContainer, consumerGroup, topic, 1)
	restartProductionContainer(t, kafkaContainer)
	waitForProductionKafka(t, kafkaContainer)
	waitForKafkaOffsets(t, kafkaContainer, consumerGroup, topic, 1)

	clickHouseProxy.DropServerResponses()
	writeUDPPayloads(t, netflowSender, corpusFixturePayload(t, "netflow", "nfv5.pcap"))
	waitForProcessMetric(t, ctx, collector, collectorMetrics, "watchdog_flow_collector_kafka_records_total", 2)
	waitForProcessMetric(t, ctx, worker, workerMetrics, `watchdog_flow_clickhouse_insert_errors_total{class="retryable"}`, 1)
	assertProcessMetric(t, workerMetrics, "watchdog_flow_worker_kafka_records_total", 1)
	clickHouseProxy.ForwardServerResponses()
	waitForProcessMetric(t, ctx, worker, workerMetrics, "watchdog_flow_worker_kafka_records_total", 2)

	writeUDPPayloads(t, netflowSender,
		corpusFixturePayload(t, "netflow", "data.pcap"),
		corpusFixturePayload(t, "netflow", "ipfixprobe-templates.pcap"),
		corpusFixturePayload(t, "netflow", "ipfixprobe-data.pcap"),
	)
	sendUDPPayloads(t, sflowAddress, corpusFixturePayload(t, "sflow", "data-sflow-expanded-sample.pcap"))

	waitForProcessMetric(t, ctx, collector, collectorMetrics, "watchdog_flow_collector_kafka_records_total", 6)
	waitForProcessMetric(t, ctx, worker, workerMetrics, "watchdog_flow_worker_kafka_records_total", 6)
	waitForProductionFacts(t, ctx, native)
	assertProcessMetric(t, collectorMetrics, "watchdog_flow_collector_datagrams_received_total{decoder=\"netflow\"}", 5)
	assertProcessMetric(t, collectorMetrics, "watchdog_flow_collector_datagrams_received_total{decoder=\"sflow\"}", 1)
	assertProcessMetric(t, workerMetrics, "watchdog_flow_worker_template_missing_total", 0)
	assertProcessMetric(t, workerMetrics, "watchdog_flow_worker_rejected_total", 0)
	assertProcessMetric(t, workerMetrics, "watchdog_flow_worker_retryable_errors_total", 0)

	runID := "flow-process-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	vmURL := startScrapingVictoriaMetrics(t, runID, collectorMetricsPort, workerMetricsPort)
	waitForProductionVMValue(t, ctx, vmURL, runID, "collector", "up", 1)
	waitForProductionVMValue(t, ctx, vmURL, runID, "worker", "up", 1)
	waitForProductionVMValue(t, ctx, vmURL, runID, "collector", "watchdog_flow_collector_kafka_records_total", 6)
	waitForProductionVMValue(t, ctx, vmURL, runID, "worker", "watchdog_flow_worker_kafka_records_total", 6)
	waitForKafkaOffsets(t, kafkaContainer, consumerGroup, topic, 6)

	worker2MetricsPort := reserveTCPPort(t)
	worker2Metrics := "127.0.0.1:" + worker2MetricsPort
	worker2 := startProductionProcess(t, root, workerBinary, productionWorkerArguments(
		artifacts, brokers, topicBase, consumerGroup, "worker-b", clickHouseProxy.Address(), database, passwordFile, worker2MetricsPort,
	)...)
	waitForProcessMetric(t, ctx, worker2, worker2Metrics, "watchdog_flow_worker_kafka_rebalances_total", 1)
	waitForWorkerAssignments(t, ctx, worker, workerMetrics, worker2, worker2Metrics, 2, 2)
	sendUDPPayloads(t, sflowAddress, corpusFixturePayload(t, "sflow", "data-sflow-expanded-sample.pcap"))
	waitForProcessMetric(t, ctx, collector, collectorMetrics, "watchdog_flow_collector_kafka_records_total", 7)
	waitForKafkaOffsets(t, kafkaContainer, consumerGroup, topic, 7)
	assertProcessMetric(t, workerMetrics, "watchdog_flow_worker_template_missing_total", 0)
	assertProcessMetric(t, worker2Metrics, "watchdog_flow_worker_template_missing_total", 0)

	worker.kill(t)
	waitForProcessMetric(t, ctx, worker2, worker2Metrics, "watchdog_flow_worker_kafka_rebalances_total", 2)
	waitForProcessMetric(t, ctx, worker2, worker2Metrics, "watchdog_flow_worker_kafka_assigned_partitions", 4)
	sendUDPPayloads(t, sflowAddress, corpusFixturePayload(t, "sflow", "data-sflow-expanded-sample.pcap"))
	waitForProcessMetric(t, ctx, collector, collectorMetrics, "watchdog_flow_collector_kafka_records_total", 8)
	waitForKafkaOffsets(t, kafkaContainer, consumerGroup, topic, 8)
	assertProcessMetric(t, worker2Metrics, "watchdog_flow_worker_template_missing_total", 0)
	assertProcessMetric(t, worker2Metrics, "watchdog_flow_worker_rejected_total", 0)
	waitForProductionFacts(t, ctx, native)

	collector.stop(t)
	worker2.stop(t)
}

func (p *silentResponseProxy) ForwardServerResponses() {
	p.responseMode.Store(proxyForward)
}

func productionWorkerArguments(
	artifacts productionBootstrap,
	brokers []string,
	topicBase, consumerGroup, workerID, clickHouseAddress, database, passwordFile, metricsPort string,
) []string {
	return []string{
		"-bootstrap-plan", artifacts.plan,
		"-plan-public-key", artifacts.publicKey,
		"-bootstrap-version-publication", artifacts.publication,
		"-geo-bundle", artifacts.geo,
		"-worker-id", workerID,
		"-kafka-brokers", strings.Join(brokers, ","),
		"-kafka-topic", topicBase,
		"-kafka-client-id", workerID,
		"-kafka-consumer-group", consumerGroup,
		"-kafka-fetch-min-bytes", "1",
		"-kafka-fetch-max-wait", "100ms",
		"-kafka-template-replay-records", "64",
		"-clickhouse-address", clickHouseAddress,
		"-clickhouse-database", database,
		"-clickhouse-password-file", passwordFile,
		"-clickhouse-read-timeout", "50ms",
		"-clickhouse-operation-timeout", "250ms",
		"-metrics-listen", "0.0.0.0:" + metricsPort,
	}
}

func startProductionKafka(t testing.TB) (string, string) {
	t.Helper()
	port := reserveTCPPort(t)
	container := "watchdog-flow-kafka-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	image := strings.TrimSpace(os.Getenv("WATCHDOG_KAFKA_IMAGE"))
	if image == "" {
		image = "apache/kafka:4.3.1"
	}
	environment := []string{
		"KAFKA_NODE_ID=1",
		"KAFKA_PROCESS_ROLES=broker,controller",
		"KAFKA_LISTENERS=CONTROLLER://:9093,INTERNAL://:19092,HOST://:9092",
		"KAFKA_ADVERTISED_LISTENERS=INTERNAL://127.0.0.1:19092,HOST://127.0.0.1:" + port,
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,HOST:PLAINTEXT",
		"KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
		"KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
		"KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
		"KAFKA_LOG_DIRS=/tmp/kraft-combined-logs",
	}
	arguments := []string{"run", "--detach", "--rm", "--name", container, "-p", "127.0.0.1:" + port + ":9092"}
	for _, value := range environment {
		arguments = append(arguments, "--env", value)
	}
	arguments = append(arguments, image)
	if output, err := exec.Command("docker", arguments...).CombinedOutput(); err != nil {
		t.Fatalf("start integration Kafka: %v\n%s", err, output)
	}
	t.Cleanup(func() { removeProductionContainer(t, container) })
	waitForProductionKafka(t, container)
	return "127.0.0.1:" + port, container
}

func waitForProductionKafka(t testing.TB, container string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		command := exec.CommandContext(ctx, "docker", "exec", container, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "127.0.0.1:19092", "--list")
		if err := command.Run(); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			logs, _ := exec.Command("docker", "logs", container).CombinedOutput()
			t.Fatalf("wait for integration Kafka: %v\n%s", ctx.Err(), logs)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func restartProductionContainer(t testing.TB, container string) {
	t.Helper()
	if output, err := exec.Command("docker", "restart", container).CombinedOutput(); err != nil {
		t.Fatalf("restart %s: %v\n%s", container, err, output)
	}
}

func pauseProductionContainer(t testing.TB, container string) {
	t.Helper()
	if output, err := exec.Command("docker", "pause", container).CombinedOutput(); err != nil {
		t.Fatalf("pause %s: %v\n%s", container, err, output)
	}
}

func unpauseProductionContainer(t testing.TB, container string) {
	t.Helper()
	if output, err := exec.Command("docker", "unpause", container).CombinedOutput(); err != nil {
		t.Fatalf("unpause %s: %v\n%s", container, err, output)
	}
}

func removeProductionContainer(t testing.TB, container string) {
	t.Helper()
	output, err := exec.Command("docker", "rm", "--force", container).CombinedOutput()
	if err != nil && !strings.Contains(string(output), "No such container") {
		t.Errorf("remove %s: %v\n%s", container, err, output)
	}
}

type productionBootstrap struct {
	plan, publicKey, publication, geo string
}

func writeProductionBootstrap(t testing.TB) productionBootstrap {
	t.Helper()
	directory := t.TempDir()
	effectiveFrom := time.Unix(0, 0).UTC()
	dimension := flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: "dimension-process",
		Version: 1, EffectiveFrom: effectiveFrom,
		Prefixes: []flowdimension.PrefixDefinition{
			{ID: "all_ipv4", CIDR: "0.0.0.0/0", Labels: map[string]string{"flow": "local", "business": "process"}},
			{ID: "all_ipv6", CIDR: "::/0", Labels: map[string]string{"flow": "local", "business": "process"}},
		},
	}
	classification := flowdimension.ClassificationBundle{
		SchemaVersion: flowdimension.LegacyClassificationSchemaVersion, Version: 1,
		EffectiveFrom: effectiveFrom, DimensionSnapshotID: dimension.SnapshotID,
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	}
	dimensionData := productionJSON(t, dimension)
	classificationData := productionJSON(t, classification)
	dimensionPath := writeProductionFile(t, directory, "dimension.json", dimensionData, 0o600)
	classificationPath := writeProductionFile(t, directory, "classification.json", classificationData, 0o600)
	publication := flowworker.EnrichmentVersionPublication{
		PublicationID: "publication-process", DimensionSnapshotID: dimension.SnapshotID,
		DimensionVersion: 1, DimensionEffectiveFrom: effectiveFrom,
		Dimension:             flowworker.VersionObjectReference{ObjectRef: filepath.Base(dimensionPath), Checksum: productionChecksum(dimensionData)},
		ClassificationVersion: 1, ClassificationEffectiveFrom: effectiveFrom,
		Classification: flowworker.VersionObjectReference{ObjectRef: filepath.Base(classificationPath), Checksum: productionChecksum(classificationData)},
	}
	publicationPath := writeProductionFile(t, directory, "publication.json", productionJSON(t, publication), 0o600)

	now := time.Now().UTC()
	sources := make([]flowplan.SourceBinding, 0, 4)
	for _, protocol := range []flowplan.Protocol{flowplan.ProtocolSFlow5, flowplan.ProtocolNetFlow5, flowplan.ProtocolNetFlow9, flowplan.ProtocolIPFIX} {
		sources = append(sources, flowplan.SourceBinding{
			Protocol: protocol, SourcePrefix: "127.0.0.1/32",
			ExporterID: fmt.Sprintf("exporter-%d", protocol), TargetID: "target-process", DeviceID: "device-process",
			OwnershipEpoch: 1, SamplingMode: flowplan.SamplingModePreScaled, Enabled: true,
		})
	}
	plan := flowplan.Plan{
		SchemaVersion: 2, Revision: 1, CollectorID: "collector-process",
		NotBefore: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour), Sources: sources,
	}
	planData := productionJSON(t, plan)
	planDigest := sha256.Sum256(planData)
	metadata := flowplan.PlanSignatureMetadata{
		PlanID: "plan-process", CollectorID: plan.CollectorID,
		ConfigVersion: plan.Revision, PlanSchemaVersion: uint16(plan.SchemaVersion),
		SpecHash: hex.EncodeToString(planDigest[:]), SigningKeyID: "process-key",
		NotBeforeUnixMilli: plan.NotBefore.UnixMilli(), ExpiresAtUnixMilli: plan.ExpiresAt.UnixMilli(),
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signingPayload, err := flowplan.BuildPlanSignaturePayload(metadata)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := flowplan.MarshalControlPlaneSignedPlan(metadata, planData, ed25519.Sign(privateKey, signingPayload))
	if err != nil {
		t.Fatal(err)
	}
	planPath := writeProductionFile(t, directory, "plan.json", envelope, 0o600)
	publicKeyPath := writeProductionFile(t, directory, "plan-public-key", []byte(base64.StdEncoding.EncodeToString(publicKey)), 0o600)
	geoPath := filepath.Join(directory, "geo")
	if err := os.Mkdir(geoPath, 0o700); err != nil {
		t.Fatal(err)
	}
	writeProductionGeoBundle(t, geoPath, effectiveFrom)
	return productionBootstrap{plan: planPath, publicKey: publicKeyPath, publication: publicationPath, geo: geoPath}
}

func writeProductionGeoBundle(t testing.TB, directory string, effectiveFrom time.Time) {
	t.Helper()
	header := "ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn,geo_leaf_code"
	files := map[string][]byte{
		"ipv4.csv.zst":   corpusZstd(t, []byte(header+"\n0.0.0.0,255.255.255.255,CN,330100,Zhejiang,Hangzhou,3,64500,330100\n")),
		"ipv6.csv.zst":   corpusZstd(t, []byte(header+"\n::,ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff,CN,330100,Zhejiang,Hangzhou,3,64500,330100\n")),
		"operators.json": productionJSON(t, []flowdimension.GeoOperator{{ID: 3, Name: "Process Carrier", ShortName: "Process", Category: "carrier", Enabled: true}}),
		"geo_dict.json": productionJSON(t, []flowdimension.GeoDictionaryEntry{
			{Kind: "continent", Code: "Asia", Name: "Asia", Enabled: true},
			{Kind: "region", Code: "EastAsia", Name: "East Asia", ParentCode: "Asia", Enabled: true},
			{Kind: "country", Code: "CN", Name: "China", ParentCode: "EastAsia", Enabled: true},
			{Kind: "province", Code: "330000", Name: "Zhejiang", ParentCode: "CN", Enabled: true},
			{Kind: "city", Code: "330100", Name: "Hangzhou", ParentCode: "330000", Enabled: true},
		}),
	}
	manifest := flowdimension.GeoManifest{
		Schema: flowdimension.GeoSchemaV2, Version: "geo-process", GeneratedAt: effectiveFrom, EffectiveFrom: effectiveFrom,
		AdminCodeSystem: flowdimension.GeoAdminCodeSystem, UnknownCountry: flowdimension.GeoUnknownCountry,
		Files: make(map[string]flowdimension.GeoFileSpec, len(files)),
	}
	for name, data := range files {
		writeProductionFile(t, directory, name, data, 0o600)
		digest := sha256.Sum256(data)
		rows := uint64(1)
		if name == "geo_dict.json" {
			rows = 5
		}
		manifest.Files[name] = flowdimension.GeoFileSpec{SHA256: hex.EncodeToString(digest[:]), Rows: rows}
	}
	writeProductionFile(t, directory, "manifest.json", productionJSON(t, manifest), 0o600)
}

type productionProcess struct {
	command  *exec.Cmd
	done     chan struct{}
	logPath  string
	stopOnce sync.Once
	mu       sync.Mutex
	err      error
}

func startProductionProcess(t testing.TB, root, binary string, arguments ...string) *productionProcess {
	t.Helper()
	logFile, err := os.CreateTemp(t.TempDir(), "process-*.log")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, arguments...)
	command.Dir = root
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	process := &productionProcess{command: command, done: make(chan struct{}), logPath: logFile.Name()}
	go func() {
		err := command.Wait()
		process.mu.Lock()
		process.err = err
		process.mu.Unlock()
		_ = logFile.Close()
		close(process.done)
	}()
	t.Cleanup(func() { process.stop(t) })
	return process
}

func (p *productionProcess) stop(t testing.TB) {
	t.Helper()
	p.stopOnce.Do(func() {
		if p.command.Process == nil {
			return
		}
		_ = p.command.Process.Signal(os.Interrupt)
		select {
		case <-p.done:
			err := p.exitError()
			if err != nil {
				t.Errorf("process %s stopped with %v; log:\n%s", filepath.Base(p.command.Path), err, readProductionLog(p.logPath))
			}
		case <-time.After(10 * time.Second):
			_ = p.command.Process.Kill()
			<-p.done
			t.Errorf("process %s did not stop gracefully; log:\n%s", filepath.Base(p.command.Path), readProductionLog(p.logPath))
		}
	})
}

func (p *productionProcess) kill(t testing.TB) {
	t.Helper()
	p.stopOnce.Do(func() {
		if p.command.Process == nil {
			return
		}
		if err := p.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Fatalf("kill process %s: %v", filepath.Base(p.command.Path), err)
		}
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			t.Fatalf("killed process %s did not exit; log:\n%s", filepath.Base(p.command.Path), readProductionLog(p.logPath))
		}
	})
}

func (p *productionProcess) exitError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func productionRepositoryRoot(t testing.TB) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve repository root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
}

func buildProductionCommand(t testing.TB, root, name string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	command := exec.Command("go", "build", "-o", path, "./cmd/"+name)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, output)
	}
	return path
}

func reserveTCPPort(t testing.TB) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func reserveUDPPort(t testing.TB) string {
	t.Helper()
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(listener.LocalAddr().(*net.UDPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func nonLoopbackIPv4(t testing.TB) string {
	t.Helper()
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range interfaces {
		if current.Flags&net.FlagUp == 0 || current.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := current.Addrs()
		if err != nil {
			continue
		}
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err == nil && ip.To4() != nil && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
				return ip.String()
			}
		}
	}
	t.Fatal("a non-loopback IPv4 address is required for source admission integration")
	return ""
}

func waitForUDPListeners(t testing.TB, ctx context.Context, process *productionProcess, addresses ...string) {
	t.Helper()
	for _, address := range addresses {
		udpAddress, err := net.ResolveUDPAddr("udp4", address)
		if err != nil {
			t.Fatal(err)
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		for {
			probe, listenErr := net.ListenUDP("udp4", udpAddress)
			if listenErr != nil {
				break
			}
			_ = probe.Close()
			select {
			case <-process.done:
				t.Fatalf("process %s exited before binding %s: %v; log:\n%s", filepath.Base(process.command.Path), address, process.exitError(), readProductionLog(process.logPath))
			case <-ctx.Done():
				t.Fatalf("wait for UDP listener %s: %v; log:\n%s", address, ctx.Err(), readProductionLog(process.logPath))
			case <-ticker.C:
			}
		}
		ticker.Stop()
	}
}

func sendUDPPayloads(t testing.TB, address string, payloads ...[]byte) {
	t.Helper()
	sendUDPPayloadsFrom(t, "", address, payloads...)
}

func sendUDPPayloadsFrom(t testing.TB, sourceIP, address string, payloads ...[]byte) {
	t.Helper()
	connection := dialUDPSender(t, sourceIP, address)
	defer connection.Close()
	writeUDPPayloads(t, connection, payloads...)
}

func dialUDPSender(t testing.TB, sourceIP, address string) *net.UDPConn {
	t.Helper()
	remote, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		t.Fatal(err)
	}
	var local *net.UDPAddr
	if sourceIP != "" {
		local = &net.UDPAddr{IP: net.ParseIP(sourceIP)}
	}
	connection, err := net.DialUDP("udp4", local, remote)
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func writeUDPPayloads(t testing.TB, connection *net.UDPConn, payloads ...[]byte) {
	t.Helper()
	for _, payload := range payloads {
		if _, err := connection.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
}

func waitForProcessMetric(t testing.TB, ctx context.Context, process *productionProcess, address, metric string, want uint64) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, found, err := readProcessMetric(ctx, address, metric)
		if err == nil && found && value >= float64(want) {
			return
		}
		select {
		case <-process.done:
			t.Fatalf("process %s exited while waiting for %s: %v; log:\n%s", filepath.Base(process.command.Path), metric, process.exitError(), readProductionLog(process.logPath))
		case <-ctx.Done():
			t.Fatalf("wait for %s=%d at %s: %v (last value=%v found=%t error=%v); log:\n%s", metric, want, address, ctx.Err(), value, found, err, readProductionLog(process.logPath))
		case <-ticker.C:
		}
	}
}

func assertProcessMetric(t testing.TB, address, metric string, want float64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, found, err := readProcessMetric(ctx, address, metric)
	if err != nil || !found || got != want {
		t.Fatalf("metric %s at %s=%v found=%t error=%v, want %v", metric, address, got, found, err, want)
	}
}

func readProcessMetric(ctx context.Context, address, metric string) (float64, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/metrics", nil)
	if err != nil {
		return 0, false, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, false, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, false, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == metric {
			value, parseErr := strconv.ParseFloat(fields[1], 64)
			return value, true, parseErr
		}
	}
	return 0, false, nil
}

func waitForProductionFacts(t testing.TB, ctx context.Context, native *NativeInserter) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		var protocols proto.ColUInt64
		var records proto.ColUInt64
		var receipts proto.ColUInt64
		factsQuery := ch.Query{
			Body:   `SELECT uniqExact(flow_protocol), count() FROM flow_records FINAL`,
			Result: proto.Results{{Name: "uniqExact(flow_protocol)", Data: &protocols}, {Name: "count()", Data: &records}},
		}
		err := native.executor.Do(ctx, factsQuery)
		if err == nil {
			receiptsQuery := ch.Query{
				Body:   `SELECT count() FROM flow_ingest_batches FINAL`,
				Result: proto.Results{{Name: "count()", Data: &receipts}},
			}
			err = native.executor.Do(ctx, receiptsQuery)
		}
		if err == nil && protocols.Rows() == 1 && receipts.Rows() == 1 && protocols[0] == 4 && records[0] > 0 && receipts[0] > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for production facts: %v (protocols=%v records=%v receipts=%v error=%v)", ctx.Err(), protocols, records, receipts, err)
		case <-ticker.C:
		}
	}
}

func waitForWorkerAssignments(
	t testing.TB,
	ctx context.Context,
	first *productionProcess,
	firstAddress string,
	second *productionProcess,
	secondAddress string,
	wantFirst, wantSecond uint64,
) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		firstValue, firstFound, firstErr := readProcessMetric(ctx, firstAddress, "watchdog_flow_worker_kafka_assigned_partitions")
		secondValue, secondFound, secondErr := readProcessMetric(ctx, secondAddress, "watchdog_flow_worker_kafka_assigned_partitions")
		if firstErr == nil && secondErr == nil && firstFound && secondFound && firstValue == float64(wantFirst) && secondValue == float64(wantSecond) {
			return
		}
		select {
		case <-first.done:
			t.Fatalf("first worker exited during rebalance: %v\n%s", first.exitError(), readProductionLog(first.logPath))
		case <-second.done:
			t.Fatalf("second worker exited during rebalance: %v\n%s", second.exitError(), readProductionLog(second.logPath))
		case <-ctx.Done():
			t.Fatalf("wait for worker assignments %d/%d: %v (got %v/%v, errors %v/%v)", wantFirst, wantSecond, ctx.Err(), firstValue, secondValue, firstErr, secondErr)
		case <-ticker.C:
		}
	}
}

func waitForKafkaOffsets(t testing.TB, container, group, topic string, want int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var lastOutput []byte
	var lastErr error
	for {
		command := exec.CommandContext(ctx, "docker", "exec", container, "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "127.0.0.1:19092", "--group", group, "--describe")
		lastOutput, lastErr = command.CombinedOutput()
		if lastErr == nil {
			partitions := 0
			var currentTotal, endTotal int64
			zeroLag := true
			for _, line := range strings.Split(string(lastOutput), "\n") {
				fields := strings.Fields(line)
				if len(fields) < 6 || fields[1] != topic {
					continue
				}
				current, currentOK := parseKafkaOffset(fields[3])
				end, endOK := parseKafkaOffset(fields[4])
				lag, lagOK := parseKafkaOffset(fields[5])
				if !currentOK || !endOK || (!lagOK && end != 0) {
					zeroLag = false
					continue
				}
				partitions++
				currentTotal += current
				endTotal += end
				zeroLag = zeroLag && lag == 0
			}
			if partitions == 4 && currentTotal == want && endTotal == want && zeroLag {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for Kafka group %s offset=end=%d: %v (error=%v)\n%s", group, want, ctx.Err(), lastErr, lastOutput)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func parseKafkaOffset(value string) (int64, bool) {
	if value == "-" {
		return 0, true
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil
}

func startScrapingVictoriaMetrics(t testing.TB, runID, collectorPort, workerPort string) string {
	t.Helper()
	config := fmt.Sprintf(`global:
  scrape_interval: 250ms
  scrape_timeout: 200ms
scrape_configs:
  - job_name: flow-collector
    static_configs:
      - targets: ["host.docker.internal:%s"]
        labels:
          integration_run: %q
          process: collector
  - job_name: flow-worker
    static_configs:
      - targets: ["host.docker.internal:%s"]
        labels:
          integration_run: %q
          process: worker
`, collectorPort, runID, workerPort, runID)
	configPath := writeProductionFile(t, t.TempDir(), "promscrape.yml", []byte(config), 0o600)
	container := "watchdog-flow-vm-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	image := strings.TrimSpace(os.Getenv("WATCHDOG_VICTORIAMETRICS_IMAGE"))
	if image == "" {
		image = "victoriametrics/victoria-metrics:latest"
	}
	command := exec.Command(
		"docker", "run", "--detach", "--rm", "--name", container,
		"--add-host", "host.docker.internal:host-gateway",
		"-p", "127.0.0.1::8428",
		"-v", configPath+":/etc/victoria-metrics/promscrape.yml:ro",
		image, "-promscrape.config=/etc/victoria-metrics/promscrape.yml",
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("start scraping VictoriaMetrics: %v\n%s", err, output)
	}
	t.Cleanup(func() {
		cleanup := exec.Command("docker", "rm", "--force", container)
		if output, err := cleanup.CombinedOutput(); err != nil && !strings.Contains(string(output), "No such container") {
			t.Errorf("remove scraping VictoriaMetrics: %v\n%s", err, output)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		output, err := exec.CommandContext(ctx, "docker", "port", container, "8428/tcp").CombinedOutput()
		if err == nil {
			address := strings.TrimSpace(string(output))
			if strings.HasPrefix(address, "127.0.0.1:") {
				baseURL := "http://" + address
				request, _ := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
				response, requestErr := http.DefaultClient.Do(request)
				if requestErr == nil {
					_, _ = io.Copy(io.Discard, response.Body)
					closeErr := response.Body.Close()
					if response.StatusCode == http.StatusOK && closeErr == nil {
						return baseURL
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for scraping VictoriaMetrics: %v (docker port output=%q)", ctx.Err(), output)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func waitForProductionVMValue(t testing.TB, ctx context.Context, baseURL, runID, process, metric string, want float64) {
	t.Helper()
	query := fmt.Sprintf(`%s{integration_run=%q,process=%q}`, metric, runID, process)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		endpoint := baseURL + "/api/v1/query?" + url.Values{"query": []string{query}, "nocache": []string{"1"}}.Encode()
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			var result struct {
				Data struct {
					Result []struct {
						Value []json.RawMessage `json:"value"`
					} `json:"result"`
				} `json:"data"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&result)
			_ = response.Body.Close()
			if decodeErr == nil && len(result.Data.Result) == 1 && len(result.Data.Result[0].Value) == 2 {
				var value string
				if json.Unmarshal(result.Data.Result[0].Value[1], &value) == nil {
					got, parseErr := strconv.ParseFloat(value, 64)
					if parseErr == nil && got == want {
						return
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for VictoriaMetrics query %q: %v", query, ctx.Err())
		case <-ticker.C:
		}
	}
}

func productionJSON(t testing.TB, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func productionChecksum(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func writeProductionFile(t testing.TB, directory, name string, data []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func readProductionLog(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	return string(data)
}
