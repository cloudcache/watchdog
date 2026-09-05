// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
// two production commands. It owns an isolated Kafka topic and ClickHouse
// database and removes its uniquely labelled VictoriaMetrics series.
func TestProductionCollectorAndWorkerFourProtocolEndToEnd(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_PRODUCTION_PROCESS_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_PRODUCTION_PROCESS_INTEGRATION=1 to run")
	}
	t.Setenv("WATCHDOG_FLOW_CLICKHOUSE_DATA_INTEGRATION", "1")
	database := fmt.Sprintf("watchdog_flow_it_process_%d", time.Now().UnixNano())
	ctx, native := openDataIntegrationClickHouse(t, database)
	removeCorpusTTLs(t, ctx, native)

	brokers := corpusKafkaBrokers()
	topicBase := fmt.Sprintf("watchdog.flow.process.%d", time.Now().UnixNano())
	topic := fmt.Sprintf("%s-v%d", topicBase, flowstream.SchemaVersion)
	admin := newCorpusKafkaAdmin(t, ctx, brokers)
	createCorpusTopic(t, ctx, admin, topic, 4)
	t.Cleanup(func() { deleteCorpusTopic(t, admin, topic) })

	root := productionRepositoryRoot(t)
	artifacts := writeProductionBootstrap(t)
	collectorBinary := buildProductionCommand(t, root, "watchdog-flow-collect")
	workerBinary := buildProductionCommand(t, root, "watchdog-flow-worker")
	collectorMetrics := reserveTCPAddress(t)
	workerMetrics := reserveTCPAddress(t)
	sflowAddress := reserveUDPAddress(t)
	netflowAddress := reserveUDPAddress(t)
	passwordFile := os.Getenv("WATCHDOG_CLICKHOUSE_PASSWORD_FILE")
	if passwordFile == "" {
		t.Fatal("WATCHDOG_CLICKHOUSE_PASSWORD_FILE is required")
	}
	clickHouseAddress := os.Getenv("WATCHDOG_CLICKHOUSE_ADDRESS")
	if clickHouseAddress == "" {
		clickHouseAddress = "127.0.0.1:9000"
	}

	worker := startProductionProcess(t, root, workerBinary,
		"-bootstrap-plan", artifacts.plan,
		"-plan-public-key", artifacts.publicKey,
		"-bootstrap-version-publication", artifacts.publication,
		"-geo-bundle", artifacts.geo,
		"-kafka-brokers", strings.Join(brokers, ","),
		"-kafka-topic", topicBase,
		"-kafka-consumer-group", "watchdog-flow-process-"+strconv.FormatInt(time.Now().UnixNano(), 10),
		"-kafka-fetch-min-bytes", "1",
		"-kafka-fetch-max-wait", "100ms",
		"-kafka-template-replay-records", "64",
		"-clickhouse-address", clickHouseAddress,
		"-clickhouse-database", database,
		"-clickhouse-password-file", passwordFile,
		"-metrics-listen", workerMetrics,
	)
	waitForProcessMetric(t, ctx, worker, workerMetrics, "watchdog_flow_worker_kafka_records_total", 0)

	collector := startProductionProcess(t, root, collectorBinary,
		"-plan", artifacts.plan,
		"-plan-public-key", artifacts.publicKey,
		"-kafka-brokers", strings.Join(brokers, ","),
		"-kafka-topic", topicBase,
		"-sflow-listen", sflowAddress,
		"-netflow-listen", netflowAddress,
		"-metrics-listen", collectorMetrics,
		"-receive-buffer-bytes", strconv.Itoa(1<<20),
	)
	waitForProcessMetric(t, ctx, collector, collectorMetrics, "watchdog_flow_collector_kafka_records_total", 0)
	waitForUDPListeners(t, ctx, collector, sflowAddress, netflowAddress)

	sendUDPPayloads(t, netflowAddress,
		corpusFixturePayload(t, "netflow", "nfv5.pcap"),
		corpusFixturePayload(t, "netflow", "template.pcap"),
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

	vmURL := strings.TrimRight(strings.TrimSpace(os.Getenv("WATCHDOG_VICTORIAMETRICS_URL")), "/")
	if vmURL == "" {
		vmURL = "http://127.0.0.1:8428"
	}
	runID := "flow-process-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	t.Cleanup(func() { deleteProductionMetricSeries(t, vmURL, runID) })
	importProductionMetrics(t, ctx, vmURL, collectorMetrics, runID, "collector")
	importProductionMetrics(t, ctx, vmURL, workerMetrics, runID, "worker")
	waitForProductionVMValue(t, ctx, vmURL, runID, "collector", "watchdog_flow_collector_kafka_records_total", 6)
	waitForProductionVMValue(t, ctx, vmURL, runID, "worker", "watchdog_flow_worker_kafka_records_total", 6)

	collector.stop(t)
	worker.stop(t)
}

type productionBootstrap struct {
	plan, publicKey, publication, geo string
}

func writeProductionBootstrap(t testing.TB) productionBootstrap {
	t.Helper()
	directory := t.TempDir()
	effectiveFrom := time.Unix(0, 0).UTC()
	dimension := flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: "dimension-process", TenantID: corpusTenantID,
		Version: 1, EffectiveFrom: effectiveFrom,
		Prefixes: []flowdimension.PrefixDefinition{
			{ID: "all_ipv4", CIDR: "0.0.0.0/0", Labels: map[string]string{"flow": "local", "business": "process"}},
			{ID: "all_ipv6", CIDR: "::/0", Labels: map[string]string{"flow": "local", "business": "process"}},
		},
	}
	classification := flowdimension.ClassificationBundle{
		SchemaVersion: flowdimension.ClassificationSchemaVersion, TenantID: corpusTenantID, Version: 1,
		EffectiveFrom: effectiveFrom, DimensionSnapshotID: dimension.SnapshotID,
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	}
	dimensionData := productionJSON(t, dimension)
	classificationData := productionJSON(t, classification)
	dimensionPath := writeProductionFile(t, directory, "dimension.json", dimensionData, 0o600)
	classificationPath := writeProductionFile(t, directory, "classification.json", classificationData, 0o600)
	publication := flowworker.EnrichmentVersionPublication{
		PublicationID: "publication-process", TenantID: corpusTenantID, DimensionSnapshotID: dimension.SnapshotID,
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
			Protocol: protocol, SourcePrefix: "127.0.0.1/32", TenantID: corpusTenantID,
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
		PlanID: "plan-process", TenantID: corpusTenantID, CollectorID: plan.CollectorID,
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

func reserveTCPAddress(t testing.TB) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func reserveUDPAddress(t testing.TB) string {
	t.Helper()
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	address := listener.LocalAddr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
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
	remote, err := net.ResolveUDPAddr("udp4", address)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.DialUDP("udp4", nil, remote)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
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
			Body:       `SELECT uniqExact(flow_protocol), count() FROM flow_records FINAL WHERE tenant_id = {tenant:String}`,
			Parameters: ch.Parameters(map[string]any{"tenant": corpusTenantID}),
			Result:     proto.Results{{Name: "uniqExact(flow_protocol)", Data: &protocols}, {Name: "count()", Data: &records}},
		}
		err := native.executor.Do(ctx, factsQuery)
		if err == nil {
			receiptsQuery := ch.Query{
				Body:       `SELECT count() FROM flow_ingest_batches FINAL WHERE has(tenant_ids, {tenant:String})`,
				Parameters: ch.Parameters(map[string]any{"tenant": corpusTenantID}),
				Result:     proto.Results{{Name: "count()", Data: &receipts}},
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

func importProductionMetrics(t testing.TB, ctx context.Context, baseURL, address, runID, process string) {
	t.Helper()
	response, err := http.Get("http://" + address + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("read %s metrics: status=%s read=%v close=%v", process, response.Status, readErr, closeErr)
	}
	values := url.Values{"extra_label": []string{"integration_run=" + runID, "process=" + process}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/v1/import/prometheus?"+values.Encode(), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	closeErr = response.Body.Close()
	if response.StatusCode/100 != 2 || closeErr != nil {
		t.Fatalf("import %s metrics: status=%s close=%v", process, response.Status, closeErr)
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

func deleteProductionMetricSeries(t testing.TB, baseURL, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	matcher := fmt.Sprintf(`{integration_run=%q}`, runID)
	endpoint := baseURL + "/api/v1/admin/tsdb/delete_series?" + url.Values{"match[]": []string{matcher}}.Encode()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Errorf("delete VictoriaMetrics process series: %v", err)
		return
	}
	_, _ = io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if response.StatusCode/100 != 2 || closeErr != nil {
		t.Errorf("delete VictoriaMetrics process series: status=%s close=%v", response.Status, closeErr)
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
