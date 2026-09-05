// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowstream/flowpb"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/klauspost/compress/zstd"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const corpusTenantID = "flow-it-tenant"

// TestRealKafkaFourProtocolCorpusToClickHouseFiveLevelRollup owns an isolated
// Kafka topic and ClickHouse database. It is deliberately opt-in because it
// requires the external services from deploy/compose.flow-dev.yml.
func TestRealKafkaFourProtocolCorpusToClickHouseFiveLevelRollup(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_KAFKA_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_KAFKA_CLICKHOUSE_INTEGRATION=1 to run")
	}
	t.Setenv("WATCHDOG_FLOW_CLICKHOUSE_DATA_INTEGRATION", "1")
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_kafka_corpus")
	removeCorpusTTLs(t, ctx, native)

	enricher := newCorpusEnricher(t)
	writer, err := NewWriter(native, WriterConfig{RetryInitial: 5 * time.Millisecond, RetryMax: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	pipeline, err := NewPipeline(enricher, writer)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := flowworker.NewBatchProcessor(5*time.Minute, corpusBinding, pipeline.Handle, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()

	brokers := corpusKafkaBrokers()
	topicBase := fmt.Sprintf("watchdog.flow.corpus.%d", time.Now().UnixNano())
	topic := fmt.Sprintf("%s-v%d", topicBase, flowstream.SchemaVersion)
	admin := newCorpusKafkaAdmin(t, ctx, brokers)
	createCorpusTopic(t, ctx, admin, topic, 4)
	t.Cleanup(func() { deleteCorpusTopic(t, admin, topic) })

	producerConfig := flowstream.DefaultProducerConfig()
	producerConfig.Kafka = flowstream.KafkaConfig{Brokers: brokers, Topic: topicBase, ClientID: "watchdog-flow-corpus-producer"}
	producerConfig.QueueSize = 128
	producer, err := flowstream.NewProducer(producerConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = producer.Close(closeContext)
	})
	if err := producer.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	inlet, err := flowstream.NewInlet(producer)
	if err != nil {
		t.Fatal(err)
	}

	receivedAt := time.Now().UTC()
	datagrams := []flowstream.Datagram{
		corpusDatagram(t, receivedAt, "192.0.2.5:2055", flowpb.RawFlow_DECODER_NETFLOW, "netflow", "nfv5.pcap"),
		corpusDatagram(t, receivedAt.Add(time.Millisecond), "192.0.2.9:2055", flowpb.RawFlow_DECODER_NETFLOW, "netflow", "template.pcap"),
		corpusDatagram(t, receivedAt.Add(2*time.Millisecond), "192.0.2.9:2055", flowpb.RawFlow_DECODER_NETFLOW, "netflow", "data.pcap"),
		corpusDatagram(t, receivedAt.Add(3*time.Millisecond), "192.0.2.10:4739", flowpb.RawFlow_DECODER_NETFLOW, "netflow", "ipfixprobe-templates.pcap"),
		corpusDatagram(t, receivedAt.Add(4*time.Millisecond), "192.0.2.10:4739", flowpb.RawFlow_DECODER_NETFLOW, "netflow", "ipfixprobe-data.pcap"),
		corpusDatagram(t, receivedAt.Add(5*time.Millisecond), "192.0.2.50:6343", flowpb.RawFlow_DECODER_SFLOW, "sflow", "data-sflow-expanded-sample.pcap"),
	}
	produced := make(chan error, len(datagrams))
	for _, datagram := range datagrams {
		if err := inlet.Send(ctx, datagram, func(err error) { produced <- err }); err != nil {
			t.Fatal(err)
		}
	}
	if err := producer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for range datagrams {
		if err := <-produced; err != nil {
			t.Fatalf("produce corpus datagram: %v", err)
		}
	}
	producerStats := producer.Stats()
	if producerStats.Records != uint64(len(datagrams)) || producerStats.Errors != 0 || producerStats.BufferedRecords != 0 {
		t.Fatalf("producer stats=%+v", producerStats)
	}

	consumerConfig := flowstream.DefaultConsumerConfig()
	consumerConfig.Kafka = flowstream.KafkaConfig{Brokers: brokers, Topic: topicBase, ClientID: "watchdog-flow-corpus-consumer"}
	consumerConfig.ConsumerGroup = fmt.Sprintf("watchdog-flow-corpus-%d", time.Now().UnixNano())
	consumerConfig.FetchMinBytes = 1
	consumerConfig.FetchMaxWait = 100 * time.Millisecond
	consumerConfig.TemplateReplayRecords = 64
	consumer, err := flowstream.NewConsumer(consumerConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- consumer.RunPartitionBatches(runContext, processor.HandleRecords) }()
	waitForCorpusConsumption(t, consumer, uint64(len(datagrams)), runDone, cancelRun)

	facts := readAndAuditCorpusFacts(t, ctx, native, writer)
	assertCorpusRuntimeStats(t, producerStats, consumer.Stats(), processor.Stats(), pipeline.Stats(), writer.Stats(), facts)
	rollupAndAuditCorpus(t, ctx, native, facts)
}

func corpusKafkaBrokers() []string {
	value := strings.TrimSpace(os.Getenv("WATCHDOG_FLOW_KAFKA_BROKERS"))
	if value == "" {
		return []string{"127.0.0.1:9092"}
	}
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func newCorpusKafkaAdmin(t testing.TB, ctx context.Context, brokers []string) *kgo.Client {
	t.Helper()
	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ClientID("watchdog-flow-corpus-admin"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	return admin
}

func createCorpusTopic(t testing.TB, ctx context.Context, admin *kgo.Client, topic string, partitions int32) {
	t.Helper()
	request := kmsg.NewPtrCreateTopicsRequest()
	item := kmsg.NewCreateTopicsRequestTopic()
	item.Topic = topic
	item.NumPartitions = partitions
	item.ReplicationFactor = 1
	request.Topics = append(request.Topics, item)
	response, err := request.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("create Kafka topic %q: %v", topic, err)
	}
	if len(response.Topics) != 1 || response.Topics[0].ErrorCode != 0 {
		t.Fatalf("create Kafka topic %q response=%+v", topic, response.Topics)
	}
}

func deleteCorpusTopic(t testing.TB, admin *kgo.Client, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := kmsg.NewPtrDeleteTopicsRequest()
	request.TopicNames = []string{topic}
	item := kmsg.NewDeleteTopicsRequestTopic()
	item.Topic = kmsg.StringPtr(topic)
	request.Topics = append(request.Topics, item)
	response, err := request.RequestWith(ctx, admin)
	if err != nil {
		t.Errorf("delete Kafka topic %q: %v", topic, err)
		return
	}
	if len(response.Topics) != 1 || response.Topics[0].ErrorCode != 0 {
		t.Errorf("delete Kafka topic %q response=%+v", topic, response.Topics)
	}
}

func waitForCorpusConsumption(t testing.TB, consumer *flowstream.Consumer, want uint64, done <-chan error, cancel context.CancelFunc) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for consumer.Stats().Records < want {
		select {
		case err := <-done:
			cancel()
			t.Fatalf("Kafka consumer stopped after %d/%d records: %v", consumer.Stats().Records, want, err)
		case <-deadline.C:
			cancel()
			<-done
			t.Fatalf("Kafka consumer timed out after %d/%d records", consumer.Stats().Records, want)
		case <-ticker.C:
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("stop Kafka corpus consumer: %v", err)
	}
}

func corpusBinding(_ string, _ uint64, protocol flowplan.Protocol, source netip.Addr, _ uint64) (flowplan.SourceBinding, error) {
	bits := 128
	if source.Is4() {
		bits = 32
	}
	return flowplan.SourceBinding{
		Protocol: protocol, SourcePrefix: netip.PrefixFrom(source, bits).String(),
		TenantID: corpusTenantID, ExporterID: fmt.Sprintf("exporter-%d", protocol),
		TargetID: "target-corpus", DeviceID: "device-corpus", OwnershipEpoch: 1,
		SamplingMode: flowplan.SamplingModePreScaled, Enabled: true,
	}, nil
}

func corpusDatagram(t testing.TB, receivedAt time.Time, source string, decoder flowpb.RawFlow_Decoder, family, name string) flowstream.Datagram {
	t.Helper()
	return flowstream.Datagram{
		CollectorID: "collector-corpus", ListenerID: "listener-corpus", RegistryVersion: 1,
		ReceivedAt: receivedAt, Source: netip.MustParseAddrPort(source), Decoder: decoder,
		Payload: corpusFixturePayload(t, family, name),
	}
}

func newCorpusEnricher(t testing.TB) *flowworker.Enricher {
	t.Helper()
	effectiveFrom := time.Unix(0, 0).UTC()
	dimension, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: "dimension-corpus", TenantID: corpusTenantID,
		Version: 1, EffectiveFrom: effectiveFrom,
		Prefixes: []flowdimension.PrefixDefinition{
			{ID: "all_ipv4", CIDR: "0.0.0.0/0", Labels: map[string]string{"flow": "local", "business": "corpus"}},
			{ID: "all_ipv6", CIDR: "::/0", Labels: map[string]string{"flow": "local", "business": "corpus"}},
		},
	}, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	classification, err := flowdimension.CompileClassification(flowdimension.ClassificationDefinition{
		TenantID: corpusTenantID, Version: 1, EffectiveFrom: effectiveFrom, DimensionSnapshotID: "dimension-corpus",
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := flowworker.NewEnrichmentVersionCatalog(flowworker.EnrichmentVersion{Dimension: dimension, Classification: classification})
	if err != nil {
		t.Fatal(err)
	}
	geo := flowdimension.NewGeoCatalog()
	loadCorpusGeo(t, geo, effectiveFrom)
	enricher, err := flowworker.NewEnricherWithVersionCatalog(versions, geo, flowworker.EnrichmentLimits{})
	if err != nil {
		t.Fatal(err)
	}
	return enricher
}

func loadCorpusGeo(t testing.TB, catalog *flowdimension.GeoCatalog, effectiveFrom time.Time) {
	t.Helper()
	directory := t.TempDir()
	header := "ip_start,ip_end,country,admin_code,subdivision,city,isp_id,asn,geo_leaf_code"
	files := map[string][]byte{
		"ipv4.csv.zst": corpusZstd(t, []byte(header+"\n0.0.0.0,255.255.255.255,CN,330100,Zhejiang,Hangzhou,3,64500,330100\n")),
		"ipv6.csv.zst": corpusZstd(t, []byte(header+"\n::,ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff,CN,330100,Zhejiang,Hangzhou,3,64500,330100\n")),
		"operators.json": corpusJSON(t, []flowdimension.GeoOperator{
			{ID: 3, Name: "Corpus Carrier", ShortName: "Corpus", Category: "carrier", Enabled: true},
		}),
		"geo_dict.json": corpusJSON(t, []flowdimension.GeoDictionaryEntry{
			{Kind: "continent", Code: "Asia", Name: "Asia", Enabled: true},
			{Kind: "region", Code: "EastAsia", Name: "East Asia", ParentCode: "Asia", Enabled: true},
			{Kind: "country", Code: "CN", Name: "China", ParentCode: "EastAsia", Enabled: true},
			{Kind: "province", Code: "330000", Name: "Zhejiang", ParentCode: "CN", Enabled: true},
			{Kind: "city", Code: "330100", Name: "Hangzhou", ParentCode: "330000", Enabled: true},
		}),
	}
	manifest := flowdimension.GeoManifest{
		Schema: flowdimension.GeoSchemaV2, Version: "geo-corpus", GeneratedAt: effectiveFrom, EffectiveFrom: effectiveFrom,
		AdminCodeSystem: flowdimension.GeoAdminCodeSystem, UnknownCountry: flowdimension.GeoUnknownCountry,
		Files: make(map[string]flowdimension.GeoFileSpec, len(files)),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		rows := uint64(0)
		switch name {
		case "ipv4.csv.zst", "ipv6.csv.zst":
			rows = 1
		case "operators.json":
			rows = 1
		case "geo_dict.json":
			rows = 5
		}
		manifest.Files[name] = flowdimension.GeoFileSpec{SHA256: hex.EncodeToString(digest[:]), Rows: rows}
	}
	if err := os.WriteFile(filepath.Join(directory, "manifest.json"), corpusJSON(t, manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if changed, err := catalog.Reload(directory, flowdimension.GeoLoadLimits{}); err != nil || !changed {
		t.Fatalf("load corpus Geo bundle changed=%t: %v", changed, err)
	}
}

func corpusZstd(t testing.TB, data []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	encoder, err := zstd.NewWriter(&buffer, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func corpusJSON(t testing.TB, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type corpusFacts struct {
	byBatch        map[[32]byte][]RecordRef
	records        int
	rawBytes       uint64
	rawPackets     uint64
	estimatedBytes uint64
	estimatedPkts  uint64
	validEstimated uint64
	buckets        map[time.Time]struct{}
	protocols      map[uint8]struct{}
}

func readAndAuditCorpusFacts(t testing.TB, ctx context.Context, native *NativeInserter, writer *Writer) corpusFacts {
	t.Helper()
	var (
		batchIDs, recordIDs                   proto.ColFixedStr32
		eventTimes                            = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
		kafkaOffsets                          proto.ColUInt64
		recordIndexes, classificationVersions proto.ColUInt32
		flowProtocols                         proto.ColUInt8
		rawBytes, rawPackets                  proto.ColUInt64
		estimatedBytes, estimatedPackets      proto.ColUInt64
		qualityFlags, dimensionFingerprints   proto.ColUInt64
		estimatedValid                        proto.ColBool
		continents                            = new(proto.ColStr).LowCardinality()
		regions                               = new(proto.ColStr).LowCardinality()
		countries                             = new(proto.ColStr).LowCardinality()
		provinces                             = new(proto.ColStr).LowCardinality()
		cities                                = new(proto.ColStr).LowCardinality()
		dispositions                          proto.ColStr
	)
	result := corpusFacts{byBatch: make(map[[32]byte][]RecordRef), buckets: make(map[time.Time]struct{}), protocols: make(map[uint8]struct{})}
	seenRecords := make(map[[32]byte]struct{})
	query := ch.Query{
		Body: `SELECT ingest_batch_id, record_id, event_time, kafka_offset, record_index, flow_protocol,
       raw_bytes, raw_packets, estimated_bytes, estimated_packets, quality_flags,
       dimension_fingerprint, classification_version, estimated_valid,
       toString(remote_geo_continent_id) AS continent, toString(remote_geo_region_id) AS region,
       toString(remote_geo_country_id) AS country, toString(remote_geo_province_id) AS province,
       toString(remote_geo_city_id) AS city, toString(disposition) AS disposition
FROM flow_records FINAL
WHERE tenant_id = {tenant:String}
ORDER BY ingest_batch_id, kafka_offset, record_index`,
		Parameters: ch.Parameters(map[string]any{"tenant": corpusTenantID}),
		Result: proto.Results{
			{Name: "ingest_batch_id", Data: &batchIDs}, {Name: "record_id", Data: &recordIDs}, {Name: "event_time", Data: eventTimes},
			{Name: "kafka_offset", Data: &kafkaOffsets}, {Name: "record_index", Data: &recordIndexes}, {Name: "flow_protocol", Data: &flowProtocols},
			{Name: "raw_bytes", Data: &rawBytes}, {Name: "raw_packets", Data: &rawPackets}, {Name: "estimated_bytes", Data: &estimatedBytes},
			{Name: "estimated_packets", Data: &estimatedPackets}, {Name: "quality_flags", Data: &qualityFlags},
			{Name: "dimension_fingerprint", Data: &dimensionFingerprints}, {Name: "classification_version", Data: &classificationVersions},
			{Name: "estimated_valid", Data: &estimatedValid}, {Name: "continent", Data: continents}, {Name: "region", Data: regions},
			{Name: "country", Data: countries}, {Name: "province", Data: provinces}, {Name: "city", Data: cities},
			{Name: "disposition", Data: &dispositions},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		columns := []int{batchIDs.Rows(), recordIDs.Rows(), eventTimes.Rows(), kafkaOffsets.Rows(), recordIndexes.Rows(), flowProtocols.Rows(), rawBytes.Rows(), rawPackets.Rows(), estimatedBytes.Rows(), estimatedPackets.Rows(), qualityFlags.Rows(), dimensionFingerprints.Rows(), classificationVersions.Rows(), estimatedValid.Rows(), continents.Rows(), regions.Rows(), countries.Rows(), provinces.Rows(), cities.Rows(), dispositions.Rows()}
		for _, rows := range columns {
			if rows != block.Rows {
				return fmt.Errorf("corpus fact column has %d rows, want %d", rows, block.Rows)
			}
		}
		for index := 0; index < block.Rows; index++ {
			if _, exists := seenRecords[recordIDs[index]]; exists {
				return fmt.Errorf("duplicate corpus record id %x", recordIDs[index][:8])
			}
			seenRecords[recordIDs[index]] = struct{}{}
			if continents.Row(index) != "Asia" || regions.Row(index) != "EastAsia" || countries.Row(index) != "CN" || provinces.Row(index) != "330000" || cities.Row(index) != "330100" || dispositions.Row(index) != "count" {
				return fmt.Errorf("corpus fact has incomplete hierarchy or disposition at offset %d index %d", kafkaOffsets[index], recordIndexes[index])
			}
			record := &flowworker.EnrichedRecord{
				SourceRecordID: recordIDs[index], RawBytes: rawBytes[index], RawPackets: rawPackets[index],
				EstimatedBytes: estimatedBytes[index], EstimatedPackets: estimatedPackets[index],
				QualityFlags: qualityFlags[index], DimensionFingerprint: dimensionFingerprints[index],
				ClassificationVersion: classificationVersions[index], EstimatedValid: estimatedValid[index],
			}
			result.byBatch[batchIDs[index]] = append(result.byBatch[batchIDs[index]], RecordRef{Record: record})
			result.records++
			result.rawBytes += rawBytes[index]
			result.rawPackets += rawPackets[index]
			result.estimatedBytes += estimatedBytes[index]
			result.estimatedPkts += estimatedPackets[index]
			if estimatedValid[index] {
				result.validEstimated++
			}
			result.protocols[flowProtocols[index]] = struct{}{}
			result.buckets[eventTimes.Row(index).UTC().Truncate(time.Minute)] = struct{}{}
		}
		return nil
	}
	if err := native.executor.Do(ctx, query); err != nil {
		t.Fatalf("read corpus facts: %v", err)
	}
	if result.records == 0 || result.rawBytes == 0 || result.rawPackets == 0 {
		t.Fatalf("empty corpus facts=%+v", result)
	}
	auditCorpusReceipts(t, ctx, native, result, writer)
	return result
}

func auditCorpusReceipts(t testing.TB, ctx context.Context, native *NativeInserter, facts corpusFacts, writer *Writer) {
	t.Helper()
	var (
		batchIDs, checksums                              proto.ColFixedStr32
		workerSchemas                                    proto.ColUInt32
		receiptSchemas                                   proto.ColUInt16
		recordCounts, rawBytes, rawPackets               proto.ColUInt64
		estimatedBytes, estimatedPackets, estimatedValid proto.ColUInt64
	)
	seen := make(map[[32]byte]struct{})
	query := ch.Query{
		Body: `SELECT ingest_batch_id, checksum, worker_schema, receipt_schema, record_count,
       raw_bytes, raw_packets, estimated_bytes, estimated_packets, estimated_valid_records
FROM flow_ingest_batches FINAL
WHERE has(tenant_ids, {tenant:String})
ORDER BY ingest_batch_id`,
		Parameters: ch.Parameters(map[string]any{"tenant": corpusTenantID}),
		Result: proto.Results{
			{Name: "ingest_batch_id", Data: &batchIDs}, {Name: "checksum", Data: &checksums},
			{Name: "worker_schema", Data: &workerSchemas}, {Name: "receipt_schema", Data: &receiptSchemas},
			{Name: "record_count", Data: &recordCounts}, {Name: "raw_bytes", Data: &rawBytes}, {Name: "raw_packets", Data: &rawPackets},
			{Name: "estimated_bytes", Data: &estimatedBytes}, {Name: "estimated_packets", Data: &estimatedPackets},
			{Name: "estimated_valid_records", Data: &estimatedValid},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for index := 0; index < block.Rows; index++ {
			id := batchIDs[index]
			if _, exists := seen[id]; exists {
				return fmt.Errorf("duplicate corpus receipt %x", id[:8])
			}
			seen[id] = struct{}{}
			records, exists := facts.byBatch[id]
			if !exists {
				return fmt.Errorf("receipt %x has no facts", id[:8])
			}
			var wantRawBytes, wantRawPackets, wantEstimatedBytes, wantEstimatedPackets, wantValid uint64
			for _, ref := range records {
				wantRawBytes += ref.Record.RawBytes
				wantRawPackets += ref.Record.RawPackets
				if ref.Record.EstimatedValid {
					wantEstimatedBytes += ref.Record.EstimatedBytes
					wantEstimatedPackets += ref.Record.EstimatedPackets
					wantValid++
				}
			}
			if workerSchemas[index] != WorkerSchemaVersion || receiptSchemas[index] != receiptSchemaVersion ||
				recordCounts[index] != uint64(len(records)) || rawBytes[index] != wantRawBytes || rawPackets[index] != wantRawPackets ||
				estimatedBytes[index] != wantEstimatedBytes || estimatedPackets[index] != wantEstimatedPackets || estimatedValid[index] != wantValid {
				return fmt.Errorf("receipt %x counters do not match %d facts", id[:8], len(records))
			}
			if want := blockChecksum(records); checksums[index] != want {
				return fmt.Errorf("receipt %x checksum=%x want=%x", id[:8], checksums[index][:8], want[:8])
			}
		}
		return nil
	}
	if err := native.executor.Do(ctx, query); err != nil {
		t.Fatalf("audit corpus receipts: %v", err)
	}
	if len(seen) != len(facts.byBatch) || uint64(len(seen)) != writer.Stats().Blocks {
		t.Fatalf("receipts=%d fact_batches=%d writer_blocks=%d", len(seen), len(facts.byBatch), writer.Stats().Blocks)
	}
}

func assertCorpusRuntimeStats(t testing.TB, producer flowstream.ProducerStats, consumer flowstream.ConsumerStats, processor flowworker.ProcessorStats, pipeline PipelineStats, writer WriterStats, facts corpusFacts) {
	t.Helper()
	if producer.Records != 6 || producer.Errors != 0 || producer.BufferedRecords != 0 || consumer.Records != 6 || consumer.Errors != 0 {
		t.Fatalf("Kafka runtime producer=%+v consumer=%+v", producer, consumer)
	}
	if processor.Datagrams != 6 || processor.Records != uint64(facts.records) || processor.TemplateMissing != 0 || processor.Rejected != 0 || processor.RetryableErrors != 0 {
		t.Fatalf("processor stats=%+v facts=%d", processor, facts.records)
	}
	if pipeline.Records != uint64(facts.records) || pipeline.SnapshotMiss != 0 || writer.Rows != uint64(facts.records) || writer.InsertErrors != 0 {
		t.Fatalf("pipeline=%+v writer=%+v facts=%d", pipeline, writer, facts.records)
	}
	for _, protocol := range []uint8{uint8(flowplan.ProtocolSFlow5), uint8(flowplan.ProtocolNetFlow5), uint8(flowplan.ProtocolNetFlow9), uint8(flowplan.ProtocolIPFIX)} {
		if _, exists := facts.protocols[protocol]; !exists {
			t.Fatalf("protocol %d has no durable ClickHouse fact; got=%v", protocol, facts.protocols)
		}
	}
}

func rollupAndAuditCorpus(t testing.TB, ctx context.Context, native *NativeInserter, facts corpusFacts) {
	t.Helper()
	runner, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	for bucket := range facts.buckets {
		request := RollupRequest{
			TenantID: corpusTenantID, Resolution: RollupOneMinute, Bucket: bucket,
			Generation: 1, GeneratedAt: bucket.Add(2 * time.Minute),
		}
		if err := runner.Run(ctx, request); err != nil {
			t.Fatalf("roll up corpus bucket %s: %v", bucket, err)
		}
	}
	var (
		kinds             = new(proto.ColStr).LowCardinality()
		values            proto.ColStr
		rawBytes, records proto.ColUInt64
		distinctValues    proto.ColUInt64
	)
	got := make(map[string]struct {
		value             string
		rawBytes, records uint64
		distinctValues    uint64
	})
	query := ch.Query{
		Body: `SELECT toString(dimension_kind) AS kind, any(dimension_value) AS value,
       sum(raw_bytes) AS raw_bytes, sum(received_records) AS records,
       uniqExact(dimension_value) AS distinct_values
FROM flow_aggregate_1m FINAL
WHERE tenant_id = {tenant:String}
  AND dimension_kind IN ('geo.continent', 'geo.region', 'geo.country', 'geo.province', 'geo.city')
GROUP BY dimension_kind
ORDER BY dimension_kind`,
		Parameters: ch.Parameters(map[string]any{"tenant": corpusTenantID}),
		Result: proto.Results{
			{Name: "kind", Data: kinds}, {Name: "value", Data: &values}, {Name: "raw_bytes", Data: &rawBytes},
			{Name: "records", Data: &records}, {Name: "distinct_values", Data: &distinctValues},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for index := 0; index < block.Rows; index++ {
			got[kinds.Row(index)] = struct {
				value             string
				rawBytes, records uint64
				distinctValues    uint64
			}{values.Row(index), rawBytes[index], records[index], distinctValues[index]}
		}
		return nil
	}
	if err := native.executor.Do(ctx, query); err != nil {
		t.Fatalf("read corpus Geo rollups: %v", err)
	}
	wantValues := map[string]string{
		"geo.continent": "Asia", "geo.region": "EastAsia", "geo.country": "CN",
		"geo.province": "330000", "geo.city": "330100",
	}
	if len(got) != len(wantValues) {
		t.Fatalf("Geo rollup levels=%v", got)
	}
	for kind, wantValue := range wantValues {
		row, exists := got[kind]
		if !exists || row.value != wantValue || row.rawBytes != facts.rawBytes || row.records != uint64(facts.records) || row.distinctValues != 1 {
			t.Fatalf("Geo rollup %s=%+v, want value=%s raw=%d records=%d", kind, row, wantValue, facts.rawBytes, facts.records)
		}
	}
}

func removeCorpusTTLs(t testing.TB, ctx context.Context, native *NativeInserter) {
	t.Helper()
	for _, table := range []string{"flow_records", "flow_aggregate_1m"} {
		if err := native.executor.Do(ctx, ch.Query{Body: "ALTER TABLE " + table + " REMOVE TTL"}); err != nil {
			t.Fatalf("remove isolated test TTL from %s: %v", table, err)
		}
	}
}

func corpusFixturePayload(t testing.TB, family, name string) []byte {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve corpus test path")
	}
	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "akvorado", "outlet", "flow", "decoder", family, "testdata", name)
	payloads := corpusPCAPUDPPayloads(t, path)
	if len(payloads) != 1 {
		t.Fatalf("%s contains %d packets, want 1", path, len(payloads))
	}
	return payloads[0]
}

func corpusPCAPUDPPayloads(t testing.TB, path string) [][]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 24 {
		t.Fatal("pcap global header is truncated")
	}
	var order binary.ByteOrder
	switch string(data[:4]) {
	case "\xd4\xc3\xb2\xa1", "\x4d\x3c\xb2\xa1":
		order = binary.LittleEndian
	case "\xa1\xb2\xc3\xd4", "\xa1\xb2\x3c\x4d":
		order = binary.BigEndian
	default:
		t.Fatal("unsupported pcap byte order")
	}
	var payloads [][]byte
	for offset := 24; offset < len(data); {
		if len(data)-offset < 16 {
			t.Fatal("pcap packet header is truncated")
		}
		captured := int(order.Uint32(data[offset+8 : offset+12]))
		offset += 16
		if captured > len(data)-offset {
			t.Fatal("pcap packet is truncated")
		}
		frame := data[offset : offset+captured]
		offset += captured
		payloads = append(payloads, corpusUDPPayload(t, frame))
	}
	return payloads
}

func corpusUDPPayload(t testing.TB, frame []byte) []byte {
	t.Helper()
	if len(frame) < 14 {
		t.Fatal("Ethernet frame is truncated")
	}
	offset := 14
	etherType := binary.BigEndian.Uint16(frame[12:14])
	for etherType == 0x8100 || etherType == 0x88a8 {
		if len(frame) < offset+4 {
			t.Fatal("VLAN frame is truncated")
		}
		etherType = binary.BigEndian.Uint16(frame[offset+2 : offset+4])
		offset += 4
	}
	if etherType != 0x0800 || len(frame) < offset+20 {
		t.Fatal("fixture is not Ethernet/IPv4")
	}
	headerLength := int(frame[offset]&0x0f) * 4
	if headerLength < 20 || len(frame) < offset+headerLength+8 || frame[offset+9] != 17 {
		t.Fatal("fixture is not a complete IPv4/UDP packet")
	}
	udpOffset := offset + headerLength
	udpLength := int(binary.BigEndian.Uint16(frame[udpOffset+4 : udpOffset+6]))
	if udpLength < 8 || udpOffset+udpLength > len(frame) {
		t.Fatal("UDP payload is truncated")
	}
	return append([]byte(nil), frame[udpOffset+8:udpOffset+udpLength]...)
}
