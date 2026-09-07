// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/netsampler/goflow2/v3/decoders/netflowlegacy"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestRealKafkaTemplateReplaySurvivesDurableFailureAndWorkerRestart verifies
// the production consumer-group handoff contract against a real broker. A
// fresh worker must rebuild NetFlow v9 and IPFIX templates from the bounded
// assignment replay without moving the committed offset backwards.
func TestRealKafkaTemplateReplaySurvivesDurableFailureAndWorkerRestart(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_KAFKA_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_KAFKA_CLICKHOUSE_INTEGRATION=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	brokers := corpusKafkaBrokers()
	topicBase := fmt.Sprintf("watchdog.flow.replay.%d", time.Now().UnixNano())
	topic := fmt.Sprintf("%s-v%d", topicBase, flowstream.SchemaVersion)
	group := fmt.Sprintf("watchdog-flow-replay-%d", time.Now().UnixNano())
	admin := newCorpusKafkaAdmin(t, ctx, brokers)
	createCorpusTopic(t, ctx, admin, topic, 1)
	t.Cleanup(func() { deleteCorpusTopic(t, admin, topic) })

	templateAndData := [][]byte{
		corpusFixturePayload(t, "netflow", "template.pcap"),
		corpusFixturePayload(t, "netflow", "data.pcap"),
		corpusFixturePayload(t, "netflow", "ipfixprobe-templates.pcap"),
		corpusFixturePayload(t, "netflow", "ipfixprobe-data.pcap"),
	}
	publishReplayDatagrams(t, ctx, brokers, topicBase, templateAndData)

	firstOffsets := consumeReplayUntil(t, ctx, brokers, topicBase, group, func(batches []*flowworker.RecordBatch) error {
		return nil
	}, 4)
	if got := committedReplayOffset(t, ctx, admin, group, topic); got != 4 {
		t.Fatalf("initial committed offset=%d, want 4", got)
	}
	if want := []int64{1, 3}; !equalReplayOffsets(firstOffsets, want) {
		t.Fatalf("initial decoded data offsets=%v, want %v", firstOffsets, want)
	}

	publishReplayDatagrams(t, ctx, brokers, topicBase, [][]byte{templateAndData[1], templateAndData[3]})
	wantFailure := errors.New("injected durable sink failure")
	failedOffsets, failedConsumer, failedProcessor, runErr := consumeReplayWithFailure(
		t, ctx, brokers, topicBase, group, 4, wantFailure,
	)
	if !errors.Is(runErr, wantFailure) {
		t.Fatalf("failed worker error=%v, want %v", runErr, wantFailure)
	}
	if got := committedReplayOffset(t, ctx, admin, group, topic); got != 4 {
		t.Fatalf("committed offset advanced across durable failure: got %d want 4", got)
	}
	if !containsReplayOffset(failedOffsets, 4) {
		t.Fatalf("fresh failed worker never decoded post-commit data; offsets=%v", failedOffsets)
	}
	if failedProcessor.TemplateMissing != 0 || failedProcessor.RetryableErrors != 1 || failedConsumer.Errors != 1 || failedConsumer.Rebalances == 0 {
		t.Fatalf("failed worker stats consumer=%+v processor=%+v", failedConsumer, failedProcessor)
	}

	recoveredOffsets := consumeReplayUntil(t, ctx, brokers, topicBase, group, func(batches []*flowworker.RecordBatch) error {
		return nil
	}, 3)
	if got := committedReplayOffset(t, ctx, admin, group, topic); got != 6 {
		t.Fatalf("recovered committed offset=%d, want 6", got)
	}
	if want := []int64{1, 3, 4, 5}; !equalReplayOffsets(recoveredOffsets, want) {
		t.Fatalf("recovered decoded data offsets=%v, want replay+new %v", recoveredOffsets, want)
	}
}

// TestRealKafkaEventTimeVersionBlockDoesNotCommit proves the complete replay
// boundary: a valid flow older than the first installed publication must not
// use that newer "current" publication, reach ClickHouse, or advance Kafka.
func TestRealKafkaEventTimeVersionBlockDoesNotCommit(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_KAFKA_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_KAFKA_CLICKHOUSE_INTEGRATION=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	brokers := corpusKafkaBrokers()
	topicBase := fmt.Sprintf("watchdog.flow.version-block.%d", time.Now().UnixNano())
	topic := fmt.Sprintf("%s-v%d", topicBase, flowstream.SchemaVersion)
	group := fmt.Sprintf("watchdog-flow-version-block-%d", time.Now().UnixNano())
	admin := newCorpusKafkaAdmin(t, ctx, brokers)
	createCorpusTopic(t, ctx, admin, topic, 1)
	t.Cleanup(func() { deleteCorpusTopic(t, admin, topic) })

	eventTime := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	packet := netflowlegacy.PacketNetFlowV5{
		Version: 5, UnixSecs: uint32(eventTime.Unix()), SamplingInterval: 1,
		Records: []netflowlegacy.RecordsNetFlowV5{{
			SrcAddr: 0x0a000001, DstAddr: 0xcb007102, Input: 3, Output: 4,
			DPkts: 2, DOctets: 100, SrcPort: 12345, DstPort: 443, Proto: 6,
		}},
	}
	payload, err := packet.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	publishReplayDatagrams(t, ctx, brokers, topicBase, [][]byte{payload})

	effectiveFrom := eventTime.Add(time.Hour)
	dimension, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion,
		SnapshotID:    "dimension-current",
		TenantID:      corpusTenantID,
		Version:       1,
		EffectiveFrom: effectiveFrom,
		Prefixes: []flowdimension.PrefixDefinition{
			{ID: "local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local"}},
		},
	}, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	classification, err := flowdimension.CompileClassification(flowdimension.ClassificationDefinition{
		TenantID: corpusTenantID, Version: 1, EffectiveFrom: effectiveFrom,
		DimensionSnapshotID: "dimension-current",
		InternalPolicy:      flowdimension.RecordPolicyCount,
		TransitPolicy:       flowdimension.RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := flowworker.NewEnrichmentVersionCatalog(flowworker.EnrichmentVersion{
		Dimension: dimension, Classification: classification,
	})
	if err != nil {
		t.Fatal(err)
	}
	enricher, err := flowworker.NewEnricherWithVersionCatalog(versions, nil, flowworker.EnrichmentLimits{})
	if err != nil {
		t.Fatal(err)
	}
	writer := &recordingBatchWriter{}
	pipeline := &Pipeline{enricher: enricher, writer: writer}
	consumer, processor := newReplayConsumer(t, brokers, topicBase, group, func(batches []*flowworker.RecordBatch) error {
		return pipeline.Handle(ctx, batches)
	})
	defer processor.Close()

	runErr := consumer.RunPartitionBatches(ctx, processor.HandleRecords)
	var blocked *flowworker.VersionBlockedError
	if !errors.Is(runErr, flowworker.ErrVersionUnavailable) || !errors.As(runErr, &blocked) {
		t.Fatalf("consumer error=%v, want version block", runErr)
	}
	if blocked.Dependency != "dimension_classification_pair" || blocked.TenantID != corpusTenantID || !blocked.EventTime.Before(effectiveFrom) {
		t.Fatalf("blocked metadata=%+v effective_from=%s", blocked, effectiveFrom)
	}
	if len(writer.batches) != 0 {
		t.Fatalf("version-blocked flow reached ClickHouse writer: %d batches", len(writer.batches))
	}
	if stats := consumer.Stats(); stats.Records != 0 || stats.Errors != 1 {
		t.Fatalf("consumer stats=%+v", stats)
	}
	if stats := processor.Stats(); stats.RetryableErrors != 1 || stats.Records != 0 {
		t.Fatalf("processor stats=%+v", stats)
	}
	if stats := pipeline.Stats(); stats.SnapshotMiss != 1 || stats.Records != 0 {
		t.Fatalf("pipeline stats=%+v", stats)
	}
	if got := committedReplayOffset(t, ctx, admin, group, topic); got != -1 {
		t.Fatalf("committed offset advanced across version block: got %d want -1", got)
	}
}

func publishReplayDatagrams(t testing.TB, ctx context.Context, brokers []string, topicBase string, payloads [][]byte) {
	t.Helper()
	config := flowstream.DefaultProducerConfig()
	config.Kafka = flowstream.KafkaConfig{Brokers: brokers, Topic: topicBase, ClientID: "watchdog-flow-replay-producer"}
	config.QueueSize = 32
	producer, err := flowstream.NewProducer(config)
	if err != nil {
		t.Fatal(err)
	}
	inlet, err := flowstream.NewInlet(producer)
	if err != nil {
		t.Fatal(err)
	}
	source := netip.MustParseAddrPort("127.0.0.1:2055")
	for _, payload := range payloads {
		decoder, err := flowstream.InspectDecoder(payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := inlet.Send(ctx, flowstream.Datagram{
			CollectorID: "collector-replay", ListenerID: "listener-replay", RegistryVersion: 1,
			ReceivedAt: time.Now().UTC(), Source: source, Decoder: decoder, Payload: payload,
		}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := producer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if stats := producer.Stats(); stats.Records != uint64(len(payloads)) || stats.Errors != 0 || stats.BufferedRecords != 0 {
		t.Fatalf("replay producer stats=%+v", stats)
	}
}

func consumeReplayUntil(
	t testing.TB,
	ctx context.Context,
	brokers []string,
	topicBase string,
	group string,
	handle func([]*flowworker.RecordBatch) error,
	wantCommittedRecords uint64,
) []int64 {
	t.Helper()
	var (
		mu      sync.Mutex
		offsets []int64
	)
	consumer, processor := newReplayConsumer(t, brokers, topicBase, group, func(batches []*flowworker.RecordBatch) error {
		mu.Lock()
		for _, batch := range batches {
			offsets = append(offsets, batch.KafkaOffset)
		}
		mu.Unlock()
		return handle(batches)
	})
	defer processor.Close()

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.RunPartitionBatches(runCtx, processor.HandleRecords) }()
	waitForReplayRecords(t, consumer, wantCommittedRecords, done, cancel)
	stats := processor.Stats()
	if stats.TemplateMissing != 0 || stats.Rejected != 0 || stats.RetryableErrors != 0 {
		t.Fatalf("replay processor stats=%+v", stats)
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]int64(nil), offsets...)
}

func consumeReplayWithFailure(
	t testing.TB,
	ctx context.Context,
	brokers []string,
	topicBase string,
	group string,
	failAtOffset int64,
	wantFailure error,
) ([]int64, flowstream.ConsumerStats, flowworker.ProcessorStats, error) {
	t.Helper()
	var offsets []int64
	consumer, processor := newReplayConsumer(t, brokers, topicBase, group, func(batches []*flowworker.RecordBatch) error {
		for _, batch := range batches {
			offsets = append(offsets, batch.KafkaOffset)
			if batch.KafkaOffset >= failAtOffset {
				return wantFailure
			}
		}
		return nil
	})
	defer processor.Close()
	err := consumer.RunPartitionBatches(ctx, processor.HandleRecords)
	return offsets, consumer.Stats(), processor.Stats(), err
}

func newReplayConsumer(
	t testing.TB,
	brokers []string,
	topicBase string,
	group string,
	handle func([]*flowworker.RecordBatch) error,
) (*flowstream.Consumer, *flowworker.Processor) {
	t.Helper()
	processor, err := flowworker.NewBatchProcessor(5*time.Minute, corpusBinding, func(_ context.Context, batches []*flowworker.RecordBatch) error {
		return handle(batches)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	config := flowstream.DefaultConsumerConfig()
	config.Kafka = flowstream.KafkaConfig{Brokers: brokers, Topic: topicBase, ClientID: "watchdog-flow-replay-consumer"}
	config.ConsumerGroup = group
	config.FetchMinBytes = 1
	config.FetchMaxWait = 100 * time.Millisecond
	config.TemplateReplayRecords = 4
	consumer, err := flowstream.NewConsumer(config, nil)
	if err != nil {
		processor.Close()
		t.Fatal(err)
	}
	return consumer, processor
}

func waitForReplayRecords(t testing.TB, consumer *flowstream.Consumer, want uint64, done <-chan error, cancel context.CancelFunc) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for consumer.Stats().Records < want {
		select {
		case err := <-done:
			cancel()
			t.Fatalf("replay consumer stopped after %d/%d records: %v", consumer.Stats().Records, want, err)
		case <-deadline.C:
			cancel()
			<-done
			t.Fatalf("replay consumer timed out after %d/%d records", consumer.Stats().Records, want)
		case <-ticker.C:
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("stop replay consumer: %v", err)
	}
}

func committedReplayOffset(t testing.TB, ctx context.Context, admin kmsg.Requestor, group, topic string) int64 {
	t.Helper()
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Version = 7
	request.Group = group
	request.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{0}}}
	response, err := request.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("fetch committed Kafka offset: %v", err)
	}
	if response.ErrorCode != 0 || len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 1 || response.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("fetch committed Kafka offset response=%+v", response)
	}
	return response.Topics[0].Partitions[0].Offset
}

func equalReplayOffsets(got, want []int64) bool {
	got = append([]int64(nil), got...)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if len(got) != len(want) {
		return false
	}
	for index := range want {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func containsReplayOffset(offsets []int64, want int64) bool {
	for _, offset := range offsets {
		if offset == want {
			return true
		}
	}
	return false
}
