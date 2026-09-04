package flowcollect

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"github.com/netsampler/goflow2/v3/decoders/netflow"
	"google.golang.org/protobuf/proto"
)

func TestKafkaCollectStateReaderFreezesBoundaryAndCoalesces(t *testing.T) {
	decoder, record, decoded := collectStateFixture(t)
	state, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 2}, "collector-a", decoder)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	newer := cloneCollectStateGeneration(t, state, state.StateGeneration+1)
	postBoundary := cloneCollectStateGeneration(t, state, state.StateGeneration+2)
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{
		0: {
			marshalCollectStateMessage(t, "state", 0, 0, state),
			marshalCollectStateMessage(t, "state", 0, 1, newer),
			marshalCollectStateMessage(t, "state", 0, 2, postBoundary),
		},
	}, map[int32][2]int64{0: {0, 2}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 10, source: source}
	records, err := reader.Read(context.Background(), collectStateRegistry(t, "collector-a", 2, record.Source.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Offset != 1 || records[0].State.StateGeneration != newer.StateGeneration {
		t.Fatalf("unexpected frozen snapshot: %+v", records)
	}
	if got := source.eventsSnapshot(); len(got) != 4 || got[0] != "partitions" || got[1] != "oldest/0" || got[2] != "newest/0" || got[3] != "consume/0/0" {
		t.Fatalf("high watermark was not frozen before consume: %v", got)
	}
}

func TestKafkaCollectStateReaderDispatchesDecoderAndQualityKeyspaces(t *testing.T) {
	now := time.Now()
	decoder, record, decoded := collectStateFixture(t)
	state, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 2}, "collector-a", decoder)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := qualityCheckpointFixture(t, "collector-a", 2, 1, 1, now.Add(-time.Second))
	plan := validPlan(now)
	plan.SchemaVersion = 2
	plan.CollectorID = "collector-a"
	plan.Revision = 1
	plan.Sources = []SourceBinding{
		{Protocol: ProtocolNetFlow9, SourcePrefix: record.Source.Addr().String() + "/32", ObservationDomainID: uint64Pointer(42), TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", OwnershipEpoch: 2, SamplingMode: SamplingModeSampled, Enabled: true},
		{Protocol: ProtocolSFlow5, SourcePrefix: "192.0.2.1/32", TenantID: "tenant-a", ExporterID: "exporter-a", TargetID: "target-a", OwnershipEpoch: 2, SamplingMode: SamplingModeSampled, Enabled: true},
	}
	registry, err := CompilePlan(plan, now)
	if err != nil {
		t.Fatal(err)
	}
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: {
		marshalCollectStateMessage(t, "state", 0, 0, state),
		marshalQualityCheckpointMessage(t, "state", 0, 1, checkpoint),
	}}, map[int32][2]int64{0: {0, 2}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 10, source: source}
	snapshot, err := reader.ReadAllWithHistory(context.Background(), registry, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.CollectStates) != 1 || len(snapshot.QualityCheckpoints) != 1 || snapshot.CollectStates[0].Offset != 0 || snapshot.QualityCheckpoints[0].Offset != 1 || !proto.Equal(snapshot.QualityCheckpoints[0].Checkpoint, checkpoint) {
		t.Fatalf("typed collect-state snapshot was not dispatched: %+v", snapshot)
	}
}

func TestKafkaCollectStateReaderAppliesQualityTombstone(t *testing.T) {
	now := time.Now()
	checkpoint := qualityCheckpointFixture(t, "collector-a", 2, 1, 1, now.Add(-time.Second))
	key, err := qualityCheckpointKafkaKey(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: {
		marshalQualityCheckpointMessage(t, "state", 0, 0, checkpoint),
		{Topic: "state", Partition: 0, Offset: 1, Key: key},
	}}, map[int32][2]int64{0: {0, 2}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 10, source: source}
	snapshot, err := reader.ReadAllWithHistory(context.Background(), qualityCheckpointRegistry(t, "collector-a", 2, 1, now), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.QualityCheckpoints) != 0 {
		t.Fatalf("tombstoned quality checkpoint was retained: %+v", snapshot.QualityCheckpoints)
	}
}

func TestKafkaCollectStateCompatibilityReaderConsumesQualityKeyspace(t *testing.T) {
	now := time.Now()
	checkpoint := qualityCheckpointFixture(t, "collector-a", 2, 1, 1, now.Add(-time.Second))
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: {
		marshalQualityCheckpointMessage(t, "state", 0, 0, checkpoint),
	}}, map[int32][2]int64{0: {0, 1}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 10, source: source}
	records, err := reader.Read(context.Background(), qualityCheckpointRegistry(t, "collector-a", 2, 1, now))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("compatibility reader returned quality checkpoints as decoder state: %+v", records)
	}
}

func TestKafkaCollectStateReaderRejectsUnknownTypedKey(t *testing.T) {
	key := make([]byte, 1+sha256.Size)
	key[0] = 'X'
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: {
		{Topic: "state", Partition: 0, Offset: 0, Key: key},
	}}, map[int32][2]int64{0: {0, 1}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 10, source: source}
	if _, err := reader.ReadAllWithHistory(context.Background(), qualityCheckpointRegistry(t, "collector-a", 2, 1, time.Now()), nil); err == nil {
		t.Fatal("unknown typed key was accepted as a tombstone")
	}
}

func TestKafkaCollectStateReaderBoundsHistoricalTypedKeys(t *testing.T) {
	first, second := make([]byte, sha256.Size), make([]byte, sha256.Size)
	first[0], second[0] = 1, 2
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: {
		{Topic: "state", Partition: 0, Offset: 0, Key: first},
		{Topic: "state", Partition: 0, Offset: 1, Key: second},
	}}, map[int32][2]int64{0: {0, 2}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 1, source: source}
	if _, err := reader.ReadAllWithHistory(context.Background(), qualityCheckpointRegistry(t, "collector-a", 2, 1, time.Now()), nil); err == nil {
		t.Fatal("distinct typed-key capacity did not bound historical tombstones")
	}
}

func TestKafkaCollectStateReaderRejectsLiveKeyAcrossPartitions(t *testing.T) {
	now := time.Now()
	checkpoint := qualityCheckpointFixture(t, "collector-a", 2, 1, 1, now.Add(-time.Second))
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{
		0: {marshalQualityCheckpointMessage(t, "state", 0, 0, checkpoint)},
		1: {marshalQualityCheckpointMessage(t, "state", 1, 0, checkpoint)},
	}, map[int32][2]int64{0: {0, 1}, 1: {0, 1}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 10, source: source}
	if _, err := reader.ReadAllWithHistory(context.Background(), qualityCheckpointRegistry(t, "collector-a", 2, 1, now), nil); err == nil {
		t.Fatal("live quality checkpoint key was accepted across partitions")
	}
}

func TestKafkaCollectStateReaderAppliesTombstone(t *testing.T) {
	decoder, record, decoded := collectStateFixture(t)
	state, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 2}, "collector-a", decoder)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: {
		marshalCollectStateMessage(t, "state", 0, 0, state),
		{Topic: "state", Partition: 0, Offset: 1, Key: bytes.Clone(state.StateKey)},
	}}, map[int32][2]int64{0: {0, 2}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 10, source: source}
	records, err := reader.Read(context.Background(), collectStateRegistry(t, "collector-a", 2, record.Source.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("tombstoned collect state was retained: %+v", records)
	}
}

func TestKafkaCollectStateReaderUsesHistoryForRemovedSource(t *testing.T) {
	decoder, record, decoded := collectStateFixture(t)
	historical := collectStateRegistry(t, "collector-a", 1, record.Source.Addr())
	state, err := BuildCollectState(record, decoded, historical.Plan().Sources[0], "collector-a", decoder)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	activePlan := historical.Plan()
	activePlan.Revision = 2
	activePlan.Sources = nil
	active, err := CompilePlan(activePlan, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: {
		marshalCollectStateMessage(t, "state", 0, 0, state),
	}}, map[int32][2]int64{0: {0, 1}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 10, source: source}
	records, err := reader.ReadWithHistory(context.Background(), active, testPlanHistory(active, historical, active))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].State.RegistryVersion != historical.Plan().Revision {
		t.Fatalf("historical state was not retained: %+v", records)
	}
}

func TestKafkaCollectStateReaderRejectsKeyMismatch(t *testing.T) {
	decoder, record, decoded := collectStateFixture(t)
	state, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 2}, "collector-a", decoder)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	message := marshalCollectStateMessage(t, "state", 0, 0, state)
	message.Key[0] ^= 0xff
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: {message}}, map[int32][2]int64{0: {0, 1}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 10, source: source}
	if _, err := reader.Read(context.Background(), collectStateRegistry(t, "collector-a", 2, record.Source.Addr())); err == nil {
		t.Fatal("Kafka key/payload mismatch was accepted")
	}
}

func TestKafkaCollectStateReaderTimesOutBeforeBoundary(t *testing.T) {
	record := decodeWALRecord(ProtocolNetFlow9, []byte{0, 9})
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: nil}, map[int32][2]int64{0: {0, 1}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: 10 * time.Millisecond, maxCandidates: 10, source: source}
	if _, err := reader.Read(context.Background(), collectStateRegistry(t, "collector-a", 1, record.Source.Addr())); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error=%v, want context deadline exceeded", err)
	}
}

func TestOpenCollectStateStoreMergesLocalAndRemoteBeforeRestore(t *testing.T) {
	now := time.Now()
	flowContext := netflowContextForSource("192.0.2.1")
	localDecoder, record, decoded := collectStateFixture(t)
	if err := localDecoder.sampling.Set(flowContext, 9, 42, 2000); err != nil {
		t.Fatal(err)
	}
	registry := collectStateRegistry(t, "collector-b", 5, record.Source.Addr())
	local, err := BuildCollectState(record, decoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 5}, "collector-b", localDecoder)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := OpenCollectStateStore(dir, "collector-b", registry, localDecoder)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Persist(local); err != nil {
		t.Fatal(err)
	}
	localDecoder.Close()

	remoteDecoder, remoteRecord, remoteDecoded := collectStateFixture(t)
	if err := remoteDecoder.sampling.Set(flowContext, 9, 42, 1000); err != nil {
		t.Fatal(err)
	}
	remote, err := BuildCollectState(remoteRecord, remoteDecoded, SourceBinding{TenantID: "tenant-a", ExporterID: "exporter-a", OwnershipEpoch: 4}, "collector-a", remoteDecoder)
	remoteDecoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	remote.StateGeneration = 999
	remoteDigest, _ := collectStateDigest(remote)
	remote.PayloadSha256 = remoteDigest[:]
	restored, _ := NewDecoder()
	defer restored.Close()
	if _, err := OpenCollectStateStoreWithRemote(dir, "collector-b", registry, restored, []CollectStateRecord{{State: remote, Offset: 999}}); err != nil {
		t.Fatal(err)
	}
	data, err := restored.Decode(decodeWALRecord(ProtocolNetFlow9, mustMarshalNFv9(t, netflowDataPacket())))
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Records) != 1 || data.Records[0].SamplingRate != 2000 {
		t.Fatalf("remote stale state overwrote newer local state at %s: %+v", now, data)
	}
}

func cloneCollectStateGeneration(t *testing.T, state *flowpb.CollectState, generation uint64) *flowpb.CollectState {
	t.Helper()
	clone := proto.Clone(state).(*flowpb.CollectState)
	clone.StateGeneration = generation
	digest, err := collectStateDigest(clone)
	if err != nil {
		t.Fatal(err)
	}
	clone.PayloadSha256 = digest[:]
	return clone
}

func marshalCollectStateMessage(t *testing.T, topic string, partition int32, offset int64, state *flowpb.CollectState) *sarama.ConsumerMessage {
	t.Helper()
	value, err := proto.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return &sarama.ConsumerMessage{Topic: topic, Partition: partition, Offset: offset, Key: bytes.Clone(state.StateKey), Value: value}
}

func marshalQualityCheckpointMessage(t *testing.T, topic string, partition int32, offset int64, checkpoint *flowpb.QualityCheckpoint) *sarama.ConsumerMessage {
	t.Helper()
	value, err := proto.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	key, err := qualityCheckpointKafkaKey(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return &sarama.ConsumerMessage{Topic: topic, Partition: partition, Offset: offset, Key: key, Value: value}
}

func netflowContextForSource(source string) netflow.FlowContext {
	return netflow.FlowContext{RouterKey: source}
}

type fakeCollectStateSource struct {
	topic      string
	partitions []int32
	offsets    map[int32][2]int64
	consumers  map[int32]*fakePartitionConsumer
	mu         sync.Mutex
	events     []string
}

func newFakeCollectStateSource(topic string, messages map[int32][]*sarama.ConsumerMessage, offsets map[int32][2]int64) *fakeCollectStateSource {
	source := &fakeCollectStateSource{topic: topic, offsets: offsets, consumers: make(map[int32]*fakePartitionConsumer)}
	for partition, records := range messages {
		source.partitions = append(source.partitions, partition)
		consumer := &fakePartitionConsumer{messages: make(chan *sarama.ConsumerMessage, len(records)), errors: make(chan *sarama.ConsumerError)}
		for _, record := range records {
			consumer.messages <- record
		}
		source.consumers[partition] = consumer
	}
	return source
}

func (s *fakeCollectStateSource) Partitions(topic string) ([]int32, error) {
	s.recordEvent("partitions")
	if topic != s.topic {
		return nil, errors.New("unexpected topic")
	}
	return append([]int32(nil), s.partitions...), nil
}

func (s *fakeCollectStateSource) GetOffset(topic string, partition int32, position int64) (int64, error) {
	if topic != s.topic {
		return 0, errors.New("unexpected topic")
	}
	offsets, ok := s.offsets[partition]
	if !ok {
		return 0, errors.New("unknown partition")
	}
	if position == sarama.OffsetOldest {
		s.recordEvent(fmt.Sprintf("oldest/%d", partition))
		return offsets[0], nil
	}
	if position == sarama.OffsetNewest {
		s.recordEvent(fmt.Sprintf("newest/%d", partition))
		return offsets[1], nil
	}
	return 0, errors.New("unexpected offset selector")
}

func (s *fakeCollectStateSource) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	s.recordEvent(fmt.Sprintf("consume/%d/%d", partition, offset))
	if topic != s.topic || s.consumers[partition] == nil {
		return nil, errors.New("unexpected partition")
	}
	return s.consumers[partition], nil
}

func (s *fakeCollectStateSource) Close() error { return nil }

func (s *fakeCollectStateSource) recordEvent(event string) {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
}

func (s *fakeCollectStateSource) eventsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

type fakePartitionConsumer struct {
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	paused   bool
}

func (c *fakePartitionConsumer) AsyncClose()                              {}
func (c *fakePartitionConsumer) Close() error                             { return nil }
func (c *fakePartitionConsumer) Messages() <-chan *sarama.ConsumerMessage { return c.messages }
func (c *fakePartitionConsumer) Errors() <-chan *sarama.ConsumerError     { return c.errors }
func (c *fakePartitionConsumer) HighWaterMarkOffset() int64               { return 0 }
func (c *fakePartitionConsumer) Pause()                                   { c.paused = true }
func (c *fakePartitionConsumer) Resume()                                  { c.paused = false }
func (c *fakePartitionConsumer) IsPaused() bool                           { return c.paused }
