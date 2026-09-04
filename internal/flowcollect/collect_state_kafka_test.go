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
	if got := source.eventsSnapshot(); len(got) != 4 || got[0] != "partitions" || got[1] != "oldest/0" || got[2] != "newest/0" || got[3] != "scan/0/0/2" {
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

func TestKafkaCollectStateReaderRejectsEmptyNonNullValue(t *testing.T) {
	key := make([]byte, sha256.Size)
	key[0] = 1
	source := newFakeCollectStateSource("state", map[int32][]*sarama.ConsumerMessage{0: {
		{Topic: "state", Partition: 0, Offset: 0, Key: key, Value: []byte{}},
	}}, map[int32][2]int64{0: {0, 1}})
	reader := &KafkaCollectStateReader{topic: "state", timeout: time.Second, maxCandidates: 10, source: source}
	if _, err := reader.ReadAllWithHistory(context.Background(), qualityCheckpointRegistry(t, "collector-a", 2, 1, time.Now()), nil); err == nil {
		t.Fatal("empty non-null Kafka value was accepted as a compaction tombstone")
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
	source.stalled[0] = true
	reader := &KafkaCollectStateReader{topic: "state", timeout: 10 * time.Millisecond, maxCandidates: 10, source: source}
	if _, err := reader.Read(context.Background(), collectStateRegistry(t, "collector-a", 1, record.Source.Addr())); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error=%v, want context deadline exceeded", err)
	}
}

func TestDecodeCollectStateFetchBlockCompletesAcrossCompactedTailHole(t *testing.T) {
	boundary := collectStatePartitionBoundary{partition: 2, oldest: 3, newest: 10}
	messages, next, complete, err := decodeCollectStateFetchBlock("state", boundary, 8, &sarama.FetchResponseBlock{HighWaterMarkOffset: 12, LogStartOffset: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 0 || next != boundary.newest || !complete {
		t.Fatalf("compacted tail did not reach frozen boundary: messages=%d next=%d complete=%t", len(messages), next, complete)
	}
}

func TestDecodeCollectStateFetchBlockAdvancesAcrossInternalOffsetHoles(t *testing.T) {
	boundary := collectStatePartitionBoundary{partition: 2, oldest: 3, newest: 10}
	batch := &sarama.RecordBatch{
		FirstOffset:     7,
		Version:         2,
		LastOffsetDelta: 0,
		FirstTimestamp:  time.Unix(100, 0),
		Records: []*sarama.Record{{
			OffsetDelta: 0,
			Key:         []byte("key"),
			Value:       []byte("value"),
		}},
	}
	messages, next, complete, err := decodeCollectStateFetchBlock("state", boundary, 3, &sarama.FetchResponseBlock{
		HighWaterMarkOffset: 12,
		LogStartOffset:      3,
		RecordsSet:          []*sarama.Records{{RecordBatch: batch}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Offset != 7 || messages[0].Topic != "state" || messages[0].Partition != 2 || next != 8 || complete {
		t.Fatalf("unexpected compacted internal-gap result: messages=%+v next=%d complete=%t", messages, next, complete)
	}
}

func TestDecodeCollectStateFetchBlockStopsBeforePostBoundaryRecord(t *testing.T) {
	boundary := collectStatePartitionBoundary{partition: 1, oldest: 0, newest: 10}
	batch := &sarama.RecordBatch{
		FirstOffset:     12,
		Version:         2,
		LastOffsetDelta: 0,
		Records:         []*sarama.Record{{OffsetDelta: 0, Key: []byte("newer"), Value: []byte("excluded")}},
	}
	messages, next, complete, err := decodeCollectStateFetchBlock("state", boundary, 8, &sarama.FetchResponseBlock{
		HighWaterMarkOffset: 13,
		RecordsSet:          []*sarama.Records{{RecordBatch: batch}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 0 || next != 10 || !complete {
		t.Fatalf("post-boundary record leaked: messages=%+v next=%d complete=%t", messages, next, complete)
	}
}

func TestDecodeCollectStateFetchBlockRejectsTransactionalState(t *testing.T) {
	boundary := collectStatePartitionBoundary{partition: 0, oldest: 0, newest: 1}
	batch := &sarama.RecordBatch{FirstOffset: 0, Version: 2, LastOffsetDelta: 0, IsTransactional: true, Records: []*sarama.Record{{OffsetDelta: 0}}}
	_, _, _, err := decodeCollectStateFetchBlock("state", boundary, 0, &sarama.FetchResponseBlock{HighWaterMarkOffset: 1, RecordsSet: []*sarama.Records{{RecordBatch: batch}}})
	if err == nil {
		t.Fatal("transactional collect-state record was accepted")
	}
}

func TestDecodeCollectStateFetchBlockPreservesNullAndEmptyValues(t *testing.T) {
	boundary := collectStatePartitionBoundary{partition: 0, oldest: 0, newest: 2}
	batch := &sarama.RecordBatch{
		FirstOffset:     0,
		Version:         2,
		LastOffsetDelta: 1,
		Records: []*sarama.Record{
			{OffsetDelta: 0, Key: []byte("null"), Value: nil},
			{OffsetDelta: 1, Key: []byte("empty"), Value: []byte{}},
		},
	}
	messages, next, complete, err := decodeCollectStateFetchBlock("state", boundary, 0, &sarama.FetchResponseBlock{HighWaterMarkOffset: 2, RecordsSet: []*sarama.Records{{RecordBatch: batch}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Value != nil || messages[1].Value == nil || len(messages[1].Value) != 0 || next != 2 || !complete {
		t.Fatalf("Kafka null/empty distinction was lost: messages=%+v next=%d complete=%t", messages, next, complete)
	}
}

func TestDecodeCollectStateFetchBlockReadsLegacyMessage(t *testing.T) {
	boundary := collectStatePartitionBoundary{partition: 3, oldest: 4, newest: 5}
	message := &sarama.Message{Version: 1, Key: []byte("legacy"), Value: []byte("state"), Timestamp: time.Unix(200, 0)}
	messages, next, complete, err := decodeCollectStateFetchBlock("state", boundary, 4, &sarama.FetchResponseBlock{
		HighWaterMarkOffset: 5,
		RecordsSet:          []*sarama.Records{{MsgSet: &sarama.MessageSet{Messages: []*sarama.MessageBlock{{Offset: 4, Msg: message}}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Offset != 4 || !bytes.Equal(messages[0].Key, message.Key) || !bytes.Equal(messages[0].Value, message.Value) || next != 5 || !complete {
		t.Fatalf("unexpected legacy fetch result: messages=%+v next=%d complete=%t", messages, next, complete)
	}
}

func TestDecodeCollectStateFetchBlockRejectsUnsafeBoundaries(t *testing.T) {
	boundary := collectStatePartitionBoundary{partition: 0, oldest: 0, newest: 2}
	for name, block := range map[string]*sarama.FetchResponseBlock{
		"watermark regression": {HighWaterMarkOffset: 1},
		"partial response":     {HighWaterMarkOffset: 2, Partial: true},
		"partial batch": {HighWaterMarkOffset: 2, RecordsSet: []*sarama.Records{{RecordBatch: &sarama.RecordBatch{
			FirstOffset: 0, LastOffsetDelta: 1, PartialTrailingRecord: true,
		}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := decodeCollectStateFetchBlock("state", boundary, 0, block); err == nil {
				t.Fatal("unsafe Kafka fetch boundary was accepted")
			}
		})
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
	messages   map[int32][]*sarama.ConsumerMessage
	stalled    map[int32]bool
	mu         sync.Mutex
	events     []string
}

func newFakeCollectStateSource(topic string, messages map[int32][]*sarama.ConsumerMessage, offsets map[int32][2]int64) *fakeCollectStateSource {
	source := &fakeCollectStateSource{topic: topic, offsets: offsets, messages: messages, stalled: make(map[int32]bool)}
	for partition := range messages {
		source.partitions = append(source.partitions, partition)
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

func (s *fakeCollectStateSource) ScanBoundary(ctx context.Context, topic string, boundary collectStatePartitionBoundary, emit func(*sarama.ConsumerMessage) error) error {
	s.recordEvent(fmt.Sprintf("scan/%d/%d/%d", boundary.partition, boundary.oldest, boundary.newest))
	if topic != s.topic || emit == nil {
		return errors.New("unexpected boundary scan")
	}
	if s.stalled[boundary.partition] {
		<-ctx.Done()
		return ctx.Err()
	}
	for _, message := range s.messages[boundary.partition] {
		if message.Offset < boundary.oldest || message.Offset >= boundary.newest {
			continue
		}
		if err := emit(message); err != nil {
			return err
		}
	}
	return nil
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
