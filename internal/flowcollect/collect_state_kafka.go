package flowcollect

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"google.golang.org/protobuf/proto"
)

type collectStateKafkaSource interface {
	Partitions(string) ([]int32, error)
	GetOffset(string, int32, int64) (int64, error)
	ConsumePartition(string, int32, int64) (sarama.PartitionConsumer, error)
	Close() error
}

type saramaCollectStateSource struct {
	client   sarama.Client
	consumer sarama.Consumer
}

func (s *saramaCollectStateSource) Partitions(topic string) ([]int32, error) {
	return s.client.Partitions(topic)
}

func (s *saramaCollectStateSource) GetOffset(topic string, partition int32, offset int64) (int64, error) {
	return s.client.GetOffset(topic, partition, offset)
}

func (s *saramaCollectStateSource) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	return s.consumer.ConsumePartition(topic, partition, offset)
}

func (s *saramaCollectStateSource) Close() error {
	return errors.Join(s.consumer.Close(), s.client.Close())
}

type KafkaCollectStateReader struct {
	topic         string
	timeout       time.Duration
	maxCandidates int
	source        collectStateKafkaSource
}

func NewKafkaCollectStateReader(config KafkaConfig, collectorID string) (*KafkaCollectStateReader, error) {
	if len(config.Brokers) == 0 || config.CollectStateTopic == "" || config.CollectStateRestoreTimeout <= 0 || config.CollectStateRestoreMaxCandidates <= 0 {
		return nil, errors.New("Kafka collect-state restore configuration is invalid")
	}
	saramaConfig, err := buildKafkaClientConfig(config, collectorID+"-state-restore")
	if err != nil {
		return nil, err
	}
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Consumer.Fetch.Default = 1 << 20
	saramaConfig.Consumer.Fetch.Max = collectStateMaxBytes
	saramaConfig.Consumer.MaxWaitTime = 250 * time.Millisecond
	if err := saramaConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validate Kafka collect-state consumer config: %w", err)
	}
	client, err := sarama.NewClient(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kafka collect-state client: %w", err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("create Kafka collect-state consumer: %w", err)
	}
	return &KafkaCollectStateReader{topic: config.CollectStateTopic, timeout: config.CollectStateRestoreTimeout, maxCandidates: config.CollectStateRestoreMaxCandidates, source: &saramaCollectStateSource{client: client, consumer: consumer}}, nil
}

func (r *KafkaCollectStateReader) Close() error {
	if r == nil || r.source == nil {
		return nil
	}
	return r.source.Close()
}

type collectStatePartitionBoundary struct {
	partition int32
	oldest    int64
	newest    int64
}

type collectStateReadResult struct {
	message *sarama.ConsumerMessage
	err     error
}

type QualityCheckpointRecord struct {
	Checkpoint *flowpb.QualityCheckpoint
	Partition  int32
	Offset     int64
}

type CollectStateKafkaSnapshot struct {
	CollectStates      []CollectStateRecord
	QualityCheckpoints []QualityCheckpointRecord
}

// Read captures every partition's next-offset high watermark before it opens
// consumers. Only messages below that frozen vector are eligible, so startup
// is finite even while active owners continue publishing.
func (r *KafkaCollectStateReader) Read(ctx context.Context, registry *Registry) ([]CollectStateRecord, error) {
	return r.ReadWithHistory(ctx, registry, nil)
}

func (r *KafkaCollectStateReader) ReadWithHistory(ctx context.Context, registry *Registry, plans *PlanHistory) ([]CollectStateRecord, error) {
	snapshot, err := r.ReadAllWithHistory(ctx, registry, plans)
	if err != nil {
		return nil, err
	}
	return snapshot.CollectStates, nil
}

func (r *KafkaCollectStateReader) ReadAllWithHistory(ctx context.Context, registry *Registry, plans *PlanHistory) (CollectStateKafkaSnapshot, error) {
	if r == nil || r.source == nil || r.topic == "" || r.timeout <= 0 || r.maxCandidates <= 0 || registry == nil || ctx == nil {
		return CollectStateKafkaSnapshot{}, errors.New("Kafka collect-state reader and registry are required")
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	partitions, err := r.source.Partitions(r.topic)
	if err != nil {
		return CollectStateKafkaSnapshot{}, fmt.Errorf("list Kafka collect-state partitions: %w", err)
	}
	if len(partitions) == 0 {
		return CollectStateKafkaSnapshot{}, errors.New("Kafka collect-state topic has no partitions")
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	boundaries := make([]collectStatePartitionBoundary, 0, len(partitions))
	for index, partition := range partitions {
		if partition < 0 || (index > 0 && partition == partitions[index-1]) {
			return CollectStateKafkaSnapshot{}, errors.New("Kafka collect-state partition metadata is invalid")
		}
		oldest, err := r.source.GetOffset(r.topic, partition, sarama.OffsetOldest)
		if err != nil {
			return CollectStateKafkaSnapshot{}, fmt.Errorf("read Kafka collect-state oldest offset for partition %d: %w", partition, err)
		}
		newest, err := r.source.GetOffset(r.topic, partition, sarama.OffsetNewest)
		if err != nil {
			return CollectStateKafkaSnapshot{}, fmt.Errorf("read Kafka collect-state high watermark for partition %d: %w", partition, err)
		}
		if oldest < 0 || newest < oldest {
			return CollectStateKafkaSnapshot{}, fmt.Errorf("Kafka collect-state offsets are invalid for partition %d: oldest=%d newest=%d", partition, oldest, newest)
		}
		boundaries = append(boundaries, collectStatePartitionBoundary{partition: partition, oldest: oldest, newest: newest})
	}
	return r.readBoundarySnapshot(ctx, registry, plans, boundaries)
}

func (r *KafkaCollectStateReader) readBoundarySnapshot(ctx context.Context, registry *Registry, plans *PlanHistory, boundaries []collectStatePartitionBoundary) (CollectStateKafkaSnapshot, error) {
	type activeConsumer struct {
		boundary collectStatePartitionBoundary
		consumer sarama.PartitionConsumer
	}
	active := make([]activeConsumer, 0, len(boundaries))
	for _, boundary := range boundaries {
		if boundary.newest == boundary.oldest {
			continue
		}
		consumer, err := r.source.ConsumePartition(r.topic, boundary.partition, boundary.oldest)
		if err != nil {
			for _, opened := range active {
				_ = opened.consumer.Close()
			}
			return CollectStateKafkaSnapshot{}, fmt.Errorf("consume Kafka collect-state partition %d: %w", boundary.partition, err)
		}
		active = append(active, activeConsumer{boundary: boundary, consumer: consumer})
	}
	if len(active) == 0 {
		return CollectStateKafkaSnapshot{}, nil
	}
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan collectStateReadResult, 256)
	var workers sync.WaitGroup
	for _, item := range active {
		workers.Add(1)
		go func() {
			defer workers.Done()
			consumeCollectStateBoundary(readCtx, r.topic, item.boundary, item.consumer, results)
			if err := item.consumer.Close(); err != nil {
				sendCollectStateReadResult(readCtx, results, collectStateReadResult{err: fmt.Errorf("close Kafka collect-state partition %d: %w", item.boundary.partition, err)})
			}
		}()
	}
	go func() {
		workers.Wait()
		close(results)
	}()

	collectByKey := make(map[string]CollectStateRecord)
	qualityByKey := make(map[string]QualityCheckpointRecord)
	keyPartitions := make(map[string]int32)
	var firstErr error
	for result := range results {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
				cancel()
			}
			continue
		}
		if firstErr != nil {
			continue
		}
		message := result.message
		key := string(message.Key)
		isCollectState := len(message.Key) == sha256.Size
		isQualityCheckpoint := isQualityCheckpointKafkaKey(message.Key)
		if !isCollectState && !isQualityCheckpoint {
			firstErr = fmt.Errorf("Kafka collect-state partition=%d offset=%d has invalid typed key", message.Partition, message.Offset)
			cancel()
			continue
		}
		if partition, exists := keyPartitions[key]; exists {
			if partition != message.Partition {
				firstErr = fmt.Errorf("Kafka collect-state key appears in partitions %d and %d", partition, message.Partition)
				cancel()
				continue
			}
		} else {
			if len(keyPartitions) >= r.maxCandidates {
				firstErr = fmt.Errorf("Kafka collect-state restore exceeds %d distinct typed keys", r.maxCandidates)
				cancel()
				continue
			}
			keyPartitions[key] = message.Partition
		}
		if message.Value == nil {
			delete(collectByKey, key)
			delete(qualityByKey, key)
			continue
		}
		if len(message.Value) > collectStateMaxBytes-collectStateHeaderSize {
			firstErr = fmt.Errorf("Kafka collect-state partition=%d offset=%d exceeds value limit", message.Partition, message.Offset)
			cancel()
			continue
		}
		if isCollectState {
			state := &flowpb.CollectState{}
			if err := proto.Unmarshal(message.Value, state); err != nil {
				firstErr = fmt.Errorf("decode Kafka collect-state partition=%d offset=%d: %w", message.Partition, message.Offset, err)
				cancel()
				continue
			}
			if err := validateCollectState(state, ""); err != nil {
				firstErr = fmt.Errorf("validate Kafka collect-state partition=%d offset=%d: %w", message.Partition, message.Offset, err)
				cancel()
				continue
			}
			if !bytes.Equal(message.Key, state.StateKey) {
				firstErr = fmt.Errorf("Kafka collect-state partition=%d offset=%d key does not match payload", message.Partition, message.Offset)
				cancel()
				continue
			}
			eligible, err := authorizeCollectStateRecovery(state, registry, plans, true)
			if err != nil {
				firstErr = fmt.Errorf("authorize Kafka collect-state partition=%d offset=%d: %w", message.Partition, message.Offset, err)
				cancel()
				continue
			}
			if !eligible {
				continue
			}
			if _, exists := collectByKey[key]; !exists && len(collectByKey)+len(qualityByKey) >= r.maxCandidates {
				firstErr = fmt.Errorf("Kafka collect-state restore exceeds %d candidates", r.maxCandidates)
				cancel()
				continue
			}
			collectByKey[key] = CollectStateRecord{State: state, Partition: message.Partition, Offset: message.Offset}
			continue
		}

		checkpoint := &flowpb.QualityCheckpoint{}
		if err := proto.Unmarshal(message.Value, checkpoint); err != nil {
			firstErr = fmt.Errorf("decode Kafka quality checkpoint partition=%d offset=%d: %w", message.Partition, message.Offset, err)
			cancel()
			continue
		}
		if err := validateQualityCheckpoint(checkpoint, 0); err != nil {
			firstErr = fmt.Errorf("validate Kafka quality checkpoint partition=%d offset=%d: %w", message.Partition, message.Offset, err)
			cancel()
			continue
		}
		expectedKey, err := qualityCheckpointKafkaKey(checkpoint)
		if err != nil || !bytes.Equal(message.Key, expectedKey) {
			firstErr = fmt.Errorf("Kafka quality checkpoint partition=%d offset=%d key does not match payload", message.Partition, message.Offset)
			cancel()
			continue
		}
		eligible, err := authorizeQualityCheckpoint(checkpoint, registry, true)
		if err != nil {
			firstErr = fmt.Errorf("authorize Kafka quality checkpoint partition=%d offset=%d: %w", message.Partition, message.Offset, err)
			cancel()
			continue
		}
		if !eligible {
			continue
		}
		if _, exists := qualityByKey[key]; !exists && len(collectByKey)+len(qualityByKey) >= r.maxCandidates {
			firstErr = fmt.Errorf("Kafka collect-state restore exceeds %d candidates", r.maxCandidates)
			cancel()
			continue
		}
		qualityByKey[key] = QualityCheckpointRecord{Checkpoint: checkpoint, Partition: message.Partition, Offset: message.Offset}
	}
	if firstErr != nil {
		return CollectStateKafkaSnapshot{}, firstErr
	}
	if err := ctx.Err(); err != nil {
		return CollectStateKafkaSnapshot{}, fmt.Errorf("read Kafka collect-state snapshot: %w", err)
	}
	snapshot := CollectStateKafkaSnapshot{CollectStates: make([]CollectStateRecord, 0, len(collectByKey)), QualityCheckpoints: make([]QualityCheckpointRecord, 0, len(qualityByKey))}
	for _, record := range collectByKey {
		snapshot.CollectStates = append(snapshot.CollectStates, record)
	}
	for _, record := range qualityByKey {
		snapshot.QualityCheckpoints = append(snapshot.QualityCheckpoints, record)
	}
	sort.Slice(snapshot.CollectStates, func(i, j int) bool {
		if snapshot.CollectStates[i].Partition != snapshot.CollectStates[j].Partition {
			return snapshot.CollectStates[i].Partition < snapshot.CollectStates[j].Partition
		}
		return snapshot.CollectStates[i].Offset < snapshot.CollectStates[j].Offset
	})
	sort.Slice(snapshot.QualityCheckpoints, func(i, j int) bool {
		if snapshot.QualityCheckpoints[i].Partition != snapshot.QualityCheckpoints[j].Partition {
			return snapshot.QualityCheckpoints[i].Partition < snapshot.QualityCheckpoints[j].Partition
		}
		return snapshot.QualityCheckpoints[i].Offset < snapshot.QualityCheckpoints[j].Offset
	})
	return snapshot, nil
}

func consumeCollectStateBoundary(ctx context.Context, topic string, boundary collectStatePartitionBoundary, consumer sarama.PartitionConsumer, results chan<- collectStateReadResult) {
	lastOffset := boundary.oldest - 1
	errorsChannel := consumer.Errors()
	for {
		select {
		case <-ctx.Done():
			sendCollectStateReadResult(ctx, results, collectStateReadResult{err: ctx.Err()})
			return
		case consumeErr, ok := <-errorsChannel:
			if !ok {
				errorsChannel = nil
				continue
			}
			if consumeErr != nil {
				sendCollectStateReadResult(ctx, results, collectStateReadResult{err: consumeErr})
				return
			}
		case message, ok := <-consumer.Messages():
			if !ok {
				sendCollectStateReadResult(ctx, results, collectStateReadResult{err: fmt.Errorf("Kafka collect-state partition %d ended before high watermark %d", boundary.partition, boundary.newest)})
				return
			}
			if message == nil || message.Topic != topic || message.Partition != boundary.partition || message.Offset <= lastOffset {
				sendCollectStateReadResult(ctx, results, collectStateReadResult{err: fmt.Errorf("Kafka collect-state partition %d returned invalid message order", boundary.partition)})
				return
			}
			lastOffset = message.Offset
			if message.Offset >= boundary.newest {
				sendCollectStateReadResult(ctx, results, collectStateReadResult{err: fmt.Errorf("Kafka collect-state partition %d advanced beyond frozen high watermark %d", boundary.partition, boundary.newest)})
				return
			}
			if !sendCollectStateReadResult(ctx, results, collectStateReadResult{message: message}) {
				return
			}
			if message.Offset+1 == boundary.newest {
				return
			}
		}
	}
}

func sendCollectStateReadResult(ctx context.Context, results chan<- collectStateReadResult, result collectStateReadResult) bool {
	select {
	case results <- result:
		return true
	case <-ctx.Done():
		return false
	}
}
