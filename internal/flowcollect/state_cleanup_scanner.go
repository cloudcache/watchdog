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
)

type KafkaStatePartitionBoundary struct {
	Partition     int32 `json:"partition"`
	OldestOffset  int64 `json:"oldest_offset"`
	HighWatermark int64 `json:"high_watermark"`
}

// KafkaStateKeySnapshot is a frozen, exact-key view. LastRecord is retained
// even when it is a Kafka-null tombstone; Present only describes its value.
type KafkaStateKeySnapshot struct {
	KafkaKey   []byte                        `json:"kafka_key"`
	CapturedAt time.Time                     `json:"captured_at"`
	Present    bool                          `json:"present"`
	Value      []byte                        `json:"value,omitempty"`
	LastRecord *KafkaRecordPosition          `json:"last_record,omitempty"`
	Boundaries []KafkaStatePartitionBoundary `json:"boundaries"`
}

func (s KafkaStateKeySnapshot) HighWatermark(partition int32) (int64, bool) {
	index := sort.Search(len(s.Boundaries), func(index int) bool { return s.Boundaries[index].Partition >= partition })
	if index == len(s.Boundaries) || s.Boundaries[index].Partition != partition {
		return 0, false
	}
	return s.Boundaries[index].HighWatermark, true
}

func (s KafkaStateKeySnapshot) ReplacementObservation(restoredOldOwnershipEpoch, restoredOldGeneration uint64) (FrozenReplacementObservation, error) {
	if !s.Present || s.LastRecord == nil || s.CapturedAt.IsZero() || restoredOldOwnershipEpoch == 0 || restoredOldGeneration == 0 {
		return FrozenReplacementObservation{}, errors.New("Kafka state-key snapshot does not prove a replacement")
	}
	highWatermark, ok := s.HighWatermark(s.LastRecord.Partition)
	if !ok || s.LastRecord.Offset < 0 || highWatermark <= s.LastRecord.Offset {
		return FrozenReplacementObservation{}, errors.New("Kafka replacement record is outside its frozen boundary")
	}
	return FrozenReplacementObservation{
		CapturedAt:                s.CapturedAt,
		Position:                  *s.LastRecord,
		HighWatermark:             highWatermark,
		RestoredOldOwnershipEpoch: restoredOldOwnershipEpoch,
		RestoredOldGeneration:     restoredOldGeneration,
	}, nil
}

func (s KafkaStateKeySnapshot) TombstoneVerification(receipt StateTombstoneReceipt) (FrozenTombstoneVerification, error) {
	if s.Present || s.CapturedAt.IsZero() || receipt.Position.Partition < 0 || receipt.Position.Offset < 0 || receipt.AcknowledgedAt.IsZero() || s.CapturedAt.Before(receipt.AcknowledgedAt) {
		return FrozenTombstoneVerification{}, errors.New("Kafka state-key snapshot does not prove tombstone absence")
	}
	if s.LastRecord != nil && s.LastRecord.Partition != receipt.Position.Partition {
		return FrozenTombstoneVerification{}, errors.New("Kafka state key was observed in a different partition")
	}
	highWatermark, ok := s.HighWatermark(receipt.Position.Partition)
	if !ok || highWatermark <= receipt.Position.Offset {
		return FrozenTombstoneVerification{}, errors.New("Kafka tombstone receipt is not covered by the frozen boundary")
	}
	return FrozenTombstoneVerification{CapturedAt: s.CapturedAt, Partition: receipt.Position.Partition, HighWatermark: highWatermark, KeyAbsent: true}, nil
}

type KafkaStateKeyScanner struct {
	topic   string
	timeout time.Duration
	source  collectStateKafkaSource
	now     func() time.Time
}

func NewKafkaStateKeyScanner(config KafkaConfig, reconcilerID string) (*KafkaStateKeyScanner, error) {
	if len(config.Brokers) == 0 || config.CollectStateTopic == "" || config.CollectStateRestoreTimeout <= 0 || reconcilerID == "" {
		return nil, errors.New("Kafka brokers, collect-state topic, scan timeout, and reconciler ID are required")
	}
	saramaConfig, err := buildKafkaClientConfig(config, reconcilerID+"-state-scan")
	if err != nil {
		return nil, err
	}
	if config.CollectStateRestoreTimeout < saramaConfig.Net.ReadTimeout {
		saramaConfig.Net.ReadTimeout = config.CollectStateRestoreTimeout
	}
	if err := saramaConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validate Kafka state-key scanner config: %w", err)
	}
	client, err := sarama.NewClient(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kafka state-key scanner client: %w", err)
	}
	return newKafkaStateKeyScanner(config.CollectStateTopic, config.CollectStateRestoreTimeout, &saramaCollectStateSource{client: client})
}

func newKafkaStateKeyScanner(topic string, timeout time.Duration, source collectStateKafkaSource) (*KafkaStateKeyScanner, error) {
	if topic == "" || timeout <= 0 || source == nil {
		return nil, errors.New("Kafka state-key scanner configuration is invalid")
	}
	return &KafkaStateKeyScanner{topic: topic, timeout: timeout, source: source, now: time.Now}, nil
}

func (s *KafkaStateKeyScanner) Close() error {
	if s == nil || s.source == nil {
		return nil
	}
	return s.source.Close()
}

func (s *KafkaStateKeyScanner) Scan(ctx context.Context, kafkaKey []byte) (KafkaStateKeySnapshot, error) {
	if s == nil || s.source == nil || ctx == nil || (len(kafkaKey) != sha256.Size && !isQualityCheckpointKafkaKey(kafkaKey)) {
		return KafkaStateKeySnapshot{}, errors.New("Kafka state-key scanner and typed key are required")
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	partitions, err := s.source.Partitions(s.topic)
	if err != nil {
		return KafkaStateKeySnapshot{}, fmt.Errorf("list Kafka state-key scan partitions: %w", err)
	}
	if len(partitions) == 0 {
		return KafkaStateKeySnapshot{}, errors.New("Kafka collect-state topic has no partitions")
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	boundaries := make([]collectStatePartitionBoundary, 0, len(partitions))
	publicBoundaries := make([]KafkaStatePartitionBoundary, 0, len(partitions))
	for index, partition := range partitions {
		if partition != int32(index) {
			return KafkaStateKeySnapshot{}, errors.New("Kafka collect-state partition metadata is invalid")
		}
		oldest, err := s.source.GetOffset(s.topic, partition, sarama.OffsetOldest)
		if err != nil {
			return KafkaStateKeySnapshot{}, fmt.Errorf("read Kafka state-key oldest offset for partition %d: %w", partition, err)
		}
		newest, err := s.source.GetOffset(s.topic, partition, sarama.OffsetNewest)
		if err != nil {
			return KafkaStateKeySnapshot{}, fmt.Errorf("read Kafka state-key high watermark for partition %d: %w", partition, err)
		}
		if oldest < 0 || newest < oldest {
			return KafkaStateKeySnapshot{}, fmt.Errorf("Kafka state-key offsets are invalid for partition %d: oldest=%d newest=%d", partition, oldest, newest)
		}
		boundaries = append(boundaries, collectStatePartitionBoundary{partition: partition, oldest: oldest, newest: newest})
		publicBoundaries = append(publicBoundaries, KafkaStatePartitionBoundary{Partition: partition, OldestOffset: oldest, HighWatermark: newest})
	}

	type partitionResult struct {
		position *KafkaRecordPosition
		value    []byte
		err      error
	}
	results := make(chan partitionResult, len(boundaries))
	var workers sync.WaitGroup
	for _, boundary := range boundaries {
		workers.Add(1)
		go func(boundary collectStatePartitionBoundary) {
			defer workers.Done()
			var position *KafkaRecordPosition
			var value []byte
			if boundary.oldest != boundary.newest {
				err := s.source.ScanBoundary(ctx, s.topic, boundary, func(message *sarama.ConsumerMessage) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					if bytes.Equal(message.Key, kafkaKey) {
						current := KafkaRecordPosition{Partition: boundary.partition, Offset: message.Offset}
						position = &current
						value = cloneKafkaNullableValue(message.Value)
					}
					return nil
				})
				if err != nil {
					results <- partitionResult{err: fmt.Errorf("scan Kafka state key in partition %d: %w", boundary.partition, err)}
					return
				}
			}
			results <- partitionResult{position: position, value: value}
		}(boundary)
	}
	go func() {
		workers.Wait()
		close(results)
	}()

	var found *partitionResult
	for result := range results {
		if result.err != nil {
			cancel()
			return KafkaStateKeySnapshot{}, result.err
		}
		if result.position == nil {
			continue
		}
		if found != nil {
			cancel()
			return KafkaStateKeySnapshot{}, fmt.Errorf("Kafka state key appears in partitions %d and %d", found.position.Partition, result.position.Partition)
		}
		current := result
		found = &current
	}
	if err := ctx.Err(); err != nil {
		return KafkaStateKeySnapshot{}, fmt.Errorf("scan Kafka state key: %w", err)
	}
	snapshot := KafkaStateKeySnapshot{KafkaKey: bytes.Clone(kafkaKey), CapturedAt: s.now().UTC(), Boundaries: publicBoundaries}
	if found != nil {
		snapshot.LastRecord = &KafkaRecordPosition{Partition: found.position.Partition, Offset: found.position.Offset}
		snapshot.Present = found.value != nil
		snapshot.Value = cloneKafkaNullableValue(found.value)
	}
	return snapshot, nil
}

func cloneKafkaNullableValue(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}
