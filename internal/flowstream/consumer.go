// SPDX-FileCopyrightText: 2024 Free Mobile
// SPDX-License-Identifier: AGPL-3.0-only
//
// Adapted from Akvorado outlet/kafka consumer and worker lifecycle.

package flowstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type RecordHandler func(context.Context, *kgo.Record) error
type PartitionBatchHandler func(context.Context, []*kgo.Record) error

type partitionHandler func(context.Context, []*kgo.Record) (int, error)

type consumerClient interface {
	PollFetches(context.Context) kgo.Fetches
	MarkCommitRecords(...*kgo.Record)
	CommitMarkedOffsets(context.Context) error
	AllowRebalance()
	CloseAllowingRebalance()
	Ping(context.Context) error
}

type Consumer struct {
	client           consumerClient
	replayWatermarks *replayWatermarks
	stats            *consumerStats
	onError          func(error)
	once             sync.Once
}

type replayPartition struct {
	topic     string
	partition int32
}

// replayWatermarks prevents the consumer-group commit from moving backwards
// while records before the previously committed offset are replayed solely to
// rebuild decoder template state.
type replayWatermarks struct {
	mu     sync.Mutex
	floors map[replayPartition]int64
}

type consumerStats struct {
	records           atomic.Uint64
	bytes             atomic.Uint64
	errors            atomic.Uint64
	rebalances        atomic.Uint64
	lostPartitions    atomic.Uint64
	polls             atomic.Uint64
	pollDurationNanos atomic.Uint64
	partitionsMu      sync.Mutex
	partitions        map[replayPartition]partitionRuntime
}

type partitionRuntime struct {
	lag   uint64
	known bool
}

type ConsumerStats struct {
	Records            uint64
	Bytes              uint64
	Errors             uint64
	Rebalances         uint64
	LostPartitions     uint64
	AssignedPartitions uint64
	LagKnownPartitions uint64
	LagRecords         uint64
	Polls              uint64
	PollDurationNanos  uint64
}

func NewConsumer(config ConsumerConfig, onError func(error)) (*Consumer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	opts, err := config.Kafka.options()
	if err != nil {
		return nil, err
	}
	startOffset := kgo.NewOffset().AtStart()
	if config.StartAtEnd {
		startOffset = kgo.NewOffset().AtEnd()
	}
	watermarks := &replayWatermarks{floors: make(map[replayPartition]int64)}
	stats := newConsumerStats()
	opts = append(opts,
		kgo.FetchMinBytes(config.FetchMinBytes),
		kgo.FetchMaxWait(config.FetchMaxWait),
		kgo.ConsumerGroup(config.ConsumerGroup),
		kgo.ConsumeTopics(topicForVersion(config.Kafka.Topic)),
		kgo.ConsumeStartOffset(startOffset),
		kgo.ConsumeResetOffset(startOffset),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(time.Second),
		kgo.BlockRebalanceOnPoll(),
		kgo.AdjustFetchOffsetsFn(func(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			return watermarks.rewind(offsets, config.TemplateReplayRecords), nil
		}),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			stats.assign(partitions)
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, client *kgo.Client, partitions map[string][]int32) {
			if err := client.CommitMarkedOffsets(ctx); err != nil && onError != nil {
				onError(fmt.Errorf("commit revoked Kafka partitions: %w", err))
			}
			stats.revoke(partitions, false)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			stats.revoke(partitions, true)
		}),
	)
	if err := kgo.ValidateOpts(opts...); err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return newConsumerWithClientWatermarksAndStats(client, watermarks, stats, onError), nil
}

func rewindAssignedOffsets(offsets map[string]map[int32]kgo.Offset, records int64) map[string]map[int32]kgo.Offset {
	rewound := make(map[string]map[int32]kgo.Offset, len(offsets))
	for topic, partitions := range offsets {
		rewound[topic] = make(map[int32]kgo.Offset, len(partitions))
		for partition, offset := range partitions {
			// Negative positions are the broker's start/end/reset sentinels and
			// mean there is no committed absolute offset to rewind.
			if offset.EpochOffset().Offset >= 0 {
				offset = offset.WithEpoch(-1).Relative(-records)
			}
			rewound[topic][partition] = offset
		}
	}
	return rewound
}

func (w *replayWatermarks) rewind(offsets map[string]map[int32]kgo.Offset, records int64) map[string]map[int32]kgo.Offset {
	if w == nil {
		return rewindAssignedOffsets(offsets, records)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for topic, partitions := range offsets {
		for partition, offset := range partitions {
			position := offset.EpochOffset().Offset
			key := replayPartition{topic: topic, partition: partition}
			if position >= 0 {
				w.floors[key] = position
			} else {
				delete(w.floors, key)
			}
		}
	}
	return rewindAssignedOffsets(offsets, records)
}

func (w *replayWatermarks) committable(record *kgo.Record) bool {
	if w == nil || record == nil {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	key := replayPartition{topic: record.Topic, partition: record.Partition}
	floor, exists := w.floors[key]
	if !exists {
		return true
	}
	if record.Offset+1 < floor {
		return false
	}
	delete(w.floors, key)
	return true
}

func newConsumerWithClient(client consumerClient, onError func(error)) *Consumer {
	return newConsumerWithClientAndWatermarks(client, nil, onError)
}

func newConsumerWithClientAndWatermarks(client consumerClient, watermarks *replayWatermarks, onError func(error)) *Consumer {
	return newConsumerWithClientWatermarksAndStats(client, watermarks, newConsumerStats(), onError)
}

func newConsumerWithClientWatermarksAndStats(client consumerClient, watermarks *replayWatermarks, stats *consumerStats, onError func(error)) *Consumer {
	return &Consumer{client: client, replayWatermarks: watermarks, stats: stats, onError: onError}
}

func (c *Consumer) Ping(ctx context.Context) error {
	if c == nil || c.client == nil {
		return errors.New("Kafka consumer is not initialized")
	}
	return c.client.Ping(ctx)
}

func (c *Consumer) Run(ctx context.Context, handler RecordHandler) error {
	if handler == nil {
		return errors.New("Kafka record handler is required")
	}
	return c.run(ctx, func(ctx context.Context, records []*kgo.Record) (int, error) {
		for index, record := range records {
			if err := handler(ctx, record); err != nil {
				return index, err
			}
		}
		return len(records), nil
	})
}

// RunPartitionBatches preserves order within each partition and lets the
// durable sink write a whole Kafka fetch as one ClickHouse batch. An error
// leaves every record in that partition batch unmarked for replay.
func (c *Consumer) RunPartitionBatches(ctx context.Context, handler PartitionBatchHandler) error {
	if handler == nil {
		return errors.New("Kafka partition batch handler is required")
	}
	return c.run(ctx, func(ctx context.Context, records []*kgo.Record) (int, error) {
		if err := handler(ctx, records); err != nil {
			return 0, err
		}
		return len(records), nil
	})
}

func (c *Consumer) run(ctx context.Context, handler partitionHandler) error {
	if c == nil || c.client == nil {
		return errors.New("Kafka consumer is not initialized")
	}
	if handler == nil {
		return errors.New("Kafka partition handler is required")
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Close(closeCtx); err != nil && c.onError != nil {
			c.onError(err)
		}
	}()

	for {
		pollStarted := time.Now()
		fetches := c.client.PollFetches(ctx)
		c.stats.polls.Add(1)
		c.stats.pollDurationNanos.Add(uint64(time.Since(pollStarted)))
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			errorsFound := make([]error, 0, len(fetchErrors))
			for _, fetchError := range fetchErrors {
				errorsFound = append(errorsFound, fmt.Errorf("fetch %s[%d]: %w", fetchError.Topic, fetchError.Partition, fetchError.Err))
			}
			c.stats.errors.Add(uint64(len(errorsFound)))
			c.client.AllowRebalance()
			return errors.Join(errorsFound...)
		}
		if err := c.processFetches(ctx, fetches, handler); err != nil {
			c.stats.errors.Add(1)
			c.client.AllowRebalance()
			return err
		}
		c.client.AllowRebalance()
	}
}

func (c *Consumer) processFetches(ctx context.Context, fetches kgo.Fetches, handler partitionHandler) error {
	type partitionKey struct {
		topic     string
		partition int32
	}
	type partitionWork struct {
		records       []*kgo.Record
		highWatermark int64
	}
	indexByPartition := make(map[partitionKey]int)
	work := make([]partitionWork, 0)
	for _, fetch := range fetches {
		for _, topic := range fetch.Topics {
			for _, partition := range topic.Partitions {
				key := partitionKey{topic: topic.Topic, partition: partition.Partition}
				index, exists := indexByPartition[key]
				if !exists {
					index = len(work)
					indexByPartition[key] = index
					work = append(work, partitionWork{highWatermark: partition.HighWatermark})
				}
				if partition.HighWatermark > work[index].highWatermark {
					work[index].highWatermark = partition.HighWatermark
				}
				work[index].records = append(work[index].records, partition.Records...)
			}
		}
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsByPartition := make([]error, len(work))
	var wait sync.WaitGroup
	for index := range work {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			records := work[index].records
			if len(records) == 0 || workCtx.Err() != nil {
				return
			}
			processed, err := handler(workCtx, records)
			if processed < 0 || processed > len(records) {
				err = errors.Join(err, fmt.Errorf("handler returned invalid processed count %d for %d records", processed, len(records)))
				processed = 0
			}
			var lastCommittable *kgo.Record
			for _, record := range records[:processed] {
				if !c.replayWatermarks.committable(record) {
					continue
				}
				lastCommittable = record
				c.client.MarkCommitRecords(record)
				c.stats.records.Add(1)
				c.stats.bytes.Add(uint64(len(record.Value)))
			}
			if lastCommittable != nil {
				c.stats.observeLag(replayPartition{topic: lastCommittable.Topic, partition: lastCommittable.Partition}, work[index].highWatermark, lastCommittable.Offset+1)
			}
			if err != nil {
				failedIndex := processed
				if failedIndex == len(records) {
					failedIndex--
				}
				failed := records[failedIndex]
				errorsByPartition[index] = fmt.Errorf("process %s[%d] offset %d: %w", failed.Topic, failed.Partition, failed.Offset, err)
				cancel()
			}
		}()
	}
	wait.Wait()
	return errors.Join(errorsByPartition...)
}

func (c *Consumer) Stats() ConsumerStats {
	if c == nil || c.stats == nil {
		return ConsumerStats{}
	}
	stats := ConsumerStats{
		Records:           c.stats.records.Load(),
		Bytes:             c.stats.bytes.Load(),
		Errors:            c.stats.errors.Load(),
		Rebalances:        c.stats.rebalances.Load(),
		LostPartitions:    c.stats.lostPartitions.Load(),
		Polls:             c.stats.polls.Load(),
		PollDurationNanos: c.stats.pollDurationNanos.Load(),
	}
	c.stats.partitionsMu.Lock()
	defer c.stats.partitionsMu.Unlock()
	stats.AssignedPartitions = uint64(len(c.stats.partitions))
	for _, partition := range c.stats.partitions {
		if partition.known {
			stats.LagKnownPartitions++
			stats.LagRecords += partition.lag
		}
	}
	return stats
}

func newConsumerStats() *consumerStats {
	return &consumerStats{partitions: make(map[replayPartition]partitionRuntime)}
}

func (s *consumerStats) assign(partitions map[string][]int32) {
	if s == nil {
		return
	}
	s.rebalances.Add(1)
	s.partitionsMu.Lock()
	defer s.partitionsMu.Unlock()
	for topic, ids := range partitions {
		for _, partition := range ids {
			key := replayPartition{topic: topic, partition: partition}
			if _, exists := s.partitions[key]; !exists {
				s.partitions[key] = partitionRuntime{}
			}
		}
	}
}

func (s *consumerStats) revoke(partitions map[string][]int32, lost bool) {
	if s == nil {
		return
	}
	s.partitionsMu.Lock()
	defer s.partitionsMu.Unlock()
	for topic, ids := range partitions {
		for _, partition := range ids {
			delete(s.partitions, replayPartition{topic: topic, partition: partition})
			if lost {
				s.lostPartitions.Add(1)
			}
		}
	}
}

func (s *consumerStats) observeLag(key replayPartition, highWatermark, nextOffset int64) {
	if s == nil {
		return
	}
	lag := int64(0)
	if highWatermark > nextOffset {
		lag = highWatermark - nextOffset
	}
	s.partitionsMu.Lock()
	defer s.partitionsMu.Unlock()
	s.partitions[key] = partitionRuntime{lag: uint64(lag), known: true}
}

func (c *Consumer) Close(ctx context.Context) error {
	if c == nil || c.client == nil {
		return nil
	}
	var commitErr error
	c.once.Do(func() {
		commitErr = c.client.CommitMarkedOffsets(ctx)
		c.client.CloseAllowingRebalance()
	})
	if commitErr != nil {
		return fmt.Errorf("commit Kafka offsets during shutdown: %w", commitErr)
	}
	return nil
}
