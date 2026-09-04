package flowcollect

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

const collectStateFetchMaxWait = 250 * time.Millisecond

// scanSaramaCollectStateBoundary uses broker fetch responses directly because
// PartitionConsumer does not expose its consumed offset. A compacted topic may
// have no retained record at high-watermark-1, so waiting for that record would
// block until the restore timeout even though the frozen boundary was reached.
func scanSaramaCollectStateBoundary(ctx context.Context, client sarama.Client, topic string, boundary collectStatePartitionBoundary, emit func(*sarama.ConsumerMessage) error) error {
	if ctx == nil || client == nil || topic == "" || emit == nil || boundary.partition < 0 || boundary.oldest < 0 || boundary.newest < boundary.oldest {
		return errors.New("Kafka collect-state boundary scan configuration is invalid")
	}
	cursor := boundary.oldest
	for cursor < boundary.newest {
		if err := ctx.Err(); err != nil {
			return err
		}
		broker, leaderEpoch, err := client.LeaderAndEpoch(topic, boundary.partition)
		if err != nil {
			return fmt.Errorf("resolve Kafka collect-state leader: %w", err)
		}
		if broker == nil {
			return errors.New("Kafka collect-state partition has no leader")
		}
		maxWait := collectStateFetchMaxWait
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return context.DeadlineExceeded
			}
			if remaining < maxWait {
				maxWait = remaining
			}
		}
		maxWaitMillis := maxWait.Milliseconds()
		if maxWaitMillis < 1 {
			maxWaitMillis = 1
		}
		request := &sarama.FetchRequest{
			Version:     11,
			MaxWaitTime: int32(maxWaitMillis),
			MinBytes:    1,
			MaxBytes:    int32(collectStateMaxBytes + kafkaRecordOverheadBytes),
			Isolation:   sarama.ReadCommitted,
		}
		request.AddBlock(topic, boundary.partition, cursor, int32(collectStateMaxBytes+kafkaRecordOverheadBytes), leaderEpoch)
		response, err := broker.Fetch(request)
		if err != nil {
			return fmt.Errorf("fetch Kafka collect-state boundary at offset %d: %w", cursor, err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		block := response.GetBlock(topic, boundary.partition)
		messages, next, complete, err := decodeCollectStateFetchBlock(topic, boundary, cursor, block)
		if err != nil {
			return err
		}
		for _, message := range messages {
			if err := emit(message); err != nil {
				return err
			}
		}
		if complete {
			return nil
		}
		if next <= cursor {
			return fmt.Errorf("Kafka collect-state partition %d fetch made no progress at offset %d", boundary.partition, cursor)
		}
		cursor = next
	}
	return nil
}

func decodeCollectStateFetchBlock(topic string, boundary collectStatePartitionBoundary, cursor int64, block *sarama.FetchResponseBlock) ([]*sarama.ConsumerMessage, int64, bool, error) {
	if block == nil {
		return nil, cursor, false, errors.New("Kafka collect-state fetch response is incomplete")
	}
	if block.Err != sarama.ErrNoError {
		return nil, cursor, false, fmt.Errorf("Kafka collect-state fetch failed: %w", block.Err)
	}
	if block.HighWaterMarkOffset < boundary.newest {
		return nil, cursor, false, fmt.Errorf("Kafka collect-state high watermark regressed from %d to %d", boundary.newest, block.HighWaterMarkOffset)
	}
	if block.Partial {
		return nil, cursor, false, errors.New("Kafka collect-state fetch returned a partial record set")
	}
	next := cursor
	messages := make([]*sarama.ConsumerMessage, 0)
	lastWireOffset := int64(-1)
	appendMessage := func(message *sarama.ConsumerMessage) error {
		if message == nil || message.Offset < 0 || (lastWireOffset >= 0 && message.Offset <= lastWireOffset) {
			return errors.New("Kafka collect-state fetch returned invalid message order")
		}
		lastWireOffset = message.Offset
		if message.Offset < cursor {
			return nil
		}
		if message.Offset >= boundary.newest {
			next = boundary.newest
			return nil
		}
		message.Topic = topic
		message.Partition = boundary.partition
		messages = append(messages, message)
		if message.Offset+1 > next {
			next = message.Offset + 1
		}
		return nil
	}

	for _, records := range block.RecordsSet {
		if records == nil || (records.MsgSet == nil) == (records.RecordBatch == nil) {
			return nil, cursor, false, errors.New("Kafka collect-state fetch returned an invalid record-set type")
		}
		if records.MsgSet != nil {
			if records.MsgSet.PartialTrailingMessage || records.MsgSet.OverflowMessage {
				return nil, cursor, false, errors.New("Kafka collect-state fetch returned a partial legacy message set")
			}
			for _, outer := range records.MsgSet.Messages {
				if outer == nil || outer.Msg == nil {
					return nil, cursor, false, errors.New("Kafka collect-state fetch returned an invalid legacy message")
				}
				inner := outer.Messages()
				if len(inner) == 0 {
					return nil, cursor, false, errors.New("Kafka collect-state fetch returned an empty legacy message block")
				}
				for _, item := range inner {
					if item == nil || item.Msg == nil {
						return nil, cursor, false, errors.New("Kafka collect-state fetch returned an invalid legacy inner message")
					}
					offset := item.Offset
					timestamp := item.Msg.Timestamp
					if item.Msg.Version >= 1 {
						offset += outer.Offset - inner[len(inner)-1].Offset
						if item.Msg.LogAppendTime {
							timestamp = outer.Msg.Timestamp
						}
					}
					if err := appendMessage(&sarama.ConsumerMessage{Key: item.Msg.Key, Value: item.Msg.Value, Offset: offset, Timestamp: timestamp, BlockTimestamp: outer.Msg.Timestamp}); err != nil {
						return nil, cursor, false, err
					}
				}
			}
			continue
		}

		batch := records.RecordBatch
		if batch.PartialTrailingRecord || batch.FirstOffset < 0 || batch.LastOffsetDelta < 0 || batch.LastOffset() < batch.FirstOffset {
			return nil, cursor, false, errors.New("Kafka collect-state fetch returned an invalid record batch")
		}
		if !batch.Control && batch.IsTransactional {
			return nil, cursor, false, errors.New("Kafka collect-state topic contains unsupported transactional records")
		}
		if !batch.Control {
			for _, record := range batch.Records {
				if record == nil || record.OffsetDelta < 0 || record.OffsetDelta > int64(batch.LastOffsetDelta) {
					return nil, cursor, false, errors.New("Kafka collect-state fetch returned an invalid record")
				}
				timestamp := batch.FirstTimestamp.Add(record.TimestampDelta)
				if batch.LogAppendTime {
					timestamp = batch.MaxTimestamp
				}
				if err := appendMessage(&sarama.ConsumerMessage{Key: record.Key, Value: record.Value, Offset: batch.FirstOffset + record.OffsetDelta, Timestamp: timestamp, Headers: record.Headers}); err != nil {
					return nil, cursor, false, err
				}
			}
		}
		if batch.LastOffset()+1 > next {
			next = batch.LastOffset() + 1
		}
	}

	if block.LastRecordsBatchOffset != nil && *block.LastRecordsBatchOffset >= cursor && *block.LastRecordsBatchOffset+1 > next {
		next = *block.LastRecordsBatchOffset + 1
	}
	if block.LogStartOffset > next {
		next = block.LogStartOffset
	}
	if next >= boundary.newest {
		return messages, boundary.newest, true, nil
	}
	if len(block.RecordsSet) == 0 {
		// The leader answered a fetch at cursor with no retained record while its
		// high watermark covers the frozen boundary. Therefore every offset in
		// [cursor, boundary.newest) is a compacted hole, not an unread message.
		return messages, boundary.newest, true, nil
	}
	return messages, next, false, nil
}
