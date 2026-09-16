// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// DayOffsetCoverage is the natural Kafka-coordinate interval of receipts whose
// event-time range intersects one UTC raw-data day. It deliberately contains
// no derived hash: the durable reconciliation watermark proves the contiguous
// prefix that covers LastOffsetExclusive.
type DayOffsetCoverage struct {
	SourceStreamID      string
	KafkaTopic          string
	KafkaPartition      uint32
	FirstOffset         uint64
	LastOffsetExclusive uint64
}

// DayOffsetCoverage reads deletion evidence from source-message receipts, not
// from the raw rows being considered for deletion. This keeps the evidence
// tied to the Kafka acknowledgement boundary and includes messages containing
// records on both sides of midnight conservatively.
func (r *RollupRunner) DayOffsetCoverage(ctx context.Context, sourceDate time.Time) ([]DayOffsetCoverage, error) {
	if r == nil || r.executor == nil {
		return nil, Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	day := sourceDate.UTC()
	if day.IsZero() || day != day.Truncate(24*time.Hour) {
		return nil, Permanent(errors.New("offset coverage UTC-aligned source date is required"))
	}
	end := day.Add(24 * time.Hour)
	stream := new(proto.ColStr).LowCardinality()
	topic := new(proto.ColStr).LowCardinality()
	var partition proto.ColUInt32
	var firstOffset, lastOffsetExclusive proto.ColUInt64
	items := make([]DayOffsetCoverage, 0)
	query := ch.Query{
		Body: `SELECT source_stream_id,kafka_topic,kafka_partition,
  min(kafka_offset) AS first_offset,max(kafka_offset)+1 AS last_offset_exclusive
FROM flow_ingest_receipts FINAL
WHERE record_count > 0
  AND min_event_time < {end:DateTime('UTC')}
  AND max_event_time >= {start:DateTime('UTC')}
GROUP BY source_stream_id,kafka_topic,kafka_partition
ORDER BY source_stream_id,kafka_topic,kafka_partition`,
		Parameters: ch.Parameters(map[string]any{
			"start": day.Format("2006-01-02 15:04:05"),
			"end":   end.Format("2006-01-02 15:04:05"),
		}),
		Result: proto.Results{
			{Name: "source_stream_id", Data: stream}, {Name: "kafka_topic", Data: topic},
			{Name: "kafka_partition", Data: &partition}, {Name: "first_offset", Data: &firstOffset},
			{Name: "last_offset_exclusive", Data: &lastOffsetExclusive},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for row := 0; row < block.Rows; row++ {
			item := DayOffsetCoverage{
				SourceStreamID: stream.Row(row), KafkaTopic: topic.Row(row), KafkaPartition: partition[row],
				FirstOffset: firstOffset[row], LastOffsetExclusive: lastOffsetExclusive[row],
			}
			if item.SourceStreamID == "" || item.KafkaTopic == "" || item.FirstOffset >= item.LastOffsetExclusive {
				return Permanent(errors.New("ClickHouse returned invalid UTC-day Kafka offset coverage"))
			}
			items = append(items, item)
		}
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return nil, classifyClickHouseError(fmt.Errorf("read UTC-day Kafka offset coverage: %w", err))
	}
	return items, nil
}
