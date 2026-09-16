// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type offsetCoverageExecutor struct {
	query ch.Query
	items []DayOffsetCoverage
}

func (executor *offsetCoverageExecutor) Do(ctx context.Context, query ch.Query) error {
	executor.query = query
	results, ok := query.Result.(proto.Results)
	if !ok || len(results) != 5 {
		return errors.New("unexpected offset coverage result contract")
	}
	streams, ok := results[0].Data.(*proto.ColLowCardinality[string])
	if !ok {
		return errors.New("unexpected stream column")
	}
	topics, ok := results[1].Data.(*proto.ColLowCardinality[string])
	if !ok {
		return errors.New("unexpected topic column")
	}
	partitions, ok := results[2].Data.(*proto.ColUInt32)
	if !ok {
		return errors.New("unexpected partition column")
	}
	first, ok := results[3].Data.(*proto.ColUInt64)
	if !ok {
		return errors.New("unexpected first offset column")
	}
	last, ok := results[4].Data.(*proto.ColUInt64)
	if !ok {
		return errors.New("unexpected last offset column")
	}
	for _, item := range executor.items {
		streams.Append(item.SourceStreamID)
		topics.Append(item.KafkaTopic)
		*partitions = append(*partitions, item.KafkaPartition)
		*first = append(*first, item.FirstOffset)
		*last = append(*last, item.LastOffsetExclusive)
	}
	return query.OnResult(ctx, proto.Block{Columns: 5, Rows: len(executor.items)})
}

func TestDayOffsetCoverageUsesReceiptEventWindowAndNaturalCoordinates(t *testing.T) {
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	want := []DayOffsetCoverage{
		{SourceStreamID: "site-a:boot-1", KafkaTopic: "watchdog.flow.raw", KafkaPartition: 0, FirstOffset: 10, LastOffsetExclusive: 21},
		{SourceStreamID: "site-a:boot-1", KafkaTopic: "watchdog.flow.raw", KafkaPartition: 1, FirstOffset: 30, LastOffsetExclusive: 32},
	}
	executor := &offsetCoverageExecutor{items: want}
	got, err := (&RollupRunner{executor: executor}).DayOffsetCoverage(context.Background(), day)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("coverage=%+v want=%+v", got, want)
	}
	for _, fragment := range []string{"FROM flow_ingest_receipts FINAL", "record_count > 0", "min_event_time <", "max_event_time >=", "GROUP BY source_stream_id,kafka_topic,kafka_partition"} {
		if !strings.Contains(executor.query.Body, fragment) {
			t.Fatalf("coverage query missing %q: %s", fragment, executor.query.Body)
		}
	}
	if strings.Contains(executor.query.Body, "flow_records") || strings.Contains(strings.ToLower(executor.query.Body), "hash") {
		t.Fatalf("coverage query must use receipts and natural coordinates: %s", executor.query.Body)
	}
}

func TestDayOffsetCoverageRejectsInvalidDayAndInvalidRows(t *testing.T) {
	runner := &RollupRunner{executor: &offsetCoverageExecutor{}}
	if _, err := runner.DayOffsetCoverage(context.Background(), time.Date(2026, 9, 15, 1, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("non-aligned day was accepted")
	}
	bad := &offsetCoverageExecutor{items: []DayOffsetCoverage{{SourceStreamID: "site-a", KafkaTopic: "raw", FirstOffset: 4, LastOffsetExclusive: 4}}}
	if _, err := (&RollupRunner{executor: bad}).DayOffsetCoverage(context.Background(), time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("empty offset interval was accepted")
	}
}
