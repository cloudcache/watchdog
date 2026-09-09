// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

const (
	reconcileTopic  = "watchdog.flow.raw-v1"
	reconcileStream = "cluster-a:raw-v1:incarnation-1"
)

func reconcileScanRequest(start, close uint64, maxBatches, maxFactRows int) ReconciliationScanRequest {
	return ReconciliationScanRequest{
		Cursor:      ReconciliationScanCursor{SourceStreamID: reconcileStream, KafkaTopic: reconcileTopic, KafkaPartition: 3, NextOffset: start},
		CloseOffset: close, MaxBatches: maxBatches, MaxFactRows: maxFactRows, MaxReadBytes: 256 << 20,
		CompareLimits: ReconciliationCompareLimits{MaxBatches: maxBatches, MaxFacts: maxFactRows},
	}
}

func insertReconcileBlock(t *testing.T, ctx context.Context, native *NativeInserter, offset int64) PreparedBlock {
	t.Helper()
	received := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	first := integrationRecord(0, received.Add(10*time.Second), "330100", 100)
	second := integrationRecord(1, received.Add(20*time.Second), "330100", 200)
	batch := integrationBatch(offset, received, first, second)
	batch.SourceStreamID = reconcileStream
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{batch}, BatchLimits{})
	if err != nil || len(blocks) != 1 {
		t.Fatalf("prepare reconcile block: blocks=%d err=%v", len(blocks), err)
	}
	if err := native.InsertFlowBlock(ctx, blocks[0]); err != nil {
		t.Fatalf("insert reconcile block: %v", err)
	}
	return blocks[0]
}

func blockReceiptCounters(block PreparedBlock, recordCount uint64) IngestAuditCounters {
	receipt := block.Receipts[0]
	return IngestAuditCounters{RecordCount: recordCount, RawBytes: receipt.RawBytes, RawPackets: receipt.RawPackets,
		EstimatedBytes: receipt.EstimatedBytes, EstimatedPackets: receipt.EstimatedPackets, EstimatedValidRecords: receipt.EstimatedValidRecords}
}

// insertReconcileReceipt writes a newer receipt directly so a test can force a
// receipt/fact disagreement under ReplacingMergeTree FINAL.
func insertReconcileReceipt(t *testing.T, ctx context.Context, native *NativeInserter, receipt IngestAuditReceipt, generation uint64) {
	t.Helper()
	eventTime := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	var (
		sourceStreamID   = new(proto.ColStr).LowCardinality()
		workerSchema     proto.ColUInt32
		receiptSchema    proto.ColUInt16
		disposition      proto.ColEnum
		kafkaTopic       = new(proto.ColStr).LowCardinality()
		kafkaPartition   proto.ColUInt32
		kafkaOffset      proto.ColUInt64
		recordCount      proto.ColUInt64
		rawBytes         proto.ColUInt64
		rawPackets       proto.ColUInt64
		estimatedBytes   proto.ColUInt64
		estimatedPackets proto.ColUInt64
		estimatedValid   proto.ColUInt64
		minEventTime     = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		maxEventTime     = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		generationCol    proto.ColUInt64
		insertedAt       = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	)
	sourceStreamID.Append(receipt.SourceStreamID)
	workerSchema.Append(WorkerSchemaVersion)
	receiptSchema.Append(receiptSchemaVersion)
	disposition.Append(string(receipt.Disposition))
	kafkaTopic.Append(receipt.KafkaTopic)
	kafkaPartition.Append(receipt.KafkaPartition)
	kafkaOffset.Append(receipt.KafkaOffset)
	recordCount.Append(receipt.Counters.RecordCount)
	rawBytes.Append(receipt.Counters.RawBytes)
	rawPackets.Append(receipt.Counters.RawPackets)
	estimatedBytes.Append(receipt.Counters.EstimatedBytes)
	estimatedPackets.Append(receipt.Counters.EstimatedPackets)
	estimatedValid.Append(receipt.Counters.EstimatedValidRecords)
	minEventTime.Append(eventTime)
	maxEventTime.Append(eventTime)
	generationCol.Append(generation)
	insertedAt.Append(eventTime)
	input := proto.Input{
		{Name: "source_stream_id", Data: sourceStreamID}, {Name: "worker_schema", Data: workerSchema},
		{Name: "receipt_schema", Data: receiptSchema}, {Name: "message_disposition", Data: &disposition},
		{Name: "kafka_topic", Data: kafkaTopic}, {Name: "kafka_partition", Data: kafkaPartition}, {Name: "kafka_offset", Data: kafkaOffset},
		{Name: "record_count", Data: recordCount}, {Name: "raw_bytes", Data: rawBytes}, {Name: "raw_packets", Data: rawPackets},
		{Name: "estimated_bytes", Data: estimatedBytes}, {Name: "estimated_packets", Data: estimatedPackets},
		{Name: "estimated_valid_records", Data: estimatedValid}, {Name: "min_event_time", Data: minEventTime},
		{Name: "max_event_time", Data: maxEventTime}, {Name: "generation", Data: generationCol}, {Name: "inserted_at", Data: insertedAt},
	}
	if err := native.executor.Do(ctx, ch.Query{Body: input.Into(flowReceiptsTable), Input: input}); err != nil {
		t.Fatalf("insert reconciliation receipt: %v", err)
	}
}

func TestRealClickHouseReconciliationScannerDetectsAndBounds(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_reconcile")
	scanner, err := NewReconciliationScanner(native)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("clean window", func(t *testing.T) {
		for _, offset := range []int64{10, 11, 12} {
			insertReconcileBlock(t, ctx, native, offset)
		}
		result, err := scanner.Scan(ctx, reconcileScanRequest(10, 13, 100, 10_000))
		if err != nil {
			t.Fatal(err)
		}
		if !result.Complete || result.NextCursor.NextOffset != 13 || result.ReceiptBatches != 3 || result.CandidateBatches != 0 || len(result.Comparison.Mismatches) != 0 {
			t.Fatalf("clean scan=%+v", result)
		}
	})

	t.Run("count mismatch", func(t *testing.T) {
		block := insertReconcileBlock(t, ctx, native, 20)
		insertReconcileReceipt(t, ctx, native, IngestAuditReceipt{
			SourceMessageKey: SourceMessageKey{SourceStreamID: reconcileStream, KafkaPartition: 3, KafkaOffset: 20},
			KafkaTopic:       reconcileTopic, Disposition: IngestDispositionPersisted, Counters: blockReceiptCounters(block, 999),
		}, 9_000_000_000_000)
		result, err := scanner.Scan(ctx, reconcileScanRequest(20, 21, 100, 10_000))
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Comparison.Mismatches) != 1 || result.Comparison.Mismatches[0].Reason != MismatchCount {
			t.Fatalf("mismatches=%+v", result.Comparison.Mismatches)
		}
	})

	t.Run("receipt without facts", func(t *testing.T) {
		insertReconcileReceipt(t, ctx, native, IngestAuditReceipt{
			SourceMessageKey: SourceMessageKey{SourceStreamID: reconcileStream, KafkaPartition: 3, KafkaOffset: 100},
			KafkaTopic:       reconcileTopic, Disposition: IngestDispositionPersisted, Counters: IngestAuditCounters{RecordCount: 4, RawBytes: 400},
		}, 9_000_000_000_000)
		result, err := scanner.Scan(ctx, reconcileScanRequest(100, 101, 100, 10_000))
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Comparison.Mismatches) != 1 || result.Comparison.Mismatches[0].Reason != MismatchMissingRecords {
			t.Fatalf("mismatches=%+v", result.Comparison.Mismatches)
		}
	})

	t.Run("committed offset missing both receipt and facts", func(t *testing.T) {
		result, err := scanner.Scan(ctx, reconcileScanRequest(105, 106, 100, 10_000))
		if err != nil {
			t.Fatal(err)
		}
		if !result.Complete || result.NextCursor.NextOffset != 106 || result.Comparison.Batches != 1 ||
			len(result.Comparison.Mismatches) != 1 || result.Comparison.Mismatches[0].Reason != MismatchMissingReceipt ||
			result.Comparison.Mismatches[0].KafkaTopic != reconcileTopic {
			t.Fatalf("fully missing committed offset scan=%+v", result)
		}
	})

	t.Run("non-persisted message is auditable without facts", func(t *testing.T) {
		batch := integrationBatch(110, time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC))
		batch.SourceStreamID = reconcileStream
		batch.MessageDisposition = flowworker.MessageDispositionTemplateMissing
		batch.CollectorID, batch.ExporterID = "", ""
		blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{batch}, BatchLimits{})
		if err != nil || len(blocks) != 1 {
			t.Fatalf("prepare receipt-only block: blocks=%d err=%v", len(blocks), err)
		}
		if err := native.InsertFlowBlock(ctx, blocks[0]); err != nil {
			t.Fatal(err)
		}
		result, err := scanner.Scan(ctx, reconcileScanRequest(110, 111, 100, 10_000))
		if err != nil || len(result.Comparison.Mismatches) != 0 || result.ReceiptBatches != 1 || result.Facts != 0 {
			t.Fatalf("receipt-only scan=%+v err=%v", result, err)
		}
	})

	t.Run("message budget", func(t *testing.T) {
		for _, offset := range []int64{50, 51, 52} {
			insertReconcileBlock(t, ctx, native, offset)
		}
		cursor := uint64(50)
		for _, want := range []struct {
			next     uint64
			complete bool
		}{{51, false}, {52, false}, {53, true}} {
			result, err := scanner.Scan(ctx, reconcileScanRequest(cursor, 53, 1, 10_000))
			if err != nil {
				t.Fatal(err)
			}
			if result.ReceiptBatches != 1 || result.NextCursor.NextOffset != want.next || result.Complete != want.complete || len(result.Comparison.Mismatches) != 0 {
				t.Fatalf("budget scan=%+v want next=%d complete=%v", result, want.next, want.complete)
			}
			cursor = result.NextCursor.NextOffset
		}
	})
}
