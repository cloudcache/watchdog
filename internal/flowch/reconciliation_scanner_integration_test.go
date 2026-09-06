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

const reconcileTopic = "watchdog.flow.raw-v1"

func reconcileScanRequest(start, close uint64, maxBatches, maxFactRows int) ReconciliationScanRequest {
	return ReconciliationScanRequest{
		Cursor:        ReconciliationScanCursor{KafkaTopic: reconcileTopic, KafkaPartition: 3, NextOffset: start},
		CloseOffset:   close,
		MaxBatches:    maxBatches,
		MaxFactRows:   maxFactRows,
		MaxReadBytes:  256 << 20,
		CompareLimits: ReconciliationCompareLimits{MaxBatches: maxBatches, MaxFacts: maxFactRows},
	}
}

func insertReconcileBlock(t *testing.T, ctx context.Context, native *NativeInserter, offset int64) PreparedBlock {
	t.Helper()
	received := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	// Distinct record ids per offset: testEnrichedRecord derives record_id from
	// the index, so reused indices would collide across offsets and the argMax
	// dedup would fold them together — real records are unique per offset.
	batch := integrationBatch(offset, received,
		integrationRecord(byte(offset*2+1), received.Add(10*time.Second), "330100", 100),
		integrationRecord(byte(offset*2+2), received.Add(20*time.Second), "330100", 200),
	)
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
	return IngestAuditCounters{
		SourceBatchCount: block.SourceBatchCount, RecordCount: recordCount,
		RawBytes: block.RawBytes, RawPackets: block.RawPackets,
		EstimatedBytes: block.EstimatedBytes, EstimatedPackets: block.EstimatedPackets,
		EstimatedValidRecords: block.EstimatedValidRecords,
	}
}

// insertReconcileReceipt writes one receipt row directly so a test can force a
// receipt/fact disagreement (higher generation wins the ReplacingMergeTree).
func insertReconcileReceipt(t *testing.T, ctx context.Context, native *NativeInserter, receipt IngestAuditReceipt, generation uint64) {
	t.Helper()
	eventTime := time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC)
	var (
		batchID          proto.ColFixedStr32
		workerSchema     proto.ColUInt32
		receiptSchema    proto.ColUInt16
		tenantIDs        = new(proto.ColStr).Array()
		kafkaTopic       = new(proto.ColStr).LowCardinality()
		kafkaPartition   proto.ColUInt32
		firstOffset      proto.ColUInt64
		lastOffset       proto.ColUInt64
		sourceBatchCount proto.ColUInt32
		recordCount      proto.ColUInt64
		rawBytes         proto.ColUInt64
		rawPackets       proto.ColUInt64
		estimatedBytes   proto.ColUInt64
		estimatedPackets proto.ColUInt64
		estimatedValid   proto.ColUInt64
		minEventTime     = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		maxEventTime     = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		checksum         proto.ColFixedStr32
		generationCol    proto.ColUInt64
		insertedAt       = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	)
	batchID.Append(receipt.BatchID)
	workerSchema.Append(WorkerSchemaVersion)
	receiptSchema.Append(receiptSchemaVersion)
	tenantIDs.Append([]string{"tenant-a"})
	kafkaTopic.Append(receipt.KafkaTopic)
	kafkaPartition.Append(receipt.KafkaPartition)
	firstOffset.Append(receipt.FirstOffset)
	lastOffset.Append(receipt.LastOffset)
	sourceBatchCount.Append(receipt.Counters.SourceBatchCount)
	recordCount.Append(receipt.Counters.RecordCount)
	rawBytes.Append(receipt.Counters.RawBytes)
	rawPackets.Append(receipt.Counters.RawPackets)
	estimatedBytes.Append(receipt.Counters.EstimatedBytes)
	estimatedPackets.Append(receipt.Counters.EstimatedPackets)
	estimatedValid.Append(receipt.Counters.EstimatedValidRecords)
	minEventTime.Append(eventTime)
	maxEventTime.Append(eventTime)
	checksum.Append(receipt.Checksum)
	generationCol.Append(generation)
	insertedAt.Append(eventTime)
	input := proto.Input{
		{Name: "ingest_batch_id", Data: batchID}, {Name: "worker_schema", Data: workerSchema},
		{Name: "receipt_schema", Data: receiptSchema}, {Name: "tenant_ids", Data: tenantIDs},
		{Name: "kafka_topic", Data: kafkaTopic}, {Name: "kafka_partition", Data: kafkaPartition},
		{Name: "first_offset", Data: firstOffset}, {Name: "last_offset", Data: lastOffset},
		{Name: "source_batch_count", Data: sourceBatchCount}, {Name: "record_count", Data: recordCount},
		{Name: "raw_bytes", Data: rawBytes}, {Name: "raw_packets", Data: rawPackets},
		{Name: "estimated_bytes", Data: estimatedBytes}, {Name: "estimated_packets", Data: estimatedPackets},
		{Name: "estimated_valid_records", Data: estimatedValid}, {Name: "min_event_time", Data: minEventTime},
		{Name: "max_event_time", Data: maxEventTime}, {Name: "checksum", Data: checksum},
		{Name: "generation", Data: generationCol}, {Name: "inserted_at", Data: insertedAt},
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

	t.Run("clean window has no mismatches and completes", func(t *testing.T) {
		for _, offset := range []int64{10, 11, 12} {
			insertReconcileBlock(t, ctx, native, offset)
		}
		result, err := scanner.Scan(ctx, reconcileScanRequest(10, 13, 100, 10_000))
		if err != nil {
			t.Fatal(err)
		}
		if !result.Complete || result.NextCursor.NextOffset != 13 {
			t.Fatalf("clean scan complete=%v next=%d", result.Complete, result.NextCursor.NextOffset)
		}
		if result.ReceiptBatches != 3 || result.CandidateBatches != 3 || len(result.Comparison.Mismatches) != 0 {
			t.Fatalf("clean scan = %+v mismatches=%d", result, len(result.Comparison.Mismatches))
		}
	})

	t.Run("counter mismatch is reported without fetching facts", func(t *testing.T) {
		block := insertReconcileBlock(t, ctx, native, 20)
		// A newer receipt for the same batch overstates the record count while its
		// facts stay unchanged; the higher generation wins FINAL.
		insertReconcileReceipt(t, ctx, native, IngestAuditReceipt{
			BatchID: block.ID, KafkaTopic: reconcileTopic, KafkaPartition: 3,
			FirstOffset: uint64(block.FirstOffset), LastOffset: uint64(block.LastOffset), Checksum: block.Checksum,
			Counters: blockReceiptCounters(block, 999),
		}, 9_000_000_000_000)
		result, err := scanner.Scan(ctx, reconcileScanRequest(20, 21, 100, 10_000))
		if err != nil {
			t.Fatal(err)
		}
		if !result.Complete {
			t.Fatalf("bounded window should complete: %+v", result)
		}
		if len(result.Comparison.Mismatches) != 1 || result.Comparison.Mismatches[0].Reason != MismatchCount {
			t.Fatalf("want one count mismatch, got %+v", result.Comparison.Mismatches)
		}
		if result.CandidateBatches != 0 {
			t.Fatalf("a counter-dirty batch must not become a checksum candidate: %+v", result)
		}
	})

	t.Run("receipt with no facts is missing_records", func(t *testing.T) {
		insertReconcileReceipt(t, ctx, native, IngestAuditReceipt{
			BatchID: [32]byte{0xCC, 0x64}, KafkaTopic: reconcileTopic, KafkaPartition: 3,
			FirstOffset: 100, LastOffset: 100, Checksum: [32]byte{0xDD},
			Counters: IngestAuditCounters{SourceBatchCount: 1, RecordCount: 4, RawBytes: 400, EstimatedValidRecords: 2},
		}, 9_000_000_000_000)
		result, err := scanner.Scan(ctx, reconcileScanRequest(100, 101, 100, 10_000))
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Comparison.Mismatches) != 1 || result.Comparison.Mismatches[0].Reason != MismatchMissingRecords {
			t.Fatalf("want one missing_records mismatch, got %+v", result.Comparison.Mismatches)
		}
	})

	t.Run("batch budget cut is incomplete and advances one batch", func(t *testing.T) {
		for _, offset := range []int64{50, 51, 52} {
			insertReconcileBlock(t, ctx, native, offset)
		}
		cursorOffset := uint64(50)
		for _, want := range []struct {
			nextOffset uint64
			complete   bool
		}{{51, false}, {52, false}, {53, true}} {
			result, err := scanner.Scan(ctx, reconcileScanRequest(cursorOffset, 53, 1, 10_000))
			if err != nil {
				t.Fatal(err)
			}
			if result.ReceiptBatches != 1 || result.NextCursor.NextOffset != want.nextOffset || result.Complete != want.complete {
				t.Fatalf("budget step from %d = receipts=%d next=%d complete=%v, want next=%d complete=%v",
					cursorOffset, result.ReceiptBatches, result.NextCursor.NextOffset, result.Complete, want.nextOffset, want.complete)
			}
			if len(result.Comparison.Mismatches) != 0 {
				t.Fatalf("clean budget step reported mismatches: %+v", result.Comparison.Mismatches)
			}
			cursorOffset = result.NextCursor.NextOffset
		}
	})
}
