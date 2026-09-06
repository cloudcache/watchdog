// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowworker"
)

func TestCompareIngestAuditMatchesWriterReceiptChecksum(t *testing.T) {
	now := time.Date(2026, 9, 6, 1, 2, 3, 0, time.UTC)
	first := testEnrichedRecord(1, 100, 1_000)
	first.EventTime = now
	first.RawPackets = 2
	first.EstimatedPackets = 20
	first.QualityFlags = 7
	first.DimensionFingerprint = 11
	first.ClassificationVersion = 3
	second := testEnrichedRecord(2, 200, 9_999)
	second.EventTime = now.Add(time.Second)
	second.RawPackets = 3
	second.EstimatedValid = false
	second.EstimatedPackets = 999
	second.QualityFlags = 13
	second.DimensionFingerprint = 17
	second.ClassificationVersion = 4
	batch := testEnrichedBatch(10, first, second)
	batch.ReceivedAt = now.Add(2 * time.Second)
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{batch}, BatchLimits{})
	if err != nil {
		t.Fatal(err)
	}
	receipt, facts := auditFixtureFromBlock(t, blocks[0])
	comparison, err := CompareIngestAudit([]IngestAuditReceipt{receipt}, facts, ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10})
	if err != nil {
		t.Fatal(err)
	}
	if comparison.Batches != 1 || comparison.Facts != 2 || len(comparison.Mismatches) != 0 {
		t.Fatalf("comparison=%+v", comparison)
	}
	if receipt.Counters.EstimatedBytes != first.EstimatedBytes || receipt.Counters.EstimatedPackets != first.EstimatedPackets || receipt.Counters.EstimatedValidRecords != 1 {
		t.Fatalf("invalid-estimate fact changed receipt counters: %+v", receipt.Counters)
	}
}

func TestCompareIngestAuditMismatchPriority(t *testing.T) {
	receipt, facts := simpleAuditFixture(t)
	for _, test := range []struct {
		name    string
		prepare func(*[]IngestAuditReceipt, *[]IngestAuditFact)
		want    ReconciliationMismatchReason
	}{
		{name: "missing receipt", prepare: func(receipts *[]IngestAuditReceipt, _ *[]IngestAuditFact) { *receipts = nil }, want: MismatchMissingReceipt},
		{name: "missing records", prepare: func(_ *[]IngestAuditReceipt, facts *[]IngestAuditFact) { *facts = nil }, want: MismatchMissingRecords},
		{name: "identity before counters", prepare: func(receipts *[]IngestAuditReceipt, facts *[]IngestAuditFact) {
			(*facts)[0].KafkaTopic = "other-v1"
			(*receipts)[0].Counters.RawBytes++
		}, want: MismatchIdentity},
		{name: "count before counters", prepare: func(receipts *[]IngestAuditReceipt, _ *[]IngestAuditFact) {
			(*receipts)[0].Counters.RecordCount++
			(*receipts)[0].Counters.RawBytes++
		}, want: MismatchCount},
		{name: "counter before checksum", prepare: func(receipts *[]IngestAuditReceipt, _ *[]IngestAuditFact) {
			(*receipts)[0].Counters.RawBytes++
			(*receipts)[0].Checksum[0]++
		}, want: MismatchCounter},
		{name: "checksum", prepare: func(receipts *[]IngestAuditReceipt, _ *[]IngestAuditFact) { (*receipts)[0].Checksum[0]++ }, want: MismatchChecksum},
	} {
		t.Run(test.name, func(t *testing.T) {
			testReceipts := append([]IngestAuditReceipt(nil), receipt...)
			testFacts := append([]IngestAuditFact(nil), facts...)
			test.prepare(&testReceipts, &testFacts)
			comparison, err := CompareIngestAudit(testReceipts, testFacts, ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(comparison.Mismatches) != 1 || comparison.Mismatches[0].Reason != test.want {
				t.Fatalf("mismatches=%+v want=%s", comparison.Mismatches, test.want)
			}
		})
	}
}

func TestCompareIngestAuditRejectsInvalidOrUnboundedInputs(t *testing.T) {
	receipts, facts := simpleAuditFixture(t)
	for _, limits := range []ReconciliationCompareLimits{{}, {MaxBatches: hardReconcileBatches + 1, MaxFacts: 1}, {MaxBatches: 1, MaxFacts: hardReconcileFacts + 1}} {
		if _, err := CompareIngestAudit(receipts, facts, limits); err == nil {
			t.Fatalf("limits %+v accepted", limits)
		}
	}
	if _, err := CompareIngestAudit(receipts, facts, ReconciliationCompareLimits{MaxBatches: 1, MaxFacts: 1}); err == nil || !strings.Contains(err.Error(), "budget") {
		t.Fatalf("fact budget error=%v", err)
	}
	duplicateReceipts := append(receipts, receipts[0])
	if _, err := CompareIngestAudit(duplicateReceipts, facts, ReconciliationCompareLimits{MaxBatches: 2, MaxFacts: 10}); err == nil || !strings.Contains(err.Error(), "duplicate ingest receipt") {
		t.Fatalf("duplicate receipt error=%v", err)
	}
	duplicateFacts := append(facts, facts[0])
	if _, err := CompareIngestAudit(receipts, duplicateFacts, ReconciliationCompareLimits{MaxBatches: 2, MaxFacts: 10}); err == nil || !strings.Contains(err.Error(), "duplicate generation-deduplicated") {
		t.Fatalf("duplicate fact error=%v", err)
	}
	overflowFacts := append([]IngestAuditFact(nil), facts...)
	overflowFacts[0].RawBytes = math.MaxUint64
	overflowFacts[1].RawBytes = 1
	if _, err := CompareIngestAudit(receipts, overflowFacts, ReconciliationCompareLimits{MaxBatches: 2, MaxFacts: 10}); err == nil || !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("overflow error=%v", err)
	}
}

func simpleAuditFixture(t testing.TB) ([]IngestAuditReceipt, []IngestAuditFact) {
	t.Helper()
	now := time.Date(2026, 9, 6, 2, 0, 0, 0, time.UTC)
	first := testEnrichedRecord(3, 100, 1_000)
	first.EventTime = now
	first.RawPackets = 1
	second := testEnrichedRecord(4, 200, 2_000)
	second.EventTime = now.Add(time.Second)
	second.RawPackets = 2
	batch := testEnrichedBatch(20, first, second)
	batch.ReceivedAt = now.Add(2 * time.Second)
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{batch}, BatchLimits{})
	if err != nil {
		t.Fatal(err)
	}
	receipt, facts := auditFixtureFromBlock(t, blocks[0])
	return []IngestAuditReceipt{receipt}, facts
}

func auditFixtureFromBlock(t testing.TB, block PreparedBlock) (IngestAuditReceipt, []IngestAuditFact) {
	t.Helper()
	receipt := IngestAuditReceipt{
		BatchID: block.ID, KafkaTopic: block.KafkaTopic, KafkaPartition: uint32(block.KafkaPartition),
		FirstOffset: uint64(block.FirstOffset), LastOffset: uint64(block.LastOffset), Checksum: block.Checksum,
		Counters: IngestAuditCounters{
			SourceBatchCount: block.SourceBatchCount, RecordCount: uint64(len(block.Records)), RawBytes: block.RawBytes, RawPackets: block.RawPackets,
			EstimatedBytes: block.EstimatedBytes, EstimatedPackets: block.EstimatedPackets, EstimatedValidRecords: block.EstimatedValidRecords,
		},
	}
	facts := make([]IngestAuditFact, 0, len(block.Records))
	for _, ref := range block.Records {
		facts = append(facts, IngestAuditFact{
			RecordID: ref.Record.SourceRecordID, BatchID: block.ID, KafkaTopic: ref.Batch.KafkaTopic, KafkaPartition: uint32(ref.Batch.KafkaPartition),
			KafkaOffset: uint64(ref.Batch.KafkaOffset), RecordIndex: ref.Record.RecordIndex,
			RawBytes: ref.Record.RawBytes, RawPackets: ref.Record.RawPackets, EstimatedValid: ref.Record.EstimatedValid,
			EstimatedBytes: ref.Record.EstimatedBytes, EstimatedPackets: ref.Record.EstimatedPackets, QualityFlags: ref.Record.QualityFlags,
			DimensionFingerprint: ref.Record.DimensionFingerprint, ClassificationVersion: ref.Record.ClassificationVersion,
		})
	}
	if checksum := ingestAuditChecksum(facts); checksum != block.Checksum {
		t.Fatalf("audit checksum=%x writer checksum=%x", checksum[:8], block.Checksum[:8])
	}
	return receipt, facts
}
