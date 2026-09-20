// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"math"
	"testing"
)

func TestCompareIngestAuditMessageReceiptAndCounters(t *testing.T) {
	receipts, facts := simpleAuditFixture()
	comparison, err := CompareIngestAudit(receipts, facts, ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10})
	if err != nil {
		t.Fatal(err)
	}
	if comparison.Batches != 1 || comparison.Facts != 2 || len(comparison.Mismatches) != 0 {
		t.Fatalf("clean comparison=%+v", comparison)
	}
}

func TestCompareIngestAuditAcceptsAuditableNonPersistedMessageWithoutFacts(t *testing.T) {
	receipt := IngestAuditReceipt{
		SourceMessageKey: SourceMessageKey{SourceStreamID: "stream-a", KafkaPartition: 1, KafkaOffset: 9},
		KafkaTopic:       "watchdog.flow.raw-v1", Disposition: IngestDispositionTemplateMissing,
	}
	comparison, err := CompareIngestAudit([]IngestAuditReceipt{receipt}, nil, ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10})
	if err != nil || len(comparison.Mismatches) != 0 || comparison.Batches != 1 {
		t.Fatalf("non-persisted comparison=%+v err=%v", comparison, err)
	}
	fact := IngestAuditFact{SourceMessageKey: receipt.SourceMessageKey, KafkaTopic: receipt.KafkaTopic, RecordIndex: 0}
	comparison, err = CompareIngestAudit([]IngestAuditReceipt{receipt}, []IngestAuditFact{fact}, ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10})
	if err != nil || len(comparison.Mismatches) != 1 || comparison.Mismatches[0].Reason != MismatchIdentity {
		t.Fatalf("non-persisted fact mismatch=%+v err=%v", comparison, err)
	}
}

func TestCompareIngestAuditAcceptsCounterOnlyPersistedReceipt(t *testing.T) {
	// A counter-only sFlow datagram persists interface counters with zero
	// flow-record rows. It must reconcile cleanly, not report missing_records.
	receipt := IngestAuditReceipt{
		SourceMessageKey:   SourceMessageKey{SourceStreamID: "stream-a", KafkaPartition: 0, KafkaOffset: 7},
		KafkaTopic:         "watchdog.flow.raw-v1",
		Disposition:        IngestDispositionPersisted,
		CounterRecordCount: 5,
		Counters:           IngestAuditCounters{RecordCount: 0},
	}
	comparison, err := CompareIngestAudit([]IngestAuditReceipt{receipt}, nil, ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10})
	if err != nil {
		t.Fatalf("counter-only persisted receipt rejected: %v", err)
	}
	if len(comparison.Mismatches) != 0 {
		t.Fatalf("counter-only persisted receipt must not be a mismatch, got %+v", comparison.Mismatches)
	}

	// A persisted receipt that claims flow records but has none is still a loss.
	receipt.Counters.RecordCount = 3
	comparison, err = CompareIngestAudit([]IngestAuditReceipt{receipt}, nil, ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(comparison.Mismatches) != 1 || comparison.Mismatches[0].Reason != MismatchMissingRecords {
		t.Fatalf("expected missing_records when flow records are claimed, got %+v", comparison.Mismatches)
	}
}

func TestClassifyCounterOnlyReceiptScannerParity(t *testing.T) {
	// The streaming scanner's !hasFacts branch must mirror the comparator: a
	// persisted receipt with zero flow records and no facts is not a loss.
	counterOnly := IngestAuditReceipt{
		SourceMessageKey:   SourceMessageKey{SourceStreamID: "stream-a", KafkaPartition: 0, KafkaOffset: 7},
		KafkaTopic:         "watchdog.flow.raw-v1",
		Disposition:        IngestDispositionPersisted,
		CounterRecordCount: 5,
	}
	if err := validateAuditReceipt(counterOnly); err != nil {
		t.Fatalf("counter-only receipt must be valid: %v", err)
	}
	empty := counterOnly
	empty.CounterRecordCount = 0
	if err := validateAuditReceipt(empty); err == nil {
		t.Fatal("persisted receipt with neither flow nor counter records must be invalid")
	}
}

func TestCompareIngestAuditMismatchPriority(t *testing.T) {
	receipts, facts := simpleAuditFixture()
	tests := []struct {
		name    string
		prepare func(*[]IngestAuditReceipt, *[]IngestAuditFact)
		want    ReconciliationMismatchReason
	}{
		{name: "missing receipt", prepare: func(receipts *[]IngestAuditReceipt, _ *[]IngestAuditFact) { *receipts = nil }, want: MismatchMissingReceipt},
		{name: "missing records", prepare: func(_ *[]IngestAuditReceipt, facts *[]IngestAuditFact) { *facts = nil }, want: MismatchMissingRecords},
		{name: "identity before count", prepare: func(receipts *[]IngestAuditReceipt, facts *[]IngestAuditFact) {
			(*facts)[0].KafkaTopic = "wrong"
			(*receipts)[0].Counters.RecordCount++
		}, want: MismatchIdentity},
		{name: "non-contiguous identity", prepare: func(_ *[]IngestAuditReceipt, facts *[]IngestAuditFact) {
			(*facts)[1].RecordIndex = 3
		}, want: MismatchIdentity},
		{name: "count before counter", prepare: func(receipts *[]IngestAuditReceipt, _ *[]IngestAuditFact) {
			(*receipts)[0].Counters.RecordCount++
			(*receipts)[0].Counters.RawBytes++
		}, want: MismatchCount},
		{name: "counter", prepare: func(receipts *[]IngestAuditReceipt, _ *[]IngestAuditFact) {
			(*receipts)[0].Counters.RawBytes++
		}, want: MismatchCounter},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testReceipts := append([]IngestAuditReceipt(nil), receipts...)
			testFacts := append([]IngestAuditFact(nil), facts...)
			test.prepare(&testReceipts, &testFacts)
			comparison, err := CompareIngestAudit(testReceipts, testFacts, ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(comparison.Mismatches) != 1 || comparison.Mismatches[0].Reason != test.want {
				t.Fatalf("comparison=%+v want=%s", comparison, test.want)
			}
		})
	}
}

func TestCompareIngestAuditRejectsDuplicateNaturalCoordinateAndOverflow(t *testing.T) {
	receipts, facts := simpleAuditFixture()
	duplicate := append(append([]IngestAuditFact(nil), facts...), facts[0])
	if _, err := CompareIngestAudit(receipts, duplicate, ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10}); err == nil {
		t.Fatal("duplicate natural record coordinate was accepted")
	}
	overflow := append([]IngestAuditFact(nil), facts...)
	overflow[0].RawBytes = math.MaxUint64
	if _, err := CompareIngestAudit(receipts, overflow, ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 10}); err == nil {
		t.Fatal("counter overflow was accepted")
	}
	if _, err := CompareIngestAudit(receipts, facts, ReconciliationCompareLimits{MaxBatches: 0, MaxFacts: 10}); err == nil {
		t.Fatal("invalid budget was accepted")
	}
}

func simpleAuditFixture() ([]IngestAuditReceipt, []IngestAuditFact) {
	key := SourceMessageKey{SourceStreamID: "cluster-a:raw-v1:incarnation-1", KafkaPartition: 3, KafkaOffset: 10}
	facts := []IngestAuditFact{
		{SourceMessageKey: key, KafkaTopic: "watchdog.flow.raw-v1", RecordIndex: 0, RawBytes: 100, RawPackets: 1, EstimatedValid: true, EstimatedBytes: 1000, EstimatedPackets: 10},
		{SourceMessageKey: key, KafkaTopic: "watchdog.flow.raw-v1", RecordIndex: 1, RawBytes: 200, RawPackets: 2, EstimatedValid: false},
	}
	receipt := IngestAuditReceipt{SourceMessageKey: key, KafkaTopic: "watchdog.flow.raw-v1", Disposition: IngestDispositionPersisted, Counters: IngestAuditCounters{
		RecordCount: 2, RawBytes: 300, RawPackets: 3, EstimatedBytes: 1000, EstimatedPackets: 10, EstimatedValidRecords: 1,
	}}
	return []IngestAuditReceipt{receipt}, facts
}
