// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import "testing"

func cleanReceiptAndFacts() (IngestAuditReceipt, factCounters) {
	counters := IngestAuditCounters{
		SourceBatchCount: 2, RecordCount: 6, RawBytes: 600, RawPackets: 12,
		EstimatedBytes: 400, EstimatedPackets: 8, EstimatedValidRecords: 4,
	}
	receipt := IngestAuditReceipt{
		BatchID: [32]byte{1}, KafkaTopic: "watchdog.flow.raw-v1", KafkaPartition: 3,
		FirstOffset: 10, LastOffset: 11, Counters: counters, Checksum: [32]byte{9},
	}
	return receipt, factCounters{FirstOffset: 10, LastOffset: 11, Counters: counters}
}

func TestClassifyBatchCountersMirrorsComparatorPriority(t *testing.T) {
	// A counter-clean batch is a candidate for the record-level checksum, not a
	// reported mismatch.
	receipt, fact := cleanReceiptAndFacts()
	if reason, candidate := classifyBatchCounters(receipt, fact); reason != "" || !candidate {
		t.Fatalf("clean batch = (%q, %v), want (\"\", true)", reason, candidate)
	}

	// Identity outranks count outranks counter, matching CompareIngestAudit.
	cases := []struct {
		name   string
		mutate func(*factCounters)
		want   ReconciliationMismatchReason
	}{
		{"identity_offset", func(f *factCounters) { f.LastOffset = 99 }, MismatchIdentity},
		{"count_records", func(f *factCounters) { f.Counters.RecordCount = 7 }, MismatchCount},
		{"count_source_batches", func(f *factCounters) { f.Counters.SourceBatchCount = 3 }, MismatchCount},
		{"counter_raw_bytes", func(f *factCounters) { f.Counters.RawBytes = 601 }, MismatchCounter},
		{"counter_estimated_valid", func(f *factCounters) { f.Counters.EstimatedValidRecords = 5 }, MismatchCounter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, fact := cleanReceiptAndFacts()
			tc.mutate(&fact)
			reason, candidate := classifyBatchCounters(receipt, fact)
			if candidate || reason != tc.want {
				t.Fatalf("got (%q, %v), want (%q, false)", reason, candidate, tc.want)
			}
		})
	}

	// A batch that trips both identity and counter must report identity (higher
	// priority), never the lower-priority reason.
	_, fact = cleanReceiptAndFacts()
	fact.FirstOffset = 0
	fact.Counters.RawBytes = 1
	if reason, _ := classifyBatchCounters(receipt, fact); reason != MismatchIdentity {
		t.Fatalf("identity must outrank counter, got %q", reason)
	}
}

func TestValidateReconciliationScanRequestRejectsBadInput(t *testing.T) {
	good := ReconciliationScanRequest{
		Cursor:        ReconciliationScanCursor{KafkaTopic: "t", KafkaPartition: 3, NextOffset: 0},
		CloseOffset:   100, MaxBatches: 10, MaxFactRows: 100, MaxReadBytes: 1 << 20,
		CompareLimits: ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 100},
	}
	if err := validateReconciliationScanRequest(good); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*ReconciliationScanRequest)
	}{
		{"no_topic", func(r *ReconciliationScanRequest) { r.Cursor.KafkaTopic = "" }},
		{"cursor_past_close", func(r *ReconciliationScanRequest) { r.Cursor.NextOffset = 101 }},
		{"zero_batches", func(r *ReconciliationScanRequest) { r.MaxBatches = 0 }},
		{"zero_fact_rows", func(r *ReconciliationScanRequest) { r.MaxFactRows = 0 }},
		{"zero_read_bytes", func(r *ReconciliationScanRequest) { r.MaxReadBytes = 0 }},
		{"bad_compare_limits", func(r *ReconciliationScanRequest) { r.CompareLimits.MaxBatches = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := good
			tc.mutate(&request)
			if err := validateReconciliationScanRequest(request); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
