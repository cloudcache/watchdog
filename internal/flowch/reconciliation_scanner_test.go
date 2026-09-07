// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import "testing"

func cleanReceiptAndFacts() (IngestAuditReceipt, factCounters) {
	counters := IngestAuditCounters{RecordCount: 2, RawBytes: 600, RawPackets: 12, EstimatedBytes: 400, EstimatedPackets: 8, EstimatedValidRecords: 1}
	receipt := IngestAuditReceipt{
		SourceMessageKey: SourceMessageKey{SourceStreamID: "cluster-a:raw-v1:incarnation-1", KafkaPartition: 3, KafkaOffset: 10},
		KafkaTopic:       "watchdog.flow.raw-v1", Disposition: IngestDispositionPersisted, Counters: counters,
	}
	fact := factCounters{KafkaTopic: receipt.KafkaTopic, TopicCount: 1, MinRecordIndex: 0, MaxRecordIndex: 1, UniqueRecords: 2, Counters: counters}
	return receipt, fact
}

func TestClassifyMessageCountersMirrorsComparatorPriority(t *testing.T) {
	receipt, fact := cleanReceiptAndFacts()
	if reason := classifyMessageCounters(receipt, fact); reason != "" {
		t.Fatalf("clean message reason=%q", reason)
	}
	cases := []struct {
		name   string
		mutate func(*factCounters)
		want   ReconciliationMismatchReason
	}{
		{"identity_topic", func(f *factCounters) { f.KafkaTopic = "other" }, MismatchIdentity},
		{"identity_index", func(f *factCounters) { f.MaxRecordIndex = 3 }, MismatchIdentity},
		{"count", func(f *factCounters) { f.Counters.RecordCount = 3; f.MaxRecordIndex = 2; f.UniqueRecords = 3 }, MismatchCount},
		{"counter", func(f *factCounters) { f.Counters.RawBytes++ }, MismatchCounter},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, candidate := cleanReceiptAndFacts()
			test.mutate(&candidate)
			if reason := classifyMessageCounters(receipt, candidate); reason != test.want {
				t.Fatalf("reason=%q want=%q", reason, test.want)
			}
		})
	}
}

func TestValidateReconciliationScanRequestRejectsBadInput(t *testing.T) {
	good := ReconciliationScanRequest{
		Cursor:      ReconciliationScanCursor{SourceStreamID: "stream-a", KafkaTopic: "t", KafkaPartition: 3},
		CloseOffset: 100, MaxBatches: 10, MaxFactRows: 100, MaxReadBytes: 1 << 20,
		CompareLimits: ReconciliationCompareLimits{MaxBatches: 10, MaxFacts: 100},
	}
	if err := validateReconciliationScanRequest(good); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*ReconciliationScanRequest)
	}{
		{"no_stream", func(r *ReconciliationScanRequest) { r.Cursor.SourceStreamID = "" }},
		{"no_topic", func(r *ReconciliationScanRequest) { r.Cursor.KafkaTopic = "" }},
		{"cursor_past_close", func(r *ReconciliationScanRequest) { r.Cursor.NextOffset = 101 }},
		{"zero_batches", func(r *ReconciliationScanRequest) { r.MaxBatches = 0 }},
		{"zero_fact_rows", func(r *ReconciliationScanRequest) { r.MaxFactRows = 0 }},
		{"zero_read_bytes", func(r *ReconciliationScanRequest) { r.MaxReadBytes = 0 }},
		{"bad_compare_limits", func(r *ReconciliationScanRequest) { r.CompareLimits.MaxBatches = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := good
			test.mutate(&request)
			if err := validateReconciliationScanRequest(request); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
