// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

type ReconciliationMismatchReason string

const (
	MismatchMissingReceipt ReconciliationMismatchReason = "missing_receipt"
	MismatchMissingRecords ReconciliationMismatchReason = "missing_records"
	MismatchIdentity       ReconciliationMismatchReason = "identity_mismatch"
	MismatchCount          ReconciliationMismatchReason = "count_mismatch"
	MismatchCounter        ReconciliationMismatchReason = "counter_mismatch"
	hardReconcileBatches                                = 10_000
	hardReconcileFacts                                  = 1_000_000
)

type ReconciliationCompareLimits struct {
	MaxBatches int
	MaxFacts   int
}

type IngestAuditCounters struct {
	RecordCount           uint64
	RawBytes              uint64
	RawPackets            uint64
	EstimatedBytes        uint64
	EstimatedPackets      uint64
	EstimatedValidRecords uint64
}

type SourceMessageKey struct {
	SourceStreamID string
	KafkaPartition uint32
	KafkaOffset    uint64
}

type IngestMessageDisposition string

const (
	IngestDispositionPersisted       IngestMessageDisposition = "persisted"
	IngestDispositionTemplateMissing IngestMessageDisposition = "template_missing"
	IngestDispositionEmpty           IngestMessageDisposition = "empty"
	IngestDispositionDecodeRejected  IngestMessageDisposition = "decode_rejected"
	IngestDispositionMappingRejected IngestMessageDisposition = "mapping_rejected"
)

type IngestAuditReceipt struct {
	SourceMessageKey
	KafkaTopic  string
	Disposition IngestMessageDisposition
	Counters    IngestAuditCounters
}

type IngestAuditFact struct {
	SourceMessageKey
	KafkaTopic       string
	RecordIndex      uint32
	RawBytes         uint64
	RawPackets       uint64
	EstimatedValid   bool
	EstimatedBytes   uint64
	EstimatedPackets uint64
}

type IngestReconciliationMismatch struct {
	Reason ReconciliationMismatchReason
	SourceMessageKey
	KafkaTopic string
	Expected   IngestAuditCounters
	Actual     IngestAuditCounters
}

type IngestReconciliationComparison struct {
	Batches    uint64
	Facts      uint64
	Mismatches []IngestReconciliationMismatch
}

// CompareIngestAudit compares message receipts with already generation-
// deduplicated facts. Count and counter conservation deliberately replace the
// V1 record-content checksum; non-counter semantic mapping is tested by the
// decoder/schema corpus instead of hashing every hot-path record.
func CompareIngestAudit(receipts []IngestAuditReceipt, facts []IngestAuditFact, limits ReconciliationCompareLimits) (IngestReconciliationComparison, error) {
	if err := validateReconciliationCompareLimits(limits); err != nil {
		return IngestReconciliationComparison{}, err
	}
	if len(receipts) > limits.MaxBatches || len(facts) > limits.MaxFacts {
		return IngestReconciliationComparison{}, errors.New("ingest reconciliation input exceeds configured budget")
	}

	receiptsByMessage := make(map[SourceMessageKey]IngestAuditReceipt, len(receipts))
	for index, receipt := range receipts {
		if err := validateAuditReceipt(receipt); err != nil {
			return IngestReconciliationComparison{}, fmt.Errorf("invalid ingest receipt %d: %w", index, err)
		}
		if _, exists := receiptsByMessage[receipt.SourceMessageKey]; exists {
			return IngestReconciliationComparison{}, fmt.Errorf("duplicate ingest receipt %s/%d/%d", receipt.SourceStreamID, receipt.KafkaPartition, receipt.KafkaOffset)
		}
		receiptsByMessage[receipt.SourceMessageKey] = receipt
	}

	factsByMessage := make(map[SourceMessageKey][]IngestAuditFact)
	seenRecords := make(map[struct {
		SourceMessageKey
		RecordIndex uint32
	}]struct{}, len(facts))
	for index, fact := range facts {
		if err := validateAuditFact(fact); err != nil {
			return IngestReconciliationComparison{}, fmt.Errorf("invalid ingest fact %d: %w", index, err)
		}
		recordKey := struct {
			SourceMessageKey
			RecordIndex uint32
		}{fact.SourceMessageKey, fact.RecordIndex}
		if _, exists := seenRecords[recordKey]; exists {
			return IngestReconciliationComparison{}, fmt.Errorf("duplicate generation-deduplicated record %s/%d/%d/%d", fact.SourceStreamID, fact.KafkaPartition, fact.KafkaOffset, fact.RecordIndex)
		}
		seenRecords[recordKey] = struct{}{}
		factsByMessage[fact.SourceMessageKey] = append(factsByMessage[fact.SourceMessageKey], fact)
	}

	keys := make([]SourceMessageKey, 0, len(receiptsByMessage)+len(factsByMessage))
	for key := range receiptsByMessage {
		keys = append(keys, key)
	}
	for key := range factsByMessage {
		if _, exists := receiptsByMessage[key]; !exists {
			keys = append(keys, key)
		}
	}
	if len(keys) > limits.MaxBatches {
		return IngestReconciliationComparison{}, errors.New("ingest reconciliation message groups exceed configured budget")
	}
	sort.Slice(keys, func(i, j int) bool { return sourceMessageKeyLess(keys[i], keys[j]) })

	comparison := IngestReconciliationComparison{Batches: uint64(len(keys)), Facts: uint64(len(facts))}
	for _, key := range keys {
		receipt, hasReceipt := receiptsByMessage[key]
		messageFacts := factsByMessage[key]
		if !hasReceipt {
			actual, topic, _, err := summarizeAuditFacts(messageFacts)
			if err != nil {
				return IngestReconciliationComparison{}, err
			}
			comparison.Mismatches = append(comparison.Mismatches, IngestReconciliationMismatch{
				Reason: MismatchMissingReceipt, SourceMessageKey: key, KafkaTopic: topic, Actual: actual,
			})
			continue
		}
		if receipt.Disposition != IngestDispositionPersisted {
			if len(messageFacts) != 0 {
				actual, _, _, err := summarizeAuditFacts(messageFacts)
				if err != nil {
					return IngestReconciliationComparison{}, err
				}
				comparison.Mismatches = append(comparison.Mismatches, mismatchFromReceipt(MismatchIdentity, receipt, actual))
			}
			continue
		}
		if len(messageFacts) == 0 {
			comparison.Mismatches = append(comparison.Mismatches, mismatchFromReceipt(MismatchMissingRecords, receipt, IngestAuditCounters{}))
			continue
		}
		actual, topic, contiguous, err := summarizeAuditFacts(messageFacts)
		if err != nil {
			return IngestReconciliationComparison{}, err
		}
		reason := ReconciliationMismatchReason("")
		switch {
		case topic != receipt.KafkaTopic || !contiguous:
			reason = MismatchIdentity
		case actual.RecordCount != receipt.Counters.RecordCount:
			reason = MismatchCount
		case !auditCountersEqual(actual, receipt.Counters):
			reason = MismatchCounter
		}
		if reason != "" {
			comparison.Mismatches = append(comparison.Mismatches, mismatchFromReceipt(reason, receipt, actual))
		}
	}
	return comparison, nil
}

func validateReconciliationCompareLimits(limits ReconciliationCompareLimits) error {
	if limits.MaxBatches < 1 || limits.MaxBatches > hardReconcileBatches {
		return fmt.Errorf("ingest reconciliation max batches must be 1..%d", hardReconcileBatches)
	}
	if limits.MaxFacts < 1 || limits.MaxFacts > hardReconcileFacts {
		return fmt.Errorf("ingest reconciliation max facts must be 1..%d", hardReconcileFacts)
	}
	return nil
}

func validateAuditReceipt(receipt IngestAuditReceipt) error {
	if !validSourceMessageKey(receipt.SourceMessageKey) || receipt.KafkaTopic == "" {
		return errors.New("receipt identity is incomplete")
	}
	if !validIngestDisposition(receipt.Disposition) {
		return errors.New("receipt disposition is invalid")
	}
	if receipt.Disposition == IngestDispositionPersisted && receipt.Counters.RecordCount == 0 {
		return errors.New("persisted receipt count is invalid")
	}
	if receipt.Disposition != IngestDispositionPersisted && receipt.Counters != (IngestAuditCounters{}) {
		return errors.New("non-persisted receipt must have zero counters")
	}
	return nil
}

func validIngestDisposition(value IngestMessageDisposition) bool {
	switch value {
	case IngestDispositionPersisted, IngestDispositionTemplateMissing, IngestDispositionEmpty,
		IngestDispositionDecodeRejected, IngestDispositionMappingRejected:
		return true
	default:
		return false
	}
}

func validateAuditFact(fact IngestAuditFact) error {
	if !validSourceMessageKey(fact.SourceMessageKey) || fact.KafkaTopic == "" {
		return errors.New("fact identity is incomplete")
	}
	return nil
}

func validSourceMessageKey(key SourceMessageKey) bool {
	return key.SourceStreamID != ""
}

func summarizeAuditFacts(facts []IngestAuditFact) (IngestAuditCounters, string, bool, error) {
	if len(facts) == 0 {
		return IngestAuditCounters{}, "", false, errors.New("cannot summarize empty ingest facts")
	}
	ordered := append([]IngestAuditFact(nil), facts...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].RecordIndex < ordered[j].RecordIndex })
	topic := ordered[0].KafkaTopic
	counters := IngestAuditCounters{RecordCount: uint64(len(ordered))}
	contiguous := true
	for index, fact := range ordered {
		if fact.KafkaTopic != topic || fact.SourceMessageKey != ordered[0].SourceMessageKey {
			contiguous = false
		}
		if fact.RecordIndex != uint32(index) {
			contiguous = false
		}
		var ok bool
		if counters.RawBytes, ok = addAuditCounter(counters.RawBytes, fact.RawBytes); !ok {
			return IngestAuditCounters{}, "", false, errors.New("ingest raw byte counter overflow")
		}
		if counters.RawPackets, ok = addAuditCounter(counters.RawPackets, fact.RawPackets); !ok {
			return IngestAuditCounters{}, "", false, errors.New("ingest raw packet counter overflow")
		}
		if fact.EstimatedValid {
			if counters.EstimatedBytes, ok = addAuditCounter(counters.EstimatedBytes, fact.EstimatedBytes); !ok {
				return IngestAuditCounters{}, "", false, errors.New("ingest estimated byte counter overflow")
			}
			if counters.EstimatedPackets, ok = addAuditCounter(counters.EstimatedPackets, fact.EstimatedPackets); !ok {
				return IngestAuditCounters{}, "", false, errors.New("ingest estimated packet counter overflow")
			}
			if counters.EstimatedValidRecords == math.MaxUint64 {
				return IngestAuditCounters{}, "", false, errors.New("ingest estimated-valid count overflow")
			}
			counters.EstimatedValidRecords++
		}
	}
	return counters, topic, contiguous, nil
}

func auditCountersEqual(actual, expected IngestAuditCounters) bool {
	return actual.RawBytes == expected.RawBytes && actual.RawPackets == expected.RawPackets &&
		actual.EstimatedBytes == expected.EstimatedBytes && actual.EstimatedPackets == expected.EstimatedPackets &&
		actual.EstimatedValidRecords == expected.EstimatedValidRecords
}

func addAuditCounter(current, value uint64) (uint64, bool) {
	if current > math.MaxUint64-value {
		return 0, false
	}
	return current + value, true
}

func mismatchFromReceipt(reason ReconciliationMismatchReason, receipt IngestAuditReceipt, actual IngestAuditCounters) IngestReconciliationMismatch {
	return IngestReconciliationMismatch{
		Reason: reason, SourceMessageKey: receipt.SourceMessageKey, KafkaTopic: receipt.KafkaTopic,
		Expected: receipt.Counters, Actual: actual,
	}
}

func sourceMessageKeyLess(left, right SourceMessageKey) bool {
	if left.SourceStreamID != right.SourceStreamID {
		return left.SourceStreamID < right.SourceStreamID
	}
	if left.KafkaPartition != right.KafkaPartition {
		return left.KafkaPartition < right.KafkaPartition
	}
	return left.KafkaOffset < right.KafkaOffset
}
