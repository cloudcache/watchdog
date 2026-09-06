// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
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
	MismatchChecksum       ReconciliationMismatchReason = "checksum_mismatch"
	hardReconcileBatches                                = 10_000
	hardReconcileFacts                                  = 1_000_000
)

type ReconciliationCompareLimits struct {
	MaxBatches int
	MaxFacts   int
}

type IngestAuditCounters struct {
	SourceBatchCount      uint32
	RecordCount           uint64
	RawBytes              uint64
	RawPackets            uint64
	EstimatedBytes        uint64
	EstimatedPackets      uint64
	EstimatedValidRecords uint64
}

type IngestAuditReceipt struct {
	BatchID        [32]byte
	KafkaTopic     string
	KafkaPartition uint32
	FirstOffset    uint64
	LastOffset     uint64
	Counters       IngestAuditCounters
	Checksum       [32]byte
}

type IngestAuditFact struct {
	RecordID              [32]byte
	BatchID               [32]byte
	KafkaTopic            string
	KafkaPartition        uint32
	KafkaOffset           uint64
	RecordIndex           uint32
	RawBytes              uint64
	RawPackets            uint64
	EstimatedValid        bool
	EstimatedBytes        uint64
	EstimatedPackets      uint64
	QualityFlags          uint64
	DimensionFingerprint  uint64
	ClassificationVersion uint32
}

type IngestReconciliationMismatch struct {
	Reason           ReconciliationMismatchReason
	BatchID          [32]byte
	KafkaTopic       string
	KafkaPartition   uint32
	FirstOffset      uint64
	LastOffset       uint64
	Expected         IngestAuditCounters
	Actual           IngestAuditCounters
	ExpectedChecksum [32]byte
	ActualChecksum   [32]byte
}

type IngestReconciliationComparison struct {
	Batches    uint64
	Facts      uint64
	Mismatches []IngestReconciliationMismatch
}

// CompareIngestAudit compares already generation-deduplicated facts with v2
// receipts. It owns no cursor, Kafka watermark, TTL decision, or metric state.
func CompareIngestAudit(receipts []IngestAuditReceipt, facts []IngestAuditFact, limits ReconciliationCompareLimits) (IngestReconciliationComparison, error) {
	if err := validateReconciliationCompareLimits(limits); err != nil {
		return IngestReconciliationComparison{}, err
	}
	if len(receipts) > limits.MaxBatches || len(facts) > limits.MaxFacts {
		return IngestReconciliationComparison{}, errors.New("ingest reconciliation input exceeds configured budget")
	}

	receiptsByBatch := make(map[[32]byte]IngestAuditReceipt, len(receipts))
	for index, receipt := range receipts {
		if err := validateAuditReceipt(receipt); err != nil {
			return IngestReconciliationComparison{}, fmt.Errorf("invalid ingest receipt %d: %w", index, err)
		}
		if _, exists := receiptsByBatch[receipt.BatchID]; exists {
			return IngestReconciliationComparison{}, fmt.Errorf("duplicate ingest receipt %x", receipt.BatchID[:8])
		}
		receiptsByBatch[receipt.BatchID] = receipt
	}

	factsByBatch := make(map[[32]byte][]IngestAuditFact)
	seenRecords := make(map[[32]byte]struct{}, len(facts))
	for index, fact := range facts {
		if err := validateAuditFact(fact); err != nil {
			return IngestReconciliationComparison{}, fmt.Errorf("invalid ingest fact %d: %w", index, err)
		}
		if _, exists := seenRecords[fact.RecordID]; exists {
			return IngestReconciliationComparison{}, fmt.Errorf("duplicate generation-deduplicated record %x", fact.RecordID[:8])
		}
		seenRecords[fact.RecordID] = struct{}{}
		factsByBatch[fact.BatchID] = append(factsByBatch[fact.BatchID], fact)
	}
	batchIDs := make([][32]byte, 0, len(receiptsByBatch)+len(factsByBatch))
	for batchID := range receiptsByBatch {
		batchIDs = append(batchIDs, batchID)
	}
	for batchID := range factsByBatch {
		if _, exists := receiptsByBatch[batchID]; !exists {
			batchIDs = append(batchIDs, batchID)
		}
	}
	if len(batchIDs) > limits.MaxBatches {
		return IngestReconciliationComparison{}, errors.New("ingest reconciliation batch groups exceed configured budget")
	}
	sort.Slice(batchIDs, func(i, j int) bool { return bytes.Compare(batchIDs[i][:], batchIDs[j][:]) < 0 })

	comparison := IngestReconciliationComparison{Batches: uint64(len(batchIDs)), Facts: uint64(len(facts))}
	for _, batchID := range batchIDs {
		receipt, hasReceipt := receiptsByBatch[batchID]
		batchFacts := factsByBatch[batchID]
		if !hasReceipt {
			actual, checksum, topic, partition, first, last, err := summarizeAuditFacts(batchFacts)
			if err != nil {
				return IngestReconciliationComparison{}, err
			}
			comparison.Mismatches = append(comparison.Mismatches, IngestReconciliationMismatch{
				Reason: MismatchMissingReceipt, BatchID: batchID, KafkaTopic: topic, KafkaPartition: partition,
				FirstOffset: first, LastOffset: last, Actual: actual, ActualChecksum: checksum,
			})
			continue
		}
		if len(batchFacts) == 0 {
			comparison.Mismatches = append(comparison.Mismatches, mismatchFromReceipt(MismatchMissingRecords, receipt, IngestAuditCounters{}, [32]byte{}))
			continue
		}
		actual, checksum, topic, partition, first, last, err := summarizeAuditFacts(batchFacts)
		if err != nil {
			return IngestReconciliationComparison{}, err
		}
		reason := ReconciliationMismatchReason("")
		switch {
		case topic != receipt.KafkaTopic || partition != receipt.KafkaPartition || first != receipt.FirstOffset || last != receipt.LastOffset:
			reason = MismatchIdentity
		case actual.SourceBatchCount != receipt.Counters.SourceBatchCount || actual.RecordCount != receipt.Counters.RecordCount:
			reason = MismatchCount
		case actual.RawBytes != receipt.Counters.RawBytes || actual.RawPackets != receipt.Counters.RawPackets ||
			actual.EstimatedBytes != receipt.Counters.EstimatedBytes || actual.EstimatedPackets != receipt.Counters.EstimatedPackets ||
			actual.EstimatedValidRecords != receipt.Counters.EstimatedValidRecords:
			reason = MismatchCounter
		case checksum != receipt.Checksum:
			reason = MismatchChecksum
		}
		if reason != "" {
			comparison.Mismatches = append(comparison.Mismatches, mismatchFromReceipt(reason, receipt, actual, checksum))
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
	if receipt.BatchID == ([32]byte{}) || receipt.Checksum == ([32]byte{}) || receipt.KafkaTopic == "" || receipt.FirstOffset > receipt.LastOffset {
		return errors.New("receipt identity is incomplete")
	}
	if receipt.Counters.RecordCount == 0 || receipt.Counters.SourceBatchCount == 0 || uint64(receipt.Counters.SourceBatchCount) > receipt.Counters.RecordCount {
		return errors.New("receipt counts are invalid")
	}
	return nil
}

func validateAuditFact(fact IngestAuditFact) error {
	if fact.RecordID == ([32]byte{}) || fact.BatchID == ([32]byte{}) || fact.KafkaTopic == "" {
		return errors.New("fact identity is incomplete")
	}
	return nil
}

func summarizeAuditFacts(facts []IngestAuditFact) (IngestAuditCounters, [32]byte, string, uint32, uint64, uint64, error) {
	if len(facts) == 0 {
		return IngestAuditCounters{}, [32]byte{}, "", 0, 0, 0, errors.New("cannot summarize empty ingest facts")
	}
	ordered := append([]IngestAuditFact(nil), facts...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].KafkaOffset != ordered[j].KafkaOffset {
			return ordered[i].KafkaOffset < ordered[j].KafkaOffset
		}
		if ordered[i].RecordIndex != ordered[j].RecordIndex {
			return ordered[i].RecordIndex < ordered[j].RecordIndex
		}
		return bytes.Compare(ordered[i].RecordID[:], ordered[j].RecordID[:]) < 0
	})
	topic, partition := ordered[0].KafkaTopic, ordered[0].KafkaPartition
	first, last := ordered[0].KafkaOffset, ordered[len(ordered)-1].KafkaOffset
	counters := IngestAuditCounters{RecordCount: uint64(len(ordered))}
	var previousOffset uint64
	for index, fact := range ordered {
		if fact.KafkaTopic != topic || fact.KafkaPartition != partition {
			topic = ""
			partition = math.MaxUint32
		}
		if index == 0 || fact.KafkaOffset != previousOffset {
			if counters.SourceBatchCount == math.MaxUint32 {
				return IngestAuditCounters{}, [32]byte{}, "", 0, 0, 0, errors.New("ingest source batch count overflow")
			}
			counters.SourceBatchCount++
			previousOffset = fact.KafkaOffset
		}
		var ok bool
		if counters.RawBytes, ok = addAuditCounter(counters.RawBytes, fact.RawBytes); !ok {
			return IngestAuditCounters{}, [32]byte{}, "", 0, 0, 0, errors.New("ingest raw byte counter overflow")
		}
		if counters.RawPackets, ok = addAuditCounter(counters.RawPackets, fact.RawPackets); !ok {
			return IngestAuditCounters{}, [32]byte{}, "", 0, 0, 0, errors.New("ingest raw packet counter overflow")
		}
		if fact.EstimatedValid {
			if counters.EstimatedBytes, ok = addAuditCounter(counters.EstimatedBytes, fact.EstimatedBytes); !ok {
				return IngestAuditCounters{}, [32]byte{}, "", 0, 0, 0, errors.New("ingest estimated byte counter overflow")
			}
			if counters.EstimatedPackets, ok = addAuditCounter(counters.EstimatedPackets, fact.EstimatedPackets); !ok {
				return IngestAuditCounters{}, [32]byte{}, "", 0, 0, 0, errors.New("ingest estimated packet counter overflow")
			}
			counters.EstimatedValidRecords++
		}
	}
	return counters, ingestAuditChecksum(ordered), topic, partition, first, last, nil
}

func ingestAuditChecksum(facts []IngestAuditFact) [32]byte {
	hash := sha256.New()
	var number [8]byte
	for _, fact := range facts {
		hash.Write(fact.RecordID[:])
		binary.BigEndian.PutUint64(number[:], fact.RawBytes)
		hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], fact.RawPackets)
		hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], fact.EstimatedBytes)
		hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], fact.EstimatedPackets)
		hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], fact.QualityFlags)
		hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], fact.DimensionFingerprint)
		hash.Write(number[:])
		binary.BigEndian.PutUint32(number[:4], fact.ClassificationVersion)
		hash.Write(number[:4])
		if fact.EstimatedValid {
			hash.Write([]byte{1})
		} else {
			hash.Write([]byte{0})
		}
	}
	var checksum [32]byte
	copy(checksum[:], hash.Sum(nil))
	return checksum
}

func addAuditCounter(current, value uint64) (uint64, bool) {
	if current > math.MaxUint64-value {
		return 0, false
	}
	return current + value, true
}

func mismatchFromReceipt(reason ReconciliationMismatchReason, receipt IngestAuditReceipt, actual IngestAuditCounters, checksum [32]byte) IngestReconciliationMismatch {
	return IngestReconciliationMismatch{
		Reason: reason, BatchID: receipt.BatchID, KafkaTopic: receipt.KafkaTopic, KafkaPartition: receipt.KafkaPartition,
		FirstOffset: receipt.FirstOffset, LastOffset: receipt.LastOffset,
		Expected: receipt.Counters, Actual: actual, ExpectedChecksum: receipt.Checksum, ActualChecksum: checksum,
	}
}
