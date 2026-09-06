// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// ReconciliationScanner is the ClickHouse input boundary for ingest audit
// reconciliation (FLOW-04C3B2B). It reads receipts (flow_ingest_batches) and
// generation-deduplicated facts (the flow_ingest_audit_v1 projection over
// flow_records) for one (topic, partition) within a bounded offset window, then
// feeds CompareIngestAudit. It owns the cursor/window/budget and the cheap
// counters-first, checksum-only-for-candidates read; it owns no Kafka watermark
// policy, TTL decision, job lease, or metric state (those belong to the B3 job).
type ReconciliationScanner struct {
	executor queryExecutor
}

// NewReconciliationScanner builds a scanner over a native ClickHouse connection.
func NewReconciliationScanner(native *NativeInserter) (*ReconciliationScanner, error) {
	if native == nil || native.executor == nil {
		return nil, errors.New("ClickHouse reconciliation scanner requires a native inserter")
	}
	return &ReconciliationScanner{executor: native.executor}, nil
}

// ReconciliationScanCursor is the resumable position for one Kafka partition.
type ReconciliationScanCursor struct {
	KafkaTopic     string
	KafkaPartition uint32
	NextOffset     uint64
}

// ReconciliationScanRequest bounds one scan. CloseOffset is the Kafka
// committed-close watermark: only batches fully below it (last_offset <
// CloseOffset) are reconciled, so in-flight offsets are never compared.
type ReconciliationScanRequest struct {
	Cursor        ReconciliationScanCursor
	CloseOffset   uint64
	MaxBatches    int
	MaxFactRows   int
	MaxReadBytes  uint64
	CompareLimits ReconciliationCompareLimits
}

// ReconciliationScanResult carries the comparison plus the boundary metadata a
// caller needs to advance safely. Complete is true only when the whole window
// up to CloseOffset was reconciled within budget; on a budget cut the caller
// must not treat an empty mismatch set as an all-clear (no fake zeros).
type ReconciliationScanResult struct {
	Comparison       IngestReconciliationComparison
	NextCursor       ReconciliationScanCursor
	Complete         bool
	ReceiptBatches   int
	CandidateBatches int
	Facts            int
}

// factCounters is the cheap per-batch aggregate read in phase 1. It mirrors the
// counters CompareIngestAudit derives from record-level facts, so a counter
// mismatch can be classified without transferring any record rows.
type factCounters struct {
	FirstOffset uint64
	LastOffset  uint64
	Counters    IngestAuditCounters
}

func (s *ReconciliationScanner) Scan(ctx context.Context, request ReconciliationScanRequest) (ReconciliationScanResult, error) {
	if s == nil || s.executor == nil {
		return ReconciliationScanResult{}, errors.New("ClickHouse reconciliation scanner is not initialized")
	}
	if err := validateReconciliationScanRequest(request); err != nil {
		return ReconciliationScanResult{}, err
	}
	result := ReconciliationScanResult{NextCursor: request.Cursor, Complete: true}
	if request.Cursor.NextOffset >= request.CloseOffset {
		return result, nil
	}

	// Phase 1a: receipts for the window. One extra row past the batch budget
	// signals the window is denser than this run can cover.
	receipts, receiptBudgetCut, err := s.readReceipts(ctx, request)
	if err != nil {
		return ReconciliationScanResult{}, err
	}
	windowEnd := request.CloseOffset
	if receiptBudgetCut {
		windowEnd = receipts[len(receipts)-1].LastOffset + 1
	}
	result.ReceiptBatches = len(receipts)

	// Phase 1b: cheap per-batch fact counters (server-side aggregate; no record
	// rows transferred) over the same window.
	factByBatch, err := s.readFactCounters(ctx, request, windowEnd)
	if err != nil {
		return ReconciliationScanResult{}, err
	}

	// Classify every batch from counters alone. Counter-clean batches become
	// candidates that still need a record-level checksum; everything else is a
	// definitive mismatch reported here without fetching rows.
	receiptByBatch := make(map[[32]byte]IngestAuditReceipt, len(receipts))
	for _, receipt := range receipts {
		receiptByBatch[receipt.BatchID] = receipt
	}
	candidates := make([][32]byte, 0, len(receiptByBatch))
	for _, receipt := range receipts {
		fact, hasFacts := factByBatch[receipt.BatchID]
		if !hasFacts {
			result.Comparison.Mismatches = append(result.Comparison.Mismatches,
				mismatchFromReceipt(MismatchMissingRecords, receipt, IngestAuditCounters{}, [32]byte{}))
			continue
		}
		if reason, candidate := classifyBatchCounters(receipt, fact); candidate {
			candidates = append(candidates, receipt.BatchID)
		} else {
			result.Comparison.Mismatches = append(result.Comparison.Mismatches,
				mismatchFromReceipt(reason, receipt, fact.Counters, [32]byte{}))
		}
	}
	for batchID, fact := range factByBatch {
		if _, hasReceipt := receiptByBatch[batchID]; hasReceipt {
			continue
		}
		result.Comparison.Mismatches = append(result.Comparison.Mismatches, IngestReconciliationMismatch{
			Reason: MismatchMissingReceipt, BatchID: batchID,
			KafkaTopic: request.Cursor.KafkaTopic, KafkaPartition: request.Cursor.KafkaPartition,
			FirstOffset: fact.FirstOffset, LastOffset: fact.LastOffset, Actual: fact.Counters,
		})
	}
	result.CandidateBatches = len(candidates)
	result.Comparison.Batches = uint64(len(receiptByBatch) + (len(factByBatch) - countCoveredFacts(factByBatch, receiptByBatch)))

	// Phase 2: fetch record-level facts only for candidate batches and let the
	// stateless comparator settle the checksum. A fact-budget cut means some
	// candidate could not be verified, so the cursor must not advance past it.
	factBudgetCut := false
	if len(candidates) > 0 {
		facts, cut, err := s.readCandidateFacts(ctx, request, windowEnd, candidates)
		if err != nil {
			return ReconciliationScanResult{}, err
		}
		factBudgetCut = cut
		result.Facts = len(facts)
		candidateReceipts := make([]IngestAuditReceipt, 0, len(candidates))
		for _, batchID := range candidates {
			candidateReceipts = append(candidateReceipts, receiptByBatch[batchID])
		}
		comparison, err := CompareIngestAudit(candidateReceipts, facts, request.CompareLimits)
		if err != nil {
			return ReconciliationScanResult{}, err
		}
		result.Comparison.Facts = comparison.Facts
		result.Comparison.Mismatches = append(result.Comparison.Mismatches, comparison.Mismatches...)
	}

	switch {
	case factBudgetCut:
		// Candidates were left unverified; re-run this same window with a larger
		// fact budget before advancing.
		result.Complete = false
	case receiptBudgetCut:
		result.NextCursor.NextOffset = windowEnd
		result.Complete = false
	default:
		result.NextCursor.NextOffset = request.CloseOffset
		result.Complete = true
	}
	return result, nil
}

func classifyBatchCounters(receipt IngestAuditReceipt, fact factCounters) (ReconciliationMismatchReason, bool) {
	// Mirrors CompareIngestAudit's identity->count->counter priority. Topic and
	// partition are fixed by the query filter, so identity reduces to offsets.
	switch {
	case fact.FirstOffset != receipt.FirstOffset || fact.LastOffset != receipt.LastOffset:
		return MismatchIdentity, false
	case fact.Counters.SourceBatchCount != receipt.Counters.SourceBatchCount || fact.Counters.RecordCount != receipt.Counters.RecordCount:
		return MismatchCount, false
	case fact.Counters.RawBytes != receipt.Counters.RawBytes || fact.Counters.RawPackets != receipt.Counters.RawPackets ||
		fact.Counters.EstimatedBytes != receipt.Counters.EstimatedBytes || fact.Counters.EstimatedPackets != receipt.Counters.EstimatedPackets ||
		fact.Counters.EstimatedValidRecords != receipt.Counters.EstimatedValidRecords:
		return MismatchCounter, false
	default:
		return "", true
	}
}

func countCoveredFacts(factByBatch map[[32]byte]factCounters, receiptByBatch map[[32]byte]IngestAuditReceipt) int {
	covered := 0
	for batchID := range factByBatch {
		if _, ok := receiptByBatch[batchID]; ok {
			covered++
		}
	}
	return covered
}

func (s *ReconciliationScanner) readReceipts(ctx context.Context, request ReconciliationScanRequest) ([]IngestAuditReceipt, bool, error) {
	var (
		batchID          proto.ColFixedStr32
		firstOffset      proto.ColUInt64
		lastOffset       proto.ColUInt64
		sourceBatchCount proto.ColUInt32
		recordCount      proto.ColUInt64
		rawBytes         proto.ColUInt64
		rawPackets       proto.ColUInt64
		estimatedBytes   proto.ColUInt64
		estimatedPackets proto.ColUInt64
		estimatedValid   proto.ColUInt64
		checksum         proto.ColFixedStr32
	)
	receipts := make([]IngestAuditReceipt, 0, request.MaxBatches+1)
	query := ch.Query{
		Body: fmt.Sprintf(`SELECT ingest_batch_id, first_offset, last_offset, source_batch_count, record_count,
  raw_bytes, raw_packets, estimated_bytes, estimated_packets, estimated_valid_records, checksum
FROM flow_ingest_batches FINAL
WHERE kafka_topic = {topic:String} AND kafka_partition = %d
  AND first_offset >= %d AND last_offset < %d
ORDER BY first_offset, last_offset, ingest_batch_id
LIMIT %d`, request.Cursor.KafkaPartition, request.Cursor.NextOffset, request.CloseOffset, request.MaxBatches+1),
		Parameters: ch.Parameters(map[string]any{"topic": request.Cursor.KafkaTopic}),
		Settings:   reconciliationReadSettings(request.MaxReadBytes),
		Result: proto.Results{
			{Name: "ingest_batch_id", Data: &batchID}, {Name: "first_offset", Data: &firstOffset},
			{Name: "last_offset", Data: &lastOffset}, {Name: "source_batch_count", Data: &sourceBatchCount},
			{Name: "record_count", Data: &recordCount}, {Name: "raw_bytes", Data: &rawBytes},
			{Name: "raw_packets", Data: &rawPackets}, {Name: "estimated_bytes", Data: &estimatedBytes},
			{Name: "estimated_packets", Data: &estimatedPackets}, {Name: "estimated_valid_records", Data: &estimatedValid},
			{Name: "checksum", Data: &checksum},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for row := 0; row < block.Rows; row++ {
			receipts = append(receipts, IngestAuditReceipt{
				BatchID: batchID.Row(row), KafkaTopic: request.Cursor.KafkaTopic, KafkaPartition: request.Cursor.KafkaPartition,
				FirstOffset: firstOffset[row], LastOffset: lastOffset[row], Checksum: checksum.Row(row),
				Counters: IngestAuditCounters{
					SourceBatchCount: sourceBatchCount[row], RecordCount: recordCount[row],
					RawBytes: rawBytes[row], RawPackets: rawPackets[row],
					EstimatedBytes: estimatedBytes[row], EstimatedPackets: estimatedPackets[row],
					EstimatedValidRecords: estimatedValid[row],
				},
			})
		}
		return nil
	}
	if err := s.executor.Do(ctx, query); err != nil {
		return nil, false, classifyClickHouseError(fmt.Errorf("read ingest reconciliation receipts: %w", err))
	}
	if len(receipts) > request.MaxBatches {
		return receipts[:request.MaxBatches], true, nil
	}
	return receipts, false, nil
}

func (s *ReconciliationScanner) readFactCounters(ctx context.Context, request ReconciliationScanRequest, windowEnd uint64) (map[[32]byte]factCounters, error) {
	var (
		batchID          proto.ColFixedStr32
		recordCount      proto.ColUInt64
		sourceBatchCount proto.ColUInt64
		firstOffset      proto.ColUInt64
		lastOffset       proto.ColUInt64
		rawBytes         proto.ColUInt64
		rawPackets       proto.ColUInt64
		estimatedBytes   proto.ColUInt64
		estimatedPackets proto.ColUInt64
		estimatedValid   proto.ColUInt64
	)
	counters := make(map[[32]byte]factCounters)
	query := ch.Query{
		Body: fmt.Sprintf(`SELECT ingest_batch_id,
  count() AS record_count,
  uniqExact(kafka_offset) AS source_batch_count,
  min(kafka_offset) AS first_offset,
  max(kafka_offset) AS last_offset,
  sum(raw_bytes) AS raw_bytes,
  sum(raw_packets) AS raw_packets,
  sumIf(estimated_bytes, estimated_valid) AS estimated_bytes,
  sumIf(estimated_packets, estimated_valid) AS estimated_packets,
  countIf(estimated_valid) AS estimated_valid_records
FROM (
  SELECT record_id,
    argMax(ingest_batch_id, ingest_generation) AS ingest_batch_id,
    argMax(kafka_offset, ingest_generation) AS kafka_offset,
    argMax(raw_bytes, ingest_generation) AS raw_bytes,
    argMax(raw_packets, ingest_generation) AS raw_packets,
    argMax(estimated_valid, ingest_generation) AS estimated_valid,
    argMax(estimated_bytes, ingest_generation) AS estimated_bytes,
    argMax(estimated_packets, ingest_generation) AS estimated_packets
  FROM flow_records
  WHERE flow_records.kafka_topic = {topic:String} AND flow_records.kafka_partition = %d
    AND flow_records.kafka_offset >= %d AND flow_records.kafka_offset < %d
  GROUP BY record_id
)
GROUP BY ingest_batch_id`, request.Cursor.KafkaPartition, request.Cursor.NextOffset, windowEnd),
		Parameters: ch.Parameters(map[string]any{"topic": request.Cursor.KafkaTopic}),
		Settings:   reconciliationReadSettings(request.MaxReadBytes),
		Result: proto.Results{
			{Name: "ingest_batch_id", Data: &batchID}, {Name: "record_count", Data: &recordCount},
			{Name: "source_batch_count", Data: &sourceBatchCount}, {Name: "first_offset", Data: &firstOffset},
			{Name: "last_offset", Data: &lastOffset}, {Name: "raw_bytes", Data: &rawBytes},
			{Name: "raw_packets", Data: &rawPackets}, {Name: "estimated_bytes", Data: &estimatedBytes},
			{Name: "estimated_packets", Data: &estimatedPackets}, {Name: "estimated_valid_records", Data: &estimatedValid},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for row := 0; row < block.Rows; row++ {
			counters[batchID.Row(row)] = factCounters{
				FirstOffset: firstOffset[row], LastOffset: lastOffset[row],
				Counters: IngestAuditCounters{
					SourceBatchCount: uint32(sourceBatchCount[row]), RecordCount: recordCount[row],
					RawBytes: rawBytes[row], RawPackets: rawPackets[row],
					EstimatedBytes: estimatedBytes[row], EstimatedPackets: estimatedPackets[row],
					EstimatedValidRecords: estimatedValid[row],
				},
			}
		}
		return nil
	}
	if err := s.executor.Do(ctx, query); err != nil {
		return nil, classifyClickHouseError(fmt.Errorf("read ingest reconciliation fact counters: %w", err))
	}
	return counters, nil
}

func (s *ReconciliationScanner) readCandidateFacts(ctx context.Context, request ReconciliationScanRequest, windowEnd uint64, candidates [][32]byte) ([]IngestAuditFact, bool, error) {
	placeholders := make([]string, 0, len(candidates))
	parameters := map[string]any{"topic": request.Cursor.KafkaTopic}
	for index, batchID := range candidates {
		key := fmt.Sprintf("b%d", index)
		placeholders = append(placeholders, fmt.Sprintf("{%s:String}", key))
		parameters[key] = fmt.Sprintf("%x", batchID)
	}
	var (
		recordID              proto.ColFixedStr32
		batchID               proto.ColFixedStr32
		kafkaOffset           proto.ColUInt64
		recordIndex           proto.ColUInt32
		rawBytes              proto.ColUInt64
		rawPackets            proto.ColUInt64
		estimatedValid        proto.ColBool
		estimatedBytes        proto.ColUInt64
		estimatedPackets      proto.ColUInt64
		qualityFlags          proto.ColUInt64
		dimensionFingerprint  proto.ColUInt64
		classificationVersion proto.ColUInt32
	)
	facts := make([]IngestAuditFact, 0, request.MaxFactRows+1)
	query := ch.Query{
		Body: fmt.Sprintf(`SELECT record_id, ingest_batch_id, kafka_offset, record_index,
  raw_bytes, raw_packets, estimated_valid, estimated_bytes, estimated_packets,
  quality_flags, dimension_fingerprint, classification_version
FROM (
  SELECT record_id,
    argMax(ingest_batch_id, ingest_generation) AS ingest_batch_id,
    argMax(kafka_offset, ingest_generation) AS kafka_offset,
    argMax(record_index, ingest_generation) AS record_index,
    argMax(raw_bytes, ingest_generation) AS raw_bytes,
    argMax(raw_packets, ingest_generation) AS raw_packets,
    argMax(estimated_valid, ingest_generation) AS estimated_valid,
    argMax(estimated_bytes, ingest_generation) AS estimated_bytes,
    argMax(estimated_packets, ingest_generation) AS estimated_packets,
    argMax(quality_flags, ingest_generation) AS quality_flags,
    argMax(dimension_fingerprint, ingest_generation) AS dimension_fingerprint,
    argMax(classification_version, ingest_generation) AS classification_version
  FROM flow_records
  WHERE flow_records.kafka_topic = {topic:String} AND flow_records.kafka_partition = %d
    AND flow_records.kafka_offset >= %d AND flow_records.kafka_offset < %d
  GROUP BY record_id
)
WHERE lower(hex(ingest_batch_id)) IN (%s)
LIMIT %d`, request.Cursor.KafkaPartition, request.Cursor.NextOffset, windowEnd, strings.Join(placeholders, ", "), request.MaxFactRows+1),
		Parameters: ch.Parameters(parameters),
		Settings:   reconciliationReadSettings(request.MaxReadBytes),
		Result: proto.Results{
			{Name: "record_id", Data: &recordID}, {Name: "ingest_batch_id", Data: &batchID},
			{Name: "kafka_offset", Data: &kafkaOffset}, {Name: "record_index", Data: &recordIndex},
			{Name: "raw_bytes", Data: &rawBytes}, {Name: "raw_packets", Data: &rawPackets},
			{Name: "estimated_valid", Data: &estimatedValid}, {Name: "estimated_bytes", Data: &estimatedBytes},
			{Name: "estimated_packets", Data: &estimatedPackets}, {Name: "quality_flags", Data: &qualityFlags},
			{Name: "dimension_fingerprint", Data: &dimensionFingerprint}, {Name: "classification_version", Data: &classificationVersion},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for row := 0; row < block.Rows; row++ {
			facts = append(facts, IngestAuditFact{
				RecordID: recordID.Row(row), BatchID: batchID.Row(row),
				KafkaTopic: request.Cursor.KafkaTopic, KafkaPartition: request.Cursor.KafkaPartition,
				KafkaOffset: kafkaOffset[row], RecordIndex: recordIndex[row],
				RawBytes: rawBytes[row], RawPackets: rawPackets[row], EstimatedValid: estimatedValid[row],
				EstimatedBytes: estimatedBytes[row], EstimatedPackets: estimatedPackets[row],
				QualityFlags: qualityFlags[row], DimensionFingerprint: dimensionFingerprint[row],
				ClassificationVersion: classificationVersion[row],
			})
		}
		return nil
	}
	if err := s.executor.Do(ctx, query); err != nil {
		return nil, false, classifyClickHouseError(fmt.Errorf("read ingest reconciliation candidate facts: %w", err))
	}
	if len(facts) > request.MaxFactRows {
		return facts[:request.MaxFactRows], true, nil
	}
	return facts, false, nil
}

func reconciliationReadSettings(maxReadBytes uint64) []ch.Setting {
	return []ch.Setting{
		{Key: "max_bytes_to_read", Value: fmt.Sprintf("%d", maxReadBytes), Important: true},
		{Key: "read_overflow_mode", Value: "throw", Important: true},
	}
}

func validateReconciliationScanRequest(request ReconciliationScanRequest) error {
	if request.Cursor.KafkaTopic == "" {
		return errors.New("reconciliation scan requires a kafka topic")
	}
	if request.Cursor.NextOffset > request.CloseOffset {
		return errors.New("reconciliation scan cursor is past the close watermark")
	}
	if request.MaxBatches < 1 || request.MaxBatches > hardReconcileBatches {
		return fmt.Errorf("reconciliation scan max batches must be 1..%d", hardReconcileBatches)
	}
	if request.MaxFactRows < 1 || request.MaxFactRows > hardReconcileFacts {
		return fmt.Errorf("reconciliation scan max fact rows must be 1..%d", hardReconcileFacts)
	}
	if request.MaxReadBytes == 0 {
		return errors.New("reconciliation scan max read bytes must be positive")
	}
	return validateReconciliationCompareLimits(request.CompareLimits)
}
