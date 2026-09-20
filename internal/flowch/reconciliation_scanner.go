// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// ReconciliationScanner compares one stable source-message receipt per Kafka
// offset with generation-deduplicated facts. All comparison work is count and
// counter based; V1 candidate-row checksum reads no longer exist.
type ReconciliationScanner struct {
	executor queryExecutor
}

func NewReconciliationScanner(native *NativeInserter) (*ReconciliationScanner, error) {
	if native == nil || native.executor == nil {
		return nil, errors.New("ClickHouse reconciliation scanner requires a native inserter")
	}
	return &ReconciliationScanner{executor: native.executor}, nil
}

type ReconciliationScanCursor struct {
	SourceStreamID string
	KafkaTopic     string
	KafkaPartition uint32
	NextOffset     uint64
}

type ReconciliationScanRequest struct {
	Cursor        ReconciliationScanCursor
	CloseOffset   uint64
	MaxBatches    int
	MaxFactRows   int
	MaxReadBytes  uint64
	CompareLimits ReconciliationCompareLimits
}

type ReconciliationScanResult struct {
	Comparison       IngestReconciliationComparison
	NextCursor       ReconciliationScanCursor
	Complete         bool
	ReceiptBatches   int
	CandidateBatches int // Always zero in receipt schema v3; kept for metric compatibility.
	Facts            int
}

type factCounters struct {
	KafkaTopic     string
	TopicCount     uint64
	MinRecordIndex uint32
	MaxRecordIndex uint32
	UniqueRecords  uint64
	Counters       IngestAuditCounters
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
	// One V2 receipt is required for every Kafka offset that may be committed.
	// Bound the contiguous source-coordinate window up front; deriving the
	// window only from rows that exist would make an offset where both receipt
	// and facts vanished invisible to reconciliation.
	windowEnd := request.CloseOffset
	if budgetEnd := request.Cursor.NextOffset + uint64(request.MaxBatches); budgetEnd < request.Cursor.NextOffset || budgetEnd > request.CloseOffset {
		windowEnd = request.CloseOffset
	} else {
		windowEnd = budgetEnd
	}
	boundedRequest := request
	boundedRequest.CloseOffset = windowEnd

	receipts, receiptCut, err := s.readReceipts(ctx, boundedRequest)
	if err != nil {
		return ReconciliationScanResult{}, err
	}
	facts, factCut, err := s.readFactCounters(ctx, boundedRequest)
	if err != nil {
		return ReconciliationScanResult{}, err
	}
	if receiptCut < windowEnd {
		windowEnd = receiptCut
	}
	if factCut < windowEnd {
		windowEnd = factCut
	}

	receiptByOffset := make(map[uint64]IngestAuditReceipt)
	for _, receipt := range receipts {
		if receipt.KafkaOffset < windowEnd {
			receiptByOffset[receipt.KafkaOffset] = receipt
		}
	}
	factByOffset := make(map[uint64]factCounters)
	var factRows uint64
	factOffsets := make([]uint64, 0, len(facts))
	for offset := range facts {
		factOffsets = append(factOffsets, offset)
	}
	sort.Slice(factOffsets, func(i, j int) bool { return factOffsets[i] < factOffsets[j] })
	for _, offset := range factOffsets {
		fact := facts[offset]
		if offset >= windowEnd {
			continue
		}
		if factRows+fact.Counters.RecordCount > uint64(request.MaxFactRows) {
			if offset < windowEnd {
				windowEnd = offset
			}
			continue
		}
		factRows += fact.Counters.RecordCount
		factByOffset[offset] = fact
	}
	// A map iteration may discover a lower budget cut after higher offsets were
	// added; filter again so the result and cursor cover one contiguous window.
	for offset := range receiptByOffset {
		if offset >= windowEnd {
			delete(receiptByOffset, offset)
		}
	}
	factRows = 0
	for offset, fact := range factByOffset {
		if offset >= windowEnd {
			delete(factByOffset, offset)
			continue
		}
		factRows += fact.Counters.RecordCount
	}

	comparison := IngestReconciliationComparison{Batches: windowEnd - request.Cursor.NextOffset, Facts: factRows}
	for offset := request.Cursor.NextOffset; offset < windowEnd; offset++ {
		receipt, hasReceipt := receiptByOffset[offset]
		fact, hasFacts := factByOffset[offset]
		key := SourceMessageKey{SourceStreamID: request.Cursor.SourceStreamID, KafkaPartition: request.Cursor.KafkaPartition, KafkaOffset: offset}
		switch {
		case !hasReceipt:
			topic := request.Cursor.KafkaTopic
			actual := IngestAuditCounters{}
			if hasFacts {
				topic = fact.KafkaTopic
				actual = fact.Counters
			}
			comparison.Mismatches = append(comparison.Mismatches, IngestReconciliationMismatch{
				Reason: MismatchMissingReceipt, SourceMessageKey: key, KafkaTopic: topic, Actual: actual,
			})
		case receipt.Disposition != IngestDispositionPersisted:
			if hasFacts {
				comparison.Mismatches = append(comparison.Mismatches, mismatchFromReceipt(MismatchIdentity, receipt, fact.Counters))
			}
		case !hasFacts:
			// Counter-only persisted messages store sFlow interface counters with
			// zero flow-record rows, so absent facts are expected. This scanner
			// only reads flow_records; confirming the counter rows exist in
			// sflow_interface_counters is a separate reconciliation. Flag missing
			// records only when the receipt claims flow records.
			if receipt.Counters.RecordCount > 0 {
				comparison.Mismatches = append(comparison.Mismatches, mismatchFromReceipt(MismatchMissingRecords, receipt, IngestAuditCounters{}))
			}
		default:
			reason := classifyMessageCounters(receipt, fact)
			if reason != "" {
				comparison.Mismatches = append(comparison.Mismatches, mismatchFromReceipt(reason, receipt, fact.Counters))
			}
		}
	}

	result.Comparison = comparison
	result.ReceiptBatches = len(receiptByOffset)
	result.Facts = int(factRows)
	if windowEnd < request.CloseOffset {
		result.NextCursor.NextOffset = windowEnd
		result.Complete = false
	} else {
		result.NextCursor.NextOffset = request.CloseOffset
		result.Complete = true
	}
	return result, nil
}

func classifyMessageCounters(receipt IngestAuditReceipt, fact factCounters) ReconciliationMismatchReason {
	identityValid := fact.TopicCount == 1 && fact.KafkaTopic == receipt.KafkaTopic &&
		fact.MinRecordIndex == 0 && fact.UniqueRecords == fact.Counters.RecordCount &&
		uint64(fact.MaxRecordIndex)+1 == fact.Counters.RecordCount
	switch {
	case !identityValid:
		return MismatchIdentity
	case fact.Counters.RecordCount != receipt.Counters.RecordCount:
		return MismatchCount
	case !auditCountersEqual(fact.Counters, receipt.Counters):
		return MismatchCounter
	default:
		return ""
	}
}

func (s *ReconciliationScanner) readReceipts(ctx context.Context, request ReconciliationScanRequest) ([]IngestAuditReceipt, uint64, error) {
	var (
		kafkaTopic         = new(proto.ColStr).LowCardinality()
		disposition        proto.ColEnum
		kafkaOffset        proto.ColUInt64
		recordCount        proto.ColUInt64
		counterRecordCount proto.ColUInt64
		rawBytes           proto.ColUInt64
		rawPackets         proto.ColUInt64
		estimatedBytes     proto.ColUInt64
		estimatedPackets   proto.ColUInt64
		estimatedValid     proto.ColUInt64
	)
	receipts := make([]IngestAuditReceipt, 0, request.MaxBatches+1)
	query := ch.Query{
		Body: fmt.Sprintf(`SELECT kafka_topic, message_disposition, kafka_offset, record_count, counter_record_count,
  raw_bytes, raw_packets, estimated_bytes, estimated_packets, estimated_valid_records
FROM flow_ingest_receipts FINAL
WHERE source_stream_id = {stream:String} AND kafka_partition = %d
  AND kafka_offset >= %d AND kafka_offset < %d
ORDER BY kafka_offset
LIMIT %d`, request.Cursor.KafkaPartition, request.Cursor.NextOffset, request.CloseOffset, request.MaxBatches+1),
		Parameters: ch.Parameters(map[string]any{"stream": request.Cursor.SourceStreamID}),
		Settings:   reconciliationReadSettings(request.MaxReadBytes),
		Result: proto.Results{
			{Name: "kafka_topic", Data: kafkaTopic}, {Name: "message_disposition", Data: &disposition}, {Name: "kafka_offset", Data: &kafkaOffset},
			{Name: "record_count", Data: &recordCount}, {Name: "counter_record_count", Data: &counterRecordCount},
			{Name: "raw_bytes", Data: &rawBytes}, {Name: "raw_packets", Data: &rawPackets}, {Name: "estimated_bytes", Data: &estimatedBytes},
			{Name: "estimated_packets", Data: &estimatedPackets}, {Name: "estimated_valid_records", Data: &estimatedValid},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for row := 0; row < block.Rows; row++ {
			receipts = append(receipts, IngestAuditReceipt{
				SourceMessageKey: SourceMessageKey{SourceStreamID: request.Cursor.SourceStreamID, KafkaPartition: request.Cursor.KafkaPartition, KafkaOffset: kafkaOffset[row]},
				KafkaTopic:       kafkaTopic.Row(row), Disposition: IngestMessageDisposition(disposition.Row(row)),
				CounterRecordCount: counterRecordCount[row],
				Counters: IngestAuditCounters{RecordCount: recordCount[row], RawBytes: rawBytes[row], RawPackets: rawPackets[row],
					EstimatedBytes: estimatedBytes[row], EstimatedPackets: estimatedPackets[row], EstimatedValidRecords: estimatedValid[row]},
			})
		}
		return nil
	}
	if err := s.executor.Do(ctx, query); err != nil {
		return nil, 0, classifyClickHouseError(fmt.Errorf("read ingest reconciliation receipts: %w", err))
	}
	if len(receipts) > request.MaxBatches {
		cut := receipts[request.MaxBatches].KafkaOffset
		return receipts[:request.MaxBatches], cut, nil
	}
	return receipts, request.CloseOffset, nil
}

func (s *ReconciliationScanner) readFactCounters(ctx context.Context, request ReconciliationScanRequest) (map[uint64]factCounters, uint64, error) {
	var (
		kafkaTopic       = new(proto.ColStr)
		topicCount       proto.ColUInt64
		kafkaOffset      proto.ColUInt64
		minRecordIndex   proto.ColUInt32
		maxRecordIndex   proto.ColUInt32
		uniqueRecords    proto.ColUInt64
		recordCount      proto.ColUInt64
		rawBytes         proto.ColUInt64
		rawPackets       proto.ColUInt64
		estimatedBytes   proto.ColUInt64
		estimatedPackets proto.ColUInt64
		estimatedValid   proto.ColUInt64
	)
	facts := make(map[uint64]factCounters)
	offsets := make([]uint64, 0, request.MaxBatches+1)
	query := ch.Query{
		Body: fmt.Sprintf(`SELECT any(kafka_topic) AS topic_name, uniqExact(kafka_topic) AS topic_count,
  kafka_offset, min(record_index) AS min_record_index, max(record_index) AS max_record_index,
  uniqExact(record_index) AS unique_records, count() AS record_count,
  sum(raw_bytes) AS raw_bytes, sum(raw_packets) AS raw_packets,
  sumIf(estimated_bytes, estimated_valid) AS estimated_bytes,
  sumIf(estimated_packets, estimated_valid) AS estimated_packets,
  countIf(estimated_valid) AS estimated_valid_records
FROM flow_records FINAL
WHERE source_stream_id = {stream:String} AND kafka_partition = %d
  AND kafka_offset >= %d AND kafka_offset < %d
GROUP BY kafka_offset
ORDER BY kafka_offset
LIMIT %d`, request.Cursor.KafkaPartition, request.Cursor.NextOffset, request.CloseOffset, request.MaxBatches+1),
		Parameters: ch.Parameters(map[string]any{"stream": request.Cursor.SourceStreamID}),
		Settings:   reconciliationReadSettings(request.MaxReadBytes),
		Result: proto.Results{
			{Name: "topic_name", Data: kafkaTopic}, {Name: "topic_count", Data: &topicCount}, {Name: "kafka_offset", Data: &kafkaOffset},
			{Name: "min_record_index", Data: &minRecordIndex}, {Name: "max_record_index", Data: &maxRecordIndex},
			{Name: "unique_records", Data: &uniqueRecords}, {Name: "record_count", Data: &recordCount},
			{Name: "raw_bytes", Data: &rawBytes}, {Name: "raw_packets", Data: &rawPackets},
			{Name: "estimated_bytes", Data: &estimatedBytes}, {Name: "estimated_packets", Data: &estimatedPackets},
			{Name: "estimated_valid_records", Data: &estimatedValid},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for row := 0; row < block.Rows; row++ {
			offset := kafkaOffset[row]
			offsets = append(offsets, offset)
			facts[offset] = factCounters{
				KafkaTopic: kafkaTopic.Row(row), TopicCount: topicCount[row], MinRecordIndex: minRecordIndex[row],
				MaxRecordIndex: maxRecordIndex[row], UniqueRecords: uniqueRecords[row],
				Counters: IngestAuditCounters{RecordCount: recordCount[row], RawBytes: rawBytes[row], RawPackets: rawPackets[row],
					EstimatedBytes: estimatedBytes[row], EstimatedPackets: estimatedPackets[row], EstimatedValidRecords: estimatedValid[row]},
			}
		}
		return nil
	}
	if err := s.executor.Do(ctx, query); err != nil {
		return nil, 0, classifyClickHouseError(fmt.Errorf("read ingest reconciliation fact counters: %w", err))
	}
	if len(offsets) > request.MaxBatches {
		cut := offsets[request.MaxBatches]
		delete(facts, cut)
		return facts, cut, nil
	}
	return facts, request.CloseOffset, nil
}

func reconciliationReadSettings(maxReadBytes uint64) []ch.Setting {
	return []ch.Setting{
		{Key: "max_bytes_to_read", Value: fmt.Sprintf("%d", maxReadBytes), Important: true},
		{Key: "read_overflow_mode", Value: "throw", Important: true},
	}
}

func validateReconciliationScanRequest(request ReconciliationScanRequest) error {
	if request.Cursor.SourceStreamID == "" || request.Cursor.KafkaTopic == "" {
		return errors.New("reconciliation scan requires a source stream and kafka topic")
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
