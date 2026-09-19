// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

// Package flowch owns the ClickHouse persistence boundary for enriched flows.
package flowch

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cloudcache/watchdog/internal/flowworker"
)

const (
	WorkerSchemaVersion = 6
	defaultMaxRows      = 50_000
	defaultMaxBytes     = 64 << 20
	hardMaxRows         = 1_000_000
	hardMaxBytes        = 1 << 30
	// A block may not touch more distinct UTC event-time day partitions than
	// this; ClickHouse rejects an insert exceeding
	// max_partitions_per_insert_block (default 100). A wide-span replay would
	// otherwise retry forever and stall the Kafka partition.
	defaultMaxPartitionDays = 90
	hardMaxPartitionDays    = 100
)

var ErrInvalidBatchGroup = errors.New("invalid ClickHouse flow batch group")

type BatchLimits struct {
	MaxRows          int
	MaxApproxBytes   int
	MaxPartitionDays int
}

type RecordRef struct {
	Batch  *flowworker.EnrichedBatch
	Record *flowworker.EnrichedRecord
}

type CounterRef struct {
	Batch   *flowworker.EnrichedBatch
	Counter *flowworker.InterfaceCounterRecord
}

type PreparedBlock struct {
	SourceStreamID        string
	KafkaTopic            string
	KafkaPartition        int32
	FirstOffset           int64
	FirstRecordIndex      uint32
	LastOffset            int64
	LastRecordIndex       uint32
	SourceBatchCount      uint32
	RawBytes              uint64
	RawPackets            uint64
	EstimatedBytes        uint64
	EstimatedPackets      uint64
	EstimatedValidRecords uint64
	MinEventTime          time.Time
	MaxEventTime          time.Time
	ApproxBytes           int
	Records               []RecordRef
	CounterRecords        []CounterRef
	Receipts              []PreparedReceipt
}

// PreparedReceipt is one stable audit row per Kafka message whose offset may be
// committed. Persisted and intentionally non-persisted outcomes are both
// represented, independently of the ClickHouse insert-block boundary.
type PreparedReceipt struct {
	Disposition           flowworker.MessageDisposition
	SourceStreamID        string
	KafkaTopic            string
	KafkaPartition        int32
	KafkaOffset           int64
	RecordCount           uint64
	CounterRecordCount    uint64
	RawBytes              uint64
	RawPackets            uint64
	EstimatedBytes        uint64
	EstimatedPackets      uint64
	EstimatedValidRecords uint64
	MinEventTime          time.Time
	MaxEventTime          time.Time
	ReceivedAt            time.Time
	// Generation is normally derived from ReceivedAt. Lifecycle dispositions
	// may set it explicitly so they deterministically replace an older receipt
	// for the same natural Kafka coordinate.
	Generation uint64
}

func PrepareBlocks(batches []*flowworker.EnrichedBatch, limits BatchLimits) ([]PreparedBlock, error) {
	limits, err := normalizeLimits(limits)
	if err != nil {
		return nil, err
	}
	if err := validateBatchGroup(batches); err != nil {
		return nil, err
	}

	blocks := make([]PreparedBlock, 0, 1)
	current := PreparedBlock{SourceStreamID: batches[0].SourceStreamID, KafkaTopic: batches[0].KafkaTopic, KafkaPartition: batches[0].KafkaPartition}
	type storagePartition struct {
		day int32
	}
	currentPartitions := make(map[storagePartition]struct{})
	currentOffset := int64(-1)
	blockEmpty := func() bool {
		return len(current.Records) == 0 && len(current.CounterRecords) == 0 && len(current.Receipts) == 0
	}
	blockRows := func() int { return len(current.Records) + len(current.CounterRecords) }
	flush := func() error {
		if blockEmpty() {
			return nil
		}
		blocks = append(blocks, current)
		current = PreparedBlock{SourceStreamID: batches[0].SourceStreamID, KafkaTopic: batches[0].KafkaTopic, KafkaPartition: batches[0].KafkaPartition}
		clear(currentPartitions)
		currentOffset = -1
		return nil
	}

	for _, batch := range batches {
		receipt, err := prepareReceipt(batch)
		if err != nil {
			return nil, err
		}
		receiptBytes := approximateReceiptBytes(receipt)
		if receiptBytes > limits.MaxApproxBytes {
			return nil, fmt.Errorf("%w: receipt %s/%d/%d exceeds block byte limit", ErrInvalidBatchGroup, batch.SourceStreamID, batch.KafkaPartition, batch.KafkaOffset)
		}
		if batch.MessageDisposition != flowworker.MessageDispositionPersisted {
			if !blockEmpty() && (blockRows() >= limits.MaxRows || len(current.Receipts) >= limits.MaxRows || current.ApproxBytes > limits.MaxApproxBytes-receiptBytes) {
				if err := flush(); err != nil {
					return nil, err
				}
			}
			if blockEmpty() {
				current.FirstOffset = batch.KafkaOffset
				current.FirstRecordIndex = 0
			}
			current.LastOffset = batch.KafkaOffset
			current.LastRecordIndex = 0
			current.SourceBatchCount++
			currentOffset = batch.KafkaOffset
			current.Receipts = append(current.Receipts, receipt)
			current.ApproxBytes += receiptBytes
			continue
		}
		for index := range batch.Records {
			record := &batch.Records[index]
			recordBytes := approximateRecordBytes(batch, record)
			if recordBytes > limits.MaxApproxBytes {
				return nil, fmt.Errorf("%w: record %s/%d/%d/%d exceeds block byte limit", ErrInvalidBatchGroup, batch.SourceStreamID, batch.KafkaPartition, batch.KafkaOffset, record.RecordIndex)
			}
			eventTime := record.EventTime.UTC()
			// Storage V2 partitions by toYYYYMMDD(event_time); bound that exact
			// physical cardinality, not just distinct calendar days.
			partition := storagePartition{day: int32(eventTime.Unix() / 86400)}
			_, samePartition := currentPartitions[partition]
			exceedsPartitions := !samePartition && len(currentPartitions) >= limits.MaxPartitionDays
			lastRecord := index == len(batch.Records)-1 && len(batch.CounterRecords) == 0
			requiredBytes := recordBytes
			if lastRecord {
				requiredBytes += receiptBytes
			}
			if requiredBytes > limits.MaxApproxBytes {
				return nil, fmt.Errorf("%w: record and receipt %s/%d/%d/%d exceed block byte limit", ErrInvalidBatchGroup, batch.SourceStreamID, batch.KafkaPartition, batch.KafkaOffset, record.RecordIndex)
			}
			if !blockEmpty() && (blockRows() == limits.MaxRows || current.ApproxBytes > limits.MaxApproxBytes-requiredBytes || exceedsPartitions) {
				if err := flush(); err != nil {
					return nil, err
				}
			}
			if blockEmpty() {
				current.FirstOffset = batch.KafkaOffset
				current.FirstRecordIndex = record.RecordIndex
			}
			current.LastOffset = batch.KafkaOffset
			current.LastRecordIndex = record.RecordIndex
			if currentOffset != batch.KafkaOffset {
				current.SourceBatchCount++
				currentOffset = batch.KafkaOffset
			}
			if current.RawBytes > math.MaxUint64-record.RawBytes {
				return nil, fmt.Errorf("%w: raw byte receipt overflow", ErrInvalidBatchGroup)
			}
			current.RawBytes += record.RawBytes
			if current.RawPackets > math.MaxUint64-record.RawPackets {
				return nil, fmt.Errorf("%w: raw packet receipt overflow", ErrInvalidBatchGroup)
			}
			current.RawPackets += record.RawPackets
			if record.EstimatedValid {
				if current.EstimatedBytes > math.MaxUint64-record.EstimatedBytes {
					return nil, fmt.Errorf("%w: estimated byte receipt overflow", ErrInvalidBatchGroup)
				}
				if current.EstimatedPackets > math.MaxUint64-record.EstimatedPackets {
					return nil, fmt.Errorf("%w: estimated packet receipt overflow", ErrInvalidBatchGroup)
				}
				current.EstimatedBytes += record.EstimatedBytes
				current.EstimatedPackets += record.EstimatedPackets
				current.EstimatedValidRecords++
			}
			if current.MinEventTime.IsZero() || eventTime.Before(current.MinEventTime) {
				current.MinEventTime = eventTime
			}
			if eventTime.After(current.MaxEventTime) {
				current.MaxEventTime = eventTime
			}
			currentPartitions[partition] = struct{}{}
			current.ApproxBytes += recordBytes
			current.Records = append(current.Records, RecordRef{Batch: batch, Record: record})
			if lastRecord {
				current.Receipts = append(current.Receipts, receipt)
				current.ApproxBytes += receiptBytes
			}
		}
		for index := range batch.CounterRecords {
			counter := &batch.CounterRecords[index]
			counterBytes := approximateCounterBytes(batch, counter)
			if counterBytes > limits.MaxApproxBytes {
				return nil, fmt.Errorf("%w: counter %s/%d/%d/%d/%d exceeds block byte limit", ErrInvalidBatchGroup, batch.SourceStreamID, batch.KafkaPartition, batch.KafkaOffset, counter.SampleIndex, counter.RecordIndex)
			}
			eventTime := time.UnixMilli(counter.EventTimeUnixMS).UTC()
			partition := storagePartition{day: int32(eventTime.Unix() / 86400)}
			_, samePartition := currentPartitions[partition]
			exceedsPartitions := !samePartition && len(currentPartitions) >= limits.MaxPartitionDays
			lastCounter := index == len(batch.CounterRecords)-1
			requiredBytes := counterBytes
			if lastCounter {
				requiredBytes += receiptBytes
			}
			if requiredBytes > limits.MaxApproxBytes {
				return nil, fmt.Errorf("%w: counter and receipt %s/%d/%d/%d/%d exceed block byte limit", ErrInvalidBatchGroup, batch.SourceStreamID, batch.KafkaPartition, batch.KafkaOffset, counter.SampleIndex, counter.RecordIndex)
			}
			if !blockEmpty() && (blockRows() == limits.MaxRows || current.ApproxBytes > limits.MaxApproxBytes-requiredBytes || exceedsPartitions) {
				if err := flush(); err != nil {
					return nil, err
				}
			}
			if blockEmpty() {
				current.FirstOffset = batch.KafkaOffset
				current.FirstRecordIndex = counter.RecordIndex
			}
			current.LastOffset = batch.KafkaOffset
			current.LastRecordIndex = counter.RecordIndex
			if currentOffset != batch.KafkaOffset {
				current.SourceBatchCount++
				currentOffset = batch.KafkaOffset
			}
			if current.MinEventTime.IsZero() || eventTime.Before(current.MinEventTime) {
				current.MinEventTime = eventTime
			}
			if eventTime.After(current.MaxEventTime) {
				current.MaxEventTime = eventTime
			}
			currentPartitions[partition] = struct{}{}
			current.ApproxBytes += counterBytes
			current.CounterRecords = append(current.CounterRecords, CounterRef{Batch: batch, Counter: counter})
			if lastCounter {
				current.Receipts = append(current.Receipts, receipt)
				current.ApproxBytes += receiptBytes
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return blocks, nil
}

func normalizeLimits(limits BatchLimits) (BatchLimits, error) {
	if limits.MaxRows == 0 {
		limits.MaxRows = defaultMaxRows
	}
	if limits.MaxApproxBytes == 0 {
		limits.MaxApproxBytes = defaultMaxBytes
	}
	if limits.MaxPartitionDays == 0 {
		limits.MaxPartitionDays = defaultMaxPartitionDays
	}
	if limits.MaxRows < 1 || limits.MaxRows > hardMaxRows || limits.MaxApproxBytes < 1 || limits.MaxApproxBytes > hardMaxBytes {
		return BatchLimits{}, fmt.Errorf("%w: rows must be 1..%d and bytes 1..%d", ErrInvalidBatchGroup, hardMaxRows, hardMaxBytes)
	}
	if limits.MaxPartitionDays < 1 || limits.MaxPartitionDays > hardMaxPartitionDays {
		return BatchLimits{}, fmt.Errorf("%w: partition days must be 1..%d", ErrInvalidBatchGroup, hardMaxPartitionDays)
	}
	return limits, nil
}

func validateBatchGroup(batches []*flowworker.EnrichedBatch) error {
	if len(batches) == 0 || batches[0] == nil || !flowworker.ValidSourceStreamID(batches[0].SourceStreamID) || batches[0].KafkaTopic == "" || batches[0].KafkaPartition < 0 {
		return fmt.Errorf("%w: ordered partition batches are required", ErrInvalidBatchGroup)
	}
	for index, batch := range batches {
		if batch == nil || batch.SchemaVersion != flowworker.EnrichedBatchSchemaVersion || batch.SourceStreamID != batches[0].SourceStreamID || batch.KafkaTopic != batches[0].KafkaTopic || batch.KafkaPartition != batches[0].KafkaPartition || batch.KafkaOffset < 0 || batch.ReceivedAt.UnixMilli() <= 0 || batch.MessageDisposition < flowworker.MessageDispositionPersisted || batch.MessageDisposition > flowworker.MessageDispositionMappingRejected {
			return fmt.Errorf("%w: source batch %d has invalid identity or records", ErrInvalidBatchGroup, index)
		}
		if batch.MessageDisposition == flowworker.MessageDispositionPersisted && len(batch.Records)+len(batch.CounterRecords) == 0 {
			return fmt.Errorf("%w: persisted source batch %d requires records", ErrInvalidBatchGroup, index)
		}
		if batch.MessageDisposition != flowworker.MessageDispositionPersisted && (len(batch.Records) != 0 || len(batch.CounterRecords) != 0) {
			return fmt.Errorf("%w: non-persisted source batch %d carries records", ErrInvalidBatchGroup, index)
		}
		if index > 0 && batches[index-1].KafkaOffset >= batch.KafkaOffset {
			return fmt.Errorf("%w: Kafka offsets must be strictly increasing", ErrInvalidBatchGroup)
		}
		for recordIndex := range batch.Records {
			record := &batch.Records[recordIndex]
			if record.EventTime.IsZero() {
				return fmt.Errorf("%w: source batch %d record %d is incomplete", ErrInvalidBatchGroup, index, recordIndex)
			}
		}
		for counterIndex := range batch.CounterRecords {
			counter := &batch.CounterRecords[counterIndex]
			if counter.EventTimeUnixMS <= 0 || counter.IfIndex == 0 || counter.TargetID == "" {
				return fmt.Errorf("%w: source batch %d counter %d is incomplete", ErrInvalidBatchGroup, index, counterIndex)
			}
		}
	}
	return nil
}

func approximateRecordBytes(batch *flowworker.EnrichedBatch, record *flowworker.EnrichedRecord) int {
	// Fixed-width ClickHouse values, column offsets and conservative protocol
	// overhead. Variable strings/arrays are counted exactly by byte length.
	size := 512 + len(batch.KafkaTopic) + len(batch.CollectorID) + len(batch.ExporterID)
	size += len(record.TargetID) + len(record.DeviceID) + len(record.Dimensions.SnapshotID) + len(record.Dimensions.Business)
	size += len(record.Dimensions.Local.PrefixID) + len(record.Dimensions.Remote.PrefixID)
	size += len(record.RemoteGeo.Country) + len(record.RemoteGeo.AdminCode) + len(record.RemoteGeo.Subdivision) + len(record.RemoteGeo.City) + len(record.RemoteGeo.Version)
	size += len(record.RemoteGeo.ContinentID) + len(record.RemoteGeo.RegionID) + len(record.RemoteGeo.CountryID) + len(record.RemoteGeo.ProvinceID) + len(record.RemoteGeo.CityID)
	size += len(record.SupplierRemoteGeo.Country) + len(record.SupplierRemoteGeo.AdminCode) + len(record.SupplierRemoteGeo.Subdivision) + len(record.SupplierRemoteGeo.City) + len(record.SupplierRemoteGeo.Version)
	size += len(record.SupplierRemoteGeo.ContinentID) + len(record.SupplierRemoteGeo.RegionID) + len(record.SupplierRemoteGeo.CountryID) + len(record.SupplierRemoteGeo.ProvinceID) + len(record.SupplierRemoteGeo.CityID)
	for index := 0; index < record.Dimensions.Local.AddressSets.Count(); index++ {
		id, _ := record.Dimensions.Local.AddressSets.At(index)
		size += len(id) + 8
	}
	for index := 0; index < record.Dimensions.Remote.AddressSets.Count(); index++ {
		id, _ := record.Dimensions.Remote.AddressSets.At(index)
		size += len(id) + 8
	}
	return size
}

func approximateCounterBytes(batch *flowworker.EnrichedBatch, record *flowworker.InterfaceCounterRecord) int {
	return 256 + len(batch.SourceStreamID) + len(batch.KafkaTopic) + len(batch.CollectorID) + len(batch.ExporterID) + len(record.TargetID) + len(record.DeviceID)
}

func prepareReceipt(batch *flowworker.EnrichedBatch) (PreparedReceipt, error) {
	receipt := PreparedReceipt{
		Disposition:    batch.MessageDisposition,
		SourceStreamID: batch.SourceStreamID, KafkaTopic: batch.KafkaTopic,
		KafkaPartition: batch.KafkaPartition, KafkaOffset: batch.KafkaOffset,
		RecordCount: uint64(len(batch.Records)), CounterRecordCount: uint64(len(batch.CounterRecords)),
		ReceivedAt: batch.ReceivedAt.UTC(),
	}
	for _, record := range batch.Records {
		if receipt.RawBytes > math.MaxUint64-record.RawBytes || receipt.RawPackets > math.MaxUint64-record.RawPackets {
			return PreparedReceipt{}, fmt.Errorf("%w: source-message raw counter overflow", ErrInvalidBatchGroup)
		}
		receipt.RawBytes += record.RawBytes
		receipt.RawPackets += record.RawPackets
		if record.EstimatedValid {
			if receipt.EstimatedBytes > math.MaxUint64-record.EstimatedBytes || receipt.EstimatedPackets > math.MaxUint64-record.EstimatedPackets {
				return PreparedReceipt{}, fmt.Errorf("%w: source-message estimated counter overflow", ErrInvalidBatchGroup)
			}
			receipt.EstimatedBytes += record.EstimatedBytes
			receipt.EstimatedPackets += record.EstimatedPackets
			receipt.EstimatedValidRecords++
		}
		if receipt.MinEventTime.IsZero() || record.EventTime.Before(receipt.MinEventTime) {
			receipt.MinEventTime = record.EventTime.UTC()
		}
		if record.EventTime.After(receipt.MaxEventTime) {
			receipt.MaxEventTime = record.EventTime.UTC()
		}
	}
	if receipt.MinEventTime.IsZero() {
		receipt.MinEventTime = receipt.ReceivedAt
		receipt.MaxEventTime = receipt.ReceivedAt
	}
	return receipt, nil
}

func approximateReceiptBytes(receipt PreparedReceipt) int {
	return 192 + len(receipt.SourceStreamID) + len(receipt.KafkaTopic)
}
