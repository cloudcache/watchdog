// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

// Package flowch owns the ClickHouse persistence boundary for enriched flows.
package flowch

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/cloudcache/watchdog/internal/flowworker"
)

const (
	WorkerSchemaVersion = 3
	defaultMaxRows      = 50_000
	defaultMaxBytes     = 64 << 20
	hardMaxRows         = 1_000_000
	hardMaxBytes        = 1 << 30
	// A block may not touch more distinct event-time days (toYYYYMMDD partitions)
	// than this; ClickHouse rejects an insert exceeding max_partitions_per_insert_
	// block (default 100), and that error is retryable, so a replay/backfill block
	// spanning many days would otherwise retry forever and stall the partition.
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

type PreparedBlock struct {
	ID                    [32]byte
	Checksum              [32]byte
	KafkaTopic            string
	KafkaPartition        int32
	FirstOffset           int64
	LastOffset            int64
	SourceBatchCount      uint32
	TenantIDs             []string
	RawBytes              uint64
	RawPackets            uint64
	EstimatedBytes        uint64
	EstimatedPackets      uint64
	EstimatedValidRecords uint64
	MinEventTime          time.Time
	MaxEventTime          time.Time
	ApproxBytes           int
	Records               []RecordRef
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
	current := PreparedBlock{KafkaTopic: batches[0].KafkaTopic, KafkaPartition: batches[0].KafkaPartition}
	currentTenants := make(map[string]struct{})
	currentDays := make(map[int32]struct{})
	currentOffset := int64(-1)
	flush := func() error {
		if len(current.Records) == 0 {
			return nil
		}
		current.TenantIDs = make([]string, 0, len(currentTenants))
		for tenantID := range currentTenants {
			current.TenantIDs = append(current.TenantIDs, tenantID)
		}
		sort.Strings(current.TenantIDs)
		current.Checksum = blockChecksum(current.Records)
		current.ID = blockID(current)
		blocks = append(blocks, current)
		current = PreparedBlock{KafkaTopic: batches[0].KafkaTopic, KafkaPartition: batches[0].KafkaPartition}
		clear(currentTenants)
		clear(currentDays)
		currentOffset = -1
		return nil
	}

	for _, batch := range batches {
		for index := range batch.Records {
			record := &batch.Records[index]
			recordBytes := approximateRecordBytes(batch, record)
			if recordBytes > limits.MaxApproxBytes {
				return nil, fmt.Errorf("%w: record %x exceeds block byte limit", ErrInvalidBatchGroup, record.SourceRecordID[:8])
			}
			eventTime := record.EventTime.UTC()
			// toYYYYMMDD partitions on UTC calendar days; bound the distinct days a
			// single insert block may touch so it can never exceed ClickHouse's
			// max_partitions_per_insert_block.
			partitionDay := int32(eventTime.Unix() / 86400)
			_, sameDay := currentDays[partitionDay]
			exceedsPartitionDays := !sameDay && len(currentDays) >= limits.MaxPartitionDays
			if len(current.Records) > 0 && (len(current.Records) == limits.MaxRows || current.ApproxBytes > limits.MaxApproxBytes-recordBytes || exceedsPartitionDays) {
				if err := flush(); err != nil {
					return nil, err
				}
			}
			if len(current.Records) == 0 {
				current.FirstOffset = batch.KafkaOffset
			}
			current.LastOffset = batch.KafkaOffset
			if currentOffset != batch.KafkaOffset {
				current.SourceBatchCount++
				currentTenants[batch.TenantID] = struct{}{}
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
			currentDays[partitionDay] = struct{}{}
			current.ApproxBytes += recordBytes
			current.Records = append(current.Records, RecordRef{Batch: batch, Record: record})
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
	if len(batches) == 0 || batches[0] == nil || batches[0].KafkaTopic == "" || batches[0].KafkaPartition < 0 {
		return fmt.Errorf("%w: ordered partition batches are required", ErrInvalidBatchGroup)
	}
	for index, batch := range batches {
		if batch == nil || batch.SchemaVersion != flowworker.EnrichedBatchSchemaVersion || batch.KafkaTopic != batches[0].KafkaTopic || batch.KafkaPartition != batches[0].KafkaPartition || batch.KafkaOffset < 0 || batch.TenantID == "" || len(batch.Records) == 0 {
			return fmt.Errorf("%w: source batch %d has invalid identity or records", ErrInvalidBatchGroup, index)
		}
		if index > 0 && batches[index-1].KafkaOffset >= batch.KafkaOffset {
			return fmt.Errorf("%w: Kafka offsets must be strictly increasing", ErrInvalidBatchGroup)
		}
		for recordIndex := range batch.Records {
			record := &batch.Records[recordIndex]
			if record.SourceRecordID == ([32]byte{}) || record.EventTime.IsZero() {
				return fmt.Errorf("%w: source batch %d record %d is incomplete", ErrInvalidBatchGroup, index, recordIndex)
			}
		}
	}
	return nil
}

func approximateRecordBytes(batch *flowworker.EnrichedBatch, record *flowworker.EnrichedRecord) int {
	// Fixed-width ClickHouse values, column offsets and conservative protocol
	// overhead. Variable strings/arrays are counted exactly by byte length.
	size := 512 + len(batch.KafkaTopic) + len(batch.TenantID) + len(batch.CollectorID) + len(batch.ExporterID)
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

func blockChecksum(records []RecordRef) [32]byte {
	hash := sha256.New()
	var number [8]byte
	for _, ref := range records {
		hash.Write(ref.Record.SourceRecordID[:])
		binary.BigEndian.PutUint64(number[:], ref.Record.RawBytes)
		hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], ref.Record.RawPackets)
		hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], ref.Record.EstimatedBytes)
		hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], ref.Record.EstimatedPackets)
		hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], ref.Record.QualityFlags)
		hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], ref.Record.DimensionFingerprint)
		hash.Write(number[:])
		binary.BigEndian.PutUint32(number[:4], ref.Record.ClassificationVersion)
		hash.Write(number[:4])
		if ref.Record.EstimatedValid {
			hash.Write([]byte{1})
		} else {
			hash.Write([]byte{0})
		}
	}
	var checksum [32]byte
	copy(checksum[:], hash.Sum(nil))
	return checksum
}

func blockID(block PreparedBlock) [32]byte {
	hash := sha256.New()
	hash.Write([]byte("watchdog-flow-ch-block-v1\x00"))
	hash.Write([]byte(block.KafkaTopic))
	hash.Write([]byte{0})
	var number [8]byte
	binary.BigEndian.PutUint32(number[:4], uint32(block.KafkaPartition))
	hash.Write(number[:4])
	binary.BigEndian.PutUint64(number[:], uint64(block.FirstOffset))
	hash.Write(number[:])
	binary.BigEndian.PutUint64(number[:], uint64(block.LastOffset))
	hash.Write(number[:])
	hash.Write(block.Checksum[:])
	var id [32]byte
	copy(id[:], hash.Sum(nil))
	return id
}
