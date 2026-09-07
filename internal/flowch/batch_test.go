// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"errors"
	"math"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

func TestPrepareBlocksIsBoundedAndDeterministic(t *testing.T) {
	batches := []*flowworker.EnrichedBatch{
		testEnrichedBatch(10, testEnrichedRecord(1, 100, 1_000)),
		testEnrichedBatch(11, testEnrichedRecord(2, 200, 2_000), testEnrichedRecord(3, 300, 3_000)),
	}
	limits := BatchLimits{MaxRows: 2, MaxApproxBytes: 1 << 20}
	first, err := PrepareBlocks(batches, limits)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PrepareBlocks(batches, limits)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("replay changed prepared ClickHouse blocks")
	}
	if len(first) != 2 || len(first[0].Records) != 2 || len(first[1].Records) != 1 {
		t.Fatalf("unexpected blocks: %+v", first)
	}
	if first[0].FirstOffset != 10 || first[0].LastOffset != 11 || first[0].SourceBatchCount != 2 || first[0].RawBytes != 300 || first[0].RawPackets != 2 || first[0].EstimatedBytes != 3_000 || first[0].EstimatedPackets != 20 || first[0].EstimatedValidRecords != 2 {
		t.Fatalf("unexpected first receipt: %+v", first[0])
	}
	if !reflect.DeepEqual(first[0].TenantIDs, []string{"tenant-a"}) || !first[0].MinEventTime.Equal(time.Date(2026, 9, 5, 1, 2, 0, 0, time.UTC)) || !first[0].MaxEventTime.Equal(first[0].MinEventTime) {
		t.Fatalf("unexpected first audit metadata: %+v", first[0])
	}
	if blockDeduplicationToken(first[0]) == blockDeduplicationToken(first[1]) || len(first[0].Receipts) != 1 || len(first[1].Receipts) != 1 {
		t.Fatalf("invalid natural block/receipt identity: %+v", first)
	}
}

func TestPrepareBlocksReceiptIdentityDoesNotDependOnBlockBudget(t *testing.T) {
	batches := []*flowworker.EnrichedBatch{
		testEnrichedBatch(10, testEnrichedRecord(0, 100, 1_000), testEnrichedRecord(1, 200, 2_000), testEnrichedRecord(2, 300, 3_000)),
		testEnrichedBatch(11, testEnrichedRecord(0, 400, 4_000)),
	}
	widelyPacked, err := PrepareBlocks(batches, BatchLimits{MaxRows: 100, MaxApproxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	tightlyPacked, err := PrepareBlocks(batches, BatchLimits{MaxRows: 1, MaxApproxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	collect := func(blocks []PreparedBlock) []PreparedReceipt {
		var receipts []PreparedReceipt
		for _, block := range blocks {
			receipts = append(receipts, block.Receipts...)
		}
		return receipts
	}
	if first, second := collect(widelyPacked), collect(tightlyPacked); !reflect.DeepEqual(first, second) || len(first) != len(batches) {
		t.Fatalf("receipt identity changed with block budget: packed=%+v split=%+v", first, second)
	}
}

func TestPrepareBlocksCanonicalizesCrossTenantAuditMetadata(t *testing.T) {
	first := testEnrichedBatch(10, testEnrichedRecord(1, 100, 1_000))
	first.TenantID = "tenant-z"
	first.Records[0].EventTime = time.Date(2026, 9, 5, 1, 3, 0, 0, time.UTC)
	second := testEnrichedBatch(11, testEnrichedRecord(2, 200, 2_000))
	second.TenantID = "tenant-a"
	second.Records[0].EventTime = time.Date(2026, 9, 5, 1, 1, 0, 0, time.UTC)

	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{first, second}, BatchLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || !reflect.DeepEqual(blocks[0].TenantIDs, []string{"tenant-a", "tenant-z"}) {
		t.Fatalf("tenant IDs are not canonical: %+v", blocks)
	}
	if !blocks[0].MinEventTime.Equal(second.Records[0].EventTime) || !blocks[0].MaxEventTime.Equal(first.Records[0].EventTime) {
		t.Fatalf("event range=%s..%s", blocks[0].MinEventTime, blocks[0].MaxEventTime)
	}
}

func TestPrepareBlocksRetainsNonPersistedMessageReceipt(t *testing.T) {
	batch := testEnrichedBatch(10)
	batch.MessageDisposition = flowworker.MessageDispositionTemplateMissing
	batch.TenantID, batch.CollectorID, batch.ExporterID = "", "", ""
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{batch}, BatchLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || len(blocks[0].Records) != 0 || len(blocks[0].Receipts) != 1 ||
		blocks[0].Receipts[0].Disposition != flowworker.MessageDispositionTemplateMissing || blocks[0].Receipts[0].RecordCount != 0 ||
		len(blocks[0].Receipts[0].TenantIDs) != 0 {
		t.Fatalf("receipt-only blocks=%+v", blocks)
	}
}

func TestPrepareBlocksRejectsMixedPartitionAndReceiptOverflow(t *testing.T) {
	first := testEnrichedBatch(10, testEnrichedRecord(1, math.MaxUint64, 0))
	second := testEnrichedBatch(11, testEnrichedRecord(2, 1, 0))
	second.KafkaPartition++
	if _, err := PrepareBlocks([]*flowworker.EnrichedBatch{first, second}, BatchLimits{}); !errors.Is(err, ErrInvalidBatchGroup) {
		t.Fatalf("mixed partition error=%v", err)
	}
	second.KafkaPartition = first.KafkaPartition
	if _, err := PrepareBlocks([]*flowworker.EnrichedBatch{first, second}, BatchLimits{}); !errors.Is(err, ErrInvalidBatchGroup) {
		t.Fatalf("overflow error=%v", err)
	}
	second = testEnrichedBatch(11, testEnrichedRecord(2, 1, 0))
	second.TenantID = ""
	if _, err := PrepareBlocks([]*flowworker.EnrichedBatch{testEnrichedBatch(10, testEnrichedRecord(1, 1, 0)), second}, BatchLimits{}); !errors.Is(err, ErrInvalidBatchGroup) {
		t.Fatalf("empty tenant error=%v", err)
	}
}

func TestPrepareBlocksRejectsPacketReceiptOverflow(t *testing.T) {
	first := testEnrichedBatch(10, testEnrichedRecord(1, 0, 0))
	second := testEnrichedBatch(11, testEnrichedRecord(2, 0, 0))
	first.Records[0].RawPackets = math.MaxUint64
	second.Records[0].RawPackets = 1
	if _, err := PrepareBlocks([]*flowworker.EnrichedBatch{first, second}, BatchLimits{}); !errors.Is(err, ErrInvalidBatchGroup) {
		t.Fatalf("raw packet overflow error=%v", err)
	}

	first = testEnrichedBatch(10, testEnrichedRecord(1, 0, 0))
	second = testEnrichedBatch(11, testEnrichedRecord(2, 0, 0))
	first.Records[0].EstimatedPackets = math.MaxUint64
	second.Records[0].EstimatedPackets = 1
	if _, err := PrepareBlocks([]*flowworker.EnrichedBatch{first, second}, BatchLimits{}); !errors.Is(err, ErrInvalidBatchGroup) {
		t.Fatalf("estimated packet overflow error=%v", err)
	}
}

func testEnrichedBatch(offset int64, records ...flowworker.EnrichedRecord) *flowworker.EnrichedBatch {
	return &flowworker.EnrichedBatch{
		SchemaVersion: flowworker.EnrichedBatchSchemaVersion, MessageDisposition: flowworker.MessageDispositionPersisted,
		SourceStreamID: "cluster-a:raw-v1:incarnation-1", KafkaTopic: "watchdog.flow.raw-v1", KafkaPartition: 3, KafkaOffset: offset,
		TenantID: "tenant-a", CollectorID: "collector-a", ExporterID: "exporter-a",
		ReceivedAt: time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC), SourceIP: netip.MustParseAddr("192.0.2.1"),
		Records: records,
	}
}

func testEnrichedRecord(index byte, rawBytes, estimatedBytes uint64) flowworker.EnrichedRecord {
	return flowworker.EnrichedRecord{
		RecordIndex: uint32(index), EventTime: time.Date(2026, 9, 5, 1, 2, 0, 0, time.UTC),
		RawBytes: rawBytes, RawPackets: 1, EstimatedValid: true, EstimatedBytes: estimatedBytes, EstimatedPackets: 10,
		TargetID: "target-a", SourceIP: netip.MustParseAddr("10.0.0.1"), DestinationIP: netip.MustParseAddr("203.0.113.1"),
		Dimensions: flowdimension.ClassifiedEndpoints{
			SnapshotID: "snapshot-a", Business: "business-a", Direction: flowdimension.DirectionOut,
			Local:  flowdimension.EndpointDimension{IP: netip.MustParseAddr("10.0.0.1"), PrefixID: "local-a"},
			Remote: flowdimension.EndpointDimension{IP: netip.MustParseAddr("203.0.113.1"), PrefixID: "remote-a"},
		},
		RemoteGeo: flowdimension.GeoInfo{
			Country: "CN", AdminCode: "330100", City: "Hangzhou", Version: "geo-a", Source: flowdimension.GeoSchemaV2,
			ContinentID: "Asia", RegionID: "EastAsia", CountryID: "CN", ProvinceID: "330000", CityID: "330100",
		},
		RemoteASNSource:         flowworker.ASNSourceGeoV2,
		Category:                flowdimension.CategoryOnNetLocalCity,
		SupplierRemoteASNSource: flowworker.ASNSourceUnknown,
		SupplierCategory:        flowdimension.CategoryUnknown,
		Disposition:             flowdimension.DispositionCount,
		ClassificationVersion:   1,
	}
}

func TestPrepareBlocksCapsPartitionDays(t *testing.T) {
	// Four records on four distinct UTC days (a replay/backfill shape); with a
	// 2-day cap they must split into two blocks so no insert exceeds
	// max_partitions_per_insert_block.
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	batches := make([]*flowworker.EnrichedBatch, 0, 4)
	for i := 0; i < 4; i++ {
		record := testEnrichedRecord(byte(i+1), 100, 1000)
		record.EventTime = base.AddDate(0, 0, i)
		batches = append(batches, testEnrichedBatch(int64(10+i), record))
	}
	blocks, err := PrepareBlocks(batches, BatchLimits{MaxRows: 100, MaxApproxBytes: 1 << 20, MaxPartitionDays: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 {
		t.Fatalf("blocks=%d, want 2 (4 distinct days capped at 2/block)", len(blocks))
	}
	for _, block := range blocks {
		days := map[int32]struct{}{}
		for _, ref := range block.Records {
			days[int32(ref.Record.EventTime.UTC().Unix()/86400)] = struct{}{}
		}
		if len(days) > 2 {
			t.Fatalf("block touches %d partition days, want <= 2", len(days))
		}
	}
}

func TestPrepareBlocksCapsTenantDayPartitions(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	batches := make([]*flowworker.EnrichedBatch, 0, 4)
	for i := 0; i < 4; i++ {
		record := testEnrichedRecord(byte(i+1), 100, 1000)
		record.EventTime = base
		batch := testEnrichedBatch(int64(10+i), record)
		batch.TenantID = []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"}[i]
		batches = append(batches, batch)
	}
	blocks, err := PrepareBlocks(batches, BatchLimits{MaxRows: 100, MaxApproxBytes: 1 << 20, MaxPartitionDays: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 {
		t.Fatalf("blocks=%d, want 2 for four tenant-day partitions capped at two", len(blocks))
	}
	for _, block := range blocks {
		partitions := map[string]struct{}{}
		for _, ref := range block.Records {
			partitions[ref.Batch.TenantID+"/"+ref.Record.EventTime.UTC().Format("20060102")] = struct{}{}
		}
		if len(partitions) > 2 {
			t.Fatalf("block touches %d tenant-day partitions, want <= 2", len(partitions))
		}
	}
}
