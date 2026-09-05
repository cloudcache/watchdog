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
	if first[0].FirstOffset != 10 || first[0].LastOffset != 11 || first[0].SourceBatchCount != 2 || first[0].RawBytes != 300 || first[0].EstimatedBytes != 3_000 {
		t.Fatalf("unexpected first receipt: %+v", first[0])
	}
	if first[0].ID == ([32]byte{}) || first[0].Checksum == ([32]byte{}) || first[0].ID == first[1].ID {
		t.Fatalf("invalid block identity: %x %x", first[0].ID, first[1].ID)
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
}

func testEnrichedBatch(offset int64, records ...flowworker.EnrichedRecord) *flowworker.EnrichedBatch {
	return &flowworker.EnrichedBatch{
		SchemaVersion: flowworker.EnrichedBatchSchemaVersion,
		KafkaTopic:    "watchdog.flow.raw-v1", KafkaPartition: 3, KafkaOffset: offset,
		TenantID: "tenant-a", CollectorID: "collector-a", ExporterID: "exporter-a",
		ReceivedAt: time.Date(2026, 9, 5, 1, 2, 3, 0, time.UTC), SourceIP: netip.MustParseAddr("192.0.2.1"),
		Records: records,
	}
}

func testEnrichedRecord(index byte, rawBytes, estimatedBytes uint64) flowworker.EnrichedRecord {
	var id [32]byte
	id[31] = index
	return flowworker.EnrichedRecord{
		SourceRecordID: id, RecordIndex: uint32(index), EventTime: time.Date(2026, 9, 5, 1, 2, 0, 0, time.UTC),
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
		RemoteASNSource:       flowworker.ASNSourceGeoV2,
		Category:              flowdimension.CategoryOnNetLocalCity,
		Disposition:           flowdimension.DispositionCount,
		ClassificationVersion: 1,
	}
}
