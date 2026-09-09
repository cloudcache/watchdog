// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

func TestRealClickHouseGeoHierarchyCountsEachFactOncePerLevel(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_geo_hierarchy")
	bucket := time.Date(2026, 9, 5, 17, 0, 0, 0, time.UTC)
	records := []flowworker.EnrichedRecord{
		integrationGeoRecord(1, bucket.Add(10*time.Second), "CN", "Asia", "EastAsia", "CN", "330000", "330100", 100),
		integrationGeoRecord(2, bucket.Add(20*time.Second), "CN", "Asia", "EastAsia", "CN", "330000", "330200", 200),
		integrationGeoRecord(3, bucket.Add(30*time.Second), "JP", "Asia", "EastAsia", "JP", "JP-13", "JP-13101", 300),
	}
	insertIntegrationBatch(t, ctx, native, integrationBatch(70, bucket.Add(2*time.Minute), records...))

	rollup, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	if err := rollup.Run(ctx, RollupRequest{
		Resolution: RollupOneMinute, Bucket: bucket,
		Generation: 1, GeneratedAt: bucket.Add(3 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	runner, err := flowquery.NewRunner(native.executor)
	if err != nil {
		t.Fatal(err)
	}
	levels := []struct {
		dimension flowquery.Dimension
		want      map[string]aggregateWant
	}{
		{flowquery.DimensionGeoContinent, map[string]aggregateWant{"Asia": {value: 600, records: 3}}},
		{flowquery.DimensionGeoRegion, map[string]aggregateWant{"EastAsia": {value: 600, records: 3}}},
		{flowquery.DimensionGeoCountry, map[string]aggregateWant{
			"CN": {value: 300, records: 2}, "JP": {value: 300, records: 1},
		}},
		{flowquery.DimensionGeoProvince, map[string]aggregateWant{
			"330000": {value: 300, records: 2}, "JP-13": {value: 300, records: 1},
		}},
		{flowquery.DimensionGeoCity, map[string]aggregateWant{
			"330100": {value: 100, records: 1}, "330200": {value: 200, records: 1}, "JP-13101": {value: 300, records: 1},
		}},
	}
	for _, level := range levels {
		result := runIntegrationAggregate(t, ctx, runner, bucket, bucket.Add(time.Minute), flowquery.BucketOneMinute, level.dimension, 10, false)
		assertAggregatePoints(t, result, level.want)
		assertGeoLevelConservation(t, result, 600, 3)
	}
}

func integrationGeoRecord(index byte, eventTime time.Time, country, continentID, regionID, countryID, provinceID, cityID string, rawBytes uint64) flowworker.EnrichedRecord {
	record := integrationRecord(index, eventTime, cityID, rawBytes)
	record.RemoteGeo.Country = country
	record.RemoteGeo.ContinentID = continentID
	record.RemoteGeo.RegionID = regionID
	record.RemoteGeo.CountryID = countryID
	record.RemoteGeo.ProvinceID = provinceID
	record.RemoteGeo.CityID = cityID
	record.RemoteGeo.Version = "geo-hierarchy-v1"
	record.Dimensions.SnapshotID = "snapshot-geo-hierarchy"
	record.Dimensions.Version = 1
	return record
}

func assertGeoLevelConservation(t *testing.T, result flowquery.Result, wantBytes float64, wantRecords uint64) {
	t.Helper()
	var bytes float64
	var records uint64
	seen := make(map[string]struct{}, len(result.Points))
	for _, point := range result.Points {
		if _, exists := seen[point.DimensionValue]; exists {
			t.Fatalf("duplicate Geo value %q in one level: %+v", point.DimensionValue, result.Points)
		}
		seen[point.DimensionValue] = struct{}{}
		bytes += point.Value
		records += point.ReceivedRecords
	}
	if bytes != wantBytes || records != wantRecords {
		t.Fatalf("Geo level is not conservative: bytes=%v records=%d, want bytes=%v records=%d", bytes, records, wantBytes, wantRecords)
	}
}
