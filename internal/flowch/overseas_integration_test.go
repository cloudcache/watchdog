// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

func TestRealClickHouseOverseasKPIAndRepair(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_overseas")
	bucket := time.Date(2026, 9, 5, 14, 10, 0, 0, time.UTC)

	records := []flowworker.EnrichedRecord{
		integrationOverseasRecord(1, bucket.Add(10*time.Second), flowdimension.DirectionOut,
			netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("198.51.100.1"), "JP", "EastAsia", 100),
		integrationOverseasRecord(2, bucket.Add(20*time.Second), flowdimension.DirectionIn,
			netip.MustParseAddr("198.51.100.2"), netip.MustParseAddr("10.0.0.2"), "US", "NorthAmerica", 80),
		integrationOverseasRecord(3, bucket.Add(30*time.Second), flowdimension.DirectionOut,
			netip.MustParseAddr("2001:db8:1::1"), netip.MustParseAddr("2001:db8:2::1"), "DE", "Europe", 50),
		integrationOverseasRecord(4, bucket.Add(40*time.Second), flowdimension.DirectionOut,
			netip.Addr{}, netip.Addr{}, "", "", 20),
	}
	insertIntegrationBatch(t, ctx, native, integrationBatch(50, bucket.Add(2*time.Minute), records...))

	rollup, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	request := RollupRequest{
		TenantID: "flow-it-tenant", Resolution: RollupOneMinute, Bucket: bucket,
		Generation: 1, GeneratedAt: bucket.Add(3 * time.Minute),
	}
	if err := rollup.Run(ctx, request); err != nil {
		t.Fatal(err)
	}

	runner, err := flowquery.NewOverseasRunner(native.executor)
	if err != nil {
		t.Fatal(err)
	}
	before := runIntegrationOverseas(t, ctx, runner, bucket, flowquery.OverseasGeoCountry)
	assertOverseasKPI(t, before, 250, 4, 170, 80, 180, 50, 20)
	assertOverseasGeo(t, before, "JP", 100, 130, 20)

	late := integrationOverseasRecord(5, bucket.Add(50*time.Second), flowdimension.DirectionOut,
		netip.MustParseAddr("2001:db8:1::5"), netip.MustParseAddr("2001:db8:2::5"), "JP", "EastAsia", 200)
	insertIntegrationBatch(t, ctx, native, integrationBatch(51, bucket.Add(4*time.Minute), late))
	request.Generation = 2
	request.GeneratedAt = bucket.Add(5 * time.Minute)
	if err := rollup.Run(ctx, request); err != nil {
		t.Fatal(err)
	}

	after := runIntegrationOverseas(t, ctx, runner, bucket, flowquery.OverseasGeoCountry)
	assertOverseasKPI(t, after, 450, 5, 370, 80, 180, 250, 20)
	assertOverseasGeo(t, after, "JP", 300, 130, 20)

	region := runIntegrationOverseas(t, ctx, runner, bucket, flowquery.OverseasGeoRegion)
	assertOverseasGeo(t, region, "EastAsia", 300, 130, 20)
}

func integrationOverseasRecord(index byte, eventTime time.Time, direction flowdimension.BusinessDirection, source, destination netip.Addr, country, region string, rawBytes uint64) flowworker.EnrichedRecord {
	record := testEnrichedRecord(index, rawBytes, rawBytes*10)
	record.EventTime = eventTime
	record.SourceIP = source
	record.DestinationIP = destination
	record.Dimensions.SnapshotID = "snapshot-overseas"
	record.Dimensions.Version = 1
	record.Dimensions.Direction = direction
	record.Dimensions.Business = "customer-a"
	if direction == flowdimension.DirectionIn {
		record.Dimensions.Local.IP = destination
		record.Dimensions.Remote.IP = source
	} else {
		record.Dimensions.Local.IP = source
		record.Dimensions.Remote.IP = destination
	}
	record.RemoteGeo = flowdimension.GeoInfo{
		Country: country, CountryID: country, RegionID: region,
		Version: "geo-overseas", Source: flowdimension.GeoSchemaV2,
	}
	record.Category = flowdimension.CategoryOverseas
	record.ClassificationVersion = 1
	return record
}

func runIntegrationOverseas(t *testing.T, ctx context.Context, runner *flowquery.OverseasRunner, bucket time.Time, level flowquery.OverseasGeoLevel) flowquery.OverseasResult {
	t.Helper()
	compiled, err := flowquery.CompileOverseas(flowquery.Scope{TenantID: "flow-it-tenant"}, flowquery.OverseasRequest{
		From: bucket, To: bucket.Add(time.Minute), Bucket: flowquery.BucketOneMinute,
		Metric: flowquery.MetricRawBytes, GeoLevel: level, View: flowquery.ViewCustomer,
		TopN: 1, IncludeOther: true,
	}, bucket.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(ctx, compiled)
	if err != nil {
		t.Fatal(err)
	}
	if !result.RollupCompleteness.Complete || result.RollupCompleteness.CoveredBuckets != 1 ||
		result.MixedVersions || result.VersionCount != 1 {
		t.Fatalf("overseas result metadata=%+v", result)
	}
	return result
}

func assertOverseasKPI(t *testing.T, result flowquery.OverseasResult, total, records, outbound, inbound, ipv4, ipv6, unknown uint64) {
	t.Helper()
	all := findOverseasPoint(t, result, flowquery.OverseasRowKPI, flowquery.OverseasScopeOverseas, flowquery.OverseasDirectionCombined, flowquery.OverseasIPFamilyAll, "", false)
	if all.Value != float64(total) || all.ReceivedRecords != records || all.ObservedRemoteIPs != records || all.ObservedLocalHosts != records {
		t.Fatalf("combined/all KPI=%+v", all)
	}
	for direction, want := range map[flowquery.OverseasDirection]uint64{
		flowquery.OverseasDirectionOut: outbound,
		flowquery.OverseasDirectionIn:  inbound,
	} {
		point := findOverseasPoint(t, result, flowquery.OverseasRowKPI, flowquery.OverseasScopeOverseas, direction, flowquery.OverseasIPFamilyAll, "", false)
		if point.Value != float64(want) || point.ObservedRemoteIPs != point.ReceivedRecords || point.ObservedLocalHosts != point.ReceivedRecords {
			t.Fatalf("%s/all KPI=%+v, want=%d", direction, point, want)
		}
	}
	for family, want := range map[flowquery.OverseasIPFamily]uint64{
		flowquery.OverseasIPFamilyIPv4:    ipv4,
		flowquery.OverseasIPFamilyIPv6:    ipv6,
		flowquery.OverseasIPFamilyUnknown: unknown,
	} {
		point := findOverseasPoint(t, result, flowquery.OverseasRowKPI, flowquery.OverseasScopeOverseas, flowquery.OverseasDirectionCombined, family, "", false)
		if point.Value != float64(want) {
			t.Fatalf("combined/%s KPI=%+v, want=%d", family, point, want)
		}
	}
}

func assertOverseasGeo(t *testing.T, result flowquery.OverseasResult, top string, topValue, otherValue, unknownValue uint64) {
	t.Helper()
	topPoint := findOverseasPoint(t, result, flowquery.OverseasRowGeo, flowquery.OverseasScopeOverseas, flowquery.OverseasDirectionCombined, flowquery.OverseasIPFamilyAll, top, false)
	other := findOverseasPoint(t, result, flowquery.OverseasRowGeo, flowquery.OverseasScopeOverseas, flowquery.OverseasDirectionCombined, flowquery.OverseasIPFamilyAll, "_other", true)
	unknown := findOverseasPoint(t, result, flowquery.OverseasRowGeo, flowquery.OverseasScopeUnknownGeo, flowquery.OverseasDirectionCombined, flowquery.OverseasIPFamilyAll, "_unassigned", false)
	if topPoint.Value != float64(topValue) || other.Value != float64(otherValue) || unknown.Value != float64(unknownValue) {
		t.Fatalf("Geo top=%+v other=%+v unknown=%+v", topPoint, other, unknown)
	}
}

func findOverseasPoint(t *testing.T, result flowquery.OverseasResult, kind flowquery.OverseasRowKind, scope flowquery.OverseasGeoScope, direction flowquery.OverseasDirection, family flowquery.OverseasIPFamily, geoValue string, other bool) flowquery.OverseasPoint {
	t.Helper()
	for _, point := range result.Points {
		if point.Kind == kind && point.GeoScope == scope && point.Direction == direction && point.IPFamily == family && point.GeoValue == geoValue && point.Other == other {
			return point
		}
	}
	t.Fatalf("overseas point missing: kind=%s scope=%s direction=%s family=%s geo=%s other=%t points=%+v", kind, scope, direction, family, geoValue, other, result.Points)
	return flowquery.OverseasPoint{}
}
