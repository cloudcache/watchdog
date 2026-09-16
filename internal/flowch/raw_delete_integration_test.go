// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

// TestRealClickHouseRawDayDeletion proves that the destructive executor drops
// exactly one UTC raw partition, includes disposition=drop rows in its physical
// pre/post proof, preserves the hourly archive, and converges on replay with a
// stable ClickHouse query ID.
func TestRealClickHouseRawDayDeletion(t *testing.T) {
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_raw_delete")
	day := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	counted := integrationRecord(1, day.Add(10*time.Minute), "geo-city-a", 100)
	dropped := integrationRecord(2, day.Add(20*time.Minute), "geo-city-b", 900)
	dropped.Disposition = flowdimension.DispositionDrop
	otherDay := integrationRecord(3, day.Add(24*time.Hour+10*time.Minute), "geo-city-c", 50)
	insertIntegrationBatch(t, ctx, native, integrationBatch(10, day.Add(time.Hour), counted, dropped))
	insertIntegrationBatch(t, ctx, native, integrationBatch(11, day.Add(25*time.Hour), otherDay))

	runner, err := NewRollupRunner(native)
	if err != nil {
		t.Fatal(err)
	}
	for hour := 0; hour < 24; hour++ {
		bucket := day.Add(time.Duration(hour) * time.Hour)
		if err := runner.Run(ctx, RollupRequest{
			Resolution: RollupOneHour, Bucket: bucket, Generation: 1, GeneratedAt: day.Add(48 * time.Hour),
		}); err != nil {
			t.Fatalf("roll up hour %d: %v", hour, err)
		}
	}
	physical, err := runner.RawDayPhysicalRecords(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	raw, archive, err := runner.DayStorageCounters(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	want := StorageCounters{RecordCount: 1, RawBytes: 100, RawPackets: 1, EstimatedBytes: 1000, EstimatedPackets: 10, EstimatedValidRecords: 1}
	if physical != 2 || raw != want || archive != want {
		t.Fatalf("pre-delete physical=%d raw=%+v archive=%+v", physical, raw, archive)
	}

	const queryID = "flow-raw-delete-real-it"
	if err := runner.DropRawDay(ctx, day, queryID); err != nil {
		t.Fatal(err)
	}
	if err := runner.DropRawDay(ctx, day, queryID); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	physical, err = runner.RawDayPhysicalRecords(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	raw, archive, err = runner.DayStorageCounters(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	otherPhysical, err := runner.RawDayPhysicalRecords(ctx, day.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if physical != 0 || raw != (StorageCounters{}) || archive != want || otherPhysical != 1 {
		t.Fatalf("post-delete physical=%d raw=%+v archive=%+v other-day=%d", physical, raw, archive, otherPhysical)
	}
}
