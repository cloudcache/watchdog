// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

type fakeHotRollupRunner struct {
	markers []flowch.RollupMarker
	// resolutionMarkers, when it has an entry, overrides markers for that
	// resolution so a test can model the 1m source and 5m target separately.
	resolutionMarkers map[flowch.RollupResolution][]flowch.RollupMarker
	needsRepair       map[int64]bool
	repairChecks      int
	requests          []flowch.RollupRequest
	sourceCovered     bool
	rawRecords        bool
}

func (runner *fakeHotRollupRunner) GenerationMarkers(_ context.Context, resolution flowch.RollupResolution, _, _ time.Time) ([]flowch.RollupMarker, error) {
	if markers, ok := runner.resolutionMarkers[resolution]; ok {
		return append([]flowch.RollupMarker(nil), markers...), nil
	}
	return append([]flowch.RollupMarker(nil), runner.markers...), nil
}

// hourMarkers returns one marker per step-aligned bucket of the UTC hour.
func hourMarkers(hour time.Time, step time.Duration, generation uint64, generatedAt time.Time) []flowch.RollupMarker {
	markers := make([]flowch.RollupMarker, 0, int(time.Hour/step))
	for bucket := hour; bucket.Before(hour.Add(time.Hour)); bucket = bucket.Add(step) {
		markers = append(markers, flowch.RollupMarker{Bucket: bucket, Generation: generation, GeneratedAt: generatedAt})
	}
	return markers
}

func (runner *fakeHotRollupRunner) CoveredThroughAtLeast(_ context.Context, resolution flowch.RollupResolution, from, to time.Time, _ uint64) (time.Time, error) {
	if resolution == flowch.RollupOneMinute && runner.sourceCovered {
		return to, nil
	}
	return from, nil
}

func (runner *fakeHotRollupRunner) RawBucketHasRecords(context.Context, flowch.RollupResolution, time.Time) (bool, error) {
	return runner.rawRecords, nil
}

func (runner *fakeHotRollupRunner) BucketNeedsRepair(_ context.Context, _ flowch.RollupResolution, bucket time.Time) (bool, error) {
	runner.repairChecks++
	return runner.needsRepair[bucket.UTC().Unix()], nil
}

func (runner *fakeHotRollupRunner) Run(_ context.Context, request flowch.RollupRequest) error {
	runner.requests = append(runner.requests, request)
	return nil
}

func TestFlowHotRollupSchedulerFillsOldestCoverageGapsBeforeRepair(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 17, 0, 0, time.UTC)
	through := now.Add(-5 * time.Minute).Truncate(time.Hour)
	from := through.Add(-4 * time.Hour)
	runner := &fakeHotRollupRunner{markers: []flowch.RollupMarker{
		{Bucket: from, Generation: 10, GeneratedAt: now.Add(-time.Hour)},
		{Bucket: from.Add(2 * time.Hour), Generation: 11, GeneratedAt: now.Add(-time.Hour)},
		{Bucket: from.Add(3 * time.Hour), Generation: lifecycleGenerationFloor + 1, GeneratedAt: now.Add(-time.Hour)},
	}}
	scheduler := &flowHotRollupScheduler{runner: runner, config: FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		MinuteLookback: time.Minute, HourLookback: 4 * time.Hour,
		MinuteLateArrivalWindow: 0, HourLateArrivalWindow: 2 * time.Hour,
		RepairInterval: 30 * time.Minute, MaxMinuteBucketsPerRun: 1, MaxHourBucketsPerRun: 1,
		MaxThreads: 4, Priority: 10, MaxMemoryBytes: 6 << 30,
	}}
	count, err := scheduler.ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 || len(runner.requests) != 2 || runner.requests[0].Resolution != flowch.RollupOneMinute ||
		runner.requests[1].Resolution != flowch.RollupOneHour || !runner.requests[1].Bucket.Equal(from.Add(time.Hour)) {
		t.Fatalf("count=%d requests=%+v", count, runner.requests)
	}
	if runner.requests[1].Generation <= 11 || runner.requests[1].Generation >= lifecycleGenerationFloor {
		t.Fatalf("hot generation=%d", runner.requests[1].Generation)
	}
	if runner.requests[1].MaxThreads != 4 || runner.requests[1].Priority != 10 || runner.requests[1].MaxMemoryBytes != 6<<30 {
		t.Fatalf("hot rollup resource guards were not forwarded: %+v", runner.requests[1])
	}
}

func TestFlowHotRollupSchedulerDerivesHourFromCompleteMinuteTier(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 17, 0, 0, time.UTC)
	through := now.Add(-5 * time.Minute).Truncate(time.Hour)
	runner := &fakeHotRollupRunner{sourceCovered: true}
	scheduler := &flowHotRollupScheduler{runner: runner, config: FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		HourLookback: time.Hour, HourLateArrivalWindow: 0, RepairInterval: 30 * time.Minute,
		MaxHourBucketsPerRun: 1, MinimumGeneration: 11,
	}}
	count, err := scheduler.scanResolution(context.Background(), now, flowch.RollupOneHour, time.Hour, time.Hour, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(runner.requests) != 1 || !runner.requests[0].Bucket.Equal(through.Add(-time.Hour)) ||
		runner.requests[0].SourceResolution != flowch.RollupOneMinute {
		t.Fatalf("hour was not derived from minute tier: count=%d requests=%+v", count, runner.requests)
	}
}

func TestFlowHotRollupSchedulerNeverFallsBackToRawHourExpansion(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 17, 0, 0, time.UTC)
	runner := &fakeHotRollupRunner{rawRecords: true}
	scheduler := &flowHotRollupScheduler{runner: runner, config: FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		HourLookback: 2 * time.Hour, HourLateArrivalWindow: 0, RepairInterval: 30 * time.Minute,
		MaxHourBucketsPerRun: 1, MinimumGeneration: 11,
	}}
	count, err := scheduler.scanResolution(context.Background(), now, flowch.RollupOneHour, time.Hour, 2*time.Hour, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 || len(runner.requests) != 0 {
		t.Fatalf("incomplete minute source fell back to raw: count=%d requests=%+v", count, runner.requests)
	}
}

func TestFlowHotRollupSchedulerDoesNotChargeEmptyMarkersToHeavyBudget(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 17, 0, 0, time.UTC)
	runner := &fakeHotRollupRunner{}
	scheduler := &flowHotRollupScheduler{runner: runner, config: FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		HourLookback: 3 * time.Hour, HourLateArrivalWindow: 0, RepairInterval: 30 * time.Minute,
		MaxHourBucketsPerRun: 1, MinimumGeneration: 11,
	}}
	count, err := scheduler.scanResolution(context.Background(), now, flowch.RollupOneHour, time.Hour, 3*time.Hour, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 || len(runner.requests) != 3 {
		t.Fatalf("empty marker count=%d requests=%+v", count, runner.requests)
	}
	for _, request := range runner.requests {
		if !request.MarkerOnly {
			t.Fatalf("empty bucket used heavy rollup: %+v", request)
		}
	}
}

func TestFlowHotRollupSchedulerBatchesOnlyFullySealedRawHour(t *testing.T) {
	now := time.Date(2026, 9, 22, 13, 7, 0, 0, time.UTC)
	runner := &fakeHotRollupRunner{}
	scheduler := &flowHotRollupScheduler{runner: runner, config: FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		MinuteLookback: 5 * time.Minute, MinuteLateArrivalWindow: 0, RepairInterval: 30 * time.Minute,
		MaxMinuteBucketsPerRun: 5,
	}}
	count, err := scheduler.scanResolution(context.Background(), now, flowch.RollupOneMinute, time.Minute, 5*time.Minute, 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 || len(runner.requests) != 1 {
		t.Fatalf("minute batch count=%d requests=%+v", count, runner.requests)
	}
	if got := runner.requests[0]; !got.Bucket.Equal(time.Date(2026, 9, 22, 12, 55, 0, 0, time.UTC)) ||
		!got.BucketEnd.Equal(time.Date(2026, 9, 22, 13, 0, 0, 0, time.UTC)) {
		t.Fatalf("sealed minute batch=%+v", got)
	}
}

func TestNextHotRollupGenerationNeverOverlapsLifecycleNamespace(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	generation, err := nextHotRollupGeneration(now, uint64(now.Unix()))
	if err != nil || generation != uint64(now.Unix())+1 {
		t.Fatalf("generation=%d err=%v", generation, err)
	}
	if _, err := nextHotRollupGeneration(now, lifecycleGenerationFloor); err == nil {
		t.Fatal("lifecycle generation was accepted as a hot-cache predecessor")
	}
}

func TestFlowHotRollupSchedulerDoesNotBlindlyRewriteStableBuckets(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 17, 0, 0, time.UTC)
	through := now.Add(-5 * time.Minute).Truncate(time.Hour)
	bucket := through.Add(-time.Hour)
	runner := &fakeHotRollupRunner{markers: []flowch.RollupMarker{{
		Bucket: bucket, Generation: 10, GeneratedAt: now.Add(-time.Hour),
	}}}
	scheduler := &flowHotRollupScheduler{runner: runner, config: FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		MinuteLookback: time.Minute, HourLookback: time.Hour,
		MinuteLateArrivalWindow: 0, HourLateArrivalWindow: time.Hour,
		RepairInterval: 30 * time.Minute, MaxMinuteBucketsPerRun: 1, MaxHourBucketsPerRun: 1,
	}}
	count, err := scheduler.ScanOnce(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(runner.requests) != 1 || runner.requests[0].Resolution != flowch.RollupOneMinute {
		t.Fatalf("stable hourly bucket was rewritten: count=%d requests=%+v", count, runner.requests)
	}
	if runner.repairChecks != 1 {
		t.Fatalf("repair checks=%d, want 1", runner.repairChecks)
	}
	if _, err := scheduler.ScanOnce(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if runner.repairChecks != 1 {
		t.Fatalf("stable bucket was rechecked before repair interval: checks=%d", runner.repairChecks)
	}
}

func TestFlowHotRollupSchedulerRegeneratesPreReleaseShapeWithoutCoverageGap(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 17, 0, 0, time.UTC)
	through := now.Add(-5 * time.Minute).Truncate(time.Hour)
	bucket := through.Add(-time.Hour)
	runner := &fakeHotRollupRunner{markers: []flowch.RollupMarker{{
		Bucket: bucket, Generation: 10, GeneratedAt: now.Add(-time.Hour),
	}}}
	scheduler := &flowHotRollupScheduler{runner: runner, config: FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		HourLookback: time.Hour, HourLateArrivalWindow: 0, RepairInterval: 30 * time.Minute,
		MaxHourBucketsPerRun: 1, MinimumGeneration: 11,
	}}
	count, err := scheduler.scanResolution(context.Background(), now, flowch.RollupOneHour, time.Hour, time.Hour, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || len(runner.requests) != 1 || !runner.requests[0].Bucket.Equal(bucket) || runner.repairChecks != 0 {
		t.Fatalf("shape regeneration did not replace the covered bucket atomically: count=%d requests=%+v checks=%d", count, runner.requests, runner.repairChecks)
	}
}

func TestFlowHotRollupSchedulerFillsFiveMinuteFromCompleteMinute(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 17, 0, 0, time.UTC)
	hour := time.Date(2026, 9, 22, 11, 0, 0, 0, time.UTC)
	runner := &fakeHotRollupRunner{resolutionMarkers: map[flowch.RollupResolution][]flowch.RollupMarker{
		flowch.RollupOneMinute:  hourMarkers(hour, time.Minute, 10, now.Add(-10*time.Minute)),
		flowch.RollupFiveMinute: nil,
	}}
	scheduler := &flowHotRollupScheduler{runner: runner, config: FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		MinuteLookback: time.Minute, HourLookback: 4 * time.Hour,
		MinuteLateArrivalWindow: 0, HourLateArrivalWindow: 2 * time.Hour,
		RepairInterval: 30 * time.Minute, MaxMinuteBucketsPerRun: 1, MaxHourBucketsPerRun: 1,
		MaxThreads: 4, Priority: 10, MaxMemoryBytes: 6 << 30,
	}}
	completed, err := scheduler.scanFiveMinute(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if completed != 1 || len(runner.requests) != 1 {
		t.Fatalf("expected one 5m run, got completed=%d requests=%d", completed, len(runner.requests))
	}
	req := runner.requests[0]
	if req.Resolution != flowch.RollupFiveMinute || req.SourceResolution != flowch.RollupOneMinute {
		t.Fatalf("wrong 5m resolution/source: %+v", req)
	}
	if !req.Bucket.Equal(hour) || req.BucketEnd.Sub(req.Bucket) != time.Hour {
		t.Fatalf("5m batch should span the covered hour: %+v", req)
	}
}

func TestFlowHotRollupSchedulerSkipsFiveMinuteWhenMinuteIncomplete(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 17, 0, 0, time.UTC)
	hour := time.Date(2026, 9, 22, 11, 0, 0, 0, time.UTC)
	runner := &fakeHotRollupRunner{resolutionMarkers: map[flowch.RollupResolution][]flowch.RollupMarker{
		// 59 of 60 minutes: the hour is not complete, so no 5m derivation.
		flowch.RollupOneMinute:  hourMarkers(hour, time.Minute, 10, now.Add(-10*time.Minute))[1:],
		flowch.RollupFiveMinute: nil,
	}}
	scheduler := &flowHotRollupScheduler{runner: runner, config: FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		MinuteLookback: time.Minute, HourLookback: 4 * time.Hour,
		RepairInterval: 30 * time.Minute, MaxMinuteBucketsPerRun: 1, MaxHourBucketsPerRun: 1,
	}}
	completed, err := scheduler.scanFiveMinute(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if completed != 0 || len(runner.requests) != 0 {
		t.Fatalf("expected no 5m runs when 1m incomplete, got completed=%d requests=%d", completed, len(runner.requests))
	}
}

func TestFlowHotRollupSchedulerRederivesFiveMinuteAfterMinuteRepair(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 17, 0, 0, time.UTC)
	hour := time.Date(2026, 9, 22, 11, 0, 0, 0, time.UTC)
	derivedAt := now.Add(-20 * time.Minute)
	config := FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		MinuteLookback: time.Hour, HourLookback: 4 * time.Hour,
		RepairInterval: 30 * time.Minute, MaxMinuteBucketsPerRun: 60, MaxHourBucketsPerRun: 4,
	}
	fiveMinute := hourMarkers(hour, 5*time.Minute, 500, derivedAt)

	// Stable: every 1m generation predates the 5m derivation, so the hour is not
	// rewritten on every scan.
	stable := &fakeHotRollupRunner{resolutionMarkers: map[flowch.RollupResolution][]flowch.RollupMarker{
		flowch.RollupOneMinute:  hourMarkers(hour, time.Minute, 10, derivedAt),
		flowch.RollupFiveMinute: fiveMinute,
	}}
	scheduler := &flowHotRollupScheduler{runner: stable, config: config}
	if completed, err := scheduler.scanFiveMinute(context.Background(), now); err != nil || completed != 0 {
		t.Fatalf("stable 5m hour was rewritten: completed=%d err=%v requests=%+v", completed, err, stable.requests)
	}

	// One minute repaired after the 5m derivation (late-arriving records): the
	// hour is derived again with a generation that supersedes the stale one.
	minute := hourMarkers(hour, time.Minute, 10, derivedAt)
	minute[37] = flowch.RollupMarker{Bucket: minute[37].Bucket, Generation: 600, GeneratedAt: derivedAt.Add(10 * time.Minute)}
	repaired := &fakeHotRollupRunner{resolutionMarkers: map[flowch.RollupResolution][]flowch.RollupMarker{
		flowch.RollupOneMinute:  minute,
		flowch.RollupFiveMinute: fiveMinute,
	}}
	scheduler = &flowHotRollupScheduler{runner: repaired, config: config}
	completed, err := scheduler.scanFiveMinute(context.Background(), now)
	if err != nil || completed != 1 || len(repaired.requests) != 1 {
		t.Fatalf("repaired 1m did not re-derive 5m: completed=%d err=%v requests=%+v", completed, err, repaired.requests)
	}
	if got := repaired.requests[0]; !got.Bucket.Equal(hour) || got.Resolution != flowch.RollupFiveMinute || got.Generation <= 500 {
		t.Fatalf("5m re-derivation=%+v", got)
	}
}

func TestFlowHotRollupSchedulerFiveMinuteScanStopsAtMinuteRetention(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 17, 0, 0, time.UTC)
	// A complete 1m hour older than the 1m TTL cannot really exist; the scan must
	// not even reach it, however long hour_lookback is.
	expired := now.Add(-5*time.Minute).Truncate(time.Hour).Add(-flowMinuteTierRetention - time.Hour)
	runner := &fakeHotRollupRunner{resolutionMarkers: map[flowch.RollupResolution][]flowch.RollupMarker{
		flowch.RollupOneMinute:  hourMarkers(expired, time.Minute, 10, now.Add(-50*time.Hour)),
		flowch.RollupFiveMinute: nil,
	}}
	scheduler := &flowHotRollupScheduler{runner: runner, config: FlowHotRollupConfig{
		Enabled: true, ScanInterval: time.Minute, SealDelay: 5 * time.Minute,
		MinuteLookback: time.Hour, HourLookback: 72 * time.Hour,
		RepairInterval: 30 * time.Minute, MaxMinuteBucketsPerRun: 60, MaxHourBucketsPerRun: 4,
	}}
	completed, err := scheduler.scanFiveMinute(context.Background(), now)
	if err != nil || completed != 0 || len(runner.requests) != 0 {
		t.Fatalf("5m scan reached past 1m retention: completed=%d err=%v requests=%+v", completed, err, runner.requests)
	}
}
