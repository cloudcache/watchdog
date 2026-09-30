// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

const lifecycleGenerationFloor = flowch.LifecycleGenerationFloor

type flowHotRollupRunner interface {
	GenerationMarkers(context.Context, flowch.RollupResolution, time.Time, time.Time) ([]flowch.RollupMarker, error)
	CoveredThroughAtLeast(context.Context, flowch.RollupResolution, time.Time, time.Time, uint64) (time.Time, error)
	RawBucketHasRecords(context.Context, flowch.RollupResolution, time.Time) (bool, error)
	RawWatermark(context.Context, time.Time, time.Time) (time.Time, error)
	BucketNeedsRepair(context.Context, flowch.RollupResolution, time.Time) (bool, error)
	Run(context.Context, flowch.RollupRequest) error
}

// flowHotRollupScheduler continuously fills the recent 1h aggregate coverage
// gap left intentionally by the cold lifecycle scheduler. Its generations are
// kept below the lifecycle namespace, so the cache can accelerate reads but can
// never supersede a reconciled archive or authorize raw deletion.
type flowHotRollupScheduler struct {
	config            FlowHotRollupConfig
	runner            flowHotRollupRunner
	logf              func(string, ...any)
	lastRepairChecked map[flowHotRollupBucket]time.Time
}

type flowHotRollupBucket struct {
	resolution flowch.RollupResolution
	bucketUnix int64
}

func (scheduler *flowHotRollupScheduler) Run(ctx context.Context) {
	if scheduler == nil || scheduler.runner == nil || !scheduler.config.Enabled {
		return
	}
	run := func(now time.Time) {
		count, err := scheduler.ScanOnce(ctx, now)
		if scheduler.logf != nil && (err != nil || count > 0) {
			scheduler.logf("Flow hot rollup rebuilt=%d err=%v", count, err)
		}
	}
	run(time.Now())
	// Wait after a completed scan instead of using a wall-clock ticker. A scan
	// can legitimately take longer than ScanInterval while rebuilding a busy
	// hour; a ticker would retain one pending tick and immediately start another
	// expensive scan, defeating the configured I/O cooling interval.
	timer := time.NewTimer(scheduler.config.ScanInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			run(time.Now())
			timer.Reset(scheduler.config.ScanInterval)
		}
	}
}

func (scheduler *flowHotRollupScheduler) ScanOnce(ctx context.Context, now time.Time) (int, error) {
	if scheduler == nil || scheduler.runner == nil || !scheduler.config.Enabled || now.IsZero() {
		return 0, errors.New("Flow hot rollup scheduler is not initialized")
	}
	if err := validateFlowHotRollupConfig(scheduler.config); err != nil {
		return 0, err
	}
	now = now.UTC()
	sealed, err := scheduler.ingestSealedThrough(ctx, now)
	if err != nil || sealed.IsZero() {
		return 0, err
	}
	// Publish sealed minutes first. A completed set of 60 markers lets the hour
	// path merge the compact 1m tier instead of expanding raw EAV records again.
	minute, err := scheduler.scanResolution(ctx, now, sealed, flowch.RollupOneMinute, time.Minute,
		scheduler.config.MinuteLookback, scheduler.config.MinuteLateArrivalWindow, scheduler.config.MaxMinuteBucketsPerRun)
	if err != nil {
		return minute, err
	}
	hourly, err := scheduler.scanResolution(ctx, now, sealed, flowch.RollupOneHour, time.Hour,
		scheduler.config.HourLookback, scheduler.config.HourLateArrivalWindow, scheduler.config.MaxHourBucketsPerRun)
	if err != nil {
		return minute + hourly, err
	}
	// The five-minute query tier is derived from the same complete one-minute
	// coverage the hourly path consumes, so it is filled last and reuses the
	// hour lookback (capped by the 1m TTL) and per-run budget rather than
	// introducing new config.
	fiveMinute, err := scheduler.scanFiveMinute(ctx, now, sealed)
	return minute + hourly + fiveMinute, err
}

// ingestSealedThrough is the instant through which buckets may close: SealDelay
// behind both the wall clock and the newest stored raw record. Sealing by the
// wall clock alone let a stalled Flow worker's hours close empty or partial
// (2026-09-30: 09-29 19:00Z-00:00Z were sealed empty seconds before the worker
// resumed, outside every late-arrival window). The zero time means no raw
// record is stored within the lookback, so nothing may close.
func (scheduler *flowHotRollupScheduler) ingestSealedThrough(ctx context.Context, now time.Time) (time.Time, error) {
	sealed := now.Add(-scheduler.config.SealDelay)
	// The newest record normally lies in the current hour, so the common probe
	// reads at most that hour; only a lagging worker needs the lookback scan.
	current := sealed.Truncate(time.Hour)
	watermark, err := scheduler.runner.RawWatermark(ctx, current, now.Add(scheduler.config.SealDelay))
	if err == nil && watermark.IsZero() {
		lookback := max(scheduler.config.MinuteLookback, scheduler.config.HourLookback)
		watermark, err = scheduler.runner.RawWatermark(ctx, current.Add(-lookback), current)
	}
	if err != nil || watermark.IsZero() {
		return time.Time{}, err
	}
	if ingested := watermark.Add(-scheduler.config.SealDelay); ingested.Before(sealed) {
		sealed = ingested
	}
	return sealed, nil
}

// flowMinuteTierRetention mirrors the flow_aggregate_1m TTL (ClickHouse
// migration 020). The five-minute tier derives only from 1m, so its scan never
// looks further back than 1m can still exist.
const flowMinuteTierRetention = 48 * time.Hour

// scanFiveMinute publishes flow_aggregate_5m for sealed hours whose one-minute
// tier is fully marked. It never scans raw: an hour whose 1m has aged out (older
// than the 1m TTL) is left to the cold lifecycle, not filled here. Each covered
// hour becomes one range INSERT of twelve five-minute buckets plus their
// generation markers, superseding any generation below the readable floor. A
// covered hour is derived again when one of its 1m buckets was regenerated
// after its 5m generation (late-arrival repair or shape regeneration), so 5m
// never keeps counts the 1m tier has since corrected.
func (scheduler *flowHotRollupScheduler) scanFiveMinute(ctx context.Context, now, sealed time.Time) (int, error) {
	sealedThrough := sealed.Truncate(time.Hour)
	from := sealedThrough.Add(-min(scheduler.config.HourLookback, flowMinuteTierRetention)).Truncate(time.Hour)
	if !sealedThrough.After(from) {
		return 0, nil
	}
	targets, err := scheduler.hourlyMarkers(ctx, flowch.RollupFiveMinute, from, sealedThrough)
	if err != nil {
		return 0, err
	}
	sources, err := scheduler.hourlyMarkers(ctx, flowch.RollupOneMinute, from, sealedThrough)
	if err != nil {
		return 0, err
	}
	completed := 0
	for hour := from; hour.Before(sealedThrough) && completed < scheduler.config.MaxHourBucketsPerRun; hour = hour.Add(time.Hour) {
		source, target := sources[hour.Unix()], targets[hour.Unix()]
		if source.readable != 60 {
			continue
		}
		if target.readable == 12 && !source.newest.After(target.oldest) {
			continue
		}
		generation, err := nextHotRollupGeneration(now, target.maxGeneration)
		if err != nil {
			return completed, err
		}
		if err := scheduler.runner.Run(ctx, flowch.RollupRequest{
			Resolution: flowch.RollupFiveMinute, SourceResolution: flowch.RollupOneMinute,
			Bucket: hour, BucketEnd: hour.Add(time.Hour), Generation: generation, GeneratedAt: now,
			MaxThreads: scheduler.config.MaxThreads, Priority: scheduler.config.Priority,
			MaxMemoryBytes: scheduler.config.MaxMemoryBytes,
		}); err != nil {
			return completed, err
		}
		completed++
	}
	return completed, nil
}

// flowHourMarkers summarizes one resolution's latest-generation markers inside
// a UTC hour. readable counts the buckets readers accept (the same rule as
// CoveredThroughAtLeast); oldest/newest span their generated_at.
type flowHourMarkers struct {
	readable       int
	maxGeneration  uint64
	oldest, newest time.Time
}

func (scheduler *flowHotRollupScheduler) hourlyMarkers(ctx context.Context, resolution flowch.RollupResolution, from, to time.Time) (map[int64]flowHourMarkers, error) {
	markers, err := scheduler.runner.GenerationMarkers(ctx, resolution, from, to)
	if err != nil {
		return nil, err
	}
	hours := make(map[int64]flowHourMarkers)
	for _, marker := range markers {
		key := marker.Bucket.UTC().Truncate(time.Hour).Unix()
		hour := hours[key]
		hour.maxGeneration = max(hour.maxGeneration, marker.Generation)
		if marker.Generation != 0 && (marker.Generation >= lifecycleGenerationFloor || marker.Generation >= scheduler.config.MinimumGeneration) {
			hour.readable++
			if hour.oldest.IsZero() || marker.GeneratedAt.Before(hour.oldest) {
				hour.oldest = marker.GeneratedAt
			}
			if marker.GeneratedAt.After(hour.newest) {
				hour.newest = marker.GeneratedAt
			}
		}
		hours[key] = hour
	}
	return hours, nil
}

func (scheduler *flowHotRollupScheduler) scanResolution(ctx context.Context, now, sealed time.Time, resolution flowch.RollupResolution,
	duration, lookback, lateArrivalWindow time.Duration, maxBuckets int) (int, error) {
	through := sealed.Truncate(duration)
	if resolution == flowch.RollupOneMinute {
		// flow_records is ordered by UTC hour, so a one-minute predicate still
		// reads the containing hour. Publish all 60 minute buckets once after the
		// hour seals; the query planner keeps the current partial hour as raw tail.
		through = sealed.Truncate(time.Hour)
	}
	from := through.Add(-lookback).Truncate(duration)
	if !through.After(from) {
		return 0, nil
	}
	markers, err := scheduler.runner.GenerationMarkers(ctx, resolution, from, through)
	if err != nil {
		return 0, err
	}
	byBucket := make(map[int64]flowch.RollupMarker, len(markers))
	for _, marker := range markers {
		byBucket[marker.Bucket.UTC().Unix()] = marker
	}
	if scheduler.lastRepairChecked == nil {
		scheduler.lastRepairChecked = make(map[flowHotRollupBucket]time.Time)
	}
	for key := range scheduler.lastRepairChecked {
		if key.resolution == resolution && key.bucketUnix < from.Unix() {
			delete(scheduler.lastRepairChecked, key)
		}
	}

	candidateLimit := maxBuckets
	if resolution == flowch.RollupOneHour {
		candidateLimit = max(maxBuckets, int(lookback/duration))
	}
	// Fill gaps oldest-first so a growing continuous prefix becomes queryable as
	// quickly as possible. Repairs are considered only after missing coverage.
	candidates := make([]flowch.RollupMarker, 0, candidateLimit)
	for bucket := from; bucket.Before(through) && len(candidates) < candidateLimit; bucket = bucket.Add(duration) {
		if _, exists := byBucket[bucket.Unix()]; !exists {
			candidates = append(candidates, flowch.RollupMarker{Bucket: bucket})
		}
	}
	// A storage-shape release can request an online regeneration of older hot
	// generations. Existing markers remain readable until the replacement
	// INSERT publishes a newer marker, so this compacts without a coverage gap.
	if scheduler.config.MinimumGeneration > 0 {
		for bucket := from; bucket.Before(through) && len(candidates) < candidateLimit; bucket = bucket.Add(duration) {
			marker, exists := byBucket[bucket.Unix()]
			if exists && marker.Generation < scheduler.config.MinimumGeneration && marker.Generation < lifecycleGenerationFloor {
				candidates = append(candidates, marker)
			}
		}
	}
	lateRepairs := make(map[int64]bool)
	repairFrom := through.Add(-lateArrivalWindow)
	for bucket := repairFrom; bucket.Before(through) && len(candidates) < candidateLimit; bucket = bucket.Add(duration) {
		marker, exists := byBucket[bucket.Unix()]
		if !exists || marker.Generation >= lifecycleGenerationFloor || marker.Generation < scheduler.config.MinimumGeneration || marker.GeneratedAt.IsZero() ||
			now.Sub(marker.GeneratedAt) < scheduler.config.RepairInterval {
			continue
		}
		key := flowHotRollupBucket{resolution: resolution, bucketUnix: bucket.Unix()}
		if checkedAt := scheduler.lastRepairChecked[key]; !checkedAt.IsZero() && now.Sub(checkedAt) < scheduler.config.RepairInterval {
			continue
		}
		needsRepair, err := scheduler.runner.BucketNeedsRepair(ctx, resolution, bucket)
		if err != nil {
			return 0, err
		}
		if !needsRepair {
			// Only a clean verdict is cached. A stale bucket that loses this
			// scan's heavy budget is checked again next scan instead of waiting
			// a whole repair interval; once repaired, its fresh generated_at
			// keeps it out of the window.
			scheduler.lastRepairChecked[key] = now
			continue
		}
		candidates = append(candidates, marker)
		lateRepairs[bucket.Unix()] = true
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Bucket.Before(candidates[j].Bucket)
	})
	if resolution == flowch.RollupOneMinute {
		return scheduler.runMinuteCandidateBatches(ctx, now, candidates)
	}

	completed := 0
	heavyCompleted := 0
	// Hours the minute scan still reaches get their 1m tier from it; an older
	// hour (the minute lookback expired while it was missing) is rebuilt here.
	minuteFrom := through.Add(-scheduler.config.MinuteLookback)
	for _, candidate := range candidates {
		generation, err := nextHotRollupGeneration(now, candidate.Generation)
		if err != nil {
			return completed, err
		}
		// A late-arrival repair means the hour's 1m tier was rolled from the same
		// incomplete raw, so it is rebuilt instead of trusted.
		stale := lateRepairs[candidate.Bucket.Unix()]
		sourceResolution := flowch.RollupResolution("")
		if resolution == flowch.RollupOneHour && !stale {
			sourceThrough, sourceErr := scheduler.runner.CoveredThroughAtLeast(
				ctx, flowch.RollupOneMinute, candidate.Bucket, candidate.Bucket.Add(time.Hour),
				scheduler.config.MinimumGeneration,
			)
			if sourceErr != nil {
				return completed, sourceErr
			}
			if sourceThrough.Equal(candidate.Bucket.Add(time.Hour)) {
				sourceResolution = flowch.RollupOneMinute
			}
		}
		markerOnly, rebuildMinutes := false, false
		if resolution == flowch.RollupOneHour && sourceResolution == "" {
			// BucketNeedsRepair already proved a stale hour has more raw records
			// than its aggregate.
			hasRaw := stale
			if !stale {
				hasRaw, err = scheduler.runner.RawBucketHasRecords(ctx, resolution, candidate.Bucket)
				if err != nil {
					return completed, err
				}
			}
			switch {
			case !hasRaw:
				markerOnly = true
			case (!stale && !candidate.Bucket.Before(minuteFrom)) || !candidate.Bucket.After(now.Add(-flowMinuteTierRetention)):
				// The minute scan completes this hour first, or its 1m tier has
				// aged out and the hour is left to the cold lifecycle archive.
				continue
			default:
				rebuildMinutes = true
			}
		}
		if !markerOnly && heavyCompleted >= maxBuckets {
			continue
		}
		if rebuildMinutes {
			if err := scheduler.rebuildMinuteHour(ctx, now, candidate.Bucket); err != nil {
				return completed, err
			}
			completed += int(time.Hour / time.Minute)
			sourceResolution = flowch.RollupOneMinute
		}
		if err := scheduler.runner.Run(ctx, flowch.RollupRequest{
			Resolution: resolution, SourceResolution: sourceResolution, Bucket: candidate.Bucket,
			Generation: generation, GeneratedAt: now, MarkerOnly: markerOnly,
			MaxThreads: scheduler.config.MaxThreads, Priority: scheduler.config.Priority,
			MaxMemoryBytes: scheduler.config.MaxMemoryBytes,
		}); err != nil {
			return completed, err
		}
		completed++
		if !markerOnly {
			heavyCompleted++
		}
	}
	return completed, nil
}

// rebuildMinuteHour regenerates all sixty 1m buckets of hour from raw in one
// batch, above every 1m generation the hour already has.
func (scheduler *flowHotRollupScheduler) rebuildMinuteHour(ctx context.Context, now, hour time.Time) error {
	markers, err := scheduler.runner.GenerationMarkers(ctx, flowch.RollupOneMinute, hour, hour.Add(time.Hour))
	if err != nil {
		return err
	}
	var current uint64
	for _, marker := range markers {
		current = max(current, marker.Generation)
	}
	generation, err := nextHotRollupGeneration(now, current)
	if err != nil {
		return err
	}
	return scheduler.runner.Run(ctx, flowch.RollupRequest{
		Resolution: flowch.RollupOneMinute, Bucket: hour, BucketEnd: hour.Add(time.Hour),
		Generation: generation, GeneratedAt: now,
		MaxThreads: scheduler.config.MaxThreads, Priority: scheduler.config.Priority,
		MaxMemoryBytes: scheduler.config.MaxMemoryBytes,
	})
}

func (scheduler *flowHotRollupScheduler) runMinuteCandidateBatches(ctx context.Context, now time.Time, candidates []flowch.RollupMarker) (int, error) {
	completed := 0
	for start := 0; start < len(candidates); {
		end := start + 1
		batchFrom := candidates[start].Bucket.UTC()
		batchThrough := batchFrom.Add(time.Minute)
		maxGeneration := candidates[start].Generation
		for end < len(candidates) && candidates[end].Bucket.UTC().Equal(batchThrough) &&
			candidates[end].Bucket.UTC().Truncate(time.Hour).Equal(batchFrom.Truncate(time.Hour)) {
			if candidates[end].Generation > maxGeneration {
				maxGeneration = candidates[end].Generation
			}
			batchThrough = batchThrough.Add(time.Minute)
			end++
		}
		generation, err := nextHotRollupGeneration(now, maxGeneration)
		if err != nil {
			return completed, err
		}
		if err := scheduler.runner.Run(ctx, flowch.RollupRequest{
			Resolution: flowch.RollupOneMinute, Bucket: batchFrom, BucketEnd: batchThrough,
			Generation: generation, GeneratedAt: now,
			MaxThreads: scheduler.config.MaxThreads, Priority: scheduler.config.Priority,
			MaxMemoryBytes: scheduler.config.MaxMemoryBytes,
		}); err != nil {
			return completed, err
		}
		completed += end - start
		start = end
	}
	return completed, nil
}

func nextHotRollupGeneration(now time.Time, current uint64) (uint64, error) {
	unix := now.UTC().Unix()
	if unix <= 0 || unix > math.MaxUint32 || current >= lifecycleGenerationFloor {
		return 0, fmt.Errorf("hot rollup generation namespace is exhausted or owned by the lifecycle archive")
	}
	generation := uint64(unix)
	if generation <= current {
		generation = current + 1
	}
	if generation >= lifecycleGenerationFloor {
		return 0, errors.New("hot rollup generation would overlap the lifecycle namespace")
	}
	return generation, nil
}
