// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type RollupResolution string

const (
	RollupOneMinute  RollupResolution = "1m"
	RollupFiveMinute RollupResolution = "5m"
	RollupOneHour    RollupResolution = "1h"
	RollupOneDay     RollupResolution = "1d"

	// LifecycleGenerationFloor reserves the high generation namespace for
	// retention-policy rollups, which stay authoritative across hot-cache
	// storage-shape rollouts.
	LifecycleGenerationFloor = uint64(1) << 32
)

type RollupRequest struct {
	Resolution       RollupResolution
	SourceResolution RollupResolution
	Bucket           time.Time
	// BucketEnd enables one hour-local batch of one-minute buckets. Data is
	// scanned once; completion markers are published only after that insert
	// succeeds. Zero preserves the single-bucket contract used by lifecycle jobs.
	BucketEnd   time.Time
	Generation  uint64
	GeneratedAt time.Time
	// MarkerOnly publishes an authoritative empty bucket after the caller has
	// separately proved that no physical countable raw record exists.
	MarkerOnly bool
	// Optional batch resource guards. Zero preserves the lifecycle runner's
	// existing defaults; the hot scheduler supplies deployment-owned limits.
	MaxThreads     uint64
	Priority       uint64
	MaxMemoryBytes uint64
}

// RollupMarker is the latest completion record for one aggregate bucket. It is
// published only after that generation's data INSERT succeeds. Generations
// below 2^32 are reserved for the recent-query cache;
// lifecycle archive generations use the policy-version namespace above it.
type RollupMarker struct {
	Bucket      time.Time
	Generation  uint64
	GeneratedAt time.Time
}

// StorageCounters is the conservation tuple used when a sealed UTC day is
// copied from raw records into the 1h archive. It intentionally contains no
// content hash: record count plus byte/packet counters detect loss, duplicate
// insertion, and counter drift without per-record CPU on the ingest path.
type StorageCounters struct {
	RecordCount           uint64
	RawBytes              uint64
	RawPackets            uint64
	EstimatedBytes        uint64
	EstimatedPackets      uint64
	EstimatedValidRecords uint64
}

type RollupRunner struct {
	executor queryExecutor
	stats    [4]rollupCounters
	terminal terminalCounters
	reaper   reaperCounters
	now      func() time.Time
}

// reaperCounters track the completion-watermark reaper (F1/F2): buckets it
// re-enqueued by reason, and buckets it abandoned after exhausting its retry cap
// (a counted, no-longer-silent permanent gap). Distinct from terminalCounters,
// which the worker bumps per terminal attempt; the reaper acts across attempts.
type reaperCounters struct {
	repairsGap    atomic.Uint64
	repairsFailed atomic.Uint64
	repairsLate   atomic.Uint64
	permanentGaps atomic.Uint64
}

// terminalCounters count rollup jobs that ended with no successful generation.
// The operation-job worker owns terminal-ness — retry-budget exhaustion in
// particular is invisible to Run, which only ever sees each retryable attempt —
// so it reports the final outcome here. A terminal failure leaves a permanent
// gap once the scheduled watermark advances past the bucket (the F1 hole), so
// this is the signal an operator alerts on.
type terminalCounters struct {
	permanent atomic.Uint64
	exhausted atomic.Uint64
}

type RollupResolutionStats struct {
	Attempts                  uint64
	Successes                 uint64
	RetryableErrors           uint64
	PermanentErrors           uint64
	InitialRebuilds           uint64
	RepairRebuilds            uint64
	LastSuccessUnix           uint64
	LatestCompletedBucketUnix uint64
}

type RollupStats struct {
	OneMinute  RollupResolutionStats
	FiveMinute RollupResolutionStats
	OneHour    RollupResolutionStats
	OneDay     RollupResolutionStats
	// TerminalPermanentFailures counts buckets abandoned on a non-retryable
	// (permanent) classification; TerminalExhaustedFailures counts buckets whose
	// retryable error (e.g. MEMORY_LIMIT) ran out of attempts. Both mean the
	// bucket has no successful generation and the aggregate has a permanent gap.
	// Resolution-agnostic: the worker reports terminal-ness without decoding the
	// job payload.
	TerminalPermanentFailures uint64
	TerminalExhaustedFailures uint64
	// Reaper activity (F1/F2). ReaperRepairs* count buckets the reaper re-enqueued
	// as the next generation: Gap = a bucket below the scheduled watermark with no
	// job at all, Failed = a bucket whose attempts were all terminal, Late =
	// a succeeded bucket whose base row count no longer matches its aggregate.
	// PermanentGaps counts buckets abandoned after the reaper's retry cap — the
	// honest, alarmable "we lost this bucket" signal (no longer a silent hole).
	ReaperRepairsGap    uint64
	ReaperRepairsFailed uint64
	ReaperRepairsLate   uint64
	PermanentGaps       uint64
}

type rollupCounters struct {
	attempts                  atomic.Uint64
	successes                 atomic.Uint64
	retryableErrors           atomic.Uint64
	permanentErrors           atomic.Uint64
	initialRebuilds           atomic.Uint64
	repairRebuilds            atomic.Uint64
	lastSuccessUnix           atomic.Uint64
	latestCompletedBucketUnix atomic.Uint64
}

// LatestGeneration reads the authoritative generation marker from
// ClickHouse. operation_jobs has finite retention, so historical repair must
// not infer generations from old job rows.
func (r *RollupRunner) LatestGeneration(ctx context.Context, resolution RollupResolution, bucket time.Time) (uint64, error) {
	duration, _, err := rollupTarget(resolution)
	if err != nil {
		return 0, Permanent(err)
	}
	request := RollupRequest{
		Resolution: resolution, Bucket: bucket,
		Generation: 1, GeneratedAt: bucket.UTC().Add(duration),
	}
	if err := ValidateRollupRequest(request); err != nil {
		return 0, Permanent(err)
	}
	if r == nil || r.executor == nil {
		return 0, Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	_, table, _ := rollupTarget(resolution)
	var generations proto.ColUInt64
	var generation uint64
	found := false
	query := ch.Query{
		Body: fmt.Sprintf(`SELECT max(generation) AS generation
FROM %s
WHERE bucket = {bucket:DateTime('UTC')}
  AND dimension_kind = '_generation'`, table),
		Parameters: ch.Parameters(map[string]any{
			"bucket": bucket.UTC().Format("2006-01-02 15:04:05"),
		}),
		Result: proto.Results{{Name: "generation", Data: &generations}},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if block.Rows != 1 || generations.Rows() != 1 || found {
			return Permanent(errors.New("ClickHouse rollup generation query returned an invalid row count"))
		}
		generation = generations[0]
		found = true
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return 0, classifyClickHouseError(fmt.Errorf("read ClickHouse %s rollup generation: %w", resolution, err))
	}
	if !found {
		return 0, Permanent(errors.New("ClickHouse rollup generation query returned no result"))
	}
	return generation, nil
}

// GenerationMarkers returns the latest complete generation for every covered
// bucket in [from,to). The _generation row is published only after the data
// INSERT succeeds, so its presence is the coverage authority for both the hot
// cache and the reconciled archive.
func (r *RollupRunner) GenerationMarkers(ctx context.Context, resolution RollupResolution, from, to time.Time) ([]RollupMarker, error) {
	duration, table, err := rollupTarget(resolution)
	if err != nil {
		return nil, Permanent(err)
	}
	from, to = from.UTC(), to.UTC()
	if r == nil || r.executor == nil {
		return nil, Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	if from.IsZero() || !to.After(from) || from.Truncate(duration) != from || to.Truncate(duration) != to {
		return nil, Permanent(fmt.Errorf("%s rollup marker range must be increasing and bucket-aligned", resolution))
	}
	var buckets proto.ColDateTime
	buckets.Location = time.UTC
	var generations proto.ColUInt64
	generatedAt := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
	markers := make([]RollupMarker, 0, int(to.Sub(from)/duration))
	query := ch.Query{
		Body: fmt.Sprintf(`SELECT
  bucket,
  max(generation) AS latest_generation,
  argMax(generated_at, generation) AS generated_at
FROM %s
WHERE bucket >= {from:DateTime('UTC')} AND bucket < {to:DateTime('UTC')}
  AND dimension_kind = '_generation'
GROUP BY bucket
ORDER BY bucket ASC`, table),
		Parameters: ch.Parameters(map[string]any{
			"from": from.Format("2006-01-02 15:04:05"),
			"to":   to.Format("2006-01-02 15:04:05"),
		}),
		Result: proto.Results{
			{Name: "bucket", Data: &buckets},
			{Name: "latest_generation", Data: &generations},
			{Name: "generated_at", Data: generatedAt},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if buckets.Rows() != block.Rows || generations.Rows() != block.Rows || generatedAt.Rows() != block.Rows {
			return Permanent(errors.New("ClickHouse rollup marker query returned inconsistent columns"))
		}
		for index := 0; index < block.Rows; index++ {
			markers = append(markers, RollupMarker{
				Bucket: buckets.Row(index).UTC(), Generation: generations[index], GeneratedAt: generatedAt.Row(index).UTC(),
			})
		}
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return nil, classifyClickHouseError(fmt.Errorf("read ClickHouse %s rollup markers: %w", resolution, err))
	}
	return markers, nil
}

// CoveredThrough returns the first uncovered bucket boundary in [from,to), or
// to when every bucket has a complete generation marker. It deliberately does
// not distinguish hot and lifecycle generations: both were built by the same
// replay-safe rollup and a later lifecycle generation supersedes the hot one.
func (r *RollupRunner) CoveredThrough(ctx context.Context, resolution RollupResolution, from, to time.Time) (time.Time, error) {
	return r.CoveredThroughAtLeast(ctx, resolution, from, to, 0)
}

// CoveredThroughAtLeast is CoveredThrough with a lower bound for hot-cache
// generations. It is used during an online storage-shape rollout so markers
// written by older binaries cannot authorize reads before their buckets have
// been republished. Lifecycle generations occupy the reserved high namespace
// and remain authoritative regardless of the hot-cache floor.
func (r *RollupRunner) CoveredThroughAtLeast(ctx context.Context, resolution RollupResolution, from, to time.Time, minimumGeneration uint64) (time.Time, error) {
	duration, _, err := rollupTarget(resolution)
	if err != nil {
		return time.Time{}, Permanent(err)
	}
	markers, err := r.GenerationMarkers(ctx, resolution, from, to)
	if err != nil {
		return time.Time{}, err
	}
	expected := from.UTC()
	for _, marker := range markers {
		if marker.Bucket.Before(expected) {
			continue
		}
		validGeneration := marker.Generation >= LifecycleGenerationFloor ||
			marker.Generation >= minimumGeneration
		if !marker.Bucket.Equal(expected) || marker.Generation == 0 || !validGeneration {
			break
		}
		expected = expected.Add(duration)
	}
	if expected.After(to.UTC()) {
		return to.UTC(), nil
	}
	return expected, nil
}

// RawBucketHasRecords cheaply tests physical countable input without FINAL.
// It is used only to prove that a marker-only hot bucket is safe. A physical
// row is sufficient to reject that shortcut; absence is definitive.
func (r *RollupRunner) RawBucketHasRecords(ctx context.Context, resolution RollupResolution, bucket time.Time) (bool, error) {
	duration, _, err := rollupTarget(resolution)
	if err != nil {
		return false, Permanent(err)
	}
	bucket = bucket.UTC()
	if r == nil || r.executor == nil {
		return false, Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	if bucket.IsZero() || bucket.Truncate(duration) != bucket {
		return false, Permanent(fmt.Errorf("%s raw existence bucket must be UTC-aligned", resolution))
	}
	value, err := r.scalarUInt64(ctx, ch.Query{
		Body: `SELECT toUInt64(count()) AS value
FROM (
  SELECT 1
  FROM flow_records
  PREWHERE event_time >= {start:DateTime('UTC')} AND event_time < {end:DateTime('UTC')}
  WHERE disposition = 'count'
  LIMIT 1
)`,
		Parameters: ch.Parameters(map[string]any{
			"start": bucket.Format("2006-01-02 15:04:05"),
			"end":   bucket.Add(duration).Format("2006-01-02 15:04:05"),
		}),
	})
	if err != nil {
		return false, fmt.Errorf("check %s physical raw bucket: %w", resolution, err)
	}
	return value != 0, nil
}

// BucketNeedsRepair reports whether a bucket's latest rolled generation no
// longer reflects its base data: the number of base records now in flow_records
// for the bucket differs from the received_records its aggregate recorded. Base
// records that arrived after the rollup ran are the common cause (F2). The
// reaper calls it only for buckets that already have a successful generation, so
// the aggregate side is populated. Public data rows and raw facts read FINAL;
// marker-only max(generation) subqueries do not need merge-time deduplication.
func (r *RollupRunner) BucketNeedsRepair(ctx context.Context, resolution RollupResolution, bucket time.Time) (bool, error) {
	duration, table, err := rollupTarget(resolution)
	if err != nil {
		return false, Permanent(err)
	}
	if r == nil || r.executor == nil {
		return false, Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	bucketUTC := bucket.UTC()
	if bucketUTC.Truncate(duration) != bucketUTC {
		return false, Permanent(fmt.Errorf("%s rollup bucket must be bucket-aligned", resolution))
	}
	end := bucketUTC.Add(duration)
	bucketParam := bucketUTC.Format("2006-01-02 15:04:05")
	stored, err := r.scalarUInt64(ctx, ch.Query{
		Body: fmt.Sprintf(`SELECT sum(received_records) AS value
FROM %s FINAL
WHERE bucket = {bucket:DateTime('UTC')}
  AND dimension_kind = 'total'
  AND generation = (
    SELECT max(generation) FROM %s
    WHERE bucket = {bucket:DateTime('UTC')} AND dimension_kind = '_generation'
  )`, table, table),
		Parameters: ch.Parameters(map[string]any{"bucket": bucketParam}),
	})
	if err != nil {
		return false, fmt.Errorf("read %s aggregate received_records: %w", resolution, err)
	}
	baseQuery := ch.Query{
		Body: `SELECT count() AS value
FROM flow_records FINAL
WHERE event_time >= {start:DateTime('UTC')}
  AND event_time < {end:DateTime('UTC')}
  AND disposition = 'count'`,
		Parameters: ch.Parameters(map[string]any{
			"start": bucketParam, "end": end.Format("2006-01-02 15:04:05"),
		}),
	}
	if resolution == RollupOneDay {
		baseQuery.Body = `SELECT sum(received_records) AS value
FROM flow_aggregate_1h FINAL
INNER JOIN (
  SELECT bucket, max(generation) AS generation
  FROM flow_aggregate_1h
  WHERE bucket >= {start:DateTime('UTC')} AND bucket < {end:DateTime('UTC')}
    AND dimension_kind = '_generation'
  GROUP BY bucket
) AS latest USING (bucket, generation)
WHERE bucket >= {start:DateTime('UTC')} AND bucket < {end:DateTime('UTC')}
  AND dimension_kind = 'total'`
	}
	live, err := r.scalarUInt64(ctx, baseQuery)
	if err != nil {
		return false, fmt.Errorf("read %s base record count: %w", resolution, err)
	}
	if live != stored || resolution != RollupOneDay {
		return live != stored, nil
	}
	newerSource, err := r.scalarUInt64(ctx, ch.Query{
		Body: `SELECT toUInt64(
  (SELECT max(source.generated_at)
   FROM flow_aggregate_1h AS source FINAL
   INNER JOIN (
     SELECT bucket, max(generation) AS generation
     FROM flow_aggregate_1h
     WHERE bucket >= {start:DateTime('UTC')} AND bucket < {end:DateTime('UTC')}
       AND dimension_kind = '_generation'
     GROUP BY bucket
   ) AS latest USING (bucket, generation)
   WHERE source.bucket >= {start:DateTime('UTC')} AND source.bucket < {end:DateTime('UTC')}
     AND source.dimension_kind = '_generation')
  >
  (SELECT argMax(generated_at, generation)
   FROM flow_aggregate_1d
   WHERE bucket = {start:DateTime('UTC')} AND dimension_kind = '_generation')
) AS value`,
		Parameters: baseQuery.Parameters,
	})
	if err != nil {
		return false, fmt.Errorf("compare %s source generation time: %w", resolution, err)
	}
	return newerSource != 0, nil
}

// scalarUInt64 runs a query returning a single UInt64 column named "value" and
// one row, classifying ClickHouse errors as retryable/permanent for the worker.
func (r *RollupRunner) scalarUInt64(ctx context.Context, query ch.Query) (uint64, error) {
	var column proto.ColUInt64
	var value uint64
	seen := false
	query.Result = proto.Results{{Name: "value", Data: &column}}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if block.Rows != 1 || column.Rows() != 1 || seen {
			return Permanent(errors.New("ClickHouse scalar query returned an invalid row count"))
		}
		value = column[0]
		seen = true
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return 0, classifyClickHouseError(err)
	}
	if !seen {
		return 0, Permanent(errors.New("ClickHouse scalar query returned no result"))
	}
	return value, nil
}

func NewRollupRunner(native *NativeInserter) (*RollupRunner, error) {
	if native == nil || native.executor == nil {
		return nil, errors.New("ClickHouse native connection is required")
	}
	return &RollupRunner{executor: native.executor, now: time.Now}, nil
}

// DayStorageCounters compares the raw source with the latest complete
// generation of each 1h archive bucket in one UTC day. _generation marker rows
// make an empty hour a complete hour; the archive side counts only public
// dimension_kind='total' rows from each hour's greatest generation.
func (r *RollupRunner) DayStorageCounters(ctx context.Context, sourceDate time.Time) (StorageCounters, StorageCounters, error) {
	if r == nil || r.executor == nil {
		return StorageCounters{}, StorageCounters{}, Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	day := sourceDate.UTC()
	if day.IsZero() || day != day.Truncate(24*time.Hour) {
		return StorageCounters{}, StorageCounters{}, Permanent(errors.New("storage counter UTC-aligned source date is required"))
	}
	end := day.Add(24 * time.Hour)
	params := ch.Parameters(map[string]any{
		"start": day.Format("2006-01-02 15:04:05"),
		"end":   end.Format("2006-01-02 15:04:05"),
	})
	raw, err := r.storageCounters(ctx, ch.Query{
		Body: `SELECT
  count() AS record_count,
  sum(raw_bytes) AS raw_bytes,
  sum(raw_packets) AS raw_packets,
  sumIf(estimated_bytes, estimated_valid) AS estimated_bytes,
  sumIf(estimated_packets, estimated_valid) AS estimated_packets,
  countIf(estimated_valid) AS estimated_valid_records
FROM flow_records FINAL
WHERE event_time >= {start:DateTime('UTC')}
  AND event_time < {end:DateTime('UTC')}
  AND disposition = 'count'`,
		Parameters: params,
	})
	if err != nil {
		return StorageCounters{}, StorageCounters{}, fmt.Errorf("read raw UTC-day counters: %w", err)
	}
	archive, err := r.storageCounters(ctx, ch.Query{
		Body: `SELECT
  sum(received_records) AS record_count,
  sum(raw_bytes) AS raw_bytes,
  sum(raw_packets) AS raw_packets,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  toUInt64(sum(received_records) - sum(unknown_sampling_records)) AS estimated_valid_records
FROM flow_aggregate_1h FINAL
INNER JOIN (
  SELECT bucket, max(generation) AS generation
  FROM flow_aggregate_1h
  WHERE bucket >= {start:DateTime('UTC')}
    AND bucket < {end:DateTime('UTC')}
    AND dimension_kind = '_generation'
  GROUP BY bucket
) AS latest USING (bucket, generation)
WHERE bucket >= {start:DateTime('UTC')}
  AND bucket < {end:DateTime('UTC')}
  AND dimension_kind = 'total'`,
		Parameters: params,
	})
	if err != nil {
		return StorageCounters{}, StorageCounters{}, fmt.Errorf("read 1h archive UTC-day counters: %w", err)
	}
	return raw, archive, nil
}

// RawDayPhysicalRecords counts every logical Flow fact in the UTC partition,
// including disposition=drop rows excluded from conservation/billing totals.
// Destructive verification uses this count so a zero billable total cannot be
// mistaken for an already-absent partition.
func (r *RollupRunner) RawDayPhysicalRecords(ctx context.Context, sourceDate time.Time) (uint64, error) {
	if r == nil || r.executor == nil {
		return 0, Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	day := sourceDate.UTC()
	if day.IsZero() || day != day.Truncate(24*time.Hour) {
		return 0, Permanent(errors.New("physical record count requires a UTC-aligned source date"))
	}
	return r.scalarUInt64(ctx, ch.Query{
		Body: `SELECT count() AS value
FROM flow_records FINAL
WHERE event_time >= {start:DateTime('UTC')}
  AND event_time < {end:DateTime('UTC')}`,
		Parameters: ch.Parameters(map[string]any{
			"start": day.Format("2006-01-02 15:04:05"),
			"end":   day.Add(24 * time.Hour).Format("2006-01-02 15:04:05"),
		}),
	})
}

// ArchiveMonthStorageCounters reads the latest complete generation for every
// one-hour bucket present in one UTC calendar month. The management lifecycle
// separately proves that every UTC day in the month reached raw_deleted.
func (r *RollupRunner) ArchiveMonthStorageCounters(ctx context.Context, monthStart time.Time) (StorageCounters, error) {
	month, end, err := archiveMonthBounds(monthStart)
	if err != nil {
		return StorageCounters{}, Permanent(err)
	}
	if r == nil || r.executor == nil {
		return StorageCounters{}, Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	return r.storageCounters(ctx, ch.Query{
		Body: `SELECT
  sum(received_records) AS record_count,
  sum(raw_bytes) AS raw_bytes,
  sum(raw_packets) AS raw_packets,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  toUInt64(sum(received_records) - sum(unknown_sampling_records)) AS estimated_valid_records
FROM flow_aggregate_1h FINAL
INNER JOIN (
  SELECT bucket, max(generation) AS generation
  FROM flow_aggregate_1h
  WHERE bucket >= {start:DateTime('UTC')}
    AND bucket < {end:DateTime('UTC')}
    AND dimension_kind = '_generation'
  GROUP BY bucket
) AS latest USING (bucket, generation)
WHERE bucket >= {start:DateTime('UTC')}
  AND bucket < {end:DateTime('UTC')}
  AND dimension_kind = 'total'`,
		Parameters: ch.Parameters(map[string]any{
			"start": month.Format("2006-01-02 15:04:05"),
			"end":   end.Format("2006-01-02 15:04:05"),
		}),
	})
}

// ArchiveMonthPhysicalRecords includes public dimensions and generation
// markers. It prevents an empty billable total from being mistaken for an
// already-removed physical archive partition.
func (r *RollupRunner) ArchiveMonthPhysicalRecords(ctx context.Context, monthStart time.Time) (uint64, error) {
	month, end, err := archiveMonthBounds(monthStart)
	if err != nil {
		return 0, Permanent(err)
	}
	if r == nil || r.executor == nil {
		return 0, Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	return r.scalarUInt64(ctx, ch.Query{
		Body: `SELECT count() AS value
FROM flow_aggregate_1h FINAL
WHERE bucket >= {start:DateTime('UTC')}
  AND bucket < {end:DateTime('UTC')}`,
		Parameters: ch.Parameters(map[string]any{
			"start": month.Format("2006-01-02 15:04:05"),
			"end":   end.Format("2006-01-02 15:04:05"),
		}),
	})
}

func archiveMonthBounds(value time.Time) (time.Time, time.Time, error) {
	month := value.UTC()
	if month.IsZero() || value.Location() != time.UTC || month.Day() != 1 || month.Hour() != 0 || month.Minute() != 0 || month.Second() != 0 || month.Nanosecond() != 0 {
		return time.Time{}, time.Time{}, errors.New("UTC-aligned archive month is required")
	}
	return month, month.AddDate(0, 1, 0), nil
}

func (r *RollupRunner) storageCounters(ctx context.Context, query ch.Query) (StorageCounters, error) {
	var recordCount, rawBytes, rawPackets, estimatedBytes, estimatedPackets, estimatedValid proto.ColUInt64
	var result StorageCounters
	seen := false
	query.Result = proto.Results{
		{Name: "record_count", Data: &recordCount},
		{Name: "raw_bytes", Data: &rawBytes},
		{Name: "raw_packets", Data: &rawPackets},
		{Name: "estimated_bytes", Data: &estimatedBytes},
		{Name: "estimated_packets", Data: &estimatedPackets},
		{Name: "estimated_valid_records", Data: &estimatedValid},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if seen || block.Rows != 1 || recordCount.Rows() != 1 || rawBytes.Rows() != 1 || rawPackets.Rows() != 1 ||
			estimatedBytes.Rows() != 1 || estimatedPackets.Rows() != 1 || estimatedValid.Rows() != 1 {
			return Permanent(errors.New("ClickHouse storage counter query returned an invalid row count"))
		}
		seen = true
		result = StorageCounters{recordCount[0], rawBytes[0], rawPackets[0], estimatedBytes[0], estimatedPackets[0], estimatedValid[0]}
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return StorageCounters{}, classifyClickHouseError(err)
	}
	if !seen {
		return StorageCounters{}, Permanent(errors.New("ClickHouse storage counter query returned no result"))
	}
	return result, nil
}

// Run rebuilds one closed bucket (or one hour-local minute range) and publishes
// its completion marker only after the data INSERT succeeds. ClickHouse INSERT
// SELECT can expose parts before a failed statement returns, so the marker is a
// separate commit record: readers can never authorize a partially written
// generation. A retry reuses the deterministic data/marker tokens and the same
// generation, or supplies a greater generation after late base records arrive.
func (r *RollupRunner) Run(ctx context.Context, request RollupRequest) error {
	dataQuery, err := buildRollupQuery(request)
	if err != nil {
		return Permanent(err)
	}
	queries := []ch.Query{dataQuery}
	if !request.MarkerOnly {
		markerQuery, markerErr := buildRollupMarkerQuery(request)
		if markerErr != nil {
			return Permanent(markerErr)
		}
		queries = append(queries, markerQuery)
	}
	if r == nil || r.executor == nil {
		return Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	index, duration := rollupStatsTarget(request.Resolution)
	r.stats[index].attempts.Add(1)
	for queryIndex, query := range queries {
		if err := r.executor.Do(ctx, query); err != nil {
			phase := "data"
			if request.MarkerOnly || queryIndex == 1 {
				phase = "marker"
			}
			classified := classifyClickHouseError(fmt.Errorf("rebuild ClickHouse %s bucket %s phase: %w", request.Resolution, phase, err))
			var permanent *PermanentError
			if errors.As(classified, &permanent) {
				r.stats[index].permanentErrors.Add(1)
			} else {
				r.stats[index].retryableErrors.Add(1)
			}
			return classified
		}
	}
	r.stats[index].successes.Add(1)
	if request.Generation == 1 {
		r.stats[index].initialRebuilds.Add(1)
	} else {
		r.stats[index].repairRebuilds.Add(1)
	}
	now := time.Now()
	if r.now != nil {
		now = r.now()
	}
	storeMax(&r.stats[index].lastSuccessUnix, uint64(now.UTC().Unix()))
	completedThrough := request.Bucket.UTC().Add(duration)
	if !request.BucketEnd.IsZero() {
		completedThrough = request.BucketEnd.UTC()
	}
	storeMax(&r.stats[index].latestCompletedBucketUnix, uint64(completedThrough.Unix()))
	return nil
}

func (r *RollupRunner) Stats() RollupStats {
	if r == nil {
		return RollupStats{}
	}
	return RollupStats{
		OneMinute:                 snapshotRollupCounters(&r.stats[0]),
		OneHour:                   snapshotRollupCounters(&r.stats[1]),
		OneDay:                    snapshotRollupCounters(&r.stats[2]),
		FiveMinute:                snapshotRollupCounters(&r.stats[3]),
		TerminalPermanentFailures: r.terminal.permanent.Load(),
		TerminalExhaustedFailures: r.terminal.exhausted.Load(),
		ReaperRepairsGap:          r.reaper.repairsGap.Load(),
		ReaperRepairsFailed:       r.reaper.repairsFailed.Load(),
		ReaperRepairsLate:         r.reaper.repairsLate.Load(),
		PermanentGaps:             r.reaper.permanentGaps.Load(),
	}
}

// RollupRepairGap, RollupRepairFailed, and RollupRepairLate name the reasons the
// reaper re-enqueues a bucket, so its counters and logs stay consistent.
const (
	RollupRepairGap    = "gap"
	RollupRepairFailed = "failed"
	RollupRepairLate   = "late"
)

// RecordReaperRepair counts a bucket the reaper re-enqueued as the next
// generation, by reason. Safe for concurrent callers; a nil runner is a no-op.
func (r *RollupRunner) RecordReaperRepair(reason string) {
	if r == nil {
		return
	}
	switch reason {
	case RollupRepairGap:
		r.reaper.repairsGap.Add(1)
	case RollupRepairFailed:
		r.reaper.repairsFailed.Add(1)
	case RollupRepairLate:
		r.reaper.repairsLate.Add(1)
	}
}

// RecordPermanentGap counts a bucket the reaper abandoned after exhausting its
// retry cap. Safe for concurrent callers; a nil runner is a no-op.
func (r *RollupRunner) RecordPermanentGap() {
	if r == nil {
		return
	}
	r.reaper.permanentGaps.Add(1)
}

// RecordTerminalFailure counts a rollup job the worker gave up on with no
// successful generation. exhausted distinguishes a retry-budget exhaustion
// (a retryable error that ran out of attempts) from a non-retryable (permanent)
// classification. Safe for concurrent callers; a nil runner is a no-op.
func (r *RollupRunner) RecordTerminalFailure(exhausted bool) {
	if r == nil {
		return
	}
	if exhausted {
		r.terminal.exhausted.Add(1)
		return
	}
	r.terminal.permanent.Add(1)
}

func snapshotRollupCounters(value *rollupCounters) RollupResolutionStats {
	return RollupResolutionStats{
		Attempts: value.attempts.Load(), Successes: value.successes.Load(),
		RetryableErrors: value.retryableErrors.Load(), PermanentErrors: value.permanentErrors.Load(),
		InitialRebuilds: value.initialRebuilds.Load(), RepairRebuilds: value.repairRebuilds.Load(),
		LastSuccessUnix: value.lastSuccessUnix.Load(), LatestCompletedBucketUnix: value.latestCompletedBucketUnix.Load(),
	}
}

func rollupStatsTarget(resolution RollupResolution) (int, time.Duration) {
	if resolution == RollupOneDay {
		return 2, 24 * time.Hour
	}
	if resolution == RollupOneHour {
		return 1, time.Hour
	}
	if resolution == RollupFiveMinute {
		return 3, 5 * time.Minute
	}
	return 0, time.Minute
}

func storeMax(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

// ValidateRollupRequest validates the public rollup contract without issuing
// ClickHouse I/O. Operation-job producers use it before enqueueing so a bad
// bucket can never consume the retry budget.
func ValidateRollupRequest(request RollupRequest) error {
	duration, _, err := rollupTarget(request.Resolution)
	if err != nil {
		return err
	}
	if request.SourceResolution != "" &&
		!((request.Resolution == RollupOneHour && request.SourceResolution == RollupOneMinute) ||
			(request.Resolution == RollupFiveMinute && request.SourceResolution == RollupOneMinute)) {
		return errors.New("rollup source resolution is invalid")
	}
	// The five-minute tier is only ever derived from the complete one-minute
	// tier (design 2026-09-23 §8.2). A cold five-minute-from-raw path is a later
	// iteration; until then a source is mandatory so a bare "5m" cannot fall
	// through to the raw scan builder.
	if request.Resolution == RollupFiveMinute && request.SourceResolution != RollupOneMinute {
		return errors.New("five-minute rollup must derive from the one-minute tier")
	}
	if request.MarkerOnly && (request.SourceResolution != "" || !request.BucketEnd.IsZero() || request.Resolution == RollupOneDay) {
		return errors.New("marker-only rollup must be one empty minute/hour bucket without a derived source")
	}
	if request.Generation == 0 || request.GeneratedAt.IsZero() {
		return errors.New("rollup generation and generated_at are required")
	}
	if request.MaxThreads > 256 || request.Priority > 10_000 ||
		(request.MaxMemoryBytes > 0 && request.MaxMemoryBytes < 1<<30) {
		return errors.New("rollup resource limits are invalid")
	}
	bucket := request.Bucket.UTC()
	end := bucket.Add(duration)
	_, bucketOffset := request.Bucket.Zone()
	_, generatedOffset := request.GeneratedAt.Zone()
	if bucket.IsZero() || bucketOffset != 0 || generatedOffset != 0 || bucket.Truncate(duration) != bucket {
		return fmt.Errorf("%s rollup bucket and generated_at must be UTC and bucket-aligned", request.Resolution)
	}
	if !request.BucketEnd.IsZero() {
		_, endOffset := request.BucketEnd.Zone()
		end = request.BucketEnd.UTC()
		switch request.Resolution {
		case RollupOneMinute:
			if endOffset != 0 || !end.After(bucket) ||
				end.Sub(bucket) > time.Hour || end.Truncate(time.Minute) != end ||
				bucket.Truncate(time.Hour) != end.Add(-time.Nanosecond).Truncate(time.Hour) {
				return errors.New("minute rollup batch must be UTC-aligned, increasing, at most one hour and hour-local")
			}
		case RollupFiveMinute:
			if endOffset != 0 || !end.After(bucket) ||
				end.Sub(bucket) > 24*time.Hour || end.Truncate(5*time.Minute) != end {
				return errors.New("five-minute rollup batch must be UTC-aligned, increasing and at most one day")
			}
		default:
			return errors.New("only minute and five-minute rollups support a batch range")
		}
	}
	if request.GeneratedAt.UTC().Before(end) {
		return fmt.Errorf("%s rollup generated_at precedes the closed bucket end", request.Resolution)
	}
	if bucket.Unix() < 0 {
		return fmt.Errorf("%s rollup bucket must not predate the Unix epoch", request.Resolution)
	}
	return nil
}

// rollupIPTopN caps how many distinct src_ip/dst_ip values a rollup group keeps
// (ranked by estimated_bytes); the long tail beyond it is folded into a single
// _other bucket. These per-IP dimensions otherwise materialize at nearly raw
// cardinality in every policy-aged archive generation. It is a package var so
// a test can lower it; wire it to config when deployment tuning is needed.
var rollupIPTopN uint32 = 1000

// remote_port was the live cardinality outlier: roughly 3.7M of 3.8M latest
// rows across seven hourly buckets. Keep enough candidates for report top-N,
// but fold its long tail just like IP dimensions so the read tier stays small.
var rollupPortTopN uint32 = 256

func buildRollupQuery(request RollupRequest) (ch.Query, error) {
	duration, table, err := rollupTarget(request.Resolution)
	if err != nil {
		return ch.Query{}, err
	}
	if err := ValidateRollupRequest(request); err != nil {
		return ch.Query{}, err
	}
	if request.MarkerOnly {
		return buildRollupMarkerQuery(request)
	}
	bucket := request.Bucket.UTC()
	generatedAt := request.GeneratedAt.UTC()
	end := bucket.Add(duration)
	if !request.BucketEnd.IsZero() {
		end = request.BucketEnd.UTC()
	}
	bucketExpression := "toStartOfMinute(event_time)"
	if request.Resolution == RollupOneHour {
		bucketExpression = "toStartOfHour(event_time)"
	}
	body := fmt.Sprintf(rollupSQL, table, bucketExpression)
	if request.Resolution == RollupOneDay {
		body = fmt.Sprintf(derivedRollupSQL, table, "flow_aggregate_1h", "flow_aggregate_1h")
	} else if request.Resolution == RollupOneHour && request.SourceResolution == RollupOneMinute {
		body = fmt.Sprintf(derivedRollupSQL, table, "flow_aggregate_1m", "flow_aggregate_1m")
	} else if request.Resolution == RollupFiveMinute && request.SourceResolution == RollupOneMinute {
		body = fmt.Sprintf(derivedFiveMinuteRollupSQL, table, "flow_aggregate_1m", "flow_aggregate_1m")
	}
	query := ch.Query{
		Body: body,
		Parameters: ch.Parameters(map[string]any{
			"bucket_start": bucket.Format("2006-01-02 15:04:05"),
			"bucket_end":   end.Format("2006-01-02 15:04:05"), "generation": request.Generation,
			"generated_at": generatedAt.Format("2006-01-02 15:04:05.000"),
			"top_n":        rollupIPTopN, "port_top_n": rollupPortTopN,
		}),
		Settings: rollupInsertSettings(request, end, "data", true),
	}
	return query, nil
}

func buildRollupMarkerQuery(request RollupRequest) (ch.Query, error) {
	duration, table, err := rollupTarget(request.Resolution)
	if err != nil {
		return ch.Query{}, err
	}
	if err := ValidateRollupRequest(request); err != nil {
		return ch.Query{}, err
	}
	bucket := request.Bucket.UTC()
	end := bucket.Add(duration)
	if !request.BucketEnd.IsZero() {
		end = request.BucketEnd.UTC()
	}
	return ch.Query{
		Body: fmt.Sprintf(generationMarkerRollupSQL, table),
		Parameters: ch.Parameters(map[string]any{
			"bucket_start":   bucket.Format("2006-01-02 15:04:05"),
			"bucket_end":     end.Format("2006-01-02 15:04:05"),
			"bucket_seconds": uint32(duration / time.Second),
			"generation":     request.Generation,
			"generated_at":   request.GeneratedAt.UTC().Format("2006-01-02 15:04:05.000"),
		}),
		Settings: rollupInsertSettings(request, end, "marker", false),
	}, nil
}

func rollupInsertSettings(request RollupRequest, end time.Time, phase string, heavy bool) []ch.Setting {
	tokenInput := fmt.Sprintf("watchdog-flow-rollup-v3\x00%s\x00%s\x00%d\x00%d\x00%d", phase, request.Resolution,
		request.Bucket.UTC().Unix(), end.UTC().Unix(), request.Generation)
	token := sha256.Sum256([]byte(tokenInput))
	settings := []ch.Setting{
		{Key: "async_insert", Value: "0", Important: true},
		{Key: "wait_for_async_insert", Value: "1", Important: true},
		{Key: "insert_deduplication_token", Value: hex.EncodeToString(token[:]), Important: true},
	}
	if heavy {
		// A heavy bucket GROUP BY must spill to disk rather than OOM.
		spill, memory := uint64(4<<30), uint64(10<<30)
		if request.MaxMemoryBytes > 0 {
			memory = request.MaxMemoryBytes
			spill = min(memory/2, uint64(4<<30))
		}
		settings = append(settings,
			ch.Setting{Key: "max_bytes_before_external_group_by", Value: strconv.FormatUint(spill, 10), Important: true},
			ch.Setting{Key: "max_memory_usage", Value: strconv.FormatUint(memory, 10), Important: true},
		)
	}
	if request.MaxThreads > 0 {
		settings = append(settings, ch.Setting{Key: "max_threads", Value: strconv.FormatUint(request.MaxThreads, 10), Important: true})
	}
	if request.Priority > 0 {
		settings = append(settings, ch.Setting{Key: "priority", Value: strconv.FormatUint(request.Priority, 10), Important: true})
	}
	return settings
}

func rollupTarget(resolution RollupResolution) (time.Duration, string, error) {
	switch resolution {
	case RollupOneMinute:
		return time.Minute, "flow_aggregate_1m", nil
	case RollupFiveMinute:
		return 5 * time.Minute, "flow_aggregate_5m", nil
	case RollupOneHour:
		return time.Hour, "flow_aggregate_1h", nil
	case RollupOneDay:
		return 24 * time.Hour, "flow_aggregate_1d", nil
	default:
		return 0, "", fmt.Errorf("unsupported rollup resolution %q", resolution)
	}
}

// rollupSQL writes data rows only. Run publishes _generation markers in a
// second synchronous INSERT after this statement succeeds.
const rollupSQL = `INSERT INTO %s (
  bucket, target_id, device_id, exporter_id,
  business_direction, category, business, dimension_kind, dimension_value,
  dimension_snapshot_id, geo_version, classification_version,
  raw_bytes, raw_packets, estimated_bytes, estimated_packets,
  received_records, unknown_sampling_records, quality_records,
  generation, generated_at)
WITH
  {bucket_start:DateTime('UTC')} AS rollup_start,
  {bucket_end:DateTime('UTC')} AS rollup_end,
  {generation:UInt64} AS rollup_generation,
  {generated_at:DateTime64(3, 'UTC')} AS rollup_generated_at,
  {top_n:UInt32} AS rollup_top_n,
  {port_top_n:UInt32} AS rollup_port_top_n
SELECT
  bucket, target_id, device_id, exporter_id,
  business_direction, category, business, dimension_kind,
  -- Fold high-cardinality long tails into one _other bucket. remote_port was
  -- the live outlier, while src_ip/dst_ip retain a larger diagnostic budget.
  if(dimension_kind IN ('src_ip', 'dst_ip', 'remote_port') AND cardinality_rank >
       if(dimension_kind = 'remote_port', rollup_port_top_n, rollup_top_n),
     '_other', dimension_value) AS dimension_value,
  dimension_snapshot_id, geo_version, classification_version,
  sum(raw_bytes) AS raw_bytes,
  sum(raw_packets) AS raw_packets,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(received_records) AS received_records,
  sum(unknown_sampling_records) AS unknown_sampling_records,
  sum(quality_records) AS quality_records,
  generation, generated_at
FROM (
  SELECT
    bucket, target_id, device_id, exporter_id,
    business_direction, category, business, dimension_kind, dimension_value,
    dimension_snapshot_id, geo_version, classification_version,
    raw_bytes, raw_packets, estimated_bytes, estimated_packets,
    received_records, unknown_sampling_records, quality_records,
    generation, generated_at,
    if(dimension_kind IN ('src_ip', 'dst_ip', 'remote_port'),
       row_number() OVER (
         PARTITION BY bucket, target_id, device_id, exporter_id,
           business_direction, category, business, dimension_kind,
           dimension_snapshot_id, geo_version, classification_version
         ORDER BY estimated_bytes DESC, dimension_value ASC),
       0) AS cardinality_rank
  FROM (
    SELECT
      %s AS bucket,
      target_id,
      device_id,
      exporter_id,
      toString(business_direction) AS business_direction,
      toString(category) AS category,
      business,
      tupleElement(dimension, 1) AS dimension_kind,
      tupleElement(dimension, 2) AS dimension_value,
      dimension_snapshot_id,
      geo_version,
      classification_version,
      sum(raw_bytes) AS raw_bytes,
      sum(raw_packets) AS raw_packets,
      sum(estimated_bytes) AS estimated_bytes,
      sum(estimated_packets) AS estimated_packets,
      count() AS received_records,
      countIf(NOT estimated_valid) AS unknown_sampling_records,
      countIf(quality_flags != 0) AS quality_records,
      rollup_generation AS generation,
      rollup_generated_at AS generated_at
    FROM flow_records FINAL
    ARRAY JOIN arrayFilter(item -> tupleElement(item, 2) != '', arrayConcat(
      [
        tuple('total', 'total'),
        tuple('category', toString(category)),
        tuple('geo.continent', if(empty(remote_geo_continent_id), '_unassigned', remote_geo_continent_id)),
        tuple('geo.region', if(empty(remote_geo_region_id), '_unassigned', remote_geo_region_id)),
        tuple('geo.country', if(empty(remote_geo_country_id), '_unassigned', remote_geo_country_id)),
        tuple('geo.province', if(empty(remote_geo_province_id), '_unassigned', remote_geo_province_id)),
        tuple('geo.city', if(empty(remote_geo_city_id), '_unassigned', remote_geo_city_id)),
        tuple('isp', if(remote_isp_id = 0, '_unassigned', toString(remote_isp_id))),
        tuple('asn', if(remote_asn = 0, '_unassigned', toString(remote_asn))),
        tuple('business', if(empty(business), '_unassigned', business)),
        tuple('local_prefix', if(empty(local_prefix_id), '_unassigned', local_prefix_id)),
        tuple('remote_prefix', if(empty(remote_prefix_id), '_unassigned', remote_prefix_id)),
        tuple('src_ip', toString(src_ip)),
        tuple('dst_ip', toString(dst_ip)),
        tuple('remote_port', if(remote_port = 0, '_unassigned', toString(remote_port))),
        tuple('protocol', toString(ip_protocol)),
        tuple('observation_interface', if(observation_if_index = 0, '_unassigned', toString(observation_if_index)))
      ],
      arrayMap(value -> tuple('address_set', value), arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids)))
    )) AS dimension
    WHERE event_time >= rollup_start
      AND event_time < rollup_end
      AND disposition = 'count'
    GROUP BY
      bucket, target_id, device_id, exporter_id, business_direction, category,
      business, dimension_kind, dimension_value, dimension_snapshot_id,
      geo_version, classification_version
  )
)
GROUP BY
  bucket, target_id, device_id, exporter_id, business_direction, category,
  business, dimension_kind, dimension_value, dimension_snapshot_id,
  geo_version, classification_version, generation, generated_at`

// generationMarkerRollupSQL is the commit-record phase. A minute range emits
// one marker per bucket; every other request emits exactly one. Readers ignore
// data generations that have no corresponding marker.
const generationMarkerRollupSQL = `INSERT INTO %s (
  bucket, target_id, device_id, exporter_id,
  business_direction, category, business, dimension_kind, dimension_value,
  dimension_snapshot_id, geo_version, classification_version,
  raw_bytes, raw_packets, estimated_bytes, estimated_packets,
  received_records, unknown_sampling_records, quality_records,
  generation, generated_at)
WITH
  {bucket_start:DateTime('UTC')} AS rollup_start,
  {bucket_end:DateTime('UTC')} AS rollup_end,
  {bucket_seconds:UInt32} AS rollup_bucket_seconds
SELECT
  toDateTime(rollup_start + toIntervalSecond(toUInt32(number) * rollup_bucket_seconds)),
  '', '', '', 'ambiguous', 'unknown', '',
  '_generation', '', '', '', 0,
  0, 0, 0, 0, 0, 0, 0, {generation:UInt64}, {generated_at:DateTime64(3, 'UTC')}
FROM numbers(toUInt64(intDiv(dateDiff('second', rollup_start, rollup_end), rollup_bucket_seconds)))`

// derivedRollupSQL compacts a finer, marker-published aggregate tier into a
// coarser one. The hot scheduler uses it for 1m -> 1h only after verifying
// all 60 source markers; lifecycle uses it for 1h -> 1d after reconciliation.
// It avoids expanding the raw EAV dimensions a second time for every hour.
const derivedRollupSQL = `INSERT INTO %s (
  bucket, target_id, device_id, exporter_id,
  business_direction, category, business, dimension_kind, dimension_value,
  dimension_snapshot_id, geo_version, classification_version,
  raw_bytes, raw_packets, estimated_bytes, estimated_packets,
  received_records, unknown_sampling_records, quality_records,
  generation, generated_at)
WITH
  {bucket_start:DateTime('UTC')} AS rollup_start,
  {bucket_end:DateTime('UTC')} AS rollup_end,
  {generation:UInt64} AS rollup_generation,
  {generated_at:DateTime64(3, 'UTC')} AS rollup_generated_at,
  {top_n:UInt32} AS rollup_top_n,
  {port_top_n:UInt32} AS rollup_port_top_n,
  latest AS (
    SELECT bucket, max(generation) AS generation
    FROM %s
    WHERE bucket >= rollup_start AND bucket < rollup_end
      AND dimension_kind = '_generation'
    GROUP BY bucket
  )
SELECT
  bucket, target_id, device_id, exporter_id,
  business_direction, category, business, dimension_kind,
  if(dimension_kind IN ('src_ip', 'dst_ip', 'remote_port') AND dimension_value != '_other' AND cardinality_rank >
       if(dimension_kind = 'remote_port', rollup_port_top_n, rollup_top_n),
     '_other', dimension_value) AS dimension_value,
  dimension_snapshot_id, geo_version, classification_version,
  sum(raw_bytes), sum(raw_packets), sum(estimated_bytes), sum(estimated_packets),
  sum(received_records), sum(unknown_sampling_records), sum(quality_records),
  generation, generated_at
FROM (
  SELECT *,
    if(dimension_kind IN ('src_ip', 'dst_ip', 'remote_port'),
       row_number() OVER (
         PARTITION BY target_id, device_id, exporter_id,
           business_direction, category, business, dimension_kind,
           dimension_snapshot_id, geo_version, classification_version
         ORDER BY (dimension_value = '_other') ASC, estimated_bytes DESC, dimension_value ASC),
       0) AS cardinality_rank
  FROM (
    SELECT
      rollup_start AS bucket,
      source.target_id, source.device_id, source.exporter_id,
      source.business_direction, source.category, source.business,
      source.dimension_kind, source.dimension_value,
      source.dimension_snapshot_id, source.geo_version, source.classification_version,
      sum(source.raw_bytes) AS raw_bytes,
      sum(source.raw_packets) AS raw_packets,
      sum(source.estimated_bytes) AS estimated_bytes,
      sum(source.estimated_packets) AS estimated_packets,
      sum(source.received_records) AS received_records,
      sum(source.unknown_sampling_records) AS unknown_sampling_records,
      sum(source.quality_records) AS quality_records,
      rollup_generation AS generation,
      rollup_generated_at AS generated_at
    FROM %s AS source FINAL
    INNER JOIN latest USING (bucket, generation)
    WHERE source.bucket >= rollup_start AND source.bucket < rollup_end
      AND source.dimension_kind != '_generation'
    GROUP BY
      source.target_id, source.device_id, source.exporter_id,
      source.business_direction, source.category, source.business,
      source.dimension_kind, source.dimension_value,
      source.dimension_snapshot_id, source.geo_version, source.classification_version
  )
)
GROUP BY
  bucket, target_id, device_id, exporter_id, business_direction, category,
  business, dimension_kind, dimension_value, dimension_snapshot_id,
  geo_version, classification_version, generation, generated_at`

// derivedFiveMinuteRollupSQL compacts the complete, marker-published one-minute
// tier into five-minute buckets. It is the hot path for flow_aggregate_5m: the
// scheduler calls it only after all source 1m markers exist. Unlike the 1m->1h
// path it drops the high-cardinality src_ip / dst_ip / remote_port kinds
// entirely (they remain in raw and the 2-day 1m tier), which is where the 5m
// query layer's capacity saving comes from; the retained non-endpoint kinds are
// unfolded in 1m, so their five-minute sums are exact. Run publishes the
// _generation markers in a separate INSERT after this statement succeeds.
const derivedFiveMinuteRollupSQL = `INSERT INTO %s (
  bucket, target_id, device_id, exporter_id,
  business_direction, category, business, dimension_kind, dimension_value,
  dimension_snapshot_id, geo_version, classification_version,
  raw_bytes, raw_packets, estimated_bytes, estimated_packets,
  received_records, unknown_sampling_records, quality_records,
  generation, generated_at)
WITH
  {bucket_start:DateTime('UTC')} AS rollup_start,
  {bucket_end:DateTime('UTC')} AS rollup_end,
  {generation:UInt64} AS rollup_generation,
  {generated_at:DateTime64(3, 'UTC')} AS rollup_generated_at,
  latest AS (
    SELECT bucket, max(generation) AS generation
    FROM %s
    WHERE bucket >= rollup_start AND bucket < rollup_end
      AND dimension_kind = '_generation'
    GROUP BY bucket
  )
SELECT
  bucket, target_id, device_id, exporter_id,
  business_direction, category, business, dimension_kind, dimension_value,
  dimension_snapshot_id, geo_version, classification_version,
  sum(raw_bytes) AS raw_bytes,
  sum(raw_packets) AS raw_packets,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  sum(received_records) AS received_records,
  sum(unknown_sampling_records) AS unknown_sampling_records,
  sum(quality_records) AS quality_records,
  generation, generated_at
FROM (
  SELECT
    toStartOfFiveMinutes(source.bucket) AS bucket,
    source.target_id AS target_id, source.device_id AS device_id, source.exporter_id AS exporter_id,
    source.business_direction AS business_direction, source.category AS category, source.business AS business,
    source.dimension_kind AS dimension_kind, source.dimension_value AS dimension_value,
    source.dimension_snapshot_id AS dimension_snapshot_id, source.geo_version AS geo_version,
    source.classification_version AS classification_version,
    source.raw_bytes AS raw_bytes, source.raw_packets AS raw_packets,
    source.estimated_bytes AS estimated_bytes, source.estimated_packets AS estimated_packets,
    source.received_records AS received_records, source.unknown_sampling_records AS unknown_sampling_records,
    source.quality_records AS quality_records,
    rollup_generation AS generation, rollup_generated_at AS generated_at
  FROM %s AS source FINAL
  INNER JOIN latest ON source.bucket = latest.bucket AND source.generation = latest.generation
  WHERE source.bucket >= rollup_start AND source.bucket < rollup_end
    AND source.dimension_kind NOT IN ('_generation', 'src_ip', 'dst_ip', 'remote_port')
)
GROUP BY
  bucket, target_id, device_id, exporter_id,
  business_direction, category, business, dimension_kind, dimension_value,
  dimension_snapshot_id, geo_version, classification_version,
  generation, generated_at`
