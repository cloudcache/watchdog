// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type RollupResolution string

const (
	RollupOneMinute RollupResolution = "1m"
	RollupOneHour   RollupResolution = "1h"
)

type RollupRequest struct {
	Resolution  RollupResolution
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
	stats    [2]rollupCounters
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
	OneMinute RollupResolutionStats
	OneHour   RollupResolutionStats
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
FROM %s FINAL
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

// BucketNeedsRepair reports whether a bucket's latest rolled generation no
// longer reflects its base data: the number of base records now in flow_records
// for the bucket differs from the received_records its aggregate recorded. Base
// records that arrived after the rollup ran are the common cause (F2). The
// reaper calls it only for buckets that already have a successful generation, so
// the aggregate side is populated. Both sides read FINAL and filter
// disposition/dimension exactly as the rollup did, so the counts are comparable.
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
    SELECT max(generation) FROM %s FINAL
    WHERE bucket = {bucket:DateTime('UTC')} AND dimension_kind = '_generation'
  )`, table, table),
		Parameters: ch.Parameters(map[string]any{"bucket": bucketParam}),
	})
	if err != nil {
		return false, fmt.Errorf("read %s aggregate received_records: %w", resolution, err)
	}
	live, err := r.scalarUInt64(ctx, ch.Query{
		Body: `SELECT count() AS value
FROM flow_records FINAL
WHERE event_time >= {start:DateTime('UTC')}
  AND event_time < {end:DateTime('UTC')}
  AND disposition = 'count'`,
		Parameters: ch.Parameters(map[string]any{
			"start": bucketParam, "end": end.Format("2006-01-02 15:04:05"),
		}),
	})
	if err != nil {
		return false, fmt.Errorf("read %s base record count: %w", resolution, err)
	}
	return live != stored, nil
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
  FROM flow_aggregate_1h FINAL
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
  FROM flow_aggregate_1h FINAL
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

// Run rebuilds one closed bucket in one INSERT SELECT. Every public
// dimension and an internal generation marker are inserted atomically. A
// repair reuses the same request, or supplies a greater generation after late
// base records arrive.
func (r *RollupRunner) Run(ctx context.Context, request RollupRequest) error {
	query, err := buildRollupQuery(request)
	if err != nil {
		return Permanent(err)
	}
	if r == nil || r.executor == nil {
		return Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	index, duration := rollupStatsTarget(request.Resolution)
	r.stats[index].attempts.Add(1)
	if err := r.executor.Do(ctx, query); err != nil {
		classified := classifyClickHouseError(fmt.Errorf("rebuild ClickHouse %s bucket: %w", request.Resolution, err))
		var permanent *PermanentError
		if errors.As(classified, &permanent) {
			r.stats[index].permanentErrors.Add(1)
		} else {
			r.stats[index].retryableErrors.Add(1)
		}
		return classified
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
	storeMax(&r.stats[index].latestCompletedBucketUnix, uint64(request.Bucket.UTC().Add(duration).Unix()))
	return nil
}

func (r *RollupRunner) Stats() RollupStats {
	if r == nil {
		return RollupStats{}
	}
	return RollupStats{
		OneMinute:                 snapshotRollupCounters(&r.stats[0]),
		OneHour:                   snapshotRollupCounters(&r.stats[1]),
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
	if resolution == RollupOneHour {
		return 1, time.Hour
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
	if request.Generation == 0 || request.GeneratedAt.IsZero() {
		return errors.New("rollup generation and generated_at are required")
	}
	bucket := request.Bucket.UTC()
	end := bucket.Add(duration)
	_, bucketOffset := request.Bucket.Zone()
	_, generatedOffset := request.GeneratedAt.Zone()
	if bucket.IsZero() || bucketOffset != 0 || generatedOffset != 0 || bucket.Truncate(duration) != bucket {
		return fmt.Errorf("%s rollup bucket and generated_at must be UTC and bucket-aligned", request.Resolution)
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

func buildRollupQuery(request RollupRequest) (ch.Query, error) {
	duration, table, err := rollupTarget(request.Resolution)
	if err != nil {
		return ch.Query{}, err
	}
	if err := ValidateRollupRequest(request); err != nil {
		return ch.Query{}, err
	}
	bucket := request.Bucket.UTC()
	generatedAt := request.GeneratedAt.UTC()
	end := bucket.Add(duration)
	tokenInput := fmt.Sprintf("watchdog-flow-rollup-v1\x00%s\x00%d\x00%d", request.Resolution, bucket.Unix(), request.Generation)
	token := sha256.Sum256([]byte(tokenInput))
	query := ch.Query{
		Body: fmt.Sprintf(rollupSQL, table),
		Parameters: ch.Parameters(map[string]any{
			"bucket_start": bucket.Format("2006-01-02 15:04:05"),
			"bucket_end":   end.Format("2006-01-02 15:04:05"), "generation": request.Generation,
			"generated_at": generatedAt.Format("2006-01-02 15:04:05.000"), "top_n": rollupIPTopN,
		}),
		Settings: []ch.Setting{
			{Key: "async_insert", Value: "0", Important: true},
			{Key: "wait_for_async_insert", Value: "1", Important: true},
			{Key: "insert_deduplication_token", Value: hex.EncodeToString(token[:]), Important: true},
			// A heavy bucket GROUP BY must spill to disk rather than OOM:
			// MEMORY_LIMIT_EXCEEDED is classified retryable, so an unguarded
			// rollup fails deterministically and the bucket is never aggregated.
			{Key: "max_bytes_before_external_group_by", Value: "4294967296", Important: true},
			{Key: "max_memory_usage", Value: "10737418240", Important: true},
		},
	}
	return query, nil
}

func rollupTarget(resolution RollupResolution) (time.Duration, string, error) {
	switch resolution {
	case RollupOneMinute:
		return time.Minute, "flow_aggregate_1m", nil
	case RollupOneHour:
		return time.Hour, "flow_aggregate_1h", nil
	default:
		return 0, "", fmt.Errorf("unsupported rollup resolution %q", resolution)
	}
}

// The marker row makes an empty repair visible as the latest complete
// generation, so dimensions that disappeared do not leak from an older run.
// Public queries must filter dimension_kind and select max(generation) per
// bucket; _generation is internal and never exposed by the registry.
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
  {top_n:UInt32} AS rollup_top_n
SELECT
  bucket, target_id, device_id, exporter_id,
  business_direction, category, business, dimension_kind,
  -- Fold the per-IP long tail (beyond the top-N by traffic) into one _other
  -- bucket so src_ip/dst_ip do not materialize at ~raw cardinality into the
  -- 180/400-day aggregate tables. Non-IP kinds have ip_rank = 0 and pass through.
  if(dimension_kind IN ('src_ip', 'dst_ip') AND ip_rank > rollup_top_n, '_other', dimension_value) AS dimension_value,
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
    if(dimension_kind IN ('src_ip', 'dst_ip'),
       row_number() OVER (
         PARTITION BY target_id, device_id, exporter_id,
           business_direction, category, business, dimension_kind,
           dimension_snapshot_id, geo_version, classification_version
         ORDER BY estimated_bytes DESC, dimension_value ASC),
       0) AS ip_rank
  FROM (
    SELECT
      rollup_start AS bucket,
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
      target_id, device_id, exporter_id, business_direction, category,
      business, dimension_kind, dimension_value, dimension_snapshot_id,
      geo_version, classification_version
  )
)
GROUP BY
  bucket, target_id, device_id, exporter_id, business_direction, category,
  business, dimension_kind, dimension_value, dimension_snapshot_id,
  geo_version, classification_version, generation, generated_at
UNION ALL
SELECT
  rollup_start, '', '', '', 'ambiguous', 'unknown', '',
  '_generation', '', '', '', 0,
  0, 0, 0, 0, 0, 0, 0, rollup_generation, rollup_generated_at`
