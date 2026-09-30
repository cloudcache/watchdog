// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type rollupGenerationExecutor struct {
	query      ch.Query
	generation uint64
	err        error
	emit       bool
}

type storageCounterExecutor struct {
	queries []ch.Query
	values  []StorageCounters
}

type rollupMarkerExecutor struct {
	query   ch.Query
	markers []RollupMarker
}

func (executor *rollupMarkerExecutor) Do(ctx context.Context, query ch.Query) error {
	executor.query = query
	results, ok := query.Result.(proto.Results)
	if !ok || len(results) != 3 {
		return errors.New("unexpected marker result contract")
	}
	buckets, ok := results[0].Data.(*proto.ColDateTime)
	if !ok {
		return errors.New("unexpected marker bucket column")
	}
	generations, ok := results[1].Data.(*proto.ColUInt64)
	if !ok {
		return errors.New("unexpected marker generation column")
	}
	generatedAt, ok := results[2].Data.(*proto.ColDateTime64)
	if !ok {
		return errors.New("unexpected marker generated_at column")
	}
	for _, marker := range executor.markers {
		buckets.Append(marker.Bucket)
		*generations = append(*generations, marker.Generation)
		generatedAt.Append(marker.GeneratedAt)
	}
	return query.OnResult(ctx, proto.Block{Columns: 3, Rows: len(executor.markers)})
}

func (executor *storageCounterExecutor) Do(ctx context.Context, query ch.Query) error {
	index := len(executor.queries)
	executor.queries = append(executor.queries, query)
	if index >= len(executor.values) {
		return errors.New("unexpected storage counter query")
	}
	results, ok := query.Result.(proto.Results)
	if !ok || len(results) != 6 {
		return errors.New("unexpected storage counter result contract")
	}
	value := executor.values[index]
	columns := []uint64{value.RecordCount, value.RawBytes, value.RawPackets, value.EstimatedBytes, value.EstimatedPackets, value.EstimatedValidRecords}
	for resultIndex, result := range results {
		column, ok := result.Data.(*proto.ColUInt64)
		if !ok {
			return errors.New("unexpected storage counter result column")
		}
		*column = append(*column, columns[resultIndex])
	}
	return query.OnResult(ctx, proto.Block{Columns: 6, Rows: 1})
}

func (e *rollupGenerationExecutor) Do(ctx context.Context, query ch.Query) error {
	e.query = query
	if e.err != nil {
		return e.err
	}
	if !e.emit {
		return nil
	}
	results, ok := query.Result.(proto.Results)
	if !ok || len(results) != 1 {
		return errors.New("unexpected generation result contract")
	}
	column, ok := results[0].Data.(*proto.ColUInt64)
	if !ok {
		return errors.New("unexpected generation result column")
	}
	if err := query.OnResult(ctx, proto.Block{Columns: 1}); err != nil {
		return err
	}
	*column = append(*column, e.generation)
	return query.OnResult(ctx, proto.Block{Columns: 1, Rows: 1})
}

func TestBuildRollupQueryIsParameterizedAndDeterministic(t *testing.T) {
	request := RollupRequest{
		Resolution: RollupOneMinute,
		Bucket:     time.Date(2026, 9, 5, 1, 2, 0, 0, time.UTC), Generation: 17,
		GeneratedAt: time.Date(2026, 9, 5, 1, 5, 0, 0, time.UTC),
		MaxThreads:  4, Priority: 10, MaxMemoryBytes: 6 << 30,
	}
	first, err := buildRollupQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildRollupQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same repair request changed query or token")
	}
	for _, required := range []string{
		"INSERT INTO flow_aggregate_1m", "FROM flow_records FINAL", "ARRAY JOIN", "arrayDistinct",
		"AND disposition = 'count'", "{generation:UInt64}",
		"'geo.continent'", "'geo.region'", "'geo.country'", "'geo.province'", "'geo.city'", "'_unassigned'",
		"tuple('asn', if(remote_asn = 0, '_unassigned'", "tuple('business', if(empty(business), '_unassigned'",
		"tuple('local_prefix', if(empty(local_prefix_id), '_unassigned'", "tuple('remote_port', if(remote_port = 0, '_unassigned'",
		"dimension_kind IN ('src_ip', 'dst_ip', 'remote_port')", "rollup_port_top_n",
	} {
		if !strings.Contains(first.Body, required) {
			t.Fatalf("rollup query missing %q", required)
		}
	}
	for _, obsolete := range []string{"tuple('province'", "tuple('city'"} {
		if strings.Contains(first.Body, obsolete) {
			t.Fatalf("rollup query retained obsolete Geo dimension %q", obsolete)
		}
	}
	if strings.Contains(first.Body, "'_generation'") || strings.Contains(first.Body, "UNION ALL") {
		t.Fatal("data INSERT must not publish its completion marker")
	}
	if setting(first, "async_insert") != "0" || setting(first, "insert_deduplication_token") == "" {
		t.Fatal("rollup insert is not synchronous and replay-stable")
	}
	// A heavy bucket group must spill to disk (external group by) under a memory
	// ceiling rather than fail with retryable MEMORY_LIMIT_EXCEEDED.
	if setting(first, "max_bytes_before_external_group_by") == "" || setting(first, "max_memory_usage") == "" {
		t.Fatalf("rollup missing memory spill/ceiling guards: %+v", first.Settings)
	}
	if setting(first, "max_threads") != "4" || setting(first, "priority") != "10" ||
		setting(first, "max_memory_usage") != "6442450944" || setting(first, "max_bytes_before_external_group_by") != "3221225472" {
		t.Fatalf("rollup did not apply request-owned resource guards: %+v", first.Settings)
	}
	marker, err := buildRollupMarkerQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(marker.Body, "'_generation'") || strings.Contains(marker.Body, "flow_records") ||
		setting(marker, "insert_deduplication_token") == setting(first, "insert_deduplication_token") {
		t.Fatalf("marker query is not an independent commit record: %+v", marker)
	}
}

func TestBuildDailyRollupReadsOnlyCompleteHourlyGenerations(t *testing.T) {
	request := RollupRequest{
		Resolution: RollupOneDay,
		Bucket:     time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), Generation: 17,
		GeneratedAt: time.Date(2026, 9, 6, 1, 0, 0, 0, time.UTC),
	}
	query, err := buildRollupQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"INSERT INTO flow_aggregate_1d", "FROM flow_aggregate_1h AS source FINAL",
		"max(generation)", "dimension_kind = '_generation'", "INNER JOIN latest USING (bucket, generation)",
		"dimension_value != '_other'",
	} {
		if !strings.Contains(query.Body, required) {
			t.Fatalf("daily rollup query missing %q", required)
		}
	}
	if strings.Contains(query.Body, "FROM flow_records") {
		t.Fatal("daily rollup unexpectedly re-read raw facts")
	}
	if strings.Contains(query.Body, "UNION ALL") {
		t.Fatal("daily data INSERT published its marker before completion")
	}
}

func TestBuildHourlyRollupCanMergeCompleteMinuteGenerations(t *testing.T) {
	request := RollupRequest{
		Resolution: RollupOneHour, SourceResolution: RollupOneMinute,
		Bucket: time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC), Generation: 18,
		GeneratedAt: time.Date(2026, 9, 5, 2, 5, 0, 0, time.UTC),
	}
	query, err := buildRollupQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"INSERT INTO flow_aggregate_1h", "FROM flow_aggregate_1m AS source FINAL",
		"FROM flow_aggregate_1m", "INNER JOIN latest USING (bucket, generation)",
		"dimension_kind = '_generation'",
	} {
		if !strings.Contains(query.Body, required) {
			t.Fatalf("hourly derived rollup query missing %q", required)
		}
	}
	if strings.Contains(query.Body, "FROM flow_records") || strings.Contains(query.Body, "ARRAY JOIN") {
		t.Fatal("hourly derived rollup unexpectedly expanded raw facts")
	}
}

func TestBuildMinuteRollupBatchScansHourOnceAndPublishesEveryMarker(t *testing.T) {
	request := RollupRequest{
		Resolution: RollupOneMinute,
		Bucket:     time.Date(2026, 9, 22, 1, 0, 0, 0, time.UTC),
		BucketEnd:  time.Date(2026, 9, 22, 2, 0, 0, 0, time.UTC),
		Generation: 19, GeneratedAt: time.Date(2026, 9, 22, 2, 5, 0, 0, time.UTC),
	}
	query, err := buildRollupQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"toStartOfMinute(event_time) AS bucket", "PARTITION BY bucket,",
		"GROUP BY\n      bucket,",
	} {
		if !strings.Contains(query.Body, required) {
			t.Fatalf("minute batch query missing %q", required)
		}
	}
	if parameter(query, "bucket_start") != "'2026-09-22 01:00:00'" ||
		parameter(query, "bucket_end") != "'2026-09-22 02:00:00'" {
		t.Fatalf("minute batch parameters=%+v", query.Parameters)
	}
	marker, err := buildRollupMarkerQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(marker.Body, "FROM numbers(") || !strings.Contains(marker.Body, "rollup_bucket_seconds") ||
		parameter(marker, "bucket_seconds") != "'60'" {
		t.Fatalf("minute batch marker query=%+v", marker)
	}
}

func TestBuildMarkerOnlyRollupDoesNotReadRaw(t *testing.T) {
	request := RollupRequest{
		Resolution: RollupOneHour, Bucket: time.Date(2026, 9, 21, 6, 0, 0, 0, time.UTC),
		Generation: 20, GeneratedAt: time.Date(2026, 9, 22, 6, 0, 0, 0, time.UTC),
		MarkerOnly: true,
	}
	query, err := buildRollupQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query.Body, "INSERT INTO flow_aggregate_1h") ||
		!strings.Contains(query.Body, "'_generation'") || strings.Contains(query.Body, "flow_records") {
		t.Fatalf("marker-only query=%s", query.Body)
	}
}

func TestRollupRunnerPublishesMarkerOnlyAfterDataSucceeds(t *testing.T) {
	request := RollupRequest{
		Resolution: RollupOneMinute,
		Bucket:     time.Date(2026, 9, 22, 1, 0, 0, 0, time.UTC),
		BucketEnd:  time.Date(2026, 9, 22, 2, 0, 0, 0, time.UTC),
		Generation: 21, GeneratedAt: time.Date(2026, 9, 22, 2, 5, 0, 0, time.UTC),
	}

	dataFailure := &queryRecorder{errors: map[int]error{1: errors.New("data insert failed")}}
	if err := (&RollupRunner{executor: dataFailure}).Run(context.Background(), request); err == nil || len(dataFailure.queries) != 1 {
		t.Fatalf("data failure published marker: err=%v queries=%d", err, len(dataFailure.queries))
	}

	markerFailure := &queryRecorder{errors: map[int]error{2: errors.New("marker insert failed")}}
	runner := &RollupRunner{executor: markerFailure}
	if err := runner.Run(context.Background(), request); err == nil || !strings.Contains(err.Error(), "marker phase") {
		t.Fatalf("marker failure was not reported: %v", err)
	}
	if len(markerFailure.queries) != 2 || strings.Contains(markerFailure.queries[0].Body, "'_generation'") ||
		!strings.Contains(markerFailure.queries[1].Body, "'_generation'") {
		t.Fatalf("unexpected two-phase queries: %+v", markerFailure.queries)
	}
	stats := runner.Stats().OneMinute
	if stats.Attempts != 1 || stats.Successes != 0 || stats.RetryableErrors != 1 || stats.LatestCompletedBucketUnix != 0 {
		t.Fatalf("marker failure incorrectly completed rollup: %+v", stats)
	}

	success := &queryRecorder{}
	if err := (&RollupRunner{executor: success}).Run(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(success.queries) != 2 ||
		setting(success.queries[0], "insert_deduplication_token") == setting(success.queries[1], "insert_deduplication_token") {
		t.Fatalf("data and marker phases are not independently replay-safe: %+v", success.queries)
	}
}

func TestRollupRunnerChecksPhysicalRawBucketWithoutFinal(t *testing.T) {
	bucket := time.Date(2026, 9, 22, 1, 0, 0, 0, time.UTC)
	executor := &scalarSequenceExecutor{values: []uint64{1}}
	hasRecords, err := (&RollupRunner{executor: executor}).RawBucketHasRecords(context.Background(), RollupOneHour, bucket)
	if err != nil || !hasRecords || len(executor.queries) != 1 {
		t.Fatalf("hasRecords=%v err=%v queries=%d", hasRecords, err, len(executor.queries))
	}
	if body := executor.queries[0].Body; strings.Contains(body, "FINAL") || !strings.Contains(body, "LIMIT 1") ||
		!strings.Contains(body, "PREWHERE event_time") {
		t.Fatalf("raw existence query=%s", body)
	}
}

func TestRollupRunnerReadsRawWatermark(t *testing.T) {
	from := time.Date(2026, 9, 30, 1, 0, 0, 0, time.UTC)
	newest := time.Date(2026, 9, 30, 1, 42, 29, 0, time.UTC)
	executor := &scalarSequenceExecutor{values: []uint64{uint64(newest.Unix()), 0}}
	runner := &RollupRunner{executor: executor}
	watermark, err := runner.RawWatermark(context.Background(), from, from.Add(time.Hour))
	if err != nil || !watermark.Equal(newest) {
		t.Fatalf("watermark=%s err=%v", watermark, err)
	}
	if body := executor.queries[0].Body; !strings.Contains(body, "max(event_time)") || strings.Contains(body, "FINAL") ||
		!strings.Contains(body, "PREWHERE event_time >= {from:DateTime('UTC')} AND event_time < {to:DateTime('UTC')}") {
		t.Fatalf("watermark query=%s", body)
	}
	// An empty window yields max() = epoch, reported as no watermark.
	if watermark, err := runner.RawWatermark(context.Background(), from, from.Add(time.Hour)); err != nil || !watermark.IsZero() {
		t.Fatalf("empty window watermark=%s err=%v", watermark, err)
	}
	if _, err := runner.RawWatermark(context.Background(), from, from); err == nil {
		t.Fatal("empty watermark window accepted")
	}
}

func TestDayStorageCountersUsesRawAndLatestCompleteHourlyGeneration(t *testing.T) {
	raw := StorageCounters{RecordCount: 2, RawBytes: 30, RawPackets: 3, EstimatedBytes: 300, EstimatedPackets: 30, EstimatedValidRecords: 1}
	archive := raw
	executor := &storageCounterExecutor{values: []StorageCounters{raw, archive}}
	runner := &RollupRunner{executor: executor}
	day := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	gotRaw, gotArchive, err := runner.DayStorageCounters(context.Background(), day)
	if err != nil {
		t.Fatal(err)
	}
	if gotRaw != raw || gotArchive != archive || len(executor.queries) != 2 {
		t.Fatalf("raw=%+v archive=%+v queries=%d", gotRaw, gotArchive, len(executor.queries))
	}
	if body := executor.queries[0].Body; !strings.Contains(body, "FROM flow_records FINAL") || !strings.Contains(body, "countIf(estimated_valid)") {
		t.Fatalf("raw counter query=%s", body)
	}
	if body := executor.queries[1].Body; !strings.Contains(body, "dimension_kind = '_generation'") ||
		!strings.Contains(body, "max(generation)") || !strings.Contains(body, "dimension_kind = 'total'") {
		t.Fatalf("archive counter query=%s", body)
	}
}

func TestDayStorageCountersRejectsNonUTCDay(t *testing.T) {
	runner := &RollupRunner{executor: &storageCounterExecutor{}}
	for _, day := range []time.Time{
		time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 5, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)),
	} {
		if _, _, err := runner.DayStorageCounters(context.Background(), day); err == nil {
			t.Fatalf("invalid day accepted: %s", day)
		}
	}
}

func TestRawDayPhysicalRecordsCountsAllLogicalRows(t *testing.T) {
	day := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	executor := &scalarSequenceExecutor{values: []uint64{9}}
	runner := &RollupRunner{executor: executor}
	got, err := runner.RawDayPhysicalRecords(context.Background(), day)
	if err != nil {
		t.Fatal(err)
	}
	if got != 9 || executor.index != 1 {
		t.Fatalf("physical records=%d queries=%d", got, executor.index)
	}
	if body := executor.queries[0].Body; !strings.Contains(body, "FROM flow_records FINAL") || strings.Contains(body, "disposition") {
		t.Fatalf("physical count does not cover all logical records: %s", body)
	}
}

func TestArchiveMonthCountersAndPhysicalRowsUseCalendarBounds(t *testing.T) {
	month := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	want := StorageCounters{RecordCount: 3, RawBytes: 40, RawPackets: 4, EstimatedBytes: 400, EstimatedPackets: 40, EstimatedValidRecords: 2}
	counters := &storageCounterExecutor{values: []StorageCounters{want}}
	runner := &RollupRunner{executor: counters}
	got, err := runner.ArchiveMonthStorageCounters(context.Background(), month)
	if err != nil {
		t.Fatal(err)
	}
	if got != want || len(counters.queries) != 1 {
		t.Fatalf("counters=%+v queries=%d", got, len(counters.queries))
	}
	query := counters.queries[0]
	if !strings.Contains(query.Body, "FROM flow_aggregate_1h FINAL") || !strings.Contains(query.Body, "dimension_kind = '_generation'") ||
		parameter(query, "start") != "'2026-09-01 00:00:00'" || parameter(query, "end") != "'2026-10-01 00:00:00'" {
		t.Fatalf("month counter query=%+v", query)
	}

	physical := &scalarSequenceExecutor{values: []uint64{91}}
	runner = &RollupRunner{executor: physical}
	rows, err := runner.ArchiveMonthPhysicalRecords(context.Background(), month)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 91 || !strings.Contains(physical.queries[0].Body, "FROM flow_aggregate_1h FINAL") ||
		parameter(physical.queries[0], "end") != "'2026-10-01 00:00:00'" {
		t.Fatalf("physical=%d query=%+v", rows, physical.queries[0])
	}
}

func TestArchiveMonthReadersRejectUnalignedMonth(t *testing.T) {
	runner := &RollupRunner{executor: &storageCounterExecutor{}}
	for _, invalid := range []time.Time{
		time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)),
	} {
		if _, err := runner.ArchiveMonthStorageCounters(context.Background(), invalid); err == nil {
			t.Fatalf("invalid month accepted: %s", invalid)
		}
	}
}

func TestBuildRollupQueryRejectsUnalignedOrUnsafeRequests(t *testing.T) {
	valid := RollupRequest{
		Resolution: RollupOneHour,
		Bucket:     time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC), Generation: 1,
		GeneratedAt: time.Date(2026, 9, 5, 2, 0, 0, 0, time.UTC),
	}
	for _, mutate := range []func(*RollupRequest){
		func(value *RollupRequest) { value.Resolution = "5m" },
		func(value *RollupRequest) { value.Bucket = value.Bucket.Add(time.Minute) },
		func(value *RollupRequest) { value.Generation = 0 },
		func(value *RollupRequest) { value.GeneratedAt = value.GeneratedAt.In(time.FixedZone("local", 3600)) },
		func(value *RollupRequest) { value.GeneratedAt = value.Bucket.Add(30 * time.Minute) },
		func(value *RollupRequest) { value.MaxThreads = 257 },
		func(value *RollupRequest) { value.MaxMemoryBytes = 512 << 20 },
		func(value *RollupRequest) { value.SourceResolution = RollupOneHour },
		func(value *RollupRequest) {
			value.MarkerOnly = true
			value.SourceResolution = RollupOneMinute
		},
		func(value *RollupRequest) {
			value.Resolution = RollupOneMinute
			value.BucketEnd = value.Bucket.Add(time.Hour + time.Minute)
		},
		func(value *RollupRequest) {
			value.Resolution = RollupOneMinute
			value.Bucket = time.Date(2026, 9, 5, 1, 59, 0, 0, time.UTC)
			value.BucketEnd = value.Bucket.Add(2 * time.Minute)
		},
		func(value *RollupRequest) {
			value.Bucket = time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC)
			value.GeneratedAt = value.Bucket.Add(time.Hour)
		},
	} {
		request := valid
		mutate(&request)
		if _, err := buildRollupQuery(request); err == nil {
			t.Fatalf("invalid request was accepted: %+v", request)
		}
	}
}

func TestRollupRunnerStatsSeparateResolutionRepairAndFailureClass(t *testing.T) {
	now := time.Date(2026, 9, 5, 3, 0, 0, 0, time.UTC)
	runner := &RollupRunner{executor: &queryRecorder{}, now: func() time.Time { return now }}
	minute := RollupRequest{
		Resolution: RollupOneMinute,
		Bucket:     time.Date(2026, 9, 5, 2, 58, 0, 0, time.UTC), Generation: 1, GeneratedAt: now,
	}
	if err := runner.Run(context.Background(), minute); err != nil {
		t.Fatal(err)
	}
	minute.Generation = 2
	if err := runner.Run(context.Background(), minute); err != nil {
		t.Fatal(err)
	}
	hour := RollupRequest{
		Resolution: RollupOneHour,
		Bucket:     time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC), Generation: 1, GeneratedAt: now,
	}
	if err := runner.Run(context.Background(), hour); err != nil {
		t.Fatal(err)
	}
	stats := runner.Stats()
	if stats.OneMinute.Attempts != 2 || stats.OneMinute.Successes != 2 || stats.OneMinute.InitialRebuilds != 1 || stats.OneMinute.RepairRebuilds != 1 ||
		stats.OneMinute.LastSuccessUnix != uint64(now.Unix()) || stats.OneMinute.LatestCompletedBucketUnix != uint64(minute.Bucket.Add(time.Minute).Unix()) {
		t.Fatalf("one-minute stats=%+v", stats.OneMinute)
	}
	if stats.OneHour.Attempts != 1 || stats.OneHour.Successes != 1 || stats.OneHour.InitialRebuilds != 1 || stats.OneHour.RepairRebuilds != 0 ||
		stats.OneHour.LatestCompletedBucketUnix != uint64(hour.Bucket.Add(time.Hour).Unix()) {
		t.Fatalf("one-hour stats=%+v", stats.OneHour)
	}

	failures := &RollupRunner{executor: &queryRecorder{errors: map[int]error{
		1: &ch.Exception{Code: proto.ErrUnknownTable, Name: "UNKNOWN_TABLE"},
		2: errors.New("connection reset"),
	}}}
	_ = failures.Run(context.Background(), minute)
	_ = failures.Run(context.Background(), minute)
	failed := failures.Stats().OneMinute
	if failed.Attempts != 2 || failed.Successes != 0 || failed.PermanentErrors != 1 || failed.RetryableErrors != 1 || failed.LastSuccessUnix != 0 {
		t.Fatalf("failure stats=%+v", failed)
	}
}

func TestRecordTerminalFailureCountsByClass(t *testing.T) {
	runner := &RollupRunner{}
	runner.RecordTerminalFailure(false) // permanent classification
	runner.RecordTerminalFailure(true)  // retry budget exhausted
	runner.RecordTerminalFailure(true)
	stats := runner.Stats()
	if stats.TerminalPermanentFailures != 1 || stats.TerminalExhaustedFailures != 2 {
		t.Fatalf("terminal failures permanent=%d exhausted=%d, want 1/2", stats.TerminalPermanentFailures, stats.TerminalExhaustedFailures)
	}
	// A nil runner must be a no-op, not a panic: the wiring is best-effort.
	var nilRunner *RollupRunner
	nilRunner.RecordTerminalFailure(true)
	if got := nilRunner.Stats(); got.TerminalExhaustedFailures != 0 {
		t.Fatalf("nil runner stats=%+v", got)
	}
}

func TestRecordReaperRepairAndPermanentGapCount(t *testing.T) {
	runner := &RollupRunner{}
	runner.RecordReaperRepair(RollupRepairGap)
	runner.RecordReaperRepair(RollupRepairFailed)
	runner.RecordReaperRepair(RollupRepairLate)
	runner.RecordReaperRepair(RollupRepairLate)
	runner.RecordReaperRepair("unrecognized") // ignored, not miscounted
	runner.RecordPermanentGap()
	stats := runner.Stats()
	if stats.ReaperRepairsGap != 1 || stats.ReaperRepairsFailed != 1 || stats.ReaperRepairsLate != 2 || stats.PermanentGaps != 1 {
		t.Fatalf("reaper stats gap=%d failed=%d late=%d gaps=%d", stats.ReaperRepairsGap, stats.ReaperRepairsFailed, stats.ReaperRepairsLate, stats.PermanentGaps)
	}
	var nilRunner *RollupRunner
	nilRunner.RecordReaperRepair(RollupRepairGap)
	nilRunner.RecordPermanentGap()
}

// scalarSequenceExecutor returns one UInt64 per Do call, in order, so a test can
// drive the two queries BucketNeedsRepair issues (stored total, then live count).
type scalarSequenceExecutor struct {
	values  []uint64
	index   int
	queries []ch.Query
}

func (e *scalarSequenceExecutor) Do(ctx context.Context, query ch.Query) error {
	current := e.index
	e.index++
	e.queries = append(e.queries, query)
	results, ok := query.Result.(proto.Results)
	if !ok || len(results) != 1 {
		return errors.New("unexpected scalar result contract")
	}
	column, ok := results[0].Data.(*proto.ColUInt64)
	if !ok {
		return errors.New("unexpected scalar result column")
	}
	if err := query.OnResult(ctx, proto.Block{Columns: 1}); err != nil {
		return err
	}
	value := uint64(0)
	if current < len(e.values) {
		value = e.values[current]
	}
	*column = append(*column, value)
	return query.OnResult(ctx, proto.Block{Columns: 1, Rows: 1})
}

func TestBucketNeedsRepairComparesStoredAndLiveCounts(t *testing.T) {
	bucket := time.Date(2026, 9, 7, 0, 5, 0, 0, time.UTC)
	// stored == live: the aggregate still reflects the base data, no repair.
	inSync := &RollupRunner{executor: &scalarSequenceExecutor{values: []uint64{3, 3}}}
	if needs, err := inSync.BucketNeedsRepair(context.Background(), RollupOneMinute, bucket); err != nil || needs {
		t.Fatalf("in sync: needs=%v err=%v", needs, err)
	}
	// live > stored: base records arrived after the roll, repair needed.
	late := &RollupRunner{executor: &scalarSequenceExecutor{values: []uint64{3, 4}}}
	if needs, err := late.BucketNeedsRepair(context.Background(), RollupOneMinute, bucket); err != nil || !needs {
		t.Fatalf("late arrival: needs=%v err=%v", needs, err)
	}
	// live < stored: raw was deleted after the roll; rebuilding would empty it.
	deleted := &RollupRunner{executor: &scalarSequenceExecutor{values: []uint64{3, 0}}}
	if needs, err := deleted.BucketNeedsRepair(context.Background(), RollupOneHour, bucket.Truncate(time.Hour)); err != nil || needs {
		t.Fatalf("deleted raw: needs=%v err=%v", needs, err)
	}
	// An unaligned bucket is a permanent (programmer) error, not a silent false.
	if _, err := inSync.BucketNeedsRepair(context.Background(), RollupOneMinute, bucket.Add(30*time.Second)); err == nil {
		t.Fatal("unaligned bucket accepted")
	}
}

func TestDailyBucketRepairTracksHourlyGenerationFreshness(t *testing.T) {
	bucket := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	inSyncExecutor := &scalarSequenceExecutor{values: []uint64{3, 3, 0}}
	needs, err := (&RollupRunner{executor: inSyncExecutor}).BucketNeedsRepair(context.Background(), RollupOneDay, bucket)
	if err != nil || needs || len(inSyncExecutor.queries) != 3 || !strings.Contains(inSyncExecutor.queries[1].Body, "FROM flow_aggregate_1h FINAL") {
		t.Fatalf("daily in-sync check: needs=%v err=%v queries=%+v", needs, err, inSyncExecutor.queries)
	}
	newerHour := &RollupRunner{executor: &scalarSequenceExecutor{values: []uint64{3, 3, 1}}}
	if needs, err := newerHour.BucketNeedsRepair(context.Background(), RollupOneDay, bucket); err != nil || !needs {
		t.Fatalf("newer hourly generation: needs=%v err=%v", needs, err)
	}
}

func TestRollupRunnerClassifiesPermanentAndRetryableFailures(t *testing.T) {
	request := RollupRequest{
		Resolution: RollupOneMinute,
		Bucket:     time.Date(2026, 9, 5, 1, 2, 0, 0, time.UTC), Generation: 1,
		GeneratedAt: time.Date(2026, 9, 5, 1, 5, 0, 0, time.UTC),
	}
	for _, test := range []struct {
		err       error
		permanent bool
	}{
		{err: &ch.Exception{Code: 60, Name: "UNKNOWN_TABLE", Message: "missing"}, permanent: true},
		{err: errors.New("connection reset"), permanent: false},
	} {
		recorder := &queryRecorder{errors: map[int]error{1: test.err}}
		err := (&RollupRunner{executor: recorder}).Run(context.Background(), request)
		var permanent *PermanentError
		if errors.As(err, &permanent) != test.permanent || len(recorder.queries) != 1 {
			t.Fatalf("error=%v permanent=%v queries=%d", err, errors.As(err, &permanent), len(recorder.queries))
		}
	}
}

func TestRollupRunnerReadsAuthoritativeGenerationMarker(t *testing.T) {
	bucket := time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC)
	executor := &rollupGenerationExecutor{generation: 19, emit: true}
	generation, err := (&RollupRunner{executor: executor}).LatestGeneration(context.Background(), RollupOneHour, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if generation != 19 || !strings.Contains(executor.query.Body, "FROM flow_aggregate_1h") ||
		strings.Contains(executor.query.Body, "FINAL") || !strings.Contains(executor.query.Body, "dimension_kind = '_generation'") {
		t.Fatalf("generation=%d query=%q", generation, executor.query.Body)
	}
	if _, err := (&RollupRunner{executor: &rollupGenerationExecutor{}}).LatestGeneration(context.Background(), RollupOneHour, bucket); err == nil {
		t.Fatal("missing ClickHouse result was accepted")
	}
	permanent := &rollupGenerationExecutor{err: &ch.Exception{Code: proto.ErrUnknownTable, Name: "UNKNOWN_TABLE"}}
	_, err = (&RollupRunner{executor: permanent}).LatestGeneration(context.Background(), RollupOneHour, bucket)
	var permanentError *PermanentError
	if !errors.As(err, &permanentError) {
		t.Fatalf("schema error was not permanent: %v", err)
	}
}

func TestRollupRunnerFindsContinuousGenerationMarkerCoverage(t *testing.T) {
	from := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	executor := &rollupMarkerExecutor{markers: []RollupMarker{
		{Bucket: from, Generation: 10, GeneratedAt: from.Add(2 * time.Hour)},
		{Bucket: from.Add(time.Hour), Generation: 11, GeneratedAt: from.Add(2 * time.Hour)},
		{Bucket: from.Add(3 * time.Hour), Generation: 12, GeneratedAt: from.Add(4 * time.Hour)},
	}}
	runner := &RollupRunner{executor: executor}
	through, err := runner.CoveredThrough(context.Background(), RollupOneHour, from, from.Add(4*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if !through.Equal(from.Add(2 * time.Hour)) {
		t.Fatalf("covered through=%s", through)
	}
	if !strings.Contains(executor.query.Body, "argMax(generated_at, generation)") ||
		!strings.Contains(executor.query.Body, "max(generation) AS latest_generation") ||
		strings.Contains(executor.query.Body, "max(generation) AS generation,") ||
		strings.Contains(executor.query.Body, "FINAL") {
		t.Fatalf("marker query=%s", executor.query.Body)
	}

	executor.markers = append(executor.markers, RollupMarker{
		Bucket: from.Add(2 * time.Hour), Generation: 13, GeneratedAt: from.Add(4 * time.Hour),
	})
	sort.Slice(executor.markers, func(i, j int) bool { return executor.markers[i].Bucket.Before(executor.markers[j].Bucket) })
	through, err = runner.CoveredThrough(context.Background(), RollupOneHour, from, from.Add(4*time.Hour))
	if err != nil || !through.Equal(from.Add(4*time.Hour)) {
		t.Fatalf("complete coverage=%s err=%v", through, err)
	}
}

func TestRollupRunnerCoverageRejectsOldHotGenerationsButAcceptsLifecycle(t *testing.T) {
	from := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	executor := &rollupMarkerExecutor{markers: []RollupMarker{
		{Bucket: from, Generation: 20, GeneratedAt: from.Add(time.Hour)},
		{Bucket: from.Add(time.Hour), Generation: 14, GeneratedAt: from.Add(2 * time.Hour)},
		{Bucket: from.Add(2 * time.Hour), Generation: LifecycleGenerationFloor, GeneratedAt: from.Add(3 * time.Hour)},
	}}
	runner := &RollupRunner{executor: executor}
	through, err := runner.CoveredThroughAtLeast(context.Background(), RollupOneHour, from, from.Add(3*time.Hour), 15)
	if err != nil {
		t.Fatal(err)
	}
	if !through.Equal(from.Add(time.Hour)) {
		t.Fatalf("old hot generation authorized coverage through %s", through)
	}

	executor.markers[1].Generation = LifecycleGenerationFloor
	through, err = runner.CoveredThroughAtLeast(context.Background(), RollupOneHour, from, from.Add(3*time.Hour), 15)
	if err != nil || !through.Equal(from.Add(3*time.Hour)) {
		t.Fatalf("lifecycle generation coverage=%s err=%v", through, err)
	}
}

func parameter(query ch.Query, key string) string {
	for _, parameter := range query.Parameters {
		if parameter.Key == key {
			return parameter.Value
		}
	}
	return ""
}

func TestBuildFiveMinuteRollupDerivesFromCompleteMinuteAndExcludesEndpoints(t *testing.T) {
	request := RollupRequest{
		Resolution: RollupFiveMinute, SourceResolution: RollupOneMinute,
		Bucket:     time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC),
		BucketEnd:  time.Date(2026, 9, 24, 2, 0, 0, 0, time.UTC),
		Generation: 21, GeneratedAt: time.Date(2026, 9, 24, 2, 5, 0, 0, time.UTC),
	}
	query, err := buildRollupQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"INSERT INTO flow_aggregate_5m", "FROM flow_aggregate_1m AS source FINAL",
		"INNER JOIN latest ON source.bucket = latest.bucket",
		"toStartOfFiveMinutes(source.bucket) AS bucket",
		"source.dimension_kind NOT IN ('_generation', 'src_ip', 'dst_ip', 'remote_port')",
	} {
		if !strings.Contains(query.Body, required) {
			t.Fatalf("five-minute derived rollup query missing %q", required)
		}
	}
	if strings.Contains(query.Body, "FROM flow_records") || strings.Contains(query.Body, "ARRAY JOIN") {
		t.Fatal("five-minute derived rollup unexpectedly expanded raw facts")
	}
	marker, err := buildRollupMarkerQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	if parameter(marker, "bucket_seconds") != "'300'" {
		t.Fatalf("five-minute marker bucket_seconds=%s", parameter(marker, "bucket_seconds"))
	}
}

func TestValidateFiveMinuteRollupBatchBounds(t *testing.T) {
	base := RollupRequest{
		Resolution: RollupFiveMinute, SourceResolution: RollupOneMinute,
		Bucket:     time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC),
		Generation: 21, GeneratedAt: time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC),
	}
	if err := ValidateRollupRequest(base); err != nil {
		t.Fatalf("single 5m bucket rejected: %v", err)
	}
	full := base
	full.BucketEnd = time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	if err := ValidateRollupRequest(full); err != nil {
		t.Fatalf("full-day 5m batch rejected: %v", err)
	}
	unaligned := base
	unaligned.BucketEnd = time.Date(2026, 9, 24, 0, 3, 0, 0, time.UTC)
	if err := ValidateRollupRequest(unaligned); err == nil {
		t.Fatal("unaligned 5m batch end accepted")
	}
	tooLong := base
	tooLong.BucketEnd = time.Date(2026, 9, 25, 0, 5, 0, 0, time.UTC)
	if err := ValidateRollupRequest(tooLong); err == nil {
		t.Fatal("over-one-day 5m batch accepted")
	}
}
