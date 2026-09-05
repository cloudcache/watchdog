// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"reflect"
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
	*column = append(*column, e.generation)
	return query.OnResult(ctx, proto.Block{})
}

func TestBuildRollupQueryIsAtomicParameterizedAndDeterministic(t *testing.T) {
	request := RollupRequest{
		TenantID: "tenant-a", Resolution: RollupOneMinute,
		Bucket: time.Date(2026, 9, 5, 1, 2, 0, 0, time.UTC), Generation: 17,
		GeneratedAt: time.Date(2026, 9, 5, 1, 5, 0, 0, time.UTC),
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
		"AND disposition = 'count'", "UNION ALL", "'_generation'", "{tenant:String}", "{generation:UInt64}",
		"'geo.continent'", "'geo.region'", "'geo.country'", "'geo.province'", "'geo.city'", "'_unassigned'",
		"tuple('asn', if(remote_asn = 0, '_unassigned'", "tuple('business', if(empty(business), '_unassigned'",
		"tuple('local_prefix', if(empty(local_prefix_id), '_unassigned'", "tuple('remote_port', if(remote_port = 0, '_unassigned'",
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
	if strings.Count(first.Body, "INSERT INTO") != 1 {
		t.Fatal("one bucket generation must be one atomic INSERT SELECT")
	}
	if setting(first, "async_insert") != "0" || setting(first, "insert_deduplication_token") == "" {
		t.Fatal("rollup insert is not synchronous and replay-stable")
	}
	if got := parameter(first, "tenant"); got != "'tenant-a'" {
		t.Fatalf("tenant parameter=%q", got)
	}
}

func TestBuildRollupQueryRejectsUnalignedOrUnsafeRequests(t *testing.T) {
	valid := RollupRequest{
		TenantID: "tenant-a", Resolution: RollupOneHour,
		Bucket: time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC), Generation: 1,
		GeneratedAt: time.Date(2026, 9, 5, 2, 0, 0, 0, time.UTC),
	}
	for _, mutate := range []func(*RollupRequest){
		func(value *RollupRequest) { value.TenantID = "tenant' OR 1=1" },
		func(value *RollupRequest) { value.Resolution = "5m" },
		func(value *RollupRequest) { value.Bucket = value.Bucket.Add(time.Minute) },
		func(value *RollupRequest) { value.Generation = 0 },
		func(value *RollupRequest) { value.GeneratedAt = value.GeneratedAt.In(time.FixedZone("local", 3600)) },
		func(value *RollupRequest) { value.GeneratedAt = value.Bucket.Add(30 * time.Minute) },
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
		TenantID: "tenant-a", Resolution: RollupOneMinute,
		Bucket: time.Date(2026, 9, 5, 2, 58, 0, 0, time.UTC), Generation: 1, GeneratedAt: now,
	}
	if err := runner.Run(context.Background(), minute); err != nil {
		t.Fatal(err)
	}
	minute.Generation = 2
	if err := runner.Run(context.Background(), minute); err != nil {
		t.Fatal(err)
	}
	hour := RollupRequest{
		TenantID: "tenant-a", Resolution: RollupOneHour,
		Bucket: time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC), Generation: 1, GeneratedAt: now,
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

func TestRollupRunnerClassifiesPermanentAndRetryableFailures(t *testing.T) {
	request := RollupRequest{
		TenantID: "tenant-a", Resolution: RollupOneMinute,
		Bucket: time.Date(2026, 9, 5, 1, 2, 0, 0, time.UTC), Generation: 1,
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
	generation, err := (&RollupRunner{executor: executor}).LatestGeneration(context.Background(), "tenant-a", RollupOneHour, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if generation != 19 || !strings.Contains(executor.query.Body, "FROM flow_aggregate_1h FINAL") ||
		!strings.Contains(executor.query.Body, "dimension_kind = '_generation'") || parameter(executor.query, "tenant") != "'tenant-a'" {
		t.Fatalf("generation=%d query=%q", generation, executor.query.Body)
	}
	if _, err := (&RollupRunner{executor: &rollupGenerationExecutor{}}).LatestGeneration(context.Background(), "tenant-a", RollupOneHour, bucket); err == nil {
		t.Fatal("missing ClickHouse result was accepted")
	}
	permanent := &rollupGenerationExecutor{err: &ch.Exception{Code: proto.ErrUnknownTable, Name: "UNKNOWN_TABLE"}}
	_, err = (&RollupRunner{executor: permanent}).LatestGeneration(context.Background(), "tenant-a", RollupOneHour, bucket)
	var permanentError *PermanentError
	if !errors.As(err, &permanentError) {
		t.Fatalf("schema error was not permanent: %v", err)
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
