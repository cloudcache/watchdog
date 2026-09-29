package server

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/snmpch"
)

// snmpRollupExecutor answers the latest-published query and records the bucket
// of every interface rollup marker the collector publishes.
type snmpRollupExecutor struct {
	latest  time.Time
	rebuilt []time.Time
}

func (e *snmpRollupExecutor) Do(_ context.Context, query ch.Query) error {
	switch {
	case strings.Contains(query.Body, "max(bucket_start)"):
		var latest int64
		if !e.latest.IsZero() {
			latest = e.latest.Unix()
		}
		query.Result.(proto.Results)[0].Data.(*proto.ColInt64).Append(latest)
	case strings.Contains(query.Body, "'generation','',''"):
		for _, parameter := range query.Parameters {
			if parameter.Key == "bucket_ms" {
				ms, err := strconv.ParseInt(strings.Trim(parameter.Value, "'"), 10, 64)
				if err != nil {
					return err
				}
				e.rebuilt = append(e.rebuilt, time.UnixMilli(ms).UTC())
			}
		}
	}
	return nil
}

func TestSNMPCollectorRebuildsEveryClosedInterfaceBucket(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 10, 32, 0, 0, time.UTC)
	closed := time.Date(2026, 9, 29, 10, 25, 0, 0, time.UTC)
	newRuntime := func(latest time.Time) (*SNMPCollectorRuntime, *snmpRollupExecutor) {
		exec := &snmpRollupExecutor{latest: latest}
		store, err := snmpch.New(exec)
		if err != nil {
			t.Fatal(err)
		}
		return &SNMPCollectorRuntime{store: store}, exec
	}
	buckets := func(from time.Time, count int) []time.Time {
		out := make([]time.Time, count)
		for i := range out {
			out[i] = from.Add(time.Duration(i) * 5 * time.Minute)
		}
		return out
	}
	expect := func(label string, got, want []time.Time) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: rebuilt %v, want %v", label, got, want)
		}
		for i := range want {
			if !got[i].Equal(want[i]) {
				t.Fatalf("%s: rebuilt %v, want %v", label, got, want)
			}
		}
	}

	// Nothing published yet: only the newest closed bucket, as before.
	runtime, exec := newRuntime(time.Time{})
	if err := runtime.rebuildClosedInterfaceBuckets(ctx, now); err != nil {
		t.Fatal(err)
	}
	expect("fresh install", exec.rebuilt, []time.Time{closed})

	// A restart resumes after the newest published bucket, so the buckets the
	// stopped collector left unpublished are rebuilt from their samples.
	runtime, exec = newRuntime(closed.Add(-20 * time.Minute))
	if err := runtime.rebuildClosedInterfaceBuckets(ctx, now); err != nil {
		t.Fatal(err)
	}
	expect("restart", exec.rebuilt, buckets(closed.Add(-15*time.Minute), 4))

	// A pass that ran past several bucket boundaries rebuilds each of them, not
	// only the newest.
	exec.rebuilt = nil
	if err := runtime.rebuildClosedInterfaceBuckets(ctx, now.Add(15*time.Minute)); err != nil {
		t.Fatal(err)
	}
	expect("long pass", exec.rebuilt, buckets(closed.Add(5*time.Minute), 3))

	// A long outage is rebuilt in bounded slices, one per poll pass.
	runtime, exec = newRuntime(closed.Add(-2 * time.Hour))
	if err := runtime.rebuildClosedInterfaceBuckets(ctx, now); err != nil {
		t.Fatal(err)
	}
	expect("outage first pass", exec.rebuilt, buckets(closed.Add(-115*time.Minute), snmpInterfaceBucketsPerPass))
	exec.rebuilt = nil
	if err := runtime.rebuildClosedInterfaceBuckets(ctx, now); err != nil {
		t.Fatal(err)
	}
	expect("outage second pass", exec.rebuilt, buckets(closed.Add(-55*time.Minute), snmpInterfaceBucketsPerPass))
}
