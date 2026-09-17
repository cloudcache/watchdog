package snmpch

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRebuildClosedInterfaceBucketPublishesValuesBeforeMarker(t *testing.T) {
	exec := &recordingExecutor{}
	store, _ := New(exec)
	bucket := time.Date(2026, 1, 2, 3, 5, 0, 0, time.UTC)
	if err := store.RebuildClosedInterfaceBucket(context.Background(), bucket, bucket.Add(6*time.Minute), 42); err != nil {
		t.Fatal(err)
	}
	if len(exec.queries) != 2 {
		t.Fatalf("queries=%d, want values+marker", len(exec.queries))
	}
	for _, required := range []string{"lagInFrame(counter_width)", "counter_width=previous_width", "counter_width=32", "previous_value>=3865470566", "interval_ms", "reset_flag", "gap_flag"} {
		if !strings.Contains(exec.queries[0].Body, required) {
			t.Fatalf("rollup missing %q: %s", required, exec.queries[0].Body)
		}
	}
	if !strings.Contains(exec.queries[0].Body, "observed_at>fromUnixTimestamp64Milli({bucket_ms:Int64})") ||
		!strings.Contains(exec.queries[0].Body, "observed_at<=fromUnixTimestamp64Milli({end_ms:Int64})") ||
		strings.Contains(exec.queries[0].Body, "previous_at>=fromUnixTimestamp64Milli({bucket_ms:Int64})") {
		t.Fatalf("rollup does not assign (previous_at,observed_at] deltas to the matching (bucket,end] interval: %s", exec.queries[0].Body)
	}
	if !strings.Contains(exec.queries[1].Body, "'generation'") {
		t.Fatalf("last write is not the publication marker: %s", exec.queries[1].Body)
	}
}

func TestRebuildClosedInterfaceBucketRejectsOpenOrUnalignedBucket(t *testing.T) {
	store, _ := New(&recordingExecutor{})
	now := time.Date(2026, 1, 2, 3, 10, 30, 0, time.UTC)
	if err := store.RebuildClosedInterfaceBucket(context.Background(), now.Truncate(5*time.Minute), now, 1); err == nil {
		t.Fatal("open bucket was accepted")
	}
	if err := store.RebuildClosedInterfaceBucket(context.Background(), now.Add(-6*time.Minute), now, 1); err == nil {
		t.Fatal("unaligned bucket was accepted")
	}
}
