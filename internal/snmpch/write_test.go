package snmpch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type recordingExecutor struct {
	queries  []ch.Query
	failures int
	err      error
}

func (e *recordingExecutor) Do(_ context.Context, query ch.Query) error {
	e.queries = append(e.queries, query)
	if e.failures > 0 {
		e.failures--
		return e.err
	}
	return nil
}

func TestWriteSamplesPreservesExactCounterAndNaturalIdentity(t *testing.T) {
	exec := &recordingExecutor{failures: 2, err: errors.New("temporary")}
	store, err := New(exec)
	if err != nil {
		t.Fatal(err)
	}
	want := uint64(1<<53 + 17)
	err = store.WriteSamples(context.Background(), []Sample{{
		ObservedAt: time.Unix(100, 0), DeviceID: "device-a", EntityKind: "port", EntityID: "port-a",
		RecipeID: "recipe-a", Metric: MetricIfInOctets, ValueKind: "counter", CounterValue: want,
		CounterWidth: 64, IntervalMS: 60_000, PollSequence: 99, SourceRunID: "run-a", SampleIndex: 7,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(exec.queries) != 3 {
		t.Fatalf("attempts=%d, want 3", len(exec.queries))
	}
	query := exec.queries[len(exec.queries)-1]
	if !strings.Contains(query.Body, "snmp_samples") {
		t.Fatalf("unexpected insert: %s", query.Body)
	}
	counter := inputColumn(t, query.Input, "counter_value").(*proto.ColUInt64)
	if counter.Rows() != 1 || (*counter)[0] != want {
		t.Fatalf("counter=%v, want %d", *counter, want)
	}
	var token string
	for _, setting := range query.Settings {
		if setting.Key == "insert_deduplication_token" {
			token = setting.Value
		}
	}
	if token != "snmp:run-a:99:7:7" {
		t.Fatalf("dedup token=%q", token)
	}
}

func TestWriteSamplesRejectsMixedPollAndCounterWidth(t *testing.T) {
	store, _ := New(&recordingExecutor{})
	base := Sample{ObservedAt: time.Unix(100, 0), DeviceID: "device-a", EntityKind: "port", RecipeID: "r", Metric: MetricIfInOctets, ValueKind: "counter", CounterWidth: 64, PollSequence: 1, SourceRunID: "run", SampleIndex: 4}
	mixed := base
	mixed.SampleIndex = 6
	if err := store.WriteSamples(context.Background(), []Sample{base, mixed}); err == nil {
		t.Fatal("non-consecutive natural identity was accepted")
	}
	base.CounterWidth = 16
	if err := store.WriteSamples(context.Background(), []Sample{base}); err == nil {
		t.Fatal("invalid counter width was accepted")
	}
}

func inputColumn(t *testing.T, input proto.Input, name string) any {
	t.Helper()
	for _, column := range input {
		if column.Name == name {
			return column.Data
		}
	}
	t.Fatalf("input column %s not found", name)
	return nil
}
