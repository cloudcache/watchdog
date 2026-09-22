package flowch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
)

type rawDeleteExecutor struct {
	query   ch.Query
	queries []ch.Query
	err     error
}

func (executor *rawDeleteExecutor) Do(_ context.Context, query ch.Query) error {
	executor.query = query
	executor.queries = append(executor.queries, query)
	return executor.err
}

func TestDropRawDayUsesExactPartitionAndStableQueryID(t *testing.T) {
	executor := &rawDeleteExecutor{}
	runner := &RollupRunner{executor: executor}
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if err := runner.DropRawDay(context.Background(), day, "flow-raw-delete-job-a"); err != nil {
		t.Fatal(err)
	}
	if executor.query.Body != "ALTER TABLE flow_records DROP PARTITION 20260901" || executor.query.QueryID != "flow-raw-delete-job-a" {
		t.Fatalf("query = %+v", executor.query)
	}
	if len(executor.query.Settings) != 1 || executor.query.Settings[0].Key != "alter_sync" || executor.query.Settings[0].Value != "2" {
		t.Fatalf("settings = %+v", executor.query.Settings)
	}
}

func TestDropRawDayFailsClosed(t *testing.T) {
	runner := &RollupRunner{executor: &rawDeleteExecutor{err: errors.New("temporary transport failure")}}
	day := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if err := runner.DropRawDay(context.Background(), day.Add(time.Hour), "job"); err == nil {
		t.Fatal("unaligned day was accepted")
	}
	err := runner.DropRawDay(context.Background(), day, "job")
	if err == nil || !strings.Contains(err.Error(), "drop raw Flow partition") {
		t.Fatalf("error = %v", err)
	}
}

func TestDropArchiveMonthUsesExactPartitionAndStableQueryID(t *testing.T) {
	executor := &rawDeleteExecutor{}
	runner := &RollupRunner{executor: executor}
	month := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if err := runner.DropArchiveMonth(context.Background(), month, "flow-archive-delete-job-a"); err != nil {
		t.Fatal(err)
	}
	if len(executor.queries) != 2 || executor.queries[0].Body != "ALTER TABLE flow_aggregate_1d DROP PARTITION 202609" ||
		executor.queries[0].QueryID != "flow-archive-delete-job-a-1d" ||
		executor.queries[1].Body != "ALTER TABLE flow_aggregate_1h DROP PARTITION 202609" || executor.queries[1].QueryID != "flow-archive-delete-job-a" {
		t.Fatalf("queries = %+v", executor.queries)
	}
	if len(executor.query.Settings) != 1 || executor.query.Settings[0].Key != "alter_sync" || executor.query.Settings[0].Value != "2" {
		t.Fatalf("settings = %+v", executor.query.Settings)
	}
}

func TestDropArchiveMonthFailsClosed(t *testing.T) {
	runner := &RollupRunner{executor: &rawDeleteExecutor{err: errors.New("temporary transport failure")}}
	month := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	for _, invalid := range []time.Time{month.AddDate(0, 0, 1), month.In(time.FixedZone("UTC+8", 8*3600))} {
		if err := runner.DropArchiveMonth(context.Background(), invalid, "job"); err == nil {
			t.Fatalf("unaligned month was accepted: %s", invalid)
		}
	}
	err := runner.DropArchiveMonth(context.Background(), month, "job")
	if err == nil || !strings.Contains(err.Error(), "drop Flow") {
		t.Fatalf("error = %v", err)
	}
}
