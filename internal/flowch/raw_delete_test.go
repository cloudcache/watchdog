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
	query ch.Query
	err   error
}

func (executor *rawDeleteExecutor) Do(_ context.Context, query ch.Query) error {
	executor.query = query
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
