package snmpch

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowch"
)

func TestRealClickHouseSNMPWriteRateAndClosedBucket(t *testing.T) {
	if os.Getenv("WATCHDOG_SNMP_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_SNMP_CLICKHOUSE_INTEGRATION=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	database := "watchdog_snmp_it"
	password := os.Getenv("WATCHDOG_SNMP_CLICKHOUSE_PASSWORD")
	admin, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{Address: "127.0.0.1:9000", Database: "default", User: "default", Password: password, ClientName: "watchdog-snmp-it-admin", OperationTimeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	_ = admin.Do(ctx, ch.Query{Body: "DROP DATABASE IF EXISTS " + database})
	if err := admin.Do(ctx, ch.Query{Body: "CREATE DATABASE " + database}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		_ = admin.Do(cleanup, ch.Query{Body: "DROP DATABASE IF EXISTS " + database})
	})
	migrations, err := flowch.LoadMigrations(os.DirFS("../../deploy/migration/clickhouse"), ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range migrations[len(migrations)-1].Statements {
		if err := admin.Do(ctx, ch.Query{Body: strings.ReplaceAll(statement, "watchdog_flow", database)}); err != nil {
			t.Fatal(err)
		}
	}
	native, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{Address: "127.0.0.1:9000", Database: database, User: "default", Password: password, ClientName: "watchdog-snmp-it", OperationTimeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	store, _ := New(native)
	if err := store.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	bucket := time.Date(2026, 1, 2, 3, 5, 0, 0, time.UTC)
	for index, observed := range []time.Time{bucket.Add(-time.Minute), bucket, bucket.Add(time.Minute)} {
		for direction, metric := range []string{MetricIfInOctets, MetricIfOutOctets} {
			value := uint64(1000 + index*60_000 + direction*30_000)
			err := store.WriteSamples(ctx, []Sample{{ObservedAt: observed, DeviceID: "device-a", EntityKind: "port", EntityID: "port-a", RecipeID: metric, Metric: metric, ValueKind: "counter", CounterValue: value, CounterWidth: 64, IntervalMS: 60_000, PollSequence: uint64(index*2 + direction + 1), SourceRunID: "run", SampleIndex: 0}})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	series, err := store.Query(ctx, QueryRequest{DeviceID: "device-a", EntityID: "port-a", Metric: MetricIfInBPS, From: bucket, To: bucket.Add(2 * time.Minute), Step: time.Minute, MaxRows: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Points) != 2 {
		t.Fatalf("rate series=%+v", series)
	}
	for _, point := range series[0].Points {
		if point.Value != 8000 {
			t.Fatalf("rate=%v, want 8000", point.Value)
		}
	}
	if err := store.RebuildClosedInterfaceBucket(ctx, bucket, bucket.Add(6*time.Minute), 7); err != nil {
		t.Fatal(err)
	}
	var values, markers proto.ColUInt64
	err = native.Do(ctx, ch.Query{Body: `SELECT countIf(row_kind='value'),countIf(row_kind='generation') FROM snmp_interface_traffic_5m FINAL WHERE bucket_start=toDateTime('2026-01-02 03:05:00') AND generation=7`, Result: proto.Results{{Name: "countIf(equals(row_kind, 'value'))", Data: &values}, {Name: "countIf(equals(row_kind, 'generation'))", Data: &markers}}})
	if err != nil {
		t.Fatal(err)
	}
	if values.Rows() != 1 || values[0] != 1 || markers[0] != 1 {
		t.Fatalf("closed bucket values=%v markers=%v", values, markers)
	}
}
