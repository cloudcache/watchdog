// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"encoding/hex"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

type queryRecorder struct {
	queries []ch.Query
	errors  map[int]error
}

func (r *queryRecorder) Do(_ context.Context, query ch.Query) error {
	r.queries = append(r.queries, query)
	return r.errors[len(r.queries)]
}

func TestNativeInserterWritesRecordsBeforeReceiptWithStableIdentity(t *testing.T) {
	batch := testEnrichedBatch(10, testEnrichedRecord(1, 100, 1_000))
	batch.AgentIP = batch.SourceIP
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{batch}, BatchLimits{})
	if err != nil {
		t.Fatal(err)
	}
	recorder := &queryRecorder{}
	inserter := &NativeInserter{executor: recorder}
	if err := inserter.InsertFlowBlock(context.Background(), blocks[0]); err != nil {
		t.Fatal(err)
	}
	if len(recorder.queries) != 2 {
		t.Fatalf("queries=%d, want records and receipt", len(recorder.queries))
	}
	if !strings.HasPrefix(recorder.queries[0].Body, `INSERT INTO "flow_records"`) || !strings.HasPrefix(recorder.queries[1].Body, `INSERT INTO "flow_ingest_batches"`) {
		t.Fatalf("unexpected insert sequence: %q then %q", recorder.queries[0].Body, recorder.queries[1].Body)
	}
	wantToken := hex.EncodeToString(blocks[0].ID[:])
	if setting(recorder.queries[0], "insert_deduplication_token") != wantToken || setting(recorder.queries[1], "insert_deduplication_token") != wantToken+"-receipt" {
		t.Fatal("record and receipt tokens are not stable and distinct")
	}
	for _, query := range recorder.queries {
		if setting(query, "async_insert") != "0" || setting(query, "wait_for_async_insert") != "1" {
			t.Fatal("insert is not explicitly synchronous")
		}
		assertInputRows(t, query.Input, 1)
	}
	assertEnumValue(t, recorder.queries[0].Input, "business_direction", "out")
	assertEnumValue(t, recorder.queries[0].Input, "category", "on_net_local_city")
	assertEnumValue(t, recorder.queries[0].Input, "remote_asn_source", "flow-geo-v2")
	for name, want := range map[string]string{
		"remote_geo_continent_id": "Asia", "remote_geo_region_id": "EastAsia", "remote_geo_country_id": "CN",
		"remote_geo_province_id": "330000", "remote_geo_city_id": "330100",
	} {
		assertLowCardinalityString(t, recorder.queries[0].Input, name, want)
	}
}

func TestNativeInserterRetriesUseIdenticalRecordBlock(t *testing.T) {
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{testEnrichedBatch(10, testEnrichedRecord(1, 100, 1_000))}, BatchLimits{})
	if err != nil {
		t.Fatal(err)
	}
	recorder := &queryRecorder{errors: map[int]error{2: errors.New("receipt unavailable")}}
	inserter := &NativeInserter{executor: recorder}
	if err := inserter.InsertFlowBlock(context.Background(), blocks[0]); err == nil {
		t.Fatal("expected receipt failure")
	}
	if err := inserter.InsertFlowBlock(context.Background(), blocks[0]); err != nil {
		t.Fatal(err)
	}
	if len(recorder.queries) != 4 {
		t.Fatalf("queries=%d, want records/receipt twice", len(recorder.queries))
	}
	if recorder.queries[0].Body != recorder.queries[2].Body || setting(recorder.queries[0], "insert_deduplication_token") != setting(recorder.queries[2], "insert_deduplication_token") {
		t.Fatal("record retry changed input contract or identity")
	}
	if !reflect.DeepEqual(columnValue(recorder.queries[0].Input, "ingest_generation"), columnValue(recorder.queries[2].Input, "ingest_generation")) {
		t.Fatal("record retry changed generation")
	}
}

func TestNativeInputColumnsMatchAuthoritativeMigration(t *testing.T) {
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{testEnrichedBatch(10, testEnrichedRecord(1, 100, 1_000))}, BatchLimits{})
	if err != nil {
		t.Fatal(err)
	}
	records, insertedAt, generation, err := buildRecordInput(blocks[0])
	if err != nil {
		t.Fatal(err)
	}
	receipt := buildReceiptInput(blocks[0], insertedAt, generation)
	schema := readClickHouseMigrations(t)
	assertColumnsMatchDDL(t, schema, "flow_records", records)
	assertColumnsMatchDDL(t, schema, "flow_ingest_batches", receipt)
}

func TestNativeInserterRejectsInvalidEnumsBeforeIO(t *testing.T) {
	record := testEnrichedRecord(1, 100, 1_000)
	record.Category = flowdimension.Category("invented")
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{testEnrichedBatch(10, record)}, BatchLimits{})
	if err != nil {
		t.Fatal(err)
	}
	recorder := &queryRecorder{}
	err = (&NativeInserter{executor: recorder}).InsertFlowBlock(context.Background(), blocks[0])
	var permanent *PermanentError
	if !errors.As(err, &permanent) || len(recorder.queries) != 0 {
		t.Fatalf("error=%v queries=%d, want permanent preflight rejection", err, len(recorder.queries))
	}
}

func TestClassifyClickHouseErrorSeparatesSchemaFromAvailability(t *testing.T) {
	for _, test := range []struct {
		code      proto.Error
		permanent bool
	}{
		{code: proto.ErrUnknownTable, permanent: true},
		{code: proto.ErrTypeMismatch, permanent: true},
		{code: proto.ErrNetworkError, permanent: false},
		{code: proto.ErrTooManyParts, permanent: false},
	} {
		err := classifyClickHouseError(&ch.Exception{Code: test.code, Name: "test", Message: "test"})
		var permanent *PermanentError
		if errors.As(err, &permanent) != test.permanent {
			t.Fatalf("code=%s permanent=%v", test.code, errors.As(err, &permanent))
		}
	}
}

func TestClickHouseIPAndCountryNormalization(t *testing.T) {
	v4 := netip.MustParseAddr("192.0.2.1")
	if got := clickHouseIP(v4).ToIP(); !got.Is4In6() || got.Unmap() != v4 {
		t.Fatalf("IPv4 mapping=%s", got)
	}
	if got := clickHouseIP(netip.Addr{}); got != (proto.IPv6{}) {
		t.Fatalf("invalid IP mapping=%s", got)
	}
	if got := string(countryCode("cn")); got != "CN" {
		t.Fatalf("country=%q", got)
	}
	if got := string(countryCode("")); got != flowdimension.GeoUnknownCountry {
		t.Fatalf("unknown country=%q", got)
	}
}

func assertColumnsMatchDDL(t *testing.T, schema, table string, input proto.Input) {
	t.Helper()
	start := strings.Index(schema, "CREATE TABLE IF NOT EXISTS watchdog_flow."+table+" (")
	if start < 0 {
		t.Fatalf("table %s not found", table)
	}
	section := schema[start:]
	end := strings.Index(section, "\n)\nENGINE")
	if end < 0 {
		t.Fatalf("table %s end not found", table)
	}
	matches := regexp.MustCompile(`(?m)^  ([a-z][a-z0-9_]*) `).FindAllStringSubmatch(section[:end], -1)
	want := make([]string, 0, len(matches))
	for _, match := range matches {
		want = append(want, match[1])
	}
	addColumnPattern := regexp.MustCompile(`(?m)ALTER TABLE watchdog_flow\.([a-z][a-z0-9_]*)\s+ADD COLUMN IF NOT EXISTS ([a-z][a-z0-9_]*) [^;\n]+ AFTER ([a-z][a-z0-9_]*);`)
	for _, match := range addColumnPattern.FindAllStringSubmatch(schema, -1) {
		if match[1] != table {
			continue
		}
		position := -1
		for index, column := range want {
			if column == match[3] {
				position = index + 1
				break
			}
		}
		if position < 0 {
			t.Fatalf("migration adds %s after missing column %s", match[2], match[3])
		}
		want = append(want, "")
		copy(want[position+1:], want[position:])
		want[position] = match[2]
	}
	got := make([]string, 0, len(input))
	for _, column := range input {
		got = append(got, column.Name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s columns differ\n got: %v\nwant: %v", table, got, want)
	}
}

func readClickHouseMigrations(t *testing.T) string {
	t.Helper()
	paths, err := filepath.Glob("../../deploy/migration/clickhouse/*.sql")
	if err != nil || len(paths) == 0 {
		t.Fatalf("list ClickHouse migrations: %v", err)
	}
	sort.Strings(paths)
	var schema strings.Builder
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		schema.Write(data)
		schema.WriteByte('\n')
	}
	return schema.String()
}

func assertInputRows(t *testing.T, input proto.Input, want int) {
	t.Helper()
	for _, column := range input {
		if got := column.Data.Rows(); got != want {
			t.Fatalf("column %s rows=%d want=%d", column.Name, got, want)
		}
	}
}

func assertEnumValue(t *testing.T, input proto.Input, name, want string) {
	t.Helper()
	column, ok := columnValue(input, name).(*proto.ColEnum)
	if !ok || len(column.Values) != 1 || column.Values[0] != want {
		t.Fatalf("column %s=%v want=%q", name, column, want)
	}
}

func assertLowCardinalityString(t *testing.T, input proto.Input, name, want string) {
	t.Helper()
	column, ok := columnValue(input, name).(*proto.ColLowCardinality[string])
	if !ok || column.Rows() != 1 || column.Row(0) != want {
		t.Fatalf("column %s=%v want=%q", name, column, want)
	}
}

func columnValue(input proto.Input, name string) proto.ColInput {
	for _, column := range input {
		if column.Name == name {
			return column.Data
		}
	}
	return nil
}

func setting(query ch.Query, key string) string {
	for _, setting := range query.Settings {
		if setting.Key == key {
			return setting.Value
		}
	}
	return ""
}
