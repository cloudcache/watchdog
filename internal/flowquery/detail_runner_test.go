// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type fakeDetailRow struct {
	eventTime     time.Time
	recordID      string
	sourceIP      string
	destinationIP string
	values        map[DetailField]any
}

type fakeDetailExecutor struct {
	blocks     [][]fakeDetailRow
	err        error
	skipColumn string
}

func (e fakeDetailExecutor) Do(ctx context.Context, query ch.Query) error {
	results, ok := query.Result.(proto.Results)
	if !ok {
		return errors.New("detail runner did not install typed results")
	}
	for _, block := range e.blocks {
		for _, result := range results {
			result.Data.Reset()
		}
		for _, row := range block {
			appendFakeDetailRow(results, row, e.skipColumn)
		}
		if query.OnResult != nil {
			if err := query.OnResult(ctx, proto.Block{Columns: len(results), Rows: len(block)}); err != nil {
				return err
			}
		}
	}
	return e.err
}

func TestDetailRunnerDecodesMultipleBlocksAndBuildsStableNextCursor(t *testing.T) {
	compiled := compiledDetailQuery(t, []DetailField{
		DetailFieldReceivedTime, DetailFieldSourceIP, DetailFieldRawBytes, DetailFieldEstimatedValid,
	})
	compiled.Limit, compiled.MaxResultRows = 2, 3
	eventTime := compiled.From.Add(30*time.Minute + 123*time.Millisecond)
	rows := []fakeDetailRow{
		detailDataRow(eventTime, 3),
		detailDataRow(eventTime, 2),
		detailDataRow(eventTime, 1),
	}
	for index := range rows {
		rows[index].sourceIP = "::ffff:192.0.2.10"
		rows[index].values = map[DetailField]any{
			DetailFieldReceivedTime:   eventTime.Add(time.Duration(index) * time.Millisecond),
			DetailFieldSourceIP:       "::ffff:192.0.2.10",
			DetailFieldRawBytes:       uint64(100 + index),
			DetailFieldEstimatedValid: index != 1,
		}
	}
	runner, err := NewDetailRunner(fakeDetailExecutor{blocks: [][]fakeDetailRow{{rows[0]}, {rows[1], rows[2]}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if result.View != ViewCustomer || len(result.Rows) != 2 || !result.HasMore || result.NextCursor == "" || !reflect.DeepEqual(result.Fields, compiled.Fields) {
		t.Fatalf("detail result=%+v", result)
	}
	if result.Rows[0].RecordID != detailRecordID(3) || result.Rows[1].RecordID != detailRecordID(2) {
		t.Fatalf("detail page order=%+v", result.Rows)
	}
	if result.Rows[0].Values[DetailFieldSourceIP] != "192.0.2.10" || result.Rows[0].Values[DetailFieldRawBytes] != uint64(100) ||
		result.Rows[1].Values[DetailFieldEstimatedValid] != false {
		t.Fatalf("typed values=%+v", result.Rows)
	}
	request := validDetailRequest()
	request.Cursor = result.NextCursor
	next, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	if queryParameter(next.Query, "cursor_time") != "'2026-09-05 10:30:00.123'" || queryParameter(next.Query, "cursor_record_id") != "'"+detailRecordID(2)+"'" {
		t.Fatalf("next-page parameters=%+v", next.Query.Parameters)
	}
}

func TestDetailRunnerReturnsCompleteShortOrEmptyPageWithoutCursor(t *testing.T) {
	compiled := compiledDetailQuery(t, []DetailField{DetailFieldDestinationIP})
	row := detailDataRow(compiled.From.Add(time.Minute), 1)
	runner, err := NewDetailRunner(fakeDetailExecutor{blocks: [][]fakeDetailRow{{row}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.HasMore || result.NextCursor != "" {
		t.Fatalf("short result=%+v", result)
	}
	emptyRunner, err := NewDetailRunner(fakeDetailExecutor{blocks: [][]fakeDetailRow{{}}})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := emptyRunner.Run(context.Background(), compiled)
	if err != nil || len(empty.Rows) != 0 || empty.HasMore || empty.NextCursor != "" {
		t.Fatalf("empty result=%+v error=%v", empty, err)
	}
}

func TestDetailRunnerEnforcesCursorAcrossBlocks(t *testing.T) {
	request := validDetailRequest()
	boundaryTime := request.From.Add(30 * time.Minute)
	cursor, err := EncodeDetailCursor(boundaryTime, detailRecordID(2))
	if err != nil {
		t.Fatal(err)
	}
	request.Cursor = cursor
	request.Fields = []DetailField{DetailFieldRawBytes}
	compiled, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	valid := detailDataRow(boundaryTime, 1)
	invalid := detailDataRow(boundaryTime, 2)
	for _, blocks := range [][][]fakeDetailRow{{{invalid}}, {{valid}, {invalid}}} {
		runner, createErr := NewDetailRunner(fakeDetailExecutor{blocks: blocks})
		if createErr != nil {
			t.Fatal(createErr)
		}
		if result, runErr := runner.Run(context.Background(), compiled); runErr == nil || len(result.Rows) != 0 {
			t.Fatalf("result=%+v error=%v", result, runErr)
		}
	}
}

func TestDetailRunnerRejectsMalformedUnorderedOrUnboundedResults(t *testing.T) {
	compiled := compiledDetailQuery(t, []DetailField{DetailFieldRawBytes})
	valid := detailDataRow(compiled.From.Add(30*time.Minute), 3)
	tests := []struct {
		name       string
		blocks     [][]fakeDetailRow
		skipColumn string
		mutate     func(*CompiledDetail)
	}{
		{"duplicate", [][]fakeDetailRow{{valid, valid}}, "", nil},
		{"ascending id", [][]fakeDetailRow{{detailDataRow(valid.eventTime, 1), detailDataRow(valid.eventTime, 2)}}, "", nil},
		{"ascending time", [][]fakeDetailRow{{valid, detailDataRow(valid.eventTime.Add(time.Millisecond), 2)}}, "", nil},
		{"outside time", [][]fakeDetailRow{{detailDataRow(compiled.To, 1)}}, "", nil},
		{"invalid id", [][]fakeDetailRow{{func() fakeDetailRow { row := valid; row.recordID = "bad"; return row }()}}, "", nil},
		{"noncanonical id", [][]fakeDetailRow{{func() fakeDetailRow { row := valid; row.recordID = strings.ToUpper(detailRecordID(0xab)); return row }()}}, "", nil},
		{"invalid source ip", [][]fakeDetailRow{{func() fakeDetailRow { row := valid; row.sourceIP = "bad"; return row }()}}, "", nil},
		{"endpoint mismatch", [][]fakeDetailRow{{func() fakeDetailRow {
			row := valid
			row.sourceIP, row.destinationIP = "198.51.100.1", "198.51.100.2"
			return row
		}()}}, "", nil},
		{"column mismatch", [][]fakeDetailRow{{valid}}, "raw_bytes", nil},
		{"row bound", [][]fakeDetailRow{{valid, detailDataRow(valid.eventTime, 2), detailDataRow(valid.eventTime, 1), detailDataRow(valid.eventTime.Add(-time.Millisecond), 4)}}, "", nil},
		{"invalid endpoint", [][]fakeDetailRow{{valid}}, "", func(value *CompiledDetail) { value.Endpoint = "remote" }},
		{"invalid view", [][]fakeDetailRow{{valid}}, "", func(value *CompiledDetail) { value.View = ViewSupplier }},
		{"unknown field", [][]fakeDetailRow{{valid}}, "", func(value *CompiledDetail) { value.Fields = []DetailField{"unknown"} }},
		{"duplicate field", [][]fakeDetailRow{{valid}}, "", func(value *CompiledDetail) { value.Fields = []DetailField{DetailFieldRawBytes, DetailFieldRawBytes} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := compiled
			if test.mutate != nil {
				test.mutate(&current)
			}
			runner, err := NewDetailRunner(fakeDetailExecutor{blocks: test.blocks, skipColumn: test.skipColumn})
			if err != nil {
				t.Fatal(err)
			}
			result, err := runner.Run(context.Background(), current)
			if err == nil || len(result.Rows) != 0 {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
}

func TestDetailRunnerReturnsExecutorFailureWithoutPartialPage(t *testing.T) {
	compiled := compiledDetailQuery(t, []DetailField{DetailFieldRawBytes})
	want := errors.New("connection reset")
	runner, err := NewDetailRunner(fakeDetailExecutor{
		blocks: [][]fakeDetailRow{{detailDataRow(compiled.From.Add(time.Minute), 1)}}, err: want,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, runErr := runner.Run(context.Background(), compiled); !errors.Is(runErr, want) || len(result.Rows) != 0 || result.NextCursor != "" {
		t.Fatalf("result=%+v error=%v", result, runErr)
	}
}

func TestDetailRunnerRejectsMissingExecutorOrCompiledContract(t *testing.T) {
	if _, err := NewDetailRunner(nil); err == nil {
		t.Fatal("nil executor was accepted")
	}
	compiled := compiledDetailQuery(t, []DetailField{DetailFieldRawBytes})
	if _, err := (&DetailRunner{}).Run(context.Background(), compiled); err == nil {
		t.Fatal("uninitialized runner was accepted")
	}
	runner, err := NewDetailRunner(fakeDetailExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	compiled.Query.Body = ""
	if _, err := runner.Run(context.Background(), compiled); err == nil {
		t.Fatal("invalid compiled detail query was accepted")
	}
}

func compiledDetailQuery(t *testing.T, fields []DetailField) CompiledDetail {
	t.Helper()
	request := validDetailRequest()
	request.Fields = fields
	compiled, err := CompileDetail(Scope{TenantID: "tenant-a"}, request, detailNow())
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func detailDataRow(eventTime time.Time, id byte) fakeDetailRow {
	return fakeDetailRow{
		eventTime: eventTime, recordID: detailRecordID(id),
		sourceIP: "192.0.2.10", destinationIP: "198.51.100.20",
		values: map[DetailField]any{DetailFieldRawBytes: uint64(1), DetailFieldDestinationIP: "198.51.100.20"},
	}
}

func detailRecordID(value byte) string {
	return strings.Repeat(fmt.Sprintf("%02x", value), 32)
}

func appendFakeDetailRow(results proto.Results, row fakeDetailRow, skipColumn string) {
	for _, result := range results {
		if result.Name == skipColumn {
			continue
		}
		switch result.Name {
		case "event_time":
			result.Data.(*proto.ColDateTime64).Append(row.eventTime)
		case "record_id":
			result.Data.(*proto.ColStr).Append(row.recordID)
		case "_source_ip":
			result.Data.(*proto.ColStr).Append(row.sourceIP)
		case "_destination_ip":
			result.Data.(*proto.ColStr).Append(row.destinationIP)
		default:
			value := row.values[DetailField(result.Name)]
			switch column := result.Data.(type) {
			case *proto.ColStr:
				if value == nil {
					value = ""
				}
				column.Append(value.(string))
			case *proto.ColUInt64:
				if value == nil {
					value = uint64(0)
				}
				column.Append(value.(uint64))
			case *proto.ColBool:
				if value == nil {
					value = false
				}
				column.Append(value.(bool))
			case *proto.ColDateTime64:
				if value == nil {
					value = time.UnixMilli(0).UTC()
				}
				column.Append(value.(time.Time))
			default:
				panic(fmt.Sprintf("unsupported fake detail column %T", result.Data))
			}
		}
	}
}
