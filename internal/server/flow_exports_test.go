// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/opjob"
)

func detailExportRow(fields []flowquery.DetailField, values ...any) flowDetailExportRow {
	return flowDetailExportRow{
		EventTime:        time.Date(2026, 9, 10, 1, 2, 3, 0, time.UTC),
		SourceCoordinate: flowquery.SourceCoordinate{SourceStreamID: "stream-1", KafkaPartition: 1, KafkaOffset: 2, RecordIndex: 3},
		Values:           values,
	}
}

// TestRenderFlowDetailCSV emits the coordinate header + fields and neutralizes CSV
// formula injection.
func TestRenderFlowDetailCSV(t *testing.T) {
	fields := []flowquery.DetailField{"source_ip", "bytes"}
	rows := flowDetailExportRows{View: flowquery.ViewRaw, Fields: fields,
		Rows: []flowDetailExportRow{detailExportRow(fields, "=cmd()", uint64(100))}}
	data, err := renderFlowDetailCSV(rows)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	out := string(data)
	if !strings.HasPrefix(out, "event_time,source_stream_id,kafka_partition,kafka_offset,record_index,source_ip,bytes\n") {
		t.Fatalf("header = %q", out)
	}
	if !strings.Contains(out, "'=cmd()") {
		t.Fatalf("formula not neutralized: %q", out)
	}
	if !strings.Contains(out, "stream-1,1,2,3,") || !strings.Contains(out, ",100\n") {
		t.Fatalf("row = %q", out)
	}
}

// TestRenderFlowDetailParquet renders a non-empty parquet artifact.
func TestRenderFlowDetailParquet(t *testing.T) {
	fields := []flowquery.DetailField{"source_ip", "bytes"}
	rows := flowDetailExportRows{View: flowquery.ViewRaw, Fields: fields,
		Rows: []flowDetailExportRow{detailExportRow(fields, "10.0.0.1", uint64(100)), detailExportRow(fields, "10.0.0.2", uint64(200))}}
	data, err := renderFlowDetailParquet(rows)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if len(data) < 4 || string(data[:4]) != "PAR1" {
		t.Fatalf("not a parquet file: %d bytes", len(data))
	}
}

// TestValidateFlowDetailExportRows rejects malformed row sets.
func TestValidateFlowDetailExportRows(t *testing.T) {
	fields := []flowquery.DetailField{"source_ip"}
	good := detailExportRow(fields, "10.0.0.1")
	for _, tc := range []struct {
		name string
		rows flowDetailExportRows
	}{
		{"bad view", flowDetailExportRows{View: "customer", Fields: fields, Rows: []flowDetailExportRow{good}}},
		{"no fields", flowDetailExportRows{View: flowquery.ViewRaw, Rows: []flowDetailExportRow{good}}},
		{"no rows", flowDetailExportRows{View: flowquery.ViewRaw, Fields: fields}},
		{"duplicate field", flowDetailExportRows{View: flowquery.ViewRaw, Fields: []flowquery.DetailField{"source_ip", "source_ip"},
			Rows: []flowDetailExportRow{detailExportRow([]flowquery.DetailField{"source_ip", "source_ip"}, "a", "b")}}},
		{"unsupported type", flowDetailExportRows{View: flowquery.ViewRaw, Fields: fields,
			Rows: []flowDetailExportRow{detailExportRow(fields, 3.14)}}},
		{"type drift", flowDetailExportRows{View: flowquery.ViewRaw, Fields: fields,
			Rows: []flowDetailExportRow{detailExportRow(fields, "a"), detailExportRow(fields, uint64(1))}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateFlowDetailExportRows(tc.rows); err == nil {
				t.Fatalf("%s: expected error", tc.name)
			}
		})
	}
	if err := validateFlowDetailExportRows(flowDetailExportRows{View: flowquery.ViewRaw, Fields: fields, Rows: []flowDetailExportRow{good}}); err != nil {
		t.Fatalf("valid rows rejected: %v", err)
	}
}

func TestSafeSpreadsheetCell(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", ""}, {"abc", "abc"}, {"=1+1", "'=1+1"}, {"+1", "'+1"}, {"-1", "'-1"}, {"@x", "'@x"},
	} {
		if got := safeSpreadsheetCell(tc.in); got != tc.want {
			t.Fatalf("safeSpreadsheetCell(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestFlowExportArtifact parses a well-formed artifact reference and rejects
// tampered ones.
func TestFlowExportArtifact(t *testing.T) {
	s := &Server{}
	s.cfg.Flow.Export.Dir = "/tmp/flow-exports"
	digest := sha256.Sum256([]byte("hello"))
	checksum := hex.EncodeToString(digest[:])
	job := opjob.Job{ID: "job1", ResultRef: "flow-export/job1-" + checksum + ".parquet"}
	path, gotChecksum, extension, err := s.flowExportArtifact(job)
	if err != nil {
		t.Fatalf("artifact: %v", err)
	}
	if gotChecksum != checksum || extension != "parquet" || !strings.HasSuffix(path, "job1-"+checksum+".parquet") {
		t.Fatalf("parsed = %s / %s / %s", path, gotChecksum, extension)
	}
	for _, ref := range []string{
		"wrong/job1-" + checksum + ".csv",
		"flow-export/other-" + checksum + ".csv",
		"flow-export/job1-" + checksum + ".txt",
		"flow-export/job1-deadbeef.csv",
		"flow-export/../job1-" + checksum + ".csv",
	} {
		if _, _, _, err := s.flowExportArtifact(opjob.Job{ID: "job1", ResultRef: ref}); err == nil {
			t.Fatalf("expected error for ref %q", ref)
		}
	}
}
