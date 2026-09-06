package watchdog

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestRenderCSVExportColumnsWritesHeaderAndRows(t *testing.T) {
	data, err := RenderCSVExportColumns(ExportTask{ValueMode: ExportValueRaw}, ExportColumns{Raw: []Sample{{
		Time:  time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		Value: 123.5,
	}}})
	if err != nil {
		t.Fatalf("RenderCSVExportColumns() error = %v", err)
	}
	got := string(data)
	for _, want := range []string{
		"timestamp,raw_value",
		"2026-06-01T00:00:00Z,123.5",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("csv missing %q:\n%s", want, got)
		}
	}
}

func TestRenderCSVExportColumnsWritesBothValues(t *testing.T) {
	ts := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	data, err := RenderCSVExportColumns(ExportTask{ValueMode: ExportValueBoth}, ExportColumns{
		Raw:               []Sample{{Time: ts, Value: 100}},
		Corrected:         []Sample{{Time: ts, Value: 110}},
		CorrectionApplied: true,
	})
	if err != nil {
		t.Fatalf("RenderCSVExportColumns() error = %v", err)
	}
	got := string(data)
	for _, want := range []string{
		"timestamp,raw_value,corrected_value",
		"2026-06-01T00:00:00Z,100,110",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("csv missing %q:\n%s", want, got)
		}
	}
}

// A corrected export without an active policy must not pretend the values
// were corrected: the column is labeled raw_value.
func TestRenderCSVExportColumnsLabelsInactiveCorrectionAsRaw(t *testing.T) {
	ts := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	raw := []Sample{{Time: ts, Value: 100}}
	data, err := RenderCSVExportColumns(ExportTask{ValueMode: ExportValueCorrected}, ExportColumns{
		Raw:       raw,
		Corrected: raw,
	})
	if err != nil {
		t.Fatalf("RenderCSVExportColumns() error = %v", err)
	}
	got := string(data)
	if !strings.Contains(got, "timestamp,raw_value") || strings.Contains(got, "corrected_value") {
		t.Fatalf("inactive correction should label output raw_value:\n%s", got)
	}
}

// The exported corrected aggregate must equal the aggregate of per-sample
// corrected values (the ordering the metrics API and billing use), not a
// correction applied to the already-aggregated value.
func TestExportWorkerCorrectsBeforeAggregation(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	samples := []Sample{
		{Time: start, Value: 100},
		{Time: start.Add(time.Minute), Value: 200},
		{Time: start.Add(2 * time.Minute), Value: 300},
	}
	policy := PortPolicy{
		PortID:              "port-a",
		SideType:            PortSideCustomer,
		SampleStep:          time.Minute,
		Enabled:             true,
		CorrectionDirection: CorrectionUp,
		CorrectionMin:       10,
		CorrectionMax:       10,
	}
	task := ExportTask{
		ID:          "export-a",
		TenantID:    "tenant-a",
		PortID:      "port-a",
		ValueMode:   ExportValueBoth,
		Aggregation: AggregationP95FiveMinute,
		RangeStart:  start,
		RangeEnd:    start.Add(3 * time.Minute),
		Step:        time.Minute,
	}
	worker := ExportWorker{
		Network: fakeExportPolicyNetwork{policy: policy},
		Rand:    fixedRand(0),
	}
	columns, err := worker.buildColumns(context.Background(), task, samples)
	if err != nil {
		t.Fatalf("buildColumns() error = %v", err)
	}
	if len(columns.Raw) != 1 || len(columns.Corrected) != 1 {
		t.Fatalf("columns = %#v", columns)
	}
	if !columns.CorrectionApplied {
		t.Fatal("expected active correction")
	}
	if columns.Raw[0].Value != 300 {
		t.Fatalf("raw p95 = %v", columns.Raw[0].Value)
	}
	// p95 of corrected samples (110, 210, 310), not corrected p95 (300+10).
	if columns.Corrected[0].Value != 310 {
		t.Fatalf("corrected p95 = %v, want 310", columns.Corrected[0].Value)
	}
}

type fakeExportPolicyNetwork struct {
	NetworkRepository
	policy PortPolicy
}

func (f fakeExportPolicyNetwork) GetPortPolicy(context.Context, ID, ID) (PortPolicy, error) {
	return f.policy, nil
}

func TestCSVExportWriterStoresFileRef(t *testing.T) {
	writer := &CSVExportWriter{}
	artifact, err := writer.WriteExport(context.Background(), ExportTask{ID: "export-a", Format: ExportFormatCSV}, ExportColumns{Raw: []Sample{{
		Time:  time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		Value: 1,
	}}})
	if err != nil {
		t.Fatalf("WriteExport() error = %v", err)
	}
	if artifact.FileRef != "exports/export-a.csv" {
		t.Fatalf("fileRef = %s", artifact.FileRef)
	}
	if len(artifact.Checksum) != 64 || artifact.SizeBytes == 0 || artifact.SchemaVersion != ExportArtifactSchemaVersion ||
		artifact.ContentType != "text/csv; charset=utf-8" || artifact.RowCount != 1 {
		t.Fatalf("artifact metadata = %+v", artifact)
	}
	if len(writer.Files[artifact.FileRef]) == 0 {
		t.Fatal("expected stored csv data")
	}
}

func TestDiskCSVExportStoreWritesAndReadsFile(t *testing.T) {
	store := DiskCSVExportStore{Dir: t.TempDir()}
	artifact, err := store.WriteExport(context.Background(), ExportTask{ID: "export-a", Format: ExportFormatCSV}, ExportColumns{Raw: []Sample{{
		Time:  time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		Value: 42,
	}}})
	if err != nil {
		t.Fatalf("WriteExport() error = %v", err)
	}
	if artifact.FileRef != "exports/export-a.csv" {
		t.Fatalf("fileRef = %q", artifact.FileRef)
	}
	if len(artifact.Checksum) != 64 || artifact.SizeBytes == 0 || artifact.SchemaVersion != ExportArtifactSchemaVersion ||
		artifact.ContentType != "text/csv; charset=utf-8" || artifact.RowCount != 1 {
		t.Fatalf("artifact metadata = %+v", artifact)
	}
	data, contentType, err := store.ReadExport(context.Background(), artifact.FileRef)
	if err != nil {
		t.Fatalf("ReadExport() error = %v", err)
	}
	if contentType != "text/csv; charset=utf-8" || !strings.Contains(string(data), "42") {
		t.Fatalf("data = %q contentType=%q", data, contentType)
	}
}

func TestRenderAndReadParquetExportColumns(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 123*int(time.Millisecond), time.UTC)
	task := ExportTask{
		ID: "export-parquet", ContractVersion: ExportExecutionContractVersion,
		Format: ExportFormatParquet, ValueLayer: QueryValueSupplier,
	}
	columns := ExportColumns{ValueLayer: QueryValueSupplier, Values: []Sample{
		{Time: start, Value: 12.5},
		{Time: start.Add(time.Minute), Value: 18.75},
	}}
	data, err := RenderParquetExportColumns(task, columns)
	if err != nil {
		t.Fatalf("RenderParquetExportColumns() error = %v", err)
	}
	if len(data) < 8 || string(data[:4]) != "PAR1" || string(data[len(data)-4:]) != "PAR1" {
		t.Fatalf("parquet magic is missing: %x", data)
	}
	rows, err := parquet.Read[exportParquetRow](bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("parquet.Read() error = %v", err)
	}
	if len(rows) != 2 || rows[0].Timestamp != start.UnixMilli() || rows[0].Value != 12.5 || rows[0].ValueLayer != string(QueryValueSupplier) {
		t.Fatalf("parquet rows = %+v", rows)
	}
}

func TestDiskExportStoreWritesAndReadsParquet(t *testing.T) {
	store := DiskExportStore{Dir: t.TempDir()}
	task := ExportTask{
		ID: "export-parquet", ContractVersion: ExportExecutionContractVersion,
		Format: ExportFormatParquet, ValueLayer: QueryValueRaw,
	}
	artifact, err := store.WriteExport(context.Background(), task, ExportColumns{
		ValueLayer: QueryValueRaw,
		Values:     []Sample{{Time: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), Value: 42}},
	})
	if err != nil {
		t.Fatalf("WriteExport() error = %v", err)
	}
	if artifact.FileRef != "exports/export-parquet.parquet" || artifact.ContentType != "application/vnd.apache.parquet" || artifact.RowCount != 1 {
		t.Fatalf("artifact = %+v", artifact)
	}
	data, contentType, err := store.ReadExport(context.Background(), artifact.FileRef)
	if err != nil {
		t.Fatalf("ReadExport() error = %v", err)
	}
	if contentType != artifact.ContentType || !bytes.Equal(data[:4], []byte("PAR1")) {
		t.Fatalf("contentType=%q data=%x", contentType, data)
	}
	if err := store.DeleteExport(context.Background(), artifact.FileRef); err != nil {
		t.Fatalf("DeleteExport() error = %v", err)
	}
	if err := store.DeleteExport(context.Background(), artifact.FileRef); err != nil {
		t.Fatalf("idempotent DeleteExport() error = %v", err)
	}
	if _, _, err := store.ReadExport(context.Background(), artifact.FileRef); err == nil {
		t.Fatal("expected deleted artifact to be unreadable")
	}
}
