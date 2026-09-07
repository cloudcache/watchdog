package watchdog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// exportArtifactFor stamps a produced export file with its sha256 checksum and
// byte size for integrity verification on download.
func exportArtifactFor(fileRef, contentType string, rowCount uint64, data []byte) ExportArtifact {
	sum := sha256.Sum256(data)
	return ExportArtifact{
		FileRef: fileRef, Checksum: hex.EncodeToString(sum[:]), SizeBytes: int64(len(data)),
		SchemaVersion: ExportArtifactSchemaVersion, ContentType: contentType, RowCount: rowCount,
	}
}

type CSVExportWriter struct {
	Files map[string][]byte
}

func (w *CSVExportWriter) WriteExport(_ context.Context, task ExportTask, columns ExportColumns) (ExportArtifact, error) {
	if task.Format != "" && task.Format != ExportFormatCSV {
		return ExportArtifact{}, errors.New("csv writer only supports csv export format")
	}
	if w.Files == nil {
		w.Files = make(map[string][]byte)
	}
	data, err := RenderCSVExportColumns(task, columns)
	if err != nil {
		return ExportArtifact{}, err
	}
	fileRef := fmt.Sprintf("exports/%s.csv", task.ID)
	w.Files[fileRef] = data
	return exportArtifactFor(fileRef, "text/csv; charset=utf-8", exportColumnRowCount(columns), data), nil
}

func (w *CSVExportWriter) WriteExportRows(_ context.Context, task ExportTask, rows ExportRows) (ExportArtifact, bool, error) {
	if task.DatasetKey != FlowTrafficDataset {
		return ExportArtifact{}, false, nil
	}
	if task.Format != ExportFormatCSV {
		return ExportArtifact{}, true, errors.New("csv writer only supports csv export format")
	}
	if w.Files == nil {
		w.Files = make(map[string][]byte)
	}
	data, err := RenderCSVExportRows(rows)
	if err != nil {
		return ExportArtifact{}, true, err
	}
	fileRef := fmt.Sprintf("exports/%s.csv", task.ID)
	w.Files[fileRef] = data
	return exportArtifactFor(fileRef, "text/csv; charset=utf-8", uint64(len(rows.Rows)), data), true, nil
}

func (w *CSVExportWriter) ReadExport(_ context.Context, fileRef string) ([]byte, string, error) {
	if w == nil || w.Files == nil {
		return nil, "", errors.New("export file not found")
	}
	data, ok := w.Files[fileRef]
	if !ok {
		return nil, "", errors.New("export file not found")
	}
	return data, "text/csv; charset=utf-8", nil
}

func (w *CSVExportWriter) DeleteExport(_ context.Context, fileRef string) error {
	if w != nil && w.Files != nil {
		delete(w.Files, fileRef)
	}
	return nil
}

type DiskExportStore struct {
	Dir string
}

// DiskCSVExportStore remains as a source-compatible alias for callers that
// predate Parquet support. DiskExportStore is the format-routing store.
type DiskCSVExportStore = DiskExportStore

func (s DiskExportStore) WriteExport(_ context.Context, task ExportTask, columns ExportColumns) (ExportArtifact, error) {
	if s.Dir == "" {
		return ExportArtifact{}, errors.New("export directory is required")
	}
	var (
		data        []byte
		contentType string
		extension   string
		err         error
	)
	switch task.Format {
	case "", ExportFormatCSV:
		data, err = RenderCSVExportColumns(task, columns)
		contentType, extension = "text/csv; charset=utf-8", "csv"
	case ExportFormatParquet:
		data, err = RenderParquetExportColumns(task, columns)
		contentType, extension = "application/vnd.apache.parquet", "parquet"
	default:
		return ExportArtifact{}, errors.New("unsupported export format")
	}
	if err != nil {
		return ExportArtifact{}, err
	}
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return ExportArtifact{}, err
	}
	fileName := string(task.ID) + "." + extension
	if strings.Contains(fileName, "/") || strings.Contains(fileName, `\`) {
		return ExportArtifact{}, errors.New("invalid export id")
	}
	path := filepath.Join(s.Dir, fileName)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return ExportArtifact{}, err
	}
	return exportArtifactFor("exports/"+fileName, contentType, exportColumnRowCount(columns), data), nil
}

func (s DiskExportStore) WriteExportRows(_ context.Context, task ExportTask, rows ExportRows) (ExportArtifact, bool, error) {
	if task.DatasetKey != FlowTrafficDataset {
		return ExportArtifact{}, false, nil
	}
	if s.Dir == "" {
		return ExportArtifact{}, true, errors.New("export directory is required")
	}
	var data []byte
	var contentType, extension string
	var err error
	switch task.Format {
	case ExportFormatCSV:
		data, err = RenderCSVExportRows(rows)
		contentType, extension = "text/csv; charset=utf-8", "csv"
	case ExportFormatParquet:
		data, err = RenderParquetExportRows(rows)
		contentType, extension = "application/vnd.apache.parquet", "parquet"
	default:
		return ExportArtifact{}, true, errors.New("unsupported Flow export format")
	}
	if err != nil {
		return ExportArtifact{}, true, err
	}
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return ExportArtifact{}, true, err
	}
	fileName := string(task.ID) + "." + extension
	if strings.Contains(fileName, "/") || strings.Contains(fileName, `\`) {
		return ExportArtifact{}, true, errors.New("invalid export id")
	}
	if err := os.WriteFile(filepath.Join(s.Dir, fileName), data, 0o600); err != nil {
		return ExportArtifact{}, true, err
	}
	return exportArtifactFor("exports/"+fileName, contentType, uint64(len(rows.Rows)), data), true, nil
}

func (s DiskExportStore) ReadExport(_ context.Context, fileRef string) ([]byte, string, error) {
	if s.Dir == "" {
		return nil, "", errors.New("export directory is required")
	}
	fileName := strings.TrimPrefix(fileRef, "exports/")
	if fileName == "" || strings.Contains(fileName, "/") || strings.Contains(fileName, `\`) {
		return nil, "", errors.New("export file not found")
	}
	data, err := os.ReadFile(filepath.Join(s.Dir, fileName))
	if err != nil {
		return nil, "", err
	}
	contentType := "application/octet-stream"
	if strings.HasSuffix(fileName, ".csv") {
		contentType = "text/csv; charset=utf-8"
	} else if strings.HasSuffix(fileName, ".parquet") {
		contentType = "application/vnd.apache.parquet"
	}
	return data, contentType, nil
}

func (s DiskExportStore) DeleteExport(_ context.Context, fileRef string) error {
	if s.Dir == "" {
		return errors.New("export directory is required")
	}
	fileName := strings.TrimPrefix(fileRef, "exports/")
	if fileName == "" || strings.Contains(fileName, "/") || strings.Contains(fileName, `\`) {
		return errors.New("export file not found")
	}
	if err := os.Remove(filepath.Join(s.Dir, fileName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// RenderCSVExportColumns renders the aggregated export columns. Columns are
// honest about what they contain: when the task asked for corrected values
// but no active policy applied (no port scope, or correction disabled), the
// output is labeled raw_value instead of pretending a correction happened.
func RenderCSVExportColumns(task ExportTask, columns ExportColumns) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if task.ContractVersion == ExportExecutionContractVersion {
		rows, err := buildExportLogicalRows(task, columns)
		if err != nil {
			return nil, err
		}
		if err := writer.Write([]string{"timestamp", "value", "value_layer"}); err != nil {
			return nil, err
		}
		for _, row := range rows {
			if err := writer.Write([]string{
				row.Timestamp.UTC().Format("2006-01-02T15:04:05Z"),
				strconv.FormatFloat(row.Value, 'f', -1, 64), row.ValueLayer,
			}); err != nil {
				return nil, err
			}
		}
		writer.Flush()
		return buf.Bytes(), writer.Error()
	}
	mode := task.ValueMode
	if mode != ExportValueRaw && !columns.CorrectionApplied {
		mode = ExportValueRaw
	}
	header := []string{"timestamp"}
	switch mode {
	case ExportValueRaw:
		header = append(header, "raw_value")
	case ExportValueBoth:
		header = append(header, "raw_value", "corrected_value")
	default:
		header = append(header, "corrected_value")
	}
	if err := writer.Write(header); err != nil {
		return nil, err
	}
	corrected := columns.Corrected
	if len(corrected) == 0 {
		corrected = columns.Raw
	}
	if mode != ExportValueRaw && len(corrected) != len(columns.Raw) {
		return nil, errors.New("export raw and corrected columns are misaligned")
	}
	for i, sample := range columns.Raw {
		row := []string{sample.Time.UTC().Format("2006-01-02T15:04:05Z")}
		rawValue := strconv.FormatFloat(sample.Value, 'f', -1, 64)
		switch mode {
		case ExportValueRaw:
			row = append(row, rawValue)
		case ExportValueBoth:
			row = append(row, rawValue, strconv.FormatFloat(corrected[i].Value, 'f', -1, 64))
		default:
			row = append(row, strconv.FormatFloat(corrected[i].Value, 'f', -1, 64))
		}
		if err := writer.Write(row); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	return buf.Bytes(), writer.Error()
}

func exportColumnRowCount(columns ExportColumns) uint64 {
	if columns.ValueLayer != "" || columns.Values != nil {
		return uint64(len(columns.Values))
	}
	return uint64(len(columns.Raw))
}

type exportLogicalRow struct {
	Timestamp  time.Time
	Value      float64
	ValueLayer string
}

func buildExportLogicalRows(task ExportTask, columns ExportColumns) ([]exportLogicalRow, error) {
	layer := columns.ValueLayer
	if layer == "" {
		layer = task.ValueLayer
	}
	if layer != QueryValueRaw && layer != QueryValueSupplier && layer != QueryValueCustomer {
		return nil, errors.New("export value layer is invalid")
	}
	rows := make([]exportLogicalRow, 0, len(columns.Values))
	for _, sample := range columns.Values {
		rows = append(rows, exportLogicalRow{Timestamp: sample.Time.UTC(), Value: sample.Value, ValueLayer: string(layer)})
	}
	return rows, nil
}
