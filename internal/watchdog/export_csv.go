package watchdog

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type CSVExportWriter struct {
	Files map[string][]byte
}

func (w *CSVExportWriter) WriteExport(_ context.Context, task ExportTask, columns ExportColumns) (string, error) {
	if task.Format != "" && task.Format != ExportFormatCSV {
		return "", errors.New("csv writer only supports csv export format")
	}
	if w.Files == nil {
		w.Files = make(map[string][]byte)
	}
	data, err := RenderCSVExportColumns(task, columns)
	if err != nil {
		return "", err
	}
	fileRef := fmt.Sprintf("exports/%s.csv", task.ID)
	w.Files[fileRef] = data
	return fileRef, nil
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

type DiskCSVExportStore struct {
	Dir string
}

func (s DiskCSVExportStore) WriteExport(_ context.Context, task ExportTask, columns ExportColumns) (string, error) {
	if task.Format != "" && task.Format != ExportFormatCSV {
		return "", errors.New("csv writer only supports csv export format")
	}
	if s.Dir == "" {
		return "", errors.New("export directory is required")
	}
	data, err := RenderCSVExportColumns(task, columns)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return "", err
	}
	fileName := string(task.ID) + ".csv"
	if strings.Contains(fileName, "/") || strings.Contains(fileName, `\`) {
		return "", errors.New("invalid export id")
	}
	path := filepath.Join(s.Dir, fileName)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", err
	}
	return "exports/" + fileName, nil
}

func (s DiskCSVExportStore) ReadExport(_ context.Context, fileRef string) ([]byte, string, error) {
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
	return data, "text/csv; charset=utf-8", nil
}

// RenderCSVExportColumns renders the aggregated export columns. Columns are
// honest about what they contain: when the task asked for corrected values
// but no active policy applied (no port scope, or correction disabled), the
// output is labeled raw_value instead of pretending a correction happened.
func RenderCSVExportColumns(task ExportTask, columns ExportColumns) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
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
