package watchdog

import (
	"bytes"
	"errors"

	"github.com/parquet-go/parquet-go"
)

type exportParquetRow struct {
	Timestamp  int64   `parquet:"timestamp,timestamp(millisecond:utc)"`
	Value      float64 `parquet:"value"`
	ValueLayer string  `parquet:"value_layer,dict"`
}

func RenderParquetExportColumns(task ExportTask, columns ExportColumns) ([]byte, error) {
	if task.ContractVersion != ExportExecutionContractVersion {
		return nil, errors.New("parquet export requires contract version 1")
	}
	logicalRows, err := buildExportLogicalRows(task, columns)
	if err != nil {
		return nil, err
	}
	rows := make([]exportParquetRow, 0, len(logicalRows))
	for _, row := range logicalRows {
		rows = append(rows, exportParquetRow{
			Timestamp: row.Timestamp.UnixMilli(), Value: row.Value, ValueLayer: row.ValueLayer,
		})
	}
	var output bytes.Buffer
	if err := parquet.Write(&output, rows); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
