package watchdog

import (
	"testing"
	"time"
)

func TestNormalizeExportTaskDefaults(t *testing.T) {
	task := normalizeExportTask(ExportTask{})
	if task.PeriodType != PeriodCustom {
		t.Fatalf("period type = %s", task.PeriodType)
	}
	if task.Step != 5*time.Minute {
		t.Fatalf("step = %s", task.Step)
	}
	if task.ValueMode != ExportValueCorrected {
		t.Fatalf("value mode = %s", task.ValueMode)
	}
	if task.Format != ExportFormatCSV {
		t.Fatalf("format = %s", task.Format)
	}
	if task.Status != ExportStatusPending {
		t.Fatalf("status = %s", task.Status)
	}
}

func TestNormalizeExportTaskPreservesExplicitValues(t *testing.T) {
	task := normalizeExportTask(ExportTask{
		PeriodType: PeriodMonth,
		Step:       15 * time.Minute,
		ValueMode:  ExportValueBoth,
		Format:     ExportFormatCSV,
		Status:     ExportStatusRunning,
	})
	if task.PeriodType != PeriodMonth || task.Step != 15*time.Minute || task.ValueMode != ExportValueBoth || task.Format != ExportFormatCSV || task.Status != ExportStatusRunning {
		t.Fatalf("task = %#v", task)
	}
}
