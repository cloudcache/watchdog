package watchdog

import (
	"testing"
	"time"
)

func TestNormalizeExportRangeMonth(t *testing.T) {
	task, err := NormalizeExportRange(ExportTask{
		PeriodType: PeriodMonth,
		RangeStart: time.Date(2026, 6, 17, 1, 2, 3, 0, time.UTC),
	}, time.Time{}, time.UTC)
	if err != nil {
		t.Fatalf("NormalizeExportRange() error = %v", err)
	}
	if !task.RangeStart.Equal(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RangeStart = %s", task.RangeStart)
	}
	if !task.RangeEnd.Equal(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RangeEnd = %s", task.RangeEnd)
	}
}

func TestNormalizeExportRangeDay(t *testing.T) {
	task, err := NormalizeExportRange(ExportTask{
		PeriodType: PeriodDay,
		RangeStart: time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC),
	}, time.Time{}, time.UTC)
	if err != nil {
		t.Fatalf("NormalizeExportRange() error = %v", err)
	}
	if !task.RangeStart.Equal(time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RangeStart = %s", task.RangeStart)
	}
}

func TestNormalizeExportRangeFixedDefaultsToLastDay(t *testing.T) {
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	task, err := NormalizeExportRange(ExportTask{PeriodType: PeriodFixed}, now, time.UTC)
	if err != nil {
		t.Fatalf("NormalizeExportRange() error = %v", err)
	}
	if !task.RangeStart.Equal(now.Add(-24*time.Hour)) || !task.RangeEnd.Equal(now) {
		t.Fatalf("range = %s - %s", task.RangeStart, task.RangeEnd)
	}
}
