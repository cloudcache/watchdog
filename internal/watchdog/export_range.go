package watchdog

import (
	"errors"
	"time"
)

func NormalizeExportRange(task ExportTask, now time.Time, loc *time.Location) (ExportTask, error) {
	if loc == nil {
		loc = time.UTC
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	task = normalizeExportTask(task)
	switch task.PeriodType {
	case PeriodFixed:
		if !task.RangeEnd.IsZero() && !task.RangeStart.IsZero() {
			return task, nil
		}
		if task.Step == 0 {
			task.Step = 5 * time.Minute
		}
		task.RangeEnd = now
		task.RangeStart = now.Add(-24 * time.Hour)
	case PeriodMonth:
		anchor := task.RangeStart.In(loc)
		if anchor.IsZero() {
			anchor = now.In(loc)
		}
		task.RangeStart = time.Date(anchor.Year(), anchor.Month(), 1, 0, 0, 0, 0, loc).UTC()
		task.RangeEnd = time.Date(anchor.Year(), anchor.Month()+1, 1, 0, 0, 0, 0, loc).UTC()
	case PeriodDay:
		anchor := task.RangeStart.In(loc)
		if anchor.IsZero() {
			anchor = now.In(loc)
		}
		task.RangeStart = time.Date(anchor.Year(), anchor.Month(), anchor.Day(), 0, 0, 0, 0, loc).UTC()
		task.RangeEnd = task.RangeStart.Add(24 * time.Hour)
	case PeriodCustom:
		if task.RangeStart.IsZero() || task.RangeEnd.IsZero() {
			return ExportTask{}, errors.New("custom export requires start and end")
		}
	default:
		return ExportTask{}, errors.New("unsupported export period type")
	}
	if !task.RangeEnd.After(task.RangeStart) {
		return ExportTask{}, errors.New("export range end must be after start")
	}
	return task, nil
}
