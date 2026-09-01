package watchdog

import (
	"errors"
	"time"
)

type ExportRequestValidation struct {
	Task           ExportTask
	Access         AccessRequest
	Grants         []Permission
	CollectionStep time.Duration
	IsAdmin        bool
}

func ValidateExportRequest(req ExportRequestValidation) error {
	task := normalizeExportTask(req.Task)
	if task.PeriodType == "" {
		return errors.New("export period type is required")
	}
	switch task.PeriodType {
	case PeriodDay, PeriodMonth, PeriodFixed, PeriodCustom:
	default:
		return errors.New("unsupported export period type")
	}
	if !task.RangeEnd.After(task.RangeStart) {
		return errors.New("export range end must be after start")
	}
	if !IsAllowedQueryStep(task.Step) {
		return errors.New("unsupported export step")
	}
	if req.CollectionStep > 0 && task.Step < req.CollectionStep {
		return errors.New("export step cannot be smaller than collection step")
	}
	if !isExportAggregation(task.Aggregation) {
		return errors.New("unsupported export aggregation")
	}
	if task.Format != ExportFormatCSV {
		return errors.New("unsupported export format")
	}
	if !CanCreateExport(req.Access, task.ValueMode, req.Grants, req.IsAdmin) {
		return errors.New("export permission denied")
	}
	return nil
}

func isExportAggregation(aggregation Aggregation) bool {
	switch aggregation {
	case AggregationP95FiveMinute,
		AggregationAverageFiveMinute,
		AggregationFourthPeakFiveMinute,
		AggregationDailyP95,
		AggregationDailyAverage,
		AggregationTotalBytes:
		return true
	default:
		return false
	}
}
