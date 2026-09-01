package watchdog

import (
	"context"
	"errors"
	"time"
)

type ExportDataProvider interface {
	LoadSamples(ctx context.Context, task ExportTask) ([]Sample, error)
}

// ExportColumns carries the aggregated output columns of an export task.
// Corrected is built by correcting raw samples per-sample BEFORE aggregation
// (the same order the metrics API and billing use), so exported aggregates
// match what the UI shows. CorrectionApplied reports whether an active port
// policy actually changed values; when false the corrected column merely
// duplicates raw and writers label the output accordingly.
type ExportColumns struct {
	Raw               []Sample
	Corrected         []Sample
	CorrectionApplied bool
}

type ExportFileWriter interface {
	WriteExport(ctx context.Context, task ExportTask, columns ExportColumns) (string, error)
}

type ExportFileReader interface {
	ReadExport(ctx context.Context, fileRef string) ([]byte, string, error)
}

type PendingExportRepository interface {
	ListPendingExportTasks(ctx context.Context, limit int) ([]ExportTask, error)
}

type ExportWorkerRunResult struct {
	Processed int
	Completed int
	Failed    int
}

type ExportWorkerLoopConfig struct {
	Interval       time.Duration
	BatchSize      int
	RunImmediately bool
}

type ExportWorker struct {
	Repo               ExportRepository
	Data               ExportDataProvider
	Writer             ExportFileWriter
	Network            NetworkRepository
	Rand               CorrectionRandom
	CompletenessPolicy func(ExportTask) CompletenessPolicy
}

func (w ExportWorker) RunLoop(ctx context.Context, cfg ExportWorkerLoopConfig) error {
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultExportWorkerInterval
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultExportWorkerBatch
	}
	if cfg.RunImmediately {
		if _, err := w.RunPending(ctx, batchSize); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := w.RunPending(ctx, batchSize); err != nil {
				return err
			}
		}
	}
}

func (w ExportWorker) RunPending(ctx context.Context, limit int) (ExportWorkerRunResult, error) {
	pendingRepo, ok := w.Repo.(PendingExportRepository)
	if !ok {
		return ExportWorkerRunResult{}, errors.New("export repository does not support pending task listing")
	}
	tasks, err := pendingRepo.ListPendingExportTasks(ctx, limit)
	if err != nil {
		return ExportWorkerRunResult{}, err
	}
	result := ExportWorkerRunResult{}
	for _, task := range tasks {
		result.Processed++
		if err := w.RunTask(ctx, task); err != nil {
			result.Failed++
			continue
		}
		result.Completed++
	}
	return result, nil
}

func (w ExportWorker) RunTask(ctx context.Context, task ExportTask) error {
	if w.Repo == nil || w.Data == nil || w.Writer == nil {
		return errors.New("export worker dependencies are required")
	}
	if err := w.Repo.MarkExportRunning(ctx, task.TenantID, task.ID); err != nil {
		return err
	}
	samples, err := w.Data.LoadSamples(ctx, task)
	if err != nil {
		_ = w.Repo.MarkExportFailed(ctx, task.TenantID, task.ID, err.Error())
		return err
	}
	if w.CompletenessPolicy != nil {
		if _, err := VerifySampleCompleteness(samples, w.CompletenessPolicy(task)); err != nil {
			_ = w.Repo.MarkExportFailed(ctx, task.TenantID, task.ID, err.Error())
			return err
		}
	}
	columns, err := w.buildColumns(ctx, task, samples)
	if err != nil {
		_ = w.Repo.MarkExportFailed(ctx, task.TenantID, task.ID, err.Error())
		return err
	}
	fileRef, err := w.Writer.WriteExport(ctx, task, columns)
	if err != nil {
		_ = w.Repo.MarkExportFailed(ctx, task.TenantID, task.ID, err.Error())
		return err
	}
	return w.Repo.MarkExportComplete(ctx, task.TenantID, task.ID, fileRef)
}

// buildColumns aggregates the raw samples and, when the task wants corrected
// values and the port has an active policy, corrects per-sample first and
// aggregates the corrected series separately — the same ordering as the
// metrics API and billing, so all three outputs agree.
func (w ExportWorker) buildColumns(ctx context.Context, task ExportTask, samples []Sample) (ExportColumns, error) {
	task = normalizeExportTask(task)
	rawAggregated, err := aggregateExportSamples(task, samples)
	if err != nil {
		return ExportColumns{}, err
	}
	columns := ExportColumns{Raw: rawAggregated}
	if task.ValueMode == ExportValueRaw {
		return columns, nil
	}
	policy := PortPolicy{}
	if task.PortID != "" && w.Network != nil {
		if loaded, err := w.Network.GetPortPolicy(ctx, task.TenantID, task.PortID); err == nil {
			policy = loaded.Normalize()
		}
	}
	if !correctionActive(policy) {
		columns.Corrected = rawAggregated
		return columns, nil
	}
	corrected := make([]Sample, len(samples))
	for i, sample := range samples {
		rng := w.Rand
		if rng == nil {
			rng = DeterministicCorrectionRNG(string(task.PortID), sample.Time)
		}
		corrected[i] = Sample{Time: sample.Time, Value: ApplyCorrectionFloat(sample.Value, policy, rng)}
	}
	correctedAggregated, err := aggregateExportSamples(task, corrected)
	if err != nil {
		return ExportColumns{}, err
	}
	columns.Corrected = correctedAggregated
	columns.CorrectionApplied = true
	return columns, nil
}
