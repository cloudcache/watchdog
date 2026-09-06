package watchdog

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
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
	ValueLayer        QueryValueLayer
	Values            []Sample
}

type ExportFileWriter interface {
	WriteExport(ctx context.Context, task ExportTask, columns ExportColumns) (ExportArtifact, error)
}

// exportArtifactTTL is how long a produced export file is downloadable before it
// is considered stale (download refuses it, and the reaper deletes it).
const exportArtifactTTL = 7 * 24 * time.Hour

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
	var completenessPolicy *CompletenessPolicy
	if task.ContractVersion == ExportExecutionContractVersion {
		policy, err := exportCompletenessPolicyFromSnapshot(task)
		if err != nil {
			_ = w.Repo.MarkExportFailed(ctx, task.TenantID, task.ID, err.Error())
			return err
		}
		completenessPolicy = &policy
	} else if w.CompletenessPolicy != nil {
		policy := w.CompletenessPolicy(task)
		completenessPolicy = &policy
	}
	if completenessPolicy != nil {
		if _, err := VerifySampleCompleteness(samples, *completenessPolicy); err != nil {
			_ = w.Repo.MarkExportFailed(ctx, task.TenantID, task.ID, err.Error())
			return err
		}
	}
	columns, err := w.buildColumns(ctx, task, samples)
	if err != nil {
		_ = w.Repo.MarkExportFailed(ctx, task.TenantID, task.ID, err.Error())
		return err
	}
	artifact, err := w.Writer.WriteExport(ctx, task, columns)
	if err != nil {
		_ = w.Repo.MarkExportFailed(ctx, task.TenantID, task.ID, err.Error())
		return err
	}
	if err := validateExportArtifact(task, artifact); err != nil {
		_ = w.Repo.MarkExportFailed(ctx, task.TenantID, task.ID, err.Error())
		return err
	}
	ttl := exportArtifactTTL
	if task.ContractVersion == ExportExecutionContractVersion && task.RetentionSeconds > 0 {
		ttl = time.Duration(task.RetentionSeconds) * time.Second
	}
	return w.Repo.MarkExportComplete(ctx, task.TenantID, task.ID, artifact, time.Now().UTC().Add(ttl))
}

func validateExportArtifact(task ExportTask, artifact ExportArtifact) error {
	if strings.TrimSpace(artifact.FileRef) == "" || artifact.SizeBytes <= 0 || artifact.RowCount == 0 {
		return errors.New("export writer returned incomplete artifact metadata")
	}
	checksum, err := hex.DecodeString(artifact.Checksum)
	if err != nil || len(checksum) != 32 {
		return errors.New("export writer returned an invalid sha256 checksum")
	}
	if task.ContractVersion == ExportExecutionContractVersion {
		if artifact.SchemaVersion != ExportArtifactSchemaVersion || strings.TrimSpace(artifact.ContentType) == "" {
			return errors.New("export writer returned an unsupported artifact schema")
		}
		extension := "." + string(task.Format)
		if !strings.HasSuffix(artifact.FileRef, extension) {
			return errors.New("export artifact format does not match the task")
		}
	}
	return nil
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
	if task.ContractVersion == ExportExecutionContractVersion {
		return ExportColumns{ValueLayer: task.ValueLayer, Values: rawAggregated}, nil
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
