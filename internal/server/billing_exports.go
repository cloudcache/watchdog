// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
	"github.com/parquet-go/parquet-go"
)

const (
	billingExportJobType       = "billing.period.export"
	billingExportPayloadSchema = 1
)

type billingExportPayload struct {
	PeriodID           string `json:"period_id"`
	CalculationVersion uint64 `json:"calculation_version"`
	Format             string `json:"format"`
}

type billingEvidenceRow struct {
	RecordKind           string  `parquet:"record_kind,dict"`
	PeriodID             string  `parquet:"period_id,dict"`
	AccountID            string  `parquet:"account_id,dict"`
	CalculationVersion   uint64  `parquet:"calculation_version"`
	ReferenceID          string  `parquet:"reference_id,dict"`
	Layer                string  `parquet:"layer,dict"`
	RightLayer           string  `parquet:"right_layer,dict"`
	Severity             string  `parquet:"severity,dict"`
	Direction            string  `parquet:"direction,dict"`
	DeviceID             string  `parquet:"device_id,dict"`
	IfIndex              uint32  `parquet:"if_index"`
	IfName               string  `parquet:"if_name,dict"`
	Status               string  `parquet:"status,dict"`
	Algorithm            string  `parquet:"algorithm,dict"`
	Unit                 string  `parquet:"unit,dict"`
	InBytes              uint64  `parquet:"in_bytes"`
	OutBytes             uint64  `parquet:"out_bytes"`
	SelectedBytes        uint64  `parquet:"selected_bytes"`
	Rate95thBPS          uint64  `parquet:"rate_95th_bps"`
	RateAverageBPS       uint64  `parquet:"rate_average_bps"`
	AlgorithmValue       uint64  `parquet:"algorithm_value"`
	LedgerEffectiveValue uint64  `parquet:"ledger_effective_value"`
	Coverage             float64 `parquet:"coverage"`
	ExpectedBuckets      uint32  `parquet:"expected_buckets"`
	ObservedBuckets      uint32  `parquet:"observed_buckets"`
	MissingBuckets       uint32  `parquet:"missing_buckets"`
	ResetBuckets         uint32  `parquet:"reset_buckets"`
	GapBuckets           uint32  `parquet:"gap_buckets"`
	UnknownSampling      uint64  `parquet:"unknown_sampling_records"`
	SourceGenerationMin  uint64  `parquet:"source_generation_min"`
	SourceGenerationMax  uint64  `parquet:"source_generation_max"`
	SignedAmount         int64   `parquet:"signed_amount"`
	ExpectedValue        int64   `parquet:"expected_value"`
	ActualValue          int64   `parquet:"actual_value"`
	DeltaValue           int64   `parquet:"delta_value"`
	ThresholdValue       uint64  `parquet:"threshold_value"`
	ThresholdPercent     float64 `parquet:"threshold_percent"`
	Allowed              uint64  `parquet:"allowed"`
	Used                 uint64  `parquet:"used"`
	Overuse              uint64  `parquet:"overuse"`
	Reason               string  `parquet:"reason,dict"`
	EvidenceRef          string  `parquet:"evidence_ref,dict"`
	ProvenanceJSON       string  `parquet:"provenance_json"`
	DateFrom             string  `parquet:"date_from,dict"`
	DateTo               string  `parquet:"date_to,dict"`
	Timezone             string  `parquet:"timezone,dict"`
	CreatedBy            string  `parquet:"created_by,dict"`
	ApprovedBy           string  `parquet:"approved_by,dict"`
	ApprovedAt           string  `parquet:"approved_at,dict"`
	ClosedBy             string  `parquet:"closed_by,dict"`
	ClosedAt             string  `parquet:"closed_at,dict"`
	CreatedAt            string  `parquet:"created_at,dict"`
}

var billingEvidenceHeader = []string{
	"record_kind", "period_id", "account_id", "calculation_version", "reference_id", "layer", "right_layer", "severity", "direction", "device_id", "if_index", "if_name", "status", "algorithm", "unit",
	"in_bytes", "out_bytes", "selected_bytes", "rate_95th_bps", "rate_average_bps", "algorithm_value", "ledger_effective_value",
	"coverage", "expected_buckets", "observed_buckets", "missing_buckets", "reset_buckets", "gap_buckets", "unknown_sampling_records", "source_generation_min", "source_generation_max",
	"signed_amount", "expected_value", "actual_value", "delta_value", "threshold_value", "threshold_percent", "allowed", "used", "overuse", "reason", "evidence_ref", "provenance_json",
	"date_from", "date_to", "timezone", "created_by", "approved_by", "approved_at", "closed_by", "closed_at", "created_at",
}

func billingEvidenceRows(evidence billing.ExportEvidence) []billingEvidenceRow {
	periodProvenance, _ := json.Marshal(map[string]any{
		"account_snapshot": json.RawMessage(evidence.Period.AccountSnapshot),
		"calculation":      json.RawMessage(evidence.Period.Provenance),
		"publication_ref":  evidence.Period.PublicationRef,
	})
	rows := []billingEvidenceRow{{
		RecordKind: "period", PeriodID: evidence.Period.ID, AccountID: evidence.Account.ID,
		CalculationVersion: evidence.Period.CalculationVersion, ReferenceID: evidence.Period.ID, Status: string(evidence.Period.Status),
		Algorithm: string(evidence.Period.Algorithm), Layer: string(evidence.Period.DefaultLayer), Direction: string(evidence.Period.Direction),
		Allowed: valueOrZero(evidence.Period.Allowed), Used: valueOrZero(evidence.Period.Used), Overuse: valueOrZero(evidence.Period.Overuse),
		ProvenanceJSON: string(periodProvenance), DateFrom: evidence.Period.DateFrom.UTC().Format(time.RFC3339Nano),
		DateTo: evidence.Period.DateTo.UTC().Format(time.RFC3339Nano), Timezone: evidence.Period.Timezone, CreatedBy: evidence.Period.CreatedBy,
		ApprovedBy: evidence.Period.ApprovedBy, ApprovedAt: formatOptionalTime(evidence.Period.ApprovedAt), ClosedBy: evidence.Period.ClosedBy,
		ClosedAt:  formatOptionalTime(evidence.Period.ClosedAt),
		CreatedAt: evidence.Period.CreatedAt.UTC().Format(time.RFC3339Nano),
	}}
	rows = append(rows, billingEvidenceRow{RecordKind: "account", PeriodID: evidence.Period.ID, AccountID: evidence.Account.ID,
		CalculationVersion: evidence.Period.CalculationVersion, ReferenceID: evidence.Account.ID, Direction: string(evidence.Account.Direction),
		Status: evidence.Account.Status, Algorithm: string(evidence.Account.Algorithm), Layer: string(evidence.Account.DefaultLayer),
		Timezone: evidence.Account.Timezone, ProvenanceJSON: string(evidence.Period.AccountSnapshot), CreatedBy: evidence.Account.CreatedBy,
		CreatedAt: evidence.Account.CreatedAt.UTC().Format(time.RFC3339Nano)})
	if evidence.Party != nil {
		partyJSON, _ := json.Marshal(evidence.Party)
		rows = append(rows, billingEvidenceRow{RecordKind: "party", PeriodID: evidence.Period.ID, AccountID: evidence.Account.ID,
			CalculationVersion: evidence.Period.CalculationVersion, ReferenceID: evidence.Party.ID, Status: evidence.Party.Status,
			ProvenanceJSON: string(partyJSON), CreatedBy: evidence.Party.CreatedBy, CreatedAt: evidence.Party.CreatedAt.UTC().Format(time.RFC3339Nano)})
	}
	for _, port := range evidence.Ports {
		rows = append(rows, billingEvidenceRow{RecordKind: "port", PeriodID: evidence.Period.ID, AccountID: evidence.Account.ID,
			CalculationVersion: evidence.Period.CalculationVersion, ReferenceID: port.PortID, Direction: string(port.Direction),
			DeviceID: port.DeviceID, IfIndex: port.IfIndex, IfName: port.IfName, CreatedAt: evidence.Period.CreatedAt.UTC().Format(time.RFC3339Nano)})
	}
	for _, value := range evidence.Values {
		adjustment := billing.EffectiveAdjustment(evidence.Adjustments, value.Layer, value.Unit)
		rows = append(rows, billingEvidenceRow{
			RecordKind: "value", PeriodID: value.PeriodID, AccountID: evidence.Account.ID, CalculationVersion: value.CalculationVersion,
			ReferenceID: value.ID, Layer: string(value.Layer), Algorithm: string(value.Algorithm), Unit: value.Unit,
			InBytes: value.InBytes, OutBytes: value.OutBytes, SelectedBytes: value.SelectedBytes, Rate95thBPS: value.Rate95thBPS,
			RateAverageBPS: value.RateAverageBPS, AlgorithmValue: value.AlgorithmValue,
			LedgerEffectiveValue: billing.ApplyAdjustment(value.AlgorithmValue, adjustment), Coverage: value.Coverage,
			ExpectedBuckets: value.ExpectedBuckets, ObservedBuckets: value.ObservedBuckets, MissingBuckets: value.MissingBuckets,
			ResetBuckets: value.ResetBuckets, GapBuckets: value.GapBuckets, UnknownSampling: value.UnknownSamplingRecords,
			SourceGenerationMin: value.SourceGenerationMin, SourceGenerationMax: value.SourceGenerationMax, ProvenanceJSON: string(value.Provenance),
			CreatedAt: value.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	for _, item := range evidence.Adjustments {
		rows = append(rows, billingEvidenceRow{RecordKind: "adjustment", PeriodID: item.PeriodID, AccountID: evidence.Account.ID,
			CalculationVersion: evidence.Period.CalculationVersion, ReferenceID: item.ID, Layer: string(item.Layer), Status: item.Status,
			Unit: item.Unit, SignedAmount: item.Amount, Reason: item.Reason, EvidenceRef: item.EvidenceRef,
			ProvenanceJSON: fmt.Sprintf(`{"reverses_adjustment_id":%q}`, item.ReversesAdjustmentID), CreatedBy: item.CreatedBy,
			ApprovedBy: item.ApprovedBy, ApprovedAt: formatOptionalTime(item.ApprovedAt), CreatedAt: item.CreatedAt.UTC().Format(time.RFC3339Nano)})
	}
	for _, item := range evidence.Reconciliations {
		rows = append(rows, billingEvidenceRow{RecordKind: "reconciliation", PeriodID: item.PeriodID, AccountID: evidence.Account.ID,
			CalculationVersion: item.CalculationVersion, ReferenceID: item.ID, Status: item.Status, ThresholdValue: item.ThresholdAbs,
			ThresholdPercent: item.ThresholdPercent, ProvenanceJSON: string(item.Summary), CreatedBy: item.CreatedBy, CreatedAt: item.CreatedAt.UTC().Format(time.RFC3339Nano)})
	}
	for _, item := range evidence.Issues {
		rows = append(rows, billingEvidenceRow{RecordKind: "issue", PeriodID: evidence.Period.ID, AccountID: evidence.Account.ID,
			CalculationVersion: evidence.Period.CalculationVersion, ReferenceID: item.ID, Layer: string(item.LeftLayer), RightLayer: string(item.RightLayer),
			Severity: item.Severity, Status: item.Status,
			ExpectedValue: item.ExpectedValue, ActualValue: item.ActualValue, DeltaValue: item.DeltaValue, ThresholdValue: item.ThresholdValue,
			Reason: item.ResolutionNote, EvidenceRef: item.Kind + ":" + item.Metric, ProvenanceJSON: string(item.Detail), CreatedAt: item.CreatedAt.UTC().Format(time.RFC3339Nano)})
	}
	return rows
}

func valueOrZero(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func renderBillingCSV(rows []billingEvidenceRow) ([]byte, error) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	if err := writer.Write(billingEvidenceHeader); err != nil {
		return nil, err
	}
	for _, row := range rows {
		fields := []string{
			row.RecordKind, row.PeriodID, row.AccountID, strconv.FormatUint(row.CalculationVersion, 10), row.ReferenceID, row.Layer, row.RightLayer, row.Severity,
			row.Direction, row.DeviceID, strconv.FormatUint(uint64(row.IfIndex), 10), row.IfName, row.Status, row.Algorithm, row.Unit,
			strconv.FormatUint(row.InBytes, 10), strconv.FormatUint(row.OutBytes, 10), strconv.FormatUint(row.SelectedBytes, 10), strconv.FormatUint(row.Rate95thBPS, 10), strconv.FormatUint(row.RateAverageBPS, 10),
			strconv.FormatUint(row.AlgorithmValue, 10), strconv.FormatUint(row.LedgerEffectiveValue, 10), strconv.FormatFloat(row.Coverage, 'g', -1, 64),
			strconv.FormatUint(uint64(row.ExpectedBuckets), 10), strconv.FormatUint(uint64(row.ObservedBuckets), 10), strconv.FormatUint(uint64(row.MissingBuckets), 10), strconv.FormatUint(uint64(row.ResetBuckets), 10),
			strconv.FormatUint(uint64(row.GapBuckets), 10), strconv.FormatUint(row.UnknownSampling, 10), strconv.FormatUint(row.SourceGenerationMin, 10), strconv.FormatUint(row.SourceGenerationMax, 10),
			strconv.FormatInt(row.SignedAmount, 10), strconv.FormatInt(row.ExpectedValue, 10), strconv.FormatInt(row.ActualValue, 10), strconv.FormatInt(row.DeltaValue, 10), strconv.FormatUint(row.ThresholdValue, 10),
			strconv.FormatFloat(row.ThresholdPercent, 'g', -1, 64), strconv.FormatUint(row.Allowed, 10), strconv.FormatUint(row.Used, 10), strconv.FormatUint(row.Overuse, 10),
			row.Reason, row.EvidenceRef, row.ProvenanceJSON, row.DateFrom, row.DateTo, row.Timezone, row.CreatedBy, row.ApprovedBy, row.ApprovedAt, row.ClosedBy, row.ClosedAt, row.CreatedAt,
		}
		for index := range fields {
			fields[index] = spreadsheetSafe(fields[index])
		}
		if err := writer.Write(fields); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	return output.Bytes(), writer.Error()
}

func renderBillingParquet(rows []billingEvidenceRow) ([]byte, error) {
	var output bytes.Buffer
	if err := parquet.Write(&output, rows); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func spreadsheetSafe(value string) string {
	if value != "" && strings.ContainsRune("=+-@", rune(value[0])) {
		return "'" + value
	}
	return value
}

func (s *Server) createBillingExport(c *gin.Context) {
	period, ok := s.billingPeriodAccess(c)
	if !ok {
		return
	}
	var request struct {
		Format             string `json:"format"`
		CalculationVersion uint64 `json:"calculation_version"`
		IdempotencyKey     string `json:"idempotency_key"`
	}
	if !addressDecodeStrict(c, &request, 16<<10) {
		return
	}
	request.Format = strings.ToLower(strings.TrimSpace(request.Format))
	if request.Format != "csv" && request.Format != "parquet" {
		fail(c, http.StatusBadRequest, "invalid_request", "format must be csv or parquet")
		return
	}
	if request.CalculationVersion == 0 {
		request.CalculationVersion = period.CalculationVersion
	}
	if request.CalculationVersion == 0 || request.CalculationVersion > period.CalculationVersion {
		fail(c, http.StatusBadRequest, "invalid_request", "calculation_version is not available")
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		idempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	}
	if idempotencyKey == "" {
		idempotencyKey = newID()
	}
	payload, err := opjob.EncodePayload(billingExportPayloadSchema, billingExportPayload{PeriodID: period.ID, CalculationVersion: request.CalculationVersion, Format: request.Format})
	if err != nil {
		writeSQLError(c, err)
		return
	}
	job, err := s.jobs.Enqueue(c, opjob.Job{JobType: billingExportJobType, IdempotencyKey: sha256hex(currentPrincipal(c).UserID + "\x00" + idempotencyKey),
		RequestHash: sha256hex(string(payload)), CheckpointJSON: payload, CreatedBy: currentPrincipal(c).UserID})
	if errors.Is(err, opjob.ErrHashMismatch) {
		fail(c, http.StatusConflict, "idempotency_conflict", err.Error())
		return
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("Location", "/api/v1/billing/exports/"+job.ID)
	c.JSON(http.StatusAccepted, job)
}

func (s *Server) runBillingExport(ctx context.Context, job opjob.Job) (string, error) {
	var payload billingExportPayload
	if err := opjob.DecodePayload(job.CheckpointJSON, billingExportPayloadSchema, &payload); err != nil {
		return "", err
	}
	if payload.PeriodID == "" || payload.CalculationVersion == 0 || (payload.Format != "csv" && payload.Format != "parquet") {
		return "", opjob.TerminalError(errors.New("invalid billing export payload"))
	}
	abilities, admin, err := loadPrincipalAbilities(ctx, s.db, job.CreatedBy)
	if err != nil || (!admin && !abilities["bill.export"]) {
		return "", opjob.TerminalError(errors.New("billing export permission was revoked"))
	}
	period, err := s.billingStore.GetPeriod(ctx, payload.PeriodID)
	if err != nil {
		return "", opjob.TerminalError(err)
	}
	if !billingAccountAllowed(ctx, s.db, job.CreatedBy, period.AccountID, admin, abilities["bill.viewAll"]) {
		return "", opjob.TerminalError(errors.New("billing account access was revoked"))
	}
	evidence, err := s.billingStore.ExportEvidence(ctx, period.ID, payload.CalculationVersion)
	if err != nil {
		return "", classifyBillingJobError(err)
	}
	rows := billingEvidenceRows(evidence)
	var data []byte
	if payload.Format == "csv" {
		data, err = renderBillingCSV(rows)
	} else {
		data, err = renderBillingParquet(rows)
	}
	if err != nil {
		return "", err
	}
	if reporter := opjob.ReporterFromContext(ctx); reporter != nil {
		if err := reporter.Report(ctx, uint64(len(rows)), nil); err != nil {
			return "", err
		}
	}
	dir := strings.TrimSpace(s.cfg.Billing.ExportDir)
	if dir == "" {
		dir = "data/billing-exports"
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	filename := job.ID + "-" + hex.EncodeToString(digest[:]) + "." + payload.Format
	if err := writeBillingArtifact(dir, filename, data); err != nil {
		return "", err
	}
	s.audit(ctx, job.CreatedBy, "billing.period.export", "billing_period", period.ID)
	return "billing-export/" + filename, nil
}

func writeBillingArtifact(dir, filename string, data []byte) error {
	temporary, err := os.CreateTemp(dir, ".billing-export-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(name)
		}
	}()
	if err := temporary.Chmod(0600); err != nil {
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, filepath.Join(dir, filename)); err != nil {
		return err
	}
	keep = true
	return nil
}

func (s *Server) listBillingExports(c *gin.Context) {
	page, ok := billingPage(c, "limit", "offset", "q", "status", "sort", "order")
	if !ok {
		return
	}
	p := currentPrincipal(c)
	filter := opjob.Filter{JobType: billingExportJobType, Status: page.Status, Search: page.Query, Limit: page.Limit, Offset: page.Offset, Ascending: strings.EqualFold(page.Order, "asc")}
	if !p.can("bill.viewAll") {
		filter.CreatedBy = p.UserID
	}
	items, err := s.jobs.List(c, filter)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	total, err := s.jobs.Count(c, filter)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) getBillingExport(c *gin.Context) {
	job, ok := s.billingExportJob(c)
	if !ok {
		return
	}
	c.Header("ETag", etag(job.RowVersion))
	c.JSON(http.StatusOK, job)
}

func (s *Server) billingExportJob(c *gin.Context) (opjob.Job, bool) {
	job, err := s.jobs.Get(c, c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return opjob.Job{}, false
	}
	p := currentPrincipal(c)
	if job.JobType != billingExportJobType || (job.CreatedBy != p.UserID && !p.can("bill.viewAll")) {
		fail(c, http.StatusNotFound, "not_found", "billing export not found")
		return opjob.Job{}, false
	}
	return job, true
}

func (s *Server) cancelBillingExport(c *gin.Context) {
	job, ok := s.billingExportJob(c)
	if !ok {
		return
	}
	if err := s.jobs.RequestCancel(c, job.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	job, _ = s.jobs.Get(c, job.ID)
	c.JSON(http.StatusAccepted, job)
}

func (s *Server) downloadBillingExport(c *gin.Context) {
	job, ok := s.billingExportJob(c)
	if !ok {
		return
	}
	if job.Status != opjob.StatusSucceeded || !strings.HasPrefix(job.ResultRef, "billing-export/") {
		fail(c, http.StatusConflict, "export_not_ready", "billing export is not ready")
		return
	}
	retention := s.cfg.Billing.ExportRetention
	if retention <= 0 {
		retention = 7 * 24 * time.Hour
	}
	if job.FinishedAt.IsZero() || time.Now().UTC().After(job.FinishedAt.Add(retention)) {
		fail(c, http.StatusGone, "export_expired", "billing export artifact has expired")
		return
	}
	filename := filepath.Base(strings.TrimPrefix(job.ResultRef, "billing-export/"))
	if !strings.HasPrefix(filename, job.ID+"-") || (filepath.Ext(filename) != ".csv" && filepath.Ext(filename) != ".parquet") {
		fail(c, http.StatusInternalServerError, "invalid_export_ref", "billing export reference is invalid")
		return
	}
	dir := strings.TrimSpace(s.cfg.Billing.ExportDir)
	if dir == "" {
		dir = "data/billing-exports"
	}
	file, err := os.Open(filepath.Join(dir, filename))
	if err != nil {
		fail(c, http.StatusGone, "export_expired", "billing export artifact is unavailable")
		return
	}
	defer file.Close()
	digestText := strings.TrimSuffix(strings.TrimPrefix(filename, job.ID+"-"), filepath.Ext(filename))
	wantDigest, err := hex.DecodeString(digestText)
	if err != nil || len(wantDigest) != sha256.Size {
		fail(c, http.StatusInternalServerError, "invalid_export_ref", "billing export checksum is invalid")
		return
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil || !bytes.Equal(hasher.Sum(nil), wantDigest) {
		fail(c, http.StatusConflict, "export_corrupt", "billing export checksum validation failed")
		return
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		writeSQLError(c, err)
		return
	}
	contentType := "text/csv; charset=utf-8"
	if filepath.Ext(filename) == ".parquet" {
		contentType = "application/vnd.apache.parquet"
	}
	c.Header("Content-Type", contentType)
	c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, filename))
	_, _ = io.Copy(c.Writer, file)
}
