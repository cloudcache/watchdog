package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
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

	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/gin-gonic/gin"
)

const (
	snmpCSVExportJobType       = "snmp.aggregate.csv"
	snmpCSVExportPayloadSchema = 1
)

type snmpCSVExportPayload struct {
	DeviceIDs   []string `json:"device_ids,omitempty"`
	PortIDs     []string `json:"port_ids,omitempty"`
	Metric      string   `json:"metric"`
	Aggregate   string   `json:"aggregate"`
	FromMS      int64    `json:"from_ms"`
	ToMS        int64    `json:"to_ms"`
	StepSeconds uint32   `json:"step_seconds"`
	MaxRows     uint32   `json:"max_rows"`
}

type createSNMPExportRequest struct {
	DeviceIDs      []string `json:"device_ids"`
	PortIDs        []string `json:"port_ids"`
	Metric         string   `json:"metric"`
	Aggregate      string   `json:"aggregate"`
	Start          string   `json:"start"`
	End            string   `json:"end"`
	StepSeconds    uint32   `json:"step_seconds"`
	MaxRows        uint32   `json:"max_rows"`
	IdempotencyKey string   `json:"idempotency_key"`
}

type snmpExportResponse struct {
	ID             string `json:"id"`
	OperationJobID string `json:"operation_job_id"`
	Status         string `json:"status"`
	Metric         string `json:"metric,omitempty"`
	Aggregation    string `json:"aggregation,omitempty"`
	RangeStart     string `json:"range_start,omitempty"`
	RangeEnd       string `json:"range_end,omitempty"`
	Step           int64  `json:"step,omitempty"`
	Format         string `json:"format"`
	ValueMode      string `json:"value_mode"`
	ValueLayer     string `json:"value_layer"`
	RowCount       uint64 `json:"row_count"`
	Checksum       string `json:"checksum,omitempty"`
	SizeBytes      int64  `json:"size_bytes,omitempty"`
	ExpiresAt      string `json:"expires_at,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

func (s *Server) startSNMPExports() error {
	if s.snmpMetrics == nil {
		return nil
	}
	dir := strings.TrimSpace(s.cfg.SNMP.ExportDir)
	if dir == "" {
		dir = "data/snmp-exports"
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.snmpExportCancel = cancel
	worker := &opjob.Worker{
		Repo: s.jobs, JobType: snmpCSVExportJobType, Owner: "watchdog-server/snmp-export",
		Handler: s.runSNMPCSVExport(dir),
	}
	go worker.Run(ctx)
	return nil
}

func (s *Server) createSNMPExport(c *gin.Context) {
	if s.snmpMetrics == nil || s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "clickhouse_unavailable", "SNMP ClickHouse store is not configured")
		return
	}
	var req createSNMPExportRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	req.Metric = strings.TrimSpace(req.Metric)
	req.Aggregate = strings.TrimSpace(req.Aggregate)
	if req.Aggregate == "" {
		req.Aggregate = "sum"
	}
	if !isSNMPMetric(req.Metric) || !validSNMPAggregateMethod(req.Aggregate) {
		fail(c, http.StatusBadRequest, "invalid_request", "metric or aggregate is unsupported")
		return
	}
	if !s.requireMetricAccess(c, req.Metric) {
		return
	}
	from, err := time.Parse(time.RFC3339, req.Start)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_range", "start must be RFC3339")
		return
	}
	to, err := time.Parse(time.RFC3339, req.End)
	if err != nil || !to.After(from) || to.Sub(from) > 400*24*time.Hour {
		fail(c, http.StatusBadRequest, "invalid_range", "end must be RFC3339 after start and the range at most 400 days")
		return
	}
	if req.StepSeconds == 0 || req.StepSeconds > 86400 {
		fail(c, http.StatusBadRequest, "invalid_range", "step_seconds must be 1..86400")
		return
	}
	if req.MaxRows == 0 {
		req.MaxRows = 250000
	}
	if req.MaxRows > 250000 {
		fail(c, http.StatusBadRequest, "invalid_range", "max_rows must be 1..250000")
		return
	}
	deviceIDs, err := parseSNMPIDs(strings.Join(req.DeviceIDs, ","))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_resource", err.Error())
		return
	}
	portIDs, err := parseSNMPIDs(strings.Join(req.PortIDs, ","))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_resource", err.Error())
		return
	}
	if _, err := s.resolveSNMPScopes(c.Request.Context(), currentPrincipal(c), deviceIDs, portIDs); err != nil {
		writeSNMPScopeError(c, err)
		return
	}
	payload := snmpCSVExportPayload{
		DeviceIDs: deviceIDs, PortIDs: portIDs, Metric: req.Metric, Aggregate: req.Aggregate,
		FromMS: from.UTC().UnixMilli(), ToMS: to.UTC().UnixMilli(), StepSeconds: req.StepSeconds, MaxRows: req.MaxRows,
	}
	checkpoint, err := opjob.EncodePayload(snmpCSVExportPayloadSchema, payload)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	if header := strings.TrimSpace(c.GetHeader("Idempotency-Key")); header != "" {
		idempotencyKey = header
	}
	if idempotencyKey == "" {
		idempotencyKey = newID()
	}
	if len(idempotencyKey) > 190 {
		fail(c, http.StatusBadRequest, "invalid_idempotency_key", "idempotency key exceeds 190 characters")
		return
	}
	job, err := s.jobs.Enqueue(c.Request.Context(), opjob.Job{
		JobType: snmpCSVExportJobType, IdempotencyKey: sha256hex(currentPrincipal(c).UserID + "\x00" + idempotencyKey),
		RequestHash: sha256hex(string(checkpoint)), CheckpointJSON: checkpoint,
		CreatedBy: currentPrincipal(c).UserID,
	})
	if errors.Is(err, opjob.ErrHashMismatch) {
		fail(c, http.StatusConflict, "idempotency_conflict", err.Error())
		return
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "snmp.export.create", "operation_job", job.ID)
	c.JSON(http.StatusAccepted, s.snmpExportView(job))
}

func (s *Server) runSNMPCSVExport(dir string) opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		var payload snmpCSVExportPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, snmpCSVExportPayloadSchema, &payload); err != nil {
			return "", err
		}
		if !isSNMPMetric(payload.Metric) || !validSNMPAggregateMethod(payload.Aggregate) || payload.StepSeconds == 0 || payload.StepSeconds > 86400 || payload.MaxRows == 0 || payload.MaxRows > 250000 {
			return "", opjob.TerminalError(errors.New("invalid SNMP CSV export payload"))
		}
		abilities, isAdmin, err := loadPrincipalAbilities(ctx, s.db, job.CreatedBy)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", opjob.TerminalError(errSNMPScopeForbidden)
			}
			return "", err
		}
		p := &principal{UserID: job.CreatedBy, IsAdmin: isAdmin, Abilities: abilities}
		allowed, err := s.metricAllowed(ctx, p, payload.Metric)
		if err != nil {
			return "", err
		}
		if !allowed {
			return "", opjob.TerminalError(errMetricScopeForbidden)
		}
		scopes, err := s.resolveSNMPScopes(ctx, p, payload.DeviceIDs, payload.PortIDs)
		if err != nil {
			if errors.Is(err, errSNMPScopeForbidden) || errors.Is(err, errSNMPScopeNotFound) {
				return "", opjob.TerminalError(err)
			}
			return "", err
		}
		result, err := s.snmpMetrics.Aggregate(ctx, snmpch.AggregateRequest{
			Scopes: scopes, Metric: payload.Metric, Method: payload.Aggregate,
			From: time.UnixMilli(payload.FromMS).UTC(), To: time.UnixMilli(payload.ToMS).UTC(),
			Step: time.Duration(payload.StepSeconds) * time.Second, MaxRows: payload.MaxRows,
		})
		if err != nil {
			return "", err
		}
		return writeSNMPCSVArtifact(ctx, dir, job.ID, result, opjob.ReporterFromContext(ctx))
	}
}

func writeSNMPCSVArtifact(ctx context.Context, dir, jobID string, result snmpch.AggregateResult, reporter *opjob.Reporter) (string, error) {
	temporary, err := os.CreateTemp(dir, ".snmp-export-*")
	if err != nil {
		return "", err
	}
	temporaryName := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0600); err != nil {
		return "", err
	}
	hash := sha256.New()
	writer := csv.NewWriter(io.MultiWriter(temporary, hash))
	if err := writer.Write([]string{"bucket_start", "metric", "aggregate", "value"}); err != nil {
		return "", err
	}
	for index, point := range result.Points {
		if index%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			if err := reporter.Report(ctx, uint64(index), nil); err != nil {
				return "", err
			}
		}
		if err := writer.Write([]string{point.Time.UTC().Format(time.RFC3339Nano), result.Metric, result.Method, strconv.FormatFloat(point.Value, 'g', -1, 64)}); err != nil {
			return "", err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	filename := jobID + "-" + digest + ".csv"
	if err := os.Rename(temporaryName, filepath.Join(dir, filename)); err != nil {
		return "", err
	}
	keep = true
	if err := reporter.Report(ctx, uint64(len(result.Points)), nil); err != nil {
		_ = os.Remove(filepath.Join(dir, filename))
		return "", err
	}
	return "snmp-csv/" + filename, nil
}

func (s *Server) listSNMPExports(c *gin.Context) {
	for name := range c.Request.URL.Query() {
		switch name {
		case "limit", "offset", "status", "q", "value_layer", "format", "sort_by", "sort_direction":
		default:
			fail(c, http.StatusBadRequest, "invalid_filter", "unsupported query parameter: "+name)
			return
		}
	}
	limit, offset := pageParams(c)
	search := strings.TrimSpace(c.Query("q"))
	if len(search) > 128 {
		fail(c, http.StatusBadRequest, "invalid_filter", "q exceeds 128 characters")
		return
	}
	valueLayer := strings.TrimSpace(c.Query("value_layer"))
	format := strings.TrimSpace(c.Query("format"))
	if (valueLayer != "" && valueLayer != "all" && valueLayer != "raw") || (format != "" && format != "all" && format != "csv") {
		c.JSON(http.StatusOK, gin.H{"items": []snmpExportResponse{}, "total": 0, "limit": limit, "offset": offset})
		return
	}
	sortBy := strings.TrimSpace(c.DefaultQuery("sort_by", "created_at"))
	sortDirection := strings.TrimSpace(c.DefaultQuery("sort_direction", "desc"))
	if sortBy != "created_at" || (sortDirection != "asc" && sortDirection != "desc") {
		fail(c, http.StatusBadRequest, "invalid_sort", "exports sort supports created_at asc|desc")
		return
	}
	status := strings.TrimSpace(c.Query("status"))
	if status == "complete" {
		status = opjob.StatusSucceeded
	}
	if status == "pending" {
		status = opjob.StatusQueued
	}
	if status != "" && status != "all" && !validOperationJobStatus(status) {
		fail(c, http.StatusBadRequest, "invalid_filter", "invalid export status")
		return
	}
	if status == "all" {
		status = ""
	}
	filter := opjob.Filter{JobType: snmpCSVExportJobType, Status: status, Search: search, Ascending: sortDirection == "asc", Limit: limit, Offset: offset}
	p := currentPrincipal(c)
	if !p.IsAdmin {
		filter.CreatedBy = p.UserID
	}
	jobs, err := s.jobs.List(c.Request.Context(), filter)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	total, err := s.jobs.Count(c.Request.Context(), filter)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	items := make([]snmpExportResponse, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, s.snmpExportView(job))
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

func (s *Server) getSNMPExport(c *gin.Context) {
	job, ok := s.authorizedSNMPExport(c)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, s.snmpExportView(job))
}

func (s *Server) cancelSNMPExport(c *gin.Context) {
	job, ok := s.authorizedSNMPExport(c)
	if !ok {
		return
	}
	if err := s.jobs.RequestCancel(c.Request.Context(), job.ID); err != nil {
		writeSQLError(c, err)
		return
	}
	job, err := s.jobs.Get(c.Request.Context(), job.ID)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "snmp.export.cancel", "operation_job", job.ID)
	c.JSON(http.StatusOK, s.snmpExportView(job))
}

func (s *Server) downloadSNMPExport(c *gin.Context) {
	job, ok := s.authorizedSNMPExport(c)
	if !ok {
		return
	}
	if job.Status != opjob.StatusSucceeded {
		fail(c, http.StatusConflict, "export_not_ready", "export is not complete")
		return
	}
	retention := s.cfg.SNMP.ExportRetention
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	if job.FinishedAt.IsZero() || time.Now().UTC().After(job.FinishedAt.Add(retention)) {
		fail(c, http.StatusGone, "export_expired", "export artifact has expired")
		return
	}
	path, checksum, err := s.snmpExportArtifact(job)
	if err != nil {
		fail(c, http.StatusConflict, "export_corrupt", err.Error())
		return
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			fail(c, http.StatusGone, "export_missing", "export artifact is missing")
			return
		}
		writeSQLError(c, err)
		return
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil || hex.EncodeToString(hash.Sum(nil)) != checksum {
		fail(c, http.StatusConflict, "export_corrupt", "export checksum validation failed")
		return
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		writeSQLError(c, err)
		return
	}
	stat, err := file.Stat()
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="watchdog-snmp-%s.csv"`, job.ID))
	c.DataFromReader(http.StatusOK, stat.Size(), "text/csv; charset=utf-8", file, nil)
}

func (s *Server) authorizedSNMPExport(c *gin.Context) (opjob.Job, bool) {
	job, err := s.jobs.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return opjob.Job{}, false
	}
	p := currentPrincipal(c)
	if job.JobType != snmpCSVExportJobType || (!p.IsAdmin && job.CreatedBy != p.UserID) {
		fail(c, http.StatusNotFound, "not_found", "export not found")
		return opjob.Job{}, false
	}
	if !p.IsAdmin {
		var payload snmpCSVExportPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, snmpCSVExportPayloadSchema, &payload); err != nil {
			fail(c, http.StatusConflict, "export_corrupt", "export payload is invalid")
			return opjob.Job{}, false
		}
		if _, err := s.resolveSNMPScopes(c.Request.Context(), p, payload.DeviceIDs, payload.PortIDs); err != nil {
			writeSNMPScopeError(c, err)
			return opjob.Job{}, false
		}
	}
	return job, true
}

func (s *Server) snmpExportView(job opjob.Job) snmpExportResponse {
	view := snmpExportResponse{
		ID: job.ID, OperationJobID: job.ID, Status: exportStatus(job.Status), Format: "csv",
		ValueMode: "raw", ValueLayer: "raw", RowCount: job.ProgressDone,
		CreatedAt: job.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: job.CreatedAt.UTC().Format(time.RFC3339Nano),
		ErrorMessage: job.LastErrorDetail,
	}
	var payload snmpCSVExportPayload
	if opjob.DecodePayload(job.CheckpointJSON, snmpCSVExportPayloadSchema, &payload) == nil {
		view.Metric, view.Aggregation = payload.Metric, payload.Aggregate
		view.RangeStart = time.UnixMilli(payload.FromMS).UTC().Format(time.RFC3339Nano)
		view.RangeEnd = time.UnixMilli(payload.ToMS).UTC().Format(time.RFC3339Nano)
		view.Step = int64(payload.StepSeconds) * int64(time.Second)
	}
	retention := s.cfg.SNMP.ExportRetention
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	if !job.FinishedAt.IsZero() {
		view.UpdatedAt = job.FinishedAt.UTC().Format(time.RFC3339Nano)
		view.ExpiresAt = job.FinishedAt.Add(retention).UTC().Format(time.RFC3339Nano)
	}
	if path, checksum, err := s.snmpExportArtifact(job); err == nil {
		view.Checksum = checksum
		if stat, statErr := os.Stat(path); statErr == nil {
			view.SizeBytes = stat.Size()
		}
	}
	return view
}

func (s *Server) snmpExportArtifact(job opjob.Job) (string, string, error) {
	const prefix = "snmp-csv/"
	if !strings.HasPrefix(job.ResultRef, prefix) {
		return "", "", errors.New("invalid SNMP export result reference")
	}
	filename := strings.TrimPrefix(job.ResultRef, prefix)
	if filepath.Base(filename) != filename || !strings.HasPrefix(filename, job.ID+"-") || !strings.HasSuffix(filename, ".csv") {
		return "", "", errors.New("invalid SNMP export result reference")
	}
	checksum := strings.TrimSuffix(strings.TrimPrefix(filename, job.ID+"-"), ".csv")
	if len(checksum) != sha256.Size*2 {
		return "", "", errors.New("invalid SNMP export checksum")
	}
	if _, err := hex.DecodeString(checksum); err != nil {
		return "", "", errors.New("invalid SNMP export checksum")
	}
	dir := strings.TrimSpace(s.cfg.SNMP.ExportDir)
	if dir == "" {
		dir = "data/snmp-exports"
	}
	return filepath.Join(dir, filename), checksum, nil
}

func exportStatus(status string) string {
	switch status {
	case opjob.StatusQueued:
		return "pending"
	case opjob.StatusSucceeded:
		return "complete"
	default:
		return status
	}
}

func validOperationJobStatus(status string) bool {
	switch status {
	case opjob.StatusQueued, opjob.StatusRunning, opjob.StatusCancelRequested, opjob.StatusSucceeded, opjob.StatusFailed, opjob.StatusCanceled:
		return true
	}
	return false
}
