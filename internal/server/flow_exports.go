// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
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
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
	"github.com/parquet-go/parquet-go"
)

// flow_exports.go migrates the hub's flow export to the v2 opjob async pattern
// (KISS-06 gate 3). One /flow/exports endpoint serves three flavors discriminated
// by kind: detail (raw/supplier records, paged over the DetailRunner), query
// (aggregate/joint Explorer results), and report (a fixed report's panels). Like
// the reports layer, the hub's QueryGateway export orchestration (ExportTask,
// canonical snapshots, gateway data provider) is retired: the create handler
// authorizes then enqueues an opjob; the worker reloads the subject, re-authorizes,
// re-runs the typed query, and renders CSV/Parquet. Row extraction and renderers
// are a faithful de-tenanted port (see flow_export_rows.go for the query/report
// row model).

const (
	flowExportJobType               = "flow.export"
	flowExportPayloadSchema         = 1
	maxFlowDetailExportRows  uint32 = 250_000
	flowDetailExportPageSize uint16 = 500
	flowExportArtifactPrefix        = "flow-export/"

	flowExportKindDetail      = "detail"
	flowExportKindQuery       = "query"
	flowExportKindReport      = "report"
	flowExportKindVPNFindings = "vpn_findings"
)

// flowExportPayload is the discriminated persisted spec. Detail exports carry the
// frozen detail projection; query/report exports carry the typed request re-run in
// the worker and the authorized view.
type flowExportPayload struct {
	Kind        string                   `json:"kind"`
	Format      string                   `json:"format"`
	MaxRows     uint32                   `json:"max_rows"`
	View        flowquery.View           `json:"view,omitempty"`
	Detail      *flowquery.DetailRequest `json:"detail,omitempty"`
	Query       *flowAggregateInput      `json:"query,omitempty"`
	Report      *flowReportRequest       `json:"report,omitempty"`
	VPNFindings *vpnFindingsExportSpec   `json:"vpn_findings,omitempty"`
}

// createFlowExportRequest is the hub POST /flow/exports contract: {query, format}
// where `query` is a QueryRequest envelope. A report export carries
// query.parameters.report; otherwise it is an aggregate/joint query export. The
// detail export is a separate endpoint (/flow/records/exports).
type createFlowExportRequest struct {
	Query            flowExportQueryEnvelope `json:"query"`
	Format           string                  `json:"format"`
	RetentionSeconds uint32                  `json:"retention_seconds,omitempty"`
	IdempotencyKey   string                  `json:"idempotency_key,omitempty"`
}

type flowExportResponse struct {
	ID             string `json:"id"`
	OperationJobID string `json:"operation_job_id"`
	Status         string `json:"status"`
	Kind           string `json:"kind,omitempty"`
	View           string `json:"view,omitempty"`
	Format         string `json:"format"`
	RangeStart     string `json:"range_start,omitempty"`
	RangeEnd       string `json:"range_end,omitempty"`
	RowCount       uint64 `json:"row_count"`
	MaxRows        uint32 `json:"max_rows,omitempty"`
	Checksum       string `json:"checksum,omitempty"`
	SizeBytes      int64  `json:"size_bytes,omitempty"`
	ExpiresAt      string `json:"expires_at,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

func (s *Server) flowExportDir() string {
	dir := strings.TrimSpace(s.cfg.Flow.Export.Dir)
	if dir == "" {
		dir = "data/flow-exports"
	}
	return dir
}

func (s *Server) flowExportRetention() time.Duration {
	if s.cfg.Flow.Export.Retention > 0 {
		return s.cfg.Flow.Export.Retention
	}
	return 24 * time.Hour
}

func (s *Server) startFlowExports() error {
	if s.flowQuery == nil || s.jobs == nil {
		return nil
	}
	dir := s.flowExportDir()
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	queryDir := strings.TrimSpace(s.cfg.Flow.Query.AsyncResultDir)
	if queryDir == "" {
		return errors.New("flow query async result directory is required")
	}
	if err := os.MkdirAll(queryDir, 0700); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.flowExportCancel = cancel
	exportWorker := &opjob.Worker{
		Repo: s.jobs, JobType: flowExportJobType, Owner: "watchdog-server/flow-export",
		Handler: s.runFlowExport(dir),
	}
	queryWorker := &opjob.Worker{
		Repo: s.jobs, JobType: flowReportQueryJobType, Owner: "watchdog-server/flow-report-query",
		Handler: s.runFlowReportQuery(queryDir), PollInterval: s.cfg.Flow.Query.AsyncWorkerPoll,
		LeaseFor: s.cfg.Flow.Query.AsyncWorkerLease, MaxAttempts: s.cfg.Flow.Query.AsyncWorkerMaxAttempts,
		RetryBase: s.cfg.Flow.Query.AsyncWorkerRetryBase,
	}
	go exportWorker.Run(ctx)
	go queryWorker.Run(ctx)
	return nil
}

// registerFlowExportRoutes wires the flow export lifecycle under /flow/exports. One
// endpoint serves all three flavors (detail records, aggregate/joint query, fixed
// report), discriminated by the request "kind".
func (s *Server) registerFlowExportRoutes(auth *gin.RouterGroup) {
	view := s.requirePermission("flow.view.customer")
	exports := auth.Group("/flow/exports", view)
	exports.POST("", s.createFlowExport)
	exports.GET("", s.listFlowExports)
	exports.GET("/:id", s.getFlowExport)
	exports.POST("/:id/cancel", s.cancelFlowExport)
	exports.GET("/:id/download", s.downloadFlowExport)
	// Detail (raw/supplier record) export keeps its own hub path.
	auth.POST("/flow/records/exports", view, s.createFlowRecordsExport)
}

func (s *Server) createFlowExport(c *gin.Context) {
	if s.flowQuery == nil || s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "flow_query_unavailable", "flow ClickHouse store is not configured")
		return
	}
	var req createFlowExportRequest
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	format := strings.ToLower(strings.TrimSpace(req.Format))
	if format == "" {
		format = "csv"
	}
	if format != "csv" && format != "parquet" {
		fail(c, http.StatusBadRequest, "invalid_request", "format must be csv or parquet")
		return
	}
	var payload flowExportPayload
	var ok bool
	if req.Query.Parameters.Report != nil {
		payload, ok = s.buildReportExportPayload(c, req.Query.toReportRequest(), format)
	} else {
		payload, ok = s.buildQueryExportPayload(c, req.Query.toAggregateInput(), format)
	}
	if !ok {
		return
	}
	s.enqueueFlowExport(c, payload, req.IdempotencyKey)
}

// createFlowRecordsExport is the hub POST /flow/records/exports contract: a detail
// (raw/supplier record) export of {query: DetailRequest, format, max_rows}.
func (s *Server) createFlowRecordsExport(c *gin.Context) {
	if s.flowQuery == nil || s.jobs == nil {
		fail(c, http.StatusServiceUnavailable, "flow_query_unavailable", "flow ClickHouse store is not configured")
		return
	}
	var req struct {
		Query            flowquery.DetailRequest `json:"query"`
		Format           string                  `json:"format"`
		RetentionSeconds uint32                  `json:"retention_seconds,omitempty"`
		MaxRows          uint32                  `json:"max_rows,omitempty"`
		IdempotencyKey   string                  `json:"idempotency_key,omitempty"`
	}
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	format := strings.ToLower(strings.TrimSpace(req.Format))
	if format == "" {
		format = "csv"
	}
	if format != "csv" && format != "parquet" {
		fail(c, http.StatusBadRequest, "invalid_request", "format must be csv or parquet")
		return
	}
	payload, ok := s.buildDetailExportPayload(c, req.Query, format, req.MaxRows)
	if !ok {
		return
	}
	s.enqueueFlowExport(c, payload, req.IdempotencyKey)
}

// enqueueFlowExport encodes a validated payload, applies the idempotency key, and
// enqueues the flow.export opjob. Shared by every flow-export create handler.
func (s *Server) enqueueFlowExport(c *gin.Context, payload flowExportPayload, idempotencyKey string) {
	checkpoint, err := opjob.EncodePayload(flowExportPayloadSchema, payload)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	key := strings.TrimSpace(idempotencyKey)
	if header := strings.TrimSpace(c.GetHeader("Idempotency-Key")); header != "" {
		key = header
	}
	if key == "" {
		key = newID()
	}
	if len(key) > 190 {
		fail(c, http.StatusBadRequest, "invalid_idempotency_key", "idempotency key exceeds 190 characters")
		return
	}
	job, err := s.jobs.Enqueue(c.Request.Context(), opjob.Job{
		JobType: flowExportJobType, IdempotencyKey: sha256hex(currentPrincipal(c).UserID + "\x00" + key),
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
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow.export.create", "operation_job", job.ID)
	c.JSON(http.StatusAccepted, s.flowExportView(job))
}

// buildDetailExportPayload validates a detail (raw/supplier record) export and
// freezes its compiler-resolved projection.
func (s *Server) buildDetailExportPayload(c *gin.Context, detail flowquery.DetailRequest, format string, maxRows uint32) (flowExportPayload, bool) {
	if maxRows == 0 {
		maxRows = maxFlowDetailExportRows
	}
	if maxRows > maxFlowDetailExportRows {
		fail(c, http.StatusBadRequest, "invalid_request", "max_rows must be 1..250000")
		return flowExportPayload{}, false
	}
	// Detail exports resolve one immutable, replayable provenance layer; the
	// customer layer is a live rollup projection, not exportable at record grain.
	view, ok := s.authorizeFlowView(c, detail.View)
	if !ok {
		return flowExportPayload{}, false
	}
	if view != flowquery.ViewRaw && view != flowquery.ViewSupplier {
		fail(c, http.StatusBadRequest, "invalid_request", "flow detail export supports the raw or supplier view")
		return flowExportPayload{}, false
	}
	if strings.TrimSpace(detail.Cursor) != "" {
		fail(c, http.StatusBadRequest, "invalid_request", "flow detail export cursor must be empty")
		return flowExportPayload{}, false
	}
	targetIDs, deviceIDs, exporterIDs := flowDetailResourceFilters(detail.Filters, detail.ColumnFilters)
	if !s.authorizeFlowResourceFilters(c, targetIDs, deviceIDs, exporterIDs) {
		return flowExportPayload{}, false
	}
	// Freeze the compiler-resolved projection so default fields, IP spelling,
	// range and sort cannot drift between creation and execution.
	detail.View = view
	detail.Limit = flowDetailExportPageSize
	compiled, err := flowquery.CompileDetail(flowquery.Scope{AllowedViews: []flowquery.View{view}}, detail, time.Now().UTC())
	if err != nil {
		writeFlowQueryError(c, err)
		return flowExportPayload{}, false
	}
	detail.IP = compiled.IP.String()
	detail.From, detail.To = compiled.From, compiled.To
	detail.Fields = append([]flowquery.DetailField(nil), compiled.Fields...)
	detail.Sort = compiled.Sort
	return flowExportPayload{Kind: flowExportKindDetail, Format: format, MaxRows: maxRows, View: view, Detail: &detail}, true
}

// buildQueryExportPayload validates an aggregate/joint query export. The export
// flattens points, so any table projection carried by the query is ignored.
func (s *Server) buildQueryExportPayload(c *gin.Context, query flowAggregateInput, format string) (flowExportPayload, bool) {
	view, ok := s.authorizeFlowView(c, query.View)
	if !ok {
		return flowExportPayload{}, false
	}
	if !s.authorizeFlowResourceFilters(c, query.Filters.TargetIDs, query.Filters.DeviceIDs, query.Filters.ExporterIDs) {
		return flowExportPayload{}, false
	}
	if query.Operator != nil && strings.TrimSpace(query.Operator.OperatorID) != "" {
		if !s.applyFlowOperatorSelection(c, query.Operator, view, query.From, query.To, &query.Filters, &query.Filter) {
			return flowExportPayload{}, false
		}
	}
	query.View = view
	query.Table = nil
	return flowExportPayload{Kind: flowExportKindQuery, Format: format, MaxRows: maxFlowDetailExportRows, View: view, Query: &query}, true
}

// buildReportExportPayload validates a fixed-report export.
func (s *Server) buildReportExportPayload(c *gin.Context, report flowReportRequest, format string) (flowExportPayload, bool) {
	if err := normalizeFlowReport(&report); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return flowExportPayload{}, false
	}
	view, ok := s.authorizeFlowView(c, report.View)
	if !ok {
		return flowExportPayload{}, false
	}
	if !s.authorizeFlowResourceFilters(c, report.Filters.TargetIDs, report.Filters.DeviceIDs, report.Filters.ExporterIDs) {
		return flowExportPayload{}, false
	}
	if report.Operator != nil && strings.TrimSpace(report.Operator.OperatorID) != "" {
		if !s.applyFlowOperatorSelection(c, report.Operator, view, report.From, report.To, &report.Filters, &report.Filter) {
			return flowExportPayload{}, false
		}
	}
	if report.Kind == flowReportVPN && !currentPrincipal(c).can("flow.vpn.view") {
		fail(c, http.StatusForbidden, "forbidden", "vpn reports require flow.vpn.view")
		return flowExportPayload{}, false
	}
	report.View = view
	return flowExportPayload{Kind: flowExportKindReport, Format: format, MaxRows: maxFlowDetailExportRows, View: view, Report: &report}, true
}

func (s *Server) runFlowExport(dir string) opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		var payload flowExportPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, flowExportPayloadSchema, &payload); err != nil {
			return "", err
		}
		if payload.Format != "csv" && payload.Format != "parquet" {
			return "", opjob.TerminalError(errors.New("invalid flow export format"))
		}
		if payload.MaxRows == 0 || payload.MaxRows > maxFlowDetailExportRows {
			return "", opjob.TerminalError(errors.New("invalid flow export row limit"))
		}
		abilities, isAdmin, err := loadPrincipalAbilities(ctx, s.db, job.CreatedBy)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", opjob.TerminalError(errors.New("flow export subject is not authorized"))
			}
			return "", err
		}
		p := &principal{UserID: job.CreatedBy, IsAdmin: isAdmin, Abilities: abilities}
		reporter := opjob.ReporterFromContext(ctx)
		var data []byte
		var rowCount int
		switch payload.Kind {
		case flowExportKindDetail:
			data, rowCount, err = s.runFlowDetailExportArtifact(ctx, p, payload, reporter)
		case flowExportKindQuery:
			data, rowCount, err = s.runFlowQueryExportArtifact(ctx, p, payload)
		case flowExportKindReport:
			data, rowCount, err = s.runFlowReportExportArtifact(ctx, p, payload)
		case flowExportKindVPNFindings:
			data, rowCount, err = s.runFlowVPNFindingsExportArtifact(ctx, p, payload)
		default:
			return "", opjob.TerminalError(errors.New("invalid flow export kind"))
		}
		if err != nil {
			return "", err
		}
		extension := "csv"
		if payload.Format == "parquet" {
			extension = "parquet"
		}
		ref, err := writeFlowExportArtifact(dir, job.ID, extension, data)
		if err != nil {
			return "", err
		}
		if err := reporter.Report(ctx, uint64(rowCount), nil); err != nil {
			_ = os.Remove(filepath.Join(dir, strings.TrimPrefix(ref, flowExportArtifactPrefix)))
			return "", err
		}
		return ref, nil
	}
}

func (s *Server) runFlowDetailExportArtifact(ctx context.Context, p *principal, payload flowExportPayload, reporter *opjob.Reporter) ([]byte, int, error) {
	if payload.Detail == nil || payload.Detail.Limit != flowDetailExportPageSize {
		return nil, 0, opjob.TerminalError(errors.New("invalid flow detail export payload"))
	}
	if payload.Detail.View != flowquery.ViewRaw && payload.Detail.View != flowquery.ViewSupplier {
		return nil, 0, opjob.TerminalError(errors.New("flow detail export view must be raw or supplier"))
	}
	targetIDs, deviceIDs, exporterIDs := flowDetailResourceFilters(payload.Detail.Filters, payload.Detail.ColumnFilters)
	if err := s.authorizeFlowExportScope(ctx, p, payload.Detail.View, targetIDs, deviceIDs, exporterIDs); err != nil {
		return nil, 0, opjob.TerminalError(err)
	}
	rows, err := s.collectFlowDetailExportRows(ctx, *payload.Detail, payload.MaxRows, reporter)
	if err != nil {
		return nil, 0, err
	}
	if payload.Format == "parquet" {
		data, err := renderFlowDetailParquet(rows)
		return data, len(rows.Rows), err
	}
	data, err := renderFlowDetailCSV(rows)
	return data, len(rows.Rows), err
}

func (s *Server) runFlowQueryExportArtifact(ctx context.Context, p *principal, payload flowExportPayload) ([]byte, int, error) {
	if payload.Query == nil {
		return nil, 0, opjob.TerminalError(errors.New("invalid flow query export payload"))
	}
	input := *payload.Query
	if err := s.authorizeFlowExportScope(ctx, p, payload.View, input.Filters.TargetIDs, input.Filters.DeviceIDs, input.Filters.ExporterIDs); err != nil {
		return nil, 0, opjob.TerminalError(err)
	}
	raw, err := s.runFlowQueryExportResult(ctx, payload.View, input)
	if err != nil {
		return nil, 0, err
	}
	rows, err := flowQueryExportRows(raw, flowExportDecoration{ValueLayer: payload.View})
	if err != nil {
		return nil, 0, err
	}
	if uint64(len(rows)) > uint64(payload.MaxRows) {
		return nil, 0, opjob.TerminalError(errors.New("flow query export exceeds its row limit"))
	}
	data, err := renderFlowExport(payload.Format, rows)
	return data, len(rows), err
}

func (s *Server) runFlowReportExportArtifact(ctx context.Context, p *principal, payload flowExportPayload) ([]byte, int, error) {
	if payload.Report == nil {
		return nil, 0, opjob.TerminalError(errors.New("invalid flow report export payload"))
	}
	report := *payload.Report
	if err := s.authorizeFlowExportScope(ctx, p, payload.View, report.Filters.TargetIDs, report.Filters.DeviceIDs, report.Filters.ExporterIDs); err != nil {
		return nil, 0, opjob.TerminalError(err)
	}
	if report.Kind == flowReportVPN && !p.can("flow.vpn.view") {
		return nil, 0, opjob.TerminalError(errors.New("vpn reports require flow.vpn.view"))
	}
	scope := flowquery.Scope{AllowedViews: []flowquery.View{payload.View}}
	panels, _, err := s.buildFlowReport(ctx, scope, payload.View, report, time.Now().UTC(), p.can("flow.vpn.view"))
	if err != nil {
		return nil, 0, err
	}
	rows, err := flowReportExportRows(panels, string(report.Kind), payload.View)
	if err != nil {
		return nil, 0, err
	}
	if uint64(len(rows)) > uint64(payload.MaxRows) {
		return nil, 0, opjob.TerminalError(errors.New("flow report export exceeds its row limit"))
	}
	data, err := renderFlowExport(payload.Format, rows)
	return data, len(rows), err
}

// runFlowQueryExportResult re-runs an aggregate/joint query and marshals its
// envelope, mirroring the queryFlow handler core (no table projection).
func (s *Server) runFlowQueryExportResult(ctx context.Context, view flowquery.View, input flowAggregateInput) (json.RawMessage, error) {
	now := time.Now().UTC()
	step := time.Duration(input.StepSeconds) * time.Second
	scope := flowquery.Scope{AllowedViews: []flowquery.View{view}}
	if len(input.Dimensions) > 0 {
		compiled, err := flowquery.CompileJoint(scope, flowquery.JointRequest{
			From: input.From, To: input.To, Interval: step, TargetPoints: input.TargetPoints,
			Metric: input.Metric, Dimensions: input.Dimensions, Filters: input.Filters, Filter: input.Filter,
			View: view, TopN: input.TopN, IncludeOther: input.IncludeOther, Timezone: input.Timezone,
			ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
		}, now)
		if err != nil {
			return nil, err
		}
		result, err := s.flowQuery.joint.Run(ctx, compiled)
		if err != nil {
			return nil, err
		}
		return marshalFlowJointResult(result, nil, s.flowGeo)
	}
	plan, err := flowquery.PlanAggregate(input.From, input.To, step, input.TargetPoints, now)
	if err != nil {
		return nil, err
	}
	queryRequest := flowquery.Request{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Interval: plan.Interval,
		Metric: input.Metric, Dimension: input.Dimension, Filters: input.Filters, Filter: input.Filter,
		View: view, TopN: input.TopN, IncludeOther: input.IncludeOther, Timezone: input.Timezone,
		ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
	}
	if err := s.applyFlowStorageBoundary(ctx, &queryRequest); err != nil {
		return nil, err
	}
	compiled, err := flowquery.Compile(scope, queryRequest, now)
	if err != nil {
		return nil, err
	}
	result, err := s.flowQuery.aggregate.Run(ctx, compiled)
	if err != nil {
		return nil, err
	}
	return marshalFlowAggregateResult(result, nil, s.flowGeo)
}

func renderFlowExport(format string, rows []flowExportRow) ([]byte, error) {
	if format == "parquet" {
		return renderFlowExportParquet(rows)
	}
	return renderFlowExportCSV(rows)
}

// authorizeFlowExportScope re-checks the export subject's view and resource scope
// without a gin context, mirroring authorizeFlow{View,ResourceFilters}.
func (s *Server) authorizeFlowExportScope(ctx context.Context, p *principal, view flowquery.View, targetIDs, deviceIDs, exporterIDs []string) error {
	if !p.can("flow.view." + string(view)) {
		return fmt.Errorf("flow %s view is not permitted", view)
	}
	if p.can("device.viewAll") {
		return nil
	}
	if len(targetIDs) > 0 || len(exporterIDs) > 0 {
		return errors.New("target- and exporter-scoped flow exports require broader device access")
	}
	for _, deviceID := range deviceIDs {
		allowed, err := s.principalCanAccessDevice(ctx, p, deviceID)
		if err != nil {
			return err
		}
		if !allowed {
			return errors.New("flow export resource permission denied")
		}
	}
	return nil
}

// flowDetailExportRows collects the full result set, paging the detail runner by
// cursor until exhausted or the frozen row cap is reached.
type flowDetailExportRows struct {
	View   flowquery.View
	Fields []flowquery.DetailField
	Rows   []flowDetailExportRow
}

type flowDetailExportRow struct {
	EventTime        time.Time
	SourceCoordinate flowquery.SourceCoordinate
	Values           []any
}

func (s *Server) collectFlowDetailExportRows(ctx context.Context, request flowquery.DetailRequest, maxRows uint32, reporter *opjob.Reporter) (flowDetailExportRows, error) {
	now := request.To
	rows := flowDetailExportRows{
		View: request.View, Fields: append([]flowquery.DetailField(nil), request.Fields...),
		Rows: make([]flowDetailExportRow, 0, flowDetailExportPageSize),
	}
	seenCursors := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return flowDetailExportRows{}, err
		}
		compiled, err := flowquery.CompileDetail(flowquery.Scope{AllowedViews: []flowquery.View{request.View}}, request, now)
		if err != nil {
			return flowDetailExportRows{}, err
		}
		page, err := s.flowQuery.detail.Run(ctx, compiled)
		if err != nil {
			return flowDetailExportRows{}, err
		}
		if page.View != request.View || !slices.Equal(page.Fields, request.Fields) || len(page.Rows) > int(request.Limit) {
			return flowDetailExportRows{}, errors.New("flow detail export runner returned an invalid page")
		}
		if request.View == flowquery.ViewSupplier && (!page.SupplierProvenanceComplete || page.MinimumFactSchema < 2) {
			return flowDetailExportRows{}, errors.New("flow supplier detail provenance is incomplete")
		}
		if uint64(len(rows.Rows)+len(page.Rows)) > uint64(maxRows) {
			return flowDetailExportRows{}, errors.New("flow detail export exceeds its frozen row limit")
		}
		for _, source := range page.Rows {
			values := make([]any, len(request.Fields))
			for index, field := range request.Fields {
				value, ok := source.Values[field]
				if !ok {
					return flowDetailExportRows{}, fmt.Errorf("flow detail export row is missing field %q", field)
				}
				values[index] = value
			}
			rows.Rows = append(rows.Rows, flowDetailExportRow{
				EventTime: source.EventTime.UTC(), SourceCoordinate: source.SourceCoordinate, Values: values,
			})
		}
		if err := reporter.Report(ctx, uint64(len(rows.Rows)), nil); err != nil {
			return flowDetailExportRows{}, err
		}
		if !page.HasMore {
			break
		}
		if page.NextCursor == "" {
			return flowDetailExportRows{}, errors.New("flow detail export page omitted its continuation cursor")
		}
		if _, exists := seenCursors[page.NextCursor]; exists {
			return flowDetailExportRows{}, errors.New("flow detail export cursor did not advance")
		}
		seenCursors[page.NextCursor] = struct{}{}
		request.Cursor = page.NextCursor
	}
	if len(rows.Rows) == 0 {
		return flowDetailExportRows{}, opjob.TerminalError(errors.New("no flow detail rows returned"))
	}
	return rows, nil
}

func renderFlowDetailCSV(rows flowDetailExportRows) ([]byte, error) {
	if err := validateFlowDetailExportRows(rows); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	header := []string{"event_time", "source_stream_id", "kafka_partition", "kafka_offset", "record_index"}
	for _, field := range rows.Fields {
		header = append(header, string(field))
	}
	if err := writer.Write(header); err != nil {
		return nil, err
	}
	for _, row := range rows.Rows {
		record := []string{
			row.EventTime.UTC().Format(time.RFC3339Nano), safeSpreadsheetCell(row.SourceCoordinate.SourceStreamID),
			strconv.FormatUint(uint64(row.SourceCoordinate.KafkaPartition), 10), strconv.FormatUint(row.SourceCoordinate.KafkaOffset, 10),
			strconv.FormatUint(uint64(row.SourceCoordinate.RecordIndex), 10),
		}
		for _, value := range row.Values {
			record = append(record, flowDetailExportText(value))
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	return output.Bytes(), writer.Error()
}

func renderFlowDetailParquet(rows flowDetailExportRows) ([]byte, error) {
	if err := validateFlowDetailExportRows(rows); err != nil {
		return nil, err
	}
	group := parquet.Group{
		"event_time": parquet.Timestamp(parquet.Millisecond), "source_stream_id": parquet.String(),
		"kafka_partition": parquet.Uint(32), "kafka_offset": parquet.Uint(64), "record_index": parquet.Uint(32),
	}
	for index, field := range rows.Fields {
		node, err := flowDetailParquetNode(rows.Rows[0].Values[index])
		if err != nil {
			return nil, fmt.Errorf("flow detail field %q: %w", field, err)
		}
		group[string(field)] = node
	}
	var output bytes.Buffer
	writer := parquet.NewGenericWriter[any](&output, parquet.NewSchema("flow_record_detail", group))
	const batchSize = 512
	for start := 0; start < len(rows.Rows); start += batchSize {
		end := min(start+batchSize, len(rows.Rows))
		batch := make([]any, 0, end-start)
		for _, row := range rows.Rows[start:end] {
			item := map[string]any{
				"event_time": row.EventTime.UTC(), "source_stream_id": row.SourceCoordinate.SourceStreamID,
				"kafka_partition": row.SourceCoordinate.KafkaPartition, "kafka_offset": row.SourceCoordinate.KafkaOffset,
				"record_index": row.SourceCoordinate.RecordIndex,
			}
			for index, field := range rows.Fields {
				item[string(field)] = row.Values[index]
			}
			batch = append(batch, item)
		}
		if _, err := writer.Write(batch); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func validateFlowDetailExportRows(rows flowDetailExportRows) error {
	if rows.View != flowquery.ViewRaw && rows.View != flowquery.ViewSupplier {
		return errors.New("flow detail export view must be raw or supplier")
	}
	if len(rows.Fields) == 0 || len(rows.Rows) == 0 {
		return errors.New("flow detail export fields and rows are required")
	}
	seen := make(map[flowquery.DetailField]struct{}, len(rows.Fields))
	for _, field := range rows.Fields {
		if _, exists := seen[field]; exists {
			return fmt.Errorf("flow detail export field %q is duplicated", field)
		}
		seen[field] = struct{}{}
	}
	for _, row := range rows.Rows {
		if row.EventTime.IsZero() || len(row.Values) != len(rows.Fields) {
			return errors.New("flow detail export row does not match its schema")
		}
		for index, value := range row.Values {
			if _, err := flowDetailParquetNode(value); err != nil {
				return fmt.Errorf("flow detail field %q: %w", rows.Fields[index], err)
			}
			if reflect.TypeOf(value) != reflect.TypeOf(rows.Rows[0].Values[index]) {
				return fmt.Errorf("flow detail field %q changed type", rows.Fields[index])
			}
		}
	}
	return nil
}

func flowDetailParquetNode(value any) (parquet.Node, error) {
	switch value.(type) {
	case string:
		return parquet.String(), nil
	case uint64:
		return parquet.Uint(64), nil
	case bool:
		return parquet.Leaf(parquet.BooleanType), nil
	case time.Time:
		return parquet.Timestamp(parquet.Millisecond), nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", value)
	}
}

func flowDetailExportText(value any) string {
	switch typed := value.(type) {
	case string:
		return safeSpreadsheetCell(typed)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case bool:
		return strconv.FormatBool(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

// safeSpreadsheetCell neutralizes CSV formula injection by prefixing a leading
// control character with a quote.
func safeSpreadsheetCell(value string) string {
	if value == "" {
		return value
	}
	switch value[0] {
	case '=', '+', '-', '@', '\t', '\r':
		return "'" + value
	default:
		return value
	}
}

func writeFlowExportArtifact(dir, jobID, extension string, data []byte) (string, error) {
	temporary, err := os.CreateTemp(dir, ".flow-export-*")
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
	if _, err := temporary.Write(data); err != nil {
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	filename := jobID + "-" + hex.EncodeToString(digest[:]) + "." + extension
	if err := os.Rename(temporaryName, filepath.Join(dir, filename)); err != nil {
		return "", err
	}
	keep = true
	return flowExportArtifactPrefix + filename, nil
}

func (s *Server) listFlowExports(c *gin.Context) {
	for name := range c.Request.URL.Query() {
		switch name {
		case "limit", "offset", "status", "q":
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
	status := strings.TrimSpace(c.Query("status"))
	if status == "complete" {
		status = opjob.StatusSucceeded
	}
	if status == "pending" {
		status = opjob.StatusQueued
	}
	if status == "all" {
		status = ""
	}
	if status != "" && !validOperationJobStatus(status) {
		fail(c, http.StatusBadRequest, "invalid_filter", "invalid export status")
		return
	}
	filter := opjob.Filter{JobType: flowExportJobType, Status: status, Search: search, Limit: limit, Offset: offset}
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
	items := make([]flowExportResponse, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, s.flowExportView(job))
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

func (s *Server) getFlowExport(c *gin.Context) {
	job, ok := s.authorizedFlowExport(c)
	if !ok {
		return
	}
	c.JSON(http.StatusOK, s.flowExportView(job))
}

func (s *Server) cancelFlowExport(c *gin.Context) {
	job, ok := s.authorizedFlowExport(c)
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
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "flow.export.cancel", "operation_job", job.ID)
	c.JSON(http.StatusOK, s.flowExportView(job))
}

func (s *Server) downloadFlowExport(c *gin.Context) {
	job, ok := s.authorizedFlowExport(c)
	if !ok {
		return
	}
	if job.Status != opjob.StatusSucceeded {
		fail(c, http.StatusConflict, "export_not_ready", "export is not complete")
		return
	}
	if job.FinishedAt.IsZero() || time.Now().UTC().After(job.FinishedAt.Add(s.flowExportRetention())) {
		fail(c, http.StatusGone, "export_expired", "export artifact has expired")
		return
	}
	path, checksum, extension, err := s.flowExportArtifact(job)
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
	contentType := "text/csv; charset=utf-8"
	if extension == "parquet" {
		contentType = "application/vnd.apache.parquet"
	}
	c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="watchdog-flow-%s.%s"`, job.ID, extension))
	c.DataFromReader(http.StatusOK, stat.Size(), contentType, file, nil)
}

func (s *Server) authorizedFlowExport(c *gin.Context) (opjob.Job, bool) {
	job, err := s.jobs.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return opjob.Job{}, false
	}
	p := currentPrincipal(c)
	if job.JobType != flowExportJobType || (!p.IsAdmin && job.CreatedBy != p.UserID) {
		fail(c, http.StatusNotFound, "not_found", "export not found")
		return opjob.Job{}, false
	}
	return job, true
}

func (s *Server) flowExportView(job opjob.Job) flowExportResponse {
	view := flowExportResponse{
		ID: job.ID, OperationJobID: job.ID, Status: exportStatus(job.Status), Format: "csv",
		RowCount: job.ProgressDone, ErrorMessage: job.LastErrorDetail,
		CreatedAt: job.CreatedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: job.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	var payload flowExportPayload
	if opjob.DecodePayload(job.CheckpointJSON, flowExportPayloadSchema, &payload) == nil {
		view.Kind = payload.Kind
		view.View = string(payload.View)
		view.Format = payload.Format
		view.MaxRows = payload.MaxRows
		var from, to time.Time
		switch {
		case payload.Detail != nil:
			from, to = payload.Detail.From, payload.Detail.To
		case payload.Query != nil:
			from, to = payload.Query.From, payload.Query.To
		case payload.Report != nil:
			from, to = payload.Report.From, payload.Report.To
		case payload.VPNFindings != nil:
			from, to = payload.VPNFindings.From, payload.VPNFindings.To
		}
		if !from.IsZero() {
			view.RangeStart = from.UTC().Format(time.RFC3339Nano)
			view.RangeEnd = to.UTC().Format(time.RFC3339Nano)
		}
	}
	if !job.FinishedAt.IsZero() {
		view.UpdatedAt = job.FinishedAt.UTC().Format(time.RFC3339Nano)
		view.ExpiresAt = job.FinishedAt.Add(s.flowExportRetention()).UTC().Format(time.RFC3339Nano)
	}
	if path, checksum, _, err := s.flowExportArtifact(job); err == nil {
		view.Checksum = checksum
		if stat, statErr := os.Stat(path); statErr == nil {
			view.SizeBytes = stat.Size()
		}
	}
	return view
}

func (s *Server) flowExportArtifact(job opjob.Job) (string, string, string, error) {
	if !strings.HasPrefix(job.ResultRef, flowExportArtifactPrefix) {
		return "", "", "", errors.New("invalid flow export result reference")
	}
	filename := strings.TrimPrefix(job.ResultRef, flowExportArtifactPrefix)
	if filepath.Base(filename) != filename || !strings.HasPrefix(filename, job.ID+"-") {
		return "", "", "", errors.New("invalid flow export result reference")
	}
	extension := strings.TrimPrefix(filepath.Ext(filename), ".")
	if extension != "csv" && extension != "parquet" {
		return "", "", "", errors.New("invalid flow export artifact extension")
	}
	checksum := strings.TrimSuffix(strings.TrimPrefix(filename, job.ID+"-"), "."+extension)
	if len(checksum) != sha256.Size*2 {
		return "", "", "", errors.New("invalid flow export checksum")
	}
	if _, err := hex.DecodeString(checksum); err != nil {
		return "", "", "", errors.New("invalid flow export checksum")
	}
	return filepath.Join(s.flowExportDir(), filename), checksum, extension, nil
}
