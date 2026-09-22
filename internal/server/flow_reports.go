// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// flow_reports.go is the KISS-06 v2 fixed-report orchestrator (reports Layer 2).
// A report is a set of panels; each panel is one direct ClickHouse query through
// the committed flowquery runners, marshaled by the Layer-1 table composer. This
// replaces the retired hub gateway dispatch (reportPanelQueries -> composer.Query):
// panels select the aggregate/joint runner directly. overview + dimensions land
// first (pure aggregate/joint); endpoints/overseas/vpn follow.

const flowReportSchemaVersion = uint16(1)

type flowReportKind string

const (
	flowReportOverview   flowReportKind = "overview"
	flowReportDimensions flowReportKind = "dimensions"
	flowReportOverseas   flowReportKind = "overseas"
	flowReportEndpoints  flowReportKind = "endpoints"
	flowReportVPN        flowReportKind = "vpn"
)

// flowReportDisplayMode is echoed to the client so it renders panel values as
// absolute (value), a share of the panel total (share), or a period-over-period
// difference. The transform is applied client-side; the server only validates and
// passes the mode through, matching the hub.
type flowReportDisplayMode string

const (
	flowReportValue      flowReportDisplayMode = "value"
	flowReportShare      flowReportDisplayMode = "share"
	flowReportDifference flowReportDisplayMode = "difference"
)

// flowReportCategories/flowReportResiduals are the six-class traffic taxonomy the
// endpoint report enriches each endpoint with (on/off-net + overseas, plus the
// non-additive residual classes).
var flowReportCategories = []string{
	"on_net_local_city", "on_net_cross_city", "on_net_cross_province",
	"off_net_in_province", "off_net_cross_province", "overseas",
}

var flowReportResiduals = []string{"unknown", "internal", "transit", "ambiguous"}

type flowReportRequest struct {
	Kind         flowReportKind               `json:"kind"`
	Side         string                       `json:"side,omitempty"`
	GroupBy      flowquery.Dimension          `json:"group_by,omitempty"`
	DisplayMode  flowReportDisplayMode        `json:"display_mode,omitempty"`
	PeakWindows  []flowquery.LocalTimeWindow  `json:"peak_windows,omitempty"`
	PanelIDs     []string                     `json:"panel_ids,omitempty"`
	Metric       flowquery.Metric             `json:"metric"`
	View         flowquery.View               `json:"view"`
	Filters      flowquery.Filters            `json:"filters,omitempty"`
	Filter       *flowquery.FilterExpression  `json:"filter,omitempty"`
	From         time.Time                    `json:"from"`
	To           time.Time                    `json:"to"`
	TargetPoints uint16                       `json:"target_points,omitempty"`
	TopN         uint16                       `json:"top_n,omitempty"`
	Timezone     string                       `json:"timezone,omitempty"`
	Operator     *flowOperatorSelection       `json:"operator_selection,omitempty"`
	Table        *flowTableRequest            `json:"table,omitempty"`  // endpoint report: client sort/search/filter/pagination of the enriched endpoint table
	Tables       map[string]*flowTableRequest `json:"tables,omitempty"` // vpn report: named client tables (port_distribution/type_distribution)
}

// flowReportQueryInput is the hub wire contract for POST /flow/reports/query: a
// gateway envelope carrying value-layer/range/limit plus a nested `report` spec.
// v2 keeps this exact wire shape (the frontend is unchanged) and maps it onto the
// internal flat flowReportRequest — the QueryGateway machinery itself is not
// reintroduced; only its request DTO is honored.
type flowReportQueryInput struct {
	From            time.Time                   `json:"from"`
	To              time.Time                   `json:"to"`
	StepSeconds     uint32                      `json:"step_seconds,omitempty"`
	Limit           uint32                      `json:"limit,omitempty"`
	ValueLayer      flowquery.View              `json:"value_layer,omitempty"`
	RequireComplete bool                        `json:"require_complete,omitempty"`
	Metric          flowquery.Metric            `json:"metric,omitempty"`
	Filters         flowquery.Filters           `json:"filters,omitempty"`
	Filter          *flowquery.FilterExpression `json:"filter,omitempty"`
	TopN            uint16                      `json:"top_n,omitempty"`
	IncludeOther    bool                        `json:"include_other,omitempty"`
	Timezone        string                      `json:"timezone,omitempty"`
	TargetPoints    uint16                      `json:"target_points,omitempty"`
	Operator        *flowOperatorSelection      `json:"operator_selection,omitempty"`
	Table           *flowTableRequest           `json:"table,omitempty"`
	Report          flowReportSpecInput         `json:"report"`
}

// flowReportSpecInput is the nested `report` discriminant of the envelope.
type flowReportSpecInput struct {
	SchemaVersion uint16                       `json:"schema_version,omitempty"`
	Kind          flowReportKind               `json:"kind"`
	Side          string                       `json:"side,omitempty"`
	GroupBy       flowquery.Dimension          `json:"group_by,omitempty"`
	DisplayMode   flowReportDisplayMode        `json:"display_mode,omitempty"`
	PeakWindows   []flowquery.LocalTimeWindow  `json:"peak_windows,omitempty"`
	PanelIDs      []string                     `json:"panel_ids,omitempty"`
	Tables        map[string]*flowTableRequest `json:"tables,omitempty"`
}

// flowOperatorSelection is the operator-scoped query binding. The frontend sends
// only {operator_id}; the server resolves and pins the immutable publication pair
// timeline before compiling the query. Populated resolved fields from clients are
// rejected so they cannot forge historical classification provenance.
type flowOperatorSelection struct {
	SchemaVersion          uint16   `json:"schema_version,omitempty"`
	OperatorID             string   `json:"operator_id"`
	FlowISPID              uint32   `json:"flow_isp_id,omitempty"`
	PublicationIDs         []string `json:"publication_ids,omitempty"`
	DimensionSnapshotIDs   []string `json:"dimension_snapshot_ids,omitempty"`
	ClassificationVersions []uint32 `json:"classification_versions,omitempty"`
}

// toReportRequest lifts the envelope's report spec + common query fields into the
// internal flat request the orchestrator consumes.
func (in flowReportQueryInput) toReportRequest() flowReportRequest {
	return flowReportRequest{
		Kind: in.Report.Kind, Side: in.Report.Side, GroupBy: in.Report.GroupBy,
		DisplayMode: in.Report.DisplayMode, PeakWindows: in.Report.PeakWindows,
		PanelIDs: in.Report.PanelIDs, Tables: in.Report.Tables,
		Metric: in.Metric, View: in.ValueLayer, Filters: in.Filters, Filter: in.Filter,
		From: in.From, To: in.To, TargetPoints: in.TargetPoints, TopN: in.TopN,
		Timezone: in.Timezone, Operator: in.Operator, Table: in.Table,
	}
}

type flowReportPanel struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Reason string          `json:"reason,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
	Meta   gin.H           `json:"meta,omitempty"`
}

// reportPanelSpec is one resolved panel: a single dimension is an aggregate query,
// multiple dimensions a joint query.
type reportPanelSpec struct {
	ID           string
	Optional     bool
	Dimensions   []flowquery.Dimension
	Filters      flowquery.Filters
	TopN         uint16
	IncludeOther bool
	Special      string // "" = aggregate/joint; "observed" = overseas runner; "vpn_share" = findings share
}

// flowReportGroupings are the dimensions a "dimensions" report may group by.
// remote_prefix/dst_ip are included so an "unknown" breakdown can surface the
// destination segments (matched base prefix, or the '_unassigned' bucket for
// ranges missing from the base library) that need correction.
var flowReportGroupings = map[flowquery.Dimension]struct{}{
	flowquery.DimensionCategory: {}, flowquery.DimensionGeoProvince: {}, flowquery.DimensionGeoCity: {},
	flowquery.DimensionISP: {}, flowquery.DimensionGeoCountry: {}, flowquery.DimensionASN: {},
	flowquery.DimensionBusiness: {}, flowquery.DimensionProtocol: {},
	flowquery.DimensionRemotePrefix: {}, flowquery.DimensionDestinationIP: {},
}

func (s *Server) registerFlowReportRoutes(auth *gin.RouterGroup, view gin.HandlerFunc) {
	reports := auth.Group("/flow/reports", view)
	reports.GET("/capabilities", s.flowReportCapabilities)
	reports.GET("/references", s.flowReportReferences)
	reports.POST("/query", s.queryFlowReport)
	reports.GET("/query/:id", s.getFlowReportQuery)
}

// flowReportCapabilities advertises the report kinds, groupings and metrics the v2
// orchestrator currently serves (overview, dimensions, overseas, endpoints).
func (s *Server) flowReportCapabilities(c *gin.Context) {
	groupings := make([]flowquery.Dimension, 0, len(flowReportGroupings))
	for dimension := range flowReportGroupings {
		groupings = append(groupings, dimension)
	}
	c.JSON(http.StatusOK, gin.H{
		"schema_version": flowReportSchemaVersion,
		"kinds":          []flowReportKind{flowReportOverview, flowReportDimensions, flowReportOverseas, flowReportEndpoints, flowReportVPN},
		"endpoint_sides": []string{"source", "destination"},
		"display_modes":  []flowReportDisplayMode{flowReportValue, flowReportShare, flowReportDifference},
		"groupings":      groupings,
		"max_top_n":      100,
	})
}

func normalizeFlowReport(req *flowReportRequest) error {
	switch req.Kind {
	case flowReportOverview, flowReportDimensions, flowReportOverseas, flowReportEndpoints, flowReportVPN:
	default:
		return errors.New("report.kind must be overview, dimensions, overseas, endpoints, or vpn")
	}
	if req.DisplayMode == "" {
		req.DisplayMode = flowReportValue
	}
	if req.DisplayMode != flowReportValue && req.DisplayMode != flowReportShare && req.DisplayMode != flowReportDifference {
		return errors.New("report.display_mode must be value, share, or difference")
	}
	if err := normalizeFlowReportPeakWindows(req.PeakWindows); err != nil {
		return err
	}
	if req.Kind == flowReportEndpoints {
		if req.Side != "source" && req.Side != "destination" {
			return errors.New("report.side must be source or destination for endpoints")
		}
	} else if req.Side != "" {
		return errors.New("report.side is only valid for an endpoints report")
	}
	// VPN reports pin estimated_bytes so numerator/denominator traffic ratios share
	// one additive unit; every other kind defaults to a rate.
	if req.Metric == "" {
		if req.Kind == flowReportVPN {
			req.Metric = flowquery.MetricEstimatedBytes
		} else {
			req.Metric = flowquery.MetricEstimatedBPS
		}
	}
	if req.Kind == flowReportVPN && req.Metric != flowquery.MetricEstimatedBytes {
		return errors.New("vpn reports require metric=estimated_bytes so traffic ratios use one additive unit")
	}
	if req.TopN == 0 {
		req.TopN = 20
	}
	if req.TopN > 100 {
		return errors.New("top_n must be 1..100")
	}
	if req.TargetPoints == 0 {
		req.TargetPoints = flowquery.DefaultTargetPoints
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}
	if _, err := time.LoadLocation(req.Timezone); err != nil {
		return errors.New("timezone is not a valid IANA location")
	}
	if !req.To.After(req.From) {
		return errors.New("to must be after from")
	}
	if req.Kind == flowReportDimensions {
		if req.GroupBy == "" {
			req.GroupBy = flowquery.DimensionCategory
		}
		if _, ok := flowReportGroupings[req.GroupBy]; !ok {
			return errors.New("group_by is unsupported")
		}
	} else if req.GroupBy != "" {
		return errors.New("group_by is only valid for a dimensions report")
	}
	if req.Kind == flowReportEndpoints {
		if req.Table == nil {
			req.Table = &flowTableRequest{SortBy: "maximum", SortDirection: "desc", Limit: req.TopN}
		}
		if err := normalizeFlowEndpointTableRequest(req.Table); err != nil {
			return err
		}
	} else if req.Table != nil {
		return errors.New("table is only valid for an endpoints report")
	}
	if err := normalizeFlowReportTables(req); err != nil {
		return err
	}
	return nil
}

// normalizeFlowReportPeakWindows validates the optional time-of-day windows that
// restrict every aggregate/joint panel (and the vpn findings scan) to peak hours.
func normalizeFlowReportPeakWindows(windows []flowquery.LocalTimeWindow) error {
	if len(windows) > 8 {
		return errors.New("report.peak_windows accepts at most 8 windows")
	}
	for index, window := range windows {
		if len(window.Days) == 0 || len(window.Days) > 7 {
			return fmt.Errorf("report.peak_windows[%d].days must contain 1..7 ISO weekdays", index)
		}
		seen := map[uint8]struct{}{}
		for _, day := range window.Days {
			if day < 1 || day > 7 {
				return fmt.Errorf("report.peak_windows[%d].days must contain ISO weekdays 1..7", index)
			}
			if _, exists := seen[day]; exists {
				return fmt.Errorf("report.peak_windows[%d].days contains duplicates", index)
			}
			seen[day] = struct{}{}
		}
		if _, err := time.Parse("15:04", window.StartLocal); err != nil {
			return fmt.Errorf("report.peak_windows[%d].start_local must be HH:MM", index)
		}
		if _, err := time.Parse("15:04", window.EndLocal); err != nil {
			return fmt.Errorf("report.peak_windows[%d].end_local must be HH:MM", index)
		}
		if window.StartLocal == window.EndLocal {
			return fmt.Errorf("report.peak_windows[%d] cannot have an empty interval", index)
		}
	}
	return nil
}

// reportPanelSpecs resolves the panels for a report: a total headline plus the
// kind's directional breakdowns. panel_ids, when present, select a subset.
func reportPanelSpecs(req flowReportRequest) []reportPanelSpec {
	base := req.Filters
	if req.Kind == flowReportOverseas {
		base.Categories = append(append([]string(nil), base.Categories...), "overseas")
	}
	specs := []reportPanelSpec{{
		ID: "total", Dimensions: []flowquery.Dimension{flowquery.DimensionTotal}, TopN: 1, IncludeOther: false,
		Filters: base, Special: "direction_split",
	}}
	addDirections := func(prefix string, optional bool, dimensions ...flowquery.Dimension) {
		for _, direction := range []string{"in", "out"} {
			filters := base
			filters.Directions = []string{direction}
			specs = append(specs, reportPanelSpec{
				ID: prefix + "_" + direction, Optional: optional, Dimensions: dimensions,
				Filters: filters, TopN: req.TopN, IncludeOther: true,
			})
		}
	}
	switch req.Kind {
	case flowReportOverview:
		addDirections("category", false, flowquery.DimensionCategory)
		addDirections("business_category", true, flowquery.DimensionBusiness, flowquery.DimensionCategory)
	case flowReportDimensions:
		addDirections("dimension", false, req.GroupBy)
	case flowReportOverseas:
		addDirections("country", false, flowquery.DimensionGeoCountry)
		addDirections("region", false, flowquery.DimensionGeoRegion)
		addDirections("asn", false, flowquery.DimensionASN)
		addDirections("remote_port", false, flowquery.DimensionRemotePort)
		addDirections("protocol", false, flowquery.DimensionProtocol)
		specs = append(specs, reportPanelSpec{ID: "observed", Optional: true, Special: "observed", Filters: base})
		specs = append(specs, reportPanelSpec{ID: "vpn_share", Optional: true, Special: "vpn_share", Filters: base})
	}
	if len(req.PanelIDs) == 0 {
		return specs
	}
	selected := make(map[string]struct{}, len(req.PanelIDs))
	for _, id := range req.PanelIDs {
		selected[id] = struct{}{}
	}
	filtered := specs[:0]
	for _, spec := range specs {
		if _, ok := selected[spec.ID]; ok {
			filtered = append(filtered, spec)
		}
	}
	return filtered
}

func (s *Server) queryFlowReport(c *gin.Context) {
	if !s.flowQueryReady(c) {
		return
	}
	var input flowReportQueryInput
	if !addressDecodeStrict(c, &input, 272<<10) {
		return
	}
	req := input.toReportRequest()
	if err := normalizeFlowReport(&req); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	view, ok := s.authorizeFlowView(c, req.View)
	if !ok {
		return
	}
	if !s.authorizeFlowResourceFilters(c, req.Filters.TargetIDs, req.Filters.DeviceIDs, req.Filters.ExporterIDs) {
		return
	}
	// Operator-scoped reports pin every panel to the operator's customer ISP identity
	// and the address snapshots effective (and installed on all active flow workers)
	// over the report range, by injecting the constraints into the shared report
	// filters before the panels compile.
	if req.Operator != nil && strings.TrimSpace(req.Operator.OperatorID) != "" {
		if !s.applyFlowOperatorSelection(c, req.Operator, view, req.From, req.To, &req.Filters, &req.Filter) {
			return
		}
	}
	// The VPN report embeds materialized findings, which are sensitive and gated by
	// flow.vpn.view on their own read API; the report kind requires the same.
	if req.Kind == flowReportVPN && !currentPrincipal(c).can("flow.vpn.view") {
		fail(c, http.StatusForbidden, "forbidden", "vpn reports require flow.vpn.view")
		return
	}
	now := time.Now().UTC()
	scope := flowquery.Scope{AllowedViews: []flowquery.View{view}}
	if s.flowReportQueryNeedsAsync(req) {
		s.enqueueFlowReportQuery(c, view, req)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), s.cfg.Flow.Query.SynchronousTimeout)
	defer cancel()
	response, err := s.buildFlowReportResponse(ctx, scope, view, req, now, currentPrincipal(c).can("flow.vpn.view"))
	if err != nil {
		writeFlowQueryError(c, err)
		return
	}
	s.auditFlowQuery(c.Request.Context(), currentPrincipal(c).UserID, "flow.report.query", "flow_report", string(req.Kind), req.Operator)
	c.JSON(http.StatusOK, response)
}

func (s *Server) buildFlowReportResponse(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, now time.Time, canVPNView bool) (gin.H, error) {
	panels, warnings, err := s.buildFlowReport(ctx, scope, view, req, now, canVPNView)
	if err != nil {
		return nil, err
	}
	// A report-level plan gives the response its requested/effective range and the
	// source→display step, mirroring the per-panel aggregate planning.
	plan, err := flowquery.PlanAggregate(req.From, req.To, 0, req.TargetPoints, now)
	if err != nil {
		return nil, err
	}
	completeRatio, partial := flowReportCompleteness(panels, warnings)
	data := gin.H{
		"schema_version": flowReportSchemaVersion,
		"kind":           req.Kind,
		"display_mode":   req.DisplayMode,
		"range": gin.H{
			"requested_from": plan.RequestedFrom, "requested_to": plan.RequestedTo,
			"effective_from": plan.EffectiveFrom, "effective_to": plan.EffectiveTo, "timezone": req.Timezone,
		},
		"plan":         gin.H{"source": plan.Source, "source_seconds": plan.SourceSeconds, "display_seconds": plan.StepSeconds},
		"watermark":    gin.H{"generated_at": now},
		"completeness": gin.H{"complete_ratio": completeRatio, "late_ratio": 0, "unknown_ratio": 0, "partial": partial, "warnings": warnings},
		"panels":       panels,
		"warnings":     warnings,
	}
	if req.Side != "" {
		data["side"] = req.Side
	}
	meta := gin.H{
		"request_id": newID(), "as_of": now, "timezone": req.Timezone, "step_seconds": plan.StepSeconds,
		"complete_ratio": completeRatio, "unknown_ratio": 0, "partial": partial,
	}
	if req.Operator != nil && req.Operator.SchemaVersion != 0 {
		meta["versions"] = gin.H{
			"publication_ids": req.Operator.PublicationIDs, "dimension_snapshot_ids": req.Operator.DimensionSnapshotIDs,
			"classification_versions": req.Operator.ClassificationVersions,
		}
		meta["operator_selection"] = req.Operator
	}
	return gin.H{
		"data": data,
		// meta carries completeness scalars; the report's panel warnings live in
		// data.warnings. The frontend concatenates data.warnings + meta.warnings, so
		// mirroring the same list here would double every warning — v2 produces no
		// separate completeness-level warnings, so meta.warnings stays absent.
		"meta": meta,
	}, nil
}

// flowReportCompleteness derives the report-level completeness from its panels: the
// minimum panel complete_ratio (panels that carry one) and whether anything is
// partial (a warning, an unavailable panel, or a partial panel).
func flowReportCompleteness(panels []flowReportPanel, warnings []string) (float64, bool) {
	complete := 1.0
	partial := len(warnings) > 0
	if partial {
		complete = 0
	}
	for _, panel := range panels {
		if panel.Status != "ready" {
			partial = true
			complete = 0
		}
		if panel.Meta == nil {
			continue
		}
		if ratio, ok := panel.Meta["complete_ratio"].(float64); ok && ratio < complete {
			complete = ratio
		}
		if flag, ok := panel.Meta["partial"].(bool); ok && flag {
			partial = true
		}
	}
	if complete < 1 {
		partial = true
	}
	return complete, partial
}

// buildFlowReport runs a report's panels with a configured concurrency bound and
// returns them in spec order with the accumulated warnings. It is the context-free
// core shared by the queryFlowReport handler and
// the report export worker: callers first authorize the view, resource filters and
// (for the vpn kind) flow.vpn.view, then pass canVPNView so the optional overseas
// vpn_share panel can degrade for flow.view-only callers.
func (s *Server) buildFlowReport(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, now time.Time, canVPNView bool) ([]flowReportPanel, []string, error) {
	specs := reportPanelSpecs(req)
	results := make([]flowReportPanelRun, len(specs))
	jobs := make(chan int, len(specs))
	for index := range specs {
		jobs <- index
	}
	close(jobs)
	workerCount := min(s.cfg.Flow.Query.PanelConcurrency, len(specs))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				results[index] = s.runFlowReportPanelSpec(ctx, scope, view, req, specs[index], now, canVPNView)
			}
		}()
	}
	workers.Wait()
	panels := make([]flowReportPanel, 0, len(specs))
	warnings := make([]string, 0)
	for _, result := range results {
		if result.err != nil {
			return nil, nil, result.err
		}
		panels = append(panels, result.panel)
		warnings = append(warnings, result.warnings...)
	}
	if req.Kind == flowReportOverview {
		if table := req.Tables["business_matrix"]; table != nil {
			composed, err := composeFlowBusinessMatrixTable(panels, *table)
			if err != nil {
				return nil, nil, err
			}
			panels = composed
		}
	}
	if req.Kind == flowReportEndpoints {
		endpointPanels, err := s.enrichEndpointReport(ctx, scope, view, req, now)
		if err != nil {
			return nil, nil, err
		}
		panels = append(panels, endpointPanels...)
	}
	if req.Kind == flowReportVPN {
		vpnPanels, err := s.vpnReportPanels(ctx, req, now)
		if err != nil {
			return nil, nil, err
		}
		for _, panel := range vpnPanels {
			if panel.Status == "unavailable" {
				warnings = append(warnings, panel.ID+": "+panel.Reason)
			}
		}
		panels = append(panels, vpnPanels...)
	}
	// Dedupe + sort so a warning that two panels raise (or a panel repeats across
	// sections) surfaces once, matching the hub — the frontend keys the warning list
	// by text, so duplicates would collide.
	return panels, uniqueSortedStrings(warnings), nil
}

type flowReportPanelRun struct {
	panel    flowReportPanel
	warnings []string
	err      error
}

func (s *Server) runFlowReportPanelSpec(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, spec reportPanelSpec, now time.Time, canVPNView bool) flowReportPanelRun {
	switch spec.Special {
	case "direction_split":
		raw, meta, err := s.runReportDirectionPanel(ctx, scope, view, req, spec, now)
		return flowReportPanelRun{panel: flowReportPanel{ID: spec.ID, Status: "ready", Data: raw, Meta: meta}, err: err}
	case "observed":
		panel, err := s.runOverseasObservedPanel(ctx, view, req, now)
		if err != nil {
			return flowReportPanelRun{
				panel:    flowReportPanel{ID: "observed", Status: "unavailable", Reason: "overseas observed query failed"},
				warnings: []string{"observed: unavailable"},
			}
		}
		return flowReportPanelRun{panel: panel}
	case "vpn_share":
		// The findings-derived share is mildly sensitive; degrade the optional
		// panel rather than failing the overseas report for flow.view-only callers.
		if !canVPNView {
			return flowReportPanelRun{
				panel:    flowReportPanel{ID: "vpn_share", Status: "unavailable", Reason: "overseas VPN share requires flow.vpn.view"},
				warnings: []string{"vpn_share: unavailable"},
			}
		}
		panel, err := s.runOverseasVPNSharePanel(ctx, scope, view, req, now)
		if err != nil {
			return flowReportPanelRun{
				panel:    flowReportPanel{ID: "vpn_share", Status: "unavailable", Reason: "overseas VPN share query failed"},
				warnings: []string{"vpn_share: unavailable"},
			}
		}
		return flowReportPanelRun{panel: panel}
	default:
		raw, meta, err := s.runReportPanel(ctx, scope, view, req, spec, now)
		if err != nil {
			if !spec.Optional {
				return flowReportPanelRun{err: err}
			}
			return flowReportPanelRun{
				panel:    flowReportPanel{ID: spec.ID, Status: "unavailable", Reason: "panel query failed"},
				warnings: []string{spec.ID + ": unavailable"},
			}
		}
		return flowReportPanelRun{panel: flowReportPanel{ID: spec.ID, Status: "ready", Data: raw, Meta: meta}}
	}
}

// uniqueSortedStrings returns the distinct values in sorted order (nil-safe).
func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

// runReportPanel executes one panel's query and marshals it. A single dimension
// uses the aggregate runner (planned onto a rollup); multiple dimensions use the
// joint runner. Both reuse the Layer-1 table composer (with geo labels).
func (s *Server) runReportPanel(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, spec reportPanelSpec, now time.Time) (json.RawMessage, gin.H, error) {
	baseFacts, err := flowReportNeedsBaseFacts(req.Filter)
	if err != nil {
		return nil, nil, err
	}
	if len(spec.Dimensions) > 1 || baseFacts {
		compiled, err := flowquery.CompileJoint(scope, flowquery.JointRequest{
			From: req.From, To: req.To, TargetPoints: req.TargetPoints, Metric: req.Metric,
			Dimensions: spec.Dimensions, Filters: spec.Filters, Filter: req.Filter, View: view,
			TopN: spec.TopN, IncludeOther: spec.IncludeOther, Timezone: req.Timezone, TimeWindows: req.PeakWindows,
			ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
		}, now)
		if err != nil {
			return nil, nil, err
		}
		result, err := s.flowQuery.joint.Run(ctx, compiled)
		if err != nil {
			return nil, nil, err
		}
		raw, err := marshalFlowJointResult(result, nil, s.flowGeo)
		if err != nil {
			return nil, nil, err
		}
		return raw, gin.H{"step_seconds": compiled.Plan.StepSeconds, "source": compiled.Plan.Source, "unit": result.Metric.Unit}, nil
	}
	plan, err := flowquery.PlanAggregate(req.From, req.To, 0, req.TargetPoints, now)
	if err != nil {
		return nil, nil, err
	}
	queryRequest := flowquery.Request{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Interval: plan.Interval,
		Metric: req.Metric, Dimension: spec.Dimensions[0], Filters: spec.Filters, Filter: req.Filter,
		View: view, TopN: spec.TopN, IncludeOther: spec.IncludeOther, Timezone: req.Timezone, TimeWindows: req.PeakWindows,
		ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
	}
	if err := s.applyFlowStorageBoundary(ctx, &queryRequest); err != nil {
		return nil, nil, err
	}
	compiled, err := flowquery.Compile(scope, queryRequest, now)
	if err != nil {
		return nil, nil, err
	}
	result, err := s.flowQuery.aggregate.Run(ctx, compiled)
	if err != nil {
		return nil, nil, err
	}
	raw, err := marshalFlowAggregateResult(result, nil, s.flowGeo)
	if err != nil {
		return nil, nil, err
	}
	return raw, gin.H{"step_seconds": plan.StepSeconds, "source": plan.Source, "unit": result.Metric.Unit}, nil
}

// runReportDirectionPanel returns the report headline as two faithful directional
// totals. Keeping the split on the server ensures every panel uses the same
// resource filters (including device_id) and the browser never has to derive a
// total from a truncated Top-N category response.
func (s *Server) runReportDirectionPanel(ctx context.Context, scope flowquery.Scope, view flowquery.View, req flowReportRequest, spec reportPanelSpec, now time.Time) (json.RawMessage, gin.H, error) {
	baseFacts, err := flowReportNeedsBaseFacts(req.Filter)
	if err != nil {
		return nil, nil, err
	}
	if baseFacts {
		filters := spec.Filters
		filters.Directions = flowDirectionValues()
		compiled, err := flowquery.CompileJoint(scope, flowquery.JointRequest{
			From: req.From, To: req.To, TargetPoints: req.TargetPoints, Metric: req.Metric,
			Dimensions: []flowquery.Dimension{flowquery.DimensionDirection}, Filters: filters, Filter: req.Filter,
			View: view, TopN: uint16(len(flowDirectionParts)), IncludeOther: false, Timezone: req.Timezone, TimeWindows: req.PeakWindows,
			ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
		}, now)
		if err != nil {
			return nil, nil, err
		}
		combined, err := s.flowQuery.joint.Run(ctx, compiled)
		if err != nil {
			return nil, nil, err
		}
		labelFlowDirectionJointResult(&combined)
		raw, err := marshalFlowJointResult(combined, nil, s.flowGeo)
		if err != nil {
			return nil, nil, err
		}
		return raw, gin.H{
			"step_seconds": combined.Plan.StepSeconds,
			"source":       combined.Plan.Source,
			"unit":         combined.Metric.Unit,
		}, nil
	}
	plan, err := flowquery.PlanAggregate(req.From, req.To, 0, req.TargetPoints, now)
	if err != nil {
		return nil, nil, err
	}
	filters := spec.Filters
	filters.Directions = flowDirectionValues()
	request := flowquery.Request{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Interval: plan.Interval,
		Metric: req.Metric, Dimension: flowquery.DimensionDirection, Filters: filters, Filter: req.Filter,
		View: view, TopN: uint16(len(flowDirectionParts)), IncludeOther: false, Timezone: req.Timezone, TimeWindows: req.PeakWindows,
		ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
	}
	if err := s.applyFlowStorageBoundary(ctx, &request); err != nil {
		return nil, nil, err
	}
	compiled, err := flowquery.Compile(scope, request, now)
	if err != nil {
		return nil, nil, err
	}
	combined, err := s.flowQuery.aggregate.Run(ctx, compiled)
	if err != nil {
		return nil, nil, err
	}
	labelFlowDirectionResult(&combined)
	combined.Plan = &plan
	raw, err := marshalFlowAggregateResult(combined, nil, s.flowGeo)
	if err != nil {
		return nil, nil, err
	}
	return raw, gin.H{
		"step_seconds": plan.StepSeconds,
		"source":       plan.Source,
		"unit":         combined.Metric.Unit,
	}, nil
}

// flowReportNeedsBaseFacts keeps fixed reports on rollups whenever their typed
// predicate is materialized there, and selects the existing bounded flow_records
// query path for cross-dimension Geo/operator predicates.
func flowReportNeedsBaseFacts(filter *flowquery.FilterExpression) (bool, error) {
	if filter == nil {
		return false, nil
	}
	supported, err := flowquery.AggregateFilterSupported(*filter)
	if err != nil {
		return false, err
	}
	return !supported, nil
}

// runOverseasObservedPanel builds the overseas observed-cardinality panel through
// the overseas runner (customer-view only; typed filters unsupported — mirrors the
// hub). Without a storage-lifecycle policy the StorageV2 recent-raw hybrid is off,
// so it reads the rolled-up aggregate; geo labels come from the ported geo service.
func (s *Server) runOverseasObservedPanel(ctx context.Context, view flowquery.View, req flowReportRequest, now time.Time) (flowReportPanel, error) {
	if view != flowquery.ViewCustomer {
		return flowReportPanel{ID: "observed", Status: "unavailable", Reason: "overseas observed cardinality is available in the customer view only"}, nil
	}
	if req.Filter != nil {
		return flowReportPanel{ID: "observed", Status: "unavailable", Reason: "overseas observed cardinality is unavailable with typed Geo/operator filters"}, nil
	}
	plan, err := flowquery.PlanAggregate(req.From, req.To, 0, req.TargetPoints, now)
	if err != nil {
		return flowReportPanel{}, err
	}
	base := flowquery.OverseasRequest{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Metric: req.Metric,
		GeoLevel: flowquery.OverseasGeoCountry, View: view, TopN: req.TopN, IncludeOther: true,
		ExecutionTimeout: s.cfg.Flow.Query.ExecutionTimeout,
		Filters: flowquery.OverseasFilters{
			Directions: req.Filters.Directions, Businesses: req.Filters.Businesses,
			TargetIDs: req.Filters.TargetIDs, DeviceIDs: req.Filters.DeviceIDs, ExporterIDs: req.Filters.ExporterIDs,
		},
	}
	result := flowquery.OverseasResult{GeoLevel: base.GeoLevel, TopN: base.TopN, IncludeOther: base.IncludeOther}
	versions := make(map[string]struct{})
	usesRaw := false
	for _, window := range flowObservedQueryWindows(plan.EffectiveFrom, plan.EffectiveTo, plan.SourceStep) {
		query := base
		query.From, query.To = window[0], window[1]
		if err := s.applyFlowOverseasStorageBoundary(ctx, &query); err != nil {
			return flowReportPanel{}, err
		}
		compiled, err := flowquery.CompileOverseas(flowquery.Scope{AllowedViews: []flowquery.View{view}}, query, now)
		if err != nil {
			return flowReportPanel{}, err
		}
		part, err := s.flowQuery.overseas.Run(ctx, compiled)
		if err != nil {
			return flowReportPanel{}, err
		}
		result.Metric = part.Metric
		usesRaw = usesRaw || compiled.UsesRawFacts
		result.RollupCompleteness.ExpectedBuckets += part.RollupCompleteness.ExpectedBuckets
		result.RollupCompleteness.CoveredBuckets += part.RollupCompleteness.CoveredBuckets
		for _, point := range part.Points {
			// The fixed report uses only observed endpoint counters. Country/region
			// series already have dedicated panels, so do not duplicate them here.
			if point.Kind != flowquery.OverseasRowKPI {
				continue
			}
			result.Points = append(result.Points, point)
			versions[point.DimensionSnapshotID+"\x00"+point.GeoVersion+"\x00"+fmt.Sprint(point.ClassificationVersion)] = struct{}{}
		}
	}
	if result.RollupCompleteness.ExpectedBuckets > 0 {
		result.RollupCompleteness.Ratio = float64(result.RollupCompleteness.CoveredBuckets) / float64(result.RollupCompleteness.ExpectedBuckets)
	}
	result.RollupCompleteness.Complete = result.RollupCompleteness.ExpectedBuckets > 0 &&
		result.RollupCompleteness.CoveredBuckets == result.RollupCompleteness.ExpectedBuckets
	result.VersionCount = uint64(len(versions))
	result.MixedVersions = result.VersionCount > 1
	raw, err := json.Marshal(gin.H{"result": result, "geo_labels": s.flowOverseasGeoLabels(result)})
	if err != nil {
		return flowReportPanel{}, err
	}
	return flowReportPanel{ID: "observed", Status: "ready", Data: raw, Meta: gin.H{
		"source": base.Bucket, "step_seconds": plan.SourceSeconds, "uses_raw": usesRaw,
		"complete_ratio": result.RollupCompleteness.Ratio, "partial": !result.RollupCompleteness.Complete,
	}}, nil
}

const (
	flowObservedBucketsPerQuery = 12
	flowObservedMinQuerySpan    = time.Hour
)

// flowObservedQueryWindows bounds the high-cardinality src/dst-IP scan without
// changing the requested report range. Each result is exact for its source
// bucket and the non-overlapping results are concatenated in chronological order.
func flowObservedQueryWindows(from, to time.Time, sourceStep time.Duration) [][2]time.Time {
	if sourceStep <= 0 || !to.After(from) {
		return nil
	}
	span := flowObservedBucketsPerQuery * sourceStep
	if span < flowObservedMinQuerySpan {
		span = flowObservedMinQuerySpan
	}
	windows := make([][2]time.Time, 0, int((to.Sub(from)+span-1)/span))
	for start := from; start.Before(to); {
		end := start.Add(span)
		if end.After(to) {
			end = to
		}
		windows = append(windows, [2]time.Time{start, end})
		start = end
	}
	return windows
}
