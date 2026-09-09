// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

const flowReportSchemaVersion = uint16(1)

type flowReportKind string

const (
	flowReportOverview   flowReportKind = "overview"
	flowReportDimensions flowReportKind = "dimensions"
	flowReportEndpoints  flowReportKind = "endpoints"
	flowReportOverseas   flowReportKind = "overseas"
	flowReportVPN        flowReportKind = "vpn"
)

type flowReportDisplayMode string

const (
	flowReportValue      flowReportDisplayMode = "value"
	flowReportShare      flowReportDisplayMode = "share"
	flowReportDifference flowReportDisplayMode = "difference"
)

type flowReportPeakWindow struct {
	Days       []uint8 `json:"days"`
	StartLocal string  `json:"start_local"`
	EndLocal   string  `json:"end_local"`
}

// flowReportSpec is the discriminant inside Flow's provider-owned parameters.
// The gateway envelope continues to own tenant, range, limit and value layer;
// common Flow filters stay beside this object so PrepareQuery and AuthorizeQuery
// cannot diverge between Explorer and fixed reports.
type flowReportSpec struct {
	SchemaVersion uint16                       `json:"schema_version"`
	Kind          flowReportKind               `json:"kind"`
	Side          string                       `json:"side,omitempty"`
	GroupBy       flowquery.Dimension          `json:"group_by,omitempty"`
	DisplayMode   flowReportDisplayMode        `json:"display_mode,omitempty"`
	PeakWindows   []flowReportPeakWindow       `json:"peak_windows,omitempty"`
	PanelIDs      []string                     `json:"panel_ids,omitempty"`
	Tables        map[string]*flowTableRequest `json:"tables,omitempty"`
}

type flowReportPanelMeta struct {
	Unit         string            `json:"unit,omitempty"`
	Timezone     string            `json:"timezone,omitempty"`
	StepSeconds  uint32            `json:"step_seconds,omitempty"`
	AsOf         time.Time         `json:"as_of"`
	Versions     map[string]string `json:"versions,omitempty"`
	Completeness QueryCompleteness `json:"completeness"`
}

type flowReportPanel struct {
	ID     string              `json:"id"`
	Status string              `json:"status"`
	Reason string              `json:"reason,omitempty"`
	Data   json.RawMessage     `json:"data,omitempty"`
	Meta   flowReportPanelMeta `json:"meta"`
}

type flowReportData struct {
	SchemaVersion uint16                `json:"schema_version"`
	Kind          flowReportKind        `json:"kind"`
	Side          string                `json:"side,omitempty"`
	DisplayMode   flowReportDisplayMode `json:"display_mode"`
	Range         flowReportRange       `json:"range"`
	Plan          flowReportPlan        `json:"plan"`
	Watermark     flowReportWatermark   `json:"watermark"`
	Versions      map[string]string     `json:"versions,omitempty"`
	Completeness  QueryCompleteness     `json:"completeness"`
	Panels        []flowReportPanel     `json:"panels"`
	Warnings      []string              `json:"warnings,omitempty"`
}

type flowReportRange struct {
	RequestedFrom time.Time `json:"requested_from"`
	RequestedTo   time.Time `json:"requested_to"`
	EffectiveFrom time.Time `json:"effective_from"`
	EffectiveTo   time.Time `json:"effective_to"`
	Timezone      string    `json:"timezone"`
}

type flowReportPlan struct {
	Source         string `json:"source"`
	SourceSeconds  uint32 `json:"source_seconds"`
	DisplaySeconds uint32 `json:"display_seconds"`
}

type flowReportWatermark struct {
	LatestCompleteBucket time.Time `json:"latest_complete_bucket"`
	GeneratedAt          time.Time `json:"generated_at"`
}

type flowReportCapabilities struct {
	SchemaVersion uint16                  `json:"schema_version"`
	Kinds         []flowReportKind        `json:"kinds"`
	EndpointSides []string                `json:"endpoint_sides"`
	DisplayModes  []flowReportDisplayMode `json:"display_modes"`
	TimePresets   []string                `json:"time_presets"`
	Groupings     []flowquery.Dimension   `json:"groupings"`
	Metrics       []flowquery.Metric      `json:"metrics"`
	Categories    []string                `json:"categories"`
	Residuals     []string                `json:"residuals"`
	MaxTopN       uint16                  `json:"max_top_n"`
}

func currentFlowReportCapabilities() flowReportCapabilities {
	return flowReportCapabilities{
		SchemaVersion: flowReportSchemaVersion,
		Kinds:         []flowReportKind{flowReportOverview, flowReportDimensions, flowReportEndpoints, flowReportOverseas, flowReportVPN},
		EndpointSides: []string{"source", "destination"},
		DisplayModes:  []flowReportDisplayMode{flowReportValue, flowReportShare, flowReportDifference},
		TimePresets: []string{
			"5m", "15m", "30m", "1h", "3h", "6h", "12h", "24h", "2d", "7d", "30d",
			"3mo", "6mo", "1y", "today", "this_week", "this_month", "previous_month", "custom",
		},
		Groupings: []flowquery.Dimension{
			flowquery.DimensionCategory, flowquery.DimensionGeoProvince, flowquery.DimensionGeoCity,
			flowquery.DimensionISP, flowquery.DimensionGeoCountry, flowquery.DimensionASN,
			flowquery.DimensionBusiness, flowquery.DimensionProtocol,
		},
		Metrics: []flowquery.Metric{
			flowquery.MetricEstimatedBPS, flowquery.MetricEstimatedBytes, flowquery.MetricEstimatedPPS,
			flowquery.MetricEstimatedPackets, flowquery.MetricRawBitsPerSecond, flowquery.MetricRawBytes,
			flowquery.MetricRawPacketsSecond, flowquery.MetricRawPackets, flowquery.MetricReceivedRecords,
		},
		Categories: []string{
			"on_net_local_city", "on_net_cross_city", "on_net_cross_province",
			"off_net_in_province", "off_net_cross_province", "overseas",
		},
		Residuals: []string{"unknown", "internal", "transit", "ambiguous"},
		MaxTopN:   100,
	}
}

func normalizeFlowReportSpec(spec *flowReportSpec, parameters *flowAggregateQueryParameters) error {
	if spec == nil || parameters == nil {
		return errors.New("report is required")
	}
	if spec.SchemaVersion != flowReportSchemaVersion {
		return errors.New("report.schema_version must be 1")
	}
	switch spec.Kind {
	case flowReportOverview, flowReportDimensions, flowReportEndpoints, flowReportOverseas, flowReportVPN:
	default:
		return errors.New("report.kind is unsupported")
	}
	if spec.DisplayMode == "" {
		spec.DisplayMode = flowReportValue
	}
	if spec.DisplayMode != flowReportValue && spec.DisplayMode != flowReportShare && spec.DisplayMode != flowReportDifference {
		return errors.New("report.display_mode must be value, share, or difference")
	}
	if parameters.Metric == "" {
		parameters.Metric = flowquery.MetricEstimatedBPS
	}
	if spec.Kind == flowReportVPN && parameters.Metric != flowquery.MetricEstimatedBytes {
		return errors.New("VPN reports require metric=estimated_bytes so traffic ratios use one additive unit")
	}
	if parameters.TopN == 0 {
		parameters.TopN = 20
	}
	if parameters.TopN > 100 {
		return errors.New("top_n must be 1..100")
	}
	if parameters.TargetPoints == 0 {
		parameters.TargetPoints = flowquery.DefaultTargetPoints
	}
	if parameters.Timezone == "" {
		parameters.Timezone = "UTC"
	}
	if _, err := time.LoadLocation(parameters.Timezone); err != nil {
		return errors.New("timezone is not a valid IANA location")
	}
	if len(spec.PeakWindows) > 8 {
		return errors.New("report.peak_windows accepts at most 8 windows")
	}
	for index, window := range spec.PeakWindows {
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
	if spec.Kind == flowReportEndpoints {
		if spec.Side != "source" && spec.Side != "destination" {
			return errors.New("report.side must be source or destination for endpoints")
		}
	} else if spec.Side != "" {
		return errors.New("report.side is only valid for endpoints")
	}
	if spec.Kind == flowReportDimensions {
		if spec.GroupBy == "" {
			spec.GroupBy = flowquery.DimensionCategory
		}
		if !flowReportGroupingAllowed(spec.GroupBy) {
			return errors.New("report.group_by is unsupported")
		}
	} else if spec.GroupBy != "" {
		return errors.New("report.group_by is only valid for dimensions")
	}
	if parameters.Table != nil && spec.Kind != flowReportEndpoints {
		return errors.New("table is only valid for endpoint reports")
	}
	allowedPanels := flowReportPanelIDs(*spec)
	if len(spec.PanelIDs) > len(allowedPanels) {
		return errors.New("report.panel_ids contains too many panels")
	}
	allowed := make(map[string]struct{}, len(allowedPanels))
	for _, panelID := range allowedPanels {
		allowed[panelID] = struct{}{}
	}
	seenPanels := make(map[string]struct{}, len(spec.PanelIDs))
	for _, panelID := range spec.PanelIDs {
		if _, ok := allowed[panelID]; !ok {
			return fmt.Errorf("report.panel_ids contains unsupported panel %q", panelID)
		}
		if _, duplicate := seenPanels[panelID]; duplicate {
			return fmt.Errorf("report.panel_ids contains duplicate panel %q", panelID)
		}
		seenPanels[panelID] = struct{}{}
	}
	if spec.Kind == flowReportEndpoints && len(spec.PanelIDs) != 0 {
		if _, hasEndpointPage := seenPanels["endpoint"]; !hasEndpointPage {
			for _, dependent := range []string{"endpoint_in", "endpoint_out", "endpoint_category_in", "endpoint_category_out", "endpoint_business"} {
				if _, selected := seenPanels[dependent]; selected {
					return errors.New("endpoint report detail panels require the endpoint page panel")
				}
			}
		}
	}
	if err := normalizeFlowReportTables(spec); err != nil {
		return err
	}
	return nil
}

func flowReportPanelIDs(spec flowReportSpec) []string {
	switch spec.Kind {
	case flowReportOverview:
		return []string{"total", "category_in", "category_out", "business_category_in", "business_category_out"}
	case flowReportDimensions:
		return []string{"total", "dimension_in", "dimension_out"}
	case flowReportEndpoints:
		return []string{"total", "endpoint", "endpoint_in", "endpoint_out", "endpoint_category_in", "endpoint_category_out", "endpoint_business"}
	case flowReportOverseas:
		return []string{
			"total", "country_in", "country_out", "region_in", "region_out", "asn_in", "asn_out",
			"remote_port_in", "remote_port_out", "protocol_in", "protocol_out", "observed", "vpn_share",
		}
	case flowReportVPN:
		return []string{"total", "vpn_findings", "vpn_rule_publication", "vpn_probe_timeline"}
	default:
		return nil
	}
}

func flowReportPanelSelected(spec flowReportSpec, panelID string) bool {
	if len(spec.PanelIDs) == 0 {
		return true
	}
	for _, selected := range spec.PanelIDs {
		if selected == panelID {
			return true
		}
	}
	return false
}

func flowReportGroupingAllowed(dimension flowquery.Dimension) bool {
	for _, candidate := range currentFlowReportCapabilities().Groupings {
		if dimension == candidate {
			return true
		}
	}
	return false
}

func (p ClickHouseFlowQueryProvider) queryReport(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
) (QueryProviderResult, error) {
	frozenNow := p.now()
	composer := p
	composer.Now = func() time.Time { return frozenNow }

	requests := reportPanelQueries(parameters)
	panels := make([]flowReportPanel, 0, len(requests)+1)
	versions := map[string]string{}
	warnings := make([]string, 0)
	completeness := QueryCompleteness{CompleteRatio: 1}
	var asOf time.Time
	var step uint32
	var unit string
	reportRange := flowReportRange{RequestedFrom: request.From, RequestedTo: request.To, EffectiveFrom: request.From, EffectiveTo: request.To, Timezone: parameters.Timezone}
	reportPlan := flowReportPlan{}
	var latestCompleteBucket time.Time
	rowCount := uint64(0)
	for _, panelRequest := range requests {
		encoded, err := json.Marshal(panelRequest.Parameters)
		if err != nil {
			return QueryProviderResult{}, fmt.Errorf("encode Flow report panel %s: %w", panelRequest.ID, err)
		}
		subrequest := request
		subrequest.Parameters = encoded
		result, err := composer.Query(ctx, subrequest)
		if err != nil {
			if panelRequest.Optional && flowReportPanelMayBeUnavailable(err) {
				panels = append(panels, flowReportPanel{ID: panelRequest.ID, Status: "unavailable", Reason: flowReportUnavailableReason(err)})
				warnings = append(warnings, panelRequest.ID+": "+flowReportUnavailableReason(err))
				completeness.Partial = true
				continue
			}
			return QueryProviderResult{}, fmt.Errorf("Flow report panel %s: %w", panelRequest.ID, err)
		}
		rows, err := flowReportResultRows(result.Data)
		if err != nil {
			return QueryProviderResult{}, fmt.Errorf("validate Flow report panel %s: %w", panelRequest.ID, err)
		}
		rowCount += rows
		if rowCount > uint64(request.Limit) {
			return QueryProviderResult{}, &QueryGatewayError{
				Code: QueryErrorRowLimit, Message: "Flow report exceeds the query row limit",
				Details: map[string]any{"max_result_rows": request.Limit, "actual_result_rows": rowCount},
			}
		}
		if err := mergeFlowReportVersions(versions, result.Versions); err != nil {
			return QueryProviderResult{}, err
		}
		mergeFlowReportCompleteness(&completeness, result.Completeness)
		if result.AsOf.After(asOf) {
			asOf = result.AsOf
		}
		if step == 0 {
			step = result.StepSeconds
		} else if result.StepSeconds != 0 && result.StepSeconds != step {
			warnings = append(warnings, "panels use different presentation steps selected from the same frozen range")
		}
		if unit == "" {
			unit = result.Unit
		} else if result.Unit != "" && result.Unit != unit {
			return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorIncomplete, Message: "Flow report panels returned different metric units"}
		}
		panelPlan, panelLastBucket := flowReportExecutionMetadata(result.Data)
		if reportPlan.Source == "" {
			reportPlan = panelPlan
		} else if panelPlan.Source != "" && (reportPlan.Source != panelPlan.Source || reportPlan.SourceSeconds != panelPlan.SourceSeconds) {
			reportPlan.Source = "mixed"
			reportPlan.SourceSeconds = 0
		}
		if latestCompleteBucket.IsZero() || (!panelLastBucket.IsZero() && panelLastBucket.Before(latestCompleteBucket)) {
			latestCompleteBucket = panelLastBucket
		}
		panels = append(panels, flowReportPanel{
			ID: panelRequest.ID, Status: "ready", Data: result.Data,
			Meta: flowReportPanelMeta{
				Unit: result.Unit, Timezone: result.Timezone, StepSeconds: result.StepSeconds,
				AsOf: result.AsOf, Versions: result.Versions, Completeness: result.Completeness,
			},
		})
	}
	if parameters.Report.Kind == flowReportEndpoints {
		addresses := flowReportEndpointAddresses(panels, "endpoint")
		endpointMeta := flowReportPanelMetadata(panels, "endpoint")
		for _, direction := range []string{"in", "out"} {
			endpointPanelID := "endpoint_" + direction
			if flowReportPanelSelected(*parameters.Report, endpointPanelID) {
				endpointPanel, endpointErr := composer.endpointDirectionReportPanel(ctx, request, parameters, direction, addresses, endpointMeta)
				if endpointErr != nil {
					return QueryProviderResult{}, fmt.Errorf("Flow report panel %s: %w", endpointPanelID, endpointErr)
				}
				rows, countErr := flowReportResultRows(endpointPanel.Data)
				if countErr != nil {
					return QueryProviderResult{}, fmt.Errorf("validate Flow report panel %s: %w", endpointPanelID, countErr)
				}
				rowCount += rows
				if rowCount > uint64(request.Limit) {
					return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "Flow report exceeds the query row limit"}
				}
				if err := mergeFlowReportVersions(versions, endpointPanel.Meta.Versions); err != nil {
					return QueryProviderResult{}, err
				}
				mergeFlowReportCompleteness(&completeness, endpointPanel.Meta.Completeness)
				if endpointPanel.Meta.AsOf.After(asOf) {
					asOf = endpointPanel.Meta.AsOf
				}
				panels = append(panels, endpointPanel)
			}
			panelID := "endpoint_category_" + direction
			if !flowReportPanelSelected(*parameters.Report, panelID) {
				continue
			}
			categoryPanel, categoryErr := composer.endpointCategoryReportPanel(ctx, request, parameters, direction, addresses, endpointMeta)
			if categoryErr != nil {
				return QueryProviderResult{}, fmt.Errorf("Flow report panel %s: %w", panelID, categoryErr)
			}
			rows, countErr := flowReportResultRows(categoryPanel.Data)
			if countErr != nil {
				return QueryProviderResult{}, fmt.Errorf("validate Flow report panel %s: %w", panelID, countErr)
			}
			rowCount += rows
			if rowCount > uint64(request.Limit) {
				return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "Flow report exceeds the query row limit"}
			}
			if err := mergeFlowReportVersions(versions, categoryPanel.Meta.Versions); err != nil {
				return QueryProviderResult{}, err
			}
			mergeFlowReportCompleteness(&completeness, categoryPanel.Meta.Completeness)
			if categoryPanel.Meta.AsOf.After(asOf) {
				asOf = categoryPanel.Meta.AsOf
			}
			panels = append(panels, categoryPanel)
		}
		if flowReportPanelSelected(*parameters.Report, "endpoint_business") {
			businessPanel, businessErr := composer.endpointBusinessReportPanel(ctx, request, parameters, addresses, endpointMeta)
			if businessErr != nil {
				return QueryProviderResult{}, fmt.Errorf("Flow report panel endpoint_business: %w", businessErr)
			}
			if businessPanel.Status == "unavailable" {
				warnings = append(warnings, businessPanel.ID+": "+businessPanel.Reason)
				completeness.Partial = true
			} else {
				rows, countErr := flowReportResultRows(businessPanel.Data)
				if countErr != nil {
					return QueryProviderResult{}, fmt.Errorf("validate Flow report panel endpoint_business: %w", countErr)
				}
				rowCount += rows
				if rowCount > uint64(request.Limit) {
					return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "Flow report exceeds the query row limit"}
				}
				if err := mergeFlowReportVersions(versions, businessPanel.Meta.Versions); err != nil {
					return QueryProviderResult{}, err
				}
				mergeFlowReportCompleteness(&completeness, businessPanel.Meta.Completeness)
			}
			panels = append(panels, businessPanel)
		}
		if parameters.Table != nil {
			var composeErr error
			panels, composeErr = composeFlowEndpointReportTable(panels, *parameters.Table)
			if composeErr != nil {
				return QueryProviderResult{}, fmt.Errorf("compose Flow endpoint report table: %w", composeErr)
			}
		}
	}
	if parameters.Report.Kind == flowReportOverview {
		if table := parameters.Report.Tables["business_matrix"]; table != nil {
			var composeErr error
			panels, composeErr = composeFlowBusinessMatrixTable(panels, *table)
			if composeErr != nil {
				return QueryProviderResult{}, fmt.Errorf("compose Flow business matrix table: %w", composeErr)
			}
		}
	}
	if parameters.Report.Kind == flowReportOverseas && flowReportPanelSelected(*parameters.Report, "observed") {
		observed, err := composer.overseasObservedReportPanel(ctx, request, parameters)
		if err != nil {
			return QueryProviderResult{}, err
		}
		panels = append(panels, observed)
		if observed.Status == "unavailable" {
			warnings = append(warnings, observed.ID+": "+observed.Reason)
			completeness.Partial = true
		} else {
			rows, countErr := flowReportResultRows(observed.Data)
			if countErr != nil {
				return QueryProviderResult{}, fmt.Errorf("validate Flow report panel %s: %w", observed.ID, countErr)
			}
			rowCount += rows
			if rowCount > uint64(request.Limit) {
				return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "Flow report exceeds the query row limit"}
			}
			if err := mergeFlowReportVersions(versions, observed.Meta.Versions); err != nil {
				return QueryProviderResult{}, err
			}
			mergeFlowReportCompleteness(&completeness, observed.Meta.Completeness)
			if observed.Meta.AsOf.After(asOf) {
				asOf = observed.Meta.AsOf
			}
		}
	}
	if parameters.Report.Kind == flowReportOverseas && flowReportPanelSelected(*parameters.Report, "vpn_share") {
		vpnShare, err := composer.overseasVPNShareReportPanel(ctx, request, parameters)
		if err != nil {
			return QueryProviderResult{}, fmt.Errorf("Flow report panel vpn_share: %w", err)
		}
		panels = append(panels, vpnShare)
		if vpnShare.Status == "unavailable" {
			warnings = append(warnings, vpnShare.ID+": "+vpnShare.Reason)
			completeness.Partial = true
		} else {
			rows, countErr := flowReportResultRows(vpnShare.Data)
			if countErr != nil {
				return QueryProviderResult{}, fmt.Errorf("validate Flow report panel %s: %w", vpnShare.ID, countErr)
			}
			rowCount += rows
			if rowCount > uint64(request.Limit) {
				return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "Flow report exceeds the query row limit"}
			}
			if err := mergeFlowReportVersions(versions, vpnShare.Meta.Versions); err != nil {
				return QueryProviderResult{}, err
			}
			mergeFlowReportCompleteness(&completeness, vpnShare.Meta.Completeness)
			if vpnShare.Meta.AsOf.After(asOf) {
				asOf = vpnShare.Meta.AsOf
			}
		}
	}

	if parameters.Report.Kind == flowReportVPN && flowReportPanelSelected(*parameters.Report, "vpn_findings") {
		vpnPanel, err := composer.vpnReportPanel(ctx, request, parameters)
		if err != nil {
			return QueryProviderResult{}, err
		}
		panels = append(panels, vpnPanel)
		if vpnPanel.Status == "unavailable" {
			warnings = append(warnings, vpnPanel.ID+": "+vpnPanel.Reason)
			completeness.Partial = true
		} else {
			rows, countErr := flowVPNReportResultRows(vpnPanel.Data)
			if countErr != nil {
				return QueryProviderResult{}, fmt.Errorf("validate Flow report panel %s: %w", vpnPanel.ID, countErr)
			}
			rowCount += rows
			if rowCount > uint64(request.Limit) {
				return QueryProviderResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "Flow report exceeds the query row limit"}
			}
			if err := mergeFlowReportVersions(versions, vpnPanel.Meta.Versions); err != nil {
				return QueryProviderResult{}, err
			}
			mergeFlowReportCompleteness(&completeness, vpnPanel.Meta.Completeness)
			if vpnPanel.Meta.AsOf.After(asOf) {
				asOf = vpnPanel.Meta.AsOf
			}
		}
	}
	if parameters.Report.Kind == flowReportVPN {
		for _, dependency := range []flowReportPanel{
			{ID: "vpn_rule_publication", Status: "unavailable", Reason: "immutable VPN rule-set publication is not active"},
			{ID: "vpn_probe_timeline", Status: "unavailable", Reason: "authorized active-probe orchestration is not configured"},
		} {
			if !flowReportPanelSelected(*parameters.Report, dependency.ID) {
				continue
			}
			panels = append(panels, dependency)
			warnings = append(warnings, dependency.ID+": "+dependency.Reason)
			completeness.Partial = true
		}
	}
	if asOf.IsZero() {
		asOf = frozenNow
	}
	if completeness.AvailableFrom != nil {
		reportRange.EffectiveFrom = *completeness.AvailableFrom
	}
	if completeness.AvailableTo != nil {
		reportRange.EffectiveTo = *completeness.AvailableTo
	}
	reportPlan.DisplaySeconds = step
	data, err := json.Marshal(flowReportData{
		SchemaVersion: flowReportSchemaVersion, Kind: parameters.Report.Kind, Side: parameters.Report.Side,
		DisplayMode: parameters.Report.DisplayMode, Range: reportRange, Plan: reportPlan,
		Watermark: flowReportWatermark{LatestCompleteBucket: latestCompleteBucket, GeneratedAt: asOf},
		Versions:  versions, Completeness: completeness, Panels: panels, Warnings: uniqueSortedStrings(warnings),
	})
	if err != nil {
		return QueryProviderResult{}, fmt.Errorf("marshal Flow report: %w", err)
	}
	return QueryProviderResult{
		Data: data, Unit: unit, Timezone: parameters.Timezone, StepSeconds: step,
		AsOf: asOf, Versions: versions, Completeness: completeness,
	}, nil
}

func (p ClickHouseFlowQueryProvider) overseasObservedReportPanel(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
) (flowReportPanel, error) {
	if p.OverseasRunner == nil {
		return flowReportPanel{ID: "observed", Status: "unavailable", Reason: "observed overseas cardinality query is not configured"}, nil
	}
	if parameters.Report != nil && len(parameters.Report.PeakWindows) != 0 {
		return flowReportPanel{
			ID: "observed", Status: "unavailable",
			Reason: "observed overseas cardinality is not materialized by local peak window",
		}, nil
	}
	if parameters.Filter != nil || parameters.OperatorSelection != nil {
		return flowReportPanel{
			ID: "observed", Status: "unavailable",
			Reason: "observed overseas IP and host cardinality is unavailable with Geo/operator typed filters",
		}, nil
	}
	view, err := flowView(request.ValueLayer)
	if err != nil {
		return flowReportPanel{}, err
	}
	plan, err := flowquery.PlanAggregate(
		request.From, request.To, time.Duration(request.StepSeconds)*time.Second,
		parameters.TargetPoints, p.now(),
	)
	if err != nil {
		return flowReportPanel{}, mapFlowQueryError(err)
	}
	overseasRequest := flowquery.OverseasRequest{
		From: plan.EffectiveFrom, To: plan.EffectiveTo, Bucket: plan.Source, Metric: parameters.Metric,
		GeoLevel: flowquery.OverseasGeoCountry, View: view, TopN: parameters.TopN, IncludeOther: true,
		Filters: flowquery.OverseasFilters{
			Directions: parameters.Filters.Directions, Businesses: parameters.Filters.Businesses,
			TargetIDs: parameters.Filters.TargetIDs, DeviceIDs: parameters.Filters.DeviceIDs, ExporterIDs: parameters.Filters.ExporterIDs,
		},
	}
	if p.StorageLifecycle != nil {
		overseasRequest.StorageV2 = true
		overseasRequest.ArchiveThrough = plan.EffectiveFrom
		if plan.Source == flowquery.BucketOneHour {
			boundary, boundaryErr := p.StorageLifecycle.FlowStorageArchiveThrough(ctx, request.TenantID, plan.EffectiveFrom, plan.EffectiveTo)
			if boundaryErr != nil {
				return flowReportPanel{}, fmt.Errorf("resolve Flow overseas storage boundary: %w", boundaryErr)
			}
			overseasRequest.ArchiveThrough = boundary
		}
	}
	compiled, err := flowquery.CompileOverseas(
		flowquery.Scope{AllowedViews: []flowquery.View{view}}, overseasRequest, p.now(),
	)
	if err != nil {
		return flowReportPanel{}, mapFlowQueryError(err)
	}
	if compiled.EstimatedRows > uint64(request.Limit) {
		return flowReportPanel{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "observed overseas report exceeds the query row limit"}
	}
	result, err := p.OverseasRunner.Run(ctx, compiled)
	if err != nil {
		return flowReportPanel{}, mapFlowQueryError(err)
	}
	data, err := json.Marshal(result)
	if err != nil {
		return flowReportPanel{}, fmt.Errorf("marshal observed overseas report: %w", err)
	}
	from, to := compiled.From, compiled.To
	completeness := QueryCompleteness{
		AvailableFrom: &from, AvailableTo: &to, CompleteRatio: result.RollupCompleteness.Ratio,
		Partial: !result.RollupCompleteness.Complete, UnknownRatio: overseasUnknownSamplingRatio(result.Points),
	}
	if completeness.Partial {
		completeness.Warnings = []string{"one or more closed overseas rollup buckets are not available"}
	}
	if result.MixedVersions {
		completeness.Warnings = append(completeness.Warnings, "observed overseas result contains multiple versions")
	}
	return flowReportPanel{
		ID: "observed", Status: "ready", Data: data,
		Meta: flowReportPanelMeta{
			Unit: result.Metric.Unit, Timezone: parameters.Timezone, StepSeconds: uint32(compiled.BucketDuration / time.Second),
			AsOf: overseasResultAsOf(result.Points, p.now()), Versions: overseasResultVersions(result.Points), Completeness: completeness,
		},
	}, nil
}

func overseasUnknownSamplingRatio(points []flowquery.OverseasPoint) float64 {
	var received, unknown uint64
	for _, point := range points {
		if point.Kind != flowquery.OverseasRowKPI || point.Direction != flowquery.OverseasDirectionCombined || point.IPFamily != flowquery.OverseasIPFamilyAll {
			continue
		}
		received += point.ReceivedRecords
		unknown += point.UnknownSamplingRecords
	}
	if received == 0 {
		return 0
	}
	return float64(unknown) / float64(received)
}

func overseasResultAsOf(points []flowquery.OverseasPoint, fallback time.Time) time.Time {
	asOf := time.Time{}
	for _, point := range points {
		if point.GeneratedAt.After(asOf) {
			asOf = point.GeneratedAt
		}
	}
	if asOf.IsZero() {
		return fallback.UTC()
	}
	return asOf.UTC()
}

func overseasResultVersions(points []flowquery.OverseasPoint) map[string]string {
	versions := map[string]map[string]struct{}{
		"dimension_snapshot_id": {}, "geo_version": {}, "classification_version": {},
	}
	for _, point := range points {
		versions["dimension_snapshot_id"][point.DimensionSnapshotID] = struct{}{}
		versions["geo_version"][point.GeoVersion] = struct{}{}
		versions["classification_version"][strconv.FormatUint(uint64(point.ClassificationVersion), 10)] = struct{}{}
	}
	result := map[string]string{}
	for key, values := range versions {
		if len(values) != 1 {
			continue
		}
		for value := range values {
			result[key] = value
		}
	}
	return result
}

type flowOverseasVPNSharePoint struct {
	Direction   string    `json:"direction"`
	VPNBytes    uint64    `json:"vpn_bytes"`
	TotalBytes  float64   `json:"total_bytes"`
	Ratio       *float64  `json:"ratio,omitempty"`
	UnknownGeo  uint64    `json:"unknown_geo_bytes"`
	FindingRows uint64    `json:"finding_rows"`
	GeneratedAt time.Time `json:"generated_at"`
}

func (p ClickHouseFlowQueryProvider) overseasVPNShareReportPanel(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
) (flowReportPanel, error) {
	if p.VPNFindings == nil {
		return flowReportPanel{ID: "vpn_share", Status: "unavailable", Reason: "VPN finding materialization is not configured"}, nil
	}
	query := parameters
	query.Report = nil
	query.Metric = flowquery.MetricEstimatedBytes
	query.Dimension = flowquery.DimensionTotal
	query.Dimensions = nil
	query.DirectionSplit = true
	query.Table = nil
	query.TopN = 1
	query.IncludeOther = false
	query.TimeWindows = reportTimeWindows(parameters.Report.PeakWindows)
	query.Filters.Categories = []string{"overseas"}
	query.Filters.Directions = nil
	encoded, err := json.Marshal(query)
	if err != nil {
		return flowReportPanel{}, err
	}
	subrequest := request
	subrequest.Parameters = encoded
	denominator, err := p.Query(ctx, subrequest)
	if err != nil {
		if flowReportPanelMayBeUnavailable(err) {
			return flowReportPanel{ID: "vpn_share", Status: "unavailable", Reason: flowReportUnavailableReason(err)}, nil
		}
		return flowReportPanel{}, err
	}
	points, err := flowReportDimensionPoints(denominator.Data)
	if err != nil {
		return flowReportPanel{}, err
	}
	totals := map[string]float64{"in": 0, "out": 0}
	for _, point := range points {
		switch strings.ToLower(point.DimensionValue) {
		case "in", "inbound":
			totals["in"] += point.Value
		case "out", "outbound":
			totals["out"] += point.Value
		}
	}
	findings, err := p.VPNFindings.ListVPNFindingsForExport(ctx, request.TenantID, VPNFindingListFilter{
		From: request.From, To: request.To, SortBy: "window_end", SortDirection: "asc",
	}, maxVPNFindingExportRows)
	if err != nil {
		return flowReportPanel{}, fmt.Errorf("load overseas VPN findings: %w", err)
	}
	if len(findings) > int(maxVPNFindingExportRows) {
		return flowReportPanel{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "overseas VPN findings exceed the fixed report scan budget"}
	}
	if len(parameters.Report.PeakWindows) != 0 {
		windows := reportTimeWindows(parameters.Report.PeakWindows)
		selected := findings[:0]
		for _, finding := range findings {
			if flowquery.InLocalTimeWindows(finding.WindowEnd, windows, parameters.Timezone) {
				selected = append(selected, finding)
			}
		}
		findings = selected
	}
	findingVersions, versionErr := flowReportFindingVersions(findings)
	if versionErr != nil {
		return flowReportPanel{ID: "vpn_share", Status: "unavailable", Reason: versionErr.Error()}, nil
	}
	versions := make(map[string]string, len(denominator.Versions)+len(findingVersions))
	if err := mergeFlowReportVersions(versions, denominator.Versions); err != nil {
		return flowReportPanel{}, err
	}
	if err := mergeFlowReportVersions(versions, findingVersions); err != nil {
		return flowReportPanel{ID: "vpn_share", Status: "unavailable", Reason: "VPN numerator and overseas denominator resolved different immutable versions"}, nil
	}
	vpn := map[string]uint64{"in": 0, "out": 0}
	var unknownGeo uint64
	var minimumComplete = 1.0
	for _, finding := range findings {
		if finding.CompleteRatio < minimumComplete {
			minimumComplete = finding.CompleteRatio
		}
		bytes := finding.LocalToRemoteBytes + finding.RemoteToLocalBytes
		country := strings.ToUpper(strings.TrimSpace(finding.RemoteCountry))
		if country == "" || country == "_UNASSIGNED" || country == "UNKNOWN" {
			unknownGeo += bytes
			continue
		}
		if country == "CN" {
			continue
		}
		vpn["in"] += finding.RemoteToLocalBytes
		vpn["out"] += finding.LocalToRemoteBytes
	}
	makePoint := func(direction string, numerator uint64, total float64) flowOverseasVPNSharePoint {
		point := flowOverseasVPNSharePoint{
			Direction: direction, VPNBytes: numerator, TotalBytes: total, UnknownGeo: unknownGeo,
			FindingRows: uint64(len(findings)), GeneratedAt: denominator.AsOf,
		}
		if total > 0 {
			ratio := float64(numerator) / total
			point.Ratio = &ratio
		}
		return point
	}
	rows := []flowOverseasVPNSharePoint{
		makePoint("in", vpn["in"], totals["in"]),
		makePoint("out", vpn["out"], totals["out"]),
		makePoint("combined", vpn["in"]+vpn["out"], totals["in"]+totals["out"]),
	}
	data, err := json.Marshal(struct {
		Points []flowOverseasVPNSharePoint `json:"points"`
	}{Points: rows})
	if err != nil {
		return flowReportPanel{}, err
	}
	completeness := denominator.Completeness
	if minimumComplete < completeness.CompleteRatio {
		completeness.CompleteRatio = minimumComplete
	}
	if minimumComplete < 1 || unknownGeo > 0 {
		completeness.Partial = true
	}
	if unknownGeo > 0 {
		completeness.Warnings = uniqueSortedStrings(append(completeness.Warnings, "VPN findings with unknown remote Geo are excluded from the overseas numerator"))
	}
	return flowReportPanel{
		ID: "vpn_share", Status: "ready", Data: data,
		Meta: flowReportPanelMeta{
			Unit: "ratio", Timezone: parameters.Timezone, StepSeconds: denominator.StepSeconds,
			AsOf: denominator.AsOf, Versions: versions, Completeness: completeness,
		},
	}, nil
}

func flowReportFindingVersions(findings []VPNFinding) (map[string]string, error) {
	values := map[string]map[string]struct{}{
		"dimension_snapshot_id": {}, "geo_version": {}, "classification_version": {},
	}
	for _, finding := range findings {
		if finding.DimensionSnapshotID != "" {
			values["dimension_snapshot_id"][finding.DimensionSnapshotID] = struct{}{}
		}
		if finding.GeoVersion != "" {
			values["geo_version"][finding.GeoVersion] = struct{}{}
		}
		if finding.ClassificationVersion != 0 {
			values["classification_version"][strconv.FormatUint(finding.ClassificationVersion, 10)] = struct{}{}
		}
	}
	result := map[string]string{}
	for key, versions := range values {
		if len(versions) > 1 {
			return nil, fmt.Errorf("VPN findings span multiple %s values", key)
		}
		for value := range versions {
			result[key] = value
		}
	}
	return result, nil
}

type flowReportPanelQuery struct {
	ID         string
	Optional   bool
	Parameters flowAggregateQueryParameters
}

func reportPanelQueries(parameters flowAggregateQueryParameters) []flowReportPanelQuery {
	base := parameters
	base.Report = nil
	base.TimeWindows = reportTimeWindows(parameters.Report.PeakWindows)
	base.Dimension, base.Dimensions = "", nil
	base.DirectionSplit = false
	base.Table = nil
	base.IncludeOther = true
	if base.TopN == 0 {
		base.TopN = 20
	}

	directionTotal := base
	directionTotal.Dimension = flowquery.DimensionTotal
	directionTotal.TopN = 1
	directionTotal.IncludeOther = false
	directionTotal.DirectionSplit = true
	queries := []flowReportPanelQuery{{ID: "total", Parameters: directionTotal}}

	addDirections := func(prefix string, dimension flowquery.Dimension, table *flowTableRequest, optional bool, filters flowquery.Filters) {
		for _, direction := range []string{"in", "out"} {
			query := base
			query.Dimension = dimension
			query.Filters = filters
			query.Filters.Directions = []string{direction}
			query.Table = table
			queries = append(queries, flowReportPanelQuery{ID: prefix + "_" + direction, Optional: optional, Parameters: query})
		}
	}

	switch parameters.Report.Kind {
	case flowReportOverview:
		addDirections("category", flowquery.DimensionCategory, nil, false, parameters.Filters)
		for _, direction := range []string{"in", "out"} {
			query := base
			query.Dimensions = []flowquery.Dimension{flowquery.DimensionBusiness, flowquery.DimensionCategory}
			query.Filters.Directions = []string{direction}
			queries = append(queries, flowReportPanelQuery{ID: "business_category_" + direction, Optional: true, Parameters: query})
		}
	case flowReportDimensions:
		addDirections("dimension", parameters.Report.GroupBy, nil, false, parameters.Filters)
	case flowReportEndpoints:
		dimension := flowquery.DimensionSourceIP
		if parameters.Report.Side == "destination" {
			dimension = flowquery.DimensionDestinationIP
		}
		endpoint := base
		endpoint.Dimension = dimension
		endpoint.Filters = parameters.Filters
		// Fetch the complete bounded TopN candidate set once. Pagination,
		// search, sorting and filters are applied after the exact direction and
		// category panels have enriched those same endpoint identities.
		endpoint.IncludeOther = false
		endpoint.Table = &flowTableRequest{SortBy: "maximum", SortDirection: "desc", Limit: endpoint.TopN}
		queries = append(queries, flowReportPanelQuery{ID: "endpoint", Parameters: endpoint})
	case flowReportOverseas:
		overseasFilters := parameters.Filters
		overseasFilters.Categories = []string{"overseas"}
		directionTotal.Filters = overseasFilters
		queries[0].Parameters = directionTotal
		for _, dimension := range []struct {
			id    string
			value flowquery.Dimension
		}{
			{"country", flowquery.DimensionGeoCountry}, {"region", flowquery.DimensionGeoRegion},
			{"asn", flowquery.DimensionASN}, {"remote_port", flowquery.DimensionRemotePort},
			{"protocol", flowquery.DimensionProtocol},
		} {
			addDirections(dimension.id, dimension.value, nil, false, overseasFilters)
		}
	case flowReportVPN:
		// The denominator is queried from the same frozen range. The findings
		// panel is appended from MySQL and clearly exposes its own generation.
	}
	if len(parameters.Report.PanelIDs) == 0 {
		return queries
	}
	selected := queries[:0]
	for _, query := range queries {
		if flowReportPanelSelected(*parameters.Report, query.ID) {
			selected = append(selected, query)
		}
	}
	return selected
}

func reportTimeWindows(windows []flowReportPeakWindow) []flowquery.LocalTimeWindow {
	result := make([]flowquery.LocalTimeWindow, len(windows))
	for index, window := range windows {
		result[index] = flowquery.LocalTimeWindow{
			Days: append([]uint8(nil), window.Days...), StartLocal: window.StartLocal, EndLocal: window.EndLocal,
		}
	}
	return result
}

func flowReportPanelMayBeUnavailable(err error) bool {
	if errors.Is(err, ErrQueryProviderUnavailable) {
		return true
	}
	var gatewayErr *QueryGatewayError
	if !errors.As(err, &gatewayErr) {
		return false
	}
	return gatewayErr.Code == QueryErrorRangeLimit || gatewayErr.Code == QueryErrorIncomplete || gatewayErr.Code == QueryErrorProviderUnavailable
}

func flowReportUnavailableReason(err error) string {
	var gatewayErr *QueryGatewayError
	if errors.As(err, &gatewayErr) && gatewayErr.Message != "" {
		return gatewayErr.Message
	}
	return "report data dependency is unavailable"
}

func flowReportEndpointAddresses(panels []flowReportPanel, panelID string) []string {
	for _, panel := range panels {
		if panel.ID != panelID || panel.Status != "ready" {
			continue
		}
		var envelope struct {
			Table *flowTablePage `json:"table"`
		}
		if json.Unmarshal(panel.Data, &envelope) != nil || envelope.Table == nil {
			points, err := flowReportDimensionPoints(panel.Data)
			if err != nil {
				return nil
			}
			seen := make(map[string]struct{}, len(points))
			for _, point := range points {
				if point.DimensionValue != "" && !point.Other {
					seen[point.DimensionValue] = struct{}{}
				}
			}
			addresses := make([]string, 0, len(seen))
			for address := range seen {
				addresses = append(addresses, address)
			}
			sort.Strings(addresses)
			return addresses
		}
		addresses := make([]string, 0, len(envelope.Table.Items))
		for _, item := range envelope.Table.Items {
			if len(item.Path) > 0 && item.Path[0] != "" {
				addresses = append(addresses, item.Path[0])
			}
		}
		sort.Strings(addresses)
		return addresses
	}
	return nil
}

func flowReportPanelMetadata(panels []flowReportPanel, panelID string) flowReportPanelMeta {
	for _, panel := range panels {
		if panel.ID == panelID && panel.Status == "ready" {
			return panel.Meta
		}
	}
	return flowReportPanelMeta{}
}

func (p ClickHouseFlowQueryProvider) endpointDirectionReportPanel(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
	direction string,
	addresses []string,
	baseMeta flowReportPanelMeta,
) (flowReportPanel, error) {
	panelID := "endpoint_" + direction
	dimension := flowquery.DimensionSourceIP
	if parameters.Report.Side == "destination" {
		dimension = flowquery.DimensionDestinationIP
	}
	if len(addresses) == 0 {
		data, err := json.Marshal(struct {
			Points    []flowquery.Point             `json:"points"`
			Metric    flowquery.MetricDefinition    `json:"metric"`
			Dimension flowquery.DimensionDefinition `json:"dimension"`
			Table     flowTablePage                 `json:"table"`
		}{
			Points: []flowquery.Point{}, Metric: flowquery.MetricDefinition{Name: parameters.Metric, Unit: baseMeta.Unit},
			Dimension: flowquery.DimensionDefinition{Kind: dimension, Additive: true},
			Table:     flowTablePage{Items: []flowTableRow{}, Limit: 1, FilterOptions: map[string][]flowTableFilterOption{}},
		})
		return flowReportPanel{ID: panelID, Status: "ready", Data: data, Meta: baseMeta}, err
	}
	query := parameters
	query.Report = nil
	query.Dimension = dimension
	query.Dimensions = nil
	query.DirectionSplit = false
	query.TimeWindows = reportTimeWindows(parameters.Report.PeakWindows)
	query.Filters.Directions = []string{direction}
	query.Filters.DimensionValues = append([]string(nil), addresses...)
	query.TopN = uint16(len(addresses))
	query.IncludeOther = false
	query.Table = &flowTableRequest{SortBy: "dimension", SortDirection: "asc", Limit: uint16(len(addresses))}
	encoded, err := json.Marshal(query)
	if err != nil {
		return flowReportPanel{}, err
	}
	subrequest := request
	subrequest.Parameters = encoded
	result, err := p.Query(ctx, subrequest)
	if err != nil {
		return flowReportPanel{}, err
	}
	return flowReportPanel{
		ID: panelID, Status: "ready", Data: result.Data,
		Meta: flowReportPanelMeta{
			Unit: result.Unit, Timezone: result.Timezone, StepSeconds: result.StepSeconds,
			AsOf: result.AsOf, Versions: result.Versions, Completeness: result.Completeness,
		},
	}, nil
}

func (p ClickHouseFlowQueryProvider) endpointCategoryReportPanel(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
	direction string,
	addresses []string,
	baseMeta flowReportPanelMeta,
) (flowReportPanel, error) {
	type categorySummary struct {
		Address  string  `json:"address"`
		Category string  `json:"category"`
		Total    float64 `json:"total"`
	}
	panelID := "endpoint_category_" + direction
	if len(addresses) == 0 {
		data, err := json.Marshal(struct {
			Points     []flowquery.JointPoint          `json:"points"`
			Summaries  []categorySummary               `json:"summaries"`
			Metric     flowquery.MetricDefinition      `json:"metric"`
			Dimensions []flowquery.DimensionDefinition `json:"dimensions"`
		}{Points: []flowquery.JointPoint{}, Summaries: []categorySummary{}, Metric: flowquery.MetricDefinition{Name: parameters.Metric}})
		return flowReportPanel{ID: panelID, Status: "ready", Data: data, Meta: baseMeta}, err
	}
	dimension := flowquery.DimensionSourceIP
	if parameters.Report.Side == "destination" {
		dimension = flowquery.DimensionDestinationIP
	}
	categories := append(append([]string(nil), currentFlowReportCapabilities().Categories...), currentFlowReportCapabilities().Residuals...)
	combined := make([]flowquery.JointPoint, 0, len(addresses)*len(categories))
	summaries := make([]categorySummary, 0, len(addresses)*len(categories))
	versions := map[string]string{}
	completeness := QueryCompleteness{CompleteRatio: 1}
	var asOf time.Time
	var unit string
	var step uint32
	for _, category := range categories {
		query := parameters
		query.Report = nil
		query.Dimension = dimension
		query.Dimensions = nil
		query.Table = nil
		query.DirectionSplit = false
		query.TimeWindows = reportTimeWindows(parameters.Report.PeakWindows)
		query.Filters.Directions = []string{direction}
		query.Filters.Categories = []string{category}
		query.Filters.DimensionValues = append([]string(nil), addresses...)
		// The endpoint table needs exact whole-window category totals, not a
		// second high-cardinality sparkline. A bounded 20-point presentation
		// keeps the auxiliary result small; the table projection integrates
		// every source bucket into an exact total for each endpoint.
		query.TargetPoints = flowquery.MinTargetPoints
		query.TopN = uint16(len(addresses))
		query.IncludeOther = false
		query.Table = &flowTableRequest{SortBy: "dimension", SortDirection: "asc", Limit: uint16(len(addresses))}
		encoded, err := json.Marshal(query)
		if err != nil {
			return flowReportPanel{}, err
		}
		subrequest := request
		subrequest.Parameters = encoded
		result, err := p.Query(ctx, subrequest)
		if err != nil {
			return flowReportPanel{}, err
		}
		points, err := flowReportDimensionPoints(result.Data)
		if err != nil {
			return flowReportPanel{}, err
		}
		for _, point := range points {
			combined = append(combined, flowquery.JointPoint{
				Bucket: point.Bucket, DimensionValues: []string{point.DimensionValue, category}, Other: point.Other,
				DimensionSnapshotID: point.DimensionSnapshotID, GeoVersion: point.GeoVersion,
				ClassificationVersion: point.ClassificationVersion, Value: point.Value,
				ReceivedRecords: point.ReceivedRecords, UnknownSamplingRecords: point.UnknownSamplingRecords,
				QualityRecords: point.QualityRecords, ObservedAt: point.GeneratedAt,
			})
		}
		var envelope struct {
			Table flowTablePage `json:"table"`
		}
		if err := json.Unmarshal(result.Data, &envelope); err != nil {
			return flowReportPanel{}, err
		}
		for _, row := range envelope.Table.Items {
			summaries = append(summaries, categorySummary{Address: endpointAddress(row), Category: category, Total: row.Total})
		}
		if err := mergeFlowReportVersions(versions, result.Versions); err != nil {
			return flowReportPanel{}, err
		}
		mergeFlowReportCompleteness(&completeness, result.Completeness)
		if result.AsOf.After(asOf) {
			asOf = result.AsOf
		}
		if unit == "" {
			unit = result.Unit
		} else if unit != result.Unit {
			return flowReportPanel{}, &QueryGatewayError{Code: QueryErrorIncomplete, Message: "endpoint category panels returned different units"}
		}
		if step == 0 {
			step = result.StepSeconds
		}
	}
	data, err := json.Marshal(struct {
		Points     []flowquery.JointPoint          `json:"points"`
		Summaries  []categorySummary               `json:"summaries"`
		Metric     flowquery.MetricDefinition      `json:"metric"`
		Dimensions []flowquery.DimensionDefinition `json:"dimensions"`
	}{
		Points: combined, Summaries: summaries, Metric: flowquery.MetricDefinition{Name: parameters.Metric, Unit: unit},
		Dimensions: []flowquery.DimensionDefinition{{Kind: dimension, Additive: true}, {Kind: flowquery.DimensionCategory, Additive: true}},
	})
	if err != nil {
		return flowReportPanel{}, err
	}
	return flowReportPanel{ID: panelID, Status: "ready", Data: data, Meta: flowReportPanelMeta{
		Unit: unit, Timezone: parameters.Timezone, StepSeconds: step, AsOf: asOf, Versions: versions, Completeness: completeness,
	}}, nil
}

func (p ClickHouseFlowQueryProvider) endpointBusinessReportPanel(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
	addresses []string,
	baseMeta flowReportPanelMeta,
) (flowReportPanel, error) {
	const panelID = "endpoint_business"
	if len(addresses) == 0 {
		data, err := json.Marshal(struct {
			Points     []flowquery.JointPoint          `json:"points"`
			Metric     flowquery.MetricDefinition      `json:"metric"`
			Dimensions []flowquery.DimensionDefinition `json:"dimensions"`
		}{Points: []flowquery.JointPoint{}, Metric: flowquery.MetricDefinition{Name: parameters.Metric}})
		return flowReportPanel{ID: panelID, Status: "ready", Data: data, Meta: baseMeta}, err
	}
	dimension := flowquery.DimensionSourceIP
	if parameters.Report.Side == "destination" {
		dimension = flowquery.DimensionDestinationIP
	}
	addressFilter := flowquery.FilterExpression{
		Op: flowquery.FilterPredicate, Field: string(dimension), Operator: flowquery.FilterIn,
		Values: append([]string(nil), addresses...),
	}
	if parameters.Filter != nil {
		addressFilter = flowquery.FilterExpression{Op: flowquery.FilterAnd, Args: []flowquery.FilterExpression{*parameters.Filter, addressFilter}}
	}
	canonical, err := flowquery.CanonicalFilter(addressFilter)
	if err != nil {
		return flowReportPanel{}, err
	}
	query := parameters
	query.Report = nil
	query.Dimension = ""
	query.Dimensions = []flowquery.Dimension{dimension, flowquery.DimensionBusiness}
	query.Filters.DimensionValues = nil
	query.Filter = &canonical
	query.DirectionSplit = false
	query.Table = nil
	query.TimeWindows = reportTimeWindows(parameters.Report.PeakWindows)
	query.TopN = 100
	query.IncludeOther = false
	encoded, err := json.Marshal(query)
	if err != nil {
		return flowReportPanel{}, err
	}
	subrequest := request
	subrequest.Parameters = encoded
	result, err := p.Query(ctx, subrequest)
	if err != nil {
		if flowReportPanelMayBeUnavailable(err) {
			return flowReportPanel{ID: panelID, Status: "unavailable", Reason: flowReportUnavailableReason(err), Meta: baseMeta}, nil
		}
		return flowReportPanel{}, err
	}
	return flowReportPanel{ID: panelID, Status: "ready", Data: result.Data, Meta: flowReportPanelMeta{
		Unit: result.Unit, Timezone: result.Timezone, StepSeconds: result.StepSeconds,
		AsOf: result.AsOf, Versions: result.Versions, Completeness: result.Completeness,
	}}, nil
}

func flowReportDimensionPoints(data json.RawMessage) ([]flowquery.Point, error) {
	var envelope struct {
		Points []struct {
			Bucket                 time.Time `json:"bucket"`
			DimensionValue         string    `json:"dimension_value"`
			DimensionValues        []string  `json:"dimension_values"`
			Other                  bool      `json:"other"`
			DimensionSnapshotID    string    `json:"dimension_snapshot_id"`
			GeoVersion             string    `json:"geo_version"`
			ClassificationVersion  uint32    `json:"classification_version"`
			Value                  float64   `json:"value"`
			ReceivedRecords        uint64    `json:"received_records"`
			UnknownSamplingRecords uint64    `json:"unknown_sampling_records"`
			QualityRecords         uint64    `json:"quality_records"`
			GeneratedAt            time.Time `json:"generated_at"`
			ObservedAt             time.Time `json:"observed_at"`
		} `json:"points"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	points := make([]flowquery.Point, 0, len(envelope.Points))
	for _, point := range envelope.Points {
		value := point.DimensionValue
		if value == "" && len(point.DimensionValues) == 1 {
			value = point.DimensionValues[0]
		}
		if value == "" || (len(point.DimensionValues) != 0 && len(point.DimensionValues) != 1) {
			return nil, errors.New("endpoint category query returned an invalid dimension tuple")
		}
		generatedAt := point.GeneratedAt
		if generatedAt.IsZero() {
			generatedAt = point.ObservedAt
		}
		points = append(points, flowquery.Point{
			Bucket: point.Bucket, DimensionValue: value, Other: point.Other,
			DimensionSnapshotID: point.DimensionSnapshotID, GeoVersion: point.GeoVersion,
			ClassificationVersion: point.ClassificationVersion, Value: point.Value,
			ReceivedRecords: point.ReceivedRecords, UnknownSamplingRecords: point.UnknownSamplingRecords,
			QualityRecords: point.QualityRecords, GeneratedAt: generatedAt,
		})
	}
	return points, nil
}

func flowReportResultRows(data json.RawMessage) (uint64, error) {
	var envelope struct {
		Points []json.RawMessage `json:"points"`
		Table  *struct {
			Items []json.RawMessage `json:"items"`
		} `json:"table"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return 0, err
	}
	rows := uint64(len(envelope.Points))
	if envelope.Table != nil {
		rows += uint64(len(envelope.Table.Items))
	}
	return rows, nil
}

func flowReportExecutionMetadata(data json.RawMessage) (flowReportPlan, time.Time) {
	var envelope struct {
		Plan *struct {
			Source        string `json:"source"`
			SourceSeconds uint32 `json:"source_seconds"`
			StepSeconds   uint32 `json:"step_seconds"`
		} `json:"plan"`
		Points []struct {
			Bucket time.Time `json:"bucket"`
		} `json:"points"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return flowReportPlan{}, time.Time{}
	}
	plan := flowReportPlan{}
	if envelope.Plan != nil {
		plan = flowReportPlan{
			Source: envelope.Plan.Source, SourceSeconds: envelope.Plan.SourceSeconds, DisplaySeconds: envelope.Plan.StepSeconds,
		}
	}
	var latest time.Time
	for _, point := range envelope.Points {
		if point.Bucket.After(latest) {
			latest = point.Bucket
		}
	}
	return plan, latest
}

func flowVPNReportResultRows(data json.RawMessage) (uint64, error) {
	var summary flowVPNReportSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return 0, err
	}
	return uint64(8 + len(summary.PortDistribution)*2 + len(summary.TypeDistribution)*2 + len(summary.Trend)*2), nil
}

func mergeFlowReportVersions(target, source map[string]string) error {
	for key, value := range source {
		if value == "" {
			continue
		}
		if existing := target[key]; existing != "" && existing != value {
			return &QueryGatewayError{
				Code: QueryErrorIncomplete, Message: "Flow report panels resolved different immutable versions",
				Details: map[string]any{"version": key, "first": existing, "next": value},
			}
		}
		target[key] = value
	}
	return nil
}

func mergeFlowReportCompleteness(target *QueryCompleteness, source QueryCompleteness) {
	if target == nil {
		return
	}
	if target.AvailableFrom == nil || (source.AvailableFrom != nil && source.AvailableFrom.After(*target.AvailableFrom)) {
		target.AvailableFrom = source.AvailableFrom
	}
	if target.AvailableTo == nil || (source.AvailableTo != nil && source.AvailableTo.Before(*target.AvailableTo)) {
		target.AvailableTo = source.AvailableTo
	}
	if source.CompleteRatio < target.CompleteRatio {
		target.CompleteRatio = source.CompleteRatio
	}
	if source.LateRatio > target.LateRatio {
		target.LateRatio = source.LateRatio
	}
	if source.UnknownRatio > target.UnknownRatio {
		target.UnknownRatio = source.UnknownRatio
	}
	target.Partial = target.Partial || source.Partial
	target.DegradedIntervals = append(target.DegradedIntervals, source.DegradedIntervals...)
	target.Warnings = uniqueSortedStrings(append(target.Warnings, source.Warnings...))
}

type flowVPNReportSummary struct {
	FindingCount         int                  `json:"finding_count"`
	SuspectedHosts       int                  `json:"suspected_hosts"`
	HighRiskHosts        int                  `json:"high_risk_hosts"`
	ActivePorts          int                  `json:"active_ports"`
	InboundBytes         uint64               `json:"inbound_bytes"`
	OutboundBytes        uint64               `json:"outbound_bytes"`
	TotalBytes           uint64               `json:"total_bytes"`
	PortDistribution     []flowReportCount    `json:"port_distribution"`
	TypeDistribution     []flowReportCount    `json:"type_distribution"`
	Trend                []flowVPNTrend       `json:"trend"`
	RuleSetVersions      []string             `json:"rule_set_versions"`
	SourceGenerations    []uint64             `json:"source_generations"`
	MinimumCompleteRatio float64              `json:"minimum_complete_ratio"`
	PortTable            *flowReportCountPage `json:"port_table,omitempty"`
	TypeTable            *flowReportCountPage `json:"type_table,omitempty"`
}

type flowReportCount struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
	Bytes uint64 `json:"bytes"`
}

type flowVPNTrend struct {
	Bucket        time.Time `json:"bucket"`
	InboundBytes  uint64    `json:"inbound_bytes"`
	OutboundBytes uint64    `json:"outbound_bytes"`
}

func (p ClickHouseFlowQueryProvider) vpnReportPanel(
	ctx context.Context,
	request QueryProviderRequest,
	parameters flowAggregateQueryParameters,
) (flowReportPanel, error) {
	if p.VPNFindings == nil {
		return flowReportPanel{ID: "vpn_findings", Status: "unavailable", Reason: "VPN finding materialization is not configured"}, nil
	}
	filter := VPNFindingListFilter{
		From: request.From, To: request.To, SortBy: "window_end", SortDirection: "asc",
	}
	items, err := p.VPNFindings.ListVPNFindingsForExport(ctx, request.TenantID, filter, maxVPNFindingExportRows)
	if err != nil {
		return flowReportPanel{}, fmt.Errorf("load VPN findings for report: %w", err)
	}
	if len(items) > int(maxVPNFindingExportRows) {
		return flowReportPanel{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "VPN findings exceed the fixed report scan budget"}
	}
	if len(parameters.Report.PeakWindows) != 0 {
		windows := reportTimeWindows(parameters.Report.PeakWindows)
		selected := items[:0]
		for _, item := range items {
			if flowquery.InLocalTimeWindows(item.WindowEnd, windows, parameters.Timezone) {
				selected = append(selected, item)
			}
		}
		items = selected
	}
	summary := summarizeVPNFindings(items, request.From, request.To, parameters.TargetPoints)
	if parameters.Report != nil {
		if table := parameters.Report.Tables["port_distribution"]; table != nil {
			summary.PortTable = buildFlowVPNDistributionTable(summary.PortDistribution, *table)
		}
		if table := parameters.Report.Tables["type_distribution"]; table != nil {
			summary.TypeTable = buildFlowVPNDistributionTable(summary.TypeDistribution, *table)
		}
	}
	data, err := json.Marshal(summary)
	if err != nil {
		return flowReportPanel{}, fmt.Errorf("marshal VPN report: %w", err)
	}
	return flowReportPanel{
		ID: "vpn_findings", Status: "ready", Data: data,
		Meta: flowReportPanelMeta{
			Timezone: parameters.Timezone, AsOf: p.now(),
			Versions: map[string]string{
				"vpn_rule_set_versions":  strings.Join(summary.RuleSetVersions, ","),
				"vpn_source_generations": joinUint64s(summary.SourceGenerations),
			},
			Completeness: QueryCompleteness{CompleteRatio: summary.MinimumCompleteRatio, Partial: summary.MinimumCompleteRatio < 1},
		},
	}, nil
}

func joinUint64s(values []uint64) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.FormatUint(value, 10)
	}
	return strings.Join(parts, ",")
}

func summarizeVPNFindings(items []VPNFinding, from, to time.Time, targetPoints uint16) flowVPNReportSummary {
	hosts, highRisk, ports := map[string]struct{}{}, map[string]struct{}{}, map[uint16]*flowReportCount{}
	types := map[string]*flowReportCount{}
	ruleVersions, generations := map[string]struct{}{}, map[uint64]struct{}{}
	minimumComplete := 1.0
	interval := flowReportTrendInterval(from, to, targetPoints)
	trend := map[time.Time]*flowVPNTrend{}
	result := flowVPNReportSummary{FindingCount: len(items), MinimumCompleteRatio: minimumComplete}
	for _, item := range items {
		hosts[item.LocalIP] = struct{}{}
		if item.RiskLevel == "high" || item.RiskLevel == "critical" {
			highRisk[item.LocalIP] = struct{}{}
		}
		bytes := item.LocalToRemoteBytes + item.RemoteToLocalBytes
		if item.PrimaryRemotePort != 0 {
			entry := ports[item.PrimaryRemotePort]
			if entry == nil {
				entry = &flowReportCount{Value: strconv.FormatUint(uint64(item.PrimaryRemotePort), 10)}
				ports[item.PrimaryRemotePort] = entry
			}
			entry.Count++
			entry.Bytes += bytes
		}
		kind := classifyVPNFindingType(item)
		entry := types[kind]
		if entry == nil {
			entry = &flowReportCount{Value: kind}
			types[kind] = entry
		}
		entry.Count++
		entry.Bytes += bytes
		result.InboundBytes += item.RemoteToLocalBytes
		result.OutboundBytes += item.LocalToRemoteBytes
		if item.CompleteRatio < minimumComplete {
			minimumComplete = item.CompleteRatio
		}
		if item.RuleSetVersion != "" {
			ruleVersions[item.RuleSetVersion] = struct{}{}
		}
		generations[item.SourceGeneration] = struct{}{}
		bucket := item.WindowEnd.UTC().Truncate(interval)
		point := trend[bucket]
		if point == nil {
			point = &flowVPNTrend{Bucket: bucket}
			trend[bucket] = point
		}
		point.InboundBytes += item.RemoteToLocalBytes
		point.OutboundBytes += item.LocalToRemoteBytes
	}
	result.SuspectedHosts = len(hosts)
	result.HighRiskHosts = len(highRisk)
	result.ActivePorts = len(ports)
	result.TotalBytes = result.InboundBytes + result.OutboundBytes
	result.MinimumCompleteRatio = minimumComplete
	result.PortDistribution = sortedFlowReportCounts(ports)
	result.TypeDistribution = sortedFlowReportCounts(types)
	for _, point := range trend {
		result.Trend = append(result.Trend, *point)
	}
	sort.Slice(result.Trend, func(i, j int) bool { return result.Trend[i].Bucket.Before(result.Trend[j].Bucket) })
	for version := range ruleVersions {
		result.RuleSetVersions = append(result.RuleSetVersions, version)
	}
	sort.Strings(result.RuleSetVersions)
	for generation := range generations {
		result.SourceGenerations = append(result.SourceGenerations, generation)
	}
	sort.Slice(result.SourceGenerations, func(i, j int) bool { return result.SourceGenerations[i] < result.SourceGenerations[j] })
	return result
}

func flowReportTrendInterval(from, to time.Time, targetPoints uint16) time.Duration {
	if targetPoints == 0 {
		targetPoints = flowquery.DefaultTargetPoints
	}
	duration := to.Sub(from)
	minimum := duration / time.Duration(targetPoints)
	for _, interval := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour, 6 * time.Hour, 24 * time.Hour} {
		if minimum <= interval {
			return interval
		}
	}
	return 24 * time.Hour
}

func classifyVPNFindingType(item VPNFinding) string {
	if ruleID := strings.TrimSpace(item.DecisionRuleID); ruleID != "" {
		return ruleID
	}
	var evidence []struct {
		RuleID       string `json:"rule_id"`
		Contribution uint16 `json:"contribution"`
	}
	if json.Unmarshal(item.Evidence, &evidence) == nil {
		sort.SliceStable(evidence, func(i, j int) bool {
			if evidence[i].Contribution == evidence[j].Contribution {
				return evidence[i].RuleID < evidence[j].RuleID
			}
			return evidence[i].Contribution > evidence[j].Contribution
		})
		for _, signal := range evidence {
			if ruleID := strings.TrimSpace(signal.RuleID); ruleID != "" {
				return ruleID
			}
		}
	}
	return "unclassified"
}

func sortedFlowReportCounts[K comparable](source map[K]*flowReportCount) []flowReportCount {
	result := make([]flowReportCount, 0, len(source))
	for _, item := range source {
		result = append(result, *item)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Bytes == result[j].Bytes {
			return result[i].Value < result[j].Value
		}
		return result[i].Bytes > result[j].Bytes
	})
	return result
}

func uniqueSortedStrings(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
