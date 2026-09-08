package watchdog

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/parquet-go/parquet-go"
)

const flowExportCompletenessMode = "query_result"

// ExportRows is the provider-neutral tabular path used by datasets whose
// results are not scalar time series. Flow keeps every selected dimension and
// the provenance/quality counters needed to interpret each exported value.
type ExportRows struct {
	Rows        []FlowExportRow
	VPNFindings []VPNFindingExportRow
	FlowDetails *FlowDetailExportRows
}

type FlowExportRow struct {
	Bucket                    time.Time
	DimensionNames            []string
	DimensionValues           []string
	Other                     bool
	Metric                    string
	Unit                      string
	Value                     float64
	ValueLayer                QueryValueLayer
	ReceivedRecords           uint64
	UnknownSamplingRecords    uint64
	QualityRecords            uint64
	SamplingCompleteness      float64
	SamplingCompletenessKnown bool
	QualityRecordRatio        float64
	QualityRecordRatioKnown   bool
	DimensionSnapshotID       string
	GeoVersion                string
	ClassificationVersion     uint32
	ObservedAt                time.Time
	QueryCompleteRatio        float64
	QueryPartial              bool
	QueryUnknownRatio         float64
	QueryWarnings             string
	QuerySource               string
	QueryStepSeconds          uint32
	QueryAsOf                 time.Time
	QueryPolicyVersion        uint64
	ReportKind                string
	ReportPanel               string
	ReportPanelStatus         string
	ReportPanelReason         string
}

type ExportRowsDataProvider interface {
	LoadExportRows(context.Context, ExportTask) (ExportRows, bool, error)
}

type ExportRowsWriter interface {
	WriteExportRows(context.Context, ExportTask, ExportRows) (ExportArtifact, bool, error)
}

func prepareFlowExportExecutionTask(ctx context.Context, gateway *QueryGateway, auth AuthContext, query QueryRequest, format ExportFormat, retentionSeconds uint32) (ExportTask, error) {
	if gateway == nil {
		return ExportTask{}, errors.New("Flow export query gateway is required")
	}
	query.Dataset = strings.TrimSpace(query.Dataset)
	if query.Dataset != FlowTrafficDataset {
		return ExportTask{}, errors.New("Flow export dataset must be flow.traffic")
	}
	if query.ValueLayer != QueryValueCustomer {
		return ExportTask{}, errors.New("Flow exports currently support the customer value layer")
	}
	if format != ExportFormatCSV && format != ExportFormatParquet {
		return ExportTask{}, errors.New("Flow export format must be csv or parquet")
	}
	descriptor, ok := gateway.Datasets.Get(FlowTrafficDataset)
	if !ok {
		return ExportTask{}, errors.New("Flow export dataset is not registered")
	}
	if err := gateway.requireModuleEnabled(ctx, auth.TenantID, descriptor.ModuleKey); err != nil {
		return ExportTask{}, err
	}
	policy, err := gateway.effectivePolicy(ctx, auth.TenantID, descriptor)
	if err != nil {
		return ExportTask{}, fmt.Errorf("load Flow export query policy: %w", err)
	}
	if !policy.Enabled || !policy.allows(query.ValueLayer) || !datasetSupportsQueryLayer(descriptor, query.ValueLayer) {
		return ExportTask{}, errors.New("Flow export dataset or value layer is disabled")
	}
	if !queryLayerAuthorized(auth, query.ValueLayer) {
		return ExportTask{}, errors.New("query permission for the Flow export value layer is required")
	}
	query, err = normalizeQueryRequest(query)
	if err != nil {
		return ExportTask{}, err
	}
	maxRange := time.Duration(policy.MaxRangeSeconds) * time.Second
	if descriptor.MaxRangeDays > 0 {
		descriptorRange := time.Duration(descriptor.MaxRangeDays) * 24 * time.Hour
		if descriptorRange < maxRange {
			maxRange = descriptorRange
		}
	}
	if query.To.Sub(query.From) > maxRange {
		return ExportTask{}, errors.New("Flow export range exceeds the dataset policy")
	}
	if query.Limit == 0 {
		query.Limit = policy.MaxResultRows
	}
	if query.Limit > policy.MaxResultRows {
		return ExportTask{}, errors.New("Flow export row limit exceeds the dataset policy")
	}
	parameters, err := decodeFlowAggregateQueryParameters(query.Parameters)
	if err != nil {
		return ExportTask{}, err
	}
	// Interactive VTable paging is a response projection, not part of a full
	// export. Freeze the same typed query without its current table page.
	parameters.Table = nil
	if parameters.Report != nil {
		parameters.Report.Tables = nil
	}
	query.Parameters, err = json.Marshal(parameters)
	if err != nil {
		return ExportTask{}, err
	}
	query.Parameters, err = gateway.prepareDatasetParameters(ctx, auth, query)
	if err != nil {
		return ExportTask{}, fmt.Errorf("prepare Flow export query: %w", err)
	}
	query, err = normalizeQueryRequest(query)
	if err != nil {
		return ExportTask{}, err
	}
	queryJSON, queryHash, err := canonicalJSONHash(exportQuerySnapshot{
		SchemaVersion: 1, Query: query, Aggregation: AggregationAverageFiveMinute,
	})
	if err != nil {
		return ExportTask{}, err
	}
	_, descriptorHash, err := canonicalJSONHash(descriptor)
	if err != nil {
		return ExportTask{}, err
	}
	versionsJSON, _, err := canonicalJSONHash(exportVersionSnapshot{
		SchemaVersion: 1, DatasetDescriptorHash: descriptorHash, QueryPolicyVersion: policy.RowVersion,
		CompletenessStepSeconds: query.StepSeconds, CompletenessMissingRatio: "0",
		CompletenessMode: flowExportCompletenessMode, SnapshotComplete: true,
	})
	if err != nil {
		return ExportTask{}, err
	}
	access := exportAccessRequest(auth, ExportTask{DatasetKey: FlowTrafficDataset})
	authorizationJSON, _, err := canonicalJSONHash(exportAuthorizationSnapshot{
		SchemaVersion: 1, SubjectID: auth.UserID, RequiredAction: ActionExportCustomer,
		ResourceType: access.Resource.Type, ResourceID: access.Resource.ID, Admin: auth.IsAdmin,
	})
	if err != nil {
		return ExportTask{}, err
	}
	if retentionSeconds == 0 {
		retentionSeconds = defaultExportRetentionSeconds
	}
	task := ExportTask{
		TenantID: auth.TenantID, CreatedBy: auth.UserID, ContractVersion: ExportExecutionContractVersion,
		DatasetKey: FlowTrafficDataset, QueryJSON: queryJSON, QueryHash: queryHash,
		ValueLayer: query.ValueLayer, VersionsJSON: versionsJSON, AuthorizationJSON: authorizationJSON,
		RetentionSeconds: retentionSeconds, PeriodType: PeriodCustom, RangeStart: query.From, RangeEnd: query.To,
		Step: time.Duration(query.StepSeconds) * time.Second, Aggregation: AggregationAverageFiveMinute,
		ValueMode: ExportValueCorrected, Format: format, Status: ExportStatusPending,
	}
	return task, validateExportExecutionTask(task)
}

func (p QueryGatewayExportDataProvider) LoadExportRows(ctx context.Context, task ExportTask) (ExportRows, bool, error) {
	if task.DatasetKey == FlowRecordDetailDataset {
		if err := validateExportExecutionTask(task); err != nil {
			return ExportRows{}, true, err
		}
		auth, ok := AuthFromContext(ctx)
		if !ok || auth.TenantID != task.TenantID || auth.UserID != task.CreatedBy {
			return ExportRows{}, true, errors.New("current export authorization is required")
		}
		rows, err := p.loadFlowDetailExportRows(ctx, task)
		return rows, true, err
	}
	if task.DatasetKey == FlowVPNFindingsDataset {
		if err := validateExportExecutionTask(task); err != nil {
			return ExportRows{}, true, err
		}
		auth, ok := AuthFromContext(ctx)
		if !ok || auth.TenantID != task.TenantID || auth.UserID != task.CreatedBy {
			return ExportRows{}, true, errors.New("current export authorization is required")
		}
		rows, err := p.loadVPNFindingExportRows(ctx, task)
		return rows, true, err
	}
	if task.DatasetKey != FlowTrafficDataset {
		return ExportRows{}, false, nil
	}
	if p.Gateway == nil {
		return ExportRows{}, true, errors.New("export query gateway is required")
	}
	if err := validateExportExecutionTask(task); err != nil {
		return ExportRows{}, true, err
	}
	var snapshot exportQuerySnapshot
	if err := decodeStrictJSON(task.QueryJSON, &snapshot); err != nil {
		return ExportRows{}, true, fmt.Errorf("decode Flow export query: %w", err)
	}
	auth, ok := AuthFromContext(ctx)
	if !ok || auth.TenantID != task.TenantID || auth.UserID != task.CreatedBy {
		return ExportRows{}, true, errors.New("current export authorization is required")
	}
	result, err := p.Gateway.Execute(ctx, auth, "export:"+string(task.ID), snapshot.Query)
	if err != nil {
		return ExportRows{}, true, err
	}
	rows, err := flowExportRows(result, task.ValueLayer)
	if err != nil {
		return ExportRows{}, true, err
	}
	if len(rows.Rows) == 0 {
		return ExportRows{}, true, errors.New("no Flow rows returned from query gateway")
	}
	return rows, true, nil
}

func flowExportRows(result QueryResult, layer QueryValueLayer) (ExportRows, error) {
	var reportProbe struct {
		SchemaVersion uint16         `json:"schema_version"`
		Kind          flowReportKind `json:"kind"`
	}
	if err := json.Unmarshal(result.Data, &reportProbe); err != nil {
		return ExportRows{}, fmt.Errorf("decode Flow export result: %w", err)
	}
	if reportProbe.Kind != "" {
		if reportProbe.SchemaVersion != flowReportSchemaVersion {
			return ExportRows{}, fmt.Errorf("unsupported Flow report export schema %d", reportProbe.SchemaVersion)
		}
		var report flowReportData
		if err := json.Unmarshal(result.Data, &report); err != nil {
			return ExportRows{}, fmt.Errorf("decode Flow report export result: %w", err)
		}
		return flowReportExportRows(report, result.Meta, layer)
	}
	return flowExportPanelRows(result.Data, flowExportDecoration{
		ValueLayer: layer, CompleteRatio: result.Meta.CompleteRatio, Partial: result.Meta.Partial,
		UnknownRatio: result.Meta.UnknownRatio, Warnings: strings.Join(result.Meta.Warnings, "; "),
		Source: result.Meta.Source, StepSeconds: result.Meta.StepSeconds, AsOf: result.Meta.AsOf,
		PolicyVersion: result.Meta.PolicyVersion,
	})
}

type flowExportDecoration struct {
	ValueLayer    QueryValueLayer
	CompleteRatio float64
	Partial       bool
	UnknownRatio  float64
	Warnings      string
	Source        string
	StepSeconds   uint32
	AsOf          time.Time
	PolicyVersion uint64
	ReportKind    string
	ReportPanel   string
	PanelStatus   string
	PanelReason   string
}

func flowExportPanelRows(data json.RawMessage, decoration flowExportDecoration) (ExportRows, error) {
	var envelope struct {
		Points     json.RawMessage                 `json:"points"`
		Metric     flowquery.MetricDefinition      `json:"metric"`
		Dimension  flowquery.DimensionDefinition   `json:"dimension"`
		Dimensions []flowquery.DimensionDefinition `json:"dimensions"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return ExportRows{}, fmt.Errorf("decode Flow export result: %w", err)
	}
	decorate := func(row *FlowExportRow) {
		row.Metric = string(envelope.Metric.Name)
		row.Unit = envelope.Metric.Unit
		row.ValueLayer = decoration.ValueLayer
		row.QueryCompleteRatio = decoration.CompleteRatio
		row.QueryPartial = decoration.Partial
		row.QueryUnknownRatio = decoration.UnknownRatio
		row.QueryWarnings = decoration.Warnings
		row.QuerySource = decoration.Source
		row.QueryStepSeconds = decoration.StepSeconds
		row.QueryAsOf = decoration.AsOf
		row.QueryPolicyVersion = decoration.PolicyVersion
		row.ReportKind = decoration.ReportKind
		row.ReportPanel = decoration.ReportPanel
		row.ReportPanelStatus = decoration.PanelStatus
		row.ReportPanelReason = decoration.PanelReason
	}
	rows := ExportRows{}
	if len(envelope.Dimensions) > 0 {
		var points []flowquery.JointPoint
		if err := json.Unmarshal(envelope.Points, &points); err != nil {
			return ExportRows{}, fmt.Errorf("decode Flow joint export points: %w", err)
		}
		rows.Rows = make([]FlowExportRow, 0, len(points))
		for _, point := range points {
			row := FlowExportRow{
				Bucket: point.Bucket, DimensionNames: flowExportDimensionNames(envelope.Dimensions),
				DimensionValues: append([]string(nil), point.DimensionValues...), Other: point.Other,
				Value: point.Value, ReceivedRecords: point.ReceivedRecords,
				UnknownSamplingRecords: point.UnknownSamplingRecords, QualityRecords: point.QualityRecords,
				SamplingCompleteness: point.SamplingCompleteness, SamplingCompletenessKnown: point.SamplingCompletenessKnown,
				QualityRecordRatio: point.QualityRecordRatio, QualityRecordRatioKnown: point.QualityRecordRatioKnown,
				DimensionSnapshotID: point.DimensionSnapshotID, GeoVersion: point.GeoVersion,
				ClassificationVersion: point.ClassificationVersion, ObservedAt: point.ObservedAt,
			}
			decorate(&row)
			rows.Rows = append(rows.Rows, row)
		}
		return rows, nil
	}
	var points []flowquery.Point
	if err := json.Unmarshal(envelope.Points, &points); err != nil {
		return ExportRows{}, fmt.Errorf("decode Flow export points: %w", err)
	}
	rows.Rows = make([]FlowExportRow, 0, len(points))
	for _, point := range points {
		row := FlowExportRow{
			Bucket: point.Bucket, DimensionNames: []string{string(envelope.Dimension.Kind)},
			DimensionValues: []string{point.DimensionValue}, Other: point.Other,
			Value: point.Value, ReceivedRecords: point.ReceivedRecords,
			UnknownSamplingRecords: point.UnknownSamplingRecords, QualityRecords: point.QualityRecords,
			SamplingCompleteness: point.SamplingCompleteness, SamplingCompletenessKnown: point.SamplingCompletenessKnown,
			QualityRecordRatio: point.QualityRecordRatio, QualityRecordRatioKnown: point.QualityRecordRatioKnown,
			DimensionSnapshotID: point.DimensionSnapshotID, GeoVersion: point.GeoVersion,
			ClassificationVersion: point.ClassificationVersion, ObservedAt: point.GeneratedAt,
		}
		decorate(&row)
		rows.Rows = append(rows.Rows, row)
	}
	return rows, nil
}

func flowReportExportRows(report flowReportData, meta QueryResultMeta, layer QueryValueLayer) (ExportRows, error) {
	rows := ExportRows{}
	reportWarnings := append([]string(nil), report.Warnings...)
	reportWarnings = append(reportWarnings, meta.Warnings...)
	for _, panel := range report.Panels {
		warnings := append([]string(nil), reportWarnings...)
		warnings = append(warnings, panel.Meta.Completeness.Warnings...)
		decoration := flowExportDecoration{
			ValueLayer: layer, CompleteRatio: panel.Meta.Completeness.CompleteRatio,
			Partial: panel.Meta.Completeness.Partial, UnknownRatio: panel.Meta.Completeness.UnknownRatio,
			Warnings: strings.Join(uniqueSortedStrings(warnings), "; "), Source: meta.Source,
			StepSeconds: panel.Meta.StepSeconds, AsOf: panel.Meta.AsOf, PolicyVersion: meta.PolicyVersion,
			ReportKind: string(report.Kind), ReportPanel: panel.ID, PanelStatus: panel.Status, PanelReason: panel.Reason,
		}
		if panel.Status != "ready" {
			rows.Rows = append(rows.Rows, FlowExportRow{
				Bucket: panel.Meta.AsOf, Metric: "panel_status", Unit: "status", ValueLayer: layer,
				QueryCompleteRatio: decoration.CompleteRatio, QueryPartial: true,
				QueryUnknownRatio: decoration.UnknownRatio, QueryWarnings: decoration.Warnings,
				QuerySource: decoration.Source, QueryStepSeconds: decoration.StepSeconds,
				QueryAsOf: decoration.AsOf, QueryPolicyVersion: decoration.PolicyVersion,
				ReportKind: decoration.ReportKind, ReportPanel: panel.ID,
				ReportPanelStatus: panel.Status, ReportPanelReason: panel.Reason,
			})
			continue
		}
		var panelRows ExportRows
		var err error
		switch panel.ID {
		case "observed":
			panelRows, err = flowObservedReportExportRows(panel.Data, decoration)
		case "vpn_share":
			panelRows, err = flowOverseasVPNShareExportRows(panel.Data, decoration)
		case "vpn_findings":
			panelRows, err = flowVPNReportExportRows(panel.Data, decoration)
		default:
			panelRows, err = flowExportPanelRows(panel.Data, decoration)
		}
		if err != nil {
			return ExportRows{}, fmt.Errorf("export Flow report panel %s: %w", panel.ID, err)
		}
		rows.Rows = append(rows.Rows, panelRows.Rows...)
	}
	return rows, nil
}

func flowOverseasVPNShareExportRows(data json.RawMessage, decoration flowExportDecoration) (ExportRows, error) {
	var result struct {
		Points []flowOverseasVPNSharePoint `json:"points"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return ExportRows{}, err
	}
	rows := ExportRows{}
	for _, point := range result.Points {
		appendRow := func(metric, unit string, value float64) {
			row := FlowExportRow{
				Bucket: point.GeneratedAt, Metric: metric, Unit: unit, Value: value,
				DimensionNames: []string{"direction"}, DimensionValues: []string{point.Direction},
			}
			decorateFlowReportExportRow(&row, decoration)
			rows.Rows = append(rows.Rows, row)
		}
		appendRow("vpn_bytes", "bytes", float64(point.VPNBytes))
		appendRow("overseas_bytes", "bytes", point.TotalBytes)
		appendRow("unknown_geo_bytes", "bytes", float64(point.UnknownGeo))
		appendRow("finding_rows", "findings", float64(point.FindingRows))
		if point.Ratio != nil {
			appendRow("vpn_share", "ratio", *point.Ratio)
		}
	}
	return rows, nil
}

func decorateFlowReportExportRow(row *FlowExportRow, decoration flowExportDecoration) {
	row.ValueLayer = decoration.ValueLayer
	row.QueryCompleteRatio = decoration.CompleteRatio
	row.QueryPartial = decoration.Partial
	row.QueryUnknownRatio = decoration.UnknownRatio
	row.QueryWarnings = decoration.Warnings
	row.QuerySource = decoration.Source
	row.QueryStepSeconds = decoration.StepSeconds
	row.QueryAsOf = decoration.AsOf
	row.QueryPolicyVersion = decoration.PolicyVersion
	row.ReportKind = decoration.ReportKind
	row.ReportPanel = decoration.ReportPanel
	row.ReportPanelStatus = decoration.PanelStatus
	row.ReportPanelReason = decoration.PanelReason
}

func flowObservedReportExportRows(data json.RawMessage, decoration flowExportDecoration) (ExportRows, error) {
	var result flowquery.OverseasResult
	if err := json.Unmarshal(data, &result); err != nil {
		return ExportRows{}, err
	}
	rows := ExportRows{Rows: make([]FlowExportRow, 0, len(result.Points)*3)}
	for _, point := range result.Points {
		base := FlowExportRow{
			Bucket: point.Bucket, DimensionNames: []string{"geo_scope", "direction", "ip_family", "geo_value"},
			DimensionValues: []string{string(point.GeoScope), string(point.Direction), string(point.IPFamily), point.GeoValue},
			Other:           point.Other, ReceivedRecords: point.ReceivedRecords,
			UnknownSamplingRecords: point.UnknownSamplingRecords, QualityRecords: point.QualityRecords,
			SamplingCompleteness: point.SamplingCompleteness, SamplingCompletenessKnown: point.SamplingCompletenessKnown,
			QualityRecordRatio: point.QualityRecordRatio, QualityRecordRatioKnown: point.QualityRecordRatioKnown,
			DimensionSnapshotID: point.DimensionSnapshotID, GeoVersion: point.GeoVersion,
			ClassificationVersion: point.ClassificationVersion, ObservedAt: point.GeneratedAt,
		}
		traffic := base
		traffic.Metric, traffic.Unit, traffic.Value = string(result.Metric.Name), result.Metric.Unit, point.Value
		decorateFlowReportExportRow(&traffic, decoration)
		rows.Rows = append(rows.Rows, traffic)
		for _, cardinality := range []struct {
			metric string
			unit   string
			value  uint64
		}{
			{"observed_remote_ips", "addresses", point.ObservedRemoteIPs},
			{"observed_local_hosts", "hosts", point.ObservedLocalHosts},
		} {
			row := base
			row.Metric, row.Unit, row.Value = cardinality.metric, cardinality.unit, float64(cardinality.value)
			decorateFlowReportExportRow(&row, decoration)
			rows.Rows = append(rows.Rows, row)
		}
	}
	return rows, nil
}

func flowVPNReportExportRows(data json.RawMessage, decoration flowExportDecoration) (ExportRows, error) {
	var summary flowVPNReportSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return ExportRows{}, err
	}
	rows := ExportRows{}
	appendRow := func(bucket time.Time, metric, unit string, value float64, names, values []string) {
		row := FlowExportRow{Bucket: bucket, Metric: metric, Unit: unit, Value: value, DimensionNames: names, DimensionValues: values}
		decorateFlowReportExportRow(&row, decoration)
		rows.Rows = append(rows.Rows, row)
	}
	asOf := decoration.AsOf
	for _, item := range []struct {
		metric string
		unit   string
		value  float64
	}{
		{"finding_count", "findings", float64(summary.FindingCount)},
		{"suspected_hosts", "hosts", float64(summary.SuspectedHosts)},
		{"high_risk_hosts", "hosts", float64(summary.HighRiskHosts)},
		{"active_ports", "ports", float64(summary.ActivePorts)},
		{"inbound_bytes", "bytes", float64(summary.InboundBytes)},
		{"outbound_bytes", "bytes", float64(summary.OutboundBytes)},
		{"total_bytes", "bytes", float64(summary.TotalBytes)},
		{"minimum_complete_ratio", "ratio", summary.MinimumCompleteRatio},
	} {
		appendRow(asOf, item.metric, item.unit, item.value, nil, nil)
	}
	for _, point := range summary.Trend {
		appendRow(point.Bucket, "inbound_bytes", "bytes", float64(point.InboundBytes), []string{"series"}, []string{"trend"})
		appendRow(point.Bucket, "outbound_bytes", "bytes", float64(point.OutboundBytes), []string{"series"}, []string{"trend"})
	}
	for _, distribution := range []struct {
		name  string
		items []flowReportCount
	}{{"port", summary.PortDistribution}, {"type", summary.TypeDistribution}} {
		for _, item := range distribution.items {
			appendRow(asOf, "finding_count", "findings", float64(item.Count), []string{"distribution", "value"}, []string{distribution.name, item.Value})
			appendRow(asOf, "traffic_bytes", "bytes", float64(item.Bytes), []string{"distribution", "value"}, []string{distribution.name, item.Value})
		}
	}
	return rows, nil
}

func flowExportDimensionNames(dimensions []flowquery.DimensionDefinition) []string {
	names := make([]string, 0, len(dimensions))
	for _, dimension := range dimensions {
		names = append(names, string(dimension.Kind))
	}
	return names
}

var flowExportHeader = []string{
	"bucket", "dimension_1_name", "dimension_1_value", "dimension_2_name", "dimension_2_value",
	"dimension_3_name", "dimension_3_value", "dimension_4_name", "dimension_4_value", "other", "metric", "unit", "value", "value_layer",
	"received_records", "unknown_sampling_records", "quality_records", "sampling_completeness", "sampling_completeness_known",
	"quality_record_ratio", "quality_record_ratio_known", "dimension_snapshot_id", "geo_version", "classification_version",
	"observed_at", "query_complete_ratio", "query_partial", "query_unknown_ratio", "query_warnings",
	"query_source", "query_step_seconds", "query_as_of", "query_policy_version",
	"report_kind", "report_panel", "report_panel_status", "report_panel_reason",
}

func RenderCSVExportRows(rows ExportRows) ([]byte, error) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	if err := writer.Write(flowExportHeader); err != nil {
		return nil, err
	}
	for _, row := range rows.Rows {
		dimensionNames := [4]string{}
		dimensions := [4]string{}
		for index := 0; index < len(row.DimensionNames) && index < len(dimensionNames); index++ {
			dimensionNames[index] = safeSpreadsheetCell(row.DimensionNames[index])
		}
		for index := 0; index < len(row.DimensionValues) && index < len(dimensions); index++ {
			dimensions[index] = safeSpreadsheetCell(row.DimensionValues[index])
		}
		record := []string{
			row.Bucket.UTC().Format(time.RFC3339Nano),
			dimensionNames[0], dimensions[0], dimensionNames[1], dimensions[1],
			dimensionNames[2], dimensions[2], dimensionNames[3], dimensions[3],
			strconv.FormatBool(row.Other), safeSpreadsheetCell(row.Metric), safeSpreadsheetCell(row.Unit),
			strconv.FormatFloat(row.Value, 'f', -1, 64), string(row.ValueLayer), strconv.FormatUint(row.ReceivedRecords, 10),
			strconv.FormatUint(row.UnknownSamplingRecords, 10), strconv.FormatUint(row.QualityRecords, 10),
			strconv.FormatFloat(row.SamplingCompleteness, 'f', -1, 64), strconv.FormatBool(row.SamplingCompletenessKnown),
			strconv.FormatFloat(row.QualityRecordRatio, 'f', -1, 64), strconv.FormatBool(row.QualityRecordRatioKnown),
			safeSpreadsheetCell(row.DimensionSnapshotID), safeSpreadsheetCell(row.GeoVersion), strconv.FormatUint(uint64(row.ClassificationVersion), 10),
			row.ObservedAt.UTC().Format(time.RFC3339Nano), strconv.FormatFloat(row.QueryCompleteRatio, 'f', -1, 64),
			strconv.FormatBool(row.QueryPartial), strconv.FormatFloat(row.QueryUnknownRatio, 'f', -1, 64), safeSpreadsheetCell(row.QueryWarnings),
			safeSpreadsheetCell(row.QuerySource), strconv.FormatUint(uint64(row.QueryStepSeconds), 10),
			row.QueryAsOf.UTC().Format(time.RFC3339Nano), strconv.FormatUint(row.QueryPolicyVersion, 10),
			safeSpreadsheetCell(row.ReportKind), safeSpreadsheetCell(row.ReportPanel),
			safeSpreadsheetCell(row.ReportPanelStatus), safeSpreadsheetCell(row.ReportPanelReason),
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	return output.Bytes(), writer.Error()
}

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

type flowExportParquetRow struct {
	Bucket                    int64   `parquet:"bucket,timestamp(millisecond:utc)"`
	Dimension1Name            string  `parquet:"dimension_1_name,dict"`
	Dimension1Value           string  `parquet:"dimension_1_value,dict"`
	Dimension2Name            string  `parquet:"dimension_2_name,dict"`
	Dimension2Value           string  `parquet:"dimension_2_value,dict"`
	Dimension3Name            string  `parquet:"dimension_3_name,dict"`
	Dimension3Value           string  `parquet:"dimension_3_value,dict"`
	Dimension4Name            string  `parquet:"dimension_4_name,dict"`
	Dimension4Value           string  `parquet:"dimension_4_value,dict"`
	Other                     bool    `parquet:"other"`
	Metric                    string  `parquet:"metric,dict"`
	Unit                      string  `parquet:"unit,dict"`
	Value                     float64 `parquet:"value"`
	ValueLayer                string  `parquet:"value_layer,dict"`
	ReceivedRecords           uint64  `parquet:"received_records"`
	UnknownSamplingRecords    uint64  `parquet:"unknown_sampling_records"`
	QualityRecords            uint64  `parquet:"quality_records"`
	SamplingCompleteness      float64 `parquet:"sampling_completeness"`
	SamplingCompletenessKnown bool    `parquet:"sampling_completeness_known"`
	QualityRecordRatio        float64 `parquet:"quality_record_ratio"`
	QualityRecordRatioKnown   bool    `parquet:"quality_record_ratio_known"`
	DimensionSnapshotID       string  `parquet:"dimension_snapshot_id,dict"`
	GeoVersion                string  `parquet:"geo_version,dict"`
	ClassificationVersion     uint32  `parquet:"classification_version"`
	ObservedAt                int64   `parquet:"observed_at,timestamp(millisecond:utc)"`
	QueryCompleteRatio        float64 `parquet:"query_complete_ratio"`
	QueryPartial              bool    `parquet:"query_partial"`
	QueryUnknownRatio         float64 `parquet:"query_unknown_ratio"`
	QueryWarnings             string  `parquet:"query_warnings"`
	QuerySource               string  `parquet:"query_source,dict"`
	QueryStepSeconds          uint32  `parquet:"query_step_seconds"`
	QueryAsOf                 int64   `parquet:"query_as_of,timestamp(millisecond:utc)"`
	QueryPolicyVersion        uint64  `parquet:"query_policy_version"`
	ReportKind                string  `parquet:"report_kind,dict"`
	ReportPanel               string  `parquet:"report_panel,dict"`
	ReportPanelStatus         string  `parquet:"report_panel_status,dict"`
	ReportPanelReason         string  `parquet:"report_panel_reason"`
}

func RenderParquetExportRows(rows ExportRows) ([]byte, error) {
	items := make([]flowExportParquetRow, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		dimensionNames := [4]string{}
		dimensions := [4]string{}
		copy(dimensionNames[:], row.DimensionNames)
		copy(dimensions[:], row.DimensionValues)
		items = append(items, flowExportParquetRow{
			Bucket:         row.Bucket.UnixMilli(),
			Dimension1Name: dimensionNames[0], Dimension1Value: dimensions[0], Dimension2Name: dimensionNames[1], Dimension2Value: dimensions[1],
			Dimension3Name: dimensionNames[2], Dimension3Value: dimensions[2], Dimension4Name: dimensionNames[3], Dimension4Value: dimensions[3],
			Other: row.Other, Metric: row.Metric, Unit: row.Unit, Value: row.Value, ValueLayer: string(row.ValueLayer),
			ReceivedRecords: row.ReceivedRecords, UnknownSamplingRecords: row.UnknownSamplingRecords, QualityRecords: row.QualityRecords,
			SamplingCompleteness: row.SamplingCompleteness, SamplingCompletenessKnown: row.SamplingCompletenessKnown,
			QualityRecordRatio: row.QualityRecordRatio, QualityRecordRatioKnown: row.QualityRecordRatioKnown,
			DimensionSnapshotID: row.DimensionSnapshotID, GeoVersion: row.GeoVersion, ClassificationVersion: row.ClassificationVersion,
			ObservedAt: row.ObservedAt.UnixMilli(), QueryCompleteRatio: row.QueryCompleteRatio, QueryPartial: row.QueryPartial,
			QueryUnknownRatio: row.QueryUnknownRatio, QueryWarnings: row.QueryWarnings,
			QuerySource: row.QuerySource, QueryStepSeconds: row.QueryStepSeconds,
			QueryAsOf: row.QueryAsOf.UnixMilli(), QueryPolicyVersion: row.QueryPolicyVersion,
			ReportKind: row.ReportKind, ReportPanel: row.ReportPanel,
			ReportPanelStatus: row.ReportPanelStatus, ReportPanelReason: row.ReportPanelReason,
		})
	}
	var output bytes.Buffer
	if err := parquet.Write(&output, items); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
