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
	query.Parameters, err = json.Marshal(parameters)
	if err != nil {
		return ExportTask{}, err
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
	var envelope struct {
		Points     json.RawMessage                 `json:"points"`
		Metric     flowquery.MetricDefinition      `json:"metric"`
		Dimension  flowquery.DimensionDefinition   `json:"dimension"`
		Dimensions []flowquery.DimensionDefinition `json:"dimensions"`
	}
	if err := json.Unmarshal(result.Data, &envelope); err != nil {
		return ExportRows{}, fmt.Errorf("decode Flow export result: %w", err)
	}
	warnings := strings.Join(result.Meta.Warnings, "; ")
	decorate := func(row *FlowExportRow) {
		row.Metric = string(envelope.Metric.Name)
		row.Unit = envelope.Metric.Unit
		row.ValueLayer = layer
		row.QueryCompleteRatio = result.Meta.CompleteRatio
		row.QueryPartial = result.Meta.Partial
		row.QueryUnknownRatio = result.Meta.UnknownRatio
		row.QueryWarnings = warnings
		row.QuerySource = result.Meta.Source
		row.QueryStepSeconds = result.Meta.StepSeconds
		row.QueryAsOf = result.Meta.AsOf
		row.QueryPolicyVersion = result.Meta.PolicyVersion
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
		})
	}
	var output bytes.Buffer
	if err := parquet.Write(&output, items); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
