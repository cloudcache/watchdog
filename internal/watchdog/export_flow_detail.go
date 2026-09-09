package watchdog

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/parquet-go/parquet-go"
)

const (
	FlowRecordDetailDataset                 = "flow.records"
	flowDetailExportCompletenessMode        = "detail_cursor_query"
	maxFlowDetailExportRows          uint32 = 250_000
	flowDetailExportPageSize                = 500
)

type flowDetailExportCreateRequest struct {
	Query            flowquery.DetailRequest `json:"query"`
	Format           ExportFormat            `json:"format"`
	RetentionSeconds uint32                  `json:"retention_seconds,omitempty"`
	MaxRows          uint32                  `json:"max_rows,omitempty"`
}

type flowDetailExportParameters struct {
	Detail flowquery.DetailRequest `json:"detail"`
}

type FlowDetailExportRows struct {
	View   flowquery.View
	Fields []flowquery.DetailField
	Rows   []FlowDetailExportRow
}

type FlowDetailExportRow struct {
	EventTime        time.Time
	SourceCoordinate flowquery.SourceCoordinate
	Values           []any
}

func prepareFlowDetailExportTask(ctx context.Context, gateway *QueryGateway, auth AuthContext, input flowDetailExportCreateRequest, now time.Time) (ExportTask, error) {
	if gateway == nil {
		return ExportTask{}, errors.New("Flow detail export query gateway is required")
	}
	if input.Query.View != flowquery.ViewRaw && input.Query.View != flowquery.ViewSupplier {
		return ExportTask{}, errors.New("Flow detail exports support raw or supplier view")
	}
	layer := QueryValueLayer(input.Query.View)
	access := exportAccessRequest(auth, ExportTask{DatasetKey: FlowRecordDetailDataset, ValueLayer: layer})
	if !queryLayerAuthorized(auth, layer) || !CanCreateExportLayer(access, layer, auth.Grants, auth.IsAdmin) {
		return ExportTask{}, errors.New("Flow detail view and export permissions are required")
	}
	if input.Format != ExportFormatCSV && input.Format != ExportFormatParquet {
		return ExportTask{}, errors.New("Flow detail export format must be csv or parquet")
	}
	if input.Query.Cursor != "" {
		return ExportTask{}, errors.New("Flow detail export cursor must be empty")
	}
	maximumRows := input.MaxRows
	if maximumRows == 0 {
		maximumRows = maxFlowDetailExportRows
	}
	if maximumRows > maxFlowDetailExportRows {
		return ExportTask{}, errors.New("Flow detail export max_rows must be 1..250000")
	}
	descriptor, ok := gateway.Datasets.Get(FlowTrafficDataset)
	if !ok {
		return ExportTask{}, errors.New("Flow dataset is not registered")
	}
	if err := gateway.requireModuleEnabled(ctx, auth.TenantID, descriptor.ModuleKey); err != nil {
		return ExportTask{}, err
	}

	input.Query.Limit = flowDetailExportPageSize
	compiled, err := flowquery.CompileDetail(flowquery.Scope{
		AllowedViews: []flowquery.View{input.Query.View},
	}, input.Query, now.UTC())
	if err != nil {
		return ExportTask{}, err
	}
	// Store the compiler-resolved projection. Default fields, IP spelling,
	// time zone and sort defaults must not drift between creation and execution.
	input.Query.IP = compiled.IP.String()
	input.Query.From, input.Query.To = compiled.From, compiled.To
	input.Query.Fields = append([]flowquery.DetailField(nil), compiled.Fields...)
	input.Query.Sort = compiled.Sort
	parameters, err := json.Marshal(flowDetailExportParameters{Detail: input.Query})
	if err != nil {
		return ExportTask{}, err
	}
	query, err := normalizeQueryRequest(QueryRequest{
		Dataset: FlowRecordDetailDataset, From: compiled.From, To: compiled.To,
		Limit: maximumRows, ValueLayer: layer, Parameters: parameters,
	})
	if err != nil {
		return ExportTask{}, err
	}
	queryJSON, queryHash, err := canonicalJSONHash(exportQuerySnapshot{
		SchemaVersion: 1, Query: query, Aggregation: AggregationAverageFiveMinute,
	})
	if err != nil {
		return ExportTask{}, err
	}
	capability, err := flowquery.DetailCapability(input.Query.View)
	if err != nil {
		return ExportTask{}, err
	}
	_, descriptorHash, err := canonicalJSONHash(struct {
		Dataset       string                         `json:"dataset"`
		SchemaVersion int                            `json:"schema_version"`
		PageSize      int                            `json:"page_size"`
		MaximumRows   uint32                         `json:"maximum_rows"`
		Capability    flowquery.DetailViewCapability `json:"capability"`
	}{FlowRecordDetailDataset, 1, flowDetailExportPageSize, maxFlowDetailExportRows, capability})
	if err != nil {
		return ExportTask{}, err
	}
	versionsJSON, _, err := canonicalJSONHash(exportVersionSnapshot{
		SchemaVersion: 1, DatasetDescriptorHash: descriptorHash,
		CompletenessMissingRatio: "0", CompletenessMode: flowDetailExportCompletenessMode, SnapshotComplete: true,
	})
	if err != nil {
		return ExportTask{}, err
	}
	authorizationJSON, _, err := canonicalJSONHash(exportAuthorizationSnapshot{
		SchemaVersion: 1, SubjectID: auth.UserID, RequiredAction: exportLayerAction(layer),
		ResourceType: access.Resource.Type, ResourceID: access.Resource.ID, Admin: auth.IsAdmin,
	})
	if err != nil {
		return ExportTask{}, err
	}
	retentionSeconds := input.RetentionSeconds
	if retentionSeconds == 0 {
		retentionSeconds = defaultExportRetentionSeconds
	}
	task := ExportTask{
		TenantID: auth.TenantID, CreatedBy: auth.UserID, ContractVersion: ExportExecutionContractVersion,
		DatasetKey: FlowRecordDetailDataset, QueryJSON: queryJSON, QueryHash: queryHash,
		ValueLayer: layer, VersionsJSON: versionsJSON, AuthorizationJSON: authorizationJSON,
		RetentionSeconds: retentionSeconds, PeriodType: PeriodCustom, RangeStart: query.From, RangeEnd: query.To,
		Aggregation: AggregationAverageFiveMinute, ValueMode: ExportValueCorrected,
		Format: input.Format, Status: ExportStatusPending,
	}
	return task, validateExportExecutionTask(task)
}

func validateFlowDetailExportSnapshot(task ExportTask, snapshot exportQuerySnapshot) error {
	if task.ValueLayer != QueryValueRaw && task.ValueLayer != QueryValueSupplier {
		return errors.New("Flow detail export value layer must be raw or supplier")
	}
	if task.TargetID != "" || task.PortID != "" || task.Step != 0 || snapshot.Query.StepSeconds != 0 || snapshot.Query.Cursor != "" {
		return errors.New("Flow detail export task projection is invalid")
	}
	if snapshot.Query.Limit == 0 || snapshot.Query.Limit > maxFlowDetailExportRows {
		return errors.New("Flow detail export row limit is invalid")
	}
	var parameters flowDetailExportParameters
	if err := decodeStrictJSON(snapshot.Query.Parameters, &parameters); err != nil {
		return errors.New("Flow detail export query parameters are invalid")
	}
	if parameters.Detail.Cursor != "" || parameters.Detail.Limit != flowDetailExportPageSize || QueryValueLayer(parameters.Detail.View) != task.ValueLayer ||
		!parameters.Detail.From.Equal(snapshot.Query.From) || !parameters.Detail.To.Equal(snapshot.Query.To) {
		return errors.New("Flow detail export query parameters do not match the task")
	}
	compiled, err := flowquery.CompileDetail(flowquery.Scope{
		AllowedViews: []flowquery.View{parameters.Detail.View},
	}, parameters.Detail, snapshot.Query.To)
	if err != nil || !slices.Equal(compiled.Fields, parameters.Detail.Fields) || compiled.Sort != parameters.Detail.Sort || compiled.IP.String() != parameters.Detail.IP {
		return errors.New("Flow detail export query parameters are not compiler-normalized")
	}
	return nil
}

func (p QueryGatewayExportDataProvider) loadFlowDetailExportRows(ctx context.Context, task ExportTask) (ExportRows, error) {
	if p.FlowRecords == nil {
		return ExportRows{}, errors.New("Flow detail export runner is required")
	}
	var snapshot exportQuerySnapshot
	if err := decodeStrictJSON(task.QueryJSON, &snapshot); err != nil {
		return ExportRows{}, err
	}
	var parameters flowDetailExportParameters
	if err := decodeStrictJSON(snapshot.Query.Parameters, &parameters); err != nil {
		return ExportRows{}, err
	}
	request := parameters.Detail
	rows := FlowDetailExportRows{
		View: request.View, Fields: append([]flowquery.DetailField(nil), request.Fields...),
		Rows: make([]FlowDetailExportRow, 0, min(int(snapshot.Query.Limit), flowDetailExportPageSize)),
	}
	seenCursors := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return ExportRows{}, err
		}
		compiled, err := flowquery.CompileDetail(flowquery.Scope{
			AllowedViews: []flowquery.View{request.View},
		}, request, snapshot.Query.To)
		if err != nil {
			return ExportRows{}, err
		}
		page, err := p.FlowRecords.Run(ctx, compiled)
		if err != nil {
			return ExportRows{}, err
		}
		if page.View != request.View || !slices.Equal(page.Fields, request.Fields) || len(page.Rows) > int(request.Limit) {
			return ExportRows{}, errors.New("Flow detail export runner returned an invalid page")
		}
		if request.View == flowquery.ViewSupplier && (!page.SupplierProvenanceComplete || page.MinimumFactSchema < 2) {
			return ExportRows{}, errors.New("Flow supplier detail provenance is incomplete")
		}
		if uint64(len(rows.Rows)+len(page.Rows)) > uint64(snapshot.Query.Limit) {
			return ExportRows{}, errors.New("Flow detail export exceeds its frozen row limit")
		}
		for _, source := range page.Rows {
			values := make([]any, len(request.Fields))
			for index, field := range request.Fields {
				value, ok := source.Values[field]
				if !ok {
					return ExportRows{}, fmt.Errorf("Flow detail export row is missing field %q", field)
				}
				values[index] = value
			}
			rows.Rows = append(rows.Rows, FlowDetailExportRow{
				EventTime: source.EventTime.UTC(), SourceCoordinate: source.SourceCoordinate, Values: values,
			})
		}
		if !page.HasMore {
			break
		}
		if page.NextCursor == "" {
			return ExportRows{}, errors.New("Flow detail export page omitted its continuation cursor")
		}
		if _, exists := seenCursors[page.NextCursor]; exists {
			return ExportRows{}, errors.New("Flow detail export cursor did not advance")
		}
		seenCursors[page.NextCursor] = struct{}{}
		request.Cursor = page.NextCursor
	}
	if len(rows.Rows) == 0 {
		return ExportRows{}, errors.New("no Flow detail rows returned")
	}
	return ExportRows{FlowDetails: &rows}, nil
}

func RenderFlowDetailCSV(rows FlowDetailExportRows) ([]byte, error) {
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

func RenderFlowDetailParquet(rows FlowDetailExportRows) ([]byte, error) {
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
			return nil, fmt.Errorf("Flow detail field %q: %w", field, err)
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

func validateFlowDetailExportRows(rows FlowDetailExportRows) error {
	if rows.View != flowquery.ViewRaw && rows.View != flowquery.ViewSupplier {
		return errors.New("Flow detail export view must be raw or supplier")
	}
	if len(rows.Fields) == 0 || len(rows.Rows) == 0 {
		return errors.New("Flow detail export fields and rows are required")
	}
	seen := make(map[flowquery.DetailField]struct{}, len(rows.Fields))
	for _, field := range rows.Fields {
		if _, exists := seen[field]; exists {
			return fmt.Errorf("Flow detail export field %q is duplicated", field)
		}
		seen[field] = struct{}{}
	}
	for _, row := range rows.Rows {
		if row.EventTime.IsZero() || len(row.Values) != len(rows.Fields) {
			return errors.New("Flow detail export row does not match its schema")
		}
		for index, value := range row.Values {
			if _, err := flowDetailParquetNode(value); err != nil {
				return fmt.Errorf("Flow detail field %q: %w", rows.Fields[index], err)
			}
			if reflect.TypeOf(value) != reflect.TypeOf(rows.Rows[0].Values[index]) {
				return fmt.Errorf("Flow detail field %q changed type", rows.Fields[index])
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
