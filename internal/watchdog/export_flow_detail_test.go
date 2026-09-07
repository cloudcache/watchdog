package watchdog

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/parquet-go/parquet-go"
)

type flowDetailExportRunnerStub struct {
	pages []flowquery.DetailResult
	calls int
}

func (r *flowDetailExportRunnerStub) Run(_ context.Context, _ flowquery.CompiledDetail) (flowquery.DetailResult, error) {
	if r.calls >= len(r.pages) {
		return flowquery.DetailResult{}, errorsForTest("unexpected detail page")
	}
	page := r.pages[r.calls]
	r.calls++
	return page, nil
}

func (r *flowDetailExportRunnerStub) RunFacet(context.Context, flowquery.CompiledDetailFacet) (flowquery.DetailFacetResult, error) {
	return flowquery.DetailFacetResult{}, nil
}

func flowDetailExportAuth(layer QueryValueLayer) AuthContext {
	viewAction, exportAction := ActionViewRaw, ActionExportRaw
	if layer == QueryValueSupplier {
		viewAction, exportAction = ActionViewSupplier, ActionExportSupplier
	}
	return AuthContext{
		TenantID: "tenant-a", UserID: "user-a",
		Grants: []Permission{{
			TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a",
			ResourceType: ResourceTenant, ResourceID: "tenant-a", Actions: []Action{viewAction, exportAction},
		}},
	}
}

func flowDetailExportInput(start time.Time, view flowquery.View, format ExportFormat) flowDetailExportCreateRequest {
	return flowDetailExportCreateRequest{
		Query: flowquery.DetailRequest{
			IP: "192.0.2.8", Endpoint: flowquery.DetailEndpointSource, From: start, To: start.Add(time.Hour), View: view,
			Fields: []flowquery.DetailField{flowquery.DetailFieldRawBytes, flowquery.DetailFieldSourceIP},
			Limit:  25,
		},
		Format: format, MaxRows: 2,
	}
}

func TestPrepareFlowDetailExportFreezesCompilerProjectionAndPermissions(t *testing.T) {
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	task, err := prepareFlowDetailExportTask(context.Background(), newFlowExportGatewayFixture(t, &queryProviderStub{}),
		flowDetailExportAuth(QueryValueRaw), flowDetailExportInput(start, flowquery.ViewRaw, ExportFormatCSV), start.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if task.DatasetKey != FlowRecordDetailDataset || task.ValueLayer != QueryValueRaw || task.Step != 0 || task.TargetID != "" {
		t.Fatalf("task = %+v", task)
	}
	var snapshot exportQuerySnapshot
	if err := decodeStrictJSON(task.QueryJSON, &snapshot); err != nil {
		t.Fatal(err)
	}
	var parameters flowDetailExportParameters
	if err := decodeStrictJSON(snapshot.Query.Parameters, &parameters); err != nil {
		t.Fatal(err)
	}
	if snapshot.Query.Limit != 2 || parameters.Detail.Limit != flowDetailExportPageSize || parameters.Detail.Sort.Field != "event_time" ||
		parameters.Detail.Sort.Direction != "desc" || strings.Join(detailFieldStrings(parameters.Detail.Fields), ",") != "raw_bytes,src_ip" {
		t.Fatalf("snapshot=%+v detail=%+v", snapshot, parameters.Detail)
	}
	if err := validateExportExecutionTask(task); err != nil {
		t.Fatalf("validate: %v", err)
	}

	denied := flowDetailExportAuth(QueryValueRaw)
	denied.Grants[0].Actions = []Action{ActionExportRaw}
	if _, err := prepareFlowDetailExportTask(context.Background(), newFlowExportGatewayFixture(t, &queryProviderStub{}), denied,
		flowDetailExportInput(start, flowquery.ViewRaw, ExportFormatCSV), start.Add(2*time.Hour)); err == nil {
		t.Fatal("missing view_raw must be rejected")
	}
	customer := flowDetailExportInput(start, flowquery.ViewCustomer, ExportFormatCSV)
	if _, err := prepareFlowDetailExportTask(context.Background(), newFlowExportGatewayFixture(t, &queryProviderStub{}), flowExportAuth(), customer, start.Add(2*time.Hour)); err == nil {
		t.Fatal("customer detail export must not be confused with aggregate export")
	}
}

func TestFlowDetailExportTraversesCursorAndWritesDynamicSchema(t *testing.T) {
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	auth := flowDetailExportAuth(QueryValueSupplier)
	input := flowDetailExportInput(start, flowquery.ViewSupplier, ExportFormatCSV)
	input.Query.Fields = []flowquery.DetailField{flowquery.DetailFieldCategory, flowquery.DetailFieldRawBytes}
	task, err := prepareFlowDetailExportTask(context.Background(), newFlowExportGatewayFixture(t, &queryProviderStub{}), auth, input, start.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	coordinate := flowquery.SourceCoordinate{SourceStreamID: "topic-a", KafkaPartition: 2, KafkaOffset: 8, RecordIndex: 1}
	cursor, err := flowquery.EncodeDetailCursor(start.Add(30*time.Minute), coordinate)
	if err != nil {
		t.Fatal(err)
	}
	fields := []flowquery.DetailField{flowquery.DetailFieldCategory, flowquery.DetailFieldRawBytes}
	runner := &flowDetailExportRunnerStub{pages: []flowquery.DetailResult{
		{View: flowquery.ViewSupplier, Fields: fields, Rows: []flowquery.DetailRow{{
			EventTime: start.Add(30 * time.Minute), SourceCoordinate: coordinate,
			Values: map[flowquery.DetailField]any{flowquery.DetailFieldCategory: "=unsafe", flowquery.DetailFieldRawBytes: uint64(42)},
		}}, HasMore: true, NextCursor: cursor, SupplierProvenanceComplete: true, MinimumFactSchema: 2},
		{View: flowquery.ViewSupplier, Fields: fields, Rows: []flowquery.DetailRow{{
			EventTime:        start.Add(20 * time.Minute),
			SourceCoordinate: flowquery.SourceCoordinate{SourceStreamID: "topic-a", KafkaPartition: 2, KafkaOffset: 9},
			Values:           map[flowquery.DetailField]any{flowquery.DetailFieldCategory: "normal", flowquery.DetailFieldRawBytes: uint64(84)},
		}}, SupplierProvenanceComplete: true, MinimumFactSchema: 2},
	}}
	rows, handled, err := (QueryGatewayExportDataProvider{FlowRecords: runner}).LoadExportRows(ContextWithAuth(context.Background(), auth), task)
	if err != nil || !handled || rows.FlowDetails == nil || len(rows.FlowDetails.Rows) != 2 || runner.calls != 2 {
		t.Fatalf("handled=%v rows=%+v calls=%d err=%v", handled, rows, runner.calls, err)
	}
	csvData, err := RenderFlowDetailCSV(*rows.FlowDetails)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(strings.NewReader(string(csvData))).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 || strings.Join(records[0], ",") != "event_time,source_stream_id,kafka_partition,kafka_offset,record_index,category,raw_bytes" ||
		records[1][5] != "'=unsafe" || records[1][6] != "42" {
		t.Fatalf("csv records = %#v", records)
	}
	parquetData, err := RenderFlowDetailParquet(*rows.FlowDetails)
	if err != nil || !bytes.HasPrefix(parquetData, []byte("PAR1")) || !bytes.HasSuffix(parquetData, []byte("PAR1")) {
		t.Fatalf("invalid parquet bytes=%d err=%v", len(parquetData), err)
	}
	file, err := parquet.OpenFile(bytes.NewReader(parquetData), int64(len(parquetData)))
	if err != nil {
		t.Fatal(err)
	}
	schema := file.Schema().String()
	for _, field := range []string{"category", "raw_bytes", "source_stream_id", "kafka_offset"} {
		if !strings.Contains(schema, field) {
			t.Fatalf("parquet schema %q is missing %q", schema, field)
		}
	}
}

func TestAPIFlowDetailExportCreatesExistingOperationExport(t *testing.T) {
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	repo := &fakeExportRepository{}
	runner := &flowDetailExportRunnerStub{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    func(*http.Request) (AuthContext, error) { return flowDetailExportAuth(QueryValueRaw), nil },
		Exports: repo, QueryGateway: newFlowExportGatewayFixture(t, &queryProviderStub{}), FlowRecords: runner,
		FlowRecordNow: func() time.Time { return start.Add(2 * time.Hour) },
	})
	body, err := json.Marshal(flowDetailExportInput(start, flowquery.ViewRaw, ExportFormatParquet))
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/flow/records/exports", bytes.NewReader(body)))
	if recorder.Code != http.StatusCreated || len(repo.tasks) != 1 || repo.tasks[0].DatasetKey != FlowRecordDetailDataset ||
		repo.tasks[0].ValueLayer != QueryValueRaw || repo.tasks[0].Format != ExportFormatParquet {
		t.Fatalf("status=%d tasks=%+v body=%s", recorder.Code, repo.tasks, recorder.Body.String())
	}
}

func TestFlowDetailExportUsesOperationLifecycleAndRechecksBothPermissions(t *testing.T) {
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	auth := flowDetailExportAuth(QueryValueRaw)
	task, err := prepareFlowDetailExportTask(context.Background(), newFlowExportGatewayFixture(t, &queryProviderStub{}),
		auth, flowDetailExportInput(start, flowquery.ViewRaw, ExportFormatCSV), start.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	task.ID, task.OperationJobID = "export-detail-a", "job-detail-a"
	payload, _, err := encodeExportExecutionPayload(task)
	if err != nil {
		t.Fatal(err)
	}
	fields := []flowquery.DetailField{flowquery.DetailFieldRawBytes, flowquery.DetailFieldSourceIP}
	runner := &flowDetailExportRunnerStub{pages: []flowquery.DetailResult{{
		View: flowquery.ViewRaw, Fields: fields, Rows: []flowquery.DetailRow{{
			EventTime:        start.Add(time.Minute),
			SourceCoordinate: flowquery.SourceCoordinate{SourceStreamID: "topic-a", KafkaPartition: 1, KafkaOffset: 3},
			Values:           map[flowquery.DetailField]any{flowquery.DetailFieldRawBytes: uint64(10), flowquery.DetailFieldSourceIP: "192.0.2.8"},
		}},
	}}}
	repo := &executionExportRepository{task: task}
	writer := &CSVExportWriter{}
	authorization := &exportAuthorizationStub{
		tenant: Tenant{ID: auth.TenantID, Status: "active"}, user: User{ID: auth.UserID, TenantID: auth.TenantID, Status: "active"}, grants: auth.Grants,
	}
	handler := NewExportExecutionJobHandler(repo, ExportWorker{
		Repo: repo, Data: QueryGatewayExportDataProvider{FlowRecords: runner}, Writer: writer,
	}, ExportExecutionJobDependencies{Authorization: authorization})
	fileRef, err := handler(context.Background(), OperationJob{ID: task.OperationJobID, TenantID: auth.TenantID, CheckpointJSON: payload})
	if err != nil || fileRef != "exports/export-detail-a.csv" || repo.task.Status != ExportStatusComplete {
		t.Fatalf("file=%q task=%+v err=%v", fileRef, repo.task, err)
	}

	viewRevoked := auth
	viewRevoked.Grants[0].Actions = []Action{ActionExportRaw}
	if canExecuteExportTask(viewRevoked, task) {
		t.Fatal("detail export must fail closed when view_raw is revoked")
	}
	exportRevoked := auth
	exportRevoked.Grants[0].Actions = []Action{ActionViewRaw}
	if canExecuteExportTask(exportRevoked, task) {
		t.Fatal("detail export must fail closed when export_raw is revoked")
	}
}

func TestFlowDetailExportFailsClosedOnSupplierProvenanceAndCursorLoop(t *testing.T) {
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	auth := flowDetailExportAuth(QueryValueSupplier)
	input := flowDetailExportInput(start, flowquery.ViewSupplier, ExportFormatCSV)
	task, err := prepareFlowDetailExportTask(context.Background(), newFlowExportGatewayFixture(t, &queryProviderStub{}), auth, input, start.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	fields := []flowquery.DetailField{flowquery.DetailFieldRawBytes, flowquery.DetailFieldSourceIP}
	row := flowquery.DetailRow{
		EventTime: start.Add(time.Minute), SourceCoordinate: flowquery.SourceCoordinate{SourceStreamID: "topic-a", KafkaOffset: 1},
		Values: map[flowquery.DetailField]any{flowquery.DetailFieldRawBytes: uint64(1), flowquery.DetailFieldSourceIP: "192.0.2.8"},
	}
	provenanceRunner := &flowDetailExportRunnerStub{pages: []flowquery.DetailResult{{View: flowquery.ViewSupplier, Fields: fields, Rows: []flowquery.DetailRow{row}}}}
	if _, _, err := (QueryGatewayExportDataProvider{FlowRecords: provenanceRunner}).LoadExportRows(ContextWithAuth(context.Background(), auth), task); err == nil || !strings.Contains(err.Error(), "provenance") {
		t.Fatalf("incomplete supplier provenance error = %v", err)
	}

	cursor, err := flowquery.EncodeDetailCursor(row.EventTime, row.SourceCoordinate)
	if err != nil {
		t.Fatal(err)
	}
	loopRunner := &flowDetailExportRunnerStub{pages: []flowquery.DetailResult{
		{View: flowquery.ViewSupplier, Fields: fields, Rows: []flowquery.DetailRow{row}, HasMore: true, NextCursor: cursor, SupplierProvenanceComplete: true, MinimumFactSchema: 2},
		{View: flowquery.ViewSupplier, Fields: fields, HasMore: true, NextCursor: cursor, SupplierProvenanceComplete: true, MinimumFactSchema: 2},
	}}
	if _, _, err := (QueryGatewayExportDataProvider{FlowRecords: loopRunner}).LoadExportRows(ContextWithAuth(context.Background(), auth), task); err == nil || !strings.Contains(err.Error(), "cursor did not advance") {
		t.Fatalf("cursor loop error = %v", err)
	}
}

func TestMySQLFlowDetailExportCreatesExistingOperationJob(t *testing.T) {
	db, tenantID := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	userID := ID("user_flow_detail_export")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES (?, ?, 'flow-detail-export@example.test', 'Flow Detail Export', 'active')
	`, userID, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM export_tasks WHERE tenant_id = ? AND created_by = ?`, tenantID, userID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM operation_jobs WHERE tenant_id = ? AND job_type = ? AND created_by = ?`, tenantID, ExportExecutionJobType, userID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM users WHERE tenant_id = ? AND id = ?`, tenantID, userID)
	})
	auth := flowDetailExportAuth(QueryValueSupplier)
	auth.TenantID, auth.UserID = tenantID, userID
	for index := range auth.Grants {
		auth.Grants[index].TenantID, auth.Grants[index].SubjectID, auth.Grants[index].ResourceID = tenantID, userID, tenantID
	}
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	task, err := prepareFlowDetailExportTask(ctx, newFlowExportGatewayFixtureForTenant(t, tenantID, &queryProviderStub{}),
		auth, flowDetailExportInput(start, flowquery.ViewSupplier, ExportFormatParquet), start.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	task.ID = "export_flow_detail_mysql"
	created, err := store.CreateExportTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if created.OperationJobID == "" || created.DatasetKey != FlowRecordDetailDataset || created.Step != 0 || created.ValueLayer != QueryValueSupplier {
		t.Fatalf("created = %+v", created)
	}
	if err := validateExportExecutionTask(created); err != nil {
		t.Fatalf("stored task must retain its detail snapshot: %v", err)
	}
}

func detailFieldStrings(fields []flowquery.DetailField) []string {
	result := make([]string, len(fields))
	for index, field := range fields {
		result[index] = string(field)
	}
	return result
}
