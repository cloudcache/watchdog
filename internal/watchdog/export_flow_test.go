package watchdog

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
)

func newFlowExportGatewayFixture(t *testing.T, provider *queryProviderStub) *QueryGateway {
	return newFlowExportGatewayFixtureForTenant(t, "tenant-a", provider)
}

func newFlowExportGatewayFixtureForTenant(t *testing.T, tenantID ID, provider *queryProviderStub) *QueryGateway {
	t.Helper()
	registries, err := NewBuiltinPlatformRegistries()
	if err != nil {
		t.Fatal(err)
	}
	policies := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	policy := QueryDatasetPolicy{
		TenantID: tenantID, DatasetKey: FlowTrafficDataset, Enabled: true, AllowCustomer: true,
		MaxRangeSeconds: 86_400, MaxConcurrent: 2, MaxResultRows: 250_000, QueryTimeoutMS: 5_000, RowVersion: 9,
	}
	policies.policies[policies.key(policy.TenantID, policy.DatasetKey)] = policy
	providers := NewQueryProviderRegistry()
	if err := providers.Register(QueryProviderRegistration{
		Kind: DatasetProviderClickHouse, Provider: provider, Enabled: true, MaxConcurrent: 2,
	}); err != nil {
		t.Fatal(err)
	}
	gateway, err := NewQueryGateway(registries, nil, policies, providers)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func flowExportAuth() AuthContext {
	return AuthContext{
		TenantID: "tenant-a", UserID: "user-a",
		Grants: []Permission{{
			TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a",
			ResourceType: ResourceTenant, ResourceID: "tenant-a",
			Actions: []Action{ActionViewCustomer, ActionExportCustomer},
		}},
	}
}

func flowExportQuery(start time.Time) QueryRequest {
	return QueryRequest{
		Dataset: FlowTrafficDataset, From: start, To: start.Add(time.Hour), StepSeconds: 0,
		Limit: 250_000, ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{"metric":"estimated_bps","dimensions":["geo.country","isp"],"top_n":20,"include_other":true,"target_points":300,"timezone":"UTC","table":{"sort_by":"maximum","sort_direction":"desc","limit":25}}`),
	}
}

func TestPrepareFlowExportFreezesFullQueryWithoutTableProjection(t *testing.T) {
	gateway := newFlowExportGatewayFixture(t, &queryProviderStub{})
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	task, err := prepareFlowExportExecutionTask(context.Background(), gateway, flowExportAuth(), flowExportQuery(start), ExportFormatCSV, 0)
	if err != nil {
		t.Fatal(err)
	}
	if task.DatasetKey != FlowTrafficDataset || task.ContractVersion != ExportExecutionContractVersion || task.TargetID != "" || task.Step != 0 || len(task.QueryHash) != 64 {
		t.Fatalf("task = %+v", task)
	}
	var snapshot exportQuerySnapshot
	if err := decodeStrictJSON(task.QueryJSON, &snapshot); err != nil {
		t.Fatal(err)
	}
	var parameters flowAggregateQueryParameters
	if err := decodeStrictJSON(snapshot.Query.Parameters, &parameters); err != nil {
		t.Fatal(err)
	}
	if parameters.Table != nil || snapshot.Query.Limit != 250_000 || snapshot.Query.StepSeconds != 0 {
		t.Fatalf("snapshot = %+v parameters = %+v", snapshot, parameters)
	}
	if err := validateExportExecutionTask(task); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestPrepareFlowExportFreezesProviderPreparedOperatorBinding(t *testing.T) {
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	provider := &queryProviderStub{prepare: func(_ context.Context, request QueryProviderRequest) (json.RawMessage, error) {
		if request.TenantID != "tenant-a" || request.ValueLayer != QueryValueCustomer {
			t.Fatalf("prepare request = %+v", request)
		}
		return json.RawMessage(`{
			"metric":"estimated_bps","dimension":"category","top_n":20,"include_other":true,"target_points":300,"timezone":"UTC",
			"filters":{"dimension_snapshot_ids":["snapshot-1"],"classification_versions":[7]},
			"filter":{"op":"predicate","field":"isp","operator":"eq","values":["12"]},
			"operator_selection":{"schema_version":1,"operator_id":"operator-a","flow_isp_id":12,"publication_ids":["publication-1"],"dimension_snapshot_ids":["snapshot-1"],"classification_versions":[7]}
		}`), nil
	}}
	gateway := newFlowExportGatewayFixture(t, provider)
	query := flowExportQuery(start)
	var parameters map[string]any
	if err := json.Unmarshal(query.Parameters, &parameters); err != nil {
		t.Fatal(err)
	}
	parameters["operator_selection"] = map[string]any{"operator_id": "operator-a"}
	query.Parameters, _ = json.Marshal(parameters)
	task, err := prepareFlowExportExecutionTask(context.Background(), gateway, flowExportAuth(), query, ExportFormatCSV, 0)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot exportQuerySnapshot
	if err := decodeStrictJSON(task.QueryJSON, &snapshot); err != nil {
		t.Fatal(err)
	}
	var frozen flowAggregateQueryParameters
	if err := decodeStrictJSON(snapshot.Query.Parameters, &frozen); err != nil {
		t.Fatal(err)
	}
	if frozen.OperatorSelection == nil || frozen.OperatorSelection.SchemaVersion != 1 || frozen.OperatorSelection.FlowISPID != 12 ||
		len(frozen.OperatorSelection.PublicationIDs) != 1 || frozen.OperatorSelection.PublicationIDs[0] != "publication-1" || frozen.Table != nil {
		t.Fatalf("frozen parameters = %+v", frozen)
	}
}

func TestFlowExportProviderAndWriterPreserveJointRows(t *testing.T) {
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	result := flowquery.JointResult{
		Metric: flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bps"},
		Dimensions: []flowquery.DimensionDefinition{
			{Kind: flowquery.DimensionGeoCountry, Additive: true},
			{Kind: flowquery.DimensionISP, Additive: true},
		},
		Points: []flowquery.JointPoint{{
			Bucket: start, DimensionValues: []string{"=unsafe", "AS64500"}, Value: 42.5,
			DimensionSnapshotID: "dim-7", GeoVersion: "geo-3", ClassificationVersion: 11,
			ReceivedRecords: 9, UnknownSamplingRecords: 2, QualityRecords: 1, ObservedAt: start.Add(time.Minute),
		}},
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	gateway := newFlowExportGatewayFixture(t, &queryProviderStub{query: func(context.Context, QueryProviderRequest) (QueryProviderResult, error) {
		return QueryProviderResult{Data: data, Unit: "bps", Timezone: "UTC", StepSeconds: 60}, nil
	}})
	auth := flowExportAuth()
	task, err := prepareFlowExportExecutionTask(context.Background(), gateway, auth, flowExportQuery(start), ExportFormatCSV, 0)
	if err != nil {
		t.Fatal(err)
	}
	rows, handled, err := (QueryGatewayExportDataProvider{Gateway: gateway}).LoadExportRows(ContextWithAuth(context.Background(), auth), task)
	if err != nil || !handled || len(rows.Rows) != 1 {
		t.Fatalf("handled=%v rows=%+v err=%v", handled, rows, err)
	}
	dataCSV, err := RenderCSVExportRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(strings.NewReader(string(dataCSV))).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[1][1] != "geo.country" || records[1][2] != "'=unsafe" ||
		records[1][3] != "isp" || records[1][4] != "AS64500" || records[1][12] != "42.5" {
		t.Fatalf("records = %#v", records)
	}
	writer := &CSVExportWriter{}
	artifact, handled, err := writer.WriteExportRows(context.Background(), task, rows)
	if err != nil || !handled || artifact.RowCount != 1 || artifact.ContentType != "text/csv; charset=utf-8" {
		t.Fatalf("handled=%v artifact=%+v err=%v", handled, artifact, err)
	}
	dataParquet, err := RenderParquetExportRows(rows)
	if err != nil || !bytes.HasPrefix(dataParquet, []byte("PAR1")) || !bytes.HasSuffix(dataParquet, []byte("PAR1")) {
		t.Fatalf("invalid parquet artifact: bytes=%d err=%v", len(dataParquet), err)
	}
}

func TestFlowExportExecutionUsesExistingOperationJobLifecycle(t *testing.T) {
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	result := flowquery.Result{
		Metric:    flowquery.MetricDefinition{Name: flowquery.MetricEstimatedBPS, Unit: "bits_per_second"},
		Dimension: flowquery.DimensionDefinition{Kind: flowquery.DimensionGeoCountry, Additive: true},
		Points: []flowquery.Point{{
			Bucket: start, DimensionValue: "CN", Value: 88, ReceivedRecords: 10,
			DimensionSnapshotID: "dim-1", GeoVersion: "geo-1", ClassificationVersion: 2,
			GeneratedAt: start.Add(time.Minute),
		}},
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	gateway := newFlowExportGatewayFixture(t, &queryProviderStub{query: func(context.Context, QueryProviderRequest) (QueryProviderResult, error) {
		return QueryProviderResult{Data: data, Unit: "bits_per_second", Timezone: "UTC", StepSeconds: 60}, nil
	}})
	auth := flowExportAuth()
	task, err := prepareFlowExportExecutionTask(context.Background(), gateway, auth, QueryRequest{
		Dataset: FlowTrafficDataset, From: start, To: start.Add(time.Hour), ValueLayer: QueryValueCustomer,
		Parameters: json.RawMessage(`{"metric":"estimated_bps","dimension":"geo.country","top_n":20,"include_other":true,"target_points":300,"timezone":"UTC"}`),
	}, ExportFormatCSV, 0)
	if err != nil {
		t.Fatal(err)
	}
	task.ID, task.OperationJobID = "export-flow-a", "job-flow-a"
	payload, _, err := encodeExportExecutionPayload(task)
	if err != nil {
		t.Fatal(err)
	}
	repo := &executionExportRepository{task: task}
	authorization := &exportAuthorizationStub{
		tenant: Tenant{ID: auth.TenantID, Status: "active"},
		user:   User{ID: auth.UserID, TenantID: auth.TenantID, Status: "active"},
		grants: auth.Grants,
	}
	handler := NewExportExecutionJobHandler(repo, ExportWorker{
		Repo: repo, Data: QueryGatewayExportDataProvider{Gateway: gateway}, Writer: &CSVExportWriter{},
	}, ExportExecutionJobDependencies{Authorization: authorization})
	ref, err := handler(context.Background(), OperationJob{ID: task.OperationJobID, TenantID: auth.TenantID, CheckpointJSON: payload})
	if err != nil || ref != "exports/export-flow-a.csv" || repo.task.Status != ExportStatusComplete {
		t.Fatalf("ref=%q task=%+v err=%v", ref, repo.task, err)
	}
}

func TestMySQLFlowExportCreatesExistingOperationJob(t *testing.T) {
	db, tenantID := operationJobTestDB(t)
	store := NewMySQLStore(db)
	ctx := context.Background()
	userID := ID("user_flow_export")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status)
		VALUES (?, ?, 'flow-export@example.test', 'Flow Export', 'active')
	`, userID, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext := context.Background()
		_, _ = db.ExecContext(cleanupContext, `DELETE FROM export_tasks WHERE tenant_id = ? AND created_by = ?`, tenantID, userID)
		_, _ = db.ExecContext(cleanupContext, `DELETE FROM operation_jobs WHERE tenant_id = ? AND job_type = ? AND created_by = ?`, tenantID, ExportExecutionJobType, userID)
		_, _ = db.ExecContext(cleanupContext, `DELETE FROM users WHERE tenant_id = ? AND id = ?`, tenantID, userID)
	})
	auth := flowExportAuth()
	auth.TenantID, auth.UserID = tenantID, userID
	for index := range auth.Grants {
		auth.Grants[index].TenantID = tenantID
		auth.Grants[index].SubjectID = userID
		auth.Grants[index].ResourceID = tenantID
	}
	gateway := newFlowExportGatewayFixtureForTenant(t, tenantID, &queryProviderStub{})
	start := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	query := flowExportQuery(start)
	task, err := prepareFlowExportExecutionTask(ctx, gateway, auth, query, ExportFormatParquet, 0)
	if err != nil {
		t.Fatal(err)
	}
	task.ID = "export_flow_mysql"
	created, err := store.CreateExportTask(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	if created.OperationJobID == "" || created.DatasetKey != FlowTrafficDataset || created.Step != 0 || created.TargetID != "" {
		t.Fatalf("created = %+v", created)
	}
	if err := validateExportExecutionTask(created); err != nil {
		t.Fatalf("stored task must retain its canonical semantic hash: %v", err)
	}
	job, err := store.GetOperationJob(ctx, tenantID, created.OperationJobID)
	if err != nil || job.JobType != ExportExecutionJobType || job.Status != OperationJobStatusQueued {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}
