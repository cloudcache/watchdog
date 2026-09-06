package watchdog

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func newExportGatewayFixture(t *testing.T, policy QueryDatasetPolicy, provider *queryProviderStub) *QueryGateway {
	t.Helper()
	registries, err := NewBuiltinPlatformRegistries()
	if err != nil {
		t.Fatal(err)
	}
	policies := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	policies.policies[policies.key(policy.TenantID, policy.DatasetKey)] = policy
	providers := NewQueryProviderRegistry()
	if err := providers.Register(QueryProviderRegistration{
		Kind: DatasetProviderVM, Provider: provider, Enabled: true, MaxConcurrent: 2,
	}); err != nil {
		t.Fatal(err)
	}
	gateway, err := NewQueryGateway(registries, nil, policies, providers)
	if err != nil {
		t.Fatal(err)
	}
	return gateway
}

func exportGatewayPolicy(layer QueryValueLayer) QueryDatasetPolicy {
	policy := QueryDatasetPolicy{
		TenantID: "tenant-a", DatasetKey: defaultExportDatasetKey, Enabled: true,
		MaxRangeSeconds: 86_400, MaxConcurrent: 2, MaxResultRows: 10_000, QueryTimeoutMS: 5_000,
		RowVersion: 7,
	}
	switch layer {
	case QueryValueRaw:
		policy.AllowRaw = true
	case QueryValueSupplier:
		policy.AllowSupplier = true
	case QueryValueCustomer:
		policy.AllowCustomer = true
	}
	return policy
}

func exportGatewayAuth(layer QueryValueLayer) AuthContext {
	viewAction := ActionViewCustomer
	if layer == QueryValueRaw {
		viewAction = ActionViewRaw
	} else if layer == QueryValueSupplier {
		viewAction = ActionViewSupplier
	}
	return AuthContext{
		TenantID: "tenant-a", UserID: "user-a",
		Grants: []Permission{
			{TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a", ResourceType: ResourceTenant, ResourceID: "tenant-a", Actions: []Action{viewAction}},
			{TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a", ResourceType: ResourceTarget, ResourceID: "target-a", Actions: []Action{exportLayerAction(layer)}},
		},
	}
}

func TestPrepareExportExecutionFreezesCanonicalQueryAndCorrection(t *testing.T) {
	gateway := newExportGatewayFixture(t, exportGatewayPolicy(QueryValueCustomer), &queryProviderStub{})
	network := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a"}},
		policy: PortPolicy{
			ID: "policy-a", TenantID: "tenant-a", PortID: "port-a", SideType: PortSideCustomer,
			CorrectionDirection: CorrectionUp, CorrectionMin: 1, CorrectionMax: 2, Enabled: true,
		},
	}
	start := time.Date(2026, 9, 6, 1, 0, 0, 0, time.UTC)
	task, err := prepareExportExecutionTask(context.Background(), gateway, network, exportGatewayAuth(QueryValueCustomer), MetricSNMPIfInBps, time.Minute, ExportTask{
		ID: "export-a", TenantID: "tenant-a", CreatedBy: "user-a", TargetID: "target-a",
		RangeStart: start, RangeEnd: start.Add(time.Hour), Step: 5 * time.Minute,
		Aggregation: AggregationP95FiveMinute, ValueMode: ExportValueCorrected, ValueLayer: QueryValueCustomer,
		Format: ExportFormatCSV,
	})
	if err != nil {
		t.Fatal(err)
	}
	if task.ContractVersion != ExportExecutionContractVersion || len(task.QueryHash) != 64 || task.RetentionSeconds != defaultExportRetentionSeconds {
		t.Fatalf("task = %+v", task)
	}
	var snapshot exportQuerySnapshot
	if err := decodeStrictJSON(task.QueryJSON, &snapshot); err != nil {
		t.Fatal(err)
	}
	var parameters victoriaMetricsQueryParameters
	if err := decodeStrictJSON(snapshot.Query.Parameters, &parameters); err != nil {
		t.Fatal(err)
	}
	if parameters.DeviceID != "device-a" || snapshot.Query.StepSeconds != 60 || snapshot.Query.RequireComplete {
		t.Fatalf("snapshot = %+v, parameters = %+v", snapshot, parameters)
	}
	if err := validateExportExecutionTask(task); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if err := verifyExportCorrectionSnapshot(context.Background(), network, task); err != nil {
		t.Fatalf("verify frozen correction: %v", err)
	}
	network.policy.CorrectionMax = 3
	if err := verifyExportCorrectionSnapshot(context.Background(), network, task); err == nil {
		t.Fatal("expected changed correction policy to be rejected")
	}
}

func TestQueryGatewayExportProviderSumsTargetSeriesBeforeAggregation(t *testing.T) {
	start := time.Date(2026, 9, 6, 1, 0, 0, 0, time.UTC)
	response := VictoriaMetricsResponse{Status: "success"}
	response.Data.Result = []VMRangeQueryItem{
		{Metric: map[string]string{"port_id": "port-a"}, Values: []VMValue{{Time: start, Value: 10}, {Time: start.Add(time.Minute), Value: 20}}},
		{Metric: map[string]string{"port_id": "port-b"}, Values: []VMValue{{Time: start, Value: 1}, {Time: start.Add(time.Minute), Value: 2}}},
	}
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	gateway := newExportGatewayFixture(t, exportGatewayPolicy(QueryValueRaw), &queryProviderStub{query: func(context.Context, QueryProviderRequest) (QueryProviderResult, error) {
		return QueryProviderResult{Data: data, Completeness: QueryCompleteness{UnknownRatio: 1}}, nil
	}})
	auth := exportGatewayAuth(QueryValueRaw)
	task, err := prepareExportExecutionTask(context.Background(), gateway, nil, auth, MetricSNMPIfInBps, time.Minute, ExportTask{
		ID: "export-a", TenantID: "tenant-a", CreatedBy: "user-a", TargetID: "target-a",
		RangeStart: start, RangeEnd: start.Add(time.Hour), Step: 5 * time.Minute,
		Aggregation: AggregationAverageFiveMinute, ValueMode: ExportValueRaw, ValueLayer: QueryValueRaw, Format: ExportFormatCSV,
	})
	if err != nil {
		t.Fatal(err)
	}
	samples, err := (QueryGatewayExportDataProvider{Gateway: gateway}).LoadSamples(ContextWithAuth(context.Background(), auth), task)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 2 || samples[0].Value != 11 || samples[1].Value != 22 {
		t.Fatalf("samples = %+v", samples)
	}
}

type exportAuthorizationStub struct {
	tenant Tenant
	user   User
	roles  []ID
	grants []Permission
	admin  bool
}

func (s *exportAuthorizationStub) GetTenant(context.Context, ID) (Tenant, error) {
	return s.tenant, nil
}
func (s *exportAuthorizationStub) GetTenantUser(context.Context, ID, ID) (User, error) {
	return s.user, nil
}
func (s *exportAuthorizationStub) ListUserRoleIDs(context.Context, ID, ID) ([]ID, error) {
	return s.roles, nil
}
func (s *exportAuthorizationStub) ListPermissionsForUser(context.Context, ID, ID) ([]Permission, error) {
	return s.grants, nil
}
func (s *exportAuthorizationStub) IsUserTenantAdmin(context.Context, ID, ID) (bool, error) {
	return s.admin, nil
}

type executionExportRepository struct{ task ExportTask }

func (r *executionExportRepository) CreateExportTask(context.Context, ExportTask) (ExportTask, error) {
	return ExportTask{}, nil
}
func (r *executionExportRepository) GetExportTask(context.Context, ID, ID) (ExportTask, error) {
	return r.task, nil
}
func (r *executionExportRepository) ListExportTasks(context.Context, ID, ID) ([]ExportTask, error) {
	return []ExportTask{r.task}, nil
}
func (r *executionExportRepository) RetryExportTask(context.Context, ID, ID) error { return nil }
func (r *executionExportRepository) MarkExportRunning(context.Context, ID, ID) error {
	r.task.Status = ExportStatusRunning
	return nil
}
func (r *executionExportRepository) MarkExportComplete(_ context.Context, _ ID, _ ID, artifact ExportArtifact, expiresAt time.Time) error {
	r.task.Status, r.task.FileRef, r.task.ExpiresAt = ExportStatusComplete, artifact.FileRef, expiresAt
	return nil
}
func (r *executionExportRepository) MarkExportFailed(_ context.Context, _ ID, _ ID, message string) error {
	r.task.Status, r.task.ErrorMessage = ExportStatusFailed, message
	return nil
}

func TestExportExecutionHandlerRechecksPermissionAndSnapshot(t *testing.T) {
	gateway := newExportGatewayFixture(t, exportGatewayPolicy(QueryValueRaw), &queryProviderStub{})
	auth := exportGatewayAuth(QueryValueRaw)
	start := time.Date(2026, 9, 6, 1, 0, 0, 0, time.UTC)
	task, err := prepareExportExecutionTask(context.Background(), gateway, nil, auth, MetricSNMPIfInBps, time.Minute, ExportTask{
		ID: "export-a", TenantID: "tenant-a", CreatedBy: "user-a", TargetID: "target-a",
		RangeStart: start, RangeEnd: start.Add(5 * time.Minute), Step: 5 * time.Minute,
		Aggregation: AggregationAverageFiveMinute, ValueMode: ExportValueRaw, ValueLayer: QueryValueRaw, Format: ExportFormatCSV,
	})
	if err != nil {
		t.Fatal(err)
	}
	task.OperationJobID = "job-a"
	payload, _, err := encodeExportExecutionPayload(task)
	if err != nil {
		t.Fatal(err)
	}
	repo := &executionExportRepository{task: task}
	authorization := &exportAuthorizationStub{
		tenant: Tenant{ID: "tenant-a", Status: "active"}, user: User{ID: "user-a", TenantID: "tenant-a", Status: "active"},
		grants: auth.Grants,
	}
	worker := ExportWorker{
		Repo: repo, Data: fakeExportDataProvider{samples: []Sample{
			{Time: start, Value: 1},
			{Time: start.Add(time.Minute), Value: 2},
			{Time: start.Add(2 * time.Minute), Value: 3},
			{Time: start.Add(3 * time.Minute), Value: 4},
			{Time: start.Add(4 * time.Minute), Value: 5},
		}},
		Writer: &CSVExportWriter{},
	}
	handler := NewExportExecutionJobHandler(repo, worker, ExportExecutionJobDependencies{Authorization: authorization})
	result, err := handler(context.Background(), OperationJob{ID: "job-a", TenantID: "tenant-a", CheckpointJSON: payload})
	if err != nil || result != "exports/export-a.csv" || repo.task.Status != ExportStatusComplete {
		t.Fatalf("result=%q task=%+v err=%v", result, repo.task, err)
	}

	repo.task = task
	authorization.grants = nil
	_, err = handler(context.Background(), OperationJob{ID: "job-a", TenantID: "tenant-a", CheckpointJSON: payload})
	if err == nil || !IsTerminalJobError(err) {
		t.Fatalf("revoked permission error = %v", err)
	}

	repo.task = task
	repo.task.VersionsJSON = json.RawMessage(`{"schema_version":1}`)
	authorization.grants = auth.Grants
	_, err = handler(context.Background(), OperationJob{ID: "job-a", TenantID: "tenant-a", CheckpointJSON: payload})
	if err == nil || !IsTerminalJobError(err) {
		t.Fatalf("tampered snapshot error = %v", err)
	}
}

func TestValidateExportExecutionRejectsMismatchedProjection(t *testing.T) {
	gateway := newExportGatewayFixture(t, exportGatewayPolicy(QueryValueRaw), &queryProviderStub{})
	auth := exportGatewayAuth(QueryValueRaw)
	start := time.Date(2026, 9, 6, 1, 0, 0, 0, time.UTC)
	task, err := prepareExportExecutionTask(context.Background(), gateway, nil, auth, MetricSNMPIfInBps, time.Minute, ExportTask{
		ID: "export-a", TenantID: "tenant-a", CreatedBy: "user-a", TargetID: "target-a",
		RangeStart: start, RangeEnd: start.Add(time.Hour), Step: 5 * time.Minute,
		Aggregation: AggregationP95FiveMinute, ValueMode: ExportValueRaw, ValueLayer: QueryValueRaw, Format: ExportFormatCSV,
	})
	if err != nil {
		t.Fatal(err)
	}
	task.TargetID = "target-b"
	if err := validateExportExecutionTask(task); err == nil {
		t.Fatal("expected task/query resource mismatch")
	}
}

func TestExportCompletenessPolicyUsesFrozenSnapshot(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	versions, _, err := canonicalJSONHash(exportVersionSnapshot{
		SchemaVersion: 1, CompletenessStepSeconds: 300,
		CompletenessMissingRatio: "0.25", SnapshotComplete: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := exportCompletenessPolicyFromSnapshot(ExportTask{
		ContractVersion: ExportExecutionContractVersion,
		RangeStart:      start, RangeEnd: start.Add(time.Hour), VersionsJSON: versions,
	})
	if err != nil {
		t.Fatalf("exportCompletenessPolicyFromSnapshot() error = %v", err)
	}
	if policy.CollectionStep != 5*time.Minute || policy.MaxMissingRatio != 0.25 || !policy.Start.Equal(start) {
		t.Fatalf("policy = %+v", policy)
	}
}
