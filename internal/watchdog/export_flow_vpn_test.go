package watchdog

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type vpnFindingExportRepositoryStub struct {
	filter VPNFindingListFilter
	limit  uint32
	items  []VPNFinding
}

func (r *vpnFindingExportRepositoryStub) ListVPNFindingsForExport(_ context.Context, tenantID ID, filter VPNFindingListFilter, limit uint32) ([]VPNFinding, error) {
	if tenantID != "tenant-vpn" {
		return nil, errorsForTest("unexpected tenant")
	}
	r.filter, r.limit = filter, limit
	return r.items, nil
}

type errorsForTest string

func (err errorsForTest) Error() string { return string(err) }

type exportDataProviderStub struct {
	called bool
}

func (provider *exportDataProviderStub) LoadSamples(context.Context, ExportTask) ([]Sample, error) {
	provider.called = true
	return []Sample{{Value: 1}}, nil
}

func TestVPNFindingExportProviderUsesFrozenQueryAndRedactedSchema(t *testing.T) {
	auth := AuthContext{TenantID: "tenant-vpn", UserID: "user-vpn", IsAdmin: true}
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	task, err := prepareVPNFindingExportTask(auth, flowVPNFindingExportCreateRequest{
		From: start, To: start.Add(24 * time.Hour), Search: " 192.0.2 ",
		ColumnFilters: map[string][]string{"risk_level": {"high"}}, SortBy: "score", SortDirection: "asc",
		Limit: 2, Format: ExportFormatCSV,
	})
	if err != nil {
		t.Fatal(err)
	}
	task.ID = "export-vpn-a"
	repo := &vpnFindingExportRepositoryStub{items: []VPNFinding{{
		ID: "finding-a", TenantID: auth.TenantID, WindowStart: start, WindowEnd: start.Add(time.Minute),
		ConversationKey: "secret-key", LocalIP: "=formula", RemoteIP: "198.51.100.8",
		Evidence: json.RawMessage(`{"tls":"secret"}`), ProbeResult: json.RawMessage(`{"server":"secret"}`),
		DispositionNote: "secret-note", DispositionBy: "secret-actor", RiskLevel: "high", Score: 80,
		GeneratedAt: start.Add(2 * time.Minute), ExpiresAt: start.Add(24 * time.Hour), RowVersion: 4,
	}}}
	provider := QueryGatewayExportDataProvider{VPNFindings: repo}
	rows, handled, err := provider.LoadExportRows(ContextWithAuth(context.Background(), auth), task)
	if err != nil || !handled || len(rows.VPNFindings) != 1 {
		t.Fatalf("handled=%v rows=%+v err=%v", handled, rows, err)
	}
	if repo.limit != 2 || repo.filter.Search != "192.0.2" || repo.filter.SortBy != "score" || repo.filter.SortDirection != "ASC" ||
		!repo.filter.From.Equal(start) || !repo.filter.To.Equal(start.Add(24*time.Hour)) {
		t.Fatalf("filter=%+v limit=%d", repo.filter, repo.limit)
	}
	data, err := RenderVPNFindingCSV(rows.VPNFindings)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	header := strings.Join(records[0], ",")
	for _, forbidden := range []string{"tenant_id", "conversation_key", "evidence,", "probe_result,", "disposition_note", "disposition_by"} {
		if strings.Contains(header, forbidden) {
			t.Fatalf("header leaks %q: %s", forbidden, header)
		}
	}
	if records[1][3] != "'=formula" || records[1][24] != "true" || records[1][33] != "true" {
		t.Fatalf("record=%+v", records[1])
	}
	parquetData, err := RenderVPNFindingParquet(rows.VPNFindings)
	if err != nil || !bytes.HasPrefix(parquetData, []byte("PAR1")) || !bytes.HasSuffix(parquetData, []byte("PAR1")) {
		t.Fatalf("invalid parquet bytes=%d err=%v", len(parquetData), err)
	}
}

func TestVPNFindingExportRejectsTruncatedFrozenResult(t *testing.T) {
	auth := AuthContext{TenantID: "tenant-vpn", UserID: "user-vpn", IsAdmin: true}
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	task, err := prepareVPNFindingExportTask(auth, flowVPNFindingExportCreateRequest{
		From: start, To: start.Add(time.Hour), Limit: 1, Format: ExportFormatCSV,
	})
	if err != nil {
		t.Fatal(err)
	}
	repo := &vpnFindingExportRepositoryStub{items: []VPNFinding{{ID: "a"}, {ID: "b"}}}
	_, handled, err := (QueryGatewayExportDataProvider{VPNFindings: repo}).LoadExportRows(ContextWithAuth(context.Background(), auth), task)
	if !handled || err == nil || !strings.Contains(err.Error(), "exceeds its frozen row limit") {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
}

func TestVPNFindingExportUsesExistingOperationJobLifecycle(t *testing.T) {
	authAdapter := flowVPNTestAuth(ActionVPNView, ActionVPNExport)
	auth, _ := authAdapter(nil)
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	task, err := prepareVPNFindingExportTask(auth, flowVPNFindingExportCreateRequest{
		From: start, To: start.Add(time.Hour), Limit: 10, Format: ExportFormatCSV,
	})
	if err != nil {
		t.Fatal(err)
	}
	task.ID, task.OperationJobID = "export-vpn-a", "job-vpn-a"
	payload, _, err := encodeExportExecutionPayload(task)
	if err != nil {
		t.Fatal(err)
	}
	exports := &executionExportRepository{task: task}
	findings := &vpnFindingExportRepositoryStub{items: []VPNFinding{{
		ID: "finding-a", WindowStart: start, WindowEnd: start.Add(time.Minute),
		GeneratedAt: start.Add(2 * time.Minute), ExpiresAt: start.Add(24 * time.Hour),
	}}}
	authorization := &exportAuthorizationStub{
		tenant: Tenant{ID: auth.TenantID, Status: "active"},
		user:   User{ID: auth.UserID, TenantID: auth.TenantID, Status: "active"},
		grants: auth.Grants,
	}
	writer := &CSVExportWriter{}
	handler := NewExportExecutionJobHandler(exports, ExportWorker{
		Repo: exports, Data: QueryGatewayExportDataProvider{VPNFindings: findings}, Writer: writer,
	}, ExportExecutionJobDependencies{Authorization: authorization})
	fileRef, err := handler(context.Background(), OperationJob{ID: task.OperationJobID, TenantID: auth.TenantID, CheckpointJSON: payload})
	if err != nil || fileRef != "exports/export-vpn-a.csv" || exports.task.Status != ExportStatusComplete || len(writer.Files[fileRef]) == 0 {
		t.Fatalf("file=%q task=%+v err=%v", fileRef, exports.task, err)
	}
}

func TestVPNFindingExportPermissionIsRechecked(t *testing.T) {
	task := ExportTask{DatasetKey: FlowVPNFindingsDataset, TenantID: "tenant-vpn", CreatedBy: "user-vpn"}
	allowed := flowVPNTestAuth(ActionVPNExport)
	auth, _ := allowed(nil)
	if !canExecuteExportTask(auth, task) || exportRequiredAction(task) != ActionVPNExport {
		t.Fatal("vpn_export grant should authorize the task")
	}
	denied := flowVPNTestAuth(ActionVPNView)
	auth, _ = denied(nil)
	if canExecuteExportTask(auth, task) {
		t.Fatal("vpn_view must not authorize VPN finding export")
	}
}

func TestQueryGatewayExportProviderKeepsLegacyFallback(t *testing.T) {
	fallback := &exportDataProviderStub{}
	samples, err := (QueryGatewayExportDataProvider{Fallback: fallback}).LoadSamples(context.Background(), ExportTask{})
	if err != nil || !fallback.called || len(samples) != 1 {
		t.Fatalf("called=%v samples=%+v err=%v", fallback.called, samples, err)
	}
}
