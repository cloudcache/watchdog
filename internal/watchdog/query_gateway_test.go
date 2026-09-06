package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type queryPolicyMemoryRepository struct {
	mu       sync.Mutex
	policies map[string]QueryDatasetPolicy
	err      error
}

func (r *queryPolicyMemoryRepository) key(tenantID ID, datasetKey string) string {
	return string(tenantID) + "\x00" + datasetKey
}

func (r *queryPolicyMemoryRepository) GetQueryDatasetPolicy(_ context.Context, tenantID ID, datasetKey string) (QueryDatasetPolicy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return QueryDatasetPolicy{}, r.err
	}
	policy, ok := r.policies[r.key(tenantID, datasetKey)]
	if !ok {
		return QueryDatasetPolicy{}, sql.ErrNoRows
	}
	return policy, nil
}

func (r *queryPolicyMemoryRepository) ListQueryDatasetPolicies(_ context.Context, tenantID ID) ([]QueryDatasetPolicy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	items := make([]QueryDatasetPolicy, 0)
	for _, policy := range r.policies {
		if policy.TenantID == tenantID {
			items = append(items, policy)
		}
	}
	return items, nil
}

func (r *queryPolicyMemoryRepository) PutQueryDatasetPolicy(_ context.Context, policy QueryDatasetPolicy, expectedVersion uint64) (QueryDatasetPolicy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := r.key(policy.TenantID, policy.DatasetKey)
	current, exists := r.policies[key]
	if (!exists && expectedVersion != 0) || (exists && current.RowVersion != expectedVersion) {
		return QueryDatasetPolicy{}, ErrQueryDatasetPolicyConflict
	}
	policy.RowVersion = expectedVersion + 1
	r.policies[key] = policy
	return policy, nil
}

func (r *queryPolicyMemoryRepository) DeleteQueryDatasetPolicy(_ context.Context, tenantID ID, datasetKey string, expectedVersion uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := r.key(tenantID, datasetKey)
	current, exists := r.policies[key]
	if !exists {
		return sql.ErrNoRows
	}
	if current.RowVersion != expectedVersion {
		return ErrQueryDatasetPolicyConflict
	}
	delete(r.policies, key)
	return nil
}

type queryModuleStates struct {
	states map[string]bool
	err    error
}

func (r queryModuleStates) ListTenantModuleStates(context.Context, ID) (map[string]bool, error) {
	return r.states, r.err
}

func (queryModuleStates) SetTenantModuleEnabled(context.Context, ID, string, bool, ID) error {
	return nil
}

type queryProviderStub struct {
	query  func(context.Context, QueryProviderRequest) (QueryProviderResult, error)
	ready  error
	closed atomic.Bool
}

func (p *queryProviderStub) Query(ctx context.Context, request QueryProviderRequest) (QueryProviderResult, error) {
	if p.query != nil {
		return p.query(ctx, request)
	}
	return QueryProviderResult{Data: json.RawMessage(`[]`)}, nil
}

func (p *queryProviderStub) Ready(context.Context) error { return p.ready }
func (p *queryProviderStub) Close() error {
	p.closed.Store(true)
	return nil
}

func newQueryGatewayFixture(t *testing.T, policy *queryPolicyMemoryRepository, moduleStates TenantModuleRepository, provider *queryProviderStub, providerMax uint32) (*QueryGateway, AuthContext) {
	t.Helper()
	registries := NewPlatformRegistries()
	module := baseModule{descriptor: ModuleDescriptor{Key: "query-test", Version: "1", DefaultEnabled: true}}
	if err := registries.Modules.Register(module); err != nil {
		t.Fatal(err)
	}
	if err := registries.Datasets.Register(DatasetDescriptor{
		Key: "query.test", ModuleKey: "query-test", Provider: DatasetProviderClickHouse,
		TimeField: "event_time", ValueLayers: []QueryValueLayer{QueryValueRaw, QueryValueSupplier, QueryValueCustomer},
		MaxRangeDays: 7, MaxResultRows: 5_000,
	}); err != nil {
		t.Fatal(err)
	}
	providers := NewQueryProviderRegistry()
	if err := providers.Register(QueryProviderRegistration{
		Kind: DatasetProviderClickHouse, Provider: provider, Enabled: true, MaxConcurrent: providerMax,
	}); err != nil {
		t.Fatal(err)
	}
	gateway, err := NewQueryGateway(registries, moduleStates, policy, providers)
	if err != nil {
		t.Fatal(err)
	}
	auth := AuthContext{
		TenantID: "tenant-a", UserID: "user-a",
		Grants: []Permission{{TenantID: "tenant-a", SubjectType: SubjectUser, SubjectID: "user-a", ResourceType: ResourceTenant, ResourceID: "tenant-a", Actions: []Action{ActionViewCustomer}}},
	}
	return gateway, auth
}

func validQueryRequest() QueryRequest {
	to := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	return QueryRequest{
		Dataset: "query.test", From: to.Add(-time.Hour), To: to,
		ValueLayer: QueryValueCustomer, Parameters: json.RawMessage(`{"b":2,"a":1}`),
	}
}

func TestQueryGatewayInjectsTenantAndReturnsCanonicalMetadata(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	var received QueryProviderRequest
	provider := &queryProviderStub{query: func(_ context.Context, request QueryProviderRequest) (QueryProviderResult, error) {
		received = request
		return QueryProviderResult{
			Data: json.RawMessage(`[{"value":42}]`), Unit: "bytes", Timezone: "UTC", StepSeconds: 300,
			Completeness: QueryCompleteness{CompleteRatio: 1}, Versions: map[string]string{"dimension": "7"},
		}, nil
	}}
	gateway, auth := newQueryGatewayFixture(t, repo, nil, provider, 2)
	fixedNow := time.Date(2026, 8, 24, 12, 1, 0, 0, time.UTC)
	gateway.Now = func() time.Time { return fixedNow }

	result, err := gateway.Execute(context.Background(), auth, "request-a", validQueryRequest())
	if err != nil {
		t.Fatal(err)
	}
	if received.TenantID != auth.TenantID || received.UserID != auth.UserID || string(received.Parameters) != `{"a":1,"b":2}` {
		t.Fatalf("provider request = %#v", received)
	}
	if received.Limit != defaultQueryLimit || result.Meta.RequestID != "request-a" || result.Meta.Source != "clickhouse" ||
		result.Meta.PolicyVersion != 0 || result.Meta.AsOf != fixedNow || result.Meta.StepSeconds != 300 || len(result.Meta.QueryHash) != 64 {
		t.Fatalf("result meta = %#v", result.Meta)
	}

	spaced := validQueryRequest()
	spaced.Parameters = json.RawMessage("{ \"a\": 1, \"b\": 2 }")
	second, err := gateway.Execute(context.Background(), auth, "request-b", spaced)
	if err != nil {
		t.Fatal(err)
	}
	if result.Meta.QueryHash != second.Meta.QueryHash {
		t.Fatalf("canonical hashes differ: %s != %s", result.Meta.QueryHash, second.Meta.QueryHash)
	}
}

func TestQueryGatewayCompatibilityUsesTenantPolicyAsLegacyRowBound(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	var received QueryProviderRequest
	provider := &queryProviderStub{query: func(_ context.Context, request QueryProviderRequest) (QueryProviderResult, error) {
		received = request
		return QueryProviderResult{Data: json.RawMessage(`[]`)}, nil
	}}
	gateway, auth := newQueryGatewayFixture(t, repo, nil, provider, 2)
	repo.policies[repo.key(auth.TenantID, "query.test")] = QueryDatasetPolicy{
		TenantID: auth.TenantID, DatasetKey: "query.test", Enabled: true, AllowCustomer: true,
		MaxRangeSeconds: 86_400, MaxConcurrent: 1, MaxResultRows: 1_700, QueryTimeoutMS: 1_000,
	}
	if _, err := gateway.executeCompatibility(context.Background(), auth, "compatibility-a", validQueryRequest()); err != nil {
		t.Fatal(err)
	}
	if received.Limit != 1_700 {
		t.Fatalf("compatibility limit = %d, want tenant policy maximum 1700", received.Limit)
	}
	if _, err := gateway.Execute(context.Background(), auth, "query-a", validQueryRequest()); err != nil {
		t.Fatal(err)
	}
	if received.Limit != defaultQueryLimit {
		t.Fatalf("public query limit = %d, want default %d", received.Limit, defaultQueryLimit)
	}
}

func TestQueryGatewayValueLayerNeedsPolicyAndRBAC(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	gateway, auth := newQueryGatewayFixture(t, repo, nil, &queryProviderStub{}, 2)
	request := validQueryRequest()
	request.ValueLayer = QueryValueRaw
	assertGatewayQueryError(t, gateway, auth, request, QueryErrorDatasetDisabled)

	repo.policies[repo.key(auth.TenantID, request.Dataset)] = QueryDatasetPolicy{
		TenantID: auth.TenantID, DatasetKey: request.Dataset, Enabled: true,
		AllowRaw: true, AllowCustomer: true, MaxRangeSeconds: 86_400, MaxConcurrent: 1,
		MaxResultRows: 100, QueryTimeoutMS: 1_000, RowVersion: 3,
	}
	assertGatewayQueryError(t, gateway, auth, request, QueryErrorPermissionDenied)
	auth.Grants[0].Actions = append(auth.Grants[0].Actions, ActionViewRaw)
	if _, err := gateway.Execute(context.Background(), auth, "", request); err != nil {
		t.Fatalf("raw query with explicit policy and grant: %v", err)
	}
}

func TestQueryGatewayModuleRangeAndRowsFailClosed(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	gateway, auth := newQueryGatewayFixture(t, repo, queryModuleStates{states: map[string]bool{"query-test": false}}, &queryProviderStub{}, 2)
	assertGatewayQueryError(t, gateway, auth, validQueryRequest(), QueryErrorModuleDisabled)

	gateway, auth = newQueryGatewayFixture(t, repo, nil, &queryProviderStub{}, 2)
	request := validQueryRequest()
	request.From = request.To.Add(-8 * 24 * time.Hour)
	assertGatewayQueryError(t, gateway, auth, request, QueryErrorRangeLimit)
	request = validQueryRequest()
	request.Limit = 5_001
	assertGatewayQueryError(t, gateway, auth, request, QueryErrorRowLimit)
}

func TestQueryGatewayTenantAndProviderConcurrencyRelease(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	provider := &queryProviderStub{query: func(ctx context.Context, _ QueryProviderRequest) (QueryProviderResult, error) {
		started <- struct{}{}
		select {
		case <-release:
			return QueryProviderResult{Data: json.RawMessage(`[]`)}, nil
		case <-ctx.Done():
			return QueryProviderResult{}, ctx.Err()
		}
	}}
	gateway, auth := newQueryGatewayFixture(t, repo, nil, provider, 1)
	repo.policies[repo.key(auth.TenantID, "query.test")] = QueryDatasetPolicy{
		TenantID: auth.TenantID, DatasetKey: "query.test", Enabled: true, AllowCustomer: true,
		MaxRangeSeconds: 86_400, MaxConcurrent: 2, MaxResultRows: 5_000, QueryTimeoutMS: 5_000, RowVersion: 1,
	}
	done := make(chan error, 1)
	go func() {
		_, err := gateway.Execute(context.Background(), auth, "", validQueryRequest())
		done <- err
	}()
	<-started
	assertGatewayQueryError(t, gateway, auth, validQueryRequest(), QueryErrorConcurrencyLimit)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.Execute(context.Background(), auth, "", validQueryRequest()); err != nil {
		t.Fatalf("concurrency slot leaked: %v", err)
	}
}

func TestQueryGatewayTenantConcurrencyIsIndependent(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	provider := &queryProviderStub{query: func(ctx context.Context, _ QueryProviderRequest) (QueryProviderResult, error) {
		started <- struct{}{}
		select {
		case <-release:
			return QueryProviderResult{Data: json.RawMessage(`[]`)}, nil
		case <-ctx.Done():
			return QueryProviderResult{}, ctx.Err()
		}
	}}
	gateway, auth := newQueryGatewayFixture(t, repo, nil, provider, 2)
	repo.policies[repo.key(auth.TenantID, "query.test")] = QueryDatasetPolicy{
		TenantID: auth.TenantID, DatasetKey: "query.test", Enabled: true, AllowCustomer: true,
		MaxRangeSeconds: 86_400, MaxConcurrent: 1, MaxResultRows: 5_000, QueryTimeoutMS: 5_000, RowVersion: 1,
	}
	done := make(chan error, 1)
	go func() {
		_, err := gateway.Execute(context.Background(), auth, "", validQueryRequest())
		done <- err
	}()
	<-started
	assertGatewayQueryError(t, gateway, auth, validQueryRequest(), QueryErrorConcurrencyLimit)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestQueryGatewayTimeoutAndCompleteness(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	provider := &queryProviderStub{query: func(ctx context.Context, _ QueryProviderRequest) (QueryProviderResult, error) {
		<-ctx.Done()
		return QueryProviderResult{}, ctx.Err()
	}}
	gateway, auth := newQueryGatewayFixture(t, repo, nil, provider, 1)
	repo.policies[repo.key(auth.TenantID, "query.test")] = QueryDatasetPolicy{
		TenantID: auth.TenantID, DatasetKey: "query.test", Enabled: true, AllowCustomer: true,
		MaxRangeSeconds: 86_400, MaxConcurrent: 1, MaxResultRows: 100, QueryTimeoutMS: 100, RowVersion: 1,
	}
	assertGatewayQueryError(t, gateway, auth, validQueryRequest(), QueryErrorTimeout)

	provider.query = func(context.Context, QueryProviderRequest) (QueryProviderResult, error) {
		return QueryProviderResult{Data: json.RawMessage(`[]`), Completeness: QueryCompleteness{Partial: true, CompleteRatio: .8}}, nil
	}
	request := validQueryRequest()
	request.RequireComplete = true
	assertGatewayQueryError(t, gateway, auth, request, QueryErrorIncomplete)

	provider.query = func(context.Context, QueryProviderRequest) (QueryProviderResult, error) {
		return QueryProviderResult{Data: json.RawMessage(`[]`), Completeness: QueryCompleteness{UnknownRatio: 1}}, nil
	}
	assertGatewayQueryError(t, gateway, auth, request, QueryErrorIncomplete)

	provider.query = func(context.Context, QueryProviderRequest) (QueryProviderResult, error) {
		return QueryProviderResult{Data: json.RawMessage(`[]`), Completeness: QueryCompleteness{CompleteRatio: 1}}, nil
	}
	if _, err := gateway.Execute(context.Background(), auth, "", request); err != nil {
		t.Fatalf("complete query: %v", err)
	}
}

func TestQueryGatewayPropagatesCallerCancellation(t *testing.T) {
	repo := &queryPolicyMemoryRepository{policies: map[string]QueryDatasetPolicy{}}
	provider := &queryProviderStub{query: func(ctx context.Context, _ QueryProviderRequest) (QueryProviderResult, error) {
		<-ctx.Done()
		return QueryProviderResult{}, ctx.Err()
	}}
	gateway, auth := newQueryGatewayFixture(t, repo, nil, provider, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := gateway.Execute(ctx, auth, "", validQueryRequest())
	var queryErr *QueryGatewayError
	if !errors.As(err, &queryErr) || queryErr.Code != QueryErrorCanceled {
		t.Fatalf("cancellation error = %#v", err)
	}
}

func TestQueryProviderRegistryLifecycle(t *testing.T) {
	provider := &queryProviderStub{ready: errors.New("not ready")}
	registry := NewQueryProviderRegistry()
	registration := QueryProviderRegistration{Kind: DatasetProviderVM, Provider: provider, Enabled: true, MaxConcurrent: 1}
	if err := registry.Register(registration); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(registration); err == nil {
		t.Fatal("duplicate provider registration succeeded")
	}
	if err := registry.Ready(context.Background()); !errors.Is(err, provider.ready) {
		t.Fatalf("ready error = %v", err)
	}
	if err := registry.Close(); err != nil || !provider.closed.Load() {
		t.Fatalf("close error=%v closed=%v", err, provider.closed.Load())
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("second close = %v", err)
	}
	if _, _, err := registry.acquire(DatasetProviderVM); !errors.Is(err, ErrQueryProviderUnavailable) {
		t.Fatalf("acquire after close = %v", err)
	}
	var _ io.Closer = provider
}

func assertGatewayQueryError(t *testing.T, gateway *QueryGateway, auth AuthContext, request QueryRequest, code QueryErrorCode) {
	t.Helper()
	_, err := gateway.Execute(context.Background(), auth, "", request)
	var queryErr *QueryGatewayError
	if !errors.As(err, &queryErr) || queryErr.Code != code {
		t.Fatalf("error = %#v, want code %s", err, code)
	}
}
