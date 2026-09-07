package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// QueryValueLayer selects which immutable correction layer a provider reads.
// Dataset policy and RBAC are separate gates: enabling a layer here never
// grants a principal access to it.
type QueryValueLayer string

const (
	QueryValueRaw      QueryValueLayer = "raw"
	QueryValueSupplier QueryValueLayer = "supplier"
	QueryValueCustomer QueryValueLayer = "customer"
)

const (
	defaultQueryMaxConcurrent = uint32(4)
	defaultQueryMaxRows       = uint32(250_000)
	defaultQueryTimeout       = 90 * time.Second
	defaultQueryLimit         = uint32(1_000)
	maxQueryParameterBytes    = 1 << 20
)

var (
	ErrQueryDatasetPolicyConflict = errors.New("query dataset policy changed since it was read")
	ErrQueryProviderUnavailable   = errors.New("query provider is unavailable")
)

// QueryDatasetPolicy is the tenant-owned admission policy persisted by
// migration 045. A missing row resolves to safe descriptor-derived defaults.
type QueryDatasetPolicy struct {
	TenantID        ID        `json:"tenant_id"`
	DatasetKey      string    `json:"dataset_key"`
	Enabled         bool      `json:"enabled"`
	AllowRaw        bool      `json:"allow_raw"`
	AllowSupplier   bool      `json:"allow_supplier"`
	AllowCustomer   bool      `json:"allow_customer"`
	MaxRangeSeconds uint32    `json:"max_range_seconds"`
	MaxConcurrent   uint32    `json:"max_concurrent"`
	MaxResultRows   uint32    `json:"max_result_rows"`
	QueryTimeoutMS  uint32    `json:"query_timeout_ms"`
	RowVersion      uint64    `json:"row_version"`
	UpdatedBy       ID        `json:"updated_by,omitempty"`
	CreatedAt       time.Time `json:"created_at,omitempty"`
	UpdatedAt       time.Time `json:"updated_at,omitempty"`
}

type QueryDatasetPolicyRepository interface {
	GetQueryDatasetPolicy(ctx context.Context, tenantID ID, datasetKey string) (QueryDatasetPolicy, error)
	ListQueryDatasetPolicies(ctx context.Context, tenantID ID) ([]QueryDatasetPolicy, error)
	PutQueryDatasetPolicy(ctx context.Context, policy QueryDatasetPolicy, expectedVersion uint64) (QueryDatasetPolicy, error)
	DeleteQueryDatasetPolicy(ctx context.Context, tenantID ID, datasetKey string, expectedVersion uint64) error
}

func defaultQueryDatasetPolicy(tenantID ID, descriptor DatasetDescriptor) QueryDatasetPolicy {
	maxRange := 400 * 24 * time.Hour
	if descriptor.MaxRangeDays > 0 {
		maxRange = time.Duration(descriptor.MaxRangeDays) * 24 * time.Hour
	}
	maxRows := descriptor.MaxResultRows
	if maxRows == 0 {
		maxRows = defaultQueryMaxRows
	}
	policy := QueryDatasetPolicy{
		TenantID: tenantID, DatasetKey: descriptor.Key, Enabled: true,
		MaxRangeSeconds: uint32(maxRange / time.Second), MaxConcurrent: defaultQueryMaxConcurrent,
		MaxResultRows: maxRows, QueryTimeoutMS: uint32(defaultQueryTimeout / time.Millisecond),
	}
	if len(descriptor.ValueLayers) == 0 || datasetSupportsQueryLayer(descriptor, QueryValueCustomer) {
		policy.AllowCustomer = true
	}
	return policy
}

func validateQueryDatasetPolicy(policy QueryDatasetPolicy) error {
	policy.DatasetKey = strings.TrimSpace(policy.DatasetKey)
	if policy.TenantID == "" || policy.DatasetKey == "" || len(policy.DatasetKey) > 128 {
		return errors.New("tenant_id and dataset_key are required")
	}
	if policy.MaxRangeSeconds < 60 || policy.MaxRangeSeconds > 315_360_000 {
		return errors.New("max_range_seconds must be between 60 and 315360000")
	}
	if policy.MaxConcurrent < 1 || policy.MaxConcurrent > 1024 {
		return errors.New("max_concurrent must be between 1 and 1024")
	}
	if policy.MaxResultRows < 1 || policy.MaxResultRows > 10_000_000 {
		return errors.New("max_result_rows must be between 1 and 10000000")
	}
	if policy.QueryTimeoutMS < 100 || policy.QueryTimeoutMS > 3_600_000 {
		return errors.New("query_timeout_ms must be between 100 and 3600000")
	}
	return nil
}

func (p QueryDatasetPolicy) allows(layer QueryValueLayer) bool {
	switch layer {
	case QueryValueRaw:
		return p.AllowRaw
	case QueryValueSupplier:
		return p.AllowSupplier
	case QueryValueCustomer:
		return p.AllowCustomer
	default:
		return false
	}
}

// QueryRequest is the provider-neutral transport envelope. Parameters remains
// provider-owned JSON so Flow and VM keep their typed compilers; tenant scope,
// time, limit, value layer and cancellation cannot be hidden inside it.
type QueryRequest struct {
	Dataset         string          `json:"dataset"`
	From            time.Time       `json:"from"`
	To              time.Time       `json:"to"`
	StepSeconds     uint32          `json:"step_seconds,omitempty"`
	Limit           uint32          `json:"limit,omitempty"`
	Cursor          string          `json:"cursor,omitempty"`
	ValueLayer      QueryValueLayer `json:"value_layer"`
	RequireComplete bool            `json:"require_complete,omitempty"`
	Parameters      json.RawMessage `json:"parameters,omitempty"`
}

type QueryProviderRequest struct {
	TenantID        ID
	UserID          ID
	Dataset         DatasetDescriptor
	From            time.Time
	To              time.Time
	StepSeconds     uint32
	Limit           uint32
	Cursor          string
	ValueLayer      QueryValueLayer
	RequireComplete bool
	Parameters      json.RawMessage
}

type QueryCompleteness struct {
	AvailableFrom     *time.Time       `json:"available_from,omitempty"`
	AvailableTo       *time.Time       `json:"available_to,omitempty"`
	CompleteRatio     float64          `json:"complete_ratio"`
	LateRatio         float64          `json:"late_ratio"`
	UnknownRatio      float64          `json:"unknown_ratio"`
	Partial           bool             `json:"partial"`
	DegradedIntervals []map[string]any `json:"degraded_intervals,omitempty"`
	Warnings          []string         `json:"warnings,omitempty"`
}

type QueryProviderResult struct {
	Data     json.RawMessage
	Unit     string
	Timezone string
	// StepSeconds is the effective presentation interval selected by a
	// provider. Zero preserves the requested envelope value.
	StepSeconds  uint32
	AsOf         time.Time
	NextCursor   string
	Versions     map[string]string
	Completeness QueryCompleteness
}

type QueryResultMeta struct {
	RequestID     string            `json:"request_id"`
	SchemaVersion string            `json:"schema_version"`
	AsOf          time.Time         `json:"as_of"`
	Source        string            `json:"source"`
	ValueLayer    QueryValueLayer   `json:"value_layer"`
	Unit          string            `json:"unit,omitempty"`
	Timezone      string            `json:"timezone,omitempty"`
	StepSeconds   uint32            `json:"step_seconds,omitempty"`
	PolicyVersion uint64            `json:"policy_version"`
	Versions      map[string]string `json:"versions,omitempty"`
	NextCursor    string            `json:"next_cursor,omitempty"`
	QueryCompleteness
}

type QueryResult struct {
	Data json.RawMessage `json:"data"`
	Meta QueryResultMeta `json:"meta"`
}

type QueryDatasetProvider interface {
	Query(context.Context, QueryProviderRequest) (QueryProviderResult, error)
	Ready(context.Context) error
}

// QueryDatasetRequestPreparer resolves provider-owned stable references before
// authorization and execution. The returned parameters are the canonical
// request identity used by delayed exports as well as interactive queries.
// Tenant, user, time range and value layer remain immutable gateway fields.
type QueryDatasetRequestPreparer interface {
	PrepareQuery(context.Context, QueryProviderRequest) (json.RawMessage, error)
}

// QueryDatasetAuthorizer is implemented by providers whose typed parameters
// name tenant resources. QueryGateway owns dataset policy and value-layer
// admission; the provider adapter owns the parameter grammar needed to enforce
// the final resource-level view permission without exposing opaque parameters
// to the platform core.
type QueryDatasetAuthorizer interface {
	AuthorizeQuery(context.Context, AuthContext, QueryProviderRequest) error
}

type QueryProviderRegistration struct {
	Kind          DatasetProviderKind
	Provider      QueryDatasetProvider
	Enabled       bool
	MaxConcurrent uint32
}

type queryProviderState struct {
	provider      QueryDatasetProvider
	enabled       bool
	maxConcurrent uint32
	inflight      uint32
}

// QueryProviderRegistry owns provider instances and their independent global
// query budget. A ClickHouse pool may be shared with maintenance work, but the
// registry never lets query concurrency consume an unbounded number of slots.
type QueryProviderRegistry struct {
	mu        sync.Mutex
	providers map[DatasetProviderKind]*queryProviderState
	closed    bool
}

func NewQueryProviderRegistry() *QueryProviderRegistry {
	return &QueryProviderRegistry{providers: map[DatasetProviderKind]*queryProviderState{}}
}

func (r *QueryProviderRegistry) Register(registration QueryProviderRegistration) error {
	if r == nil || registration.Provider == nil {
		return errors.New("query provider is required")
	}
	if registration.Kind != DatasetProviderVM && registration.Kind != DatasetProviderClickHouse {
		return fmt.Errorf("unsupported query provider kind %q", registration.Kind)
	}
	if registration.MaxConcurrent == 0 || registration.MaxConcurrent > 4096 {
		return errors.New("query provider max_concurrent must be between 1 and 4096")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("query provider registry is closed")
	}
	if _, exists := r.providers[registration.Kind]; exists {
		return fmt.Errorf("query provider %q is already registered", registration.Kind)
	}
	r.providers[registration.Kind] = &queryProviderState{
		provider: registration.Provider, enabled: registration.Enabled, maxConcurrent: registration.MaxConcurrent,
	}
	return nil
}

func (r *QueryProviderRegistry) acquire(kind DatasetProviderKind) (QueryDatasetProvider, func(), error) {
	if r == nil {
		return nil, nil, ErrQueryProviderUnavailable
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, nil, ErrQueryProviderUnavailable
	}
	state := r.providers[kind]
	if state == nil || !state.enabled {
		r.mu.Unlock()
		return nil, nil, ErrQueryProviderUnavailable
	}
	if state.inflight >= state.maxConcurrent {
		r.mu.Unlock()
		return nil, nil, &QueryGatewayError{Code: QueryErrorConcurrencyLimit, Message: "query provider concurrency limit reached", Retryable: true}
	}
	state.inflight++
	r.mu.Unlock()
	var once sync.Once
	return state.provider, func() {
		once.Do(func() {
			r.mu.Lock()
			state.inflight--
			r.mu.Unlock()
		})
	}, nil
}

func (r *QueryProviderRegistry) Ready(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrQueryProviderUnavailable
	}
	providers := make([]QueryDatasetProvider, 0, len(r.providers))
	for _, state := range r.providers {
		if state.enabled {
			providers = append(providers, state.provider)
		}
	}
	r.mu.Unlock()
	for _, provider := range providers {
		if err := provider.Ready(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (r *QueryProviderRegistry) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	providers := make([]QueryDatasetProvider, 0, len(r.providers))
	for _, state := range r.providers {
		providers = append(providers, state.provider)
	}
	r.mu.Unlock()
	var joined error
	for _, provider := range providers {
		if closer, ok := provider.(io.Closer); ok {
			joined = errors.Join(joined, closer.Close())
		}
	}
	return joined
}

type QueryErrorCode string

const (
	QueryErrorInvalidRequest      QueryErrorCode = "QUERY_INVALID"
	QueryErrorDatasetNotFound     QueryErrorCode = "QUERY_DATASET_NOT_FOUND"
	QueryErrorModuleDisabled      QueryErrorCode = "QUERY_MODULE_DISABLED"
	QueryErrorDatasetDisabled     QueryErrorCode = "QUERY_DATASET_DISABLED"
	QueryErrorPermissionDenied    QueryErrorCode = "QUERY_PERMISSION_DENIED"
	QueryErrorRangeLimit          QueryErrorCode = "QUERY_RANGE_LIMIT"
	QueryErrorRowLimit            QueryErrorCode = "QUERY_ROW_LIMIT"
	QueryErrorConcurrencyLimit    QueryErrorCode = "QUERY_CONCURRENCY_LIMIT"
	QueryErrorProviderUnavailable QueryErrorCode = "QUERY_PROVIDER_UNAVAILABLE"
	QueryErrorTimeout             QueryErrorCode = "QUERY_TIMEOUT"
	QueryErrorCanceled            QueryErrorCode = "QUERY_CANCELED"
	QueryErrorIncomplete          QueryErrorCode = "QUERY_INCOMPLETE"
	QueryErrorProviderFailure     QueryErrorCode = "QUERY_PROVIDER_FAILURE"
)

type QueryGatewayError struct {
	Code      QueryErrorCode
	Message   string
	Retryable bool
	Details   map[string]any
	Cause     error
}

func (e *QueryGatewayError) Error() string {
	if e == nil {
		return "query failed"
	}
	return e.Message
}

func (e *QueryGatewayError) Unwrap() error { return e.Cause }

type queryInflightState struct{ count uint32 }

type QueryGateway struct {
	Datasets      *DatasetRegistry
	Modules       *ModuleRegistry
	TenantModules TenantModuleRepository
	Policies      QueryDatasetPolicyRepository
	Providers     *QueryProviderRegistry
	Now           func() time.Time

	mu       sync.Mutex
	inflight map[string]*queryInflightState
}

func NewQueryGateway(registries *PlatformRegistries, modules TenantModuleRepository, policies QueryDatasetPolicyRepository, providers *QueryProviderRegistry) (*QueryGateway, error) {
	if registries == nil || registries.Datasets == nil || registries.Modules == nil {
		return nil, errors.New("query gateway requires dataset and module registries")
	}
	if policies == nil || providers == nil {
		return nil, errors.New("query gateway requires policy storage and provider registry")
	}
	return &QueryGateway{
		Datasets: registries.Datasets, Modules: registries.Modules, TenantModules: modules,
		Policies: policies, Providers: providers, Now: time.Now, inflight: map[string]*queryInflightState{},
	}, nil
}

func (g *QueryGateway) Execute(ctx context.Context, auth AuthContext, requestID string, request QueryRequest) (QueryResult, error) {
	return g.execute(ctx, auth, requestID, request, false)
}

// executeCompatibility preserves legacy metrics endpoints, which had no
// caller-visible total-row limit. The tenant policy maximum becomes their
// effective bound; the provider still enforces it after execution. New query
// clients retain the deliberately smaller defaultQueryLimit.
func (g *QueryGateway) executeCompatibility(ctx context.Context, auth AuthContext, requestID string, request QueryRequest) (QueryResult, error) {
	return g.execute(ctx, auth, requestID, request, true)
}

func (g *QueryGateway) prepareDatasetParameters(ctx context.Context, auth AuthContext, request QueryRequest) (json.RawMessage, error) {
	if g == nil || g.Datasets == nil || g.Providers == nil {
		return nil, ErrQueryProviderUnavailable
	}
	descriptor, ok := g.Datasets.Get(request.Dataset)
	if !ok {
		return nil, queryError(QueryErrorDatasetNotFound, "dataset is not registered", false, nil)
	}
	provider, release, err := g.Providers.acquire(descriptor.Provider)
	if err != nil {
		return nil, err
	}
	defer release()
	preparer, ok := provider.(QueryDatasetRequestPreparer)
	if !ok {
		return request.Parameters, nil
	}
	parameters, err := preparer.PrepareQuery(ctx, QueryProviderRequest{
		TenantID: auth.TenantID, UserID: auth.UserID, Dataset: descriptor,
		From: request.From, To: request.To, StepSeconds: request.StepSeconds, Limit: request.Limit,
		Cursor: request.Cursor, ValueLayer: request.ValueLayer, RequireComplete: request.RequireComplete,
		Parameters: request.Parameters,
	})
	if err != nil {
		return nil, err
	}
	request.Parameters = parameters
	normalized, err := normalizeQueryRequest(request)
	if err != nil {
		return nil, err
	}
	return normalized.Parameters, nil
}

func (g *QueryGateway) execute(ctx context.Context, auth AuthContext, requestID string, request QueryRequest, usePolicyLimit bool) (QueryResult, error) {
	if auth.TenantID == "" || auth.UserID == "" {
		return QueryResult{}, queryError(QueryErrorPermissionDenied, "authenticated tenant and user are required", false, nil)
	}
	request, err := normalizeQueryRequest(request)
	if err != nil {
		return QueryResult{}, err
	}
	descriptor, ok := g.Datasets.Get(request.Dataset)
	if !ok {
		return QueryResult{}, queryError(QueryErrorDatasetNotFound, "dataset is not registered", false, nil)
	}
	if err := g.requireModuleEnabled(ctx, auth.TenantID, descriptor.ModuleKey); err != nil {
		return QueryResult{}, err
	}
	if !datasetSupportsQueryLayer(descriptor, request.ValueLayer) {
		return QueryResult{}, queryError(QueryErrorInvalidRequest, "dataset does not support the requested value layer", false, nil)
	}
	policy, err := g.effectivePolicy(ctx, auth.TenantID, descriptor)
	if err != nil {
		return QueryResult{}, queryError(QueryErrorProviderUnavailable, "query policy storage is unavailable", true, err)
	}
	if !policy.Enabled {
		return QueryResult{}, queryError(QueryErrorDatasetDisabled, "dataset is disabled for this tenant", false, nil)
	}
	if !policy.allows(request.ValueLayer) {
		return QueryResult{}, queryError(QueryErrorDatasetDisabled, "value layer is disabled for this dataset", false, nil)
	}
	if !queryLayerAuthorized(auth, request.ValueLayer) {
		return QueryResult{}, queryError(QueryErrorPermissionDenied, "principal is not entitled to this value layer", false, nil)
	}
	maxRange := time.Duration(policy.MaxRangeSeconds) * time.Second
	if descriptor.MaxRangeDays > 0 {
		descriptorRange := time.Duration(descriptor.MaxRangeDays) * 24 * time.Hour
		if descriptorRange < maxRange {
			maxRange = descriptorRange
		}
	}
	if request.To.Sub(request.From) > maxRange {
		return QueryResult{}, &QueryGatewayError{Code: QueryErrorRangeLimit, Message: "query time range exceeds the dataset policy", Details: map[string]any{"max_range_seconds": uint64(maxRange / time.Second)}}
	}
	if request.Limit == 0 {
		request.Limit = min(defaultQueryLimit, policy.MaxResultRows)
		if usePolicyLimit {
			request.Limit = policy.MaxResultRows
		}
	}
	if request.Limit > policy.MaxResultRows {
		return QueryResult{}, &QueryGatewayError{Code: QueryErrorRowLimit, Message: "query limit exceeds the dataset policy", Details: map[string]any{"max_result_rows": policy.MaxResultRows}}
	}
	release, err := g.acquireTenant(auth.TenantID, descriptor.Key, policy.MaxConcurrent)
	if err != nil {
		return QueryResult{}, err
	}
	defer release()
	provider, releaseProvider, err := g.Providers.acquire(descriptor.Provider)
	if err != nil {
		if errors.Is(err, ErrQueryProviderUnavailable) {
			return QueryResult{}, queryError(QueryErrorProviderUnavailable, "dataset provider is disabled or unavailable", true, err)
		}
		return QueryResult{}, err
	}
	defer releaseProvider()

	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(policy.QueryTimeoutMS)*time.Millisecond)
	defer cancel()
	providerRequest := QueryProviderRequest{
		TenantID: auth.TenantID, UserID: auth.UserID, Dataset: descriptor,
		From: request.From, To: request.To, StepSeconds: request.StepSeconds, Limit: request.Limit,
		Cursor: request.Cursor, ValueLayer: request.ValueLayer, RequireComplete: request.RequireComplete,
		Parameters: request.Parameters,
	}
	if preparer, ok := provider.(QueryDatasetRequestPreparer); ok {
		providerRequest.Parameters, err = preparer.PrepareQuery(queryCtx, providerRequest)
		if err != nil {
			return QueryResult{}, err
		}
		request.Parameters = providerRequest.Parameters
		request, err = normalizeQueryRequest(request)
		if err != nil {
			return QueryResult{}, err
		}
		providerRequest.Parameters = request.Parameters
	}
	if authorizer, ok := provider.(QueryDatasetAuthorizer); ok {
		if err := authorizer.AuthorizeQuery(queryCtx, auth, providerRequest); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return QueryResult{}, queryError(QueryErrorCanceled, "query was canceled", false, err)
			}
			if errors.Is(queryCtx.Err(), context.DeadlineExceeded) {
				return QueryResult{}, queryError(QueryErrorTimeout, "query authorization timed out", true, err)
			}
			var gatewayErr *QueryGatewayError
			if errors.As(err, &gatewayErr) {
				return QueryResult{}, gatewayErr
			}
			return QueryResult{}, queryError(QueryErrorPermissionDenied, "query resource permission denied", false, err)
		}
	}
	providerResult, err := provider.Query(queryCtx, providerRequest)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || (errors.Is(err, context.Canceled) && ctx.Err() != nil) {
			return QueryResult{}, queryError(QueryErrorCanceled, "query was canceled", false, err)
		}
		if errors.Is(queryCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
			return QueryResult{}, queryError(QueryErrorTimeout, "query provider timed out", true, err)
		}
		var gatewayErr *QueryGatewayError
		if errors.As(err, &gatewayErr) {
			return QueryResult{}, gatewayErr
		}
		return QueryResult{}, queryError(QueryErrorProviderFailure, "query provider failed", true, err)
	}
	if !json.Valid(providerResult.Data) {
		return QueryResult{}, queryError(QueryErrorProviderFailure, "query provider returned invalid JSON", false, nil)
	}
	if request.RequireComplete && !queryResultIsComplete(providerResult.Completeness) {
		return QueryResult{}, &QueryGatewayError{Code: QueryErrorIncomplete, Message: "complete data is not available for the requested range", Details: map[string]any{"complete_ratio": providerResult.Completeness.CompleteRatio}}
	}
	now := g.Now().UTC()
	if providerResult.AsOf.IsZero() {
		providerResult.AsOf = now
	}
	return QueryResult{Data: providerResult.Data, Meta: QueryResultMeta{
		RequestID: requestID, SchemaVersion: "query-result-v2",
		AsOf: providerResult.AsOf.UTC(), Source: string(descriptor.Provider), ValueLayer: request.ValueLayer,
		Unit: providerResult.Unit, Timezone: providerResult.Timezone, StepSeconds: effectiveQueryStep(request.StepSeconds, providerResult.StepSeconds),
		PolicyVersion: policy.RowVersion, Versions: providerResult.Versions, NextCursor: providerResult.NextCursor,
		QueryCompleteness: providerResult.Completeness,
	}}, nil
}

func effectiveQueryStep(requested, provided uint32) uint32 {
	if provided != 0 {
		return provided
	}
	return requested
}

func queryResultIsComplete(completeness QueryCompleteness) bool {
	return !completeness.Partial && completeness.CompleteRatio == 1 && completeness.LateRatio == 0 && completeness.UnknownRatio == 0
}

func normalizeQueryRequest(request QueryRequest) (QueryRequest, error) {
	request.Dataset = strings.TrimSpace(request.Dataset)
	request.Cursor = strings.TrimSpace(request.Cursor)
	if request.Dataset == "" || len(request.Dataset) > 128 {
		return request, queryError(QueryErrorInvalidRequest, "dataset is required", false, nil)
	}
	if request.From.IsZero() || request.To.IsZero() || !request.To.After(request.From) {
		return request, queryError(QueryErrorInvalidRequest, "from and to must define a non-empty range", false, nil)
	}
	switch request.ValueLayer {
	case QueryValueRaw, QueryValueSupplier, QueryValueCustomer:
	default:
		return request, queryError(QueryErrorInvalidRequest, "value_layer must be raw, supplier or customer", false, nil)
	}
	if len(request.Cursor) > 2048 {
		return request, queryError(QueryErrorInvalidRequest, "cursor is too long", false, nil)
	}
	if len(request.Parameters) == 0 {
		request.Parameters = json.RawMessage("{}")
	}
	if len(request.Parameters) > maxQueryParameterBytes {
		return request, queryError(QueryErrorInvalidRequest, "parameters exceed 1 MiB", false, nil)
	}
	var parameters any
	if err := json.Unmarshal(request.Parameters, &parameters); err != nil {
		return request, queryError(QueryErrorInvalidRequest, "parameters must be valid JSON", false, err)
	}
	canonical, err := json.Marshal(parameters)
	if err != nil {
		return request, queryError(QueryErrorInvalidRequest, "parameters cannot be canonicalized", false, err)
	}
	request.Parameters = canonical
	request.From = request.From.UTC()
	request.To = request.To.UTC()
	return request, nil
}

func (g *QueryGateway) effectivePolicy(ctx context.Context, tenantID ID, descriptor DatasetDescriptor) (QueryDatasetPolicy, error) {
	policy, err := g.Policies.GetQueryDatasetPolicy(ctx, tenantID, descriptor.Key)
	if errors.Is(err, sql.ErrNoRows) {
		return defaultQueryDatasetPolicy(tenantID, descriptor), nil
	}
	return policy, err
}

func (g *QueryGateway) requireModuleEnabled(ctx context.Context, tenantID ID, moduleKey string) error {
	module, ok := g.Modules.Get(moduleKey)
	if !ok {
		return queryError(QueryErrorModuleDisabled, "dataset module is not registered", false, nil)
	}
	enabled := module.Descriptor().DefaultEnabled
	if g.TenantModules != nil {
		states, err := g.TenantModules.ListTenantModuleStates(ctx, tenantID)
		if err != nil {
			return queryError(QueryErrorProviderUnavailable, "module state is unavailable", true, err)
		}
		if override, exists := states[moduleKey]; exists {
			enabled = override
		}
	}
	if !enabled {
		return queryError(QueryErrorModuleDisabled, "dataset module is disabled for this tenant", false, nil)
	}
	return nil
}

func (g *QueryGateway) acquireTenant(tenantID ID, datasetKey string, maximum uint32) (func(), error) {
	key := string(tenantID) + "\x00" + datasetKey
	g.mu.Lock()
	state := g.inflight[key]
	if state == nil {
		state = &queryInflightState{}
		g.inflight[key] = state
	}
	if state.count >= maximum {
		g.mu.Unlock()
		return nil, &QueryGatewayError{Code: QueryErrorConcurrencyLimit, Message: "tenant dataset concurrency limit reached", Retryable: true, Details: map[string]any{"max_concurrent": maximum}}
	}
	state.count++
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			state.count--
			if state.count == 0 {
				delete(g.inflight, key)
			}
			g.mu.Unlock()
		})
	}, nil
}

func queryLayerAuthorized(auth AuthContext, layer QueryValueLayer) bool {
	if auth.IsAdmin {
		return true
	}
	action := ActionViewCustomer
	switch layer {
	case QueryValueRaw:
		action = ActionViewRaw
	case QueryValueSupplier:
		action = ActionViewSupplier
	}
	return HasPermission(AccessRequest{
		TenantID: auth.TenantID, UserID: auth.UserID, RoleIDs: auth.RoleIDs, Action: action,
		Resource: ResourceRef{Type: ResourceTenant, ID: auth.TenantID},
	}, auth.Grants)
}

func queryError(code QueryErrorCode, message string, retryable bool, cause error) *QueryGatewayError {
	return &QueryGatewayError{Code: code, Message: message, Retryable: retryable, Cause: cause}
}
