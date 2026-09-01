package watchdog

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeBillingRepository struct {
	accounts []BillingAccount
	ports    []BillingAccountPort
	periods  []BillingPeriod
}

func (r *fakeBillingRepository) CreateBillingAccount(_ context.Context, account BillingAccount) (BillingAccount, error) {
	account = normalizeBillingAccount(account)
	r.accounts = append(r.accounts, account)
	return account, nil
}

func (r *fakeBillingRepository) GetBillingAccount(_ context.Context, _ ID, accountID ID) (BillingAccount, error) {
	for _, account := range r.accounts {
		if account.ID == accountID {
			return account, nil
		}
	}
	return BillingAccount{}, errNotFoundForTest{}
}

func (r *fakeBillingRepository) ListBillingAccounts(context.Context, ID) ([]BillingAccount, error) {
	return r.accounts, nil
}

func (r *fakeBillingRepository) UpdateBillingAccount(_ context.Context, account BillingAccount) (BillingAccount, error) {
	account = normalizeBillingAccount(account)
	for index, current := range r.accounts {
		if current.ID == account.ID {
			r.accounts[index] = account
			return account, nil
		}
	}
	r.accounts = append(r.accounts, account)
	return account, nil
}

func (r *fakeBillingRepository) ListBillingAccountPorts(_ context.Context, _ ID, accountID ID) ([]BillingAccountPort, error) {
	ports := make([]BillingAccountPort, 0, len(r.ports))
	for _, port := range r.ports {
		if port.BillingAccountID == accountID {
			ports = append(ports, port)
		}
	}
	return ports, nil
}

func (r *fakeBillingRepository) ReplaceBillingAccountPorts(_ context.Context, _ ID, _ ID, ports []BillingAccountPort) error {
	r.ports = ports
	return nil
}

func (r *fakeBillingRepository) CreateBillingPeriod(_ context.Context, period BillingPeriod) (BillingPeriod, error) {
	period = normalizeBillingPeriod(period)
	r.periods = append(r.periods, period)
	return period, nil
}

func (r *fakeBillingRepository) GetBillingPeriod(_ context.Context, _ ID, periodID ID) (BillingPeriod, error) {
	for _, period := range r.periods {
		if period.ID == periodID {
			return period, nil
		}
	}
	return BillingPeriod{}, errNotFoundForTest{}
}

func (r *fakeBillingRepository) MarkBillingPeriodComputed(_ context.Context, _ ID, periodID ID, computedValue float64, totalBytes uint64) error {
	for index := range r.periods {
		if r.periods[index].ID == periodID {
			r.periods[index].Status = BillingPeriodComputed
			r.periods[index].ComputedValue = computedValue
			r.periods[index].TotalBytes = totalBytes
		}
	}
	return nil
}

func (r *fakeBillingRepository) UpdateBillingPeriodStatus(_ context.Context, _ ID, periodID ID, status BillingPeriodStatus) error {
	for index := range r.periods {
		if r.periods[index].ID == periodID {
			r.periods[index].Status = status
		}
	}
	return nil
}

func TestAPIBillingCreateAccount(t *testing.T) {
	repo := &fakeBillingRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), Billing: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/billing/accounts", strings.NewReader(`{"ID":"billing-a","Name":"Customer A","BillingDay":1,"Aggregation":"p95_5m","ValueMode":"corrected"}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.accounts) != 1 || repo.accounts[0].TenantID != "tenant-a" {
		t.Fatalf("accounts = %#v", repo.accounts)
	}
}

func TestAPIBillingRejectsRawWithoutAdmin(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), Billing: &fakeBillingRepository{}})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/billing/accounts", strings.NewReader(`{"id":"billing-a","name":"Customer A","value_mode":"raw"}`)))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestAPIBillingPatchAccount(t *testing.T) {
	repo := &fakeBillingRepository{accounts: []BillingAccount{{ID: "billing-a", TenantID: "tenant-a", Name: "Old"}}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), Billing: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/api/v1/billing/accounts/billing-a", strings.NewReader(`{"name":"Customer A","billing_day":5,"aggregation":"avg_5m","value_mode":"corrected"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if repo.accounts[0].Name != "Customer A" || repo.accounts[0].BillingDay != 5 {
		t.Fatalf("accounts = %#v", repo.accounts)
	}
}

func TestAPIBillingReplacePorts(t *testing.T) {
	repo := &fakeBillingRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    billingTestAuth(false),
		Billing: repo,
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a"}},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/billing/accounts/billing-a/ports", strings.NewReader(`{"ports":[{"port_id":"port-a","direction":"max"}]}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.ports) != 1 || repo.ports[0].TenantID != "tenant-a" || repo.ports[0].BillingAccountID != "billing-a" {
		t.Fatalf("ports = %#v", repo.ports)
	}
}

func TestAPIBillingReplacePortsRejectsUnknownPort(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    billingTestAuth(false),
		Billing: &fakeBillingRepository{},
		Network: &fakeNetworkRepository{
			devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
			ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a"}},
		},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/billing/accounts/billing-a/ports", strings.NewReader(`{"ports":[{"PortID":"missing","Direction":"max"}]}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", rec.Code, rec.Body.String())
	}
}

func TestAPIBillingReplacePortsRejectsInvalidDirection(t *testing.T) {
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), Billing: &fakeBillingRepository{}})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/billing/accounts/billing-a/ports", strings.NewReader(`{"ports":[{"PortID":"port-a","Direction":"x=y"}]}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", rec.Code, rec.Body.String())
	}
}

func TestAPIBillingListPorts(t *testing.T) {
	repo := &fakeBillingRepository{ports: []BillingAccountPort{
		{TenantID: "tenant-a", BillingAccountID: "billing-a", PortID: "port-a", Direction: BillingDirectionMax},
		{TenantID: "tenant-a", BillingAccountID: "billing-b", PortID: "port-b", Direction: BillingDirectionIn},
	}}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), Billing: repo})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/billing/accounts/billing-a/ports", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "port-a") || strings.Contains(body, "port-b") {
		t.Fatalf("body = %s", body)
	}
}

func TestAPIBillingCreatePeriod(t *testing.T) {
	repo := &fakeBillingRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), Billing: repo})
	rec := httptest.NewRecorder()
	body := `{"id":"period-a","range_start":"2026-06-01T00:00:00Z","range_end":"2026-07-01T00:00:00Z"}`
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/billing/accounts/billing-a/periods", strings.NewReader(body)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if len(repo.periods) != 1 || repo.periods[0].RangeEnd.Before(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("periods = %#v", repo.periods)
	}
}

func TestAPIBillingComputeApproveVoidPeriod(t *testing.T) {
	repo := &fakeBillingRepository{
		accounts: []BillingAccount{{ID: "billing-a", TenantID: "tenant-a", Name: "Customer A", Aggregation: AggregationP95FiveMinute}},
		periods:  []BillingPeriod{{ID: "period-a", TenantID: "tenant-a", BillingAccountID: "billing-a", Status: BillingPeriodOpen}},
	}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: billingTestAuth(false), Billing: repo})
	compute := httptest.NewRecorder()
	router.ServeHTTP(compute, httptest.NewRequest(http.MethodPost, "/api/v1/billing/periods/period-a/compute", strings.NewReader(`{"samples":[{"Time":"2026-06-01T00:00:00Z","Value":10},{"Time":"2026-06-01T00:05:00Z","Value":20}]}`)))
	if compute.Code != http.StatusOK {
		t.Fatalf("compute status = %d, body = %s", compute.Code, compute.Body.String())
	}
	if repo.periods[0].Status != BillingPeriodComputed || repo.periods[0].ComputedValue != 20 {
		t.Fatalf("period after compute = %#v", repo.periods[0])
	}
	approve := httptest.NewRecorder()
	router.ServeHTTP(approve, httptest.NewRequest(http.MethodPost, "/api/v1/billing/periods/period-a/approve", nil))
	if approve.Code != http.StatusOK {
		t.Fatalf("approve status = %d, body = %s", approve.Code, approve.Body.String())
	}
	if repo.periods[0].Status != BillingPeriodApproved {
		t.Fatalf("period after approve = %#v", repo.periods[0])
	}
	repo.periods[0].Status = BillingPeriodComputed
	voided := httptest.NewRecorder()
	router.ServeHTTP(voided, httptest.NewRequest(http.MethodPost, "/api/v1/billing/periods/period-a/void", nil))
	if voided.Code != http.StatusOK {
		t.Fatalf("void status = %d, body = %s", voided.Code, voided.Body.String())
	}
	if repo.periods[0].Status != BillingPeriodVoid {
		t.Fatalf("period after void = %#v", repo.periods[0])
	}
}

func TestAPIBillingComputeLoadsAccountPortsFromVictoriaMetrics(t *testing.T) {
	repo := &fakeBillingRepository{
		accounts: []BillingAccount{{ID: "billing-a", TenantID: "tenant-a", Name: "Customer A", Aggregation: AggregationP95FiveMinute}},
		ports:    []BillingAccountPort{{TenantID: "tenant-a", BillingAccountID: "billing-a", PortID: "port-a", Direction: BillingDirectionSum}},
		periods: []BillingPeriod{{
			ID:               "period-a",
			TenantID:         "tenant-a",
			BillingAccountID: "billing-a",
			Status:           BillingPeriodOpen,
			RangeStart:       time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			RangeEnd:         time.Date(2026, 6, 1, 0, 10, 0, 0, time.UTC),
		}},
	}
	// VM returns raw octet counters; billing must derive bps rates from them.
	client := &fakeMetricsQueryClient{response: vmResponseForBillingTest([]Sample{
		{Time: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), Value: 10},
		{Time: time.Date(2026, 6, 1, 0, 5, 0, 0, time.UTC), Value: 20},
	})}
	network := &fakeNetworkRepository{
		devices: []NetworkDevice{{ID: "device-a", TenantID: "tenant-a", TargetID: "target-a"}},
		ports:   []NetworkPort{{ID: "port-a", TenantID: "tenant-a", DeviceID: "device-a", IfIndex: 1}},
	}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth:    billingTestAuth(false),
		Billing: repo,
		Network: network,
		Metrics: MetricsService{Client: client},
	})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/billing/periods/period-a/compute", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("compute status = %d, body = %s", rec.Code, rec.Body.String())
	}
	// Each direction: (20-10 octets)/300s x 8 bits; direction=sum doubles it.
	wantValue := 2 * ((20.0 - 10.0) / 300.0 * 8)
	if math.Abs(repo.periods[0].ComputedValue-wantValue) > 1e-9 {
		t.Fatalf("computed value = %v, want %v", repo.periods[0].ComputedValue, wantValue)
	}
	if !strings.Contains(client.query.Query, `device_id="device-a"`) || !strings.Contains(client.query.Query, `if_index="1"`) {
		t.Fatalf("query = %s", client.query.Query)
	}
}

func vmResponseForBillingTest(samples []Sample) VictoriaMetricsResponse {
	response := VictoriaMetricsResponse{Status: "success"}
	response.Data.Result = []VMRangeQueryItem{{Metric: map[string]string{"__name__": MetricSNMPIfInBps}}}
	for _, sample := range samples {
		response.Data.Result[0].Values = append(response.Data.Result[0].Values, VMValue{Time: sample.Time, Value: sample.Value})
	}
	return response
}

func billingTestAuth(admin bool) AuthContextAdapter {
	return func(*http.Request) (AuthContext, error) {
		actions := []Action{ActionConfigure}
		if admin {
			actions = append(actions, ActionAdmin)
		}
		return AuthContext{
			TenantID: "tenant-a",
			UserID:   "user-a",
			IsAdmin:  admin,
			Grants: []Permission{{
				TenantID:     "tenant-a",
				SubjectType:  SubjectUser,
				SubjectID:    "user-a",
				ResourceType: ResourceTenant,
				ResourceID:   "tenant-a",
				Actions:      actions,
			}},
		}, nil
	}
}
