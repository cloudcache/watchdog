package watchdog

import (
	"encoding/json"
	"errors"
	"net/http"
)

type billingAPI struct {
	repo    BillingRepository
	network NetworkRepository
	metrics MetricsService
}

func registerBillingRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo BillingRepository, network NetworkRepository, metrics MetricsService) {
	api := billingAPI{repo: repo, network: network, metrics: metrics}
	configureTenant := RequirePermission(ActionConfigure, TenantResource)
	mux.Handle("GET /api/v1/billing/accounts", auth(configureTenant(http.HandlerFunc(api.listAccounts))))
	mux.Handle("POST /api/v1/billing/accounts", auth(configureTenant(http.HandlerFunc(api.createAccount))))
	mux.Handle("GET /api/v1/billing/accounts/{billing_account_id}", auth(configureTenant(http.HandlerFunc(api.getAccount))))
	mux.Handle("PATCH /api/v1/billing/accounts/{billing_account_id}", auth(configureTenant(http.HandlerFunc(api.patchAccount))))
	mux.Handle("GET /api/v1/billing/accounts/{billing_account_id}/ports", auth(configureTenant(http.HandlerFunc(api.listAccountPorts))))
	mux.Handle("PUT /api/v1/billing/accounts/{billing_account_id}/ports", auth(configureTenant(http.HandlerFunc(api.replaceAccountPorts))))
	mux.Handle("POST /api/v1/billing/accounts/{billing_account_id}/periods", auth(configureTenant(http.HandlerFunc(api.createPeriod))))
	mux.Handle("POST /api/v1/billing/periods/{billing_period_id}/compute", auth(configureTenant(http.HandlerFunc(api.computePeriod))))
	mux.Handle("POST /api/v1/billing/periods/{billing_period_id}/approve", auth(configureTenant(http.HandlerFunc(api.approvePeriod))))
	mux.Handle("POST /api/v1/billing/periods/{billing_period_id}/void", auth(configureTenant(http.HandlerFunc(api.voidPeriod))))
}

func (api billingAPI) listAccounts(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	accounts, err := api.repo.ListBillingAccounts(r.Context(), auth.TenantID)
	if err != nil {
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": accounts})
}

func (api billingAPI) createAccount(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	account, err := decodeBillingAccountRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := authorizeBillingValueMode(auth, account.ValueMode); err != nil {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, err.Error(), nil)
		return
	}
	account.TenantID = auth.TenantID
	created, err := api.repo.CreateBillingAccount(r.Context(), account)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api billingAPI) getAccount(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	account, err := api.repo.GetBillingAccount(r.Context(), auth.TenantID, ID(r.PathValue("billing_account_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Billing account not found", nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, account)
}

func (api billingAPI) patchAccount(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	account, err := decodeBillingAccountRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := authorizeBillingValueMode(auth, account.ValueMode); err != nil {
		WriteAPIError(w, http.StatusForbidden, APIErrorPermissionDenied, err.Error(), nil)
		return
	}
	account.ID = ID(r.PathValue("billing_account_id"))
	account.TenantID = auth.TenantID
	updated, err := api.repo.UpdateBillingAccount(r.Context(), account)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api billingAPI) listAccountPorts(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	ports, err := api.repo.ListBillingAccountPorts(r.Context(), auth.TenantID, ID(r.PathValue("billing_account_id")))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": ports})
}

func (api billingAPI) replaceAccountPorts(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	ports, err := decodeBillingAccountPortsRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	accountID := ID(r.PathValue("billing_account_id"))
	for i := range ports {
		if ports[i].PortID == "" {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "billing port id is required", nil)
			return
		}
		if !isSupportedBillingDirection(ports[i].Direction) {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "unsupported billing port direction", nil)
			return
		}
		if api.network != nil {
			if _, err := api.network.GetPort(r.Context(), auth.TenantID, ports[i].PortID); err != nil {
				WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "billing port must be a discovered network port", nil)
				return
			}
		}
		ports[i].BillingAccountID = accountID
		ports[i].TenantID = auth.TenantID
	}
	if err := api.repo.ReplaceBillingAccountPorts(r.Context(), auth.TenantID, accountID, ports); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (api billingAPI) createPeriod(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	period, err := decodeBillingPeriodRequest(r)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	period.TenantID = auth.TenantID
	period.BillingAccountID = ID(r.PathValue("billing_account_id"))
	created, err := api.repo.CreateBillingPeriod(r.Context(), period)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusCreated, created)
}

func (api billingAPI) computePeriod(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	period, err := api.repo.GetBillingPeriod(r.Context(), auth.TenantID, ID(r.PathValue("billing_period_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Billing period not found", nil)
		return
	}
	account, err := api.repo.GetBillingAccount(r.Context(), auth.TenantID, period.BillingAccountID)
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Billing account not found", nil)
		return
	}
	samples, err := api.loadBillingSamples(r, auth, account, period)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	computed, err := ComputeBillingPeriod(account, period, samples)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := api.repo.MarkBillingPeriodComputed(r.Context(), auth.TenantID, period.ID, computed.ComputedValue, computed.TotalBytes); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, computed)
}

func (api billingAPI) loadBillingSamples(r *http.Request, auth AuthContext, account BillingAccount, period BillingPeriod) ([]Sample, error) {
	if api.metrics.Client != nil && api.network != nil {
		return BillingMetricsLoader{
			Billing: api.repo,
			Network: api.network,
			Metrics: api.metrics,
		}.LoadSamples(r.Context(), auth.TenantID, account, period)
	}
	return decodeBillingComputeRequest(r)
}

func (api billingAPI) approvePeriod(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	period, err := api.repo.GetBillingPeriod(r.Context(), auth.TenantID, ID(r.PathValue("billing_period_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Billing period not found", nil)
		return
	}
	updated, err := ApproveBillingPeriod(period)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := api.repo.UpdateBillingPeriodStatus(r.Context(), auth.TenantID, period.ID, updated.Status); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, updated)
}

func (api billingAPI) voidPeriod(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	period, err := api.repo.GetBillingPeriod(r.Context(), auth.TenantID, ID(r.PathValue("billing_period_id")))
	if err != nil {
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Billing period not found", nil)
		return
	}
	updated, err := VoidBillingPeriod(period)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if err := api.repo.UpdateBillingPeriodStatus(r.Context(), auth.TenantID, period.ID, updated.Status); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	WriteAPIJSON(w, http.StatusOK, updated)
}

func decodeBillingAccountRequest(r *http.Request) (BillingAccount, error) {
	defer r.Body.Close()
	var account BillingAccount
	if err := json.NewDecoder(r.Body).Decode(&account); err != nil {
		return BillingAccount{}, err
	}
	if account.Name == "" {
		return BillingAccount{}, errors.New("billing account name is required")
	}
	return account, nil
}

func decodeBillingAccountPortsRequest(r *http.Request) ([]BillingAccountPort, error) {
	defer r.Body.Close()
	var body struct {
		Ports []BillingAccountPort `json:"ports"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body.Ports, nil
}

func decodeBillingPeriodRequest(r *http.Request) (BillingPeriod, error) {
	defer r.Body.Close()
	var period BillingPeriod
	if err := json.NewDecoder(r.Body).Decode(&period); err != nil {
		return BillingPeriod{}, err
	}
	if !period.RangeEnd.After(period.RangeStart) {
		return BillingPeriod{}, errors.New("billing period range end must be after start")
	}
	return period, nil
}

func decodeBillingComputeRequest(r *http.Request) ([]Sample, error) {
	defer r.Body.Close()
	var body struct {
		Samples []Sample `json:"samples"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, err
	}
	if len(body.Samples) == 0 {
		return nil, errors.New("samples are required")
	}
	return body.Samples, nil
}

func authorizeBillingValueMode(auth AuthContext, mode ExportValueMode) error {
	if mode == "" || mode == ExportValueCorrected {
		return nil
	}
	req := AccessRequest{
		TenantID: auth.TenantID,
		UserID:   auth.UserID,
		RoleIDs:  auth.RoleIDs,
		Action:   ActionAdmin,
		Resource: ResourceRef{Type: ResourceTenant, ID: auth.TenantID},
	}
	if auth.IsAdmin || HasPermission(req, auth.Grants) {
		return nil
	}
	return errors.New("raw billing value mode requires admin permission")
}

func isSupportedBillingDirection(direction BillingDirection) bool {
	switch direction {
	case "", BillingDirectionMax, BillingDirectionIn, BillingDirectionOut, BillingDirectionSum:
		return true
	default:
		return false
	}
}
