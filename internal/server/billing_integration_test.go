// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/snmpch"
)

type apiBillingSNMPReader struct{}

func (apiBillingSNMPReader) ReadBilling(_ context.Context, request snmpch.BillingRequest) (snmpch.BillingResult, error) {
	return snmpch.BillingResult{
		From: request.From, To: request.To, ExpectedBuckets: 2, ObservedBuckets: 2, Coverage: 1,
		TotalInBytes: 100, TotalOutBytes: 200, TotalSelectedBytes: 300, GenerationMin: 4, GenerationMax: 4, ExpectedPorts: 1,
		Buckets: []snmpch.BillingBucket{{SelectedBPS: 1000, Coverage: 1, PresentPorts: 1}, {SelectedBPS: 1000, Coverage: 1, PresentPorts: 1}},
	}, nil
}

type apiBillingFlowReader struct{}

func (apiBillingFlowReader) ReadBilling(_ context.Context, request flowch.FlowBillingRequest) (flowch.FlowBillingResult, error) {
	return flowch.FlowBillingResult{
		From: request.From, To: request.To, SourceGenerationMin: 7, SourceGenerationMax: 7,
		DimensionSnapshotRefs: []string{"dimension-api"}, GeoVersionRefs: []string{"geo-api"}, ClassificationVersions: []string{"3"},
		Layers: []flowch.BillingLayerResult{
			{Layer: "raw", InBytes: 100, OutBytes: 200, SelectedBytes: 300, RateBuckets: apiBillingRateBuckets(request.From, false), Coverage: .5, ExpectedBuckets: 2, ObservedBuckets: 2, UnknownSamplingRecords: 1},
			{Layer: "supplier", InBytes: 100, OutBytes: 200, SelectedBytes: 300, RateBuckets: apiBillingRateBuckets(request.From, true), Coverage: 1, ExpectedBuckets: 2, ObservedBuckets: 2},
			{Layer: "customer", InBytes: 100, OutBytes: 200, SelectedBytes: 300, RateBuckets: apiBillingRateBuckets(request.From, true), Coverage: 1, ExpectedBuckets: 2, ObservedBuckets: 2},
		},
	}, nil
}

func apiBillingRateBuckets(from time.Time, secondComplete bool) []flowch.BillingRateBucket {
	return []flowch.BillingRateBucket{
		{Time: from, SelectedBPS: 1000, Observed: true, Complete: true},
		{Time: from.Add(5 * time.Minute), SelectedBPS: 1000, Observed: true, Complete: secondComplete},
	}
}

func TestBillingAPIAsyncLifecycleAndVTables(t *testing.T) {
	dsn := isolatedMySQLDSN(t)
	exportDir := t.TempDir()
	s, err := New(Config{MySQL: MySQLConfig{DSN: dsn}, Admin: AdminConfig{Username: "kiss07-admin", Password: "kiss07-password"}, Billing: BillingConfig{ExportDir: exportDir}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	service, err := billing.NewService(s.billingStore, apiBillingSNMPReader{}, apiBillingFlowReader{})
	if err != nil {
		t.Fatal(err)
	}
	s.billingService = service
	s.startBillingJobs()

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]any{"username": "kiss07-admin", "password": "kiss07-password"}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login=%d %s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	headers := map[string]string{"X-CSRF-Token": cookieValue(cookies, csrfCookie)}
	var actor string
	if err := s.db.QueryRow(`SELECT id FROM users WHERE username='kiss07-admin'`).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	deviceID, portID := newID(), newID()
	if _, err := s.db.Exec(`INSERT INTO devices (id,host) VALUES (?,?)`, deviceID, "kiss07-api-"+deviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO ports (id,device_id,if_index,if_name) VALUES (?,?,9,'et-0/0/9')`, portID, deviceID); err != nil {
		t.Fatal(err)
	}

	partyResponse := requestJSON(t, s, http.MethodPost, "/api/v1/billing/parties", map[string]any{
		"kind": "customer", "status": "active", "name": "API customer " + deviceID,
	}, headers, cookies...)
	if partyResponse.Code != http.StatusCreated || partyResponse.Header().Get("ETag") != `"1"` {
		t.Fatalf("party=%d %s", partyResponse.Code, partyResponse.Body.String())
	}
	var party billing.Party
	decodeJSON(t, partyResponse, &party)
	accountResponse := requestJSON(t, s, http.MethodPost, "/api/v1/billing/accounts", map[string]any{
		"party_id": party.ID, "name": "API account " + deviceID, "status": "active", "bill_type": "cdr", "algorithm": "95th",
		"billing_day": 1, "timezone": "Asia/Singapore", "direction": "agg", "default_layer": "customer", "cdr_bps": 900, "reconcile_percent": 5,
	}, headers, cookies...)
	if accountResponse.Code != http.StatusCreated {
		t.Fatalf("account=%d %s", accountResponse.Code, accountResponse.Body.String())
	}
	var account billing.Account
	decodeJSON(t, accountResponse, &account)
	t.Cleanup(func() {
		_, _ = s.db.Exec(`DELETE FROM operation_jobs WHERE created_by=?`, actor)
		_, _ = s.db.Exec(`DELETE FROM reconciliation_issues WHERE run_id IN (SELECT id FROM reconciliation_runs WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?))`, account.ID)
		_, _ = s.db.Exec(`DELETE FROM reconciliation_runs WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = s.db.Exec(`DELETE FROM billing_adjustments WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = s.db.Exec(`DELETE FROM billing_period_values WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = s.db.Exec(`DELETE FROM billing_period_ports WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = s.db.Exec(`DELETE FROM billing_periods WHERE account_id=?`, account.ID)
		_, _ = s.db.Exec(`DELETE FROM billing_account_ports WHERE account_id=?`, account.ID)
		_, _ = s.db.Exec(`DELETE FROM billing_accounts WHERE id=?`, account.ID)
		_, _ = s.db.Exec(`DELETE FROM parties WHERE id=?`, party.ID)
		_, _ = s.db.Exec(`DELETE FROM devices WHERE id=?`, deviceID)
		_, _ = s.db.Exec(`DELETE FROM audit_logs WHERE actor_id=?`, actor)
	})

	accounts := requestJSON(t, s, http.MethodGet, "/api/v1/billing/accounts?limit=10&offset=0&q="+deviceID+"&status=active&type=cdr&sort=name&order=asc", nil, nil, cookies...)
	if accounts.Code != http.StatusOK || !strings.Contains(accounts.Body.String(), account.ID) || !strings.Contains(accounts.Body.String(), `"total":1`) {
		t.Fatalf("account VTable=%d %s", accounts.Code, accounts.Body.String())
	}
	accountDetail := requestJSON(t, s, http.MethodGet, "/api/v1/billing/accounts/"+account.ID, nil, nil, cookies...)
	if accountDetail.Code != http.StatusOK || !strings.Contains(accountDetail.Body.String(), `"suggested_period"`) {
		t.Fatalf("account suggested period=%d %s", accountDetail.Code, accountDetail.Body.String())
	}
	ports := requestJSON(t, s, http.MethodPut, "/api/v1/billing/accounts/"+account.ID+"/ports", map[string]any{
		"items": []map[string]any{{"port_id": portID, "direction": "agg"}},
	}, map[string]string{"X-CSRF-Token": headers["X-CSRF-Token"], "If-Match": accountResponse.Header().Get("ETag")}, cookies...)
	if ports.Code != http.StatusOK || ports.Header().Get("ETag") != `"2"` {
		t.Fatalf("ports=%d %s", ports.Code, ports.Body.String())
	}

	now := time.Now().UTC().Truncate(5 * time.Minute)
	periodResponse := requestJSON(t, s, http.MethodPost, "/api/v1/billing/accounts/"+account.ID+"/periods", map[string]any{
		"date_from": now.Add(-10 * time.Minute), "date_to": now,
	}, headers, cookies...)
	if periodResponse.Code != http.StatusCreated {
		t.Fatalf("period=%d %s", periodResponse.Code, periodResponse.Body.String())
	}
	var period billing.Period
	decodeJSON(t, periodResponse, &period)
	invalidExternal := requestJSON(t, s, http.MethodPost, "/api/v1/billing/periods/"+period.ID+"/external", map[string]any{
		"unit": "bps", "rate_95th_bps": 900, "algorithm_value": 1000,
		"coverage": 1, "expected_buckets": 2, "observed_buckets": 2, "provenance": map[string]any{"file": "invalid.csv"},
	}, map[string]string{"X-CSRF-Token": headers["X-CSRF-Token"], "If-Match": periodResponse.Header().Get("ETag")}, cookies...)
	if invalidExternal.Code != http.StatusBadRequest {
		t.Fatalf("inconsistent external evidence=%d %s", invalidExternal.Code, invalidExternal.Body.String())
	}
	external := requestJSON(t, s, http.MethodPost, "/api/v1/billing/periods/"+period.ID+"/external", map[string]any{
		"unit": "bps", "rate_95th_bps": 1000, "rate_average_bps": 1000, "algorithm_value": 1000,
		"coverage": 1, "expected_buckets": 2, "observed_buckets": 2, "provenance": map[string]any{"file": "invoice.csv", "row": 2},
	}, map[string]string{"X-CSRF-Token": headers["X-CSRF-Token"], "If-Match": periodResponse.Header().Get("ETag")}, cookies...)
	if external.Code != http.StatusOK || external.Header().Get("ETag") != `"2"` {
		t.Fatalf("external=%d %s", external.Code, external.Body.String())
	}
	jobResponse := requestJSON(t, s, http.MethodPost, "/api/v1/billing/periods/"+period.ID+"/calculate", map[string]any{"idempotency_key": "calc-" + period.ID},
		map[string]string{"X-CSRF-Token": headers["X-CSRF-Token"], "If-Match": external.Header().Get("ETag")}, cookies...)
	if jobResponse.Code != http.StatusAccepted {
		t.Fatalf("calculate enqueue=%d %s", jobResponse.Code, jobResponse.Body.String())
	}
	var job struct {
		ID string `json:"id"`
	}
	decodeJSON(t, jobResponse, &job)
	waitBillingJob(t, s, job.ID, cookies)
	periodView := requestJSON(t, s, http.MethodGet, "/api/v1/billing/periods/"+period.ID, nil, nil, cookies...)
	if periodView.Code != http.StatusOK || !strings.Contains(periodView.Body.String(), `"status":"calculated"`) || !strings.Contains(periodView.Body.String(), `"layer":"customer"`) {
		t.Fatalf("period view=%d %s", periodView.Code, periodView.Body.String())
	}
	var view struct {
		Period billing.Period  `json:"period"`
		Values []billing.Value `json:"values"`
	}
	decodeJSON(t, periodView, &view)
	issuesResponse := requestJSON(t, s, http.MethodGet, "/api/v1/billing/periods/"+period.ID+"/issues?limit=50&sort=severity&order=desc&status=open", nil, nil, cookies...)
	if issuesResponse.Code != http.StatusOK {
		t.Fatalf("issues=%d %s", issuesResponse.Code, issuesResponse.Body.String())
	}
	var issuePage struct {
		Items []billing.ReconciliationIssue `json:"items"`
	}
	decodeJSON(t, issuesResponse, &issuePage)
	if len(issuePage.Items) == 0 {
		t.Fatal("missing-sampling evidence did not create an issue")
	}
	for _, issue := range issuePage.Items {
		resolved := requestJSON(t, s, http.MethodPatch, "/api/v1/billing/issues/"+issue.ID, map[string]any{"status": "acknowledged", "resolution_note": "reviewed"},
			map[string]string{"X-CSRF-Token": headers["X-CSRF-Token"], "If-Match": `"` + strconv.FormatUint(issue.RowVersion, 10) + `"`}, cookies...)
		if resolved.Code != http.StatusOK {
			t.Fatalf("resolve issue=%d %s", resolved.Code, resolved.Body.String())
		}
	}
	adjustmentResponse := requestJSON(t, s, http.MethodPost, "/api/v1/billing/periods/"+period.ID+"/adjustments", map[string]any{
		"layer": "customer", "unit": "bps", "amount": 100, "reason": "contract", "evidence_ref": "ticket-api",
	}, headers, cookies...)
	if adjustmentResponse.Code != http.StatusCreated {
		t.Fatalf("adjustment=%d %s", adjustmentResponse.Code, adjustmentResponse.Body.String())
	}
	var adjustment billing.Adjustment
	decodeJSON(t, adjustmentResponse, &adjustment)
	approvedAdjustment := requestJSON(t, s, http.MethodPost, "/api/v1/billing/adjustments/"+adjustment.ID+"/approve", map[string]any{},
		map[string]string{"X-CSRF-Token": headers["X-CSRF-Token"], "If-Match": adjustmentResponse.Header().Get("ETag")}, cookies...)
	if approvedAdjustment.Code != http.StatusOK {
		t.Fatalf("approve adjustment=%d %s", approvedAdjustment.Code, approvedAdjustment.Body.String())
	}
	periodView = requestJSON(t, s, http.MethodGet, "/api/v1/billing/periods/"+period.ID, nil, nil, cookies...)
	decodeJSON(t, periodView, &view)
	approvedPeriod := requestJSON(t, s, http.MethodPost, "/api/v1/billing/periods/"+period.ID+"/approve", map[string]any{"calculation_version": view.Period.CalculationVersion},
		map[string]string{"X-CSRF-Token": headers["X-CSRF-Token"], "If-Match": periodView.Header().Get("ETag")}, cookies...)
	if approvedPeriod.Code != http.StatusOK {
		t.Fatalf("approve period=%d %s", approvedPeriod.Code, approvedPeriod.Body.String())
	}
	closedPeriod := requestJSON(t, s, http.MethodPost, "/api/v1/billing/periods/"+period.ID+"/close", map[string]any{"calculation_version": view.Period.CalculationVersion},
		map[string]string{"X-CSRF-Token": headers["X-CSRF-Token"], "If-Match": approvedPeriod.Header().Get("ETag")}, cookies...)
	if closedPeriod.Code != http.StatusOK || !strings.Contains(closedPeriod.Body.String(), `"status":"closed"`) {
		t.Fatalf("close period=%d %s", closedPeriod.Code, closedPeriod.Body.String())
	}
	exportIDs := make(map[string]string)
	for _, format := range []string{"csv", "parquet"} {
		exportResponse := requestJSON(t, s, http.MethodPost, "/api/v1/billing/periods/"+period.ID+"/exports", map[string]any{
			"format": format, "calculation_version": view.Period.CalculationVersion, "idempotency_key": "export-" + format + "-" + period.ID,
		}, headers, cookies...)
		if exportResponse.Code != http.StatusAccepted {
			t.Fatalf("%s export=%d %s", format, exportResponse.Code, exportResponse.Body.String())
		}
		var exportJob struct {
			ID string `json:"id"`
		}
		decodeJSON(t, exportResponse, &exportJob)
		exportIDs[format] = exportJob.ID
		waitBillingExport(t, s, exportJob.ID, cookies)
		download := requestJSON(t, s, http.MethodGet, "/api/v1/billing/exports/"+exportJob.ID+"/download", nil, nil, cookies...)
		if download.Code != http.StatusOK {
			t.Fatalf("%s download=%d %s", format, download.Code, download.Body.String())
		}
		if format == "csv" && !strings.Contains(download.Body.String(), "record_kind,period_id") {
			t.Fatalf("CSV evidence=%q", download.Body.String())
		}
		if format == "parquet" && !strings.HasPrefix(download.Body.String(), "PAR1") {
			t.Fatalf("Parquet evidence magic=%q", download.Body.String()[:min(16, download.Body.Len())])
		}
	}
	csvJob, err := s.jobs.Get(context.Background(), exportIDs["csv"])
	if err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(exportDir, filepath.Base(strings.TrimPrefix(csvJob.ResultRef, "billing-export/")))
	if err := os.WriteFile(artifact, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	corrupt := requestJSON(t, s, http.MethodGet, "/api/v1/billing/exports/"+exportIDs["csv"]+"/download", nil, nil, cookies...)
	if corrupt.Code != http.StatusConflict || !strings.Contains(corrupt.Body.String(), "export_corrupt") {
		t.Fatalf("corrupt export=%d %s", corrupt.Code, corrupt.Body.String())
	}
	if _, err := s.db.Exec(`UPDATE operation_jobs SET finished_at=UTC_TIMESTAMP(3)-INTERVAL 8 DAY WHERE id=?`, exportIDs["parquet"]); err != nil {
		t.Fatal(err)
	}
	expired := requestJSON(t, s, http.MethodGet, "/api/v1/billing/exports/"+exportIDs["parquet"]+"/download", nil, nil, cookies...)
	if expired.Code != http.StatusGone || !strings.Contains(expired.Body.String(), "export_expired") {
		t.Fatalf("expired export=%d %s", expired.Code, expired.Body.String())
	}
	accountDelete := requestJSON(t, s, http.MethodDelete, "/api/v1/billing/accounts/"+account.ID, nil,
		map[string]string{"X-CSRF-Token": headers["X-CSRF-Token"], "If-Match": `"2"`}, cookies...)
	if accountDelete.Code != http.StatusConflict {
		t.Fatalf("account with evidence deleted=%d %s", accountDelete.Code, accountDelete.Body.String())
	}
	var auditCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM audit_logs WHERE actor_id=? AND action LIKE 'billing.%'`, actor).Scan(&auditCount); err != nil || auditCount < 10 {
		t.Fatalf("billing audit count=%d err=%v", auditCount, err)
	}
}

func waitBillingJob(t *testing.T, s *Server, jobID string, cookies []*http.Cookie) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		response := requestJSON(t, s, http.MethodGet, "/api/v1/billing/jobs/"+jobID, nil, nil, cookies...)
		if response.Code != http.StatusOK {
			t.Fatalf("billing job=%d %s", response.Code, response.Body.String())
		}
		var job struct {
			Status string `json:"status"`
			Error  string `json:"last_error_detail"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		if job.Status == "succeeded" {
			return
		}
		if job.Status == "failed" || job.Status == "canceled" {
			t.Fatalf("billing job status=%s error=%s", job.Status, job.Error)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("billing job did not finish")
}

func waitBillingExport(t *testing.T, s *Server, jobID string, cookies []*http.Cookie) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		response := requestJSON(t, s, http.MethodGet, "/api/v1/billing/exports/"+jobID, nil, nil, cookies...)
		if response.Code != http.StatusOK {
			t.Fatalf("billing export=%d %s", response.Code, response.Body.String())
		}
		var job struct {
			Status string `json:"status"`
			Error  string `json:"last_error_detail"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
		if job.Status == "succeeded" {
			return
		}
		if job.Status == "failed" || job.Status == "canceled" {
			t.Fatalf("billing export status=%s error=%s", job.Status, job.Error)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("billing export did not finish")
}
