package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
)

type snmpA2Executor struct {
	mu        sync.Mutex
	calls     int
	failCalls int
	block     bool
	started   chan struct{}
}

func (e *snmpA2Executor) Do(ctx context.Context, query ch.Query) error {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()
	if call <= e.failCalls {
		return errors.New("temporary ClickHouse failure")
	}
	if e.block {
		select {
		case e.started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
	results := query.Result.(proto.Results)
	if len(results) == 5 { // Scoped aggregate input; server applies policy, then aggregates.
		devices := query.ExternalData[0].Data.(*proto.ColStr)
		ports := query.ExternalData[1].Data.(*proto.ColStr)
		results[0].Data.(*proto.ColDateTime).Append(time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC))
		results[1].Data.(*proto.ColStr).Append(devices.Row(0))
		results[2].Data.(*proto.ColLowCardinality[string]).Append("port")
		results[3].Data.(*proto.ColStr).Append(ports.Row(0))
		results[4].Data.(*proto.ColFloat64).Append(8000)
		return query.OnResult(ctx, proto.Block{Rows: 1})
	}
	if len(results) == 2 { // Export aggregate; no presentation policy is applied.
		results[0].Data.(*proto.ColDateTime).Append(time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC))
		results[1].Data.(*proto.ColFloat64).Append(8000)
		return query.OnResult(ctx, proto.Block{Rows: 1})
	}
	if len(results) == 12 { // Billing.
		buckets := results[0].Data.(*proto.ColDateTime)
		inBytes := results[1].Data.(*proto.ColUInt64)
		outBytes := results[2].Data.(*proto.ColUInt64)
		selectedBytes := results[3].Data.(*proto.ColUInt64)
		inBPS := results[4].Data.(*proto.ColFloat64)
		outBPS := results[5].Data.(*proto.ColFloat64)
		selectedBPS := results[6].Data.(*proto.ColFloat64)
		coverage := results[7].Data.(*proto.ColFloat64)
		reset := results[8].Data.(*proto.ColUInt8)
		gap := results[9].Data.(*proto.ColUInt8)
		generation := results[10].Data.(*proto.ColUInt64)
		presentPorts := results[11].Data.(*proto.ColUInt64)
		for index := 0; index < 2; index++ {
			buckets.Append(time.Date(2026, 9, 9, 1, index*5, 0, 0, time.UTC))
			inBytes.Append(uint64(100 + index))
			outBytes.Append(uint64(200 + index))
			selectedBytes.Append(uint64(300 + index*2))
			inBPS.Append(float64(10 + index))
			outBPS.Append(float64(20 + index))
			selectedBPS.Append(float64(30 + index*2))
			coverage.Append(1)
			reset.Append(0)
			gap.Append(0)
			generation.Append(7)
			presentPorts.Append(1)
		}
		return query.OnResult(ctx, proto.Block{Rows: 2})
	}
	return errors.New("unexpected SNMP A2 query shape")
}

func TestSNMPA2APIPermissionsJobsAndBilling(t *testing.T) {
	dsn := isolatedMySQLDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}
	if err := EnsureRBACSeed(ctx, db); err != nil {
		t.Fatal(err)
	}

	userID, roleID := newID(), newID()
	deviceA, deviceB := newID(), newID()
	portA, portB := newID(), newID()
	accountA, accountB := newID(), newID()
	cleanup := func() {
		_, _ = db.Exec(`DELETE FROM operation_jobs WHERE created_by=? OR idempotency_key LIKE 'snmp-a2-%'`, userID)
		_, _ = db.Exec(`DELETE FROM billing_accounts WHERE id IN (?,?)`, accountA, accountB)
		_, _ = db.Exec(`DELETE FROM users WHERE id=?`, userID)
		_, _ = db.Exec(`DELETE FROM roles WHERE id=?`, roleID)
		_, _ = db.Exec(`DELETE FROM devices WHERE id IN (?,?)`, deviceA, deviceB)
	}
	cleanup()
	t.Cleanup(cleanup)
	if _, err := db.Exec(`INSERT INTO users (id,username,status) VALUES (?,?,'active')`, userID, "snmp-a2-"+userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO roles (id,name,title) VALUES (?,?,?)`, roleID, "snmp-a2-"+roleID, "SNMP A2 test"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO role_permissions (role_id,permission_id)
		SELECT ?,id FROM permissions WHERE ability IN ('device.view','port.view','bill.view')`, roleID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO user_roles (user_id,role_id) VALUES (?,?)`, userID, roleID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO devices (id,host) VALUES (?,?),(?,?)`, deviceA, "snmp-a2-a-"+deviceA, deviceB, "snmp-a2-b-"+deviceB); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ports (id,device_id,if_index) VALUES (?,?,1),(?,?,1)`, portA, deviceA, portB, deviceB); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO user_port_permissions (user_id,port_id) VALUES (?,?)`, userID, portA); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO billing_accounts (id,name) VALUES (?,?),(?,?)`, accountA, "snmp-a2-a-"+accountA, accountB, "snmp-a2-b-"+accountB); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO billing_account_ports (account_id,port_id,direction) VALUES (?,?,'agg')`, accountA, portA); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO user_billing_permissions (user_id,account_id) VALUES (?,?)`, userID, accountA); err != nil {
		t.Fatal(err)
	}

	p := &principal{UserID: userID, Abilities: map[string]bool{"device.view": true, "port.view": true, "bill.view": true}}
	normalExec := &snmpA2Executor{}
	normalStore, _ := snmpch.New(normalExec)
	s := &Server{db: db, jobs: opjob.NewStore(db), snmpMetrics: normalStore, cfg: Config{SNMP: SNMPConfig{ExportDir: t.TempDir(), ExportRetention: time.Hour}}}

	aggregate := directSNMPRequest(t, s.aggregateMetrics, p, http.MethodGet,
		"/api/v1/metrics/aggregate?port_ids="+portA+"&metric="+snmpch.MetricIfInBPS+"&aggregate=sum&time_mode=custom&start=2026-09-09T01:00:00Z&end=2026-09-09T01:10:00Z&step=300&max_data_points=10", nil)
	if aggregate.Code != http.StatusOK || !strings.Contains(aggregate.Body.String(), `"values":[[`) || !strings.Contains(aggregate.Body.String(), `"8000"`) {
		t.Fatalf("aggregate: status=%d body=%s", aggregate.Code, aggregate.Body.String())
	}
	denied := directSNMPRequest(t, s.aggregateMetrics, p, http.MethodGet,
		"/api/v1/metrics/aggregate?port_ids="+portB+"&metric="+snmpch.MetricIfInBPS+"&aggregate=sum&time_mode=fixed&window=1h", nil)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("denied aggregate: status=%d body=%s", denied.Code, denied.Body.String())
	}

	retryExec := &snmpA2Executor{failCalls: 1}
	s.snmpMetrics, _ = snmpch.New(retryExec)
	createBody := map[string]any{
		"port_ids": []string{portA}, "metric": snmpch.MetricIfInBPS, "aggregate": "sum",
		"start": "2026-09-09T01:00:00Z", "end": "2026-09-09T01:10:00Z", "step_seconds": 300, "max_rows": 10,
	}
	created := directSNMPRequest(t, s.createSNMPExport, p, http.MethodPost, "/api/v1/metrics/exports", createBody)
	if created.Code != http.StatusAccepted {
		t.Fatalf("create export: status=%d body=%s", created.Code, created.Body.String())
	}
	var createdView snmpExportResponse
	if err := json.Unmarshal(created.Body.Bytes(), &createdView); err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(context.Background())
	worker := &opjob.Worker{Repo: s.jobs, JobType: snmpCSVExportJobType, Owner: "snmp-a2-test", Handler: s.runSNMPCSVExport(s.cfg.SNMP.ExportDir), PollInterval: 5 * time.Millisecond, LeaseFor: 60 * time.Millisecond, RetryBase: 5 * time.Millisecond, MaxAttempts: 2}
	go worker.Run(workerCtx)
	job := waitSNMPJob(t, s.jobs, createdView.ID, opjob.StatusSucceeded)
	stopWorker()
	if job.AttemptCount != 2 || job.ProgressDone != 1 || job.ResultRef == "" {
		t.Fatalf("retried export job=%+v", job)
	}
	otherCheckpoint, _ := opjob.EncodePayload(snmpCSVExportPayloadSchema, snmpCSVExportPayload{
		PortIDs: []string{portA}, Metric: snmpch.MetricIfInBPS, Aggregate: "sum",
		FromMS: time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC).UnixMilli(), ToMS: time.Date(2026, 9, 9, 1, 10, 0, 0, time.UTC).UnixMilli(), StepSeconds: 300, MaxRows: 10,
	})
	otherJob, err := s.jobs.Enqueue(context.Background(), opjob.Job{JobType: snmpCSVExportJobType, IdempotencyKey: "snmp-a2-other-" + newID(), RequestHash: sha256hex(string(otherCheckpoint)), CheckpointJSON: otherCheckpoint})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.jobs.RequestCancel(context.Background(), otherJob.ID); err != nil {
		t.Fatal(err)
	}
	listed := directSNMPRequest(t, s.listSNMPExports, p, http.MethodGet, "/api/v1/exports?limit=1&offset=0&q="+createdView.ID+"&sort_by=created_at&sort_direction=desc", nil)
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"total":1`) {
		t.Fatalf("list exports: status=%d body=%s", listed.Code, listed.Body.String())
	}
	otherPrincipal := &principal{UserID: newID(), Abilities: p.Abilities}
	getDenied := directSNMPRequest(t, s.getSNMPExport, otherPrincipal, http.MethodGet, "/api/v1/exports/"+createdView.ID, nil)
	if getDenied.Code != http.StatusNotFound {
		t.Fatalf("cross-owner export: status=%d body=%s", getDenied.Code, getDenied.Body.String())
	}

	blockExec := &snmpA2Executor{block: true, started: make(chan struct{}, 1)}
	s.snmpMetrics, _ = snmpch.New(blockExec)
	createBody["idempotency_key"] = "snmp-a2-cancel-" + newID()
	cancelCreated := directSNMPRequest(t, s.createSNMPExport, p, http.MethodPost, "/api/v1/metrics/exports", createBody)
	var cancelView snmpExportResponse
	if cancelCreated.Code != http.StatusAccepted || json.Unmarshal(cancelCreated.Body.Bytes(), &cancelView) != nil {
		t.Fatalf("create cancel export: status=%d body=%s", cancelCreated.Code, cancelCreated.Body.String())
	}
	cancelCtx, stopCancelWorker := context.WithCancel(context.Background())
	defer stopCancelWorker()
	cancelWorker := &opjob.Worker{Repo: s.jobs, JobType: snmpCSVExportJobType, Owner: "snmp-a2-cancel", Handler: s.runSNMPCSVExport(s.cfg.SNMP.ExportDir), PollInterval: 5 * time.Millisecond, LeaseFor: 30 * time.Millisecond}
	go cancelWorker.Run(cancelCtx)
	select {
	case <-blockExec.started:
	case <-time.After(3 * time.Second):
		t.Fatal("cancel test export did not start")
	}
	if err := s.jobs.RequestCancel(context.Background(), cancelView.ID); err != nil {
		t.Fatal(err)
	}
	waitSNMPJob(t, s.jobs, cancelView.ID, opjob.StatusCanceled)
	stopCancelWorker()

	s.snmpMetrics = normalStore
	billing := directSNMPRequest(t, s.readSNMPBilling, p, http.MethodGet,
		"/api/v1/billing/accounts/"+accountA+"/snmp-usage?start=2026-09-09T01:00:00Z&end=2026-09-09T01:10:00Z&limit=1&offset=1", nil)
	if billing.Code != http.StatusOK || !strings.Contains(billing.Body.String(), `"total_selected_bytes":602`) || !strings.Contains(billing.Body.String(), `"total":2`) {
		t.Fatalf("billing: status=%d body=%s", billing.Code, billing.Body.String())
	}
	billingDenied := directSNMPRequest(t, s.readSNMPBilling, p, http.MethodGet,
		"/api/v1/billing/accounts/"+accountB+"/snmp-usage?start=2026-09-09T01:00:00Z&end=2026-09-09T01:10:00Z", nil)
	if billingDenied.Code != http.StatusForbidden {
		t.Fatalf("billing denied: status=%d body=%s", billingDenied.Code, billingDenied.Body.String())
	}
}

func directSNMPRequest(t *testing.T, handler gin.HandlerFunc, p *principal, method, target string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var input *bytes.Reader
	if body == nil {
		input = bytes.NewReader(nil)
	} else {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		input = bytes.NewReader(raw)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, input)
	c.Set(principalKey, p)
	if strings.Contains(target, "/billing/accounts/") {
		parts := strings.Split(target, "/")
		c.Params = gin.Params{{Key: "id", Value: strings.Split(parts[5], "?")[0]}}
	} else if strings.Contains(target, "/exports/") {
		parts := strings.Split(target, "/")
		c.Params = gin.Params{{Key: "id", Value: strings.Split(parts[4], "?")[0]}}
	}
	handler(c)
	return recorder
}

func waitSNMPJob(t *testing.T, store *opjob.Store, id, status string) opjob.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := store.Get(context.Background(), id)
		if err == nil && job.Status == status {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	job, err := store.Get(context.Background(), id)
	t.Fatalf("job %s did not reach %s: job=%+v err=%v", id, status, job, err)
	return opjob.Job{}
}
