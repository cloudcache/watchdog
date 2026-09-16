// SPDX-License-Identifier: AGPL-3.0-only

package billing_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/server"
	"github.com/cloudcache/watchdog/internal/snmpch"
	_ "github.com/go-sql-driver/mysql"
)

type fixedSNMPBilling struct{ result snmpch.BillingResult }

func (f fixedSNMPBilling) ReadBilling(context.Context, snmpch.BillingRequest) (snmpch.BillingResult, error) {
	return f.result, nil
}

type fixedFlowBilling struct{ result flowch.FlowBillingResult }

func (f fixedFlowBilling) ReadBilling(context.Context, flowch.FlowBillingRequest) (flowch.FlowBillingResult, error) {
	return f.result, nil
}

type failedFlowBilling struct{}

func (failedFlowBilling) ReadBilling(context.Context, flowch.FlowBillingRequest) (flowch.FlowBillingResult, error) {
	return flowch.FlowBillingResult{}, errors.New("injected Flow read failure")
}

func TestBillingLifecycleMySQLAndClickHouseEvidence(t *testing.T) {
	dsn := isolatedMySQLDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}

	userID, deviceID, portID := billing.NewID(), billing.NewID(), billing.NewID()
	if _, err := db.ExecContext(ctx, `INSERT INTO users (id,username,status) VALUES (?,?,'active')`, userID, "kiss07-"+userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO devices (id,host) VALUES (?,?)`, deviceID, "kiss07-"+deviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO ports (id,device_id,if_index,if_name) VALUES (?,?,7,'xe-0/0/7')`, portID, deviceID); err != nil {
		t.Fatal(err)
	}
	var party billing.Party
	var account billing.Account
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM reconciliation_issues WHERE run_id IN (SELECT id FROM reconciliation_runs WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?))`, account.ID)
		_, _ = db.Exec(`DELETE FROM reconciliation_runs WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_adjustments WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_period_values WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_period_ports WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_periods WHERE account_id=?`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_account_ports WHERE account_id=?`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_accounts WHERE id=?`, account.ID)
		_, _ = db.Exec(`DELETE FROM parties WHERE id=?`, party.ID)
		_, _ = db.Exec(`DELETE FROM audit_logs WHERE actor_id=?`, userID)
		_, _ = db.Exec(`DELETE FROM users WHERE id=?`, userID)
		_, _ = db.Exec(`DELETE FROM devices WHERE id=?`, deviceID)
	})

	store := billing.NewStore(db)
	party, err = store.CreateParty(ctx, billing.Party{Kind: "customer", Status: "active", Name: "KISS-07 customer " + userID}, userID)
	if err != nil {
		t.Fatal(err)
	}
	cdr := uint64(10)
	account, err = store.CreateAccount(ctx, billing.Account{
		PartyID: party.ID, Name: "KISS-07 account " + userID, Status: "active", BillType: "cdr",
		Algorithm: billing.Algorithm95th, BillingDay: 1, Timezone: "Asia/Singapore", Direction: billing.DirectionAgg,
		DefaultLayer: billing.LayerCustomer, CDRBPS: &cdr, ReconcilePercent: 5,
	}, userID)
	if err != nil {
		t.Fatal(err)
	}
	// Empty binding direction inherits the account default; the resolved value
	// is then frozen in billing_period_ports.
	if err := store.ReplaceAccountPorts(ctx, account.ID, []billing.AccountPort{{PortID: portID}}, account.RowVersion, userID); err != nil {
		t.Fatal(err)
	}
	account, err = store.GetAccount(ctx, account.ID)
	if err != nil || account.RowVersion != 2 {
		t.Fatalf("account after binding=%+v err=%v", account, err)
	}
	from := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	period, err := store.CreatePeriod(ctx, account.ID, from, from.Add(100*time.Minute), from.Add(24*time.Hour), userID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteAccount(ctx, account.ID, account.RowVersion, userID); !errors.Is(err, billing.ErrInUse) {
		t.Fatalf("account with billing evidence could be deleted: %v", err)
	}
	if err := store.DeleteParty(ctx, party.ID, party.RowVersion, userID); !errors.Is(err, billing.ErrInUse) {
		t.Fatalf("party with billing accounts could be deleted: %v", err)
	}
	if _, err := store.CreatePeriod(ctx, account.ID, from.Add(5*time.Minute), from.Add(15*time.Minute), from.Add(24*time.Hour), userID); err == nil {
		t.Fatal("overlapping billing period was accepted")
	}
	account.Direction = billing.DirectionOut
	account.ReconcilePercent = 99
	account.Name += " edited"
	account, err = store.UpdateAccount(ctx, account, account.RowVersion, userID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceAccountPorts(ctx, account.ID, []billing.AccountPort{{PortID: portID, Direction: billing.DirectionOut}}, account.RowVersion, userID); err != nil {
		t.Fatal(err)
	}
	party.Name += " edited"
	party, err = store.UpdateParty(ctx, party, party.RowVersion, userID)
	if err != nil {
		t.Fatal(err)
	}
	period, err = store.GetPeriod(ctx, period.ID)
	if err != nil || period.Direction != billing.DirectionAgg || period.ReconcilePercent != 5 {
		t.Fatalf("period account snapshot changed with live account: %+v err=%v", period, err)
	}
	periodPorts, err := store.ListPeriodPorts(ctx, period.ID)
	if err != nil || len(periodPorts) != 1 || periodPorts[0].Direction != billing.DirectionAgg {
		t.Fatalf("period port snapshot changed with live binding: %+v err=%v", periodPorts, err)
	}
	var snapshot billing.PeriodAccountSnapshot
	if err := json.Unmarshal(period.AccountSnapshot, &snapshot); err != nil || snapshot.Account.Direction != billing.DirectionAgg || snapshot.Party == nil || strings.HasSuffix(snapshot.Party.Name, " edited") {
		t.Fatalf("frozen account/party snapshot=%+v err=%v", snapshot, err)
	}
	_, err = store.SaveExternalDraft(ctx, period.ID, billing.Value{
		Unit: "bps", Rate95thBPS: 15, RateAverageBPS: 13, AlgorithmValue: 15,
		Coverage: 1, ExpectedBuckets: 20, ObservedBuckets: 20, Provenance: json.RawMessage(`{"file":"external.csv","row":2}`),
	}, period.RowVersion, userID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveExternalDraft(ctx, period.ID, billing.Value{
		Unit: "bps", Rate95thBPS: 15, AlgorithmValue: 15, Coverage: 1, ExpectedBuckets: 20, ObservedBuckets: 20,
		Provenance: json.RawMessage(`{"file":"stale.csv"}`),
	}, period.RowVersion, userID); !errors.Is(err, billing.ErrConflict) {
		t.Fatalf("stale external import err=%v", err)
	}
	period, _ = store.GetPeriod(ctx, period.ID)
	offsetOperation := billing.NewID()
	offsetService, err := billing.NewService(store,
		fixedSNMPBilling{result: snmpch.BillingResult{From: from.Add(time.Minute), To: from.Add(100 * time.Minute)}},
		fixedFlowBilling{result: flowch.FlowBillingResult{From: from, To: from.Add(100 * time.Minute)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := offsetService.CalculateForOperation(ctx, period.ID, period.RowVersion, userID, offsetOperation); !errors.Is(err, billing.ErrWindowOffset) {
		t.Fatalf("window offset err=%v", err)
	}
	offsetRun, err := store.GetRunByOperation(ctx, offsetOperation)
	if err != nil || offsetRun.Status != "issues" || offsetRun.IssueCount != 1 || offsetRun.CalculationVersion != 0 {
		t.Fatalf("window offset run=%+v err=%v", offsetRun, err)
	}
	offsetIssues, total, err := store.ListIssues(ctx, period.ID, 0, billing.PageFilter{Limit: 10})
	if err != nil || total != 1 || len(offsetIssues) != 1 || offsetIssues[0].Kind != "window_offset" || offsetIssues[0].Severity != "critical" {
		t.Fatalf("window offset issues=%+v total=%d err=%v", offsetIssues, total, err)
	}
	if _, err := store.ResolveIssue(ctx, offsetIssues[0].ID, "acknowledged", "reader contract corrected", userID, offsetIssues[0].RowVersion); err != nil {
		t.Fatalf("resolve window offset issue: %v", err)
	}
	if _, err := offsetService.CalculateForOperation(ctx, period.ID, period.RowVersion, userID, offsetOperation); !errors.Is(err, billing.ErrWindowOffset) {
		t.Fatalf("window offset retry err=%v", err)
	}

	rates := make([]float64, 20)
	buckets := make([]snmpch.BillingBucket, 20)
	for index := range rates {
		rates[index] = float64(index + 1)
		buckets[index] = snmpch.BillingBucket{SelectedBPS: rates[index], Coverage: 1, PresentPorts: 1}
	}
	failedService, err := billing.NewService(store, fixedSNMPBilling{result: snmpch.BillingResult{}}, failedFlowBilling{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := failedService.Calculate(ctx, period.ID, period.RowVersion, userID); err == nil {
		t.Fatal("injected Flow failure was accepted")
	}
	var committedValues, committedRuns int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing_period_values WHERE period_id=? AND calculation_version>0`, period.ID).Scan(&committedValues); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM reconciliation_runs WHERE period_id=?`, period.ID).Scan(&committedRuns); err != nil {
		t.Fatal(err)
	}
	if committedValues != 0 || committedRuns != 1 {
		t.Fatalf("failed evidence read wrote a partial generation or extra run: values=%d runs=%d", committedValues, committedRuns)
	}
	service, err := billing.NewService(store,
		fixedSNMPBilling{result: snmpch.BillingResult{
			From: from, To: from.Add(100 * time.Minute), Buckets: buckets, ExpectedBuckets: 20, ObservedBuckets: 20,
			Coverage: 1, TotalSelectedBytes: 2200, GenerationMin: 4, GenerationMax: 4, ExpectedPorts: 1,
		}},
		fixedFlowBilling{result: flowch.FlowBillingResult{
			From: from, To: from.Add(100 * time.Minute), SourceGenerationMin: 7, SourceGenerationMax: 8,
			DimensionSnapshotRefs: []string{"dimension-7"}, GeoVersionRefs: []string{"geo-9"}, ClassificationVersions: []string{"3"},
			Layers: []flowch.BillingLayerResult{
				{Layer: "raw", SelectedBytes: 2000, RateBuckets: timestampedRates(from, rates, 19), Coverage: .95, ExpectedBuckets: 20, ObservedBuckets: 20, UnknownSamplingRecords: 1},
				{Layer: "supplier", SelectedBytes: 1800, RateBuckets: timestampedRates(from, scaleRates(rates, .9), -1), Coverage: 1, ExpectedBuckets: 20, ObservedBuckets: 20},
				{Layer: "customer", SelectedBytes: 1600, RateBuckets: timestampedRates(from, scaleRates(rates, .8), -1), Coverage: 1, ExpectedBuckets: 20, ObservedBuckets: 20},
			},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	operationID := billing.NewID()
	first, err := service.CalculateForOperation(ctx, period.ID, period.RowVersion, userID, operationID)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := service.CalculateForOperation(ctx, period.ID, period.RowVersion, userID, operationID)
	if err != nil || retry.Period.CalculationVersion != first.Period.CalculationVersion {
		t.Fatalf("operation retry=%+v err=%v", retry.Period, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing_period_values WHERE period_id=? AND calculation_version=1`, period.ID).Scan(&committedValues); err != nil || committedValues != 5 {
		t.Fatalf("idempotent operation values=%d err=%v", committedValues, err)
	}
	second, err := service.Calculate(ctx, period.ID, first.Period.RowVersion, userID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Period.CalculationVersion != 1 || second.Period.CalculationVersion != 2 {
		t.Fatalf("calculation versions first=%d second=%d", first.Period.CalculationVersion, second.Period.CalculationVersion)
	}
	lateRetry, err := service.CalculateForOperation(ctx, period.ID, second.Period.RowVersion, userID, operationID)
	if err != nil || lateRetry.Period.CalculationVersion != 1 || !sameLayerValues(first.Values, lateRetry.Values) {
		t.Fatalf("late operation retry=%+v err=%v", lateRetry, err)
	}
	if !sameLayerValues(first.Values, second.Values) {
		t.Fatalf("repeat calculation changed values: first=%+v second=%+v", first.Values, second.Values)
	}
	if _, err := store.ApprovePeriod(ctx, period.ID, first.Period.RowVersion, first.Period.CalculationVersion, userID); !errors.Is(err, billing.ErrConflict) {
		t.Fatalf("stale approval err=%v", err)
	}
	for _, issue := range second.Issues {
		if _, err := store.ResolveIssue(ctx, issue.ID, "acknowledged", "reviewed against source evidence", userID, issue.RowVersion); err != nil {
			t.Fatal(err)
		}
	}
	adjustment, err := store.CreateAdjustment(ctx, billing.AdjustmentInput{
		PeriodID: period.ID, Layer: billing.LayerCustomer, Unit: "bps", Amount: 100,
		Reason: "approved contract correction", EvidenceRef: "ticket-7", Actor: userID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApproveAdjustment(ctx, adjustment.ID, adjustment.RowVersion, userID); err != nil {
		t.Fatal(err)
	}
	period, _ = store.GetPeriod(ctx, period.ID)
	approved, err := store.ApprovePeriod(ctx, period.ID, period.RowVersion, period.CalculationVersion, userID)
	if err != nil {
		t.Fatal(err)
	}
	closed, err := store.ClosePeriod(ctx, period.ID, approved.RowVersion, approved.CalculationVersion, userID)
	if err != nil || closed.Status != billing.PeriodClosed {
		t.Fatalf("close=%+v err=%v", closed, err)
	}
	if _, err := service.Calculate(ctx, period.ID, closed.RowVersion, userID); !errors.Is(err, billing.ErrImmutable) {
		t.Fatalf("closed calculation err=%v", err)
	}
	if _, err := store.SaveExternalDraft(ctx, period.ID, billing.Value{Provenance: json.RawMessage(`{}`)}, closed.RowVersion, userID); !errors.Is(err, billing.ErrImmutable) {
		t.Fatalf("closed external import err=%v", err)
	}
	if _, err := store.CreateAdjustment(ctx, billing.AdjustmentInput{PeriodID: period.ID, Layer: billing.LayerCustomer, Unit: "bps", Amount: 1, Reason: "not allowed", Actor: userID}); !errors.Is(err, billing.ErrImmutable) {
		t.Fatalf("closed ordinary adjustment err=%v", err)
	}
	closingEvidence, err := store.ExportEvidence(ctx, period.ID, closed.CalculationVersion)
	if err != nil || len(closingEvidence.Adjustments) != 1 {
		t.Fatalf("closing evidence adjustments=%+v err=%v", closingEvidence.Adjustments, err)
	}
	reversal, err := store.ReverseAdjustment(ctx, adjustment.ID, userID, "reverse ticket-7")
	if err != nil {
		t.Fatal(err)
	}
	if reversal.Amount != -adjustment.Amount || reversal.ReversesAdjustmentID != adjustment.ID {
		t.Fatalf("reversal=%+v original=%+v", reversal, adjustment)
	}
	if _, err := store.ApproveAdjustment(ctx, reversal.ID, reversal.RowVersion, userID); err != nil {
		t.Fatal(err)
	}
	repeatedEvidence, err := store.ExportEvidence(ctx, period.ID, closed.CalculationVersion)
	if err != nil || len(repeatedEvidence.Adjustments) != 1 || repeatedEvidence.Adjustments[0].ID != closingEvidence.Adjustments[0].ID {
		t.Fatalf("closed evidence changed after reversal: before=%+v after=%+v err=%v", closingEvidence.Adjustments, repeatedEvidence.Adjustments, err)
	}
	ledger, _, err := store.ListAdjustments(ctx, period.ID, billing.PageFilter{Limit: 20})
	if err != nil || billing.EffectiveAdjustment(ledger, billing.LayerCustomer, "bps") != 0 {
		t.Fatalf("ledger=%+v err=%v", ledger, err)
	}
}

func scaleRates(input []float64, scale float64) []float64 {
	result := make([]float64, len(input))
	for index := range input {
		result[index] = input[index] * scale
	}
	return result
}

func timestampedRates(from time.Time, input []float64, incompleteIndex int) []flowch.BillingRateBucket {
	result := make([]flowch.BillingRateBucket, len(input))
	for index, value := range input {
		result[index] = flowch.BillingRateBucket{
			Time: from.Add(time.Duration(index) * 5 * time.Minute), SelectedBPS: value, Observed: true, Complete: index != incompleteIndex,
		}
	}
	return result
}

func sameLayerValues(left, right []billing.Value) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Layer != right[index].Layer || left[index].AlgorithmValue != right[index].AlgorithmValue ||
			left[index].SelectedBytes != right[index].SelectedBytes || left[index].Coverage != right[index].Coverage {
			return false
		}
	}
	return true
}
