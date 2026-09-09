// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/parquet-go/parquet-go"
)

func TestBillingEvidenceCSVAndParquetShareRowsAndProvenance(t *testing.T) {
	now := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	allowed, used, overuse := uint64(800), uint64(1000), uint64(200)
	evidence := billing.ExportEvidence{
		Period: billing.Period{ID: "period-1", AccountID: "account-1", Status: billing.PeriodClosed, Algorithm: billing.Algorithm95th,
			DefaultLayer: billing.LayerCustomer, Direction: billing.DirectionIn, CalculationVersion: 3, DateFrom: now.Add(-time.Hour), DateTo: now,
			Timezone: "Asia/Singapore", Allowed: &allowed, Used: &used, Overuse: &overuse, ApprovedBy: "approver", ApprovedAt: &now,
			ClosedBy: "closer", ClosedAt: &now, AccountSnapshot: json.RawMessage(`{"account":{"id":"account-1"}}`), Provenance: json.RawMessage(`{"snapshot":"v7"}`), CreatedAt: now},
		Account: billing.Account{ID: "account-1", CreatedAt: now},
		Party:   &billing.Party{ID: "party-1", Kind: "customer", Name: "Customer", Status: "active", CreatedAt: now},
		Ports:   []billing.AccountPort{{PortID: "port-1", DeviceID: "device-1", IfIndex: 7, IfName: "xe-0/0/7", Direction: billing.DirectionIn}},
		Values: []billing.Value{{ID: "value-1", PeriodID: "period-1", CalculationVersion: 3, Layer: billing.LayerCustomer,
			Algorithm: billing.Algorithm95th, Unit: "bps", AlgorithmValue: 900, Rate95thBPS: 900, Coverage: .95,
			ExpectedBuckets: 20, ObservedBuckets: 19, MissingBuckets: 1, GapBuckets: 1, SourceGenerationMin: 7, SourceGenerationMax: 9,
			Provenance: json.RawMessage(`{"geo":"v9"}`), CreatedAt: now}},
		Adjustments: []billing.Adjustment{{ID: "adjustment-1", PeriodID: "period-1", Layer: billing.LayerCustomer, Unit: "bps",
			Amount: 100, Status: "approved", Reason: "=unsafe", EvidenceRef: "ticket-1", CreatedAt: now}},
		Reconciliations: []billing.ReconciliationRun{{ID: "run-1", PeriodID: "period-1", CalculationVersion: 3, Status: "issues", ThresholdAbs: 20, ThresholdPercent: 5, Summary: json.RawMessage(`{"issues":1}`), CreatedAt: now}},
		Issues: []billing.ReconciliationIssue{{ID: "issue-1", LeftLayer: billing.LayerCustomer, RightLayer: billing.LayerExternal, Severity: "warning", Status: "acknowledged", Kind: "missing_bucket",
			Metric: "buckets", Detail: json.RawMessage(`{"automatic_adjustment":false}`), CreatedAt: now}},
	}
	rows := billingEvidenceRows(evidence)
	if len(rows) != 8 || rows[4].LedgerEffectiveValue != 1000 || rows[4].SourceGenerationMin != 7 || rows[4].SourceGenerationMax != 9 ||
		rows[3].DeviceID != "device-1" || rows[3].IfIndex != 7 || rows[7].RightLayer != "external" || rows[7].Severity != "warning" {
		t.Fatalf("rows=%+v", rows)
	}
	csvData, err := renderBillingCSV(rows)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(csvData), "'\u003dunsafe") && !strings.Contains(string(csvData), "'=unsafe") {
		t.Fatalf("CSV formula prefix was not escaped: %s", csvData)
	}
	csvRows, err := csv.NewReader(bytes.NewReader(csvData)).ReadAll()
	if err != nil || len(csvRows) != len(rows)+1 || len(csvRows[0]) != len(billingEvidenceHeader) {
		t.Fatalf("CSV shape rows=%d header=%d err=%v", len(csvRows), len(csvRows[0]), err)
	}
	parquetData, err := renderBillingParquet(rows)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(parquetData, []byte("PAR1")) || !bytes.HasSuffix(parquetData, []byte("PAR1")) {
		t.Fatal("Parquet magic is missing")
	}
	decoded, err := parquet.Read[billingEvidenceRow](bytes.NewReader(parquetData), int64(len(parquetData)))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(rows) || decoded[4].ProvenanceJSON != `{"geo":"v9"}` || decoded[4].LedgerEffectiveValue != 1000 ||
		decoded[0].Timezone != "Asia/Singapore" || decoded[0].ClosedBy != "closer" || decoded[6].ThresholdPercent != 5 {
		t.Fatalf("decoded=%+v", decoded)
	}
}

func TestBillingWindowOffsetIsTerminal(t *testing.T) {
	if err := classifyBillingJobError(billing.ErrWindowOffset); !opjob.IsTerminalError(err) {
		t.Fatalf("window offset must not retry: %v", err)
	}
}
