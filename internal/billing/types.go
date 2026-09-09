// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Algorithm string

const (
	Algorithm95th    Algorithm = "95th"
	AlgorithmAverage Algorithm = "average"
	AlgorithmTotal   Algorithm = "total"
)

type Layer string

const (
	LayerRaw      Layer = "raw"
	LayerSupplier Layer = "supplier"
	LayerCustomer Layer = "customer"
	LayerSNMP     Layer = "snmp"
	LayerExternal Layer = "external"
)

type Direction string

const (
	DirectionIn  Direction = "in"
	DirectionOut Direction = "out"
	DirectionAgg Direction = "agg"
)

type PeriodStatus string

const (
	PeriodOpen       PeriodStatus = "open"
	PeriodCalculated PeriodStatus = "calculated"
	PeriodApproved   PeriodStatus = "approved"
	PeriodClosed     PeriodStatus = "closed"
)

type Party struct {
	ID         string    `json:"id"`
	Kind       string    `json:"kind"`
	Status     string    `json:"status"`
	Name       string    `json:"name"`
	Ref        string    `json:"ref"`
	Notes      string    `json:"notes"`
	RowVersion uint64    `json:"row_version"`
	CreatedBy  string    `json:"created_by,omitempty"`
	UpdatedBy  string    `json:"updated_by,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Account struct {
	ID               string    `json:"id"`
	PartyID          string    `json:"party_id,omitempty"`
	Name             string    `json:"name"`
	Status           string    `json:"status"`
	BillType         string    `json:"bill_type"`
	Algorithm        Algorithm `json:"algorithm"`
	BillingDay       uint8     `json:"billing_day"`
	Timezone         string    `json:"timezone"`
	Direction        Direction `json:"direction"`
	DefaultLayer     Layer     `json:"default_layer"`
	CDRBPS           *uint64   `json:"cdr_bps,omitempty"`
	QuotaBytes       *uint64   `json:"quota_bytes,omitempty"`
	ReconcileAbs     uint64    `json:"reconcile_abs"`
	ReconcilePercent float64   `json:"reconcile_percent"`
	Ref              string    `json:"ref"`
	Notes            string    `json:"notes"`
	RowVersion       uint64    `json:"row_version"`
	CreatedBy        string    `json:"created_by,omitempty"`
	UpdatedBy        string    `json:"updated_by,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type AccountPort struct {
	AccountID string    `json:"account_id"`
	PortID    string    `json:"port_id"`
	DeviceID  string    `json:"device_id,omitempty"`
	IfIndex   uint32    `json:"if_index,omitempty"`
	IfName    string    `json:"if_name,omitempty"`
	Direction Direction `json:"direction"`
	CreatedBy string    `json:"created_by,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type Period struct {
	ID                         string          `json:"id"`
	AccountID                  string          `json:"account_id"`
	DateFrom                   time.Time       `json:"date_from"`
	DateTo                     time.Time       `json:"date_to"`
	Timezone                   string          `json:"timezone"`
	Direction                  Direction       `json:"direction"`
	Status                     PeriodStatus    `json:"status"`
	Algorithm                  Algorithm       `json:"algorithm"`
	DefaultLayer               Layer           `json:"default_layer"`
	ReconcileAbs               uint64          `json:"reconcile_abs"`
	ReconcilePercent           float64         `json:"reconcile_percent"`
	Allowed                    *uint64         `json:"allowed,omitempty"`
	Used                       *uint64         `json:"used,omitempty"`
	Overuse                    *uint64         `json:"overuse,omitempty"`
	CalculationVersion         uint64          `json:"calculation_version"`
	PublicationRef             string          `json:"publication_ref"`
	AccountSnapshot            json.RawMessage `json:"account_snapshot"`
	AdjustmentSnapshot         json.RawMessage `json:"adjustment_snapshot"`
	Provenance                 json.RawMessage `json:"provenance"`
	ApprovedCalculationVersion *uint64         `json:"approved_calculation_version,omitempty"`
	ApprovedBy                 string          `json:"approved_by,omitempty"`
	ApprovedAt                 *time.Time      `json:"approved_at,omitempty"`
	ClosedBy                   string          `json:"closed_by,omitempty"`
	ClosedAt                   *time.Time      `json:"closed_at,omitempty"`
	RowVersion                 uint64          `json:"row_version"`
	CreatedBy                  string          `json:"created_by,omitempty"`
	CreatedAt                  time.Time       `json:"created_at"`
	UpdatedAt                  time.Time       `json:"updated_at"`
}

type PeriodAccountSnapshot struct {
	Account Account `json:"account"`
	Party   *Party  `json:"party,omitempty"`
}

type Value struct {
	ID                     string          `json:"id"`
	PeriodID               string          `json:"period_id"`
	CalculationVersion     uint64          `json:"calculation_version"`
	Layer                  Layer           `json:"layer"`
	Algorithm              Algorithm       `json:"algorithm"`
	Unit                   string          `json:"unit"`
	InBytes                uint64          `json:"in_bytes"`
	OutBytes               uint64          `json:"out_bytes"`
	SelectedBytes          uint64          `json:"selected_bytes"`
	Rate95thBPS            uint64          `json:"rate_95th_bps"`
	RateAverageBPS         uint64          `json:"rate_average_bps"`
	AlgorithmValue         uint64          `json:"algorithm_value"`
	Coverage               float64         `json:"coverage"`
	ExpectedBuckets        uint32          `json:"expected_buckets"`
	ObservedBuckets        uint32          `json:"observed_buckets"`
	MissingBuckets         uint32          `json:"missing_buckets"`
	ResetBuckets           uint32          `json:"reset_buckets"`
	GapBuckets             uint32          `json:"gap_buckets"`
	UnknownSamplingRecords uint64          `json:"unknown_sampling_records"`
	SourceGenerationMin    uint64          `json:"source_generation_min"`
	SourceGenerationMax    uint64          `json:"source_generation_max"`
	Provenance             json.RawMessage `json:"provenance"`
	CreatedAt              time.Time       `json:"created_at"`
}

type Adjustment struct {
	ID                   string     `json:"id"`
	PeriodID             string     `json:"period_id"`
	Layer                Layer      `json:"layer"`
	Unit                 string     `json:"unit"`
	Amount               int64      `json:"amount"`
	Reason               string     `json:"reason"`
	EvidenceRef          string     `json:"evidence_ref"`
	Status               string     `json:"status"`
	ReversesAdjustmentID string     `json:"reverses_adjustment_id,omitempty"`
	RowVersion           uint64     `json:"row_version"`
	CreatedBy            string     `json:"created_by,omitempty"`
	ApprovedBy           string     `json:"approved_by,omitempty"`
	ApprovedAt           *time.Time `json:"approved_at,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
}

type ReconciliationRun struct {
	ID                 string          `json:"id"`
	OperationRef       string          `json:"operation_ref"`
	PeriodID           string          `json:"period_id"`
	CalculationVersion uint64          `json:"calculation_version"`
	Status             string          `json:"status"`
	ThresholdAbs       uint64          `json:"threshold_abs"`
	ThresholdPercent   float64         `json:"threshold_percent"`
	IssueCount         uint32          `json:"issue_count"`
	Summary            json.RawMessage `json:"summary"`
	CreatedBy          string          `json:"created_by,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
}

type ReconciliationIssue struct {
	ID             string          `json:"id"`
	RunID          string          `json:"run_id"`
	Kind           string          `json:"kind"`
	Severity       string          `json:"severity"`
	LeftLayer      Layer           `json:"left_layer,omitempty"`
	RightLayer     Layer           `json:"right_layer,omitempty"`
	Metric         string          `json:"metric,omitempty"`
	ExpectedValue  int64           `json:"expected_value"`
	ActualValue    int64           `json:"actual_value"`
	DeltaValue     int64           `json:"delta_value"`
	ThresholdValue uint64          `json:"threshold_value"`
	Status         string          `json:"status"`
	Detail         json.RawMessage `json:"detail"`
	ResolutionNote string          `json:"resolution_note"`
	ResolvedBy     string          `json:"resolved_by,omitempty"`
	ResolvedAt     *time.Time      `json:"resolved_at,omitempty"`
	RowVersion     uint64          `json:"row_version"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

func ValidateAccount(account Account) error {
	account.Name = strings.TrimSpace(account.Name)
	if account.Name == "" {
		return errors.New("billing account name is required")
	}
	if account.Status != "active" && account.Status != "paused" {
		return errors.New("billing account status must be active or paused")
	}
	if account.BillType != "cdr" && account.BillType != "quota" {
		return errors.New("billing account type must be cdr or quota")
	}
	if !ValidAlgorithm(account.Algorithm) || !ValidDirection(account.Direction) || !ValidFlowLayer(account.DefaultLayer) {
		return errors.New("billing account algorithm, direction or default layer is invalid")
	}
	if account.BillingDay < 1 || account.BillingDay > 31 {
		return errors.New("billing day must be 1..31")
	}
	if account.ReconcilePercent < 0 || account.ReconcilePercent > 100 {
		return errors.New("reconciliation percent must be 0..100")
	}
	if _, err := time.LoadLocation(account.Timezone); err != nil {
		return fmt.Errorf("invalid IANA timezone: %w", err)
	}
	if account.BillType == "cdr" && account.CDRBPS == nil {
		return errors.New("cdr_bps is required for cdr accounts")
	}
	if account.BillType == "cdr" && account.Algorithm == AlgorithmTotal {
		return errors.New("cdr accounts require the 95th or average rate algorithm")
	}
	if account.BillType == "quota" && account.QuotaBytes == nil {
		return errors.New("quota_bytes is required for quota accounts")
	}
	if account.BillType == "quota" && account.Algorithm != AlgorithmTotal {
		return errors.New("quota accounts require the total bytes algorithm")
	}
	return nil
}

func ValidatePeriodWindow(from, to time.Time, timezone string, now time.Time) error {
	if _, err := time.LoadLocation(timezone); err != nil {
		return fmt.Errorf("invalid IANA timezone: %w", err)
	}
	from, to = from.UTC(), to.UTC()
	if !to.After(from) || to.Sub(from) > 400*24*time.Hour {
		return errors.New("billing period must be a positive interval of at most 400 days")
	}
	if !from.Equal(from.Truncate(5*time.Minute)) || !to.Equal(to.Truncate(5*time.Minute)) {
		return errors.New("billing period boundaries must align to UTC five-minute buckets")
	}
	if !now.IsZero() && to.After(now.UTC().Truncate(5*time.Minute)) {
		return errors.New("billing period must contain only closed five-minute buckets")
	}
	return nil
}

func ValidAlgorithm(value Algorithm) bool {
	return value == Algorithm95th || value == AlgorithmAverage || value == AlgorithmTotal
}

func ValidDirection(value Direction) bool {
	return value == DirectionIn || value == DirectionOut || value == DirectionAgg
}

func ValidFlowLayer(value Layer) bool {
	return value == LayerRaw || value == LayerSupplier || value == LayerCustomer
}

func ValidLayer(value Layer) bool {
	return ValidFlowLayer(value) || value == LayerSNMP || value == LayerExternal
}
