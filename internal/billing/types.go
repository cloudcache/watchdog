// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Algorithm string

const (
	Algorithm95th    Algorithm = "95th"
	AlgorithmDaily95 Algorithm = "daily_95th"
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

type MeasurementType string

const (
	MeasurementBandwidth MeasurementType = "bandwidth"
	MeasurementTraffic   MeasurementType = "traffic"
)

type BillingMethod string

const (
	BillingPackagePort    BillingMethod = "package_port"
	BillingMonthly95th    BillingMethod = "monthly_95th"
	BillingDaily95th      BillingMethod = "daily_95th"
	BillingMonthlyAverage BillingMethod = "monthly_average"
)

var (
	decimalPricePattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,13})(\.[0-9]{1,6})?$`)
	currencyPattern     = regexp.MustCompile(`^[A-Z]{3}$`)
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
	ID                   string          `json:"id"`
	PartyID              string          `json:"party_id,omitempty"`
	Name                 string          `json:"name"`
	Status               string          `json:"status"`
	MeasurementType      MeasurementType `json:"measurement_type"`
	BillingMethod        BillingMethod   `json:"billing_method"`
	Algorithm            Algorithm       `json:"algorithm"`
	BillingDay           uint8           `json:"billing_day"`
	Timezone             string          `json:"timezone"`
	Direction            Direction       `json:"direction"`
	DefaultLayer         Layer           `json:"default_layer"`
	PriceCurrency        string          `json:"price_currency"`
	UnitPrice            string          `json:"unit_price"`
	MinimumPercent       float64         `json:"minimum_percent"`
	ContractBandwidthBPS *uint64         `json:"contract_bandwidth_bps,omitempty"`
	TrafficAllowance     *uint64         `json:"traffic_allowance_bytes,omitempty"`
	ReconcileAbs         uint64          `json:"reconcile_abs"`
	ReconcilePercent     float64         `json:"reconcile_percent"`
	Ref                  string          `json:"ref"`
	Notes                string          `json:"notes"`
	RowVersion           uint64          `json:"row_version"`
	CreatedBy            string          `json:"created_by,omitempty"`
	UpdatedBy            string          `json:"updated_by,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

type AccountPort struct {
	AccountID   string    `json:"account_id"`
	PortID      string    `json:"port_id"`
	DeviceID    string    `json:"device_id,omitempty"`
	IfIndex     uint32    `json:"if_index,omitempty"`
	IfName      string    `json:"if_name,omitempty"`
	CapacityBPS uint64    `json:"capacity_bps,omitempty"`
	Direction   Direction `json:"direction"`
	CreatedBy   string    `json:"created_by,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
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
	Account       Account       `json:"account"`
	Party         *Party        `json:"party,omitempty"`
	CapacityCheck CapacityCheck `json:"capacity_check"`
}

type CapacityCheck struct {
	ContractBPS      uint64 `json:"contract_bps"`
	SelectedBPS      uint64 `json:"selected_bps"`
	DifferenceBPS    uint64 `json:"difference_bps"`
	KnownPortCount   int    `json:"known_port_count"`
	UnknownPortCount int    `json:"unknown_port_count"`
	Status           string `json:"status"`
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
	RateDaily95thBPS       uint64          `json:"rate_daily_95th_bps"`
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
	if account.MeasurementType != MeasurementBandwidth && account.MeasurementType != MeasurementTraffic {
		return errors.New("billing measurement type must be bandwidth or traffic")
	}
	if !ValidAlgorithm(account.Algorithm) || !ValidDirection(account.Direction) || !ValidFlowLayer(account.DefaultLayer) {
		return errors.New("billing account algorithm, direction or default layer is invalid")
	}
	if !ValidBillingMethod(account.BillingMethod) {
		return errors.New("billing method must be package_port, monthly_95th, daily_95th or monthly_average")
	}
	if !currencyPattern.MatchString(account.PriceCurrency) {
		return errors.New("billing price currency must be a three-letter uppercase code")
	}
	if !decimalPricePattern.MatchString(account.UnitPrice) {
		return errors.New("billing unit price must be a non-negative decimal with at most six fractional digits")
	}
	if account.Algorithm != algorithmFor(account.MeasurementType, account.BillingMethod) {
		return errors.New("billing algorithm does not match the measurement type and billing method")
	}
	if account.MeasurementType == MeasurementTraffic && account.BillingMethod != BillingPackagePort {
		return errors.New("traffic measurement currently requires package_port billing")
	}
	if account.MinimumPercent < 0 || account.MinimumPercent > 100 {
		return errors.New("billing minimum percent must be 0..100")
	}
	if account.BillingMethod == BillingPackagePort && account.MinimumPercent != 0 {
		return errors.New("package_port billing does not use a minimum percent")
	}
	if account.MeasurementType == MeasurementBandwidth && (account.ContractBandwidthBPS == nil || *account.ContractBandwidthBPS == 0) {
		return errors.New("contract_bandwidth_bps is required for bandwidth measurement")
	}
	if account.MeasurementType == MeasurementTraffic && account.ContractBandwidthBPS != nil {
		return errors.New("contract_bandwidth_bps is only valid for bandwidth measurement")
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
	if account.MeasurementType == MeasurementTraffic && account.TrafficAllowance == nil {
		return errors.New("traffic_allowance_bytes is required for traffic measurement")
	}
	return nil
}

func ValidatePeriodWindow(from, to time.Time, timezone string, now time.Time) error {
	return validatePeriodWindow(from, to, timezone, now, DefaultLimits().MaxPeriodDuration)
}

func validatePeriodWindow(from, to time.Time, timezone string, now time.Time, maxDuration time.Duration) error {
	if _, err := time.LoadLocation(timezone); err != nil {
		return fmt.Errorf("invalid IANA timezone: %w", err)
	}
	from, to = from.UTC(), to.UTC()
	if !to.After(from) || to.Sub(from) > maxDuration {
		return fmt.Errorf("billing period must be a positive interval of at most %s", maxDuration)
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
	return value == Algorithm95th || value == AlgorithmDaily95 || value == AlgorithmAverage || value == AlgorithmTotal
}

func ValidBillingMethod(value BillingMethod) bool {
	return value == BillingPackagePort || value == BillingMonthly95th || value == BillingDaily95th || value == BillingMonthlyAverage
}

func algorithmFor(measurement MeasurementType, method BillingMethod) Algorithm {
	if measurement == MeasurementTraffic {
		return AlgorithmTotal
	}
	switch method {
	case BillingMonthly95th:
		return Algorithm95th
	case BillingDaily95th:
		return AlgorithmDaily95
	default:
		return AlgorithmAverage
	}
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
