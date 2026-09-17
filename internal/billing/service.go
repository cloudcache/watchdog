// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/snmpch"
)

type SNMPBillingReader interface {
	ReadBilling(context.Context, snmpch.BillingRequest) (snmpch.BillingResult, error)
}

type FlowBillingReader interface {
	ReadBilling(context.Context, flowch.FlowBillingRequest) (flowch.FlowBillingResult, error)
}

type Service struct {
	store *Store
	snmp  SNMPBillingReader
	flow  FlowBillingReader
	now   func() time.Time
}

func NewService(store *Store, snmp SNMPBillingReader, flow FlowBillingReader) (*Service, error) {
	if store == nil || store.db == nil {
		return nil, errors.New("billing MySQL store is required")
	}
	if snmp == nil || flow == nil {
		return nil, errors.New("SNMP and Flow ClickHouse readers are required")
	}
	return &Service{store: store, snmp: snmp, flow: flow, now: time.Now}, nil
}

type CalculationResult struct {
	Period Period                `json:"period"`
	Values []Value               `json:"values"`
	Issues []ReconciliationIssue `json:"issues"`
}

func (s *Service) Calculate(ctx context.Context, periodID string, expectedRowVersion uint64, actor string) (CalculationResult, error) {
	return s.calculate(ctx, periodID, expectedRowVersion, actor, "")
}

func (s *Service) CalculateForOperation(ctx context.Context, periodID string, expectedRowVersion uint64, actor, operationRef string) (CalculationResult, error) {
	if operationRef == "" {
		return CalculationResult{}, errors.New("calculation operation reference is required")
	}
	return s.calculate(ctx, periodID, expectedRowVersion, actor, operationRef)
}

func (s *Service) calculate(ctx context.Context, periodID string, expectedRowVersion uint64, actor, operationRef string) (CalculationResult, error) {
	if operationRef != "" {
		if run, runErr := s.store.GetRunByOperation(ctx, operationRef); runErr == nil {
			if run.PeriodID != periodID {
				return CalculationResult{}, ErrConflict
			}
			var summary struct {
				Reason string `json:"reason"`
			}
			if json.Unmarshal(run.Summary, &summary) == nil && summary.Reason == "window_offset" {
				return CalculationResult{}, ErrWindowOffset
			}
			evidence, err := s.store.ExportEvidence(ctx, periodID, run.CalculationVersion)
			if err != nil {
				return CalculationResult{}, err
			}
			return CalculationResult{Period: evidence.Period, Values: evidence.Values, Issues: evidence.Issues}, nil
		} else if !errors.Is(runErr, ErrNotFound) {
			return CalculationResult{}, runErr
		}
	}
	period, err := s.store.GetPeriod(ctx, periodID)
	if err != nil {
		return CalculationResult{}, err
	}
	if period.RowVersion != expectedRowVersion {
		return CalculationResult{}, ErrConflict
	}
	if period.Status == PeriodApproved || period.Status == PeriodClosed {
		return CalculationResult{}, ErrImmutable
	}
	ports, err := s.store.ListPeriodPorts(ctx, period.ID)
	if err != nil {
		return CalculationResult{}, err
	}
	if len(ports) == 0 {
		return CalculationResult{}, errors.New("billing account has no bound ports")
	}
	snmpPorts := make([]snmpch.BillingPort, 0, len(ports))
	flowPorts := make([]flowch.BillingPortScope, 0, len(ports))
	for _, port := range ports {
		snmpPorts = append(snmpPorts, snmpch.BillingPort{PortID: port.PortID, Direction: string(port.Direction)})
		flowPorts = append(flowPorts, flowch.BillingPortScope{DeviceID: port.DeviceID, IfIndex: port.IfIndex, Direction: string(port.Direction)})
	}
	now := s.now().UTC()
	type snmpOutcome struct {
		result snmpch.BillingResult
		err    error
	}
	type flowOutcome struct {
		result flowch.FlowBillingResult
		err    error
	}
	queryContext, cancel := context.WithCancel(ctx)
	defer cancel()
	snmpResult, flowResult := make(chan snmpOutcome, 1), make(chan flowOutcome, 1)
	go func() {
		result, readErr := s.snmp.ReadBilling(queryContext, snmpch.BillingRequest{Ports: snmpPorts, From: period.DateFrom, To: period.DateTo, Now: now})
		snmpResult <- snmpOutcome{result: result, err: readErr}
	}()
	go func() {
		result, readErr := s.flow.ReadBilling(queryContext, flowch.FlowBillingRequest{Ports: flowPorts, From: period.DateFrom, To: period.DateTo, Now: now})
		flowResult <- flowOutcome{result: result, err: readErr}
	}()
	snmpRead, flowRead := <-snmpResult, <-flowResult
	if snmpRead.err != nil || flowRead.err != nil {
		cancel()
		return CalculationResult{}, fmt.Errorf("%w: SNMP=%v Flow=%v", ErrEvidenceUnavailable, snmpRead.err, flowRead.err)
	}
	if !billingReadWindowsMatch(period, snmpRead.result, flowRead.result) {
		detail, _ := json.Marshal(map[string]any{
			"automatic_adjustment": false,
			"expected":             map[string]time.Time{"from": period.DateFrom, "to": period.DateTo},
			"snmp":                 map[string]time.Time{"from": snmpRead.result.From, "to": snmpRead.result.To},
			"flow":                 map[string]time.Time{"from": flowRead.result.From, "to": flowRead.result.To},
		})
		if _, recordErr := s.store.RecordWindowOffset(ctx, period.ID, period.RowVersion, actor, operationRef, detail); recordErr != nil {
			return CalculationResult{}, fmt.Errorf("%w: record issue: %v", ErrWindowOffset, recordErr)
		}
		return CalculationResult{}, ErrWindowOffset
	}
	rates, completeBuckets, err := commonCompleteBillingRates(period, snmpRead.result, flowRead.result)
	if err != nil {
		return CalculationResult{}, fmt.Errorf("%w: %v", ErrEvidenceUnavailable, err)
	}
	values := flowValues(period, flowRead.result, rates)
	values = append(values, snmpValue(period, snmpRead.result, rates[LayerSNMP]))
	external, err := s.store.ExternalDraft(ctx, period.ID)
	if err != nil {
		return CalculationResult{}, err
	}
	if external != nil {
		copy := *external
		copy.ID, copy.CalculationVersion, copy.Algorithm = "", 0, period.Algorithm
		values = append(values, copy)
	}
	issues := Reconcile(values, period.ReconcileAbs, period.ReconcilePercent)
	provenance, _ := json.Marshal(map[string]any{
		"period_row_version":  period.RowVersion,
		"account_snapshot":    json.RawMessage(period.AccountSnapshot),
		"port_snapshot":       ports,
		"dimension_refs":      flowRead.result.DimensionSnapshotRefs,
		"geo_refs":            flowRead.result.GeoVersionRefs,
		"classification_refs": flowRead.result.ClassificationVersions,
		"rate_policy":         "common_complete_5m_intersection",
		"rate_bucket_count":   completeBuckets,
		"generated_at":        now,
	})
	publicationRef, err := publicationReferences(flowRead.result)
	if err != nil {
		return CalculationResult{}, err
	}
	updated, err := s.store.CommitCalculation(ctx, CalculationCommit{
		PeriodID: period.ID, ExpectedRowVersion: period.RowVersion,
		PublicationRef: publicationRef, Provenance: provenance, Values: values, Issues: issues,
		ThresholdAbs: period.ReconcileAbs, ThresholdPercent: period.ReconcilePercent, Actor: actor, OperationRef: operationRef,
	})
	if err != nil {
		return CalculationResult{}, err
	}
	committedValues, err := s.store.ListValues(ctx, period.ID, updated.CalculationVersion)
	if err != nil {
		return CalculationResult{}, err
	}
	committedIssues, _, err := s.store.ListIssues(ctx, period.ID, updated.CalculationVersion, PageFilter{Limit: 500, Sort: "created_at"})
	if err != nil {
		return CalculationResult{}, err
	}
	return CalculationResult{Period: updated, Values: committedValues, Issues: committedIssues}, nil
}

func billingReadWindowsMatch(period Period, snmpResult snmpch.BillingResult, flowResult flowch.FlowBillingResult) bool {
	return snmpResult.From.Equal(period.DateFrom) && snmpResult.To.Equal(period.DateTo) &&
		flowResult.From.Equal(period.DateFrom) && flowResult.To.Equal(period.DateTo)
}

func flowValues(period Period, result flowch.FlowBillingResult, rates map[Layer][]RateSample) []Value {
	values := make([]Value, 0, len(result.Layers))
	for _, layer := range result.Layers {
		layerRates := rates[Layer(layer.Layer)]
		values = append(values, BuildValue(period.ID, 0, period.Algorithm, LayerStats{
			Layer: Layer(layer.Layer), InBytes: layer.InBytes, OutBytes: layer.OutBytes, SelectedBytes: layer.SelectedBytes,
			SelectedRates: rateValues(layerRates), RateSamples: layerRates, Timezone: period.Timezone,
			Coverage: layer.Coverage, ExpectedBuckets: layer.ExpectedBuckets,
			ObservedBuckets: layer.ObservedBuckets, UnknownSamplingRecords: layer.UnknownSamplingRecords,
			SourceGenerationMin: result.SourceGenerationMin, SourceGenerationMax: result.SourceGenerationMax,
			Provenance: map[string]any{
				"source": "flow_records", "dimension_refs": result.DimensionSnapshotRefs,
				"geo_refs": result.GeoVersionRefs, "classification_refs": result.ClassificationVersions,
				"rate_policy": "common_complete_5m_intersection", "rate_bucket_count": len(rates[Layer(layer.Layer)]),
			},
		}))
	}
	return values
}

func snmpValue(period Period, result snmpch.BillingResult, rates []RateSample) Value {
	return BuildValue(period.ID, 0, period.Algorithm, LayerStats{
		Layer: LayerSNMP, InBytes: result.TotalInBytes, OutBytes: result.TotalOutBytes, SelectedBytes: result.TotalSelectedBytes,
		SelectedRates: rateValues(rates), RateSamples: rates, Timezone: period.Timezone,
		Coverage: result.Coverage, ExpectedBuckets: result.ExpectedBuckets, ObservedBuckets: result.ObservedBuckets,
		ResetBuckets: result.ResetBuckets, GapBuckets: result.GapBuckets, SourceGenerationMin: result.GenerationMin, SourceGenerationMax: result.GenerationMax,
		Provenance: map[string]any{"source": "snmp_interface_traffic_5m", "gap_buckets": result.GapBuckets, "expected_ports": result.ExpectedPorts,
			"rate_policy": "common_complete_5m_intersection", "rate_bucket_count": len(rates)},
	})
}

func commonCompleteBillingRates(period Period, snmpResult snmpch.BillingResult, flowResult flowch.FlowBillingResult) (map[Layer][]RateSample, int, error) {
	expected := int(period.DateTo.Sub(period.DateFrom) / (5 * time.Minute))
	if expected <= 0 || snmpResult.ExpectedBuckets != uint32(expected) {
		return nil, 0, errors.New("SNMP bucket grid does not equal billing period")
	}
	snmpByTime := make(map[int64]snmpch.BillingBucket, len(snmpResult.Buckets))
	for index, bucket := range snmpResult.Buckets {
		when := bucket.Time.UTC()
		if when.IsZero() {
			when = period.DateFrom.Add(time.Duration(index) * 5 * time.Minute)
		}
		snmpByTime[when.Unix()] = bucket
	}
	flowByLayer := make(map[Layer]map[int64]flowch.BillingRateBucket, 3)
	for _, layer := range flowResult.Layers {
		name := Layer(layer.Layer)
		if !ValidFlowLayer(name) || layer.ExpectedBuckets != uint32(expected) {
			return nil, 0, errors.New("Flow bucket grid does not equal billing period")
		}
		items := make(map[int64]flowch.BillingRateBucket, expected)
		if len(layer.RateBuckets) == 0 {
			return nil, 0, errors.New("Flow rate evidence is not a complete timestamped grid")
		}
		for _, bucket := range layer.RateBuckets {
			items[bucket.Time.UTC().Unix()] = bucket
		}
		flowByLayer[name] = items
	}
	for _, layer := range []Layer{LayerRaw, LayerSupplier, LayerCustomer} {
		if len(flowByLayer[layer]) != expected {
			return nil, 0, errors.New("Flow rate evidence is missing grid buckets")
		}
	}
	rates := map[Layer][]RateSample{LayerRaw: {}, LayerSupplier: {}, LayerCustomer: {}, LayerSNMP: {}}
	for index := 0; index < expected; index++ {
		timestamp := period.DateFrom.Add(time.Duration(index) * 5 * time.Minute)
		when := timestamp.Unix()
		snmpBucket, ok := snmpByTime[when]
		if !ok {
			continue
		}
		expectedPorts := snmpResult.ExpectedPorts
		if expectedPorts == 0 {
			expectedPorts = 1
		}
		snmpComplete := snmpBucket.PresentPorts >= expectedPorts && !snmpBucket.Reset && !snmpBucket.Gap
		if snmpResult.ExpectedPorts > 0 {
			snmpComplete = snmpComplete && snmpBucket.Coverage >= 0.999999
		}
		raw, rawOK := flowByLayer[LayerRaw][when]
		supplier, supplierOK := flowByLayer[LayerSupplier][when]
		customer, customerOK := flowByLayer[LayerCustomer][when]
		if !snmpComplete || !rawOK || !supplierOK || !customerOK || !raw.Complete || !supplier.Complete || !customer.Complete {
			continue
		}
		rates[LayerRaw] = append(rates[LayerRaw], RateSample{Time: timestamp, Value: raw.SelectedBPS})
		rates[LayerSupplier] = append(rates[LayerSupplier], RateSample{Time: timestamp, Value: supplier.SelectedBPS})
		rates[LayerCustomer] = append(rates[LayerCustomer], RateSample{Time: timestamp, Value: customer.SelectedBPS})
		rates[LayerSNMP] = append(rates[LayerSNMP], RateSample{Time: timestamp, Value: snmpBucket.SelectedBPS})
	}
	return rates, len(rates[LayerRaw]), nil
}

func rateValues(samples []RateSample) []float64 {
	values := make([]float64, 0, len(samples))
	for _, sample := range samples {
		values = append(values, sample.Value)
	}
	return values
}

func publicationReferences(result flowch.FlowBillingResult) (string, error) {
	if len(result.DimensionSnapshotRefs)+len(result.GeoVersionRefs)+len(result.ClassificationVersions) > 10000 {
		return "", errors.New("billing publication reference budget exceeded")
	}
	value, err := json.Marshal(map[string]any{"dimension": result.DimensionSnapshotRefs, "geo": result.GeoVersionRefs, "classification": result.ClassificationVersions})
	if err != nil {
		return "", err
	}
	if len(value) > 8<<20 {
		return "", errors.New("billing publication reference byte budget exceeded")
	}
	return string(value), nil
}
