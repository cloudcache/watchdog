package watchdog

import (
	"testing"
	"time"
)

func TestTransformVMRangeValuesCorrectedAppliesPolicy(t *testing.T) {
	response := testVMRangeResponse(100)
	policy := PortPolicy{Enabled: true, CorrectionDirection: CorrectionUp, CorrectionMin: 10, CorrectionMax: 10}
	transformed, err := TransformVMRangeValues(response, MetricValueCorrected, policy, fixedRand(0))
	if err != nil {
		t.Fatalf("TransformVMRangeValues() error = %v", err)
	}
	if transformed.Data.Result[0].Values[0].Value != 110 {
		t.Fatalf("value = %v, want 110", transformed.Data.Result[0].Values[0].Value)
	}
}

func TestTransformVMRangeValuesRawKeepsValue(t *testing.T) {
	response := testVMRangeResponse(100)
	policy := PortPolicy{Enabled: true, CorrectionDirection: CorrectionDown, CorrectionMin: 10, CorrectionMax: 10}
	transformed, err := TransformVMRangeValues(response, MetricValueRaw, policy, fixedRand(0))
	if err != nil {
		t.Fatalf("TransformVMRangeValues() error = %v", err)
	}
	if transformed.Data.Result[0].Values[0].Value != 100 {
		t.Fatalf("value = %v, want 100", transformed.Data.Result[0].Values[0].Value)
	}
}

func TestTransformVMRangeValuesBothReturnsRawAndCorrected(t *testing.T) {
	response := testVMRangeResponse(100)
	policy := PortPolicy{Enabled: true, CorrectionDirection: CorrectionUp, CorrectionMin: 10, CorrectionMax: 10}
	transformed, err := TransformVMRangeValues(response, MetricValueBoth, policy, fixedRand(0))
	if err != nil {
		t.Fatalf("TransformVMRangeValues() error = %v", err)
	}
	if len(transformed.Data.Result) != 2 {
		t.Fatalf("result len = %d, want 2", len(transformed.Data.Result))
	}
	if transformed.Data.Result[0].Metric["value_mode"] != "raw" || transformed.Data.Result[0].Values[0].Value != 100 {
		t.Fatalf("raw result = %#v", transformed.Data.Result[0])
	}
	if transformed.Data.Result[1].Metric["value_mode"] != "corrected" || transformed.Data.Result[1].Values[0].Value != 110 {
		t.Fatalf("corrected result = %#v", transformed.Data.Result[1])
	}
}

func testVMRangeResponse(value float64) VictoriaMetricsResponse {
	response := VictoriaMetricsResponse{Status: "success"}
	response.Data.Result = []VMRangeQueryItem{{
		Metric: map[string]string{"__name__": MetricSNMPIfInBps, "port_id": "port-a"},
		Values: []VMValue{{Time: time.Unix(100, 0), Value: value}},
	}}
	return response
}

func TestTransformVMRangeValuesCorrectedIsDeterministicAcrossReads(t *testing.T) {
	policy := PortPolicy{Enabled: true, CorrectionDirection: CorrectionUp, CorrectionMin: 1, CorrectionMax: 1000}
	first, err := TransformVMRangeValues(testVMRangeResponse(500), MetricValueCorrected, policy, nil)
	if err != nil {
		t.Fatalf("first transform error = %v", err)
	}
	second, err := TransformVMRangeValues(testVMRangeResponse(500), MetricValueCorrected, policy, nil)
	if err != nil {
		t.Fatalf("second transform error = %v", err)
	}
	if first.Data.Result[0].Values[0].Value != second.Data.Result[0].Values[0].Value {
		t.Fatalf("corrected value not reproducible: %v vs %v", first.Data.Result[0].Values[0].Value, second.Data.Result[0].Values[0].Value)
	}
	if first.Data.Result[0].Values[0].Value == 500 {
		t.Fatalf("correction was not applied")
	}
}
