package watchdog

import (
	"testing"
	"time"
)

func TestVerifySampleCompletenessPassesCompleteRange(t *testing.T) {
	start := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	samples := []Sample{
		{Time: start, Value: 1},
		{Time: start.Add(5 * time.Minute), Value: 2},
		{Time: start.Add(10 * time.Minute), Value: 3},
	}
	report, err := VerifySampleCompleteness(samples, CompletenessPolicy{
		Start:           start,
		End:             start.Add(15 * time.Minute),
		CollectionStep:  5 * time.Minute,
		MaxMissingRatio: 0,
	})
	if err != nil {
		t.Fatalf("VerifySampleCompleteness() error = %v", err)
	}
	if report.ExpectedSamples != 3 || report.ActualSamples != 3 || report.MissingSamples != 0 {
		t.Fatalf("report = %#v, want 3/3/0", report)
	}
}

func TestVerifySampleCompletenessRejectsMissingSamples(t *testing.T) {
	start := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	samples := []Sample{
		{Time: start, Value: 1},
		{Time: start.Add(10 * time.Minute), Value: 3},
	}
	report, err := VerifySampleCompleteness(samples, CompletenessPolicy{
		Start:           start,
		End:             start.Add(15 * time.Minute),
		CollectionStep:  5 * time.Minute,
		MaxMissingRatio: 0,
	})
	if err == nil {
		t.Fatal("expected missing sample error")
	}
	if report.MissingSamples != 1 {
		t.Fatalf("MissingSamples = %d, want 1", report.MissingSamples)
	}
}

func TestVerifySampleCompletenessIgnoresOffStepSamples(t *testing.T) {
	start := time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC)
	samples := []Sample{
		{Time: start, Value: 1},
		{Time: start.Add(6 * time.Minute), Value: 2},
	}
	report, err := VerifySampleCompleteness(samples, CompletenessPolicy{
		Start:           start,
		End:             start.Add(10 * time.Minute),
		CollectionStep:  5 * time.Minute,
		MaxMissingRatio: 0.5,
	})
	if err != nil {
		t.Fatalf("VerifySampleCompleteness() error = %v", err)
	}
	// With slot-based coverage, both 0m and 6m fall in different 5m slots → 2 covered
	if report.ExpectedSamples != 2 || report.ActualSamples != 2 {
		t.Fatalf("report = %#v, want expected=2 actual=2", report)
	}
}
