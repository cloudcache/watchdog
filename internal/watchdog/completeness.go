package watchdog

import (
	"errors"
	"time"
)

type CompletenessPolicy struct {
	Start           time.Time
	End             time.Time
	CollectionStep  time.Duration
	MaxMissingRatio float64
}

type CompletenessReport struct {
	ExpectedSamples int
	ActualSamples   int
	MissingSamples  int
	MissingRatio    float64
}

func VerifySampleCompleteness(samples []Sample, policy CompletenessPolicy) (CompletenessReport, error) {
	if !policy.End.After(policy.Start) {
		return CompletenessReport{}, errors.New("completeness range end must be after start")
	}
	if policy.CollectionStep <= 0 {
		return CompletenessReport{}, errors.New("collection step is required")
	}
	expected := expectedSampleCount(policy.Start, policy.End, policy.CollectionStep)
	seen := make(map[int64]struct{}, expected)
	for _, sample := range samples {
		if sample.Time.Before(policy.Start) || !sample.Time.Before(policy.End) {
			continue
		}
		offset := sample.Time.Sub(policy.Start)
		if offset < 0 {
			continue
		}
		slot := int64(offset / policy.CollectionStep)
		seen[slot] = struct{}{}
	}
	actual := len(seen)
	missing := expected - actual
	if missing < 0 {
		missing = 0
	}
	report := CompletenessReport{
		ExpectedSamples: expected,
		ActualSamples:   actual,
		MissingSamples:  missing,
	}
	if expected > 0 {
		report.MissingRatio = float64(missing) / float64(expected)
	}
	if report.MissingRatio > policy.MaxMissingRatio {
		return report, errors.New("sample completeness is below required threshold")
	}
	return report, nil
}

func expectedSampleCount(start, end time.Time, step time.Duration) int {
	if !end.After(start) || step <= 0 {
		return 0
	}
	duration := end.Sub(start)
	count := int(duration / step)
	if duration%step != 0 {
		count++
	}
	return count
}
