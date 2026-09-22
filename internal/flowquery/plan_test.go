// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"testing"
	"time"
)

func TestPlanAggregateKeepsRangeIndependentFromStorageResolution(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 37, 45, 0, time.UTC)
	tests := []struct {
		name     string
		rangeFor time.Duration
		source   Bucket
		step     time.Duration
	}{
		{"five minutes", 5 * time.Minute, BucketOneMinute, time.Minute},
		{"one hour", time.Hour, BucketOneMinute, time.Minute},
		{"six hours", 6 * time.Hour, BucketOneMinute, 5 * time.Minute},
		{"one day", 24 * time.Hour, BucketOneHour, time.Hour},
		{"seven days", 7 * 24 * time.Hour, BucketOneHour, time.Hour},
		{"thirty days", 30 * 24 * time.Hour, BucketOneHour, 3 * time.Hour},
		{"one year", 365 * 24 * time.Hour, BucketOneDay, 2 * 24 * time.Hour},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := PlanAggregate(now.Add(-test.rangeFor), now, 0, 300, now)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Source != test.source || plan.Interval != test.step {
				t.Fatalf("plan=%+v, want source=%s step=%s", plan, test.source, test.step)
			}
			if plan.StepSeconds != uint32(test.step/time.Second) || plan.SourceSeconds != uint32(plan.SourceStep/time.Second) {
				t.Fatalf("serialized plan metadata=%+v", plan)
			}
		})
	}
}

func TestPlanAggregateHonorsExplicitDisplayStepWithoutTreatingItAsATableName(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	plan, err := PlanAggregate(now.Add(-24*time.Hour), now, 15*time.Minute, 200, now)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Source != BucketOneMinute || plan.SourceStep != time.Minute || plan.Interval != 15*time.Minute {
		t.Fatalf("plan=%+v", plan)
	}
}

func TestPlanAggregateUsesHourlySourceForAutomaticFullDayButPreservesExplicitStep(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	automatic, err := PlanAggregate(now.Add(-24*time.Hour), now, 0, 300, now)
	if err != nil {
		t.Fatal(err)
	}
	if automatic.Source != BucketOneHour || automatic.SourceStep != time.Hour || automatic.Interval != time.Hour {
		t.Fatalf("automatic plan=%+v", automatic)
	}
	explicit, err := PlanAggregate(now.Add(-24*time.Hour), now, 15*time.Minute, 300, now)
	if err != nil {
		t.Fatal(err)
	}
	if explicit.Source != BucketOneMinute || explicit.SourceStep != time.Minute || explicit.Interval != 15*time.Minute {
		t.Fatalf("explicit plan=%+v", explicit)
	}
}

func TestPlanAggregateRejectsUnsafeDensityOrRange(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		from  time.Time
		to    time.Time
		step  time.Duration
		point uint16
		field string
		code  ErrorCode
	}{
		{"future", now.Add(-time.Hour), now.Add(time.Minute), 0, 300, "to", ErrorIncompleteRange},
		{"too few points", now.Add(-time.Hour), now, 0, 4, "target_points", ErrorLimitExceeded},
		{"sub-minute", now.Add(-time.Hour), now, 30 * time.Second, 300, "step_seconds", ErrorInvalid},
		{"minute source overflow", now.Add(-30 * 24 * time.Hour), now, 5 * time.Minute, 300, "from/to", ErrorLimitExceeded},
		{"hour source overflow", now.Add(-401 * 24 * time.Hour), now, 0, 300, "from/to", ErrorLimitExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := PlanAggregate(test.from, test.to, test.step, test.point, now)
			if !IsRequestError(err, test.field, test.code) {
				t.Fatalf("error=%v, want %s/%s", err, test.field, test.code)
			}
		})
	}
}
