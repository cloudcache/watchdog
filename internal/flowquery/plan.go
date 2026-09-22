// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowquery

import (
	"fmt"
	"time"
)

const (
	DefaultTargetPoints = uint16(300)
	MinTargetPoints     = uint16(5)
	MaxTargetPoints     = uint16(2000)
)

// AggregatePlan separates the user-visible time range and graph density from
// the physical rollup table. EffectiveFrom/EffectiveTo are the closed source
// buckets that can actually be queried.
type AggregatePlan struct {
	RequestedFrom time.Time     `json:"requested_from"`
	RequestedTo   time.Time     `json:"requested_to"`
	EffectiveFrom time.Time     `json:"effective_from"`
	EffectiveTo   time.Time     `json:"effective_to"`
	Source        Bucket        `json:"source"`
	SourceStep    time.Duration `json:"-"`
	Interval      time.Duration `json:"-"`
	SourceSeconds uint32        `json:"source_seconds"`
	StepSeconds   uint32        `json:"step_seconds"`
	TargetPoints  uint16        `json:"target_points"`
	// Approximate marks a plan whose top-N ranking came from the approximate
	// topKWeighted candidate path; it is populated by callers that run that path.
	Approximate bool `json:"approximate,omitempty"`
}

var niceIntervals = []time.Duration{
	time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
	time.Hour,
	2 * time.Hour,
	3 * time.Hour,
	6 * time.Hour,
	12 * time.Hour,
	24 * time.Hour,
	2 * 24 * time.Hour,
	7 * 24 * time.Hour,
	30 * 24 * time.Hour,
}

// PlanAggregate implements the same mature planning split used by flow
// explorers such as Akvorado: start/end describe the business window, points
// describes display density, and the server independently selects a physical
// rollup table. requestedStep is an optional explicit display interval; zero
// asks the planner to choose a stable, human-friendly interval.
func PlanAggregate(from, to time.Time, requestedStep time.Duration, targetPoints uint16, now time.Time) (AggregatePlan, error) {
	if from.IsZero() || to.IsZero() || !to.After(from) {
		return AggregatePlan{}, requestError("from/to", ErrorInvalid, "from and to must define a non-empty range")
	}
	from, to, now = from.UTC(), to.UTC(), now.UTC()
	if to.After(now) {
		return AggregatePlan{}, requestError("to", ErrorIncompleteRange, "to cannot be in the future")
	}
	if targetPoints == 0 {
		targetPoints = DefaultTargetPoints
	}
	if targetPoints < MinTargetPoints || targetPoints > MaxTargetPoints {
		return AggregatePlan{}, requestError("target_points", ErrorLimitExceeded, fmt.Sprintf("target_points must be %d..%d", MinTargetPoints, MaxTargetPoints))
	}

	interval := requestedStep
	if interval == 0 {
		minimum := time.Duration((to.Sub(from) + time.Duration(targetPoints) - 1) / time.Duration(targetPoints))
		interval = chooseNiceInterval(minimum)
		// A full-day dashboard over current raw facts must not fan out into
		// minute source buckets. Keep explicit caller-selected steps intact,
		// but make the automatic report plan use the hourly path at 24h and
		// above. This bounds grouping cardinality while preserving the exact
		// requested business window.
		if to.Sub(from) >= 24*time.Hour {
			interval = max(interval, time.Hour)
		}
	} else if interval < time.Minute || interval%time.Minute != 0 || interval > 30*24*time.Hour {
		return AggregatePlan{}, requestError("step_seconds", ErrorInvalid, "step_seconds must be a whole number of minutes from 60 seconds through 30 days")
	}

	source, sourceStep, maxSourcePoints := BucketOneMinute, time.Minute, 10_080
	minuteSourcePoints := int((to.Sub(from) + time.Minute - 1) / time.Minute)
	if interval >= 24*time.Hour {
		source, sourceStep, maxSourcePoints = BucketOneDay, 24*time.Hour, 400
	} else if interval >= time.Hour {
		source, sourceStep, maxSourcePoints = BucketOneHour, time.Hour, 9_600
	} else if minuteSourcePoints > maxSourcePoints {
		if requestedStep != 0 {
			return AggregatePlan{}, requestError("from/to", ErrorLimitExceeded, "the requested step requires more than 10,080 minute source buckets; increase step_seconds or shorten the range")
		}
		source, sourceStep, maxSourcePoints = BucketOneHour, time.Hour, 9_600
		interval = chooseNiceInterval(max(interval, sourceStep))
	}
	if interval < sourceStep || interval%sourceStep != 0 {
		return AggregatePlan{}, requestError("step_seconds", ErrorInvalid, "step_seconds must be a whole multiple of the selected source resolution")
	}

	effectiveFrom := from.Truncate(sourceStep)
	effectiveTo := to.Truncate(sourceStep)
	if !effectiveTo.After(effectiveFrom) {
		return AggregatePlan{}, requestError("from/to", ErrorInvalid, "range contains no closed Flow rollup bucket")
	}
	sourcePoints := int(effectiveTo.Sub(effectiveFrom) / sourceStep)
	if sourcePoints > maxSourcePoints {
		return AggregatePlan{}, requestError("from/to", ErrorLimitExceeded, fmt.Sprintf("range reads %d source buckets; maximum for %s is %d", sourcePoints, source, maxSourcePoints))
	}
	return AggregatePlan{
		RequestedFrom: from, RequestedTo: to, EffectiveFrom: effectiveFrom, EffectiveTo: effectiveTo,
		Source: source, SourceStep: sourceStep, Interval: interval,
		SourceSeconds: uint32(sourceStep / time.Second), StepSeconds: uint32(interval / time.Second),
		TargetPoints: targetPoints,
	}, nil
}

func chooseNiceInterval(minimum time.Duration) time.Duration {
	minimum = max(minimum, time.Minute)
	for _, interval := range niceIntervals {
		if interval >= minimum {
			return interval
		}
	}
	return niceIntervals[len(niceIntervals)-1]
}
