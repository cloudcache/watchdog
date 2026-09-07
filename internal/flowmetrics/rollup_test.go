// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowmetrics

import (
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

func TestRollupMetricsExposeFixedResolutionFailureAndRepairContract(t *testing.T) {
	now := time.Date(2026, 9, 5, 3, 0, 0, 0, time.UTC)
	metrics, err := NewRollup(func() flowch.RollupStats {
		return flowch.RollupStats{
			OneMinute: flowch.RollupResolutionStats{
				Attempts: 7, Successes: 5, RetryableErrors: 1, PermanentErrors: 1,
				InitialRebuilds: 4, RepairRebuilds: 1, LastSuccessUnix: uint64(now.Add(-time.Minute).Unix()),
				LatestCompletedBucketUnix: uint64(now.Add(-2 * time.Minute).Unix()),
			},
			TerminalPermanentFailures: 2,
			TerminalExhaustedFailures: 3,
			ReaperRepairsGap:          4,
			ReaperRepairsFailed:       5,
			ReaperRepairsLate:         6,
			PermanentGaps:             7,
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	metrics.now = func() time.Time { return now }
	body := string(metrics.PrometheusText())
	for _, expected := range []string{
		`watchdog_flow_rollup_attempts_total{resolution="1m"} 7`,
		`watchdog_flow_rollup_success_total{resolution="1m"} 5`,
		`watchdog_flow_rollup_errors_total{resolution="1m",class="retryable"} 1`,
		`watchdog_flow_rollup_errors_total{resolution="1m",class="permanent"} 1`,
		`watchdog_flow_rollup_rebuilds_total{resolution="1m",kind="initial"} 4`,
		`watchdog_flow_rollup_rebuilds_total{resolution="1m",kind="repair"} 1`,
		`watchdog_flow_rollup_completed_bucket_known{resolution="1m"} 1`,
		`watchdog_flow_rollup_completed_bucket_age_seconds{resolution="1m"} 120`,
		`watchdog_flow_rollup_completed_bucket_known{resolution="1h"} 0`,
		`watchdog_flow_rollup_completed_bucket_age_seconds{resolution="1h"} 0`,
		`watchdog_flow_rollup_terminal_failed_total{class="permanent"} 2`,
		`watchdog_flow_rollup_terminal_failed_total{class="exhausted"} 3`,
		`watchdog_flow_rollup_reaper_repairs_total{reason="gap"} 4`,
		`watchdog_flow_rollup_reaper_repairs_total{reason="failed"} 5`,
		`watchdog_flow_rollup_reaper_repairs_total{reason="late"} 6`,
		`watchdog_flow_rollup_permanent_gaps_total 7`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("rollup metrics missing %q:\n%s", expected, body)
		}
	}
	for _, forbidden := range []string{"tenant=", "bucket=", "job_id=", "generation="} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("rollup metrics contain forbidden high-cardinality label %q", forbidden)
		}
	}
}

func TestRollupMetricsRejectNilProviderAndClampFutureBucketAge(t *testing.T) {
	if _, err := NewRollup(nil); err == nil {
		t.Fatal("nil rollup stats provider was accepted")
	}
	now := time.Date(2026, 9, 5, 3, 0, 0, 0, time.UTC)
	metrics, _ := NewRollup(func() flowch.RollupStats {
		return flowch.RollupStats{OneHour: flowch.RollupResolutionStats{LatestCompletedBucketUnix: uint64(now.Add(time.Hour).Unix())}}
	})
	metrics.now = func() time.Time { return now }
	if body := string(metrics.PrometheusText()); !strings.Contains(body, `watchdog_flow_rollup_completed_bucket_age_seconds{resolution="1h"} 0`) {
		t.Fatalf("future age was not clamped:\n%s", body)
	}
}
