// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowmetrics

import (
	"bytes"
	"errors"
	"strconv"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

type Rollup struct {
	stats func() flowch.RollupStats
	now   func() time.Time
}

func NewRollup(stats func() flowch.RollupStats) (*Rollup, error) {
	if stats == nil {
		return nil, errors.New("rollup stats provider is required")
	}
	return &Rollup{stats: stats, now: time.Now}, nil
}

// PrometheusText renders only fixed resolution, class, and rebuild-kind
// labels. Tenant and bucket identities remain in ClickHouse/MySQL, never in
// the metrics series.
func (m *Rollup) PrometheusText() []byte {
	if m == nil || m.stats == nil {
		return nil
	}
	stats := m.stats()
	now := time.Now()
	if m.now != nil {
		now = m.now()
	}
	var output bytes.Buffer
	writeFamily(&output, "watchdog_flow_rollup_attempts_total", "ClickHouse rollup rebuild attempts.", "counter",
		rollupSample("1m", stats.OneMinute.Attempts), rollupSample("1h", stats.OneHour.Attempts))
	writeFamily(&output, "watchdog_flow_rollup_success_total", "ClickHouse rollup rebuilds completed successfully.", "counter",
		rollupSample("1m", stats.OneMinute.Successes), rollupSample("1h", stats.OneHour.Successes))
	writeFamily(&output, "watchdog_flow_rollup_errors_total", "ClickHouse rollup rebuild failures by retry class.", "counter",
		rollupClassSample("1m", "retryable", stats.OneMinute.RetryableErrors),
		rollupClassSample("1m", "permanent", stats.OneMinute.PermanentErrors),
		rollupClassSample("1h", "retryable", stats.OneHour.RetryableErrors),
		rollupClassSample("1h", "permanent", stats.OneHour.PermanentErrors))
	writeFamily(&output, "watchdog_flow_rollup_terminal_failed_total", "Rollup buckets abandoned with no successful generation; the scheduled watermark advanced past a permanent gap in the aggregates.", "counter",
		sample{labels: `{class="permanent"}`, value: uintValue(stats.TerminalPermanentFailures)},
		sample{labels: `{class="exhausted"}`, value: uintValue(stats.TerminalExhaustedFailures)})
	writeFamily(&output, "watchdog_flow_rollup_rebuilds_total", "Successful initial and repair rollup rebuilds.", "counter",
		rollupKindSample("1m", "initial", stats.OneMinute.InitialRebuilds),
		rollupKindSample("1m", "repair", stats.OneMinute.RepairRebuilds),
		rollupKindSample("1h", "initial", stats.OneHour.InitialRebuilds),
		rollupKindSample("1h", "repair", stats.OneHour.RepairRebuilds))
	writeFamily(&output, "watchdog_flow_rollup_last_success_timestamp_seconds", "Unix timestamp of the latest successful rebuild in this process.", "gauge",
		rollupSample("1m", stats.OneMinute.LastSuccessUnix), rollupSample("1h", stats.OneHour.LastSuccessUnix))
	oneMinuteKnown, oneMinuteAge := rollupAge(now, stats.OneMinute.LatestCompletedBucketUnix)
	oneHourKnown, oneHourAge := rollupAge(now, stats.OneHour.LatestCompletedBucketUnix)
	writeFamily(&output, "watchdog_flow_rollup_completed_bucket_known", "Whether this process has completed a bucket for the resolution.", "gauge",
		rollupSample("1m", oneMinuteKnown), rollupSample("1h", oneHourKnown))
	writeFamily(&output, "watchdog_flow_rollup_completed_bucket_age_seconds", "Age of the latest bucket end completed by this process; read only when completed_bucket_known is 1.", "gauge",
		sample{labels: `{resolution="1m"}`, value: strconv.FormatFloat(oneMinuteAge, 'g', -1, 64)},
		sample{labels: `{resolution="1h"}`, value: strconv.FormatFloat(oneHourAge, 'g', -1, 64)})
	return output.Bytes()
}

func rollupSample(resolution string, value uint64) sample {
	return sample{labels: `{resolution="` + resolution + `"}`, value: uintValue(value)}
}

func rollupClassSample(resolution, class string, value uint64) sample {
	return sample{labels: `{resolution="` + resolution + `",class="` + class + `"}`, value: uintValue(value)}
}

func rollupKindSample(resolution, kind string, value uint64) sample {
	return sample{labels: `{resolution="` + resolution + `",kind="` + kind + `"}`, value: uintValue(value)}
}

func rollupAge(now time.Time, completedBucketUnix uint64) (uint64, float64) {
	if completedBucketUnix == 0 {
		return 0, 0
	}
	age := now.UTC().Sub(time.Unix(int64(completedBucketUnix), 0).UTC()).Seconds()
	if age < 0 {
		age = 0
	}
	return 1, age
}
