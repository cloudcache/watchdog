// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowmetrics

import (
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

func TestReconciliationKeepsLastCompleteSnapshotWhileScanning(t *testing.T) {
	metrics := NewReconciliation()
	at := time.Unix(1_800_000_000, 0)
	metrics.PublishComplete(at, map[flowch.ReconciliationMismatchReason]uint64{flowch.MismatchCount: 3})
	first := string(metrics.PrometheusText())
	for _, want := range []string{
		`watchdog_flow_ingest_reconciliation_mismatches{reason="count_mismatch"} 3`,
		`watchdog_flow_ingest_reconciliation_scan_complete 1`,
		`watchdog_flow_ingest_reconciliation_last_success_timestamp_seconds 1800000000`,
	} {
		if !strings.Contains(first, want) {
			t.Fatalf("metrics missing %q:\n%s", want, first)
		}
	}
	metrics.Begin()
	partial := string(metrics.PrometheusText())
	if !strings.Contains(partial, `watchdog_flow_ingest_reconciliation_mismatches{reason="count_mismatch"} 3`) ||
		!strings.Contains(partial, `watchdog_flow_ingest_reconciliation_scan_complete 0`) {
		t.Fatalf("partial scan published false zero or remained complete:\n%s", partial)
	}
}
