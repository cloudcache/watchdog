// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package watchdog

import (
	"strings"
	"testing"
)

type staticFlowRollupMetrics string

func (m staticFlowRollupMetrics) PrometheusText() []byte { return []byte(m) }

func TestRuntimeMetricsComposesFlowRollupProvider(t *testing.T) {
	runtime := &BackendRuntime{flowRollupMetrics: staticFlowRollupMetrics("watchdog_flow_rollup_attempts_total 3\n")}
	body := string(runtime.RuntimeMetrics())
	if !strings.Contains(body, "watchdog_flow_rollup_attempts_total 3") || !strings.Contains(body, "watchdog_collector_principal_provider_enabled 0") {
		t.Fatalf("runtime metrics did not compose providers:\n%s", body)
	}
}
