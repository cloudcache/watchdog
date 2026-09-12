// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"strings"
	"testing"
	"time"
)

// TestVPNFindingsExportWhere builds the findings WHERE from a spec, uppercases the
// country, and rejects a filter column outside the allowlist.
func TestVPNFindingsExportWhere(t *testing.T) {
	from := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	where, args, err := vpnFindingsExportWhere(vpnFindingsExportSpec{
		From: from, To: from.Add(time.Hour), Search: "10.0",
		ColumnFilters: map[string][]string{"disposition": {"confirmed"}, "remote_country": {"us"}},
	})
	if err != nil {
		t.Fatalf("where: %v", err)
	}
	joined := strings.Join(where, " AND ")
	if !strings.Contains(joined, "window_end>=?") || !strings.Contains(joined, "window_end<?") {
		t.Fatalf("missing window clause: %s", joined)
	}
	if !strings.Contains(joined, "disposition IN (?)") || !strings.Contains(joined, "remote_country IN (?)") {
		t.Fatalf("missing filter clause: %s", joined)
	}
	upper := false
	for _, arg := range args {
		if arg == "US" {
			upper = true
		}
	}
	if !upper {
		t.Fatalf("remote_country not uppercased: %v", args)
	}

	if _, _, err := vpnFindingsExportWhere(vpnFindingsExportSpec{ColumnFilters: map[string][]string{"evil": {"x"}}}); err == nil {
		t.Fatalf("expected error for disallowed filter column")
	}
	if _, _, err := vpnFindingsExportWhere(vpnFindingsExportSpec{From: from, To: from}); err == nil {
		t.Fatalf("expected error for empty range interval")
	}
}

// TestRenderVPNFindingsCSV emits the findings header, a row, and neutralizes CSV
// formula injection on a string column.
func TestRenderVPNFindingsCSV(t *testing.T) {
	at := time.Date(2026, 9, 10, 1, 0, 0, 0, time.UTC)
	items := []vpnFindingDTO{{
		ID: "f1", LocalIP: "10.0.0.1", RemoteIP: "8.8.8.8", RiskLevel: "high", Verdict: "review",
		RemoteCountry: "=US", Score: 80, LocalToRemoteBytes: 100, RemoteToLocalBytes: 900,
		WindowStart: at, WindowEnd: at.Add(time.Minute), GeneratedAt: at, Disposition: "unreviewed", ProbeStatus: "not_requested",
	}}
	data, err := renderVPNFindingsCSV(items)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	out := string(data)
	if !strings.HasPrefix(out, strings.Join(vpnFindingsExportHeader, ",")+"\n") {
		t.Fatalf("header = %q", strings.SplitN(out, "\n", 2)[0])
	}
	if !strings.Contains(out, "\nf1,") {
		t.Fatalf("row missing: %q", out)
	}
	if !strings.Contains(out, "'=US") {
		t.Fatalf("formula not neutralized: %q", out)
	}
}

// TestRenderVPNFindingsParquet renders a non-empty parquet artifact.
func TestRenderVPNFindingsParquet(t *testing.T) {
	at := time.Date(2026, 9, 10, 1, 0, 0, 0, time.UTC)
	items := []vpnFindingDTO{
		{ID: "f1", LocalIP: "10.0.0.1", RiskLevel: "high", WindowStart: at, WindowEnd: at, GeneratedAt: at},
		{ID: "f2", LocalIP: "10.0.0.2", RiskLevel: "critical", WindowStart: at, WindowEnd: at, GeneratedAt: at},
	}
	data, err := renderVPNFindingsParquet(items)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if len(data) < 4 || string(data[:4]) != "PAR1" {
		t.Fatalf("not a parquet file: %d bytes", len(data))
	}
}
