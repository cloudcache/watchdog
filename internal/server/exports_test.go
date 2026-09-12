// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
)

// TestExportViewDispatch: the generic /api/v1/exports surface renders each job type
// in its own shape, so flow/vpn export jobs are no longer invisible behind the
// SNMP-only binding (the P6.2 contract fix). DB-free — the view helpers only decode
// the checkpoint and stat an (absent) artifact.
func TestExportViewDispatch(t *testing.T) {
	s := &Server{cfg: Config{SNMP: SNMPConfig{ExportRetention: time.Hour}}}

	snmpCheckpoint, err := opjob.EncodePayload(snmpCSVExportPayloadSchema, snmpCSVExportPayload{
		Metric: "if_in_bps", Aggregate: "sum", FromMS: 1, ToMS: 2, StepSeconds: 300,
	})
	if err != nil {
		t.Fatal(err)
	}
	snmpJob := opjob.Job{ID: "snmp-1", JobType: snmpCSVExportJobType, Status: opjob.StatusSucceeded, CheckpointJSON: snmpCheckpoint}
	switch view := s.exportView(snmpJob).(type) {
	case snmpExportResponse:
		if view.Metric != "if_in_bps" || view.ValueMode != "raw" || view.Status != exportStatus(opjob.StatusSucceeded) {
			t.Fatalf("snmp export view = %+v", view)
		}
	default:
		t.Fatalf("snmp job dispatched to %T, want snmpExportResponse", view)
	}

	flowCheckpoint, err := opjob.EncodePayload(flowExportPayloadSchema, flowExportPayload{
		Kind: flowExportKindReport, Format: "csv", View: flowquery.ViewCustomer,
	})
	if err != nil {
		t.Fatal(err)
	}
	flowJob := opjob.Job{ID: "flow-1", JobType: flowExportJobType, Status: opjob.StatusQueued, CheckpointJSON: flowCheckpoint}
	switch view := s.exportView(flowJob).(type) {
	case flowExportResponse:
		if view.Kind != flowExportKindReport || view.Format != "csv" || view.View != string(flowquery.ViewCustomer) {
			t.Fatalf("flow export view = %+v", view)
		}
	default:
		t.Fatalf("flow job dispatched to %T, want flowExportResponse", view)
	}

	// A non-export job type still renders a minimal envelope rather than panicking.
	other := s.exportView(opjob.Job{ID: "x-1", JobType: "device.refresh", Status: opjob.StatusRunning})
	if _, ok := other.(gin.H); !ok {
		t.Fatalf("unknown job dispatched to %T, want gin.H", other)
	}
}

// TestMergeExportPage: the union of per-type export jobs is ordered newest-first and
// sliced to the requested [offset, offset+limit) window (the top-K merge behind
// listExports), including the boundary cases.
func TestMergeExportPage(t *testing.T) {
	base := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	// Two types interleaved in time: snmp at t0/t2/t4, flow at t1/t3.
	jobs := []opjob.Job{
		{ID: "snmp-t0", JobType: snmpCSVExportJobType, CreatedAt: base},
		{ID: "flow-t3", JobType: flowExportJobType, CreatedAt: base.Add(3 * time.Minute)},
		{ID: "snmp-t4", JobType: snmpCSVExportJobType, CreatedAt: base.Add(4 * time.Minute)},
		{ID: "flow-t1", JobType: flowExportJobType, CreatedAt: base.Add(1 * time.Minute)},
		{ID: "snmp-t2", JobType: snmpCSVExportJobType, CreatedAt: base.Add(2 * time.Minute)},
	}
	wantOrder := []string{"snmp-t4", "flow-t3", "snmp-t2", "flow-t1", "snmp-t0"}

	ids := func(page []opjob.Job) []string {
		out := make([]string, len(page))
		for i, j := range page {
			out[i] = j.ID
		}
		return out
	}
	eq := func(got, want []string) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}

	if got := ids(mergeExportPage(append([]opjob.Job(nil), jobs...), 10, 0)); !eq(got, wantOrder) {
		t.Fatalf("full page order = %v, want %v", got, wantOrder)
	}
	if got := ids(mergeExportPage(append([]opjob.Job(nil), jobs...), 2, 1)); !eq(got, wantOrder[1:3]) {
		t.Fatalf("windowed page = %v, want %v", got, wantOrder[1:3])
	}
	if got := ids(mergeExportPage(append([]opjob.Job(nil), jobs...), 5, 3)); !eq(got, wantOrder[3:]) {
		t.Fatalf("tail page = %v, want %v", got, wantOrder[3:])
	}
	if got := mergeExportPage(append([]opjob.Job(nil), jobs...), 5, 99); len(got) != 0 {
		t.Fatalf("offset past end = %v, want empty", ids(got))
	}
}
