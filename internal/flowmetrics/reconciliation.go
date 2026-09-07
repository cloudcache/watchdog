// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowmetrics

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
)

var reconciliationReasons = []flowch.ReconciliationMismatchReason{
	flowch.MismatchMissingReceipt,
	flowch.MismatchMissingRecords,
	flowch.MismatchIdentity,
	flowch.MismatchCount,
	flowch.MismatchCounter,
}

// Reconciliation publishes one process-wide snapshot. Starting or failing a
// scan only marks it incomplete; mismatch values remain the last complete
// snapshot so a partial scan can never masquerade as zero loss.
type Reconciliation struct {
	mu          sync.RWMutex
	complete    bool
	lastSuccess time.Time
	mismatches  map[flowch.ReconciliationMismatchReason]uint64
}

func NewReconciliation() *Reconciliation {
	return &Reconciliation{mismatches: make(map[flowch.ReconciliationMismatchReason]uint64)}
}

func (m *Reconciliation) Begin() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.complete = false
	m.mu.Unlock()
}

func (m *Reconciliation) PublishComplete(at time.Time, mismatches map[flowch.ReconciliationMismatchReason]uint64) {
	if m == nil {
		return
	}
	snapshot := make(map[flowch.ReconciliationMismatchReason]uint64, len(reconciliationReasons))
	for _, reason := range reconciliationReasons {
		snapshot[reason] = mismatches[reason]
	}
	m.mu.Lock()
	m.mismatches = snapshot
	m.lastSuccess = at.UTC()
	m.complete = true
	m.mu.Unlock()
}

func (m *Reconciliation) PrometheusText() []byte {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	complete := m.complete
	lastSuccess := m.lastSuccess
	mismatches := make(map[flowch.ReconciliationMismatchReason]uint64, len(m.mismatches))
	for reason, count := range m.mismatches {
		mismatches[reason] = count
	}
	m.mu.RUnlock()

	samples := make([]sample, 0, len(reconciliationReasons))
	for _, reason := range reconciliationReasons {
		samples = append(samples, sample{labels: `{reason="` + string(reason) + `"}`, value: uintValue(mismatches[reason])})
	}
	var output bytes.Buffer
	writeFamily(&output, "watchdog_flow_ingest_reconciliation_mismatches", "Message windows with an ingest receipt/fact conservation mismatch in the last complete scan.", "gauge", samples...)
	last := "0"
	if !lastSuccess.IsZero() {
		last = strconv.FormatInt(lastSuccess.Unix(), 10)
	}
	writeFamily(&output, "watchdog_flow_ingest_reconciliation_last_success_timestamp_seconds", "Unix timestamp of the last complete Kafka-to-ClickHouse reconciliation scan.", "gauge", sample{value: last})
	completeValue := "0"
	if complete {
		completeValue = "1"
	}
	writeFamily(&output, "watchdog_flow_ingest_reconciliation_scan_complete", "Whether the latest attempted reconciliation reached every frozen committed offset.", "gauge", sample{value: completeValue})
	return output.Bytes()
}
