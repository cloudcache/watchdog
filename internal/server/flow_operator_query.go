// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// flow_operator_query.go pins an operator-scoped flow query to a single customer ISP
// identity and to the address dimension snapshots that were effective — and verified
// installed on every active flow worker — over the query's time range. It is the
// KISS re-derivation of the hub's operator-classification binding
// (internal/watchdog/flow_operator_query.go): the hub's flow_enrichment_publications
// timeline + per-worker ACK table were redesigned in KISS into
// dimension_snapshot_activations + dimension_snapshot_acks, and multi-tenancy was
// dropped, so the resolver is rebuilt against those tables rather than copied.
//
// classification_version is intentionally NOT pinned: KISS does not persist it in the
// control plane (it lives only in worker-delivered classification bundles), and the
// worker fails closed unless a bundle's DimensionSnapshotID equals the address
// snapshot id it is paired with — so pinning the snapshot id transitively pins the
// classification version. remote_isp_id (the flowquery `isp` field) is the customer
// ISP identity that matches an operator's flow_isp_id.

var (
	errFlowOperatorInvalid     = errors.New("flow operator query selection is invalid")
	errFlowOperatorUnavailable = errors.New("flow operator query classification is unavailable")
)

// flowOperatorQueryBinding is the immutable bridge between a management-plane operator
// and the customer ISP identity stored in Flow facts.
type flowOperatorQueryBinding struct {
	OperatorID           string
	FlowISPID            uint16
	DimensionSnapshotIDs []string
	ExpectedWorkers      uint32
}

// resolveFlowOperatorQueryBinding uses one repeatable-read snapshot so the operator
// identity, the effective dimension-snapshot timeline, the active worker set and the
// install ACK counts cannot be combined from different control-plane moments.
func resolveFlowOperatorQueryBinding(ctx context.Context, db *sql.DB, operatorID string, from, to time.Time) (flowOperatorQueryBinding, error) {
	if db == nil || ctx == nil || strings.TrimSpace(operatorID) == "" || from.IsZero() || to.IsZero() || !to.After(from) {
		return flowOperatorQueryBinding{}, errFlowOperatorInvalid
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return flowOperatorQueryBinding{}, err
	}
	defer tx.Rollback()

	var flowISPID uint16
	var enabled bool
	if err := tx.QueryRowContext(ctx,
		`SELECT flow_isp_id, enabled FROM isp_operators WHERE id = ?`, operatorID).Scan(&flowISPID, &enabled); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return flowOperatorQueryBinding{}, errFlowOperatorInvalid
		}
		return flowOperatorQueryBinding{}, err
	}
	if flowISPID == 0 || !enabled {
		return flowOperatorQueryBinding{}, errFlowOperatorInvalid
	}

	// The address-dimension activation timeline is append-only and event-time ordered;
	// a snapshot's window is [effective_from, next activation's effective_from). Select
	// the windows overlapping [from,to) — the same coverage predicate the hub used.
	rows, err := tx.QueryContext(ctx, `
		WITH activation_timeline AS (
			SELECT snapshot_id, effective_from,
			       LEAD(effective_from) OVER (ORDER BY effective_from) AS next_effective_from
			FROM dimension_snapshot_activations
			WHERE module_key = ? AND dimension_key = ?
		)
		SELECT snapshot_id, effective_from
		FROM activation_timeline
		WHERE effective_from < ?
		  AND (next_effective_from IS NULL OR next_effective_from > ?)
		ORDER BY effective_from
	`, address.AddressDimensionModuleKey, address.AddressDimensionKey, to.UTC(), from.UTC())
	if err != nil {
		return flowOperatorQueryBinding{}, err
	}
	type activationRow struct {
		snapshotID    string
		effectiveFrom time.Time
	}
	activations := make([]activationRow, 0, 4)
	for rows.Next() {
		var row activationRow
		if err := rows.Scan(&row.snapshotID, &row.effectiveFrom); err != nil {
			rows.Close()
			return flowOperatorQueryBinding{}, err
		}
		row.effectiveFrom = row.effectiveFrom.UTC()
		activations = append(activations, row)
	}
	if err := rows.Close(); err != nil {
		return flowOperatorQueryBinding{}, err
	}
	if err := rows.Err(); err != nil {
		return flowOperatorQueryBinding{}, err
	}
	// The earliest overlapping activation must start at or before `from`, else the
	// range's start is uncovered by any published enrichment.
	if len(activations) == 0 || activations[0].effectiveFrom.After(from.UTC()) {
		return flowOperatorQueryBinding{}, errFlowOperatorUnavailable
	}
	if len(activations) > 100 {
		return flowOperatorQueryBinding{}, fmt.Errorf("%w: query crosses more than 100 address dimension publications", errFlowOperatorUnavailable)
	}

	snapshotSeen := make(map[string]struct{}, len(activations))
	snapshotIDs := make([]string, 0, len(activations))
	for _, row := range activations {
		if _, ok := snapshotSeen[row.snapshotID]; ok {
			continue
		}
		snapshotSeen[row.snapshotID] = struct{}{}
		snapshotIDs = append(snapshotIDs, row.snapshotID)
	}

	var expectedWorkers uint32
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM agents WHERE kind = 'flow_worker' AND status = 'active'`).Scan(&expectedWorkers); err != nil {
		return flowOperatorQueryBinding{}, err
	}
	if expectedWorkers == 0 {
		return flowOperatorQueryBinding{}, errFlowOperatorUnavailable
	}

	// Per snapshot, count the active flow workers that have installed it. The flow
	// worker registers its agent id and reports ACKs under the same worker id
	// (cmd/watchdog-flow-worker: AgentID == WorkerID == --worker-id), so the join
	// dimension_snapshot_acks.worker_id = agents.id is exact.
	placeholders := make([]string, len(snapshotIDs))
	args := make([]any, len(snapshotIDs))
	for i, id := range snapshotIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	readyRows, err := tx.QueryContext(ctx, `
		SELECT acks.snapshot_id, COUNT(DISTINCT acks.worker_id)
		FROM dimension_snapshot_acks AS acks
		JOIN agents AS workers ON workers.id = acks.worker_id
		WHERE acks.snapshot_id IN (`+strings.Join(placeholders, ",")+`)
		  AND acks.state = 'installed'
		  AND workers.kind = 'flow_worker' AND workers.status = 'active'
		GROUP BY acks.snapshot_id
	`, args...)
	if err != nil {
		return flowOperatorQueryBinding{}, err
	}
	readyByID := make(map[string]uint32, len(snapshotIDs))
	for readyRows.Next() {
		var id string
		var ready uint32
		if err := readyRows.Scan(&id, &ready); err != nil {
			readyRows.Close()
			return flowOperatorQueryBinding{}, err
		}
		readyByID[id] = ready
	}
	if err := readyRows.Close(); err != nil {
		return flowOperatorQueryBinding{}, err
	}
	if err := readyRows.Err(); err != nil {
		return flowOperatorQueryBinding{}, err
	}
	for _, id := range snapshotIDs {
		if readyByID[id] != expectedWorkers {
			return flowOperatorQueryBinding{}, fmt.Errorf("%w: snapshot %s installed on %d of %d active flow workers", errFlowOperatorUnavailable, id, readyByID[id], expectedWorkers)
		}
	}
	if err := tx.Commit(); err != nil {
		return flowOperatorQueryBinding{}, err
	}
	sort.Strings(snapshotIDs)
	return flowOperatorQueryBinding{
		OperatorID: operatorID, FlowISPID: flowISPID,
		DimensionSnapshotIDs: snapshotIDs, ExpectedWorkers: expectedWorkers,
	}, nil
}

// flowFilterReferencesISP reports whether a typed filter tree already constrains the
// isp field — operator selection owns that predicate, so a client may not pre-set it.
func flowFilterReferencesISP(filter *flowquery.FilterExpression) bool {
	if filter == nil {
		return false
	}
	if filter.Op == flowquery.FilterPredicate {
		return filter.Field == "isp"
	}
	for i := range filter.Args {
		if flowFilterReferencesISP(&filter.Args[i]) {
			return true
		}
	}
	return false
}

// applyFlowOperatorSelection resolves the operator binding and injects its immutable
// constraints — an isp=<flow_isp_id> predicate plus the effective dimension-snapshot
// pin — into the query filters. Shared by /flow/query and /flow/reports/query. It
// returns false (after writing the failure response) when the selection cannot be
// honored, so the caller returns immediately.
func (s *Server) applyFlowOperatorSelection(c *gin.Context, sel *flowOperatorSelection, view flowquery.View, from, to time.Time, filters *flowquery.Filters, filter **flowquery.FilterExpression) bool {
	if view != flowquery.ViewCustomer {
		fail(c, http.StatusBadRequest, "invalid_request", "operator selection requires the customer value layer")
		return false
	}
	// The frontend sends only {operator_id}; a populated resolved selection would be
	// client-forged (KISS resolves the binding server-side in one request).
	if sel.SchemaVersion != 0 || sel.FlowISPID != 0 || len(sel.PublicationIDs) != 0 ||
		len(sel.DimensionSnapshotIDs) != 0 || len(sel.ClassificationVersions) != 0 {
		fail(c, http.StatusBadRequest, "invalid_request", "operator selection may only contain operator_id")
		return false
	}
	if len(filters.DimensionSnapshotIDs) != 0 || len(filters.ClassificationVersions) != 0 || flowFilterReferencesISP(*filter) {
		fail(c, http.StatusBadRequest, "invalid_request", "operator selection owns the ISP and enrichment-version constraints")
		return false
	}
	binding, err := resolveFlowOperatorQueryBinding(c.Request.Context(), s.db, sel.OperatorID, from, to)
	if err != nil {
		switch {
		case errors.Is(err, errFlowOperatorInvalid):
			fail(c, http.StatusBadRequest, "invalid_request", "flow operator selection is invalid")
		case errors.Is(err, errFlowOperatorUnavailable):
			fail(c, http.StatusServiceUnavailable, "flow_operator_unavailable", "flow operator classification is not installed on every active worker for the requested range")
		default:
			fail(c, http.StatusServiceUnavailable, "flow_operator_unavailable", "flow operator classification readiness is unavailable")
		}
		return false
	}
	if err := injectFlowOperatorConstraints(filters, filter, binding.DimensionSnapshotIDs, binding.FlowISPID); err != nil {
		writeFlowQueryError(c, err)
		return false
	}
	return true
}

// injectFlowOperatorConstraints pins the query filters to the operator's ISP identity
// (an isp=<flow_isp_id> predicate ANDed onto any existing typed filter) and to the
// effective dimension snapshots. Pure so the filter merge is unit-testable.
func injectFlowOperatorConstraints(filters *flowquery.Filters, filter **flowquery.FilterExpression, snapshotIDs []string, flowISPID uint16) error {
	filters.DimensionSnapshotIDs = append([]string(nil), snapshotIDs...)
	isp := flowquery.FilterExpression{
		Op: flowquery.FilterPredicate, Field: "isp", Operator: flowquery.FilterEqual,
		Values: []string{strconv.FormatUint(uint64(flowISPID), 10)},
	}
	if *filter == nil {
		*filter = &isp
		return nil
	}
	combined, err := flowquery.CanonicalFilter(flowquery.FilterExpression{
		Op: flowquery.FilterAnd, Args: []flowquery.FilterExpression{**filter, isp},
	})
	if err != nil {
		return err
	}
	*filter = &combined
	return nil
}
