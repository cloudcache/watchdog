// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/gin-gonic/gin"
)

// flow_operator_query.go pins an operator-scoped Flow query to one customer ISP
// identity and to every immutable AddressSnap + classification publication pair
// effective over the requested range. A pair is queryable only after every active
// flow worker has reported an installed_at milestone for that exact publication.
// remote_isp_id (the flowquery `isp` field) is the customer ISP identity that
// matches an operator's global flow_isp_id.

var (
	errFlowOperatorInvalid     = errors.New("flow operator query selection is invalid")
	errFlowOperatorUnavailable = errors.New("flow operator query classification is unavailable")
)

// flowOperatorQueryBinding is the immutable bridge between a management-plane operator
// and the customer ISP identity stored in Flow facts.
type flowOperatorQueryBinding struct {
	OperatorID             string
	FlowISPID              uint16
	PublicationIDs         []string
	DimensionSnapshotIDs   []string
	ClassificationVersions []uint32
	ExpectedWorkers        uint32
}

// resolveFlowOperatorQueryBinding uses one repeatable-read snapshot so the operator
// identity, the effective pair timeline, the active worker set and the
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

	// The signed pair timeline is append-only and event-time ordered. A publication's
	// window is [effective_from, next publication's effective_from). Select every pair
	// whose window overlaps [from,to).
	rows, err := tx.QueryContext(ctx, `
		WITH publication_timeline AS (
			SELECT id, dimension_snapshot_id, classification_version, effective_from,
			       LEAD(effective_from) OVER (ORDER BY effective_from) AS next_effective_from
			FROM flow_enrichment_publications
		)
		SELECT id, dimension_snapshot_id, classification_version, effective_from
		FROM publication_timeline
		WHERE effective_from < ?
		  AND (next_effective_from IS NULL OR next_effective_from > ?)
		ORDER BY effective_from
	`, to.UTC(), from.UTC())
	if err != nil {
		return flowOperatorQueryBinding{}, err
	}
	type publicationRow struct {
		publicationID         string
		snapshotID            string
		classificationVersion uint32
		effectiveFrom         time.Time
	}
	publications := make([]publicationRow, 0, 4)
	for rows.Next() {
		var row publicationRow
		if err := rows.Scan(&row.publicationID, &row.snapshotID, &row.classificationVersion, &row.effectiveFrom); err != nil {
			rows.Close()
			return flowOperatorQueryBinding{}, err
		}
		row.effectiveFrom = row.effectiveFrom.UTC()
		publications = append(publications, row)
	}
	if err := rows.Close(); err != nil {
		return flowOperatorQueryBinding{}, err
	}
	if err := rows.Err(); err != nil {
		return flowOperatorQueryBinding{}, err
	}
	// The earliest overlapping publication must start at or before `from`, else the
	// range's start is uncovered by any published enrichment.
	if len(publications) == 0 || publications[0].effectiveFrom.After(from.UTC()) {
		return flowOperatorQueryBinding{}, errFlowOperatorUnavailable
	}
	if len(publications) > 100 {
		return flowOperatorQueryBinding{}, fmt.Errorf("%w: query crosses more than 100 enrichment publications", errFlowOperatorUnavailable)
	}

	publicationIDs := make([]string, 0, len(publications))
	snapshotSeen := make(map[string]struct{}, len(publications))
	snapshotIDs := make([]string, 0, len(publications))
	classificationSeen := make(map[uint32]struct{}, len(publications))
	classificationVersions := make([]uint32, 0, len(publications))
	for _, row := range publications {
		publicationIDs = append(publicationIDs, row.publicationID)
		if _, ok := snapshotSeen[row.snapshotID]; !ok {
			snapshotSeen[row.snapshotID] = struct{}{}
			snapshotIDs = append(snapshotIDs, row.snapshotID)
		}
		if _, ok := classificationSeen[row.classificationVersion]; !ok {
			classificationSeen[row.classificationVersion] = struct{}{}
			classificationVersions = append(classificationVersions, row.classificationVersion)
		}
	}

	var expectedWorkers uint32
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM agents WHERE kind = 'flow_worker' AND status = 'active'`).Scan(&expectedWorkers); err != nil {
		return flowOperatorQueryBinding{}, err
	}
	if expectedWorkers == 0 {
		return flowOperatorQueryBinding{}, errFlowOperatorUnavailable
	}

	// Per publication, count active flow workers that have ever installed the exact
	// pair. installed_at is an irreversible milestone: a later failed refresh may
	// update state/error fields but must not erase readiness for the installed pair.
	placeholders := make([]string, len(publicationIDs))
	args := make([]any, len(publicationIDs))
	for i, id := range publicationIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	readyRows, err := tx.QueryContext(ctx, `
		SELECT acks.publication_id, COUNT(DISTINCT acks.worker_id)
		FROM flow_enrichment_publication_acks AS acks
		JOIN agents AS workers ON workers.id = acks.worker_id
		WHERE acks.publication_id IN (`+strings.Join(placeholders, ",")+`)
		  AND acks.installed_at IS NOT NULL
		  AND workers.kind = 'flow_worker' AND workers.status = 'active'
		GROUP BY acks.publication_id
	`, args...)
	if err != nil {
		return flowOperatorQueryBinding{}, err
	}
	readyByID := make(map[string]uint32, len(publicationIDs))
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
	for _, id := range publicationIDs {
		if readyByID[id] != expectedWorkers {
			return flowOperatorQueryBinding{}, fmt.Errorf("%w: publication %s installed on %d of %d active flow workers", errFlowOperatorUnavailable, id, readyByID[id], expectedWorkers)
		}
	}
	if err := tx.Commit(); err != nil {
		return flowOperatorQueryBinding{}, err
	}
	sort.Strings(snapshotIDs)
	sort.Slice(classificationVersions, func(i, j int) bool { return classificationVersions[i] < classificationVersions[j] })
	return flowOperatorQueryBinding{
		OperatorID: operatorID, FlowISPID: flowISPID,
		PublicationIDs: publicationIDs, DimensionSnapshotIDs: snapshotIDs,
		ClassificationVersions: classificationVersions, ExpectedWorkers: expectedWorkers,
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
	if err := injectFlowOperatorConstraints(filters, filter, binding.DimensionSnapshotIDs, binding.ClassificationVersions, binding.FlowISPID); err != nil {
		writeFlowQueryError(c, err)
		return false
	}
	sel.SchemaVersion = flowEnrichmentPairSchemaVersion
	sel.FlowISPID = uint32(binding.FlowISPID)
	sel.PublicationIDs = append([]string(nil), binding.PublicationIDs...)
	sel.DimensionSnapshotIDs = append([]string(nil), binding.DimensionSnapshotIDs...)
	sel.ClassificationVersions = append([]uint32(nil), binding.ClassificationVersions...)
	return true
}

// injectFlowOperatorConstraints pins the query filters to the operator's ISP identity
// (an isp=<flow_isp_id> predicate ANDed onto any existing typed filter) and to the
// effective dimension snapshots and classification versions. Pure so the filter
// merge is unit-testable.
func injectFlowOperatorConstraints(filters *flowquery.Filters, filter **flowquery.FilterExpression, snapshotIDs []string, classificationVersions []uint32, flowISPID uint16) error {
	filters.DimensionSnapshotIDs = append([]string(nil), snapshotIDs...)
	filters.ClassificationVersions = append([]uint32(nil), classificationVersions...)
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

// auditFlowQuery records the resolved version provenance for operator-scoped
// queries. Non-operator queries keep the compact generic audit row.
func (s *Server) auditFlowQuery(ctx context.Context, actor, action, resource, resourceID string, selection *flowOperatorSelection) {
	if selection == nil || selection.SchemaVersion == 0 {
		s.audit(ctx, actor, action, resource, resourceID)
		return
	}
	detail, err := json.Marshal(map[string]any{
		"operator_selection": selection,
	})
	if err != nil {
		return
	}
	_, _ = s.db.ExecContext(ctx, `INSERT INTO audit_logs
		(id,actor_id,action,resource,resource_id,detail_json) VALUES (?,?,?,?,?,CAST(? AS JSON))`,
		newID(), actor, action, resource, resourceID, detail)
}
