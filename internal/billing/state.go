// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

type CalculationCommit struct {
	PeriodID           string
	ExpectedRowVersion uint64
	PublicationRef     string
	Provenance         json.RawMessage
	Values             []Value
	Issues             []ReconciliationIssue
	ThresholdAbs       uint64
	ThresholdPercent   float64
	Actor              string
	OperationRef       string
}

func (s *Store) CommitCalculation(ctx context.Context, input CalculationCommit) (Period, error) {
	if input.PeriodID == "" || input.ExpectedRowVersion == 0 || len(input.Provenance) == 0 || !json.Valid(input.Provenance) {
		return Period{}, errors.New("calculation identity, versions and provenance are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Period{}, err
	}
	defer tx.Rollback()
	if input.OperationRef != "" {
		var existingPeriodID string
		err := tx.QueryRowContext(ctx, `SELECT period_id FROM reconciliation_runs WHERE operation_ref=?`, input.OperationRef).Scan(&existingPeriodID)
		if err == nil {
			if existingPeriodID != input.PeriodID {
				return Period{}, ErrConflict
			}
			_ = tx.Rollback()
			return s.GetPeriod(ctx, input.PeriodID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Period{}, err
		}
	} else {
		input.OperationRef = NewID()
	}
	period, err := scanPeriod(tx.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=? FOR UPDATE`, input.PeriodID))
	if err != nil {
		return Period{}, err
	}
	if period.RowVersion != input.ExpectedRowVersion {
		return Period{}, ErrConflict
	}
	if period.Status == PeriodApproved || period.Status == PeriodClosed {
		return Period{}, ErrImmutable
	}
	generation := period.CalculationVersion + 1
	byLayer := make(map[Layer]Value, len(input.Values))
	for _, value := range input.Values {
		if _, duplicate := byLayer[value.Layer]; duplicate || !ValidLayer(value.Layer) {
			return Period{}, errors.New("calculation values contain a duplicate or invalid layer")
		}
		value.ID, value.PeriodID, value.CalculationVersion, value.Algorithm = NewID(), period.ID, generation, period.Algorithm
		if value.Layer == LayerExternal && value.Unit == "" {
			value.Unit = algorithmUnit(period.Algorithm)
		}
		byLayer[value.Layer] = value
	}
	for _, required := range []Layer{LayerRaw, LayerSupplier, LayerCustomer, LayerSNMP} {
		if _, ok := byLayer[required]; !ok {
			return Period{}, fmt.Errorf("calculation layer %s is required", required)
		}
	}
	ordered := make([]Value, 0, len(byLayer))
	for _, layer := range []Layer{LayerRaw, LayerSupplier, LayerCustomer, LayerSNMP, LayerExternal} {
		if value, ok := byLayer[layer]; ok {
			if err := insertValue(ctx, tx, value); err != nil {
				return Period{}, fmt.Errorf("insert %s billing value: %w", layer, err)
			}
			ordered = append(ordered, value)
		}
	}
	base := byLayer[period.DefaultLayer]
	adjustment, err := approvedAdjustmentTx(ctx, tx, period.ID, period.DefaultLayer, base.Unit)
	if err != nil {
		return Period{}, err
	}
	used := ApplyAdjustment(base.AlgorithmValue, adjustment)
	overuse := uint64(0)
	if period.Allowed != nil && used > *period.Allowed {
		overuse = used - *period.Allowed
	}
	adjustmentSnapshot, err := allAdjustmentsTx(ctx, tx, period.ID)
	if err != nil {
		return Period{}, err
	}
	runID := NewID()
	status := "ok"
	if len(input.Issues) > 0 {
		status = "issues"
	}
	summary, _ := json.Marshal(map[string]any{
		"kind": "calculation", "automatic_adjustment": false, "layers": len(ordered), "issues": len(input.Issues),
		"used": used, "overuse": overuse, "publication_ref": input.PublicationRef, "period_provenance": input.Provenance,
		"adjustments": adjustmentSnapshot,
	})
	if _, err := tx.ExecContext(ctx, `INSERT INTO reconciliation_runs
		(id,operation_ref,period_id,calculation_version,status,threshold_abs,threshold_percent,issue_count,summary_json,created_by)
		VALUES (?,?,?,?,?,?,?,?,?,NULLIF(?,''))`, runID, input.OperationRef, period.ID, generation, status, input.ThresholdAbs, input.ThresholdPercent, len(input.Issues), summary, input.Actor); err != nil {
		return Period{}, err
	}
	for _, issue := range input.Issues {
		if !ValidLayer(issue.LeftLayer) || (issue.RightLayer != "" && !ValidLayer(issue.RightLayer)) || len(issue.Detail) == 0 || !json.Valid(issue.Detail) {
			return Period{}, errors.New("reconciliation issue is invalid")
		}
		issue.ID, issue.RunID = NewID(), runID
		if _, err := tx.ExecContext(ctx, `INSERT INTO reconciliation_issues
			(id,run_id,kind,severity,left_layer,right_layer,metric,expected_value,actual_value,delta_value,threshold_value,status,detail_json,resolution_note)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, issue.ID, issue.RunID, issue.Kind, issue.Severity, issue.LeftLayer, issue.RightLayer,
			issue.Metric, issue.ExpectedValue, issue.ActualValue, issue.DeltaValue, issue.ThresholdValue, issue.Status, issue.Detail, issue.ResolutionNote); err != nil {
			return Period{}, err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE billing_periods SET status='calculated',used=?,overuse=?,calculation_version=?,
		publication_ref=?,provenance_json=?,approved_calculation_version=NULL,approved_by=NULL,approved_at=NULL,row_version=row_version+1
		WHERE id=? AND row_version=?`, used, overuse, generation, input.PublicationRef, input.Provenance, period.ID, input.ExpectedRowVersion)
	if err != nil {
		return Period{}, err
	}
	if err := changed(result); err != nil {
		return Period{}, err
	}
	if err := auditTx(ctx, tx, input.Actor, "billing.period.calculate", "billing_period", period.ID,
		fmt.Sprintf(`{"calculation_version":%d,"issue_count":%d}`, generation, len(input.Issues))); err != nil {
		return Period{}, err
	}
	if err := tx.Commit(); err != nil {
		return Period{}, err
	}
	return s.GetPeriod(ctx, period.ID)
}

func (s *Store) ApprovePeriod(ctx context.Context, periodID string, expectedRowVersion, calculationVersion uint64, actor string) (Period, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Period{}, err
	}
	defer tx.Rollback()
	period, err := scanPeriod(tx.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=? FOR UPDATE`, periodID))
	if err != nil {
		return Period{}, err
	}
	if period.RowVersion != expectedRowVersion || period.CalculationVersion != calculationVersion {
		return Period{}, ErrConflict
	}
	if period.Status != PeriodCalculated {
		return Period{}, ErrImmutable
	}
	var critical, pending int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM reconciliation_issues i JOIN reconciliation_runs r ON r.id=i.run_id
		WHERE r.period_id=? AND r.calculation_version=? AND i.status='open' AND i.severity='critical'`, periodID, calculationVersion).Scan(&critical); err != nil {
		return Period{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing_adjustments WHERE period_id=? AND status='pending'`, periodID).Scan(&pending); err != nil {
		return Period{}, err
	}
	if critical > 0 || pending > 0 {
		return Period{}, errors.New("critical reconciliation issues and pending adjustments must be handled before approval")
	}
	result, err := tx.ExecContext(ctx, `UPDATE billing_periods SET status='approved',approved_calculation_version=?,
		approved_by=NULLIF(?,''),approved_at=UTC_TIMESTAMP(3),row_version=row_version+1
		WHERE id=? AND row_version=? AND calculation_version=? AND status='calculated'`, calculationVersion, actor, periodID, expectedRowVersion, calculationVersion)
	if err != nil {
		return Period{}, err
	}
	if err := changed(result); err != nil {
		return Period{}, err
	}
	if err := auditTx(ctx, tx, actor, "billing.period.approve", "billing_period", periodID, fmt.Sprintf(`{"calculation_version":%d}`, calculationVersion)); err != nil {
		return Period{}, err
	}
	if err := tx.Commit(); err != nil {
		return Period{}, err
	}
	return s.GetPeriod(ctx, periodID)
}

func (s *Store) ClosePeriod(ctx context.Context, periodID string, expectedRowVersion, calculationVersion uint64, actor string) (Period, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Period{}, err
	}
	defer tx.Rollback()
	period, err := scanPeriod(tx.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=? FOR UPDATE`, periodID))
	if err != nil {
		return Period{}, err
	}
	if period.RowVersion != expectedRowVersion || period.CalculationVersion != calculationVersion || period.ApprovedCalculationVersion == nil || *period.ApprovedCalculationVersion != calculationVersion {
		return Period{}, ErrConflict
	}
	if period.Status != PeriodApproved {
		return Period{}, ErrImmutable
	}
	var openIssues int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM reconciliation_issues i JOIN reconciliation_runs r ON r.id=i.run_id
		WHERE r.period_id=? AND r.calculation_version=? AND i.status='open'`, periodID, calculationVersion).Scan(&openIssues); err != nil {
		return Period{}, err
	}
	if openIssues > 0 {
		return Period{}, errors.New("open reconciliation issues must be handled before close")
	}
	ledger, err := allAdjustmentsTx(ctx, tx, period.ID)
	if err != nil {
		return Period{}, err
	}
	ledgerSnapshot, err := json.Marshal(ledger)
	if err != nil {
		return Period{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE billing_periods SET status='closed',closed_by=NULLIF(?,''),closed_at=UTC_TIMESTAMP(3),adjustment_snapshot_json=?,row_version=row_version+1
		WHERE id=? AND row_version=? AND status='approved'`, actor, ledgerSnapshot, periodID, expectedRowVersion)
	if err != nil {
		return Period{}, err
	}
	if err := changed(result); err != nil {
		return Period{}, err
	}
	if err := auditTx(ctx, tx, actor, "billing.period.close", "billing_period", periodID, fmt.Sprintf(`{"calculation_version":%d}`, calculationVersion)); err != nil {
		return Period{}, err
	}
	if err := tx.Commit(); err != nil {
		return Period{}, err
	}
	return s.GetPeriod(ctx, periodID)
}

func algorithmUnit(algorithm Algorithm) string {
	if algorithm == AlgorithmTotal {
		return "bytes"
	}
	return "bps"
}

func approvedAdjustmentTx(ctx context.Context, tx *sql.Tx, periodID string, layer Layer, unit string) (int64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT amount FROM billing_adjustments WHERE period_id=? AND layer=? AND unit=? AND status='approved' ORDER BY created_at,id`, periodID, layer, unit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var total int64
	for rows.Next() {
		var amount int64
		if err := rows.Scan(&amount); err != nil {
			return 0, err
		}
		total = addInt64(total, amount)
	}
	return total, rows.Err()
}

func allAdjustmentsTx(ctx context.Context, tx *sql.Tx, periodID string) ([]Adjustment, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+adjustmentColumns+` FROM billing_adjustments WHERE period_id=? ORDER BY created_at,id`, periodID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Adjustment, 0)
	for rows.Next() {
		item, err := scanAdjustment(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

type AdjustmentInput struct {
	PeriodID    string
	Layer       Layer
	Unit        string
	Amount      int64
	Reason      string
	EvidenceRef string
	Actor       string
}

func (s *Store) CreateAdjustment(ctx context.Context, input AdjustmentInput) (Adjustment, error) {
	if input.PeriodID == "" || !ValidLayer(input.Layer) || (input.Unit != "bps" && input.Unit != "bytes") || input.Amount == 0 || input.Amount == math.MinInt64 || input.Reason == "" {
		return Adjustment{}, errors.New("adjustment period, layer, unit, non-zero amount and reason are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Adjustment{}, err
	}
	defer tx.Rollback()
	period, err := scanPeriod(tx.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=? FOR UPDATE`, input.PeriodID))
	if err != nil {
		return Adjustment{}, err
	}
	if period.Status != PeriodCalculated {
		return Adjustment{}, ErrImmutable
	}
	value, err := valueByLayerTx(ctx, tx, period.ID, period.CalculationVersion, input.Layer)
	if err != nil {
		return Adjustment{}, err
	}
	if value.Unit != input.Unit {
		return Adjustment{}, errors.New("adjustment unit does not match the calculated layer")
	}
	id := NewID()
	if _, err := tx.ExecContext(ctx, `INSERT INTO billing_adjustments
		(id,period_id,layer,unit,amount,reason,evidence_ref,status,created_by) VALUES (?,?,?,?,?,?,?,'pending',NULLIF(?,''))`,
		id, input.PeriodID, input.Layer, input.Unit, input.Amount, input.Reason, input.EvidenceRef, input.Actor); err != nil {
		return Adjustment{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE billing_periods SET row_version=row_version+1 WHERE id=?`, input.PeriodID); err != nil {
		return Adjustment{}, err
	}
	if err := auditTx(ctx, tx, input.Actor, "billing.adjustment.create", "billing_adjustment", id, fmt.Sprintf(`{"period_id":%q,"amount":%d}`, input.PeriodID, input.Amount)); err != nil {
		return Adjustment{}, err
	}
	if err := tx.Commit(); err != nil {
		return Adjustment{}, err
	}
	return s.GetAdjustment(ctx, id)
}

func (s *Store) ReverseAdjustment(ctx context.Context, adjustmentID string, actor, reason string) (Adjustment, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return Adjustment{}, errors.New("reversal reason is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Adjustment{}, err
	}
	defer tx.Rollback()
	original, err := scanAdjustment(tx.QueryRowContext(ctx, `SELECT `+adjustmentColumns+` FROM billing_adjustments WHERE id=? FOR UPDATE`, adjustmentID))
	if err != nil {
		return Adjustment{}, err
	}
	if original.Status != "approved" || original.Amount == math.MinInt64 {
		return Adjustment{}, errors.New("only approved adjustments can be reversed")
	}
	period, err := scanPeriod(tx.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=? FOR UPDATE`, original.PeriodID))
	if err != nil {
		return Adjustment{}, err
	}
	if period.Status == PeriodOpen {
		return Adjustment{}, ErrImmutable
	}
	id := NewID()
	if _, err := tx.ExecContext(ctx, `INSERT INTO billing_adjustments
		(id,period_id,layer,unit,amount,reason,evidence_ref,status,reverses_adjustment_id,created_by)
		VALUES (?,?,?,?,?,?,?,'pending',?,NULLIF(?,''))`, id, original.PeriodID, original.Layer, original.Unit, -original.Amount,
		reason, original.EvidenceRef, original.ID, actor); err != nil {
		return Adjustment{}, err
	}
	if period.Status == PeriodCalculated {
		if _, err := tx.ExecContext(ctx, `UPDATE billing_periods SET row_version=row_version+1 WHERE id=?`, period.ID); err != nil {
			return Adjustment{}, err
		}
	}
	if err := auditTx(ctx, tx, actor, "billing.adjustment.reverse", "billing_adjustment", id, fmt.Sprintf(`{"reverses":%q}`, original.ID)); err != nil {
		return Adjustment{}, err
	}
	if err := tx.Commit(); err != nil {
		return Adjustment{}, err
	}
	return s.GetAdjustment(ctx, id)
}

const adjustmentColumns = `id,period_id,layer,unit,amount,reason,evidence_ref,status,COALESCE(reverses_adjustment_id,''),
	row_version,COALESCE(created_by,''),COALESCE(approved_by,''),approved_at,created_at`

func scanAdjustment(scanner interface{ Scan(...any) error }) (Adjustment, error) {
	var item Adjustment
	var approvedAt sql.NullTime
	err := scanner.Scan(&item.ID, &item.PeriodID, &item.Layer, &item.Unit, &item.Amount, &item.Reason, &item.EvidenceRef,
		&item.Status, &item.ReversesAdjustmentID, &item.RowVersion, &item.CreatedBy, &item.ApprovedBy, &approvedAt, &item.CreatedAt)
	if err != nil {
		return Adjustment{}, normalizeSQLError(err)
	}
	if approvedAt.Valid {
		value := approvedAt.Time.UTC()
		item.ApprovedAt = &value
	}
	return item, nil
}

func (s *Store) GetAdjustment(ctx context.Context, id string) (Adjustment, error) {
	return scanAdjustment(s.db.QueryRowContext(ctx, `SELECT `+adjustmentColumns+` FROM billing_adjustments WHERE id=?`, id))
}

func (s *Store) ListAdjustments(ctx context.Context, periodID string, filter PageFilter) ([]Adjustment, int, error) {
	filter, order, err := normalizePage(filter, map[string]string{"created_at": "created_at", "status": "status", "layer": "layer", "amount": "amount"}, "created_at")
	if err != nil {
		return nil, 0, err
	}
	where, args := "period_id=?", []any{periodID}
	if filter.Query != "" {
		where += " AND (id LIKE ? OR reason LIKE ? OR evidence_ref LIKE ?)"
		like := "%" + filter.Query + "%"
		args = append(args, like, like, like)
	}
	if filter.Status != "" {
		where += " AND status=?"
		args = append(args, filter.Status)
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing_adjustments WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+adjustmentColumns+` FROM billing_adjustments WHERE `+where+` ORDER BY `+order+`,id LIMIT ? OFFSET ?`, append(args, filter.Limit, filter.Offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]Adjustment, 0)
	for rows.Next() {
		item, scanErr := scanAdjustment(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *Store) ApproveAdjustment(ctx context.Context, adjustmentID string, expected uint64, actor string) (Adjustment, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Adjustment{}, err
	}
	defer tx.Rollback()
	item, err := scanAdjustment(tx.QueryRowContext(ctx, `SELECT `+adjustmentColumns+` FROM billing_adjustments WHERE id=? FOR UPDATE`, adjustmentID))
	if err != nil {
		return Adjustment{}, err
	}
	if item.RowVersion != expected {
		return Adjustment{}, ErrConflict
	}
	if item.Status != "pending" {
		return Adjustment{}, ErrImmutable
	}
	period, err := scanPeriod(tx.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=? FOR UPDATE`, item.PeriodID))
	if err != nil {
		return Adjustment{}, err
	}
	if item.ReversesAdjustmentID == "" && period.Status != PeriodCalculated {
		return Adjustment{}, ErrImmutable
	}
	result, err := tx.ExecContext(ctx, `UPDATE billing_adjustments SET status='approved',approved_by=NULLIF(?,''),approved_at=UTC_TIMESTAMP(3),row_version=row_version+1
		WHERE id=? AND row_version=? AND status='pending'`, actor, item.ID, expected)
	if err != nil {
		return Adjustment{}, err
	}
	if err := changed(result); err != nil {
		return Adjustment{}, err
	}
	if period.Status == PeriodCalculated && item.Layer == period.DefaultLayer {
		values, err := valueByLayerTx(ctx, tx, period.ID, period.CalculationVersion, period.DefaultLayer)
		if err != nil {
			return Adjustment{}, err
		}
		adjustment, err := approvedAdjustmentTx(ctx, tx, period.ID, period.DefaultLayer, values.Unit)
		if err != nil {
			return Adjustment{}, err
		}
		used := ApplyAdjustment(values.AlgorithmValue, adjustment)
		overuse := uint64(0)
		if period.Allowed != nil && used > *period.Allowed {
			overuse = used - *period.Allowed
		}
		if _, err := tx.ExecContext(ctx, `UPDATE billing_periods SET used=?,overuse=?,row_version=row_version+1 WHERE id=?`, used, overuse, period.ID); err != nil {
			return Adjustment{}, err
		}
	}
	if err := auditTx(ctx, tx, actor, "billing.adjustment.approve", "billing_adjustment", item.ID, fmt.Sprintf(`{"period_id":%q}`, item.PeriodID)); err != nil {
		return Adjustment{}, err
	}
	if err := tx.Commit(); err != nil {
		return Adjustment{}, err
	}
	return s.GetAdjustment(ctx, item.ID)
}

func valueByLayerTx(ctx context.Context, tx *sql.Tx, periodID string, generation uint64, layer Layer) (Value, error) {
	return scanValue(tx.QueryRowContext(ctx, `SELECT `+valueColumns+` FROM billing_period_values
		WHERE period_id=? AND calculation_version=? AND layer=?`, periodID, generation, layer))
}

func (s *Store) ResolveIssue(ctx context.Context, issueID, status, note, actor string, expected uint64) (ReconciliationIssue, error) {
	if status != "acknowledged" && status != "resolved" {
		return ReconciliationIssue{}, errors.New("issue status must be acknowledged or resolved")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReconciliationIssue{}, err
	}
	defer tx.Rollback()
	item, err := scanIssue(tx.QueryRowContext(ctx, `SELECT `+issueColumns+` FROM reconciliation_issues WHERE id=? FOR UPDATE`, issueID))
	if err != nil {
		return ReconciliationIssue{}, err
	}
	if item.RowVersion != expected || item.Status != "open" {
		return ReconciliationIssue{}, ErrConflict
	}
	var periodID string
	var generation uint64
	if err := tx.QueryRowContext(ctx, `SELECT period_id,calculation_version FROM reconciliation_runs WHERE id=?`, item.RunID).Scan(&periodID, &generation); err != nil {
		return ReconciliationIssue{}, normalizeSQLError(err)
	}
	period, err := scanPeriod(tx.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=? FOR UPDATE`, periodID))
	if err != nil {
		return ReconciliationIssue{}, err
	}
	windowOffsetOpen := item.Kind == "window_offset" && period.Status == PeriodOpen && period.CalculationVersion == generation
	if !windowOffsetOpen && (period.Status != PeriodCalculated || period.CalculationVersion != generation) {
		return ReconciliationIssue{}, ErrImmutable
	}
	result, err := tx.ExecContext(ctx, `UPDATE reconciliation_issues SET status=?,resolution_note=?,resolved_by=NULLIF(?,''),resolved_at=UTC_TIMESTAMP(3),row_version=row_version+1
		WHERE id=? AND row_version=? AND status='open'`, status, note, actor, issueID, expected)
	if err != nil {
		return ReconciliationIssue{}, err
	}
	if err := changed(result); err != nil {
		return ReconciliationIssue{}, err
	}
	if err := auditTx(ctx, tx, actor, "billing.issue.resolve", "billing_issue", issueID, fmt.Sprintf(`{"status":%q,"row_version":%d}`, status, expected+1)); err != nil {
		return ReconciliationIssue{}, err
	}
	if err := tx.Commit(); err != nil {
		return ReconciliationIssue{}, err
	}
	return s.GetIssue(ctx, issueID)
}

func (s *Store) GetIssue(ctx context.Context, id string) (ReconciliationIssue, error) {
	return scanIssue(s.db.QueryRowContext(ctx, `SELECT `+issueColumns+` FROM reconciliation_issues WHERE id=?`, id))
}

const issueColumns = `id,run_id,kind,severity,left_layer,right_layer,metric,expected_value,actual_value,delta_value,
	threshold_value,status,detail_json,resolution_note,COALESCE(resolved_by,''),resolved_at,row_version,created_at,updated_at`

const issueJoinColumns = `i.id,i.run_id,i.kind,i.severity,i.left_layer,i.right_layer,i.metric,i.expected_value,i.actual_value,i.delta_value,
	i.threshold_value,i.status,i.detail_json,i.resolution_note,COALESCE(i.resolved_by,''),i.resolved_at,i.row_version,i.created_at,i.updated_at`

func scanIssue(scanner interface{ Scan(...any) error }) (ReconciliationIssue, error) {
	var item ReconciliationIssue
	var detail []byte
	var resolvedAt sql.NullTime
	err := scanner.Scan(&item.ID, &item.RunID, &item.Kind, &item.Severity, &item.LeftLayer, &item.RightLayer, &item.Metric,
		&item.ExpectedValue, &item.ActualValue, &item.DeltaValue, &item.ThresholdValue, &item.Status, &detail,
		&item.ResolutionNote, &item.ResolvedBy, &resolvedAt, &item.RowVersion, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		return ReconciliationIssue{}, normalizeSQLError(err)
	}
	item.Detail = json.RawMessage(detail)
	if resolvedAt.Valid {
		value := resolvedAt.Time.UTC()
		item.ResolvedAt = &value
	}
	return item, nil
}

func (s *Store) ListIssues(ctx context.Context, periodID string, generation uint64, filter PageFilter) ([]ReconciliationIssue, int, error) {
	filter, order, err := normalizePage(filter, map[string]string{"created_at": "i.created_at", "status": "i.status", "severity": "i.severity", "kind": "i.kind", "delta_value": "i.delta_value"}, "created_at")
	if err != nil {
		return nil, 0, err
	}
	where, args := []string{"r.period_id=?", "r.calculation_version=?", `r.id=(SELECT rr.id FROM reconciliation_runs rr
		WHERE rr.period_id=r.period_id AND rr.calculation_version=r.calculation_version ORDER BY rr.created_at DESC,rr.id DESC LIMIT 1)`}, []any{periodID, generation}
	if filter.Status != "" {
		where, args = append(where, "i.status=?"), append(args, filter.Status)
	}
	if filter.Type != "" {
		where, args = append(where, "i.kind=?"), append(args, filter.Type)
	}
	if filter.Query != "" {
		where = append(where, "(i.id LIKE ? OR i.kind LIKE ? OR i.metric LIKE ? OR i.resolution_note LIKE ?)")
		like := "%" + filter.Query + "%"
		args = append(args, like, like, like, like)
	}
	predicate := stringsJoin(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM reconciliation_issues i JOIN reconciliation_runs r ON r.id=i.run_id WHERE `+predicate, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+issueJoinColumns+` FROM reconciliation_issues i JOIN reconciliation_runs r ON r.id=i.run_id WHERE `+predicate+` ORDER BY `+order+`,i.id LIMIT ? OFFSET ?`, append(args, filter.Limit, filter.Offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]ReconciliationIssue, 0)
	for rows.Next() {
		item, scanErr := scanIssue(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

const runColumns = `id,operation_ref,period_id,calculation_version,status,threshold_abs,threshold_percent,issue_count,
	summary_json,COALESCE(created_by,''),created_at`

func scanRun(scanner interface{ Scan(...any) error }) (ReconciliationRun, error) {
	var item ReconciliationRun
	var summary []byte
	err := scanner.Scan(&item.ID, &item.OperationRef, &item.PeriodID, &item.CalculationVersion, &item.Status,
		&item.ThresholdAbs, &item.ThresholdPercent, &item.IssueCount, &summary, &item.CreatedBy, &item.CreatedAt)
	item.Summary = json.RawMessage(summary)
	return item, normalizeSQLError(err)
}

func (s *Store) GetRunByOperation(ctx context.Context, operationRef string) (ReconciliationRun, error) {
	return scanRun(s.db.QueryRowContext(ctx, `SELECT `+runColumns+` FROM reconciliation_runs WHERE operation_ref=?`, operationRef))
}

func (s *Store) ListReconciliationRuns(ctx context.Context, periodID string, filter PageFilter) ([]ReconciliationRun, int, error) {
	filter, order, err := normalizePage(filter, map[string]string{"created_at": "created_at", "status": "status", "calculation_version": "calculation_version", "issue_count": "issue_count"}, "created_at")
	if err != nil {
		return nil, 0, err
	}
	where, args := "period_id=?", []any{periodID}
	if filter.Query != "" {
		where += " AND (id LIKE ? OR operation_ref LIKE ?)"
		like := "%" + filter.Query + "%"
		args = append(args, like, like)
	}
	if filter.Status != "" {
		where += " AND status=?"
		args = append(args, filter.Status)
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM reconciliation_runs WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+runColumns+` FROM reconciliation_runs WHERE `+where+` ORDER BY `+order+`,id LIMIT ? OFFSET ?`, append(args, filter.Limit, filter.Offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]ReconciliationRun, 0)
	for rows.Next() {
		item, scanErr := scanRun(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

// RecordWindowOffset persists a calculation failure as reconciliation evidence
// without creating or advancing a value generation. Retrying the same operation
// returns the original run, so a deterministic contract error is never duplicated.
func (s *Store) RecordWindowOffset(ctx context.Context, periodID string, expectedRowVersion uint64, actor, operationRef string, detail json.RawMessage) (ReconciliationRun, error) {
	if periodID == "" || expectedRowVersion == 0 || len(detail) == 0 || !json.Valid(detail) {
		return ReconciliationRun{}, errors.New("window offset identity, version and detail are required")
	}
	if operationRef == "" {
		operationRef = NewID()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReconciliationRun{}, err
	}
	defer tx.Rollback()
	if existing, findErr := scanRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+` FROM reconciliation_runs WHERE operation_ref=?`, operationRef)); findErr == nil {
		if existing.PeriodID != periodID {
			return ReconciliationRun{}, ErrConflict
		}
		return existing, nil
	} else if !errors.Is(findErr, ErrNotFound) {
		return ReconciliationRun{}, findErr
	}
	period, err := scanPeriod(tx.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=? FOR UPDATE`, periodID))
	if err != nil {
		return ReconciliationRun{}, err
	}
	if period.RowVersion != expectedRowVersion {
		return ReconciliationRun{}, ErrConflict
	}
	if period.Status == PeriodApproved || period.Status == PeriodClosed {
		return ReconciliationRun{}, ErrImmutable
	}
	run := ReconciliationRun{
		ID: NewID(), OperationRef: operationRef, PeriodID: period.ID, CalculationVersion: period.CalculationVersion,
		Status: "issues", ThresholdAbs: period.ReconcileAbs, ThresholdPercent: period.ReconcilePercent, IssueCount: 1, CreatedBy: actor,
	}
	run.Summary, _ = json.Marshal(map[string]any{
		"kind": "evidence_failure", "reason": "window_offset", "automatic_adjustment": false, "detail": json.RawMessage(detail),
	})
	if _, err := tx.ExecContext(ctx, `INSERT INTO reconciliation_runs
		(id,operation_ref,period_id,calculation_version,status,threshold_abs,threshold_percent,issue_count,summary_json,created_by)
		VALUES (?,?,?,?,?,?,?,?,?,NULLIF(?,''))`, run.ID, run.OperationRef, run.PeriodID, run.CalculationVersion, run.Status,
		run.ThresholdAbs, run.ThresholdPercent, run.IssueCount, run.Summary, actor); err != nil {
		_ = tx.Rollback()
		if existing, findErr := s.GetRunByOperation(ctx, operationRef); findErr == nil && existing.PeriodID == periodID {
			return existing, nil
		}
		return ReconciliationRun{}, err
	}
	issueID := NewID()
	if _, err := tx.ExecContext(ctx, `INSERT INTO reconciliation_issues
		(id,run_id,kind,severity,left_layer,right_layer,metric,expected_value,actual_value,delta_value,threshold_value,status,detail_json,resolution_note)
		VALUES (?,?,'window_offset','critical','snmp','raw','reader_window',0,0,0,0,'open',?,'')`, issueID, run.ID, detail); err != nil {
		return ReconciliationRun{}, err
	}
	if err := auditTx(ctx, tx, actor, "billing.period.window_offset", "billing_period", period.ID,
		fmt.Sprintf(`{"calculation_version":%d,"run_id":%q}`, period.CalculationVersion, run.ID)); err != nil {
		return ReconciliationRun{}, err
	}
	if err := tx.Commit(); err != nil {
		return ReconciliationRun{}, err
	}
	return scanRun(s.db.QueryRowContext(ctx, `SELECT `+runColumns+` FROM reconciliation_runs WHERE id=?`, run.ID))
}

func (s *Store) ReconcileCurrent(ctx context.Context, periodID string, expectedRowVersion uint64, actor, operationRef string) (ReconciliationRun, error) {
	if operationRef == "" {
		return ReconciliationRun{}, errors.New("reconciliation operation reference is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReconciliationRun{}, err
	}
	defer tx.Rollback()
	if existing, err := scanRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+` FROM reconciliation_runs WHERE operation_ref=?`, operationRef)); err == nil {
		return existing, nil
	} else if !errors.Is(err, ErrNotFound) {
		return ReconciliationRun{}, err
	}
	period, err := scanPeriod(tx.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=? FOR UPDATE`, periodID))
	if err != nil {
		return ReconciliationRun{}, err
	}
	if period.RowVersion != expectedRowVersion {
		return ReconciliationRun{}, ErrConflict
	}
	if period.Status != PeriodCalculated || period.CalculationVersion == 0 {
		return ReconciliationRun{}, ErrImmutable
	}
	absolute, percent := period.ReconcileAbs, period.ReconcilePercent
	rows, err := tx.QueryContext(ctx, `SELECT `+valueColumns+` FROM billing_period_values WHERE period_id=? AND calculation_version=?`, period.ID, period.CalculationVersion)
	if err != nil {
		return ReconciliationRun{}, err
	}
	values := make([]Value, 0, 5)
	for rows.Next() {
		value, scanErr := scanValue(rows)
		if scanErr != nil {
			_ = rows.Close()
			return ReconciliationRun{}, scanErr
		}
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return ReconciliationRun{}, err
	}
	issues := Reconcile(values, absolute, percent)
	run := ReconciliationRun{ID: NewID(), OperationRef: operationRef, PeriodID: period.ID, CalculationVersion: period.CalculationVersion,
		Status: "ok", ThresholdAbs: absolute, ThresholdPercent: percent, IssueCount: uint32(len(issues)), CreatedBy: actor}
	if len(issues) > 0 {
		run.Status = "issues"
	}
	run.Summary, _ = json.Marshal(map[string]any{"kind": "reconciliation", "automatic_adjustment": false, "layers": len(values), "issues": len(issues), "rerun": true})
	if _, err := tx.ExecContext(ctx, `INSERT INTO reconciliation_runs
		(id,operation_ref,period_id,calculation_version,status,threshold_abs,threshold_percent,issue_count,summary_json,created_by)
		VALUES (?,?,?,?,?,?,?,?,?,NULLIF(?,''))`, run.ID, run.OperationRef, run.PeriodID, run.CalculationVersion, run.Status,
		run.ThresholdAbs, run.ThresholdPercent, run.IssueCount, run.Summary, actor); err != nil {
		return ReconciliationRun{}, err
	}
	for _, issue := range issues {
		issue.ID, issue.RunID = NewID(), run.ID
		if _, err := tx.ExecContext(ctx, `INSERT INTO reconciliation_issues
			(id,run_id,kind,severity,left_layer,right_layer,metric,expected_value,actual_value,delta_value,threshold_value,status,detail_json,resolution_note)
			VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, issue.ID, issue.RunID, issue.Kind, issue.Severity, issue.LeftLayer, issue.RightLayer,
			issue.Metric, issue.ExpectedValue, issue.ActualValue, issue.DeltaValue, issue.ThresholdValue, issue.Status, issue.Detail, issue.ResolutionNote); err != nil {
			return ReconciliationRun{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE billing_periods SET row_version=row_version+1 WHERE id=? AND row_version=?`, period.ID, expectedRowVersion); err != nil {
		return ReconciliationRun{}, err
	}
	if err := auditTx(ctx, tx, actor, "billing.period.reconcile", "billing_period", period.ID, fmt.Sprintf(`{"calculation_version":%d,"issue_count":%d}`, period.CalculationVersion, len(issues))); err != nil {
		return ReconciliationRun{}, err
	}
	if err := tx.Commit(); err != nil {
		return ReconciliationRun{}, err
	}
	return scanRun(s.db.QueryRowContext(ctx, `SELECT `+runColumns+` FROM reconciliation_runs WHERE id=?`, run.ID))
}

func stringsJoin(input []string, separator string) string {
	result := ""
	for index, value := range input {
		if index > 0 {
			result += separator
		}
		result += value
	}
	return result
}
