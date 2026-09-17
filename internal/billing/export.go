// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"context"
	"encoding/json"
	"errors"
)

type calculationEvidenceSummary struct {
	Kind             string          `json:"kind"`
	Used             uint64          `json:"used"`
	Overuse          uint64          `json:"overuse"`
	PublicationRef   string          `json:"publication_ref"`
	PeriodProvenance json.RawMessage `json:"period_provenance"`
	Adjustments      []Adjustment    `json:"adjustments"`
}

type ExportEvidence struct {
	Period          Period                `json:"period"`
	Account         Account               `json:"account"`
	Party           *Party                `json:"party,omitempty"`
	Ports           []AccountPort         `json:"ports"`
	Values          []Value               `json:"values"`
	Adjustments     []Adjustment          `json:"adjustments"`
	Reconciliations []ReconciliationRun   `json:"reconciliations"`
	Issues          []ReconciliationIssue `json:"issues"`
}

func (s *Store) ExportEvidence(ctx context.Context, periodID string, generation uint64) (ExportEvidence, error) {
	period, err := s.GetPeriod(ctx, periodID)
	if err != nil {
		return ExportEvidence{}, err
	}
	if generation == 0 {
		generation = period.CalculationVersion
	}
	if generation == 0 || generation > period.CalculationVersion {
		return ExportEvidence{}, errors.New("calculation version is not available")
	}
	var snapshot PeriodAccountSnapshot
	if len(period.AccountSnapshot) == 0 || json.Unmarshal(period.AccountSnapshot, &snapshot) != nil || snapshot.Account.ID != period.AccountID {
		return ExportEvidence{}, errors.New("billing period account snapshot is invalid")
	}
	ports, err := s.ListPeriodPorts(ctx, periodID)
	if err != nil {
		return ExportEvidence{}, err
	}
	values, err := s.ListValues(ctx, periodID, generation)
	if err != nil {
		return ExportEvidence{}, err
	}
	if len(values) == 0 {
		return ExportEvidence{}, ErrNotFound
	}
	runs, issues, err := s.allReconciliationEvidence(ctx, periodID, generation)
	if err != nil {
		return ExportEvidence{}, err
	}
	adjustments, err := s.allAdjustments(ctx, period)
	if err != nil {
		return ExportEvidence{}, err
	}
	if generation != period.CalculationVersion {
		summary, ok := calculationSummary(runs)
		if !ok {
			return ExportEvidence{}, errors.New("billing calculation evidence summary is unavailable")
		}
		period.Status, period.Used, period.Overuse = PeriodCalculated, &summary.Used, &summary.Overuse
		period.PublicationRef, period.Provenance, adjustments = summary.PublicationRef, summary.PeriodProvenance, summary.Adjustments
		period.ApprovedCalculationVersion, period.ApprovedBy, period.ApprovedAt, period.ClosedBy, period.ClosedAt = nil, "", nil, "", nil
	}
	period.CalculationVersion = generation
	return ExportEvidence{Period: period, Account: snapshot.Account, Party: snapshot.Party, Ports: ports, Values: values, Adjustments: adjustments, Reconciliations: runs, Issues: issues}, nil
}

func calculationSummary(runs []ReconciliationRun) (calculationEvidenceSummary, bool) {
	for _, run := range runs {
		var summary calculationEvidenceSummary
		if json.Unmarshal(run.Summary, &summary) == nil && summary.Kind == "calculation" && len(summary.PeriodProvenance) > 0 {
			return summary, true
		}
	}
	return calculationEvidenceSummary{}, false
}

func (s *Store) allAdjustments(ctx context.Context, period Period) ([]Adjustment, error) {
	if period.Status == PeriodClosed {
		var items []Adjustment
		if len(period.AdjustmentSnapshot) == 0 || json.Unmarshal(period.AdjustmentSnapshot, &items) != nil {
			return nil, errors.New("closed billing adjustment snapshot is invalid")
		}
		return items, nil
	}
	query := `SELECT ` + adjustmentColumns + ` FROM billing_adjustments WHERE period_id=?`
	args := []any{period.ID}
	if period.ClosedAt != nil {
		query += ` AND created_at<=?`
		args = append(args, period.ClosedAt.UTC())
	}
	query += ` ORDER BY created_at,id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Adjustment, 0)
	for rows.Next() {
		if len(items) >= s.limits.MaxExportRows {
			return nil, errors.New("billing export adjustment budget exceeded")
		}
		item, scanErr := scanAdjustment(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) allReconciliationEvidence(ctx context.Context, periodID string, generation uint64) ([]ReconciliationRun, []ReconciliationIssue, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+runColumns+` FROM reconciliation_runs WHERE period_id=? AND calculation_version=? ORDER BY created_at,id`, periodID, generation)
	if err != nil {
		return nil, nil, err
	}
	runs := make([]ReconciliationRun, 0)
	for rows.Next() {
		item, scanErr := scanRun(rows)
		if scanErr != nil {
			_ = rows.Close()
			return nil, nil, scanErr
		}
		runs = append(runs, item)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	issueRows, err := s.db.QueryContext(ctx, `SELECT `+issueJoinColumns+` FROM reconciliation_issues i JOIN reconciliation_runs r ON r.id=i.run_id
		WHERE r.period_id=? AND r.calculation_version=? ORDER BY i.created_at,i.id`, periodID, generation)
	if err != nil {
		return nil, nil, err
	}
	defer issueRows.Close()
	issues := make([]ReconciliationIssue, 0)
	for issueRows.Next() {
		if len(issues) >= s.limits.MaxExportRows {
			return nil, nil, errors.New("billing export issue budget exceeded")
		}
		item, scanErr := scanIssue(issueRows)
		if scanErr != nil {
			return nil, nil, scanErr
		}
		issues = append(issues, item)
	}
	return runs, issues, issueRows.Err()
}
