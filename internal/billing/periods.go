// SPDX-License-Identifier: AGPL-3.0-only

package billing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const periodColumns = `id,account_id,date_from,date_to,timezone,direction,status,algorithm,default_layer,reconcile_abs,reconcile_percent,
	allowed,used,overuse,calculation_version,publication_ref,account_snapshot_json,adjustment_snapshot_json,provenance_json,approved_calculation_version,
	COALESCE(approved_by,''),approved_at,COALESCE(closed_by,''),closed_at,row_version,
	COALESCE(created_by,''),created_at,updated_at`

func nullableUint64(value sql.NullString) *uint64 {
	if !value.Valid {
		return nil
	}
	parsed, err := strconv.ParseUint(value.String, 10, 64)
	if err != nil {
		return nil
	}
	return &parsed
}

func scanPeriod(scanner interface{ Scan(...any) error }) (Period, error) {
	var item Period
	var allowed, used, overuse, approvedVersion sql.NullString
	var approvedAt, closedAt sql.NullTime
	var accountSnapshot, adjustmentSnapshot, provenance []byte
	err := scanner.Scan(&item.ID, &item.AccountID, &item.DateFrom, &item.DateTo, &item.Timezone, &item.Direction,
		&item.Status, &item.Algorithm, &item.DefaultLayer, &item.ReconcileAbs, &item.ReconcilePercent,
		&allowed, &used, &overuse, &item.CalculationVersion, &item.PublicationRef,
		&accountSnapshot, &adjustmentSnapshot, &provenance, &approvedVersion, &item.ApprovedBy, &approvedAt, &item.ClosedBy, &closedAt, &item.RowVersion,
		&item.CreatedBy, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		return Period{}, normalizeSQLError(err)
	}
	item.Allowed, item.Used, item.Overuse, item.ApprovedCalculationVersion = nullableUint64(allowed), nullableUint64(used), nullableUint64(overuse), nullableUint64(approvedVersion)
	item.Provenance = json.RawMessage(provenance)
	item.AccountSnapshot = json.RawMessage(accountSnapshot)
	item.AdjustmentSnapshot = json.RawMessage(adjustmentSnapshot)
	if approvedAt.Valid {
		value := approvedAt.Time.UTC()
		item.ApprovedAt = &value
	}
	if closedAt.Valid {
		value := closedAt.Time.UTC()
		item.ClosedAt = &value
	}
	item.DateFrom, item.DateTo = item.DateFrom.UTC(), item.DateTo.UTC()
	return item, nil
}

func (s *Store) GetPeriod(ctx context.Context, id string) (Period, error) {
	return scanPeriod(s.db.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=?`, id))
}

func (s *Store) ListPeriods(ctx context.Context, accountID string, filter PageFilter) ([]Period, int, error) {
	filter, order, err := normalizePage(filter, map[string]string{"date_from": "date_from", "date_to": "date_to", "status": "status", "used": "used", "created_at": "created_at"}, "date_from")
	if err != nil {
		return nil, 0, err
	}
	where, args := []string{"account_id=?"}, []any{accountID}
	if filter.Query != "" {
		where, args = append(where, "id LIKE ?"), append(args, "%"+filter.Query+"%")
	}
	if filter.Status != "" {
		where, args = append(where, "status=?"), append(args, filter.Status)
	}
	predicate := strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing_periods WHERE `+predicate, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE `+predicate+` ORDER BY `+order+`,id LIMIT ? OFFSET ?`, append(args, filter.Limit, filter.Offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]Period, 0)
	for rows.Next() {
		item, scanErr := scanPeriod(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *Store) CreatePeriod(ctx context.Context, accountID string, from, to time.Time, now time.Time, actor string) (Period, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Period{}, err
	}
	defer tx.Rollback()
	account, err := scanAccount(tx.QueryRowContext(ctx, `SELECT `+accountColumns+` FROM billing_accounts WHERE id=? FOR UPDATE`, accountID))
	if err != nil {
		return Period{}, err
	}
	if account.Status != "active" {
		return Period{}, errors.New("billing account is paused")
	}
	if err := ValidatePeriodWindow(from, to, account.Timezone, now); err != nil {
		return Period{}, err
	}
	var overlaps bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM billing_periods
		WHERE account_id=? AND date_from<? AND date_to>?)`, accountID, to.UTC(), from.UTC()).Scan(&overlaps); err != nil {
		return Period{}, err
	}
	if overlaps {
		return Period{}, errors.New("billing period overlaps an existing period")
	}
	ports, err := listAccountPortsTx(ctx, tx, accountID)
	if err != nil {
		return Period{}, err
	}
	if len(ports) == 0 {
		return Period{}, errors.New("billing account has no bound ports")
	}
	var party *Party
	if account.PartyID != "" {
		item, partyErr := scanParty(tx.QueryRowContext(ctx, `SELECT `+partyColumns+` FROM parties WHERE id=?`, account.PartyID))
		if partyErr != nil {
			return Period{}, partyErr
		}
		party = &item
	}
	accountSnapshot, err := json.Marshal(struct {
		Account Account `json:"account"`
		Party   *Party  `json:"party,omitempty"`
	}{Account: account, Party: party})
	if err != nil {
		return Period{}, err
	}
	var allowed *uint64
	if account.BillType == "cdr" {
		allowed = account.CDRBPS
	} else {
		allowed = account.QuotaBytes
	}
	id := NewID()
	_, err = tx.ExecContext(ctx, `INSERT INTO billing_periods
		(id,account_id,date_from,date_to,timezone,direction,status,algorithm,default_layer,reconcile_abs,reconcile_percent,allowed,publication_ref,account_snapshot_json,adjustment_snapshot_json,provenance_json,created_by)
		VALUES (?,?,?,?,?,?,'open',?,?,?,?,?,'',?,'[]','{}',NULLIF(?,''))`, id, accountID, from.UTC(), to.UTC(), account.Timezone,
		account.Direction, account.Algorithm, account.DefaultLayer, account.ReconcileAbs, account.ReconcilePercent, allowed, accountSnapshot, actor)
	if err != nil {
		return Period{}, fmt.Errorf("create billing period: %w", err)
	}
	for _, port := range ports {
		if _, err := tx.ExecContext(ctx, `INSERT INTO billing_period_ports
			(period_id,port_id,device_id,if_index,if_name,direction) VALUES (?,?,?,?,?,?)`,
			id, port.PortID, port.DeviceID, port.IfIndex, port.IfName, port.Direction); err != nil {
			return Period{}, fmt.Errorf("snapshot billing period port: %w", err)
		}
	}
	if err := auditTx(ctx, tx, actor, "billing.period.create", "billing_period", id,
		fmt.Sprintf(`{"account_id":%q,"date_from":%q,"date_to":%q}`, accountID, from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339))); err != nil {
		return Period{}, err
	}
	if err := tx.Commit(); err != nil {
		return Period{}, err
	}
	return s.GetPeriod(ctx, id)
}

// SuggestedPeriodWindow returns the latest fully closed account billing cycle.
// The configured day is interpreted at 00:00 in the account timezone and is
// clamped to the last day of shorter months.
func SuggestedPeriodWindow(account Account, now time.Time) (time.Time, time.Time, error) {
	location, err := time.LoadLocation(account.Timezone)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid IANA timezone: %w", err)
	}
	if account.BillingDay < 1 || account.BillingDay > 31 {
		return time.Time{}, time.Time{}, errors.New("billing day must be 1..31")
	}
	localNow := now.In(location)
	boundary := func(year int, month time.Month) time.Time {
		lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, location).Day()
		day := int(account.BillingDay)
		if day > lastDay {
			day = lastDay
		}
		return time.Date(year, month, day, 0, 0, 0, 0, location)
	}
	end := boundary(localNow.Year(), localNow.Month())
	if end.After(localNow) {
		previous := localNow.AddDate(0, -1, 0)
		end = boundary(previous.Year(), previous.Month())
	}
	previous := end.AddDate(0, -1, 0)
	start := boundary(previous.Year(), previous.Month())
	return start.UTC(), end.UTC(), nil
}

func listAccountPortsTx(ctx context.Context, tx *sql.Tx, accountID string) ([]AccountPort, error) {
	rows, err := tx.QueryContext(ctx, `SELECT bap.account_id,bap.port_id,p.device_id,p.if_index,p.if_name,bap.direction,
		COALESCE(bap.created_by,''),bap.created_at FROM billing_account_ports bap JOIN ports p ON p.id=bap.port_id
		WHERE bap.account_id=? ORDER BY p.device_id,p.if_index,p.id`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]AccountPort, 0)
	for rows.Next() {
		var item AccountPort
		if err := rows.Scan(&item.AccountID, &item.PortID, &item.DeviceID, &item.IfIndex, &item.IfName, &item.Direction, &item.CreatedBy, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ListPeriodPorts(ctx context.Context, periodID string) ([]AccountPort, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT p.account_id,bpp.port_id,bpp.device_id,bpp.if_index,bpp.if_name,bpp.direction,
		'',p.created_at FROM billing_period_ports bpp JOIN billing_periods p ON p.id=bpp.period_id
		WHERE bpp.period_id=? ORDER BY bpp.device_id,bpp.if_index,bpp.port_id`, periodID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]AccountPort, 0)
	for rows.Next() {
		var item AccountPort
		if err := rows.Scan(&item.AccountID, &item.PortID, &item.DeviceID, &item.IfIndex, &item.IfName, &item.Direction, &item.CreatedBy, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const valueColumns = `id,period_id,calculation_version,layer,algorithm,unit,in_bytes,out_bytes,selected_bytes,
	rate_95th_bps,rate_average_bps,algorithm_value,coverage,expected_buckets,observed_buckets,missing_buckets,
	reset_buckets,gap_buckets,unknown_sampling_records,source_generation_min,source_generation_max,provenance_json,created_at`

func scanValue(scanner interface{ Scan(...any) error }) (Value, error) {
	var item Value
	var provenance []byte
	err := scanner.Scan(&item.ID, &item.PeriodID, &item.CalculationVersion, &item.Layer, &item.Algorithm, &item.Unit,
		&item.InBytes, &item.OutBytes, &item.SelectedBytes, &item.Rate95thBPS, &item.RateAverageBPS, &item.AlgorithmValue,
		&item.Coverage, &item.ExpectedBuckets, &item.ObservedBuckets, &item.MissingBuckets, &item.ResetBuckets, &item.GapBuckets,
		&item.UnknownSamplingRecords, &item.SourceGenerationMin, &item.SourceGenerationMax, &provenance, &item.CreatedAt)
	item.Provenance = json.RawMessage(provenance)
	return item, normalizeSQLError(err)
}

func (s *Store) ListValues(ctx context.Context, periodID string, generation uint64) ([]Value, error) {
	if generation == 0 {
		period, err := s.GetPeriod(ctx, periodID)
		if err != nil {
			return nil, err
		}
		generation = period.CalculationVersion
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+valueColumns+` FROM billing_period_values
		WHERE period_id=? AND calculation_version=? ORDER BY FIELD(layer,'raw','supplier','customer','snmp','external'),id`, periodID, generation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Value, 0)
	for rows.Next() {
		item, scanErr := scanValue(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) SaveExternalDraft(ctx context.Context, periodID string, value Value, expected uint64, actor string) (Value, error) {
	if expected == 0 {
		return Value{}, errors.New("billing period row version is required")
	}
	if value.Layer != "" && value.Layer != LayerExternal {
		return Value{}, errors.New("external import layer must be external")
	}
	if len(value.Provenance) == 0 || !json.Valid(value.Provenance) {
		return Value{}, errors.New("external provenance must be valid JSON")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Value{}, err
	}
	defer tx.Rollback()
	period, err := scanPeriod(tx.QueryRowContext(ctx, `SELECT `+periodColumns+` FROM billing_periods WHERE id=? FOR UPDATE`, periodID))
	if err != nil {
		return Value{}, err
	}
	if period.RowVersion != expected {
		return Value{}, ErrConflict
	}
	if period.Status == PeriodApproved || period.Status == PeriodClosed {
		return Value{}, ErrImmutable
	}
	value.ID, value.PeriodID, value.CalculationVersion, value.Layer = NewID(), periodID, 0, LayerExternal
	value.Algorithm = period.Algorithm
	if value.Unit == "" {
		value.Unit = algorithmUnit(period.Algorithm)
	}
	if value.Unit != algorithmUnit(period.Algorithm) {
		return Value{}, errors.New("external value unit does not match the period algorithm")
	}
	if err := ValidateExternalValue(value); err != nil {
		return Value{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM billing_period_values WHERE period_id=? AND calculation_version=0 AND layer='external'`, periodID); err != nil {
		return Value{}, err
	}
	if err := insertValue(ctx, tx, value); err != nil {
		return Value{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE billing_periods SET row_version=row_version+1 WHERE id=? AND row_version=?`, periodID, expected)
	if err != nil {
		return Value{}, err
	}
	if err := changed(result); err != nil {
		return Value{}, err
	}
	if err := auditTx(ctx, tx, actor, "billing.external.import", "billing_period", periodID, value.Provenance); err != nil {
		return Value{}, err
	}
	if err := tx.Commit(); err != nil {
		return Value{}, err
	}
	return scanValue(s.db.QueryRowContext(ctx, `SELECT `+valueColumns+` FROM billing_period_values WHERE id=?`, value.ID))
}

// ValidateExternalValue keeps imported evidence internally reproducible. It does
// not compare the evidence with another layer; reconciliation performs that step.
func ValidateExternalValue(value Value) error {
	if math.IsNaN(value.Coverage) || math.IsInf(value.Coverage, 0) || value.Coverage < 0 || value.Coverage > 1 {
		return errors.New("external coverage must be between 0 and 1")
	}
	if value.ExpectedBuckets == 0 || value.ObservedBuckets > value.ExpectedBuckets ||
		value.MissingBuckets != value.ExpectedBuckets-value.ObservedBuckets || value.ResetBuckets > value.ObservedBuckets || value.GapBuckets > value.ExpectedBuckets {
		return errors.New("external bucket counters are inconsistent")
	}
	expectedCoverage := float64(value.ObservedBuckets) / float64(value.ExpectedBuckets)
	tolerance := math.Max(1/float64(value.ExpectedBuckets), 0.0001)
	if math.Abs(value.Coverage-expectedCoverage) > tolerance {
		return errors.New("external coverage does not match its bucket counters")
	}
	switch value.Algorithm {
	case Algorithm95th:
		if value.AlgorithmValue != value.Rate95thBPS {
			return errors.New("external algorithm value does not match rate_95th_bps")
		}
	case AlgorithmAverage:
		if value.AlgorithmValue != value.RateAverageBPS {
			return errors.New("external algorithm value does not match rate_average_bps")
		}
	case AlgorithmTotal:
		if value.AlgorithmValue != value.SelectedBytes {
			return errors.New("external algorithm value does not match selected_bytes")
		}
	default:
		return errors.New("external algorithm is invalid")
	}
	return nil
}

func (s *Store) ExternalDraft(ctx context.Context, periodID string) (*Value, error) {
	value, err := scanValue(s.db.QueryRowContext(ctx, `SELECT `+valueColumns+` FROM billing_period_values
		WHERE period_id=? AND calculation_version=0 AND layer='external'`, periodID))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func insertValue(ctx context.Context, tx *sql.Tx, value Value) error {
	if value.ID == "" {
		value.ID = NewID()
	}
	if !ValidLayer(value.Layer) || !ValidAlgorithm(value.Algorithm) || (value.Unit != "bps" && value.Unit != "bytes") || len(value.Provenance) == 0 || !json.Valid(value.Provenance) {
		return errors.New("billing value is invalid")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO billing_period_values
		(id,period_id,calculation_version,layer,algorithm,unit,in_bytes,out_bytes,selected_bytes,rate_95th_bps,rate_average_bps,algorithm_value,
		coverage,expected_buckets,observed_buckets,missing_buckets,reset_buckets,gap_buckets,unknown_sampling_records,source_generation_min,source_generation_max,provenance_json)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, value.ID, value.PeriodID, value.CalculationVersion, value.Layer, value.Algorithm, value.Unit,
		value.InBytes, value.OutBytes, value.SelectedBytes, value.Rate95thBPS, value.RateAverageBPS, value.AlgorithmValue, value.Coverage,
		value.ExpectedBuckets, value.ObservedBuckets, value.MissingBuckets, value.ResetBuckets, value.GapBuckets, value.UnknownSamplingRecords,
		value.SourceGenerationMin, value.SourceGenerationMax, value.Provenance)
	return err
}
