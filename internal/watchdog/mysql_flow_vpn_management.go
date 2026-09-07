package watchdog

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

const (
	vpnFindingLocalIPExpression  = "CASE WHEN LEFT(HEX(f.local_ip), 24) = '00000000000000000000FFFF' THEN SUBSTRING(INET6_NTOA(f.local_ip), 8) ELSE INET6_NTOA(f.local_ip) END"
	vpnFindingRemoteIPExpression = "CASE WHEN LEFT(HEX(f.remote_ip), 24) = '00000000000000000000FFFF' THEN SUBSTRING(INET6_NTOA(f.remote_ip), 8) ELSE INET6_NTOA(f.remote_ip) END"
)

var vpnFindingColumnExpressions = map[string]string{
	"window_end":             "f.window_end",
	"local_ip":               vpnFindingLocalIPExpression,
	"remote_ip":              vpnFindingRemoteIPExpression,
	"primary_protocol":       "f.primary_protocol",
	"primary_local_port":     "f.primary_local_port",
	"primary_remote_port":    "f.primary_remote_port",
	"local_to_remote_bytes":  "f.local_to_remote_bytes",
	"remote_to_local_bytes":  "f.remote_to_local_bytes",
	"remote_asn":             "f.remote_asn",
	"remote_country":         "f.remote_country",
	"score":                  "f.score",
	"risk_level":             "f.risk_level",
	"verdict":                "f.verdict",
	"disposition":            "f.disposition",
	"probe_status":           "f.probe_status",
	"rule_set_version":       "f.rule_set_version",
	"complete_ratio":         "f.complete_ratio",
	"flow_record_count":      "f.flow_record_count",
	"active_bucket_count":    "f.active_bucket_count",
	"max_duration_ms":        "f.max_duration_ms",
	"source_generation":      "f.source_generation",
	"classification_version": "f.classification_version",
	"dimension_snapshot_id":  "f.dimension_snapshot_id",
	"geo_version":            "f.geo_version",
	"probe_recommended":      "f.probe_recommended",
	"probe_block_reason":     "f.probe_block_reason",
	"decision_rule_id":       "f.decision_rule_id",
	"remote_prefix_id":       "f.remote_prefix_id",
}

var vpnFindingEnumFilters = map[string]map[string]struct{}{
	"risk_level":  stringSet("low", "medium", "high", "critical"),
	"verdict":     stringSet("observe", "review", "probe_candidate", "allowlisted", "suppressed"),
	"disposition": stringSet("unreviewed", "confirmed", "false_positive", "allowed", "suppressed"),
	"probe_status": stringSet(
		"not_requested", "queued", "running", "completed", "failed", "canceled",
	),
}

func stringSet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func (s *MySQLStore) ListVPNFindingsPage(ctx context.Context, tenantID ID, filter VPNFindingListFilter) ([]VPNFinding, int64, error) {
	if err := validateVPNFindingListFilter(&filter); err != nil {
		return nil, 0, err
	}
	where, args, err := vpnFindingWhere(tenantID, filter.Search, filter.From, filter.To, filter.ColumnFilters, "")
	if err != nil {
		return nil, 0, err
	}
	var total int64
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM flow_vpn_findings f WHERE "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	orderColumn := vpnFindingColumnExpressions[filter.SortBy]
	queryArgs := append(append([]any(nil), args...), filter.Limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, vpnFindingSelect()+" WHERE "+where+
		" ORDER BY "+orderColumn+" "+filter.SortDirection+", f.id "+filter.SortDirection+" LIMIT ? OFFSET ?", queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]VPNFinding, 0, filter.Limit)
	for rows.Next() {
		item, err := scanVPNFinding(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *MySQLStore) ListVPNFindingFacets(ctx context.Context, tenantID ID, filter VPNFindingFacetFilter) ([]VPNFindingFacet, error) {
	if err := validateVPNFindingFacetFilter(&filter); err != nil {
		return nil, err
	}
	where, args, err := vpnFindingWhere(tenantID, filter.Query, filter.From, filter.To, filter.ColumnFilters, filter.Field)
	if err != nil {
		return nil, err
	}
	expression := vpnFindingColumnExpressions[filter.Field]
	facetExpression := "CAST(" + expression + " AS CHAR)"
	if filter.Search != "" {
		where += " AND LOCATE(LOWER(?), LOWER(" + facetExpression + ")) > 0"
		args = append(args, filter.Search)
	}
	args = append(args, filter.Limit)
	rows, err := s.db.QueryContext(ctx, "SELECT "+facetExpression+", COUNT(*) FROM flow_vpn_findings f WHERE "+where+
		" GROUP BY "+facetExpression+" ORDER BY COUNT(*) DESC, "+facetExpression+" ASC LIMIT ?", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]VPNFindingFacet, 0, filter.Limit)
	for rows.Next() {
		var item VPNFindingFacet
		if err := rows.Scan(&item.Value, &item.Count); err != nil {
			return nil, err
		}
		if filter.Field == "remote_country" && item.Value == "" {
			item.Value = "_unknown"
		}
		if filter.Field == "window_end" {
			parsed, err := time.ParseInLocation("2006-01-02 15:04:05", item.Value, time.UTC)
			if err != nil {
				return nil, fmt.Errorf("decode VPN finding window facet: %w", err)
			}
			item.Value = parsed.Format(time.RFC3339Nano)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *MySQLStore) GetVPNFinding(ctx context.Context, tenantID, findingID ID) (VPNFinding, error) {
	return scanVPNFinding(s.db.QueryRowContext(ctx, vpnFindingSelect()+" WHERE f.tenant_id = ? AND f.id = ?", tenantID, findingID))
}

func (s *MySQLStore) UpdateVPNFindingDisposition(ctx context.Context, tenantID, findingID ID, disposition, note string, actorID ID, expectedVersion uint64) (VPNFinding, error) {
	disposition = strings.TrimSpace(disposition)
	note = strings.TrimSpace(note)
	if err := validateVPNFindingDisposition(disposition, note); err != nil || actorID == "" || expectedVersion == 0 {
		return VPNFinding{}, errors.New("invalid VPN finding disposition update")
	}
	var actor any = actorID
	var dispositionAt any = time.Now().UTC()
	if disposition == "unreviewed" {
		actor = nil
		dispositionAt = nil
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE flow_vpn_findings
		SET disposition = ?, disposition_note = ?, disposition_by = ?, disposition_at = ?, row_version = row_version + 1
		WHERE tenant_id = ? AND id = ? AND row_version = ?
	`, disposition, note, actor, dispositionAt, tenantID, findingID, expectedVersion)
	if err != nil {
		return VPNFinding{}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return VPNFinding{}, err
	}
	if affected != 1 {
		var exists int
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM flow_vpn_findings WHERE tenant_id = ? AND id = ?", tenantID, findingID).Scan(&exists); err != nil {
			return VPNFinding{}, err
		}
		if exists == 0 {
			return VPNFinding{}, sql.ErrNoRows
		}
		return VPNFinding{}, ErrVPNFindingConflict
	}
	return s.GetVPNFinding(ctx, tenantID, findingID)
}

func validateVPNFindingDisposition(disposition, note string) error {
	if _, ok := vpnFindingEnumFilters["disposition"][strings.TrimSpace(disposition)]; !ok {
		return errors.New("VPN finding disposition is invalid")
	}
	if len(strings.TrimSpace(note)) > 2000 {
		return errors.New("VPN finding disposition note is too long")
	}
	return nil
}

func validateVPNFindingListFilter(filter *VPNFindingListFilter) error {
	if filter.Limit == 0 {
		filter.Limit = 25
	}
	if filter.Limit < 1 || filter.Limit > 100 || filter.Offset < 0 || filter.Offset > 10_000_000 {
		return errors.New("VPN finding limit must be 1..100 and offset must be 0..10000000")
	}
	filter.Search = strings.TrimSpace(filter.Search)
	if len(filter.Search) > 200 {
		return errors.New("VPN finding search is too long")
	}
	if err := validateVPNFindingRange(filter.From, filter.To); err != nil {
		return err
	}
	if err := validateVPNFindingColumnFilters(filter.ColumnFilters); err != nil {
		return err
	}
	if filter.SortBy == "" {
		filter.SortBy = "window_end"
	}
	if _, ok := vpnFindingColumnExpressions[filter.SortBy]; !ok {
		return errors.New("VPN finding sort_by is invalid")
	}
	filter.SortDirection = strings.ToUpper(strings.TrimSpace(filter.SortDirection))
	if filter.SortDirection == "" {
		filter.SortDirection = "DESC"
	}
	if filter.SortDirection != "ASC" && filter.SortDirection != "DESC" {
		return errors.New("VPN finding sort_direction is invalid")
	}
	return nil
}

func validateVPNFindingFacetFilter(filter *VPNFindingFacetFilter) error {
	filter.Field = strings.TrimSpace(filter.Field)
	if _, ok := vpnFindingColumnExpressions[filter.Field]; !ok {
		return errors.New("VPN finding facet field is invalid")
	}
	filter.Search = strings.TrimSpace(filter.Search)
	filter.Query = strings.TrimSpace(filter.Query)
	if len(filter.Search) > 200 || len(filter.Query) > 200 {
		return errors.New("VPN finding facet search is too long")
	}
	if filter.Limit == 0 {
		filter.Limit = 50
	}
	if filter.Limit < 1 || filter.Limit > 100 {
		return errors.New("VPN finding facet limit must be 1..100")
	}
	if err := validateVPNFindingRange(filter.From, filter.To); err != nil {
		return err
	}
	return validateVPNFindingColumnFilters(filter.ColumnFilters)
}

func validateVPNFindingRange(from, to time.Time) error {
	if from.IsZero() != to.IsZero() {
		return errors.New("VPN finding from and to must be provided together")
	}
	if !from.IsZero() && (!to.After(from) || to.Sub(from) > 90*24*time.Hour) {
		return errors.New("VPN finding range must be positive and no longer than 90 days")
	}
	return nil
}

func validateVPNFindingColumnFilters(filters map[string][]string) error {
	if len(filters) > 32 {
		return errors.New("too many VPN finding column filters")
	}
	valueCount := 0
	for field, values := range filters {
		if _, ok := vpnFindingColumnExpressions[field]; !ok {
			return fmt.Errorf("unsupported VPN finding column filter %q", field)
		}
		if len(values) == 0 || len(values) > 20 {
			return fmt.Errorf("VPN finding column filter %q must have 1..20 values", field)
		}
		valueCount += len(values)
		if valueCount > 64 {
			return errors.New("too many VPN finding column filter values")
		}
		for _, value := range values {
			if err := validateVPNFindingFilterValue(field, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateVPNFindingFilterValue(field, value string) error {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 200 {
		return fmt.Errorf("VPN finding column filter %q has an invalid value", field)
	}
	if allowed, ok := vpnFindingEnumFilters[field]; ok {
		if _, exists := allowed[value]; !exists {
			return fmt.Errorf("VPN finding column filter %q has an unsupported value", field)
		}
		return nil
	}
	switch field {
	case "local_ip", "remote_ip":
		if _, err := netip.ParseAddr(value); err != nil {
			return fmt.Errorf("VPN finding column filter %q must be an IP address", field)
		}
	case "window_end":
		if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
			return errors.New("VPN finding window_end filter must be RFC3339")
		}
	case "remote_country":
		if value != "_unknown" && (len(value) != 2 || value != strings.ToUpper(value)) {
			return errors.New("VPN finding remote_country filter must be an uppercase country code or _unknown")
		}
	case "primary_protocol":
		parsed, err := strconv.ParseUint(value, 10, 8)
		if err != nil || parsed == 0 {
			return errors.New("VPN finding primary_protocol filter must be 1..255")
		}
	case "primary_local_port", "primary_remote_port":
		if _, err := strconv.ParseUint(value, 10, 16); err != nil {
			return fmt.Errorf("VPN finding column filter %q must be 0..65535", field)
		}
	case "remote_asn", "active_bucket_count", "classification_version":
		if _, err := strconv.ParseUint(value, 10, 32); err != nil {
			return fmt.Errorf("VPN finding column filter %q must be a 32-bit unsigned integer", field)
		}
	case "score":
		parsed, err := strconv.ParseUint(value, 10, 16)
		if err != nil || parsed > 100 {
			return errors.New("VPN finding score filter must be 0..100")
		}
	case "local_to_remote_bytes", "remote_to_local_bytes", "flow_record_count", "max_duration_ms", "source_generation":
		if _, err := strconv.ParseUint(value, 10, 64); err != nil {
			return fmt.Errorf("VPN finding column filter %q must be an unsigned integer", field)
		}
	case "complete_ratio":
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil || parsed < 0 || parsed > 1 {
			return errors.New("VPN finding complete_ratio filter must be between 0 and 1")
		}
	case "probe_recommended":
		if value != "0" && value != "1" {
			return errors.New("VPN finding probe_recommended filter must be 0 or 1")
		}
	}
	return nil
}

func vpnFindingWhere(tenantID ID, search string, from, to time.Time, filters map[string][]string, excludedField string) (string, []any, error) {
	clauses := []string{"f.tenant_id = ?"}
	args := []any{tenantID}
	if !from.IsZero() {
		clauses = append(clauses, "f.window_end >= ?", "f.window_end < ?")
		args = append(args, from.UTC(), to.UTC())
	}
	if search != "" {
		clauses = append(clauses, "(LOCATE(LOWER(?), LOWER(f.id)) > 0"+
			" OR LOCATE(LOWER(?), LOWER("+vpnFindingLocalIPExpression+")) > 0"+
			" OR LOCATE(LOWER(?), LOWER("+vpnFindingRemoteIPExpression+")) > 0"+
			" OR LOCATE(LOWER(?), LOWER(f.remote_prefix_id)) > 0"+
			" OR LOCATE(LOWER(?), LOWER(f.rule_set_version)) > 0)")
		args = append(args, search, search, search, search, search)
	}
	for field, values := range filters {
		if field == excludedField {
			continue
		}
		expression, ok := vpnFindingColumnExpressions[field]
		if !ok {
			return "", nil, fmt.Errorf("unsupported VPN finding column filter %q", field)
		}
		placeholders := make([]string, 0, len(values))
		for _, value := range values {
			placeholders = append(placeholders, "?")
			args = append(args, vpnFindingFilterArgument(field, value))
		}
		clauses = append(clauses, expression+" IN ("+strings.Join(placeholders, ",")+")")
	}
	return strings.Join(clauses, " AND "), args, nil
}

func vpnFindingFilterArgument(field, value string) any {
	value = strings.TrimSpace(value)
	if field == "remote_country" && value == "_unknown" {
		return ""
	}
	if field == "window_end" {
		parsed, _ := time.Parse(time.RFC3339Nano, value)
		return parsed.UTC()
	}
	return value
}

func vpnFindingSelect() string {
	return `SELECT
		f.id, f.tenant_id, f.window_start, f.window_end, f.conversation_key,
		f.local_ip, f.remote_ip, f.primary_protocol, f.primary_local_port, f.primary_remote_port,
		f.local_to_remote_bytes, f.remote_to_local_bytes, f.flow_record_count, f.active_bucket_count,
		f.max_duration_ms, f.remote_asn, f.remote_country, f.remote_prefix_id, f.complete_ratio,
		f.score, f.risk_level, f.verdict, f.probe_recommended, f.probe_block_reason, f.decision_rule_id,
		f.evidence_schema_version, f.evidence_json, f.rule_set_version, f.dimension_snapshot_id,
		f.geo_version, f.classification_version, f.source_generation, f.generated_at,
		f.disposition, f.disposition_note, f.disposition_by, f.disposition_at,
		f.probe_status, f.probe_job_id, f.probe_result_json, f.expires_at, f.row_version,
		f.created_at, f.updated_at
	FROM flow_vpn_findings f`
}

type vpnFindingScanner interface {
	Scan(...any) error
}

func scanVPNFinding(scanner vpnFindingScanner) (VPNFinding, error) {
	var item VPNFinding
	var conversationKey, localIP, remoteIP []byte
	var dispositionBy, probeJobID sql.NullString
	var dispositionAt sql.NullTime
	if err := scanner.Scan(
		&item.ID, &item.TenantID, &item.WindowStart, &item.WindowEnd, &conversationKey,
		&localIP, &remoteIP, &item.PrimaryProtocol, &item.PrimaryLocalPort, &item.PrimaryRemotePort,
		&item.LocalToRemoteBytes, &item.RemoteToLocalBytes, &item.FlowRecordCount, &item.ActiveBucketCount,
		&item.MaxDurationMS, &item.RemoteASN, &item.RemoteCountry, &item.RemotePrefixID, &item.CompleteRatio,
		&item.Score, &item.RiskLevel, &item.Verdict, &item.ProbeRecommended, &item.ProbeBlockReason, &item.DecisionRuleID,
		&item.EvidenceSchemaVersion, &item.Evidence, &item.RuleSetVersion, &item.DimensionSnapshotID,
		&item.GeoVersion, &item.ClassificationVersion, &item.SourceGeneration, &item.GeneratedAt,
		&item.Disposition, &item.DispositionNote, &dispositionBy, &dispositionAt,
		&item.ProbeStatus, &probeJobID, &item.ProbeResult, &item.ExpiresAt, &item.RowVersion,
		&item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return VPNFinding{}, err
	}
	item.ConversationKey = hex.EncodeToString(conversationKey)
	item.LocalIP = canonicalVPNFindingIP(localIP)
	item.RemoteIP = canonicalVPNFindingIP(remoteIP)
	if dispositionBy.Valid {
		item.DispositionBy = ID(dispositionBy.String)
	}
	if dispositionAt.Valid {
		at := dispositionAt.Time.UTC()
		item.DispositionAt = &at
	}
	if probeJobID.Valid {
		item.ProbeJobID = ID(probeJobID.String)
	}
	return item, nil
}

func canonicalVPNFindingIP(value []byte) string {
	if len(value) != 16 {
		return ""
	}
	var bytes [16]byte
	copy(bytes[:], value)
	return netip.AddrFrom16(bytes).Unmap().String()
}
