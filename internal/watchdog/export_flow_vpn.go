package watchdog

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

const (
	FlowVPNFindingsDataset                  = "flow.vpn_findings"
	vpnFindingExportCompletenessMode        = "mysql_snapshot_query"
	maxVPNFindingExportRows          uint32 = 250_000
)

// VPNFindingExportRepository exposes one bounded, stable SQL result to the
// asynchronous export worker. Implementations return at most limit+1 rows so
// the worker can reject truncation instead of silently producing a partial file.
type VPNFindingExportRepository interface {
	ListVPNFindingsForExport(context.Context, ID, VPNFindingListFilter, uint32) ([]VPNFinding, error)
}

type vpnFindingExportParameters struct {
	Search        string              `json:"search,omitempty"`
	ColumnFilters map[string][]string `json:"column_filters,omitempty"`
	SortBy        string              `json:"sort_by"`
	SortDirection string              `json:"sort_direction"`
}

// VPNFindingExportRow is intentionally not the persistence model. Evidence,
// probe result, conversation key, disposition note/actor and tenant identity
// are omitted from artifacts even though the worker needs the source record.
type VPNFindingExportRow struct {
	ID                    ID
	WindowStart           time.Time
	WindowEnd             time.Time
	LocalIP               string
	RemoteIP              string
	PrimaryProtocol       uint8
	PrimaryLocalPort      uint16
	PrimaryRemotePort     uint16
	LocalToRemoteBytes    uint64
	RemoteToLocalBytes    uint64
	FlowRecordCount       uint64
	ActiveBucketCount     uint32
	MaxDurationMS         uint64
	RemoteASN             uint32
	RemoteCountry         string
	RemotePrefixID        string
	CompleteRatio         float64
	Score                 uint16
	RiskLevel             string
	Verdict               string
	ProbeRecommended      bool
	ProbeBlockReason      string
	DecisionRuleID        string
	EvidenceSchemaVersion uint16
	EvidencePresent       bool
	RuleSetVersion        string
	DimensionSnapshotID   string
	GeoVersion            string
	ClassificationVersion uint64
	SourceGeneration      uint64
	GeneratedAt           time.Time
	Disposition           string
	ProbeStatus           string
	ProbeResultPresent    bool
	ExpiresAt             time.Time
	RowVersion            uint64
}

func prepareVPNFindingExportTask(auth AuthContext, input flowVPNFindingExportCreateRequest) (ExportTask, error) {
	if !canExportVPNFindings(auth) {
		return ExportTask{}, errors.New("VPN finding export permission is required")
	}
	if input.Format != ExportFormatCSV && input.Format != ExportFormatParquet {
		return ExportTask{}, errors.New("VPN finding export format must be csv or parquet")
	}
	limit := input.Limit
	if limit == 0 {
		limit = maxVPNFindingExportRows
	}
	if limit > maxVPNFindingExportRows {
		return ExportTask{}, errors.New("VPN finding export limit must be 1..250000")
	}
	parameters, filter, err := normalizeVPNFindingExportParameters(vpnFindingExportParameters{
		Search: input.Search, ColumnFilters: input.ColumnFilters,
		SortBy: input.SortBy, SortDirection: input.SortDirection,
	})
	if err != nil {
		return ExportTask{}, err
	}
	if input.From.IsZero() || input.To.IsZero() {
		return ExportTask{}, errors.New("VPN finding export from and to are required")
	}
	filter.From, filter.To = input.From.UTC(), input.To.UTC()
	if err := validateVPNFindingRange(filter.From, filter.To); err != nil {
		return ExportTask{}, err
	}
	parameterJSON, err := json.Marshal(parameters)
	if err != nil {
		return ExportTask{}, err
	}
	query, err := normalizeQueryRequest(QueryRequest{
		Dataset: FlowVPNFindingsDataset, From: filter.From, To: filter.To,
		Limit: limit, ValueLayer: QueryValueCustomer, Parameters: parameterJSON,
	})
	if err != nil {
		return ExportTask{}, err
	}
	queryJSON, queryHash, err := canonicalJSONHash(exportQuerySnapshot{
		SchemaVersion: 1, Query: query, Aggregation: AggregationAverageFiveMinute,
	})
	if err != nil {
		return ExportTask{}, err
	}
	_, descriptorHash, err := canonicalJSONHash(struct {
		Dataset        string   `json:"dataset"`
		SchemaVersion  int      `json:"schema_version"`
		MaximumRows    uint32   `json:"maximum_rows"`
		RedactedFields []string `json:"redacted_fields"`
	}{
		Dataset: FlowVPNFindingsDataset, SchemaVersion: 1, MaximumRows: maxVPNFindingExportRows,
		RedactedFields: []string{"conversation_key", "evidence", "probe_result", "disposition_note", "disposition_by", "tenant_id"},
	})
	if err != nil {
		return ExportTask{}, err
	}
	versionsJSON, _, err := canonicalJSONHash(exportVersionSnapshot{
		SchemaVersion: 1, DatasetDescriptorHash: descriptorHash,
		CompletenessMissingRatio: "0", CompletenessMode: vpnFindingExportCompletenessMode, SnapshotComplete: true,
	})
	if err != nil {
		return ExportTask{}, err
	}
	resource := ResourceRef{Type: ResourceTenant, ID: auth.TenantID}
	authorizationJSON, _, err := canonicalJSONHash(exportAuthorizationSnapshot{
		SchemaVersion: 1, SubjectID: auth.UserID, RequiredAction: ActionVPNExport,
		ResourceType: resource.Type, ResourceID: resource.ID, Admin: auth.IsAdmin,
	})
	if err != nil {
		return ExportTask{}, err
	}
	retentionSeconds := input.RetentionSeconds
	if retentionSeconds == 0 {
		retentionSeconds = defaultExportRetentionSeconds
	}
	task := ExportTask{
		TenantID: auth.TenantID, CreatedBy: auth.UserID, ContractVersion: ExportExecutionContractVersion,
		DatasetKey: FlowVPNFindingsDataset, QueryJSON: queryJSON, QueryHash: queryHash,
		ValueLayer: QueryValueCustomer, VersionsJSON: versionsJSON, AuthorizationJSON: authorizationJSON,
		RetentionSeconds: retentionSeconds, PeriodType: PeriodCustom, RangeStart: query.From, RangeEnd: query.To,
		Aggregation: AggregationAverageFiveMinute, ValueMode: ExportValueCorrected,
		Format: input.Format, Status: ExportStatusPending,
	}
	return task, validateExportExecutionTask(task)
}

func normalizeVPNFindingExportParameters(input vpnFindingExportParameters) (vpnFindingExportParameters, VPNFindingListFilter, error) {
	input.Search = strings.TrimSpace(input.Search)
	normalizedFilters := make(map[string][]string, len(input.ColumnFilters))
	for field, values := range input.ColumnFilters {
		field = strings.TrimSpace(field)
		normalized := make([]string, 0, len(values))
		seen := make(map[string]struct{}, len(values))
		for _, value := range values {
			value = strings.TrimSpace(value)
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			normalized = append(normalized, value)
		}
		sort.Strings(normalized)
		normalizedFilters[field] = normalized
	}
	filter := VPNFindingListFilter{
		Search: input.Search, ColumnFilters: normalizedFilters,
		SortBy: strings.TrimSpace(input.SortBy), SortDirection: strings.TrimSpace(input.SortDirection), Limit: 100,
	}
	if err := validateVPNFindingListFilter(&filter); err != nil {
		return vpnFindingExportParameters{}, VPNFindingListFilter{}, err
	}
	input.ColumnFilters = normalizedFilters
	input.SortBy, input.SortDirection = filter.SortBy, filter.SortDirection
	return input, filter, nil
}

func (p QueryGatewayExportDataProvider) loadVPNFindingExportRows(ctx context.Context, task ExportTask) (ExportRows, error) {
	if p.VPNFindings == nil {
		return ExportRows{}, errors.New("VPN finding export repository is required")
	}
	var snapshot exportQuerySnapshot
	if err := decodeStrictJSON(task.QueryJSON, &snapshot); err != nil {
		return ExportRows{}, err
	}
	var parameters vpnFindingExportParameters
	if err := decodeStrictJSON(snapshot.Query.Parameters, &parameters); err != nil {
		return ExportRows{}, err
	}
	_, filter, err := normalizeVPNFindingExportParameters(parameters)
	if err != nil {
		return ExportRows{}, err
	}
	filter.From, filter.To = snapshot.Query.From, snapshot.Query.To
	items, err := p.VPNFindings.ListVPNFindingsForExport(ctx, task.TenantID, filter, snapshot.Query.Limit)
	if err != nil {
		return ExportRows{}, err
	}
	if uint32(len(items)) > snapshot.Query.Limit {
		return ExportRows{}, errors.New("VPN finding export exceeds its frozen row limit")
	}
	if len(items) == 0 {
		return ExportRows{}, errors.New("no VPN findings returned by the frozen query")
	}
	rows := make([]VPNFindingExportRow, 0, len(items))
	for _, item := range items {
		rows = append(rows, vpnFindingExportRow(item))
	}
	return ExportRows{VPNFindings: rows}, nil
}

func vpnFindingExportRow(item VPNFinding) VPNFindingExportRow {
	return VPNFindingExportRow{
		ID: item.ID, WindowStart: item.WindowStart, WindowEnd: item.WindowEnd,
		LocalIP: item.LocalIP, RemoteIP: item.RemoteIP, PrimaryProtocol: item.PrimaryProtocol,
		PrimaryLocalPort: item.PrimaryLocalPort, PrimaryRemotePort: item.PrimaryRemotePort,
		LocalToRemoteBytes: item.LocalToRemoteBytes, RemoteToLocalBytes: item.RemoteToLocalBytes,
		FlowRecordCount: item.FlowRecordCount, ActiveBucketCount: item.ActiveBucketCount, MaxDurationMS: item.MaxDurationMS,
		RemoteASN: item.RemoteASN, RemoteCountry: item.RemoteCountry, RemotePrefixID: item.RemotePrefixID,
		CompleteRatio: item.CompleteRatio, Score: item.Score, RiskLevel: item.RiskLevel, Verdict: item.Verdict,
		ProbeRecommended: item.ProbeRecommended, ProbeBlockReason: item.ProbeBlockReason,
		DecisionRuleID: item.DecisionRuleID, EvidenceSchemaVersion: item.EvidenceSchemaVersion,
		EvidencePresent: jsonDocumentPresent(item.Evidence), RuleSetVersion: item.RuleSetVersion,
		DimensionSnapshotID: item.DimensionSnapshotID, GeoVersion: item.GeoVersion,
		ClassificationVersion: item.ClassificationVersion, SourceGeneration: item.SourceGeneration,
		GeneratedAt: item.GeneratedAt, Disposition: item.Disposition, ProbeStatus: item.ProbeStatus,
		ProbeResultPresent: jsonDocumentPresent(item.ProbeResult), ExpiresAt: item.ExpiresAt, RowVersion: item.RowVersion,
	}
}

func jsonDocumentPresent(value json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(value))
	return trimmed != "" && trimmed != "null" && trimmed != "{}" && trimmed != "[]"
}

var vpnFindingExportHeader = []string{
	"finding_id", "window_start", "window_end", "local_ip", "remote_ip", "primary_protocol",
	"primary_local_port", "primary_remote_port", "local_to_remote_bytes", "remote_to_local_bytes",
	"flow_record_count", "active_bucket_count", "max_duration_ms", "remote_asn", "remote_country",
	"remote_prefix_id", "complete_ratio", "score", "risk_level", "verdict", "probe_recommended",
	"probe_block_reason", "decision_rule_id", "evidence_schema_version", "evidence_present", "rule_set_version",
	"dimension_snapshot_id", "geo_version", "classification_version", "source_generation", "generated_at",
	"disposition", "probe_status", "probe_result_present", "expires_at", "row_version",
}

func RenderVPNFindingCSV(rows []VPNFindingExportRow) ([]byte, error) {
	var output bytes.Buffer
	writer := csv.NewWriter(&output)
	if err := writer.Write(vpnFindingExportHeader); err != nil {
		return nil, err
	}
	for _, row := range rows {
		record := []string{
			safeSpreadsheetCell(string(row.ID)), row.WindowStart.UTC().Format(time.RFC3339Nano), row.WindowEnd.UTC().Format(time.RFC3339Nano),
			safeSpreadsheetCell(row.LocalIP), safeSpreadsheetCell(row.RemoteIP), strconv.FormatUint(uint64(row.PrimaryProtocol), 10),
			strconv.FormatUint(uint64(row.PrimaryLocalPort), 10), strconv.FormatUint(uint64(row.PrimaryRemotePort), 10),
			strconv.FormatUint(row.LocalToRemoteBytes, 10), strconv.FormatUint(row.RemoteToLocalBytes, 10),
			strconv.FormatUint(row.FlowRecordCount, 10), strconv.FormatUint(uint64(row.ActiveBucketCount), 10), strconv.FormatUint(row.MaxDurationMS, 10),
			strconv.FormatUint(uint64(row.RemoteASN), 10), safeSpreadsheetCell(row.RemoteCountry), safeSpreadsheetCell(row.RemotePrefixID),
			strconv.FormatFloat(row.CompleteRatio, 'f', -1, 64), strconv.FormatUint(uint64(row.Score), 10), safeSpreadsheetCell(row.RiskLevel),
			safeSpreadsheetCell(row.Verdict), strconv.FormatBool(row.ProbeRecommended), safeSpreadsheetCell(row.ProbeBlockReason),
			safeSpreadsheetCell(row.DecisionRuleID), strconv.FormatUint(uint64(row.EvidenceSchemaVersion), 10), strconv.FormatBool(row.EvidencePresent),
			safeSpreadsheetCell(row.RuleSetVersion), safeSpreadsheetCell(row.DimensionSnapshotID), safeSpreadsheetCell(row.GeoVersion),
			strconv.FormatUint(row.ClassificationVersion, 10), strconv.FormatUint(row.SourceGeneration, 10), row.GeneratedAt.UTC().Format(time.RFC3339Nano),
			safeSpreadsheetCell(row.Disposition), safeSpreadsheetCell(row.ProbeStatus), strconv.FormatBool(row.ProbeResultPresent),
			row.ExpiresAt.UTC().Format(time.RFC3339Nano), strconv.FormatUint(row.RowVersion, 10),
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	return output.Bytes(), writer.Error()
}

type vpnFindingExportParquetRow struct {
	ID                    string  `parquet:"finding_id,dict"`
	WindowStart           int64   `parquet:"window_start,timestamp(millisecond:utc)"`
	WindowEnd             int64   `parquet:"window_end,timestamp(millisecond:utc)"`
	LocalIP               string  `parquet:"local_ip,dict"`
	RemoteIP              string  `parquet:"remote_ip,dict"`
	PrimaryProtocol       uint8   `parquet:"primary_protocol"`
	PrimaryLocalPort      uint16  `parquet:"primary_local_port"`
	PrimaryRemotePort     uint16  `parquet:"primary_remote_port"`
	LocalToRemoteBytes    uint64  `parquet:"local_to_remote_bytes"`
	RemoteToLocalBytes    uint64  `parquet:"remote_to_local_bytes"`
	FlowRecordCount       uint64  `parquet:"flow_record_count"`
	ActiveBucketCount     uint32  `parquet:"active_bucket_count"`
	MaxDurationMS         uint64  `parquet:"max_duration_ms"`
	RemoteASN             uint32  `parquet:"remote_asn"`
	RemoteCountry         string  `parquet:"remote_country,dict"`
	RemotePrefixID        string  `parquet:"remote_prefix_id,dict"`
	CompleteRatio         float64 `parquet:"complete_ratio"`
	Score                 uint16  `parquet:"score"`
	RiskLevel             string  `parquet:"risk_level,dict"`
	Verdict               string  `parquet:"verdict,dict"`
	ProbeRecommended      bool    `parquet:"probe_recommended"`
	ProbeBlockReason      string  `parquet:"probe_block_reason,dict"`
	DecisionRuleID        string  `parquet:"decision_rule_id,dict"`
	EvidenceSchemaVersion uint16  `parquet:"evidence_schema_version"`
	EvidencePresent       bool    `parquet:"evidence_present"`
	RuleSetVersion        string  `parquet:"rule_set_version,dict"`
	DimensionSnapshotID   string  `parquet:"dimension_snapshot_id,dict"`
	GeoVersion            string  `parquet:"geo_version,dict"`
	ClassificationVersion uint64  `parquet:"classification_version"`
	SourceGeneration      uint64  `parquet:"source_generation"`
	GeneratedAt           int64   `parquet:"generated_at,timestamp(millisecond:utc)"`
	Disposition           string  `parquet:"disposition,dict"`
	ProbeStatus           string  `parquet:"probe_status,dict"`
	ProbeResultPresent    bool    `parquet:"probe_result_present"`
	ExpiresAt             int64   `parquet:"expires_at,timestamp(millisecond:utc)"`
	RowVersion            uint64  `parquet:"row_version"`
}

func RenderVPNFindingParquet(rows []VPNFindingExportRow) ([]byte, error) {
	items := make([]vpnFindingExportParquetRow, 0, len(rows))
	for _, row := range rows {
		items = append(items, vpnFindingExportParquetRow{
			ID: string(row.ID), WindowStart: row.WindowStart.UnixMilli(), WindowEnd: row.WindowEnd.UnixMilli(),
			LocalIP: row.LocalIP, RemoteIP: row.RemoteIP, PrimaryProtocol: row.PrimaryProtocol,
			PrimaryLocalPort: row.PrimaryLocalPort, PrimaryRemotePort: row.PrimaryRemotePort,
			LocalToRemoteBytes: row.LocalToRemoteBytes, RemoteToLocalBytes: row.RemoteToLocalBytes,
			FlowRecordCount: row.FlowRecordCount, ActiveBucketCount: row.ActiveBucketCount, MaxDurationMS: row.MaxDurationMS,
			RemoteASN: row.RemoteASN, RemoteCountry: row.RemoteCountry, RemotePrefixID: row.RemotePrefixID,
			CompleteRatio: row.CompleteRatio, Score: row.Score, RiskLevel: row.RiskLevel, Verdict: row.Verdict,
			ProbeRecommended: row.ProbeRecommended, ProbeBlockReason: row.ProbeBlockReason, DecisionRuleID: row.DecisionRuleID,
			EvidenceSchemaVersion: row.EvidenceSchemaVersion, EvidencePresent: row.EvidencePresent,
			RuleSetVersion: row.RuleSetVersion, DimensionSnapshotID: row.DimensionSnapshotID, GeoVersion: row.GeoVersion,
			ClassificationVersion: row.ClassificationVersion, SourceGeneration: row.SourceGeneration,
			GeneratedAt: row.GeneratedAt.UnixMilli(), Disposition: row.Disposition, ProbeStatus: row.ProbeStatus,
			ProbeResultPresent: row.ProbeResultPresent, ExpiresAt: row.ExpiresAt.UnixMilli(), RowVersion: row.RowVersion,
		})
	}
	var output bytes.Buffer
	if err := parquet.Write(&output, items); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func canExportVPNFindings(auth AuthContext) bool {
	if auth.IsAdmin {
		return true
	}
	return HasPermission(AccessRequest{
		TenantID: auth.TenantID, UserID: auth.UserID, RoleIDs: auth.RoleIDs, Action: ActionVPNExport,
		Resource: ResourceRef{Type: ResourceTenant, ID: auth.TenantID},
	}, auth.Grants)
}
