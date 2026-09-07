package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var ErrVPNFindingConflict = errors.New("VPN finding changed since read")

type VPNFinding struct {
	ID                    ID              `json:"id"`
	TenantID              ID              `json:"tenant_id,omitempty"`
	WindowStart           time.Time       `json:"window_start"`
	WindowEnd             time.Time       `json:"window_end"`
	ConversationKey       string          `json:"conversation_key"`
	LocalIP               string          `json:"local_ip"`
	RemoteIP              string          `json:"remote_ip"`
	PrimaryProtocol       uint8           `json:"primary_protocol"`
	PrimaryLocalPort      uint16          `json:"primary_local_port"`
	PrimaryRemotePort     uint16          `json:"primary_remote_port"`
	LocalToRemoteBytes    uint64          `json:"local_to_remote_bytes"`
	RemoteToLocalBytes    uint64          `json:"remote_to_local_bytes"`
	FlowRecordCount       uint64          `json:"flow_record_count"`
	ActiveBucketCount     uint32          `json:"active_bucket_count"`
	MaxDurationMS         uint64          `json:"max_duration_ms"`
	RemoteASN             uint32          `json:"remote_asn"`
	RemoteCountry         string          `json:"remote_country"`
	RemotePrefixID        string          `json:"remote_prefix_id"`
	CompleteRatio         float64         `json:"complete_ratio"`
	Score                 uint16          `json:"score"`
	RiskLevel             string          `json:"risk_level"`
	Verdict               string          `json:"verdict"`
	ProbeRecommended      bool            `json:"probe_recommended"`
	ProbeBlockReason      string          `json:"probe_block_reason"`
	DecisionRuleID        string          `json:"decision_rule_id"`
	EvidenceSchemaVersion uint16          `json:"evidence_schema_version"`
	Evidence              json.RawMessage `json:"evidence"`
	RuleSetVersion        string          `json:"rule_set_version"`
	DimensionSnapshotID   string          `json:"dimension_snapshot_id"`
	GeoVersion            string          `json:"geo_version"`
	ClassificationVersion uint64          `json:"classification_version"`
	SourceGeneration      uint64          `json:"source_generation"`
	GeneratedAt           time.Time       `json:"generated_at"`
	Disposition           string          `json:"disposition"`
	DispositionNote       string          `json:"disposition_note"`
	DispositionBy         ID              `json:"disposition_by,omitempty"`
	DispositionAt         *time.Time      `json:"disposition_at,omitempty"`
	ProbeStatus           string          `json:"probe_status"`
	ProbeJobID            ID              `json:"probe_job_id,omitempty"`
	ProbeResult           json.RawMessage `json:"probe_result"`
	ExpiresAt             time.Time       `json:"expires_at"`
	RowVersion            uint64          `json:"row_version"`
	CreatedAt             time.Time       `json:"created_at"`
	UpdatedAt             time.Time       `json:"updated_at"`
}

type VPNFindingListFilter struct {
	Search        string
	From          time.Time
	To            time.Time
	ColumnFilters map[string][]string
	SortBy        string
	SortDirection string
	Limit         int
	Offset        int
}

type VPNFindingFacet struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type VPNFindingFacetFilter struct {
	Field         string
	Search        string
	Query         string
	From          time.Time
	To            time.Time
	ColumnFilters map[string][]string
	Limit         int
}

type VPNFindingRepository interface {
	ListVPNFindingsPage(context.Context, ID, VPNFindingListFilter) ([]VPNFinding, int64, error)
	ListVPNFindingFacets(context.Context, ID, VPNFindingFacetFilter) ([]VPNFindingFacet, error)
	GetVPNFinding(context.Context, ID, ID) (VPNFinding, error)
	UpdateVPNFindingDisposition(context.Context, ID, ID, string, string, ID, uint64) (VPNFinding, error)
}
