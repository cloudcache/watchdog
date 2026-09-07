package watchdog

import (
	"context"
	"errors"
	"time"
)

const (
	AddressDimensionModuleKey            = "flow"
	AddressDimensionKey                  = "address"
	AddressDimensionPublishJob           = "address_dimension_publish"
	AddressDimensionPayloadV1            = 1
	AddressDimensionStatusActive         = "active"
	AddressDimensionStatusRetired        = "retired"
	AddressDimensionApprovalPending      = "pending"
	AddressDimensionApprovalApproved     = "approved"
	AddressDimensionApprovalRejected     = "rejected"
	AddressDimensionSourceManifestV1     = uint16(1)
	DimensionPublicationSigningPayloadV2 = uint16(2)
	AddressDimensionSigningPayloadV2     = DimensionPublicationSigningPayloadV2
)

var (
	ErrAddressDimensionInvalid      = errors.New("address dimension publication is invalid")
	ErrAddressDimensionDraftChanged = errors.New("address dimension draft changed after preview")
	ErrAddressDimensionConflict     = errors.New("address dimension publication conflicts with another version")
)

type DimensionPublicationSnapshot struct {
	ID                      ID                       `json:"id"`
	TenantID                ID                       `json:"tenant_id"`
	ModuleKey               string                   `json:"module_key"`
	DimensionKey            string                   `json:"dimension_key"`
	Version                 uint64                   `json:"version"`
	EffectiveFrom           time.Time                `json:"effective_from"`
	ObjectRef               string                   `json:"object_ref"`
	Checksum                string                   `json:"checksum"`
	DraftDigest             string                   `json:"draft_digest"`
	SourceManifestVersion   uint16                   `json:"source_manifest_version"`
	SourceManifest          []AddressDimensionSource `json:"source_manifest"`
	SourcePrefixCount       uint64                   `json:"source_prefix_count"`
	BundleSchemaVersion     uint32                   `json:"bundle_schema_version"`
	EntryCount              uint64                   `json:"entry_count"`
	PrefixCount             uint64                   `json:"prefix_count"`
	AddressSetCount         uint64                   `json:"address_set_count"`
	MaxAddressSetsPerRecord uint32                   `json:"max_address_sets_per_record"`
	Status                  string                   `json:"status"`
	ApprovalState           string                   `json:"approval_state"`
	DecidedBy               ID                       `json:"decided_by,omitempty"`
	DecidedAt               *time.Time               `json:"decided_at,omitempty"`
	DecisionReason          string                   `json:"decision_reason,omitempty"`
	SignatureAlgorithm      string                   `json:"signature_algorithm,omitempty"`
	SigningKeyID            string                   `json:"signing_key_id,omitempty"`
	Signature               []byte                   `json:"signature,omitempty"`
	SignedAt                *time.Time               `json:"signed_at,omitempty"`
	RetentionUntil          *time.Time               `json:"retention_until,omitempty"`
	ObjectDeletedAt         *time.Time               `json:"object_deleted_at,omitempty"`
	RowVersion              uint64                   `json:"row_version"`
	CreatedBy               ID                       `json:"created_by,omitempty"`
	RetiredBy               ID                       `json:"retired_by,omitempty"`
	CreatedAt               time.Time                `json:"created_at"`
	RetiredAt               *time.Time               `json:"retired_at,omitempty"`
}

type AddressDimensionSnapshot = DimensionPublicationSnapshot

type AddressDimensionPreview struct {
	DraftDigest             string                   `json:"draft_digest"`
	SourceManifestVersion   uint16                   `json:"source_manifest_version"`
	SourceManifest          []AddressDimensionSource `json:"source_manifest"`
	SourcePrefixCount       uint64                   `json:"source_prefix_count"`
	BundleSchemaVersion     uint32                   `json:"bundle_schema_version"`
	EffectiveFrom           time.Time                `json:"effective_from"`
	PrefixCount             uint64                   `json:"prefix_count"`
	AddressSetCount         uint64                   `json:"address_set_count"`
	OperatorCount           uint64                   `json:"operator_count"`
	EnabledAddressSetCount  uint64                   `json:"enabled_address_set_count"`
	MaxAddressSetsPerRecord uint32                   `json:"max_address_sets_per_record"`
	EstimatedBundleBytes    uint64                   `json:"estimated_bundle_bytes"`
}

// AddressDimensionSource pins one active immutable import generation. The
// source rows are not copied into the definition object; an index builder reads
// them by ImportID and verifies ChecksumSHA256 before applying manual overlays.
type AddressDimensionSource struct {
	Slot           string `json:"slot"`
	ImportID       ID     `json:"import_id"`
	SlotRowVersion uint64 `json:"slot_row_version"`
	ChecksumSHA256 string `json:"checksum_sha256"`
	RowCountV4     uint64 `json:"row_count_v4"`
	RowCountV6     uint64 `json:"row_count_v6"`
}

type AddressDimensionPublishRequest struct {
	EffectiveFrom time.Time `json:"effective_from"`
	PreviewDigest string    `json:"preview_digest"`
}

type AddressDimensionListFilter struct {
	Status    string
	Search    string
	Sort      string
	Desc      bool
	Limit     int
	Offset    int
	Cursor    string
	TableMode bool
}

type DimensionPublicationActivation struct {
	ID                   ID        `json:"id"`
	TenantID             ID        `json:"tenant_id"`
	ModuleKey            string    `json:"module_key"`
	DimensionKey         string    `json:"dimension_key"`
	SnapshotID           ID        `json:"snapshot_id"`
	EffectiveFrom        time.Time `json:"effective_from"`
	Reason               string    `json:"reason"`
	RollbackOfSnapshotID ID        `json:"rollback_of_snapshot_id,omitempty"`
	CreatedBy            ID        `json:"created_by,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
}

type AddressDimensionActivation = DimensionPublicationActivation

type DimensionPublicationAcknowledgement struct {
	TenantID        ID         `json:"tenant_id"`
	SnapshotID      ID         `json:"snapshot_id"`
	WorkerID        string     `json:"worker_id"`
	BootID          string     `json:"boot_id"`
	SoftwareVersion string     `json:"software_version"`
	Checksum        string     `json:"checksum"`
	State           string     `json:"state"`
	AttemptedAt     time.Time  `json:"attempted_at"`
	InstalledAt     *time.Time `json:"installed_at,omitempty"`
	ErrorCode       string     `json:"error_code,omitempty"`
	ErrorMessage    string     `json:"error_message,omitempty"`
	RowVersion      uint64     `json:"row_version"`
}

type AddressDimensionAcknowledgement = DimensionPublicationAcknowledgement

type DimensionPublicationReference struct {
	TenantID       ID        `json:"tenant_id"`
	SnapshotID     ID        `json:"snapshot_id"`
	ConsumerKind   string    `json:"consumer_kind"`
	ConsumerID     string    `json:"consumer_id"`
	MinEventTime   time.Time `json:"min_event_time"`
	MaxEventTime   time.Time `json:"max_event_time"`
	RetainUntil    time.Time `json:"retain_until"`
	LastObservedAt time.Time `json:"last_observed_at"`
	RowVersion     uint64    `json:"row_version"`
}

type AddressDimensionReference = DimensionPublicationReference

type AddressDimensionPublisher interface {
	PreviewAddressDimension(context.Context, ID, time.Time) (AddressDimensionPreview, error)
	PublishAddressDimension(context.Context, ID, ID, AddressDimensionPublishRequest) (AddressDimensionSnapshot, error)
	ListAddressDimensionSnapshots(context.Context, ID, AddressDimensionListFilter) ([]AddressDimensionSnapshot, string, int, error)
	GetAddressDimensionSnapshot(context.Context, ID, ID) (AddressDimensionSnapshot, error)
}

type AddressDimensionLifecycle interface {
	ApproveAddressDimension(context.Context, ID, ID, uint64, AddressDimensionApproval) (AddressDimensionSnapshot, error)
	RejectAddressDimension(context.Context, ID, ID, ID, uint64, string) (AddressDimensionSnapshot, error)
	ActivateAddressDimension(context.Context, ID, ID, AddressDimensionActivationRequest) (AddressDimensionActivation, error)
	RollbackAddressDimension(context.Context, ID, ID, AddressDimensionRollbackRequest) (AddressDimensionActivation, error)
	RetireAddressDimension(context.Context, ID, ID, AddressDimensionRetireRequest) (AddressDimensionSnapshot, error)
	GetAddressDimensionActivationAt(context.Context, ID, time.Time) (AddressDimensionActivation, error)
	ReportAddressDimensionAcknowledgement(context.Context, AddressDimensionAcknowledgement) (AddressDimensionAcknowledgement, error)
	ReportAddressDimensionReference(context.Context, AddressDimensionReference) (AddressDimensionReference, error)
}
