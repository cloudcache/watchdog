package address

import (
	"errors"
	"time"
)

const (
	AddressDimensionModuleKey     = "flow"
	AddressDimensionKey           = "address"
	AddressDimensionPublishJob    = "address_dimension_publish"
	AddressSnapshotBuildJob       = "address_snapshot_build"
	AddressDimensionPayloadV1     = 1
	AddressSnapshotObjectFormat   = "wads"
	AddressSnapshotBuilderVersion = "watchdog-addresssnap-v1"

	AddressDimensionStatusActive  = "active"
	AddressDimensionStatusRetired = "retired"

	AddressDimensionApprovalPending  = "pending"
	AddressDimensionApprovalApproved = "approved"
	AddressDimensionApprovalRejected = "rejected"

	AddressDimensionSourceManifestV1 = uint16(1)

	// defaultAddressObjectRetention is how long a retired snapshot's object is
	// kept before GC may reclaim it.
	defaultAddressObjectRetention = 720 * time.Hour
)

var (
	ErrAddressDimensionInvalid      = errors.New("address dimension publication is invalid")
	ErrAddressDimensionDraftChanged = errors.New("address dimension draft changed after preview")
	ErrAddressDimensionConflict     = errors.New("address dimension publication conflicts with another version")
	ErrAddressSnapshotBuildRace     = errors.New("address snapshot build raced another publication")
)

// DimensionPublicationSnapshot is one immutable published generation and its
// lifecycle state. Signature columns are retained but unused in the
// checksum-only build; consumers authenticate by Checksum.
type DimensionPublicationSnapshot struct {
	ID                      ID                       `json:"id"`
	ModuleKey               string                   `json:"module_key"`
	DimensionKey            string                   `json:"dimension_key"`
	Version                 uint64                   `json:"version"`
	EffectiveFrom           time.Time                `json:"effective_from"`
	ObjectRef               string                   `json:"object_ref"`
	ObjectFormat            string                   `json:"object_format"`
	ObjectFormatVersion     uint16                   `json:"object_format_version"`
	BuilderVersion          string                   `json:"builder_version,omitempty"`
	BuildJobID              ID                       `json:"build_job_id,omitempty"`
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
	GeoNodeCount            uint64                   `json:"geo_node_count"`
	EnabledAddressSetCount  uint64                   `json:"enabled_address_set_count"`
	MaxAddressSetsPerRecord uint32                   `json:"max_address_sets_per_record"`
	DefinitionBytes         uint64                   `json:"definition_bytes"`
}

// AddressDimensionSource pins one active immutable import generation.
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
