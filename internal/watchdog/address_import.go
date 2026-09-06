package watchdog

import (
	"context"
	"errors"
	"time"
)

const (
	AddressImportSlotGeo      = "geo"
	AddressImportSlotASN      = "asn"
	AddressImportSlotCombined = "combined"

	AddressImportFormatMMDB = "mmdb"
	AddressImportFormatIPDB = "ipdb"

	AddressImportStatusQuarantined = "quarantined"
	AddressImportStatusQueued      = "queued"
	AddressImportStatusImporting   = "importing"
	AddressImportStatusReady       = "ready"
	AddressImportStatusFailed      = "failed"
	AddressImportStatusRetired     = "retired"
)

var (
	ErrAddressImportInvalid         = errors.New("address import is invalid")
	ErrAddressImportNotWritable     = errors.New("address import generation is not writable")
	ErrAddressImportVersionConflict = errors.New("address import slot version conflict")
)

// AddressImport is one immutable source generation. The uploaded artifact is
// content-addressed by ChecksumSHA256; rows are written only while Status is
// importing and become immutable when the generation reaches ready.
type AddressImport struct {
	ID             ID         `json:"id"`
	TenantID       ID         `json:"tenant_id"`
	SourceSlot     string     `json:"source_slot"`
	Format         string     `json:"format"`
	OriginalName   string     `json:"original_name"`
	ArtifactRef    string     `json:"artifact_ref"`
	ChecksumSHA256 string     `json:"checksum_sha256"`
	SizeBytes      uint64     `json:"size_bytes"`
	DatabaseType   string     `json:"database_type,omitempty"`
	BuildEpoch     *int64     `json:"build_epoch,omitempty"`
	IPVersion      *uint8     `json:"ip_version,omitempty"`
	Language       string     `json:"language,omitempty"`
	Status         string     `json:"status"`
	RowCountV4     uint64     `json:"row_count_v4"`
	RowCountV6     uint64     `json:"row_count_v6"`
	CreatedBy      ID         `json:"created_by"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorDetail    string     `json:"error_detail,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	ReadyAt        *time.Time `json:"ready_at,omitempty"`
	ActivatedAt    *time.Time `json:"activated_at,omitempty"`
}

type AddressImportSlot struct {
	TenantID    ID        `json:"tenant_id"`
	SourceSlot  string    `json:"source_slot"`
	ImportID    ID        `json:"import_id"`
	RowVersion  uint64    `json:"row_version"`
	ActivatedBy ID        `json:"activated_by"`
	ActivatedAt time.Time `json:"activated_at"`
}

type AddressImportListFilter struct {
	SourceSlot string
	Status     string
	Limit      int
	Cursor     string
}

type AddressImportRepository interface {
	CreateAddressImport(context.Context, AddressImport) (AddressImport, error)
	GetAddressImport(ctx context.Context, tenantID, importID ID) (AddressImport, error)
	ListAddressImports(ctx context.Context, tenantID ID, filter AddressImportListFilter) ([]AddressImport, string, error)
	BeginAddressImport(ctx context.Context, tenantID, importID ID) error
	InsertAddressImportBatch(ctx context.Context, tenantID, importID ID, records []AddressImportRecord) error
	CompleteAddressImport(ctx context.Context, tenantID, importID ID, metadata AddressImportMetadata, language string) (AddressImport, error)
	FailAddressImport(ctx context.Context, tenantID, importID ID, code, detail string) error
	GetAddressImportSlot(ctx context.Context, tenantID ID, sourceSlot string) (AddressImportSlot, error)
	ActivateAddressImport(ctx context.Context, tenantID, importID, actorID ID, expectedVersion uint64) (AddressImportSlot, error)
}
