package address

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
	ID             string     `json:"id"`
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
	CreatedBy      ID         `json:"created_by,omitempty"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorDetail    string     `json:"error_detail,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	ReadyAt        *time.Time `json:"ready_at,omitempty"`
	ActivatedAt    *time.Time `json:"activated_at,omitempty"`
}

type AddressImportSlot struct {
	SourceSlot  string    `json:"source_slot"`
	ImportID    ID        `json:"import_id"`
	RowVersion  uint64    `json:"row_version"`
	ActivatedBy ID        `json:"activated_by,omitempty"`
	ActivatedAt time.Time `json:"activated_at"`
}

type AddressImportListFilter struct {
	SourceSlot string
	Status     string
	Format     string
	Search     string
	Sort       string
	Desc       bool
	Limit      int
	Offset     int
	Cursor     string
	TableMode  bool
}

type AddressBasePrefix struct {
	ID              uint64            `json:"id"`
	ImportID        ID                `json:"import_id"`
	Family          uint8             `json:"family"`
	PrefixLength    uint8             `json:"prefix_length"`
	CIDR            string            `json:"cidr"`
	ContinentCode   string            `json:"continent_code,omitempty"`
	CountryCode     string            `json:"country_code,omitempty"`
	CountryName     string            `json:"country_name,omitempty"`
	SubdivisionCode string            `json:"subdivision_code,omitempty"`
	SubdivisionName string            `json:"subdivision_name,omitempty"`
	CityCode        string            `json:"city_code,omitempty"`
	CityName        string            `json:"city_name,omitempty"`
	ASN             *uint32           `json:"asn,omitempty"`
	OperatorName    string            `json:"operator_name,omitempty"`
	Latitude        *float64          `json:"latitude,omitempty"`
	Longitude       *float64          `json:"longitude,omitempty"`
	Labels          map[string]string `json:"labels"`
	CreatedAt       time.Time         `json:"created_at"`
}

type AddressBasePrefixFilter struct {
	Family      uint8
	CountryCode string
	ASN         *uint32
	Operator    string
	Search      string
	Sort        string
	Desc        bool
	Limit       int
	Offset      int
	Cursor      string
	TableMode   bool
}

// AddressImportRepository is the de-tenanted import persistence contract.
type AddressImportRepository interface {
	CreateAddressImport(context.Context, AddressImport) (AddressImport, error)
	GetAddressImport(ctx context.Context, importID ID) (AddressImport, error)
	ListAddressImports(ctx context.Context, filter AddressImportListFilter) ([]AddressImport, string, int, error)
	BeginAddressImport(ctx context.Context, importID ID) error
	InsertAddressImportBatch(ctx context.Context, importID ID, records []AddressImportRecord) error
	CompleteAddressImport(ctx context.Context, importID ID, metadata AddressImportMetadata, language string) (AddressImport, error)
	FailAddressImport(ctx context.Context, importID ID, code, detail string) error
	GetAddressImportSlot(ctx context.Context, sourceSlot string) (AddressImportSlot, error)
	ActivateAddressImport(ctx context.Context, importID, actorID ID, expectedVersion uint64) (AddressImportSlot, error)
	ListAddressBasePrefixes(ctx context.Context, importID ID, filter AddressBasePrefixFilter) ([]AddressBasePrefix, string, int, error)
	LookupAddressBasePrefixes(ctx context.Context, importID ID, address string, limit int) ([]AddressBasePrefix, error)
}
