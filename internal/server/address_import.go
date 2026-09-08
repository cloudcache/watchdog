package server

import (
	"errors"
	"strings"
	"time"
)

// Address import source slots, formats, and lifecycle states. An upload lands as
// queued, the background runner flips it to importing while it streams rows, then
// ready on success or failed on error. Only a ready generation can be activated
// into its slot; activation is what a snapshot build reads from.
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
	errAddressImportInvalid         = errors.New("address import is invalid")
	errAddressImportNotWritable     = errors.New("address import generation is not writable")
	errAddressImportVersionConflict = errors.New("address import slot version conflict")
)

// AddressImport is one immutable source generation. The uploaded artifact is
// content-addressed by ChecksumSHA256; base-prefix rows are written only while
// Status is importing and become immutable when the generation reaches ready.
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
	CreatedBy      string     `json:"created_by,omitempty"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorDetail    string     `json:"error_detail,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	ReadyAt        *time.Time `json:"ready_at,omitempty"`
	ActivatedAt    *time.Time `json:"activated_at,omitempty"`
}

// AddressImportSlot names the ready generation that currently backs a slot.
type AddressImportSlot struct {
	SourceSlot  string    `json:"source_slot"`
	ImportID    string    `json:"import_id"`
	RowVersion  uint64    `json:"row_version"`
	ActivatedBy string    `json:"activated_by,omitempty"`
	ActivatedAt time.Time `json:"activated_at"`
}

// AddressBasePrefix is one decoded source prefix with its geo/operator/ASN
// attribution. Rows are immutable once their import is ready.
type AddressBasePrefix struct {
	ID              uint64            `json:"id"`
	ImportID        string            `json:"import_id"`
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

type addressImportListFilter struct {
	SourceSlot string
	Status     string
	Format     string
	Search     string
	Limit      int
	Offset     int
}

type addressBasePrefixFilter struct {
	Family      uint8
	CountryCode string
	ASN         *uint32
	Operator    string
	Search      string
	Limit       int
	Offset      int
}

func validAddressImportSlot(value string) bool {
	return value == AddressImportSlotGeo || value == AddressImportSlotASN || value == AddressImportSlotCombined
}

// escapeSQLLike escapes the LIKE metacharacters so user search text matches
// literally under the default backslash escape.
func escapeSQLLike(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "%", "\\%")
	value = strings.ReplaceAll(value, "_", "\\_")
	return value
}
