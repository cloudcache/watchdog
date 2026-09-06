package watchdog

import (
	"context"
	"errors"
	"time"
)

const (
	AddressDimensionModuleKey     = "flow"
	AddressDimensionKey           = "address"
	AddressDimensionPublishJob    = "address_dimension_publish"
	AddressDimensionPayloadV1     = 1
	AddressDimensionStatusActive  = "active"
	AddressDimensionStatusRetired = "retired"
)

var (
	ErrAddressDimensionInvalid      = errors.New("address dimension publication is invalid")
	ErrAddressDimensionDraftChanged = errors.New("address dimension draft changed after preview")
	ErrAddressDimensionConflict     = errors.New("address dimension publication conflicts with another version")
)

type AddressDimensionSnapshot struct {
	ID                      ID         `json:"id"`
	TenantID                ID         `json:"tenant_id"`
	ModuleKey               string     `json:"module_key"`
	DimensionKey            string     `json:"dimension_key"`
	Version                 uint64     `json:"version"`
	EffectiveFrom           time.Time  `json:"effective_from"`
	ObjectRef               string     `json:"object_ref"`
	Checksum                string     `json:"checksum"`
	DraftDigest             string     `json:"draft_digest"`
	BundleSchemaVersion     uint32     `json:"bundle_schema_version"`
	EntryCount              uint64     `json:"entry_count"`
	PrefixCount             uint64     `json:"prefix_count"`
	AddressSetCount         uint64     `json:"address_set_count"`
	MaxAddressSetsPerRecord uint32     `json:"max_address_sets_per_record"`
	Status                  string     `json:"status"`
	RowVersion              uint64     `json:"row_version"`
	CreatedBy               ID         `json:"created_by,omitempty"`
	RetiredBy               ID         `json:"retired_by,omitempty"`
	CreatedAt               time.Time  `json:"created_at"`
	RetiredAt               *time.Time `json:"retired_at,omitempty"`
}

type AddressDimensionPreview struct {
	DraftDigest             string    `json:"draft_digest"`
	BundleSchemaVersion     uint32    `json:"bundle_schema_version"`
	EffectiveFrom           time.Time `json:"effective_from"`
	PrefixCount             uint64    `json:"prefix_count"`
	AddressSetCount         uint64    `json:"address_set_count"`
	EnabledAddressSetCount  uint64    `json:"enabled_address_set_count"`
	MaxAddressSetsPerRecord uint32    `json:"max_address_sets_per_record"`
	EstimatedBundleBytes    uint64    `json:"estimated_bundle_bytes"`
}

type AddressDimensionPublishRequest struct {
	EffectiveFrom time.Time `json:"effective_from"`
	PreviewDigest string    `json:"preview_digest"`
}

type AddressDimensionListFilter struct {
	Status string
	Limit  int
	Cursor string
}

type AddressDimensionPublisher interface {
	PreviewAddressDimension(context.Context, ID, time.Time) (AddressDimensionPreview, error)
	PublishAddressDimension(context.Context, ID, ID, AddressDimensionPublishRequest) (AddressDimensionSnapshot, error)
	ListAddressDimensionSnapshots(context.Context, ID, AddressDimensionListFilter) ([]AddressDimensionSnapshot, string, error)
	GetAddressDimensionSnapshot(context.Context, ID, ID) (AddressDimensionSnapshot, error)
}
