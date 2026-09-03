package watchdog

import (
	"context"
	"time"
)

type AddressPrefix struct {
	ID        string            `json:"id"`
	TenantID  ID                `json:"tenant_id"`
	CIDR      string            `json:"cidr"`
	Labels    map[string]string `json:"labels"`
	Source    string            `json:"source"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

type AddressSet struct {
	ID             string         `json:"id"`
	TenantID       ID             `json:"tenant_id"`
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	Selector       map[string]any `json:"selector"`
	MatchDirection string         `json:"match_direction"`
	Enabled        bool           `json:"enabled"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

type AddressSetRepository interface {
	ListAddressPrefixes(ctx context.Context, tenantID ID) ([]AddressPrefix, error)
	UpsertAddressPrefix(ctx context.Context, prefix AddressPrefix) (AddressPrefix, error)
	DeleteAddressPrefix(ctx context.Context, tenantID ID, prefixID string) error
	ListAddressSets(ctx context.Context, tenantID ID) ([]AddressSet, error)
	GetAddressSet(ctx context.Context, tenantID ID, setID string) (AddressSet, error)
	UpsertAddressSet(ctx context.Context, set AddressSet) (AddressSet, error)
	DeleteAddressSet(ctx context.Context, tenantID ID, setID string) error
}
