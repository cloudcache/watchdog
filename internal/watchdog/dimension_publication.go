// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package watchdog

import (
	"context"
	"errors"
	"strings"
	"time"
)

// DimensionPublicationScope is a server-owned authorization and persistence
// boundary. API clients never supply it.
type DimensionPublicationScope struct {
	ModuleKey    string
	DimensionKey string
}

func (p *MySQLDimensionPublicationStore) GetDimensionPublicationSnapshot(ctx context.Context, tenantID, snapshotID ID) (DimensionPublicationSnapshot, error) {
	if p == nil || p.store == nil || tenantID == "" || snapshotID == "" || p.scope.validate() != nil {
		return DimensionPublicationSnapshot{}, ErrAddressDimensionInvalid
	}
	return scanAddressDimensionSnapshot(p.store.db.QueryRowContext(ctx, `SELECT `+addressDimensionSnapshotColumns+`
		FROM dimension_snapshots
		WHERE tenant_id = ? AND module_key = ? AND dimension_key = ? AND id = ?`,
		tenantID, p.scope.ModuleKey, p.scope.DimensionKey, snapshotID))
}

var addressDimensionPublicationScope = DimensionPublicationScope{
	ModuleKey: AddressDimensionModuleKey, DimensionKey: AddressDimensionKey,
}

func (scope DimensionPublicationScope) validate() error {
	for _, value := range []string{scope.ModuleKey, scope.DimensionKey} {
		if value == "" || len(value) > 64 || strings.TrimSpace(value) != value {
			return errors.New("dimension publication scope is invalid")
		}
		for _, character := range value {
			if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '_' || character == '-' {
				continue
			}
			return errors.New("dimension publication scope is invalid")
		}
	}
	return nil
}

// MySQLDimensionPublicationStore owns the shared immutable publication
// persistence dependencies. Typed modules embed it and provide their own
// compiler and HTTP contract.
type MySQLDimensionPublicationStore struct {
	store           *MySQLStore
	objects         DimensionObjectStore
	objectRetention time.Duration
	now             func() time.Time
	scope           DimensionPublicationScope
}

func newMySQLDimensionPublicationStore(store *MySQLStore, objects DimensionObjectStore, scope DimensionPublicationScope) (*MySQLDimensionPublicationStore, error) {
	if store == nil || store.db == nil || objects == nil {
		return nil, errors.New("MySQL store and dimension object store are required")
	}
	if err := scope.validate(); err != nil {
		return nil, err
	}
	return &MySQLDimensionPublicationStore{
		store: store, objects: objects, objectRetention: defaultAddressObjectRetention,
		now: func() time.Time { return time.Now().UTC() }, scope: scope,
	}, nil
}
