package address

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// DimensionPublicationScope is a server-owned persistence boundary. It is fixed
// to the single address dimension in this build (module_key/dimension_key are
// NOT tenant, so they are kept — only tenant scoping was removed).
type DimensionPublicationScope struct {
	ModuleKey    string
	DimensionKey string
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

// Publisher owns the immutable publication persistence + the WADS build/commit.
// De-tenanted port of MySQLAddressDimensionPublisher/MySQLDimensionPublicationStore.
type Publisher struct {
	store           *Store
	objects         DimensionObjectStore
	objectRetention time.Duration
	now             func() time.Time
	scope           DimensionPublicationScope
}

type PublisherOption func(*Publisher) error

func WithObjectRetention(retention time.Duration) PublisherOption {
	return func(p *Publisher) error {
		if retention <= 0 {
			return errors.New("address dimension object retention must be positive")
		}
		p.objectRetention = retention
		return nil
	}
}

// WithClock overrides the publisher clock so tests get deterministic
// observed_at / attempted_at / retirement timestamps. Faithful de-tenant port of
// the legacy withAddressDimensionClock option.
func WithClock(now func() time.Time) PublisherOption {
	return func(p *Publisher) error {
		if now == nil {
			return errors.New("address dimension publisher clock is required")
		}
		p.now = now
		return nil
	}
}

// NewPublisher builds the address dimension publisher over a Store and object store.
func NewPublisher(store *Store, objects DimensionObjectStore, options ...PublisherOption) (*Publisher, error) {
	if store == nil || store.db == nil || objects == nil {
		return nil, errors.New("store and dimension object store are required")
	}
	if err := addressDimensionPublicationScope.validate(); err != nil {
		return nil, err
	}
	p := &Publisher{
		store: store, objects: objects, objectRetention: defaultAddressObjectRetention,
		now: func() time.Time { return time.Now().UTC() }, scope: addressDimensionPublicationScope,
	}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("publisher option is required")
		}
		if err := option(p); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *Publisher) GetDimensionPublicationSnapshot(ctx context.Context, snapshotID ID) (DimensionPublicationSnapshot, error) {
	if p == nil || p.store == nil || snapshotID == "" || p.scope.validate() != nil {
		return DimensionPublicationSnapshot{}, ErrAddressDimensionInvalid
	}
	return scanAddressDimensionSnapshot(p.store.db.QueryRowContext(ctx, `SELECT `+addressDimensionSnapshotColumns+`
		FROM dimension_snapshots
		WHERE module_key = ? AND dimension_key = ? AND id = ?`,
		p.scope.ModuleKey, p.scope.DimensionKey, snapshotID))
}

func (p *Publisher) GetAddressDimensionSnapshot(ctx context.Context, snapshotID ID) (AddressDimensionSnapshot, error) {
	if p == nil {
		return AddressDimensionSnapshot{}, ErrAddressDimensionInvalid
	}
	return p.GetDimensionPublicationSnapshot(ctx, snapshotID)
}

// lockDimensionPublication serializes publish/activation globally. In the
// de-tenanted build the single-tenant analog of the per-tenant row lock is the
// watchdog_installation singleton row.
func lockDimensionPublication(ctx context.Context, tx *sql.Tx) error {
	var id int
	return tx.QueryRowContext(ctx, `SELECT id FROM watchdog_installation WHERE id = 1 FOR UPDATE`).Scan(&id)
}

func insertAddressDimensionAudit(ctx context.Context, tx *sql.Tx, actorID, snapshotID ID, action string, detail map[string]any) error {
	return insertAddressDimensionResourceAudit(ctx, tx, actorID, "dimension_snapshot", snapshotID, action, time.Now().UTC(), detail)
}

// insertAddressDimensionResourceAudit writes an audit row with an explicit resource
// type and occurrence time. The GC destruction receipt uses resource
// "dimension_object" / action "dimension_object.destroyed" at the deletion instant,
// faithfully mirroring the legacy recordDestructionReceipt.
func insertAddressDimensionResourceAudit(ctx context.Context, tx *sql.Tx, actorID ID, resource, resourceID, action string, occurredAt time.Time, detail map[string]any) error {
	payload, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	id, err := newManagementID()
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO audit_logs (id, actor_id, action, resource, resource_id, detail_json, occurred_at)
		VALUES (?, NULLIF(?, ''), ?, ?, ?, ?, ?)
	`, id, actorID, action, resource, resourceID, payload, occurredAt); err != nil {
		return fmt.Errorf("insert address dimension audit: %w", err)
	}
	return nil
}
