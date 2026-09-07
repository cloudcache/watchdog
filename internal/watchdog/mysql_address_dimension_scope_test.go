package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"
)

func TestMySQLDimensionPublicationScopesAreIsolated(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	server, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.PingContext(ctx); err != nil {
		t.Skipf("mysql not reachable: %v", err)
	}

	schema := "watchdog_dimension_scope_" + randomSchemaSuffix(t)
	createScratchSchema(ctx, t, server, schema)
	db := openScratchSchema(t, dsn, schema)
	defer db.Close()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	const tenantID = ID("tenant_dimension_scope")
	if _, err := db.ExecContext(ctx, "INSERT INTO tenants (id, name, status) VALUES (?, 'Dimension Scope', 'active')", tenantID); err != nil {
		t.Fatal(err)
	}
	const actorID = ID("actor_dimension_scope")
	if _, err := db.ExecContext(ctx, `INSERT INTO users
		(id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES (?, ?, 'dimension-scope@test.invalid', 'Dimension Scope', 'active', 'test', 'dimension-scope')`, actorID, tenantID); err != nil {
		t.Fatal(err)
	}
	const (
		addressSnapshotID = ID("snapshot_address_scope")
		vpnSnapshotID     = ID("snapshot_vpn_scope")
	)
	digest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dimension_snapshots (
			id, tenant_id, module_key, dimension_key, version, effective_from,
			object_ref, checksum, draft_digest, source_manifest_version, source_manifest,
			bundle_schema_version, entry_count
		) VALUES
			(?, ?, 'flow', 'address', 1, '2026-09-08 00:00:00.000',
			 'dimension-snapshots/tenant_dimension_scope/snapshot_address_scope/bundle.json',
			 ?, ?, 0, JSON_ARRAY(), 1, 1),
			(?, ?, 'flow', 'vpn_rule_set', 1, '2026-09-08 00:00:00.000',
			 'dimension-snapshots/tenant_dimension_scope/snapshot_vpn_scope/bundle.json',
			 ?, ?, 0, JSON_ARRAY(), 1, 1)
	`, addressSnapshotID, tenantID, digest, digest, vpnSnapshotID, tenantID, digest, digest); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	objects := DiskDimensionObjectStore{Dir: t.TempDir()}
	publisher, err := NewMySQLAddressDimensionPublisher(store, objects)
	if err != nil {
		t.Fatal(err)
	}
	vpnStore, err := newMySQLDimensionPublicationStore(store, objects, DimensionPublicationScope{ModuleKey: "flow", DimensionKey: "vpn_rule_set"})
	if err != nil {
		t.Fatal(err)
	}
	if item, err := publisher.GetAddressDimensionSnapshot(ctx, tenantID, vpnSnapshotID); !errors.Is(err, sql.ErrNoRows) || item.ID != "" {
		t.Fatalf("cross-kind get returned item=%+v err=%v", item, err)
	}
	if item, err := vpnStore.GetDimensionPublicationSnapshot(ctx, tenantID, addressSnapshotID); !errors.Is(err, sql.ErrNoRows) || item.ID != "" {
		t.Fatalf("reverse cross-kind get returned item=%+v err=%v", item, err)
	}
	if item, err := publisher.RejectAddressDimension(ctx, tenantID, actorID, vpnSnapshotID, 1, "not an address snapshot"); !errors.Is(err, sql.ErrNoRows) || item.ID != "" {
		t.Fatalf("cross-kind lifecycle returned item=%+v err=%v", item, err)
	}
	rejected, err := vpnStore.RejectDimensionPublication(ctx, tenantID, actorID, vpnSnapshotID, 1, "test scope isolation")
	if err != nil {
		t.Fatal(err)
	}
	if rejected.ApprovalState != AddressDimensionApprovalRejected || rejected.RowVersion != 2 {
		t.Fatalf("unexpected VPN snapshot after reject: %+v", rejected)
	}
	address, err := publisher.GetAddressDimensionSnapshot(ctx, tenantID, addressSnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if address.ApprovalState != AddressDimensionApprovalPending || address.RowVersion != 1 {
		t.Fatalf("VPN lifecycle modified address snapshot: %+v", address)
	}
}
