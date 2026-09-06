package watchdog

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	_ "github.com/go-sql-driver/mysql"
)

func TestMySQLAddressDimensionPreviewPublishAndDraftCAS(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run address dimension integration test")
	}
	ctx := context.Background()
	store, err := OpenMySQLStore(ctx, MySQLConfig{DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := ApplyMySQLMigrations(ctx, store.db); err != nil {
		t.Fatal(err)
	}
	tenantID := ID("01JADDRESSDIMENSIONTENANT1")
	userID := ID("01JADDRESSDIMENSIONUSER001")
	_, _ = store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	defer store.db.ExecContext(ctx, `DELETE FROM tenants WHERE id = ?`, tenantID)
	if _, err := store.db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Address Dimension', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id) VALUES (?, ?, 'dimension@test.invalid', 'Dimension', 'active', 'test', 'dimension-test')`, userID, tenantID); err != nil {
		t.Fatal(err)
	}
	prefix, err := store.UpsertAddressPrefix(ctx, AddressPrefix{
		ID: "00000000-0000-4000-8000-000000000001", TenantID: tenantID, CIDR: "10.0.0.0/8",
		Labels: map[string]string{"flow": "local", "business": "private"}, Source: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	objects := DiskDimensionObjectStore{Dir: t.TempDir(), MaxBytes: 1 << 20}
	publisher, err := NewMySQLAddressDimensionPublisher(store, objects)
	if err != nil {
		t.Fatal(err)
	}
	effective := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	preview, err := publisher.PreviewAddressDimension(ctx, tenantID, effective)
	if err != nil {
		t.Fatal(err)
	}
	prefix.Labels["business"] = "changed"
	if _, err := store.UpdateAddressPrefix(ctx, prefix, prefix.RowVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective, PreviewDigest: preview.DraftDigest}); !errors.Is(err, ErrAddressDimensionDraftChanged) {
		t.Fatalf("expected draft CAS error, got %v", err)
	}
	preview, err = publisher.PreviewAddressDimension(ctx, tenantID, effective)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := publisher.PublishAddressDimension(ctx, tenantID, userID, AddressDimensionPublishRequest{EffectiveFrom: effective, PreviewDigest: preview.DraftDigest})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != 1 || snapshot.PrefixCount != 1 || snapshot.Status != AddressDimensionStatusActive {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	path, err := objects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := flowdimension.DecodeAndCompileBundle(data, snapshot.Checksum, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	classified := compiled.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("203.0.113.7"))
	if classified.Direction != flowdimension.DirectionOut || classified.Business != "changed" {
		t.Fatalf("published bundle classification = %#v", classified)
	}
	items, cursor, err := publisher.ListAddressDimensionSnapshots(ctx, tenantID, AddressDimensionListFilter{Limit: 10})
	if err != nil || len(items) != 1 || cursor != "" || items[0].ID != snapshot.ID {
		t.Fatalf("list snapshots = %#v %q %v", items, cursor, err)
	}
}
