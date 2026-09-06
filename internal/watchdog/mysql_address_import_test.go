package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPrepareAddressImportBatchCanonicalizesStorageBounds(t *testing.T) {
	latitude := 39.9042
	batch, err := prepareAddressImportBatch([]AddressImportRecord{{
		Prefix: "192.0.2.0/24", CountryCode: "CN", Latitude: &latitude, Source: "mmdb",
	}, {
		Prefix: "2001:db8::/126", CountryCode: "US", Source: "ipdb",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0].family != 4 || batch[0].bits != 24 || batch[1].family != 6 || batch[1].bits != 126 {
		t.Fatalf("batch = %#v", batch)
	}
	if got := addressFromNumber(addressNumberFromBytes(batch[0].start), 4).String(); got != "192.0.2.0" {
		t.Fatalf("v4 start = %s", got)
	}
	if got := addressFromNumber(addressNumberFromBytes(batch[0].end), 4).String(); got != "192.0.2.255" {
		t.Fatalf("v4 end = %s", got)
	}
	if _, err := prepareAddressImportBatch([]AddressImportRecord{{Prefix: "192.0.2.1/24", Source: "mmdb"}}); err == nil {
		t.Fatal("non-canonical importer output must be rejected")
	}
}

func addressNumberFromBytes(value [16]byte) addressNumber {
	return addressNumberFromAddr(addressFromBytes(value))
}

func addressFromBytes(value [16]byte) netip.Addr {
	return netip.AddrFrom16(value)
}

func TestMySQLAddressImportGenerationLifecycleAndCAS(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run address import repository integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}

	const tenantID = ID("tenant_addr_import_test")
	const actorID = ID("user_addr_import_test")
	cleanupAddressImportFixture(t, db, tenantID)
	defer cleanupAddressImportFixture(t, db, tenantID)
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Address import test', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES (?, ?, 'address-import@test.invalid', 'Address import test', 'active', 'test', 'address-import-test')
	`, actorID, tenantID); err != nil {
		t.Fatal(err)
	}

	store := NewMySQLStore(db)
	first, err := store.CreateAddressImport(ctx, addressImportFixture("import_addr_test_first", tenantID, actorID, AddressImportSlotCombined, "first.mmdb"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != AddressImportStatusQueued {
		t.Fatalf("created status = %q", first.Status)
	}
	if err := store.BeginAddressImport(ctx, tenantID, first.ID); err != nil {
		t.Fatal(err)
	}
	// A takeover may call Begin again. It is intentionally idempotent.
	if err := store.BeginAddressImport(ctx, tenantID, first.ID); err != nil {
		t.Fatalf("repeat begin: %v", err)
	}
	records := []AddressImportRecord{
		{Prefix: "192.0.2.0/24", CountryCode: "CN", CountryName: "China", ASN: 4134, Operator: "China Telecom", Source: "mmdb"},
		{Prefix: "2001:db8::/126", CountryCode: "US", ASN: 64496, Source: "mmdb"},
	}
	if err := store.InsertAddressImportBatch(ctx, tenantID, first.ID, records); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAddressImportBatch(ctx, tenantID, first.ID, records); err != nil {
		t.Fatalf("idempotent batch replay: %v", err)
	}
	metadata := AddressImportMetadata{Format: AddressImportFormatMMDB, DatabaseType: "GeoLite2-City", BuildTime: time.Unix(1_700_000_000, 0), IPVersion: 6}
	first, err = store.CompleteAddressImport(ctx, tenantID, first.ID, metadata, "en")
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != AddressImportStatusReady || first.RowCountV4 != 1 || first.RowCountV6 != 1 || first.BuildEpoch == nil || *first.BuildEpoch != 1_700_000_000 {
		t.Fatalf("completed import = %#v", first)
	}
	if err := store.InsertAddressImportBatch(ctx, tenantID, first.ID, records[:1]); !errors.Is(err, ErrAddressImportNotWritable) {
		t.Fatalf("write after ready error = %v", err)
	}
	slot, err := store.ActivateAddressImport(ctx, tenantID, first.ID, actorID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if slot.ImportID != first.ID || slot.RowVersion != 1 {
		t.Fatalf("first slot = %#v", slot)
	}

	second, err := store.CreateAddressImport(ctx, addressImportFixture("import_addr_test_second", tenantID, actorID, AddressImportSlotCombined, "second.ipdb"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BeginAddressImport(ctx, tenantID, second.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAddressImportBatch(ctx, tenantID, second.ID, records[:1]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteAddressImport(ctx, tenantID, second.ID, AddressImportMetadata{Format: AddressImportFormatIPDB, DatabaseType: "ipip-city", IPVersion: 4}, "CN"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivateAddressImport(ctx, tenantID, second.ID, actorID, 0); !errors.Is(err, ErrAddressImportVersionConflict) {
		t.Fatalf("stale activation error = %v", err)
	}
	slot, err = store.ActivateAddressImport(ctx, tenantID, second.ID, actorID, 1)
	if err != nil || slot.RowVersion != 2 || slot.ImportID != second.ID {
		t.Fatalf("second activation = %#v, err=%v", slot, err)
	}

	items, cursor, err := store.ListAddressImports(ctx, tenantID, AddressImportListFilter{SourceSlot: AddressImportSlotCombined, Status: AddressImportStatusReady, Limit: 1})
	if err != nil || len(items) != 1 || cursor == "" {
		t.Fatalf("first page = %#v cursor=%q err=%v", items, cursor, err)
	}
	firstPageID := items[0].ID
	items, _, err = store.ListAddressImports(ctx, tenantID, AddressImportListFilter{SourceSlot: AddressImportSlotCombined, Status: AddressImportStatusReady, Limit: 1, Cursor: cursor})
	if err != nil || len(items) != 1 || items[0].ID == firstPageID {
		t.Fatalf("second page = %#v err=%v", items, err)
	}

	failed, err := store.CreateAddressImport(ctx, addressImportFixture("import_addr_test_failed", tenantID, actorID, AddressImportSlotGeo, "failed.mmdb"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FailAddressImport(ctx, tenantID, failed.ID, "DECODE", "bad fixture"); err != nil {
		t.Fatal(err)
	}
	if err := store.FailAddressImport(ctx, tenantID, failed.ID, "DECODE", "bad fixture"); err != nil {
		t.Fatalf("idempotent fail: %v", err)
	}
	if _, err := store.ActivateAddressImport(ctx, tenantID, failed.ID, actorID, 0); !errors.Is(err, ErrAddressImportNotWritable) {
		t.Fatalf("failed activation error = %v", err)
	}
}

func addressImportFixture(id string, tenantID, actorID ID, slot, name string) AddressImport {
	format := AddressImportFormatMMDB
	if strings.HasSuffix(name, ".ipdb") {
		format = AddressImportFormatIPDB
	}
	return AddressImport{
		ID: ID(id), TenantID: tenantID, SourceSlot: slot, Format: format,
		OriginalName: name, ArtifactRef: "address-imports/" + id + "/source", ChecksumSHA256: strings.Repeat("a", 64),
		SizeBytes: 1024, Status: AddressImportStatusQueued, CreatedBy: actorID,
	}
}

func cleanupAddressImportFixture(t *testing.T, db *sql.DB, tenantID ID) {
	t.Helper()
	if _, err := db.Exec("DELETE FROM tenants WHERE id = ?", tenantID); err != nil {
		t.Fatalf("cleanup address import fixture: %v", err)
	}
}
