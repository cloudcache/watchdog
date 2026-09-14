// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package address

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

// TestStoreDeleteAddressImport: a non-active generation deletes (returning its
// artifact ref and cascading its base prefixes); an active generation is refused;
// a missing id reports not-found. Opt-in via WATCHDOG_TEST_MYSQL_DSN.
func TestStoreDeleteAddressImport(t *testing.T) {
	db := addressTestDB(t)
	store := NewStore(db)
	ctx := context.Background()
	actor := ID("user_del_test")
	seedAddressTestUser(t, db, actor)

	ready := func(id string) {
		t.Helper()
		if _, err := store.CreateAddressImport(ctx, AddressImport{
			ID: id, SourceSlot: AddressImportSlotCombined, Format: AddressImportFormatMMDB,
			OriginalName: id + ".mmdb", ArtifactRef: "address-imports/" + id + "/source.mmdb",
			ChecksumSHA256: strings.Repeat("a", 64), SizeBytes: 1024, Status: AddressImportStatusQueued, CreatedBy: actor,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.BeginAddressImport(ctx, id); err != nil {
			t.Fatal(err)
		}
		if err := store.InsertAddressImportBatch(ctx, id, []AddressImportRecord{
			{Prefix: "192.0.2.0/24", CountryCode: "CN", Source: "mmdb"},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.CompleteAddressImport(ctx, id, AddressImportMetadata{Format: AddressImportFormatMMDB, IPVersion: 4}, "en"); err != nil {
			t.Fatal(err)
		}
	}

	// A non-active generation deletes and returns its artifact ref; the row and its
	// cascaded base prefixes are gone.
	ready("import_del_ok")
	ref, err := store.DeleteAddressImport(ctx, "import_del_ok")
	if err != nil {
		t.Fatalf("delete non-active: %v", err)
	}
	if ref != "address-imports/import_del_ok/source.mmdb" {
		t.Fatalf("artifact ref = %q", ref)
	}
	if _, err := store.GetAddressImport(ctx, "import_del_ok"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("import should be gone, got %v", err)
	}
	if prefixes, _, _, err := store.ListAddressBasePrefixes(ctx, "import_del_ok", AddressBasePrefixFilter{Limit: 10}); err != nil || len(prefixes) != 0 {
		t.Fatalf("base prefixes should cascade-delete: n=%d err=%v", len(prefixes), err)
	}

	// An active generation (backing a slot) is refused.
	ready("import_del_active")
	if _, err := store.ActivateAddressImport(ctx, "import_del_active", actor, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteAddressImport(ctx, "import_del_active"); !errors.Is(err, ErrAddressImportActive) {
		t.Fatalf("delete active = %v, want ErrAddressImportActive", err)
	}

	// A missing id reports not-found.
	if _, err := store.DeleteAddressImport(ctx, "import_del_missing"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("delete missing = %v, want sql.ErrNoRows", err)
	}
}
