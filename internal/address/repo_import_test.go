package address

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"
)

// Pure unit port: importer output must be canonical (network address at the
// prefix boundary) before it reaches storage.
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
	return addressNumberFromAddr(netip.AddrFrom16(value))
}

// Integration port (de-tenanted) of the generation lifecycle + CAS: idempotent
// begin/batch replay, base-prefix keyset + table pagination, longest-prefix
// lookup, write-after-ready rejection, slot activation CAS, and failure states.
func TestStoreAddressImportGenerationLifecycleAndCAS(t *testing.T) {
	db := addressTestDB(t)
	store := NewStore(db)
	ctx := context.Background()
	const actorID = ID("user_addr_import_test")
	seedAddressTestUser(t, db, actorID)

	first, err := store.CreateAddressImport(ctx, addressImportFixture("import_addr_test_first", actorID, AddressImportSlotCombined, "first.mmdb"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != AddressImportStatusQueued {
		t.Fatalf("created status = %q", first.Status)
	}
	if err := store.BeginAddressImport(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	// A crash-takeover may call Begin again; it is intentionally idempotent.
	if err := store.BeginAddressImport(ctx, first.ID); err != nil {
		t.Fatalf("repeat begin: %v", err)
	}
	records := []AddressImportRecord{
		{Prefix: "192.0.2.0/24", CountryCode: "CN", CountryName: "China", SubdivisionCode: "BJ", SubdivisionName: "Beijing", CityName: "Beijing", ASN: 4134, Operator: "China Telecom", Source: "mmdb"},
		{Prefix: "192.0.2.0/25", CountryCode: "CN", CountryName: "China", SubdivisionCode: "BJ", SubdivisionName: "Beijing", CityName: "Haidian", ASN: 4134, Operator: "China Telecom", Source: "mmdb"},
		{Prefix: "2001:db8::/126", CountryCode: "US", ASN: 64496, Source: "mmdb"},
	}
	if err := store.InsertAddressImportBatch(ctx, first.ID, records); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAddressImportBatch(ctx, first.ID, records); err != nil {
		t.Fatalf("idempotent batch replay: %v", err)
	}
	metadata := AddressImportMetadata{Format: AddressImportFormatMMDB, DatabaseType: "GeoLite2-City", BuildTime: time.Unix(1_700_000_000, 0), IPVersion: 6}
	first, err = store.CompleteAddressImport(ctx, first.ID, metadata, "en")
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != AddressImportStatusReady || first.RowCountV4 != 2 || first.RowCountV6 != 1 || first.BuildEpoch == nil || *first.BuildEpoch != 1_700_000_000 {
		t.Fatalf("completed import = %#v", first)
	}
	asn := uint32(4134)
	prefixes, prefixCursor, _, err := store.ListAddressBasePrefixes(ctx, first.ID, AddressBasePrefixFilter{
		Family: 4, CountryCode: "cn", ASN: &asn, Operator: "China Telecom", Search: "Beijing", Limit: 1,
	})
	if err != nil || len(prefixes) != 1 || prefixCursor == "" || prefixes[0].Labels["source"] != "mmdb" {
		t.Fatalf("prefix first page=%#v cursor=%q err=%v", prefixes, prefixCursor, err)
	}
	prefixes, _, _, err = store.ListAddressBasePrefixes(ctx, first.ID, AddressBasePrefixFilter{Family: 4, Cursor: prefixCursor, Limit: 1})
	if err != nil || len(prefixes) != 1 {
		t.Fatalf("prefix second page=%#v err=%v", prefixes, err)
	}
	matches, err := store.LookupAddressBasePrefixes(ctx, first.ID, "192.0.2.42", 20)
	if err != nil || len(matches) != 2 || matches[0].CIDR != "192.0.2.0/25" || matches[1].CIDR != "192.0.2.0/24" {
		t.Fatalf("lookup matches=%#v err=%v", matches, err)
	}
	if err := store.InsertAddressImportBatch(ctx, first.ID, records[:1]); !errors.Is(err, ErrAddressImportNotWritable) {
		t.Fatalf("write after ready error = %v", err)
	}
	slot, err := store.ActivateAddressImport(ctx, first.ID, actorID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if slot.ImportID != first.ID || slot.RowVersion != 1 {
		t.Fatalf("first slot = %#v", slot)
	}

	second, err := store.CreateAddressImport(ctx, addressImportFixture("import_addr_test_second", actorID, AddressImportSlotCombined, "second.ipdb"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BeginAddressImport(ctx, second.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAddressImportBatch(ctx, second.ID, records[:1]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteAddressImport(ctx, second.ID, AddressImportMetadata{Format: AddressImportFormatIPDB, DatabaseType: "ipip-city", IPVersion: 4}, "CN"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivateAddressImport(ctx, second.ID, actorID, 0); !errors.Is(err, ErrAddressImportVersionConflict) {
		t.Fatalf("stale activation error = %v", err)
	}
	slot, err = store.ActivateAddressImport(ctx, second.ID, actorID, 1)
	if err != nil || slot.RowVersion != 2 || slot.ImportID != second.ID {
		t.Fatalf("second activation = %#v, err=%v", slot, err)
	}

	items, cursor, _, err := store.ListAddressImports(ctx, AddressImportListFilter{SourceSlot: AddressImportSlotCombined, Status: AddressImportStatusReady, Limit: 1})
	if err != nil || len(items) != 1 || cursor == "" {
		t.Fatalf("first page = %#v cursor=%q err=%v", items, cursor, err)
	}
	firstPageID := items[0].ID
	items, _, _, err = store.ListAddressImports(ctx, AddressImportListFilter{SourceSlot: AddressImportSlotCombined, Status: AddressImportStatusReady, Limit: 1, Cursor: cursor})
	if err != nil || len(items) != 1 || items[0].ID == firstPageID {
		t.Fatalf("second page = %#v err=%v", items, err)
	}
	items, cursor, total, err := store.ListAddressImports(ctx, AddressImportListFilter{
		Search: "second", SourceSlot: AddressImportSlotCombined, Status: AddressImportStatusReady,
		Format: AddressImportFormatIPDB, Sort: "name", Desc: true, Limit: 25, Offset: 0, TableMode: true,
	})
	if err != nil || len(items) != 1 || items[0].ID != second.ID || cursor != "" || total != 1 {
		t.Fatalf("server import page = %#v cursor=%q total=%d err=%v", items, cursor, total, err)
	}
	basePrefixes, baseCursor, baseTotal, err := store.ListAddressBasePrefixes(ctx, first.ID, AddressBasePrefixFilter{
		Family: 4, CountryCode: "CN", Sort: "cidr", Desc: true, Limit: 1, Offset: 1, TableMode: true,
	})
	if err != nil || len(basePrefixes) != 1 || baseCursor != "" || baseTotal != 2 {
		t.Fatalf("server prefix page = %#v cursor=%q total=%d err=%v", basePrefixes, baseCursor, baseTotal, err)
	}

	failed, err := store.CreateAddressImport(ctx, addressImportFixture("import_addr_test_failed", actorID, AddressImportSlotGeo, "failed.mmdb"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FailAddressImport(ctx, failed.ID, "DECODE", "bad fixture"); err != nil {
		t.Fatal(err)
	}
	if err := store.FailAddressImport(ctx, failed.ID, "DECODE", "bad fixture"); err != nil {
		t.Fatalf("idempotent fail: %v", err)
	}
	if _, err := store.ActivateAddressImport(ctx, failed.ID, actorID, 0); !errors.Is(err, ErrAddressImportNotWritable) {
		t.Fatalf("failed activation error = %v", err)
	}
}

func addressImportFixture(id string, actorID ID, slot, name string) AddressImport {
	format := AddressImportFormatMMDB
	if strings.HasSuffix(name, ".ipdb") {
		format = AddressImportFormatIPDB
	}
	return AddressImport{
		ID: ID(id), SourceSlot: slot, Format: format,
		OriginalName: name, ArtifactRef: "address-imports/" + id + "/source", ChecksumSHA256: strings.Repeat("a", 64),
		SizeBytes: 1024, Status: AddressImportStatusQueued, CreatedBy: actorID,
	}
}
