package address

import (
	"context"
	"strings"
	"testing"
)

// Effective-view building blocks (opt-in via WATCHDOG_TEST_MYSQL_DSN): bulk
// reassign upserts corrections over the base, and the correction lookup resolves
// operator/geo names for overlay — re-reassign updates in place, not duplicates.
func TestBulkReassignAndCorrectionOverlay(t *testing.T) {
	db := addressTestDB(t)
	store := NewStore(db)
	ctx := context.Background()
	const actorID = ID("user_effective_test")
	seedAddressTestUser(t, db, actorID)

	op, err := store.CreateISPOperator(ctx, ISPOperator{Code: "ct", Name: "中国电信", Category: "telecom", FlowISPID: 1, ASNs: []uint32{4134}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	geo, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{Kind: GeoKindProvince, Code: "330000", Name: "浙江", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}

	imp, err := store.CreateAddressImport(ctx, AddressImport{
		ID: "imp_effective", SourceSlot: AddressImportSlotCombined, Format: AddressImportFormatMMDB,
		OriginalName: "base.mmdb", ArtifactRef: "address-imports/imp_effective/source.mmdb",
		ChecksumSHA256: strings.Repeat("a", 64), SizeBytes: 1024, Status: AddressImportStatusQueued, CreatedBy: actorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BeginAddressImport(ctx, imp.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAddressImportBatch(ctx, imp.ID, []AddressImportRecord{
		{Prefix: "1.0.0.0/24", CountryCode: "CN", ASN: 4134, Source: "mmdb"},
		{Prefix: "2.0.0.0/24", CountryCode: "US", Source: "mmdb"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteAddressImport(ctx, imp.ID, AddressImportMetadata{Format: AddressImportFormatMMDB, IPVersion: 4}, "en"); err != nil {
		t.Fatal(err)
	}

	if corr, err := store.LookupAddressPrefixCorrections(ctx, []string{"1.0.0.0/24", "2.0.0.0/24"}); err != nil || len(corr) != 0 {
		t.Fatalf("expected no corrections initially, got %d (err=%v)", len(corr), err)
	}

	asn := uint32(9999)
	applied, err := store.BulkReassignAddressPrefixes(ctx, []string{"1.0.0.0/24"}, op.ID, geo.ID, &asn)
	if err != nil || applied != 1 {
		t.Fatalf("reassign applied=%d err=%v", applied, err)
	}
	corr, err := store.LookupAddressPrefixCorrections(ctx, []string{"1.0.0.0/24", "2.0.0.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	if got := corr["1.0.0.0/24"]; got.OperatorName != "中国电信" || got.GeoName != "浙江" ||
		got.ASN == nil || *got.ASN != 9999 || got.Source != "correction" {
		t.Fatalf("correction = %#v", got)
	}
	if _, ok := corr["2.0.0.0/24"]; ok {
		t.Fatal("2.0.0.0/24 must carry no correction")
	}

	// Re-reassign the same CIDR updates in place (ON DUPLICATE KEY), never errors on the unique cidr.
	asn2 := uint32(1234)
	if _, err := store.BulkReassignAddressPrefixes(ctx, []string{"1.0.0.0/24"}, op.ID, "", &asn2); err != nil {
		t.Fatal(err)
	}
	again, _ := store.LookupAddressPrefixCorrections(ctx, []string{"1.0.0.0/24"})
	if got := again["1.0.0.0/24"]; got.ASN == nil || *got.ASN != 1234 {
		t.Fatalf("re-reassign did not update in place: %#v", got)
	}
}
