package address

import (
	"context"
	"sort"
	"strings"
	"testing"
)

// Region-group (geo_line) effective-set resolution: within a dimension OR,
// across dimensions AND, members unioned, exclude lines subtracted (recursively).
// Opt-in via WATCHDOG_TEST_MYSQL_DSN.
func TestListAddressPrefixesByLine(t *testing.T) {
	db := addressTestDB(t)
	store := NewStore(db)
	ctx := context.Background()
	const actorID = ID("user_line_test")
	seedAddressTestUser(t, db, actorID)

	op, err := store.CreateISPOperator(ctx, ISPOperator{Code: "ct", Name: "中国电信", Category: "telecom", FlowISPID: 1, ASNs: []uint32{4134}, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	cn, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{Kind: GeoKindCountry, Code: "CN", Name: "中国", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	zj, err := store.CreateGeoDictionary(ctx, GeoDictionaryNode{Kind: GeoKindProvince, Code: "ZJ", Name: "浙江", ParentID: cn.ID, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}

	imp, err := store.CreateAddressImport(ctx, AddressImport{
		ID: "imp_line", SourceSlot: AddressImportSlotCombined, Format: AddressImportFormatMMDB,
		OriginalName: "base.mmdb", ArtifactRef: "address-imports/imp_line/source.mmdb",
		ChecksumSHA256: strings.Repeat("a", 64), SizeBytes: 1024, Status: AddressImportStatusQueued, CreatedBy: actorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BeginAddressImport(ctx, imp.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertAddressImportBatch(ctx, imp.ID, []AddressImportRecord{
		{Prefix: "1.0.0.0/24", CountryCode: "CN", SubdivisionName: "浙江", Operator: "中国电信", ASN: 4134, Source: "mmdb"},
		{Prefix: "2.0.0.0/24", CountryCode: "US", Source: "mmdb"},
		{Prefix: "3.0.0.0/24", CountryCode: "CN", SubdivisionName: "广东", Operator: "中国电信", Source: "mmdb"},
		{Prefix: "4.0.0.0/24", CountryCode: "CN", SubdivisionName: "浙江", Source: "mmdb"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteAddressImport(ctx, imp.ID, AddressImportMetadata{Format: AddressImportFormatMMDB, IPVersion: 4}, "en"); err != nil {
		t.Fatal(err)
	}

	cidrs := func(lineID ID) []string {
		items, _, err := store.ListAddressPrefixesByLine(ctx, imp.ID, lineID, 0, 100, 0)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]string, len(items))
		for i, item := range items {
			out[i] = item.CIDR
		}
		sort.Strings(out)
		return out
	}
	mustLine := func(line GeoLine) ID {
		saved, err := store.CreateGeoLine(ctx, line)
		if err != nil {
			t.Fatal(err)
		}
		return saved.ID
	}

	geoCN := mustLine(GeoLine{Code: "cn", Name: "China", GeoSelector: GeoLineSelector{GeoNodeIDs: []ID{cn.ID}}, Enabled: true})
	assertCIDRs(t, "geo=CN", cidrs(geoCN), "1.0.0.0/24", "3.0.0.0/24", "4.0.0.0/24")

	opCT := mustLine(GeoLine{Code: "ct", Name: "CT", GeoSelector: GeoLineSelector{OperatorIDs: []ID{op.ID}}, Enabled: true})
	assertCIDRs(t, "operator=CT", cidrs(opCT), "1.0.0.0/24", "3.0.0.0/24")

	geoZJ := mustLine(GeoLine{Code: "zj", Name: "ZJ", GeoSelector: GeoLineSelector{GeoNodeIDs: []ID{zj.ID}}, Enabled: true})
	assertCIDRs(t, "geo=ZJ", cidrs(geoZJ), "1.0.0.0/24", "4.0.0.0/24")

	// Across dimensions AND: CN AND operator=CT excludes the operator-less CN row.
	cnAndCT := mustLine(GeoLine{Code: "cnct", Name: "CN+CT", GeoSelector: GeoLineSelector{GeoNodeIDs: []ID{cn.ID}, OperatorIDs: []ID{op.ID}}, Enabled: true})
	assertCIDRs(t, "CN AND CT", cidrs(cnAndCT), "1.0.0.0/24", "3.0.0.0/24")

	// Exclude: CN minus the 浙江 group leaves only 广东.
	cnNoZJ := mustLine(GeoLine{Code: "cn_no_zj", Name: "CN-ZJ", GeoSelector: GeoLineSelector{GeoNodeIDs: []ID{cn.ID}}, ExcludeLineIDs: []ID{geoZJ}, Enabled: true})
	assertCIDRs(t, "CN exclude ZJ", cidrs(cnNoZJ), "3.0.0.0/24")

	// Members only, and selector ∪ members.
	membersOnly := mustLine(GeoLine{Code: "m", Name: "M", Members: []string{"2.0.0.0/24"}, Enabled: true})
	assertCIDRs(t, "members", cidrs(membersOnly), "2.0.0.0/24")

	cnOrMember := mustLine(GeoLine{Code: "cn_or_m", Name: "CN or M", GeoSelector: GeoLineSelector{GeoNodeIDs: []ID{cn.ID}}, Members: []string{"2.0.0.0/24"}, Enabled: true})
	assertCIDRs(t, "CN or member", cidrs(cnOrMember), "1.0.0.0/24", "2.0.0.0/24", "3.0.0.0/24", "4.0.0.0/24")
}

func assertCIDRs(t *testing.T, label string, got []string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("%s: got %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: got %v, want %v", label, got, want)
		}
	}
}
