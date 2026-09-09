package address

import (
	"net/netip"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

// Pure ports of the WADS-builder core from internal/watchdog/address_snapshot_publish_test.go
// (no tenant, no DB): longest-prefix sweep, disjoint coalescing, and a stable
// supplier-geo hierarchy. These underwrite the WADS-bytes parity guarantee.
func TestNormalizeAddressSnapshotImportPrefixesUsesLongestPrefix(t *testing.T) {
	makePrefix := func(id uint64, cidr string, asn uint32) addressSnapshotImportPrefix {
		prefix := netip.MustParsePrefix(cidr)
		start, end := addressSnapshotPrefixRange(prefix)
		return addressSnapshotImportPrefix{id: id, bits: uint8(prefix.Bits()), start: start, end: end, asn: asn}
	}
	ranges, err := normalizeAddressSnapshotImportPrefixes([]addressSnapshotImportPrefix{
		makePrefix(1, "192.0.2.0/24", 64500),
		makePrefix(2, "192.0.2.64/26", 64501),
		makePrefix(3, "2001:db8::/126", 64502),
		makePrefix(4, "2001:db8::2/127", 64503),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 5 {
		t.Fatalf("normalized ranges = %#v", ranges)
	}
	wants := []struct {
		start string
		end   string
		asn   uint32
	}{
		{"192.0.2.0", "192.0.2.63", 64500},
		{"192.0.2.64", "192.0.2.127", 64501},
		{"192.0.2.128", "192.0.2.255", 64500},
		{"2001:db8::", "2001:db8::1", 64502},
		{"2001:db8::2", "2001:db8::3", 64503},
	}
	for index, want := range wants {
		if ranges[index].Start.String() != want.start || ranges[index].End.String() != want.end || ranges[index].ASN != want.asn {
			t.Fatalf("range %d = %#v, want %#v", index, ranges[index], want)
		}
	}
}

func TestNormalizeDisjointAddressSnapshotImportPrefixesCoalescesCanonicalInput(t *testing.T) {
	makePrefix := func(id uint64, cidr string, asn uint32) addressSnapshotImportPrefix {
		prefix := netip.MustParsePrefix(cidr)
		start, end := addressSnapshotPrefixRange(prefix)
		return addressSnapshotImportPrefix{id: id, bits: uint8(prefix.Bits()), start: start, end: end, asn: asn}
	}
	ranges, canonical := normalizeDisjointAddressSnapshotImportPrefixes([]addressSnapshotImportPrefix{
		makePrefix(1, "192.0.2.0/25", 64500),
		makePrefix(2, "192.0.2.128/25", 64500),
		makePrefix(3, "2001:db8::/126", 64501),
	})
	if !canonical || len(ranges) != 2 || ranges[0].Start.String() != "192.0.2.0" || ranges[0].End.String() != "192.0.2.255" {
		t.Fatalf("canonical ranges = %#v, canonical=%v", ranges, canonical)
	}
	if _, canonical := normalizeDisjointAddressSnapshotImportPrefixes([]addressSnapshotImportPrefix{
		makePrefix(1, "192.0.2.0/24", 64500), makePrefix(2, "192.0.2.64/26", 64501),
	}); canonical {
		t.Fatal("nested prefixes must use the longest-prefix sweep")
	}
}

func TestRegisterAddressSnapshotSupplierGeoKeepsStableHierarchy(t *testing.T) {
	nodes := map[string]flowdimension.AddressSnapshotBuildGeoNode{}
	geo := registerAddressSnapshotSupplierGeo(nodes, "AS", "CN", "China", "BJ", "Beijing", "1816670", "Beijing")
	if geo.ContinentID != "supplier/continent/AS" || geo.CountryID != "supplier/country/CN" || geo.ProvinceID != "supplier/province/CN/BJ" || geo.CityID == "" {
		t.Fatalf("supplier Geo path = %#v", geo)
	}
	if len(nodes) != 4 || nodes[geo.CountryID].ParentID != geo.ContinentID || nodes[geo.CityID].ParentID != geo.ProvinceID {
		t.Fatalf("supplier Geo nodes = %#v", nodes)
	}
}
