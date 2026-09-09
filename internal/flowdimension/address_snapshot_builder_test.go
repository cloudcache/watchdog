// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowdimension

import (
	"bytes"
	"context"
	"errors"
	"net/netip"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestBuildAddressSnapshotContextHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := BuildAddressSnapshotContext(ctx, addressSnapshotBuildFixture(t), AddressSnapshotLimits{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build error = %v", err)
	}
}

func TestBuildAddressSnapshotMergesSourcesAndManualDefinition(t *testing.T) {
	input := addressSnapshotBuildFixture(t)
	result, err := BuildAddressSnapshot(input, AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Data) == 0 || !strings.HasPrefix(result.ChecksumSHA256, "sha256:") || result.Artifact.SourceManifestSHA256 == "" {
		t.Fatalf("build result = %#v", result)
	}
	decoded, err := DecodeAddressSnapshot(result.Data, AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Sources) != 3 || len(decoded.IPv4Ranges) != 6 || len(decoded.IPv6Ranges) != 1 {
		t.Fatalf("artifact counts: sources=%d v4=%d v6=%d", len(decoded.Sources), len(decoded.IPv4Ranges), len(decoded.IPv6Ranges))
	}

	local := lookupAddressSnapshotValue(t, decoded, "10.1.2.3")
	if !local.Local || addressSnapshotText(t, decoded, local.PrimaryPrefixID) != "prefix-local" || addressSnapshotText(t, decoded, local.Business) != "corp" ||
		!addressSnapshotSetContains(t, decoded, local.OutAddressSetIDs, "set-local") {
		t.Fatalf("local value = %#v", local)
	}

	combined := lookupAddressSnapshotValue(t, decoded, "203.0.113.10")
	if addressSnapshotText(t, decoded, combined.SupplierGeo.CountryID) != "supplier-country-cn" || combined.SupplierASN != 64500 || combined.SupplierISPID != 1 ||
		addressSnapshotText(t, decoded, combined.CustomerGeo.CountryID) != "supplier-country-cn" || combined.CustomerISPID != 1 ||
		!addressSnapshotSetContains(t, decoded, combined.InAddressSetIDs, "set-remote") ||
		!addressSnapshotSetContains(t, decoded, combined.InAddressSetIDs, "set-shared") {
		t.Fatalf("combined value = %#v", combined)
	}

	geoOverride := lookupAddressSnapshotValue(t, decoded, "203.0.113.100")
	if addressSnapshotText(t, decoded, geoOverride.SupplierGeo.CountryID) != "supplier-country-us" || geoOverride.SupplierASN != 64500 || geoOverride.CustomerISPID != 1 {
		t.Fatalf("Geo source overlay = %#v", geoOverride)
	}

	manual := lookupAddressSnapshotValue(t, decoded, "203.0.113.150")
	if addressSnapshotText(t, decoded, manual.CustomerGeo.CountryID) != "customer-country-cn" ||
		addressSnapshotText(t, decoded, manual.CustomerGeo.ProvinceID) != "customer-province-zhejiang" ||
		manual.CustomerISPID != 1 || manual.CustomerASN != 64500 || manual.CustomerOverrideBits == 0 ||
		addressSnapshotText(t, decoded, manual.PrimaryPrefixID) != "prefix-zhejiang" {
		t.Fatalf("manual overlay = %#v", manual)
	}

	asnOverride := lookupAddressSnapshotValue(t, decoded, "203.0.113.220")
	if asnOverride.SupplierASN != 64501 || asnOverride.SupplierISPID != 3 || asnOverride.CustomerASN != 64500 || asnOverride.CustomerISPID != 1 {
		t.Fatalf("ASN/manual precedence = %#v", asnOverride)
	}
	ipv6 := lookupAddressSnapshotValue(t, decoded, "2001:db8::42")
	if ipv6.SupplierASN != 64500 || ipv6.CustomerISPID != 1 || addressSnapshotText(t, decoded, ipv6.SupplierGeo.CountryID) != "supplier-country-cn" {
		t.Fatalf("IPv6 value = %#v", ipv6)
	}
	withoutASN := lookupAddressSnapshotValue(t, decoded, "198.51.100.10")
	if withoutASN.SupplierASN != 0 || withoutASN.SupplierISPID != 0 || withoutASN.CustomerASN != 0 || withoutASN.CustomerISPID != 0 ||
		addressSnapshotText(t, decoded, withoutASN.SupplierGeo.CountryID) != "supplier-country-us" {
		t.Fatalf("Geo-only value = %#v", withoutASN)
	}
}

func TestBuildAddressSnapshotIsDeterministicAndManifestSensitive(t *testing.T) {
	left := addressSnapshotBuildFixture(t)
	right := addressSnapshotBuildFixture(t)
	for begin, end := 0, len(right.Sources)-1; begin < end; begin, end = begin+1, end-1 {
		right.Sources[begin], right.Sources[end] = right.Sources[end], right.Sources[begin]
	}
	first, err := BuildAddressSnapshot(left, AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildAddressSnapshot(right, AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Data, second.Data) || first.ChecksumSHA256 != second.ChecksumSHA256 {
		t.Fatal("source input order changed deterministic output")
	}
	right.Sources[0].SlotRowVersion++
	changed, err := BuildAddressSnapshot(right, AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if changed.Artifact.SourceManifestSHA256 == first.Artifact.SourceManifestSHA256 || changed.ChecksumSHA256 == first.ChecksumSHA256 {
		t.Fatal("source generation change did not invalidate output")
	}
}

func TestBuildAddressSnapshotRejectsInvalidOrOverlappingSources(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*AddressSnapshotBuildInput)
		want   string
	}{
		{name: "overlap", mutate: func(input *AddressSnapshotBuildInput) {
			input.Sources[0].Ranges = append(input.Sources[0].Ranges, AddressSnapshotBuildRange{Start: netip.MustParseAddr("203.0.113.200"), End: netip.MustParseAddr("203.0.113.250")})
			input.Sources[0].RowCountV4++
		}, want: "ranges are not canonical"},
		{name: "bad checksum", mutate: func(input *AddressSnapshotBuildInput) { input.Sources[0].ChecksumSHA256 = "bad" }, want: "source manifest is invalid"},
		{name: "range budget", mutate: func(input *AddressSnapshotBuildInput) {}, want: "range count exceeds build limit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := addressSnapshotBuildFixture(t)
			test.mutate(&input)
			limits := AddressSnapshotLimits{}
			if test.name == "range budget" {
				limits.MaxIPv4Ranges = 1
			}
			if _, err := BuildAddressSnapshot(input, limits); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func addressSnapshotBuildFixture(t *testing.T) AddressSnapshotBuildInput {
	t.Helper()
	bundle := SnapshotBundle{
		SchemaVersion: BundleSchemaVersion, SnapshotID: "snapshot-build", Version: 4,
		EffectiveFrom: time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC),
		GeoNodes: []GeoNodeDefinition{
			{ID: "customer-continent-asia", Kind: "continent", Code: "AS", Name: "Asia", Enabled: true},
			{ID: "customer-country-cn", Kind: "country", Code: "CN", Name: "China", ParentID: "customer-continent-asia", Enabled: true},
			{ID: "customer-province-zhejiang", Kind: "province", Code: "330000", Name: "Zhejiang", ParentID: "customer-country-cn", Enabled: true},
		},
		Operators: []OperatorDefinition{{ID: "customer-telecom", FlowISPID: 1, Code: "CT", Name: "China Telecom", Category: "carrier", ASNs: []uint32{64500}, Enabled: true}},
		Prefixes: []PrefixDefinition{
			{ID: "prefix-local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local", "business": "corp"}},
			{ID: "prefix-zhejiang", CIDR: "203.0.113.128/25", Labels: map[string]string{
				"geo.continent": "AS", "geo.continent_id": "customer-continent-asia", "geo.country": "CN", "geo.country_id": "customer-country-cn",
				"geo.province": "330000", "geo.province_id": "customer-province-zhejiang", "operator.id": "customer-telecom", "asn": "64500",
			}},
		},
		AddressSets: []AddressSetDefinition{
			{ID: "set-local", Name: "Local", Selector: LabelSelector{Labels: map[string][]string{"business": {"corp"}}}, MatchDirection: "both", Enabled: true},
			{ID: "set-remote", Name: "Remote", Members: []string{"203.0.113.0/24"}, MatchDirection: "both", Enabled: true},
			{ID: "set-shared", Name: "Shared", Members: []string{"203.0.113.0/25"}, MatchDirection: "both", Enabled: true},
			{ID: "set-disabled", Name: "Disabled", Members: []string{"198.51.100.0/24"}, MatchDirection: "both", Enabled: false},
		},
	}
	definition, err := CompileBundle(bundle, CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	checksum := func(character string) string { return "sha256:" + strings.Repeat(character, 64) }
	return AddressSnapshotBuildInput{
		Definition: definition, BuilderVersion: "watchdog-test-1",
		SupplierGeoNodes: []AddressSnapshotBuildGeoNode{
			{ID: "supplier-continent-asia", Kind: "continent", Code: "AS", Name: "Asia", Enabled: true},
			{ID: "supplier-continent-na", Kind: "continent", Code: "NA", Name: "North America", Enabled: true},
			{ID: "supplier-country-cn", Kind: "country", Code: "CN", Name: "China", ParentID: "supplier-continent-asia", Enabled: true},
			{ID: "supplier-country-us", Kind: "country", Code: "US", Name: "United States", ParentID: "supplier-continent-na", Enabled: true},
		},
		SupplierOperators: []AddressSnapshotBuildOperator{
			{ID: 1, StableID: "supplier-mobile", Code: "CM", Name: "China Mobile", Category: "carrier", ASNs: []uint32{64500}, Enabled: true},
			{ID: 3, StableID: "supplier-other", Code: "OTHER", Name: "Other Carrier", Category: "carrier", ASNs: []uint32{64501}, Enabled: true},
		},
		Sources: []AddressSnapshotBuildSource{
			{Slot: AddressSnapshotSourceCombined, ImportID: "import-combined", ChecksumSHA256: checksum("a"), SlotRowVersion: 1, RowCountV4: 2, RowCountV6: 1, Ranges: []AddressSnapshotBuildRange{
				{
					Start: netip.MustParseAddr("198.51.100.0"), End: netip.MustParseAddr("198.51.100.255"),
					Geo: AddressSnapshotBuildGeo{CountryCode: "US", ContinentID: "supplier-continent-na", CountryID: "supplier-country-us"},
				},
				{
					Start: netip.MustParseAddr("203.0.113.0"), End: netip.MustParseAddr("203.0.113.255"), ISPID: 1, ASN: 64500,
					Geo: AddressSnapshotBuildGeo{CountryCode: "CN", ContinentID: "supplier-continent-asia", CountryID: "supplier-country-cn"},
				},
				{
					Start: netip.MustParseAddr("2001:db8::"), End: netip.MustParseAddr("2001:db8::ffff"), ISPID: 1, ASN: 64500,
					Geo: AddressSnapshotBuildGeo{CountryCode: "CN", ContinentID: "supplier-continent-asia", CountryID: "supplier-country-cn"},
				},
			}},
			{Slot: AddressSnapshotSourceGeo, ImportID: "import-geo", ChecksumSHA256: checksum("b"), SlotRowVersion: 2, RowCountV4: 1, Ranges: []AddressSnapshotBuildRange{{
				Start: netip.MustParseAddr("203.0.113.64"), End: netip.MustParseAddr("203.0.113.127"),
				Geo: AddressSnapshotBuildGeo{CountryCode: "US", ContinentID: "supplier-continent-na", CountryID: "supplier-country-us"},
			}}},
			{Slot: AddressSnapshotSourceASN, ImportID: "import-asn", ChecksumSHA256: checksum("c"), SlotRowVersion: 3, RowCountV4: 1, Ranges: []AddressSnapshotBuildRange{{
				Start: netip.MustParseAddr("203.0.113.192"), End: netip.MustParseAddr("203.0.113.255"), ISPID: 3, ASN: 64501,
			}}},
		},
	}
}

func lookupAddressSnapshotValue(t *testing.T, snapshot AddressSnapshotArtifact, address string) AddressSnapshotValue {
	t.Helper()
	parsed := netip.MustParseAddr(address)
	if parsed.Is4() {
		key := addressToUint32(parsed)
		index := sort.Search(len(snapshot.IPv4Ranges), func(index int) bool { return snapshot.IPv4Ranges[index].End >= key })
		if index < len(snapshot.IPv4Ranges) && snapshot.IPv4Ranges[index].Start <= key {
			return snapshot.Values[snapshot.IPv4Ranges[index].ValueIndex]
		}
	} else {
		key := parsed.As16()
		index := sort.Search(len(snapshot.IPv6Ranges), func(index int) bool { return bytes.Compare(snapshot.IPv6Ranges[index].End[:], key[:]) >= 0 })
		if index < len(snapshot.IPv6Ranges) && bytes.Compare(snapshot.IPv6Ranges[index].Start[:], key[:]) <= 0 {
			return snapshot.Values[snapshot.IPv6Ranges[index].ValueIndex]
		}
	}
	t.Fatalf("address %s is absent from snapshot", address)
	return AddressSnapshotValue{}
}

func addressSnapshotText(t *testing.T, snapshot AddressSnapshotArtifact, index uint32) string {
	t.Helper()
	if uint64(index) >= uint64(len(snapshot.Strings)) {
		t.Fatalf("string index %d outside dictionary", index)
	}
	return snapshot.Strings[index]
}

func addressSnapshotSetContains(t *testing.T, snapshot AddressSnapshotArtifact, values []uint32, want string) bool {
	t.Helper()
	for _, value := range values {
		if addressSnapshotText(t, snapshot, value) == want {
			return true
		}
	}
	return false
}
