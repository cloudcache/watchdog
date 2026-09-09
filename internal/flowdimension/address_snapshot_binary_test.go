// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowdimension

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net/netip"
	"reflect"
	"testing"
	"time"
)

func TestAddressSnapshotBinaryRoundTripIsDeterministic(t *testing.T) {
	snapshot := addressSnapshotBinaryFixture(t)
	first, err := EncodeAddressSnapshot(snapshot, AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := EncodeAddressSnapshot(snapshot, AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("same canonical snapshot produced different bytes")
	}
	if string(first[:4]) != "WADS" || binary.BigEndian.Uint16(first[4:6]) != AddressSnapshotFormatVersion {
		t.Fatalf("unexpected header: %x", first[:12])
	}
	decoded, err := DecodeAddressSnapshot(first, AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, snapshot) {
		t.Fatalf("round trip differs\n got: %#v\nwant: %#v", decoded, snapshot)
	}

	digest := sha256.Sum256(first)
	if got, want := hex.EncodeToString(digest[:]), "4489476d603efeac8c825ccfd58a20ed5a13e82bbd02c5de4542a6ba8b094e44"; got != want {
		t.Fatalf("WADS v1 fixture sha256 = %s, want %s", got, want)
	}
}

func TestAddressSnapshotBinaryRejectsContainerCorruptionAndLimits(t *testing.T) {
	encoded, err := EncodeAddressSnapshot(addressSnapshotBinaryFixture(t), AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	declaredUncompressed := int(binary.BigEndian.Uint64(encoded[20:28]))
	tests := []struct {
		name   string
		mutate func([]byte) []byte
		limits AddressSnapshotLimits
	}{
		{name: "bad magic", mutate: func(value []byte) []byte { value[0] ^= 0xff; return value }},
		{name: "unknown version", mutate: func(value []byte) []byte { binary.BigEndian.PutUint16(value[4:6], 2); return value }},
		{name: "unknown flags", mutate: func(value []byte) []byte { binary.BigEndian.PutUint16(value[6:8], 3); return value }},
		{name: "reserved header", mutate: func(value []byte) []byte { value[35] = 1; return value }},
		{name: "truncated", mutate: func(value []byte) []byte { return value[:len(value)-1] }},
		{name: "trailing", mutate: func(value []byte) []byte { return append(value, 0) }},
		{name: "compressed corruption", mutate: func(value []byte) []byte { value[len(value)-1] ^= 0xff; return value }},
		{name: "uncompressed budget", mutate: func(value []byte) []byte { return value }, limits: AddressSnapshotLimits{MaxUncompressedBytes: declaredUncompressed - 1}},
		{name: "compressed budget", mutate: func(value []byte) []byte { return value }, limits: AddressSnapshotLimits{MaxCompressedBytes: len(encoded) - addressSnapshotHeaderSize - 1}},
		{name: "decoded memory budget", mutate: func(value []byte) []byte { return value }, limits: AddressSnapshotLimits{MaxDecodedMemoryBytes: 1}},
		{name: "zstd window budget", mutate: func(value []byte) []byte { return value }, limits: AddressSnapshotLimits{MaxZstdWindowBytes: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := test.mutate(append([]byte(nil), encoded...))
			if _, err := DecodeAddressSnapshot(value, test.limits); !errors.Is(err, ErrInvalidAddressSnapshot) {
				t.Fatalf("DecodeAddressSnapshot() error = %v", err)
			}
		})
	}
}

func TestAddressSnapshotPayloadRejectsImpossibleCountsAndTrailingBytesBeforeAllocation(t *testing.T) {
	snapshot := addressSnapshotBinaryFixture(t)
	limits, err := normalizeAddressSnapshotLimits(AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := marshalAddressSnapshotPayload(snapshot, limits)
	if err != nil {
		t.Fatal(err)
	}

	reader := addressSnapshotReader{data: payload}
	for range 4 {
		_ = reader.string16()
	}
	_ = reader.u64()
	_ = reader.i64()
	if reader.err != nil {
		t.Fatal(reader.err)
	}
	badCount := append([]byte(nil), payload...)
	binary.BigEndian.PutUint32(badCount[reader.position:reader.position+4], uint32(limits.MaxStrings+1))
	if _, err := unmarshalAddressSnapshotPayload(badCount, limits); !errors.Is(err, ErrInvalidAddressSnapshot) {
		t.Fatalf("impossible count error = %v", err)
	}
	if _, err := unmarshalAddressSnapshotPayload(append(payload, 0), limits); !errors.Is(err, ErrInvalidAddressSnapshot) {
		t.Fatalf("trailing payload error = %v", err)
	}
}

func TestAddressSnapshotBinaryRejectsNonCanonicalOrDanglingTables(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*AddressSnapshotArtifact)
	}{
		{name: "string order", mutate: func(snapshot *AddressSnapshotArtifact) {
			snapshot.Strings[1], snapshot.Strings[2] = snapshot.Strings[2], snapshot.Strings[1]
		}},
		{name: "source checksum", mutate: func(snapshot *AddressSnapshotArtifact) {
			snapshot.Sources[0].ChecksumSHA256 = addressSnapshotStringIndex(t, snapshot.Strings, "private")
		}},
		{name: "Geo parent", mutate: func(snapshot *AddressSnapshotArtifact) {
			snapshot.GeoNodes[1].ParentID = addressSnapshotStringIndex(t, snapshot.Strings, "missing-parent")
		}},
		{name: "Geo value", mutate: func(snapshot *AddressSnapshotArtifact) {
			snapshot.Values[1].CustomerGeo.CountryID = addressSnapshotStringIndex(t, snapshot.Strings, "missing-parent")
		}},
		{name: "operator ASN order", mutate: func(snapshot *AddressSnapshotArtifact) {
			snapshot.Operators[0].ASNs = []uint32{9808, 4134}
		}},
		{name: "unknown operator ID", mutate: func(snapshot *AddressSnapshotArtifact) {
			snapshot.Values[1].SupplierISPID = 42
		}},
		{name: "unknown address set", mutate: func(snapshot *AddressSnapshotArtifact) {
			snapshot.Values[1].InAddressSetIDs = []uint32{addressSnapshotStringIndex(t, snapshot.Strings, "private")}
		}},
		{name: "disabled address set", mutate: func(snapshot *AddressSnapshotArtifact) {
			snapshot.AddressSets[0].Enabled = false
		}},
		{name: "value order", mutate: func(snapshot *AddressSnapshotArtifact) {
			other := snapshot.Values[1]
			other.Business = 0
			snapshot.Values = append(snapshot.Values, other)
		}},
		{name: "IPv4 overlap", mutate: func(snapshot *AddressSnapshotArtifact) {
			snapshot.IPv4Ranges = append(snapshot.IPv4Ranges, AddressSnapshotIPv4Range{Start: snapshot.IPv4Ranges[0].End, End: snapshot.IPv4Ranges[0].End + 1, ValueIndex: 1})
		}},
		{name: "IPv6 dangling value", mutate: func(snapshot *AddressSnapshotArtifact) {
			snapshot.IPv6Ranges[0].ValueIndex = uint32(len(snapshot.Values))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := addressSnapshotBinaryFixture(t)
			test.mutate(&snapshot)
			if _, err := EncodeAddressSnapshot(snapshot, AddressSnapshotLimits{}); !errors.Is(err, ErrInvalidAddressSnapshot) {
				t.Fatalf("EncodeAddressSnapshot() error = %v", err)
			}
		})
	}
}

func TestCanonicalAddressSnapshotStringsAndRangeHelpers(t *testing.T) {
	strings, err := CanonicalAddressSnapshotStrings("beta", "alpha", "beta", "")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(strings, []string{"", "alpha", "beta"}) {
		t.Fatalf("strings = %#v", strings)
	}
	if _, err := CanonicalAddressSnapshotStrings(" bad"); !errors.Is(err, ErrInvalidAddressSnapshot) {
		t.Fatalf("bad string error = %v", err)
	}
	v4, err := AddressSnapshotIPv4(netip.MustParseAddr("10.0.0.0"), netip.MustParseAddr("10.0.0.255"), 1)
	if err != nil || v4.Start != 0x0a000000 || v4.End != 0x0a0000ff {
		t.Fatalf("IPv4 helper = %+v, %v", v4, err)
	}
	if _, err := AddressSnapshotIPv4(netip.MustParseAddr("2001:db8::"), netip.MustParseAddr("2001:db8::1"), 1); !errors.Is(err, ErrInvalidAddressSnapshot) {
		t.Fatalf("IPv4 family error = %v", err)
	}
	v6, err := AddressSnapshotIPv6(netip.MustParseAddr("2001:db8::"), netip.MustParseAddr("2001:db8::ffff"), 1)
	if err != nil || v6.Start != netip.MustParseAddr("2001:db8::").As16() || v6.End != netip.MustParseAddr("2001:db8::ffff").As16() {
		t.Fatalf("IPv6 helper = %+v, %v", v6, err)
	}
}

func addressSnapshotBinaryFixture(t *testing.T) AddressSnapshotArtifact {
	t.Helper()
	checksum := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	strings, err := CanonicalAddressSnapshotStrings(
		"10.0.0.0/8", "110000", "AS", "Asia", "Beijing", "CN", "China", "China Mobile", "China Mobile Customer",
		"carrier", "combined", "continent", "country", "customer-mobile", "customer-mobile-code", "geo-continent-asia", "geo-country-cn", "geo-province-beijing", "prefix-a", "private", "province",
		"missing-parent", "set-a", "set-one", checksum, "source-import-a", "supplier-mobile", "supplier-mobile-code",
	)
	if err != nil {
		t.Fatal(err)
	}
	index := func(value string) uint32 { return addressSnapshotStringIndex(t, strings, value) }
	geo := AddressSnapshotGeoValue{
		CountryCode: index("CN"), AdminCode: index("110000"), Subdivision: index("Beijing"), City: index("Beijing"),
		ContinentID: index("geo-continent-asia"), CountryID: index("geo-country-cn"), ProvinceID: index("geo-province-beijing"),
	}
	v4, err := AddressSnapshotIPv4(netip.MustParseAddr("10.0.0.0"), netip.MustParseAddr("10.255.255.255"), 1)
	if err != nil {
		t.Fatal(err)
	}
	v6, err := AddressSnapshotIPv6(netip.MustParseAddr("2001:db8::"), netip.MustParseAddr("2001:db8::ffff"), 1)
	if err != nil {
		t.Fatal(err)
	}
	return AddressSnapshotArtifact{
		SnapshotID: "snapshot-a", Version: 7,
		EffectiveFrom: time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC), BuilderVersion: "watchdog-1",
		SourceManifestSHA256: checksum, Strings: strings,
		Sources: []AddressSnapshotSource{{
			Slot: index("combined"), ImportID: index("source-import-a"), ChecksumSHA256: index(checksum),
			SlotRowVersion: 3, RowCountV4: 1, RowCountV6: 1,
		}},
		GeoNodes: []AddressSnapshotGeoNode{
			{Namespace: AddressSnapshotGeoSupplier, ID: index("geo-continent-asia"), Kind: index("continent"), Code: index("AS"), Name: index("Asia"), Enabled: true},
			{Namespace: AddressSnapshotGeoSupplier, ID: index("geo-country-cn"), Kind: index("country"), Code: index("CN"), Name: index("China"), ParentID: index("geo-continent-asia"), Enabled: true},
			{Namespace: AddressSnapshotGeoSupplier, ID: index("geo-province-beijing"), Kind: index("province"), Code: index("110000"), Name: index("Beijing"), ParentID: index("geo-country-cn"), Enabled: true},
			{Namespace: AddressSnapshotGeoCustomer, ID: index("geo-continent-asia"), Kind: index("continent"), Code: index("AS"), Name: index("Asia"), Enabled: true},
			{Namespace: AddressSnapshotGeoCustomer, ID: index("geo-country-cn"), Kind: index("country"), Code: index("CN"), Name: index("China"), ParentID: index("geo-continent-asia"), Enabled: true},
			{Namespace: AddressSnapshotGeoCustomer, ID: index("geo-province-beijing"), Kind: index("province"), Code: index("110000"), Name: index("Beijing"), ParentID: index("geo-country-cn"), Enabled: true},
		},
		Operators: []AddressSnapshotOperator{
			{Namespace: AddressSnapshotOperatorSupplier, ID: 1, StableID: index("supplier-mobile"), Code: index("supplier-mobile-code"), Name: index("China Mobile"), Category: index("carrier"), ASNs: []uint32{9808}, Enabled: true},
			{Namespace: AddressSnapshotOperatorCustomer, ID: 1, StableID: index("customer-mobile"), Code: index("customer-mobile-code"), Name: index("China Mobile Customer"), Category: index("carrier"), ASNs: []uint32{9808}, Enabled: true},
		},
		AddressSets: []AddressSnapshotSet{{ID: index("set-a"), Name: index("set-one"), Enabled: true}},
		Values: []AddressSnapshotValue{{}, {
			SupplierGeo: geo, CustomerGeo: geo, SupplierISPID: 1, CustomerISPID: 1, SupplierASN: 9808, CustomerASN: 9808,
			CustomerOverrideBits: uint8(GeoOverrideAdminCode), Local: true,
			PrimaryPrefixID: index("prefix-a"), PrimaryPrefixCIDR: index("10.0.0.0/8"), Business: index("private"),
			InAddressSetIDs: []uint32{index("set-a")}, OutAddressSetIDs: []uint32{index("set-a")},
		}},
		IPv4Ranges: []AddressSnapshotIPv4Range{v4}, IPv6Ranges: []AddressSnapshotIPv6Range{v6},
	}
}

func addressSnapshotStringIndex(t *testing.T, dictionary []string, value string) uint32 {
	t.Helper()
	for index, candidate := range dictionary {
		if candidate == value {
			return uint32(index)
		}
	}
	t.Fatalf("string %q is absent from fixture dictionary", value)
	return 0
}
