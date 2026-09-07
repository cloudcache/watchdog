// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowdimension

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"testing"
)

func TestAddressSnapshotIndexClassifiesAndResolvesBothFamilies(t *testing.T) {
	built, err := BuildAddressSnapshot(addressSnapshotBuildFixture(t), AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	index, err := DecodeAndCompileAddressSnapshot(built.Data, built.ChecksumSHA256, AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	metadata := index.Metadata()
	if metadata.SnapshotID != "snapshot-build" || metadata.TenantID != "tenant-build" || metadata.Version != 4 || metadata.Checksum != built.ChecksumSHA256 || metadata.PrefixCount != 2 || metadata.EnabledAddressSetCount != 3 {
		t.Fatalf("metadata = %+v", metadata)
	}

	dimensions := index.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("203.0.113.150"))
	if dimensions.Direction != DirectionOut || dimensions.Business != "corp" || dimensions.Local.PrefixID != "prefix-local" || dimensions.Remote.PrefixID != "prefix-zhejiang" {
		t.Fatalf("dimensions = %+v", dimensions)
	}
	if got := dimensions.Remote.AddressSets.IDs(); len(got) != 1 || got[0] != "set-remote" {
		t.Fatalf("remote sets = %v", got)
	}
	resolution, matched := index.ResolveAddress(dimensions.Remote.IP)
	if !matched || resolution.SupplierGeo.CountryID != "supplier-country-cn" || resolution.SupplierGeo.ISPID != 1 || resolution.SupplierASN != 64500 ||
		resolution.CustomerGeo.CountryID != "customer-country-cn" || resolution.CustomerGeo.ProvinceID != "customer-province-zhejiang" ||
		resolution.CustomerGeo.ISPID != 1 || resolution.CustomerASN != 64500 || resolution.CustomerOverrideFields == 0 ||
		resolution.SupplierGeo.Source != GeoSchemaV2 || resolution.CustomerGeo.Source != "flow_geo_override" || resolution.CustomerGeo.Version != "snapshot-build" {
		t.Fatalf("resolution = %+v matched=%t", resolution, matched)
	}

	ipv6, matched := index.ResolveAddress(netip.MustParseAddr("2001:db8::42"))
	if !matched || ipv6.SupplierASN != 64500 || ipv6.CustomerGeo.ISPID != 1 || ipv6.SupplierGeo.CountryID != "supplier-country-cn" {
		t.Fatalf("IPv6 resolution = %+v matched=%t", ipv6, matched)
	}
	withoutASN, matched := index.ResolveAddress(netip.MustParseAddr("198.51.100.10"))
	if !matched || withoutASN.SupplierASN != 0 || withoutASN.CustomerASN != 0 || withoutASN.SupplierGeo.CountryID != "supplier-country-us" {
		t.Fatalf("Geo-only resolution = %+v matched=%t", withoutASN, matched)
	}
	if _, matched := index.ResolveAddress(netip.MustParseAddr("192.0.2.1")); matched {
		t.Fatal("unpublished address matched AddressSnap")
	}
}

func TestAddressSnapshotIndexInternalMembershipIsPrecomputed(t *testing.T) {
	built, err := BuildAddressSnapshot(addressSnapshotBuildFixture(t), AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	index, err := DecodeAndCompileAddressSnapshot(built.Data, built.ChecksumSHA256, AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	dimensions := index.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("10.2.3.4"))
	if dimensions.Direction != DirectionInternal || dimensions.Business != "corp" {
		t.Fatalf("internal dimensions = %+v", dimensions)
	}
	for _, endpoint := range []EndpointDimension{dimensions.Local, dimensions.Remote} {
		sets := endpoint.AddressSets.IDs()
		if len(sets) != 1 || sets[0] != "set-local" {
			t.Fatalf("internal sets = %v", sets)
		}
	}
	local := netip.MustParseAddr("10.1.2.3")
	remote := netip.MustParseAddr("203.0.113.150")
	allocations := testing.AllocsPerRun(1_000, func() {
		_ = index.ClassifyEndpoints(local, remote)
		_, _ = index.ResolveAddress(remote)
	})
	if allocations != 0 {
		t.Fatalf("AddressSnap hot-path allocations = %f", allocations)
	}
}

func TestDecodeAndCompileAddressSnapshotRejectsExternalChecksumMismatch(t *testing.T) {
	built, err := BuildAddressSnapshot(addressSnapshotBuildFixture(t), AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	checksum := built.ChecksumSHA256[:len(built.ChecksumSHA256)-1] + "0"
	if checksum == built.ChecksumSHA256 {
		checksum = built.ChecksumSHA256[:len(built.ChecksumSHA256)-1] + "1"
	}
	if _, err := DecodeAndCompileAddressSnapshot(built.Data, checksum, AddressSnapshotLimits{}); err == nil || errors.Is(err, ErrInvalidAddressSnapshot) {
		t.Fatalf("checksum mismatch error = %v", err)
	}
}

func TestDecodeAndCompileAddressSnapshotEnforcesResidentMemoryBudget(t *testing.T) {
	built, err := BuildAddressSnapshot(addressSnapshotBuildFixture(t), AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	decodeFootprint := uint64(len(built.Data)) + binary.BigEndian.Uint64(built.Data[20:28])*2
	_, err = DecodeAndCompileAddressSnapshot(built.Data, built.ChecksumSHA256, AddressSnapshotLimits{MaxDecodedMemoryBytes: decodeFootprint})
	if !errors.Is(err, ErrInvalidAddressSnapshot) {
		t.Fatalf("compiled memory limit error = %v", err)
	}
}
