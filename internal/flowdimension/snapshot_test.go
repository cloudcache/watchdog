package flowdimension

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestDecodeAndCompileBundleChecksWireContract(t *testing.T) {
	bundle := testBundle("snapshot-a", 1, testMinute(12, 0))
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := DecodeAndCompileBundle(data, bundleChecksum(data), CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	metadata := snapshot.Metadata()
	if metadata.Checksum != bundleChecksum(data) || metadata.PrefixCount != len(bundle.Prefixes) || metadata.EnabledAddressSetCount != 4 {
		t.Fatalf("unexpected metadata: %+v", metadata)
	}

	badChecksum := "sha256:" + strings.Repeat("0", 64)
	if _, err := DecodeAndCompileBundle(data, badChecksum, CompileLimits{}); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("checksum error = %v", err)
	}
	withUnknown := append(data[:len(data)-1], []byte(`,"unexpected":true}`)...)
	if _, err := DecodeAndCompileBundle(withUnknown, bundleChecksum(withUnknown), CompileLimits{}); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}
	if _, err := DecodeAndCompileBundle(data, bundleChecksum(data), CompileLimits{MaxBundleBytes: len(data) - 1}); err == nil || !strings.Contains(err.Error(), "bundle size") {
		t.Fatalf("size error = %v", err)
	}
}

func TestLabelSelectorAcceptsExistingStringAndArrayShapes(t *testing.T) {
	data := []byte(`{
  "schema_version":1,
  "snapshot_id":"snapshot-wire",
  "tenant_id":"tenant-a",
  "version":1,
  "effective_from":"2026-09-05T12:00:00Z",
  "prefixes":[{"id":"remote","cidr":"203.0.113.0/24","labels":{"provider":"isp-a","region":"east"}}],
  "address_sets":[{"id":"set-a","selector":{"labels":{"provider":"isp-a","region":["east","west"]}},"match_direction":"both","enabled":true}]
}`)
	legacy, err := DecodeAndCompileBundle(data, bundleChecksum(data), CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Metadata().SchemaVersion != 1 || legacy.Metadata().OperatorCount != 0 {
		t.Fatalf("legacy metadata = %+v", legacy.Metadata())
	}

	bad := strings.Replace(string(data), `"selector":{"labels":`, `"selector":{"unknown":true,"labels":`, 1)
	if _, err := DecodeAndCompileBundle([]byte(bad), bundleChecksum([]byte(bad)), CompileLimits{}); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("selector unknown field error = %v", err)
	}
}

func TestCompileBundleCarriesImmutableTenantOperatorDefinitions(t *testing.T) {
	bundle := testBundle("snapshot-operators", 1, testMinute(12, 0))
	bundle.Operators = []OperatorDefinition{
		{ID: "operator-telecom", FlowISPID: 3, Code: "CT", Name: "China Telecom", ShortName: "Telecom", Category: "carrier", ASNs: []uint32{4134, 4809}, Enabled: true},
		{ID: "operator-disabled", FlowISPID: 9, Code: "LAB", Name: "Lab Network", Category: "other", ASNs: []uint32{}, Enabled: false},
	}
	snapshot, err := CompileBundle(bundle, CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if metadata := snapshot.Metadata(); metadata.SchemaVersion != BundleSchemaVersion || metadata.OperatorCount != 2 {
		t.Fatalf("operator metadata = %+v", metadata)
	}
	byID, ok := snapshot.OperatorByID("operator-telecom")
	if !ok || byID.FlowISPID != 3 || len(byID.ASNs) != 2 {
		t.Fatalf("operator by id = %+v, %t", byID, ok)
	}
	byFlowID, ok := snapshot.OperatorByFlowISPID(3)
	if !ok || byFlowID.ID != byID.ID {
		t.Fatalf("operator by Flow ISP id = %+v, %t", byFlowID, ok)
	}
	definitions := snapshot.OperatorDefinitions()
	definitions[0].ASNs[0] = 1
	byID.ASNs[0] = 2
	again, _ := snapshot.OperatorByFlowISPID(3)
	if again.ASNs[0] != 4134 {
		t.Fatalf("compiled operator storage was exposed: %+v", again)
	}
	if _, ok := snapshot.OperatorByFlowISPID(0); ok {
		t.Fatal("reserved unknown Flow ISP id resolved")
	}
}

func TestCompileBundleCarriesImmutableGeoAndAddressSetMetadata(t *testing.T) {
	bundle := testBundle("snapshot-display-metadata", 1, testMinute(12, 0))
	bundle.GeoNodes = []GeoNodeDefinition{
		{ID: "geo-city-hangzhou", Kind: "city", Code: "330100", Name: "Hangzhou", ParentID: "geo-province-zhejiang", Enabled: true},
		{ID: "geo-continent-asia", Kind: "continent", Code: "AS", Name: "Asia", Enabled: true},
		{ID: "geo-country-cn", Kind: "country", Code: "CN", Name: "China", ParentID: "geo-continent-asia", Enabled: true},
		{ID: "geo-province-zhejiang", Kind: "province", Code: "330000", Name: "Zhejiang", ParentID: "geo-country-cn", Enabled: true},
	}
	snapshot, err := CompileBundle(bundle, CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	if metadata := snapshot.Metadata(); metadata.GeoNodeCount != 4 {
		t.Fatalf("Geo node metadata = %+v", metadata)
	}
	geoNodes := snapshot.GeoNodeDefinitions()
	sets := snapshot.AddressSetMetadata()
	if len(geoNodes) != 4 || geoNodes[0].Name != "Hangzhou" || geoNodes[0].ParentID != "geo-province-zhejiang" {
		t.Fatalf("Geo definitions = %#v", geoNodes)
	}
	if len(sets) != len(bundle.AddressSets) || sets[0].ID != "set-out-region" || sets[0].Name != "Outbound east" || !sets[0].Enabled {
		t.Fatalf("address-set metadata = %#v", sets)
	}
	geoNodes[0].Name = "mutated"
	sets[0].Name = "mutated"
	if snapshot.GeoNodeDefinitions()[0].Name != "Hangzhou" || snapshot.AddressSetMetadata()[0].Name != "Outbound east" {
		t.Fatal("compiled display metadata storage was exposed")
	}
}

func TestCompileBundlePreservesV1V2CompatibilityAndEnforcesV3Metadata(t *testing.T) {
	version2 := testBundle("snapshot-v2", 1, testMinute(12, 0))
	version2.SchemaVersion = 2
	for index := range version2.AddressSets {
		version2.AddressSets[index].Name = ""
	}
	if _, err := CompileBundle(version2, CompileLimits{}); err != nil {
		t.Fatalf("schema v2 bundle: %v", err)
	}
	withV3Name := version2
	withV3Name.AddressSets = append([]AddressSetDefinition(nil), version2.AddressSets...)
	withV3Name.AddressSets[0].Name = "not supported"
	if _, err := CompileBundle(withV3Name, CompileLimits{}); err == nil || !strings.Contains(err.Error(), "does not support address_sets") {
		t.Fatalf("schema v2 name error = %v", err)
	}
	withV3Geo := version2
	withV3Geo.GeoNodes = []GeoNodeDefinition{{ID: "geo-country-cn", Kind: "country", Code: "CN", Name: "China", Enabled: true}}
	if _, err := CompileBundle(withV3Geo, CompileLimits{}); err == nil || !strings.Contains(err.Error(), "does not support geo_nodes") {
		t.Fatalf("schema v2 Geo error = %v", err)
	}
	version3 := testBundle("snapshot-v3-missing-name", 1, testMinute(12, 0))
	version3.AddressSets[0].Name = ""
	if _, err := CompileBundle(version3, CompileLimits{}); err == nil || !strings.Contains(err.Error(), "name is required") {
		t.Fatalf("schema v3 missing name error = %v", err)
	}
}

func TestCompileBundleRejectsInvalidGeoDefinitions(t *testing.T) {
	valid := []GeoNodeDefinition{
		{ID: "geo-continent-asia", Kind: "continent", Code: "AS", Name: "Asia", Enabled: true},
		{ID: "geo-country-cn", Kind: "country", Code: "CN", Name: "China", ParentID: "geo-continent-asia", Enabled: true},
	}
	tests := []struct {
		name  string
		nodes []GeoNodeDefinition
		want  string
	}{
		{name: "non-canonical order", nodes: []GeoNodeDefinition{valid[1], valid[0]}, want: "canonically ordered"},
		{name: "missing parent", nodes: []GeoNodeDefinition{{ID: "geo-country-cn", Kind: "country", Code: "CN", Name: "China", ParentID: "missing", Enabled: true}}, want: "missing, self, or non-ancestor"},
		{name: "non-ancestor parent", nodes: []GeoNodeDefinition{{ID: "geo-city-a", Kind: "city", Code: "A", Name: "A", ParentID: "geo-city-b", Enabled: true}, {ID: "geo-city-b", Kind: "city", Code: "B", Name: "B", Enabled: true}}, want: "missing, self, or non-ancestor"},
		{name: "enabled below disabled", nodes: []GeoNodeDefinition{{ID: "geo-continent-asia", Kind: "continent", Code: "AS", Name: "Asia", Enabled: false}, {ID: "geo-country-cn", Kind: "country", Code: "CN", Name: "China", ParentID: "geo-continent-asia", Enabled: true}}, want: "disabled parent"},
		{name: "duplicate kind code", nodes: []GeoNodeDefinition{{ID: "geo-country-cn-a", Kind: "country", Code: "CN", Name: "China A", Enabled: true}, {ID: "geo-country-cn-b", Kind: "country", Code: "CN", Name: "China B", Enabled: true}}, want: "duplicates kind/code"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := testBundle("snapshot-invalid-geo", 1, testMinute(12, 0))
			bundle.GeoNodes = test.nodes
			if _, err := CompileBundle(bundle, CompileLimits{}); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompileBundleRejectsInvalidOperatorDefinitions(t *testing.T) {
	valid := OperatorDefinition{ID: "operator-a", FlowISPID: 1, Code: "A", Name: "Operator A", Category: "carrier", ASNs: []uint32{64500}, Enabled: true}
	for name, mutate := range map[string]func(*SnapshotBundle){
		"schema v1 content": func(bundle *SnapshotBundle) {
			bundle.SchemaVersion = 1
			bundle.Operators = []OperatorDefinition{valid}
		},
		"zero Flow ISP id": func(bundle *SnapshotBundle) {
			item := valid
			item.FlowISPID = 0
			bundle.Operators = []OperatorDefinition{item}
		},
		"duplicate management id": func(bundle *SnapshotBundle) {
			other := valid
			other.FlowISPID = 2
			bundle.Operators = []OperatorDefinition{valid, other}
		},
		"duplicate Flow ISP id": func(bundle *SnapshotBundle) {
			other := valid
			other.ID = "operator-b"
			bundle.Operators = []OperatorDefinition{valid, other}
		},
		"ASN zero": func(bundle *SnapshotBundle) {
			item := valid
			item.ASNs = []uint32{0}
			bundle.Operators = []OperatorDefinition{item}
		},
		"ASN not canonical": func(bundle *SnapshotBundle) {
			item := valid
			item.ASNs = []uint32{64501, 64500}
			bundle.Operators = []OperatorDefinition{item}
		},
		"operator order not canonical": func(bundle *SnapshotBundle) {
			other := valid
			other.ID = "operator-b"
			other.FlowISPID = 2
			bundle.Operators = []OperatorDefinition{other, valid}
		},
		"ASN assigned twice": func(bundle *SnapshotBundle) {
			other := valid
			other.ID = "operator-b"
			other.FlowISPID = 2
			bundle.Operators = []OperatorDefinition{valid, other}
		},
	} {
		t.Run(name, func(t *testing.T) {
			bundle := testBundle("snapshot-invalid-operator", 1, testMinute(12, 0))
			mutate(&bundle)
			if _, err := CompileBundle(bundle, CompileLimits{}); err == nil {
				t.Fatal("invalid operator definition was accepted")
			}
		})
	}
}

func TestCompileBundleRejectsInvalidOrUnboundedDefinitions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*SnapshotBundle)
		limits CompileLimits
		want   string
	}{
		{name: "non-canonical-cidr", mutate: func(bundle *SnapshotBundle) { bundle.Prefixes[0].CIDR = "10.0.0.1/8" }, want: "canonical"},
		{name: "duplicate-prefix-id", mutate: func(bundle *SnapshotBundle) { bundle.Prefixes[1].ID = bundle.Prefixes[0].ID }, want: "duplicates id"},
		{name: "duplicate-prefix-cidr", mutate: func(bundle *SnapshotBundle) { bundle.Prefixes[1].CIDR = bundle.Prefixes[0].CIDR }, want: "duplicates cidr"},
		{name: "invalid-direction", mutate: func(bundle *SnapshotBundle) { bundle.AddressSets[0].MatchDirection = "src" }, want: "in, out, or both"},
		{name: "empty-selector", mutate: func(bundle *SnapshotBundle) { bundle.AddressSets[0].Selector.Labels = nil }, want: "1..64"},
		{name: "not-minute-boundary", mutate: func(bundle *SnapshotBundle) { bundle.EffectiveFrom = bundle.EffectiveFrom.Add(time.Second) }, want: "minute boundary"},
		{name: "non-utc-effective-time", mutate: func(bundle *SnapshotBundle) {
			bundle.EffectiveFrom = bundle.EffectiveFrom.In(time.FixedZone("UTC+8", 8*60*60))
		}, want: "UTC minute boundary"},
		{name: "prefix-limit", limits: CompileLimits{MaxPrefixes: 1}, want: "prefixes, limit"},
		{name: "set-limit", limits: CompileLimits{MaxAddressSets: 1}, want: "address sets, limit"},
		{name: "expansion-limit", limits: CompileLimits{MaxAddressSetsPerRecord: 2}, want: "expansion 3"},
		{name: "address-set-work-limit", limits: CompileLimits{MaxAddressSetEvaluations: 1}, want: "address-set evaluations"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := testBundle("snapshot-a", 1, testMinute(12, 0))
			if test.mutate != nil {
				test.mutate(&bundle)
			}
			if _, err := CompileBundle(bundle, test.limits); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompiledSnapshotClassifiesDirectionPrefixesAndSets(t *testing.T) {
	bundle := testBundle("snapshot-a", 1, testMinute(12, 0))
	snapshot, err := CompileBundle(bundle, CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	// Compilation owns immutable copies; management-side draft mutation cannot
	// alter an already selected event-time snapshot.
	bundle.Prefixes[1].Labels["business"] = "mutated"
	bundle.AddressSets[2].Selector.Labels["provider"][0] = "mutated"

	out := snapshot.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("203.0.113.200"))
	if out.Direction != DirectionOut || out.Business != "customer" || out.Local.Side != EndpointSrc || out.Remote.Side != EndpointDst {
		t.Fatalf("unexpected outbound classification: %+v", out)
	}
	if out.Local.PrefixID != "local-specific" || out.Local.PrefixCIDR != "10.1.0.0/16" {
		t.Fatalf("unexpected local LPM: %+v", out.Local)
	}
	if out.Remote.PrefixID != "remote-specific" || out.Remote.PrefixCIDR != "203.0.113.128/25" {
		t.Fatalf("unexpected remote LPM: %+v", out.Remote)
	}
	assertStrings(t, out.Local.AddressSets.IDs(), []string{"set-local-business"})
	assertStrings(t, out.Remote.AddressSets.IDs(), []string{"set-both-provider", "set-out-region"})
	owned := out.Remote.AddressSets.IDs()
	owned[0] = "mutated"
	reused := make([]string, 0, out.Remote.AddressSets.Count())
	reused = out.Remote.AddressSets.AppendTo(reused)
	assertStrings(t, reused, []string{"set-both-provider", "set-out-region"})
	reused[0] = "mutated-again"
	again := snapshot.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("203.0.113.200"))
	if first, ok := again.Remote.AddressSets.At(0); !ok || first != "set-both-provider" {
		t.Fatalf("membership storage was exposed: %q %v", first, ok)
	}
	if _, ok := again.Remote.AddressSets.At(again.Remote.AddressSets.Count()); ok {
		t.Fatal("out-of-range membership lookup succeeded")
	}

	in := snapshot.ClassifyEndpoints(netip.MustParseAddr("203.0.113.200"), netip.MustParseAddr("10.1.2.3"))
	if in.Direction != DirectionIn || in.Local.Side != EndpointDst || in.Remote.Side != EndpointSrc {
		t.Fatalf("unexpected inbound classification: %+v", in)
	}
	assertStrings(t, in.Remote.AddressSets.IDs(), []string{"set-both-provider", "set-in-region"})

	unassigned := snapshot.ClassifyEndpoints(netip.MustParseAddr("10.2.1.1"), netip.MustParseAddr("198.51.100.8"))
	if unassigned.Direction != DirectionOut || unassigned.Local.PrefixID != "local-root" || unassigned.Remote.PrefixID != UnassignedDimensionID || unassigned.Remote.PrefixCIDR != "" {
		t.Fatalf("unassigned endpoint must remain an exclusive primary member: %+v", unassigned)
	}

	ipv6 := snapshot.ClassifyEndpoints(netip.MustParseAddr("2001:db8:1::10"), netip.MustParseAddr("2001:db8:2::20"))
	if ipv6.Direction != DirectionOut || ipv6.Local.PrefixID != "local-v6" || ipv6.Remote.PrefixID != "remote-v6" {
		t.Fatalf("unexpected IPv6 classification: %+v", ipv6)
	}
}

func TestAddressSetSelectorRequiresEveryLabel(t *testing.T) {
	bundle := testBundle("snapshot-a", 1, testMinute(12, 0))
	bundle.AddressSets = append(bundle.AddressSets, AddressSetDefinition{
		ID: "set-and-miss", Name: "Missing conjunction", Selector: LabelSelector{Labels: map[string][]string{
			"provider": {"isp-b"},
			"region":   {"west"},
		}}, MatchDirection: "both", Enabled: true,
	})
	snapshot, err := CompileBundle(bundle, CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	result := snapshot.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("203.0.113.200"))
	assertStrings(t, result.Remote.AddressSets.IDs(), []string{"set-both-provider", "set-out-region"})
}

func TestNestedPrefixesInheritHierarchyLabelsAndMultipleGroupsWithoutASN(t *testing.T) {
	bundle := SnapshotBundle{
		SchemaVersion: BundleSchemaVersion,
		SnapshotID:    "snapshot-hierarchy",
		TenantID:      "tenant-a",
		Version:       1,
		EffectiveFrom: testMinute(12, 0),
		Prefixes: []PrefixDefinition{
			{ID: "local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local", "business": "default"}},
			{ID: "local-customer", CIDR: "10.1.0.0/16", Labels: map[string]string{"business": "customer-a"}},
			{ID: "asia", CIDR: "203.0.0.0/16", Labels: map[string]string{"continent": "asia"}},
			{ID: "china", CIDR: "203.0.113.0/24", Labels: map[string]string{"region": "east-asia", "country": "CN", "flow.geo.country": "CN", "flow.geo.admin_code": "330000"}},
			{ID: "hangzhou", CIDR: "203.0.113.128/25", Labels: map[string]string{"province": "330000", "city": "330100", "flow.geo.city": "330100"}},
		},
		AddressSets: []AddressSetDefinition{
			{ID: "group-asia", Name: "Asia", Selector: LabelSelector{Labels: map[string][]string{"continent": {"asia"}}}, MatchDirection: "both", Enabled: true},
			{ID: "group-china", Name: "China", Selector: LabelSelector{Labels: map[string][]string{"country": {"CN"}}}, MatchDirection: "both", Enabled: true},
			{ID: "group-hangzhou", Name: "Hangzhou", Selector: LabelSelector{Labels: map[string][]string{"country": {"CN"}, "city": {"330100"}}}, MatchDirection: "both", Enabled: true},
			{ID: "group-customer", Name: "Customer A", Selector: LabelSelector{Labels: map[string][]string{"business": {"customer-a"}}}, MatchDirection: "both", Enabled: true},
		},
	}
	snapshot, err := CompileBundle(bundle, CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}

	classified := snapshot.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("203.0.113.200"))
	if classified.Direction != DirectionOut || classified.Business != "customer-a" {
		t.Fatalf("covering local labels were not inherited: %+v", classified)
	}
	if classified.Local.PrefixID != "local-customer" || classified.Remote.PrefixID != "hangzhou" {
		t.Fatalf("primary prefix must remain the most-specific match: %+v", classified)
	}
	assertStrings(t, classified.Local.AddressSets.IDs(), []string{"group-customer"})
	assertStrings(t, classified.Remote.AddressSets.IDs(), []string{"group-asia", "group-china", "group-hangzhou"})
	overridden, fields, ok := snapshot.ApplyGeoOverride(netip.MustParseAddr("203.0.113.200"), GeoInfo{})
	if !ok || overridden.Country != "CN" || overridden.AdminCode != "330000" || overridden.City != "330100" ||
		fields != GeoOverrideCountry|GeoOverrideAdminCode|GeoOverrideCity {
		t.Fatalf("nested Geo override labels were not inherited: info=%+v fields=%b", overridden, fields)
	}
}

func TestCompiledSnapshotClassifiesNonBusinessTopologies(t *testing.T) {
	snapshot, err := CompileBundle(testBundle("snapshot-a", 1, testMinute(12, 0)), CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	internal := snapshot.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("10.2.3.4"))
	if internal.Direction != DirectionInternal || internal.Local.Side != EndpointSrc || internal.Remote.Side != EndpointDst || internal.Business != "customer" {
		t.Fatalf("unexpected internal result: %+v", internal)
	}
	// An internal (local<->local) flow carries the union of in+out membership so
	// intra-network traffic keeps its address-set attribution (F11). The local
	// endpoint (10.1.2.3 in 10.1.0.0/16) is in set-local-business; the remote
	// endpoint (10.2.3.4 in the 10.0.0.0/8 root) is not in that set's prefix.
	if internal.Local.AddressSets.Count() != 1 || internal.Remote.AddressSets.Count() != 0 {
		t.Fatalf("internal flow local endpoint must carry union address-set membership: %+v", internal)
	}
	transit := snapshot.ClassifyEndpoints(netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("198.51.100.1"))
	if transit.Direction != DirectionTransit || transit.Local.IP.IsValid() || transit.Remote.IP.IsValid() {
		t.Fatalf("unexpected transit result: %+v", transit)
	}
	ambiguous := snapshot.ClassifyEndpoints(netip.Addr{}, netip.MustParseAddr("10.1.2.3"))
	if ambiguous.Direction != DirectionAmbiguous || ambiguous.Local.IP.IsValid() || ambiguous.Remote.IP.IsValid() {
		t.Fatalf("unexpected ambiguous result: %+v", ambiguous)
	}
}

func TestGeoOverrideUsesItsOwnLongestPrefixAndOverlaysSelectedGeo(t *testing.T) {
	bundle := testBundle("snapshot-a", 1, testMinute(12, 0))
	bundle.Prefixes[2].Labels["flow.geo.country"] = "CN"
	bundle.Prefixes[2].Labels["flow.geo.admin_code"] = "330100"
	bundle.Prefixes[2].Labels["flow.geo.isp_id"] = "3"
	bundle.Prefixes[2].Labels["flow.geo.asn"] = "0"
	bundle.Prefixes[2].Labels["flow.geo.reason"] = "operator correction"
	snapshot, err := CompileBundle(bundle, CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	// remote-specific /25 is the primary dimension prefix, but the independent
	// override LPM must still find remote-root /24.
	classified := snapshot.ClassifyEndpoints(netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("203.0.113.200"))
	if classified.Remote.PrefixID != "remote-specific" {
		t.Fatalf("unexpected primary prefix: %+v", classified.Remote)
	}
	base := GeoInfo{
		Country: "US", Subdivision: "California", City: "San Francisco", ISPID: 9, ASN: 64500, Version: "geo-7", Source: GeoSchemaV2,
		ContinentID: "NorthAmerica", RegionID: "NorthernAmerica", CountryID: "US", ProvinceID: "US-CA", CityID: "US-SFO",
	}
	resolved, fields, matched := snapshot.ApplyGeoOverride(classified.Remote.IP, base)
	if !matched || resolved.Country != "CN" || resolved.AdminCode != "330100" || resolved.Subdivision != "California" || resolved.City != "San Francisco" || resolved.ISPID != 3 || resolved.ASN != 0 {
		t.Fatalf("unexpected override: %+v fields=%d matched=%v", resolved, fields, matched)
	}
	if resolved.Version != "geo-7" || resolved.Source != "flow_geo_override" || fields&GeoOverrideASN == 0 {
		t.Fatalf("override provenance was not preserved: %+v fields=%d", resolved, fields)
	}
	if resolved.ContinentID != "" || resolved.RegionID != "" || resolved.CountryID != "CN" || resolved.ProvinceID != "330000" || resolved.CityID != "330100" {
		t.Fatalf("override retained an incompatible supplier Geo path: %+v", resolved)
	}
}

func TestCompileBundleRejectsInvalidGeoOverrideLabels(t *testing.T) {
	tests := []struct {
		name  string
		label map[string]string
		want  string
	}{
		{name: "unknown-key", label: map[string]string{"flow.geo.typo": "CN"}, want: "unknown Geo override"},
		{name: "reason-only", label: map[string]string{"flow.geo.reason": "note"}, want: "at least one"},
		{name: "hmt-not-canonical", label: map[string]string{"flow.geo.country": "HK"}, want: "HMT"},
		{name: "admin-without-country", label: map[string]string{"flow.geo.admin_code": "330100"}, want: "country=CN"},
		{name: "foreign-admin", label: map[string]string{"flow.geo.country": "US", "flow.geo.admin_code": "330100"}, want: "country=CN"},
		{name: "bad-isp", label: map[string]string{"flow.geo.isp_id": "65536"}, want: "16-bit"},
		{name: "bad-asn", label: map[string]string{"flow.geo.asn": "-1"}, want: "32-bit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle := testBundle("snapshot-a", 1, testMinute(12, 0))
			for key, value := range test.label {
				bundle.Prefixes[2].Labels[key] = value
			}
			if _, err := CompileBundle(bundle, CompileLimits{}); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClassifyEndpointsHotPathDoesNotAllocate(t *testing.T) {
	snapshot, err := CompileBundle(testBundle("snapshot-a", 1, testMinute(12, 0)), CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	source := netip.MustParseAddr("10.1.2.3")
	destination := netip.MustParseAddr("203.0.113.200")
	allocations := testing.AllocsPerRun(1000, func() {
		result := snapshot.ClassifyEndpoints(source, destination)
		if result.Direction != DirectionOut || result.Remote.AddressSets.Count() != 2 {
			panic("unexpected classification")
		}
	})
	if allocations != 0 {
		t.Fatalf("classify allocations/run = %f, want 0", allocations)
	}
}

func BenchmarkClassifyEndpoints(b *testing.B) {
	snapshot, err := CompileBundle(testBundle("snapshot-a", 1, testMinute(12, 0)), CompileLimits{})
	if err != nil {
		b.Fatal(err)
	}
	source := netip.MustParseAddr("10.1.2.3")
	destination := netip.MustParseAddr("203.0.113.200")
	b.ReportAllocs()
	for b.Loop() {
		result := snapshot.ClassifyEndpoints(source, destination)
		if result.Direction != DirectionOut {
			b.Fatal("unexpected classification")
		}
	}
}

func TestSnapshotCatalogSelectsByEventTime(t *testing.T) {
	first, err := CompileBundle(testBundle("snapshot-1", 1, testMinute(12, 0)), CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	secondBundle := testBundle("snapshot-2", 2, testMinute(13, 0))
	secondBundle.Prefixes[1].Labels["business"] = "version-2"
	second, err := CompileBundle(secondBundle, CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewSnapshotCatalog(second, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Install(first); err != nil {
		t.Fatalf("installing the same compiled snapshot should be idempotent: %v", err)
	}
	selected, err := catalog.Select("tenant-a", testMinute(12, 59))
	if err != nil || selected.Metadata().Version != 1 {
		t.Fatalf("selected=%+v err=%v", selected.Metadata(), err)
	}
	classified, err := catalog.ClassifyAt("tenant-a", testMinute(13, 0), netip.MustParseAddr("10.1.2.3"), netip.MustParseAddr("203.0.113.200"))
	if err != nil || classified.Version != 2 || classified.Business != "version-2" {
		t.Fatalf("classification=%+v err=%v", classified, err)
	}
	if _, err := catalog.Select("tenant-a", testMinute(11, 59)); err != ErrNoDimensionSnapshot {
		t.Fatalf("pre-history error = %v", err)
	}
	if _, err := catalog.Select("tenant-b", testMinute(13, 0)); err != ErrNoDimensionSnapshot {
		t.Fatalf("cross-tenant error = %v", err)
	}

	sameEffective, _ := CompileBundle(testBundle("snapshot-3", 3, testMinute(13, 0)), CompileLimits{})
	if err := catalog.Install(sameEffective); err == nil || !strings.Contains(err.Error(), "effective_from") {
		t.Fatalf("same effective time error = %v", err)
	}
	sameVersion, _ := CompileBundle(testBundle("snapshot-4", 2, testMinute(14, 0)), CompileLimits{})
	if err := catalog.Install(sameVersion); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("same version error = %v", err)
	}
	nonMonotonic, _ := CompileBundle(testBundle("snapshot-5", 4, testMinute(12, 30)), CompileLimits{})
	if err := catalog.Install(nonMonotonic); err == nil || !strings.Contains(err.Error(), "not monotonic") {
		t.Fatalf("non-monotonic error = %v", err)
	}
	crossTenantBundle := testBundle("snapshot-1", 1, testMinute(12, 0))
	crossTenantBundle.TenantID = "tenant-b"
	crossTenant, _ := CompileBundle(crossTenantBundle, CompileLimits{})
	if err := catalog.Install(crossTenant); err == nil || !strings.Contains(err.Error(), "id is immutable") {
		t.Fatalf("cross-tenant snapshot id error = %v", err)
	}
}

func TestGeoOverrideFieldBitsAreStableAndRejectUnknownBits(t *testing.T) {
	for field, want := range map[GeoOverrideFields]uint8{
		GeoOverrideCountry: 1, GeoOverrideAdminCode: 2, GeoOverrideSubdivision: 4,
		GeoOverrideCity: 8, GeoOverrideISPID: 16, GeoOverrideASN: 32,
	} {
		if uint8(field) != want || !field.Valid() {
			t.Fatalf("Geo override field=%08b want=%08b valid=%t", field, want, field.Valid())
		}
	}
	if !GeoOverrideFields(0).Valid() || !GeoOverrideKnownFields.Valid() || GeoOverrideFields(1<<7).Valid() {
		t.Fatal("Geo override persisted bitset validity changed")
	}
}

func testBundle(snapshotID string, version uint64, effectiveFrom time.Time) SnapshotBundle {
	return SnapshotBundle{
		SchemaVersion: BundleSchemaVersion,
		SnapshotID:    snapshotID,
		TenantID:      "tenant-a",
		Version:       version,
		EffectiveFrom: effectiveFrom,
		Prefixes: []PrefixDefinition{
			{ID: "local-root", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local", "business": "default"}},
			{ID: "local-specific", CIDR: "10.1.0.0/16", Labels: map[string]string{"flow": "local", "business": "customer"}},
			{ID: "remote-root", CIDR: "203.0.113.0/24", Labels: map[string]string{"provider": "isp-a", "region": "west"}},
			{ID: "remote-specific", CIDR: "203.0.113.128/25", Labels: map[string]string{"provider": "isp-b", "region": "east"}},
			{ID: "local-v6", CIDR: "2001:db8:1::/48", Labels: map[string]string{"flow": "local", "business": "ipv6"}},
			{ID: "remote-v6", CIDR: "2001:db8:2::/48", Labels: map[string]string{"provider": "isp-v6"}},
		},
		AddressSets: []AddressSetDefinition{
			{ID: "set-out-region", Name: "Outbound east", Selector: LabelSelector{Labels: map[string][]string{"region": {"east"}}}, MatchDirection: "out", Enabled: true},
			{ID: "set-in-region", Name: "Inbound east", Selector: LabelSelector{Labels: map[string][]string{"region": {"east"}}}, MatchDirection: "in", Enabled: true},
			{ID: "set-both-provider", Name: "ISP B", Selector: LabelSelector{Labels: map[string][]string{"provider": {"isp-b"}}}, MatchDirection: "both", Enabled: true},
			{ID: "set-local-business", Name: "Customer", Selector: LabelSelector{Labels: map[string][]string{"business": {"customer"}}}, MatchDirection: "both", Enabled: true},
			{ID: "set-disabled", Name: "Disabled", Selector: LabelSelector{Labels: map[string][]string{"region": {"east"}}}, MatchDirection: "both", Enabled: false},
		},
	}
}

func testMinute(hour, minute int) time.Time {
	return time.Date(2026, 9, 5, hour, minute, 0, 0, time.UTC)
}

func bundleChecksum(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func assertStrings(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %v, want %v", got, want)
	}
}
