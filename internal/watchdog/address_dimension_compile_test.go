package watchdog

import (
	"net/netip"
	"testing"
	"time"
)

func TestCompileAddressDimensionDraftResolvesTypedTaxonomy(t *testing.T) {
	asn := uint32(4134)
	source := AddressDimensionDraftSource{
		Geography: []GeoDictionaryNode{
			{ID: "geo-continent", Kind: GeoKindContinent, Code: "AS", Name: "Asia", Enabled: true},
			{ID: "geo-country-cn", Kind: GeoKindCountry, Code: "CN", ParentID: "geo-continent", Name: "China", Enabled: true},
			{ID: "geo-province-zj", Kind: GeoKindProvince, Code: "330000", ParentID: "geo-country-cn", Name: "Zhejiang", Enabled: true},
		},
		Operators: []ISPOperator{{ID: "operator-telecom", FlowISPID: 3, Code: "CT", Name: "China Telecom", Category: "carrier", Enabled: true}},
		Prefixes: []AddressPrefix{{
			ID: "prefix-hangzhou", CIDR: "192.0.2.0/24", GeoLeafID: "geo-province-zj",
			OperatorID: "operator-telecom", ASN: &asn, Labels: map[string]string{"flow": "local", "business": "office"},
		}},
		Sets: []AddressSet{{
			ID: "set-zhejiang-telecom", Name: "Zhejiang Telecom", MatchDirection: "both", Enabled: true,
			Selector: map[string]any{
				"geo_node_ids": []string{"geo-province-zj"}, "operator_ids": []string{"operator-telecom"},
				"asns": []float64{4134}, "families": []float64{4},
			},
		}},
	}
	draft, digest, err := CompileAddressDimensionDraft(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(draft.Prefixes) != 1 || len(draft.AddressSets) != 1 || digest[:7] != "sha256:" {
		t.Fatalf("unexpected draft: %#v %s", draft, digest)
	}
	labels := draft.Prefixes[0].Labels
	for key, want := range map[string]string{
		"geo.continent_id": "geo-continent", "geo.country_id": "geo-country-cn", "geo.province_id": "geo-province-zj",
		"operator.id": "operator-telecom", "operator.code": "CT", "asn": "4134", "ip.family": "4",
	} {
		if labels[key] != want {
			t.Fatalf("label %s = %q, want %q", key, labels[key], want)
		}
	}
	effective := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	_, compiled, _, err := encodeAddressDimensionBundle(draft, "snapshot-address", "tenant-address", 1, effective)
	if err != nil {
		t.Fatal(err)
	}
	result := compiled.ClassifyEndpoints(netip.MustParseAddr("192.0.2.10"), netip.MustParseAddr("203.0.113.10"))
	if result.Direction != "out" || result.Business != "office" || result.Local.AddressSets.Count() != 1 {
		t.Fatalf("unexpected classification: %#v", result)
	}
}

func TestCompileAddressDimensionDraftIsDeterministic(t *testing.T) {
	left := AddressDimensionDraftSource{Prefixes: []AddressPrefix{
		{ID: "prefix-b", CIDR: "2001:db8::/32", Labels: map[string]string{"flow": "local"}},
		{ID: "prefix-a", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local"}},
	}}
	right := AddressDimensionDraftSource{Prefixes: []AddressPrefix{left.Prefixes[1], left.Prefixes[0]}}
	leftDraft, leftDigest, err := CompileAddressDimensionDraft(left)
	if err != nil {
		t.Fatal(err)
	}
	rightDraft, rightDigest, err := CompileAddressDimensionDraft(right)
	if err != nil {
		t.Fatal(err)
	}
	if leftDigest != rightDigest || leftDraft.Prefixes[0].CIDR != rightDraft.Prefixes[0].CIDR {
		t.Fatalf("draft ordering is not deterministic: %s != %s", leftDigest, rightDigest)
	}
}

func TestCompileAddressDimensionDraftPinsAndOrdersActiveImportSources(t *testing.T) {
	combined := AddressDimensionSource{
		Slot: AddressImportSlotCombined, ImportID: "import-combined", SlotRowVersion: 2,
		ChecksumSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RowCountV4: 10, RowCountV6: 2,
	}
	geo := AddressDimensionSource{
		Slot: AddressImportSlotGeo, ImportID: "import-geo", SlotRowVersion: 3,
		ChecksumSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", RowCountV4: 20,
	}
	asn := AddressDimensionSource{
		Slot: AddressImportSlotASN, ImportID: "import-asn", SlotRowVersion: 4,
		ChecksumSHA256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", RowCountV6: 30,
	}
	left, leftDigest, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{Sources: []AddressDimensionSource{asn, combined, geo}})
	if err != nil {
		t.Fatal(err)
	}
	right, rightDigest, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{Sources: []AddressDimensionSource{geo, asn, combined}})
	if err != nil {
		t.Fatal(err)
	}
	if leftDigest != rightDigest || len(left.Sources) != 3 || left.Sources[0].Slot != AddressImportSlotCombined || left.Sources[1].Slot != AddressImportSlotGeo || left.Sources[2].Slot != AddressImportSlotASN {
		t.Fatalf("source manifest is not deterministic: %#v %#v %s %s", left.Sources, right.Sources, leftDigest, rightDigest)
	}
	changed := right.Sources
	changed[0].SlotRowVersion++
	_, changedDigest, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{Sources: changed})
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == leftDigest {
		t.Fatal("slot version change did not invalidate the preview digest")
	}
	if count, err := countAddressDimensionSourcePrefixes(left.Sources); err != nil || count != 62 {
		t.Fatalf("source prefix count = %d, %v", count, err)
	}
}

func TestCompileAddressDimensionDraftRejectsInvalidSourceManifest(t *testing.T) {
	valid := AddressDimensionSource{
		Slot: AddressImportSlotGeo, ImportID: "import-geo", SlotRowVersion: 1,
		ChecksumSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for name, sources := range map[string][]AddressDimensionSource{
		"duplicate slot": {valid, valid},
		"unknown slot":   {{Slot: "other", ImportID: "import-other", SlotRowVersion: 1, ChecksumSHA256: valid.ChecksumSHA256}},
		"bad checksum":   {{Slot: AddressImportSlotGeo, ImportID: "import-geo", SlotRowVersion: 1, ChecksumSHA256: "bad"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{Sources: sources}); err == nil {
				t.Fatal("invalid source manifest was accepted")
			}
		})
	}
}

func TestCompileAddressDimensionDraftRejectsDisabledReferences(t *testing.T) {
	_, _, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{
		Geography: []GeoDictionaryNode{{ID: "geo-disabled", Kind: GeoKindCountry, Code: "CN", Name: "China", Enabled: false}},
		Prefixes:  []AddressPrefix{{ID: "prefix-a", CIDR: "10.0.0.0/8", GeoLeafID: "geo-disabled", Labels: map[string]string{"flow": "local"}}},
	})
	if err == nil {
		t.Fatal("expected disabled geography reference to fail publication")
	}
}
