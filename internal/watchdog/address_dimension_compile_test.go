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
	if len(draft.Operators) != 1 || draft.Operators[0].FlowISPID != 3 || draft.Operators[0].ID != "operator-telecom" {
		t.Fatalf("operator definitions = %#v", draft.Operators)
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

func TestCompileAddressDimensionDraftCanonicalizesOperators(t *testing.T) {
	first := ISPOperator{ID: "operator-first", FlowISPID: 2, Code: "FIRST", Name: "First", Category: "carrier", ASNs: []uint32{64501, 64500, 64501}, Enabled: true}
	second := ISPOperator{ID: "operator-second", FlowISPID: 1, Code: "SECOND", Name: "Second", Category: "other", Enabled: false}
	left, leftDigest, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{Operators: []ISPOperator{first, second}})
	if err != nil {
		t.Fatal(err)
	}
	right, rightDigest, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{Operators: []ISPOperator{second, first}})
	if err != nil {
		t.Fatal(err)
	}
	if leftDigest != rightDigest || len(left.Operators) != 2 || left.Operators[0].FlowISPID != 1 || right.Operators[1].FlowISPID != 2 {
		t.Fatalf("operator draft is not deterministic: %#v %#v %s %s", left.Operators, right.Operators, leftDigest, rightDigest)
	}
	if got := left.Operators[1].ASNs; len(got) != 2 || got[0] != 64500 || got[1] != 64501 {
		t.Fatalf("operator ASNs are not canonical: %v", got)
	}
	changed := second
	changed.Name = "Second renamed"
	_, changedDigest, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{Operators: []ISPOperator{first, changed}})
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == leftDigest {
		t.Fatal("operator change did not invalidate the draft digest")
	}
	first.FlowISPID = 0
	if _, _, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{Operators: []ISPOperator{first}}); err == nil {
		t.Fatal("zero Flow ISP id was accepted")
	}
	first.FlowISPID = 2
	second.FlowISPID = 2
	if _, _, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{Operators: []ISPOperator{first, second}}); err == nil {
		t.Fatal("duplicate Flow ISP id was accepted")
	}
	second.FlowISPID = 1
	second.Enabled = true
	second.ASNs = []uint32{64500}
	if _, _, err := CompileAddressDimensionDraft(AddressDimensionDraftSource{Operators: []ISPOperator{first, second}}); err == nil {
		t.Fatal("ASN assigned to two enabled operators was accepted")
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
