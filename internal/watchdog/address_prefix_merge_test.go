package watchdog

import "testing"

func TestPreviewAddressPrefixMergeCoalescesOnlyEqualAttribution(t *testing.T) {
	asn := func(value uint32) *uint32 { return &value }

	// Same attribution + adjacent CIDRs → coalesce into one.
	preview, err := PreviewAddressPrefixMerge([]AddressPrefix{
		{CIDR: "10.0.0.0/24", GeoLeafID: "g1", OperatorID: "o1", Source: "manual"},
		{CIDR: "10.0.1.0/24", GeoLeafID: "g1", OperatorID: "o1", Source: "manual"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if preview.InputPrefixes != 2 || preview.ResultPrefixes != 1 || len(preview.Groups) != 1 {
		t.Fatalf("equal-attribution adjacent should merge to 1: %+v", preview)
	}
	if len(preview.Groups[0].ResultCIDRs) != 1 || preview.Groups[0].ResultCIDRs[0] != "10.0.0.0/23" {
		t.Fatalf("merged CIDR = %v, want [10.0.0.0/23]", preview.Groups[0].ResultCIDRs)
	}

	// Each differing attribute must block the merge, keeping coverage attributed.
	for name, prefixes := range map[string][]AddressPrefix{
		"geo": {
			{CIDR: "10.0.0.0/24", GeoLeafID: "g1", Source: "manual"},
			{CIDR: "10.0.1.0/24", GeoLeafID: "g2", Source: "manual"},
		},
		"operator": {
			{CIDR: "10.0.0.0/24", GeoLeafID: "g1", OperatorID: "o1", Source: "manual"},
			{CIDR: "10.0.1.0/24", GeoLeafID: "g1", OperatorID: "o2", Source: "manual"},
		},
		"asn": {
			{CIDR: "10.0.0.0/24", GeoLeafID: "g1", ASN: asn(100), Source: "manual"},
			{CIDR: "10.0.1.0/24", GeoLeafID: "g1", ASN: asn(200), Source: "manual"},
		},
		"labels": {
			{CIDR: "10.0.0.0/24", GeoLeafID: "g1", Labels: map[string]string{"t": "a"}, Source: "manual"},
			{CIDR: "10.0.1.0/24", GeoLeafID: "g1", Labels: map[string]string{"t": "b"}, Source: "manual"},
		},
		"source": {
			{CIDR: "10.0.0.0/24", GeoLeafID: "g1", Source: "manual"},
			{CIDR: "10.0.1.0/24", GeoLeafID: "g1", Source: "import"},
		},
	} {
		preview, err := PreviewAddressPrefixMerge(prefixes)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if preview.ResultPrefixes != 2 || len(preview.Groups) != 2 {
			t.Fatalf("%s differs → must not merge: %+v", name, preview)
		}
	}

	// Same attribution but non-adjacent → one group, no reduction.
	preview, err = PreviewAddressPrefixMerge([]AddressPrefix{
		{CIDR: "10.0.0.0/24", GeoLeafID: "g1", Source: "manual"},
		{CIDR: "10.0.2.0/24", GeoLeafID: "g1", Source: "manual"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Groups) != 1 || preview.ResultPrefixes != 2 {
		t.Fatalf("non-adjacent equal attribution should stay 2 in one group: %+v", preview)
	}

	// Order-independent labels still group together.
	preview, err = PreviewAddressPrefixMerge([]AddressPrefix{
		{CIDR: "10.0.0.0/24", GeoLeafID: "g1", Labels: map[string]string{"a": "1", "b": "2"}, Source: "manual"},
		{CIDR: "10.0.1.0/24", GeoLeafID: "g1", Labels: map[string]string{"b": "2", "a": "1"}, Source: "manual"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if preview.ResultPrefixes != 1 {
		t.Fatalf("identical labels (any order) should merge: %+v", preview)
	}
}
