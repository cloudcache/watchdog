package watchdog

import (
	"reflect"
	"testing"
)

func TestPreviewAddressSetOperationNormalizesCIDRIPAndRanges(t *testing.T) {
	preview, err := PreviewAddressSetOperation(AddressSetOperationRequest{
		Operation: "normalize",
		Left:      []string{"192.0.2.9/24", "192.0.3.0-192.0.3.255", "2001:db8::1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"192.0.2.0/23", "2001:db8::1/128"}
	if !reflect.DeepEqual(preview.Result, want) {
		t.Fatalf("result = %#v, want %#v", preview.Result, want)
	}
	if preview.ResultAddressesV4 != "512" || preview.ResultAddressesV6 != "1" || !preview.Lossless {
		t.Fatalf("unexpected counts/lossless: %#v", preview)
	}
}

func TestPreviewAddressSetOperationSetAlgebraIPv4AndIPv6(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		left      []string
		right     []string
		universe  []string
		want      []string
	}{
		{name: "union", operation: "union", left: []string{"10.0.0.0/25"}, right: []string{"10.0.0.128/25"}, want: []string{"10.0.0.0/24"}},
		{name: "intersection", operation: "intersection", left: []string{"10.0.0.0/24", "2001:db8::/126"}, right: []string{"10.0.0.128/25", "2001:db8::2/127"}, want: []string{"10.0.0.128/25", "2001:db8::2/127"}},
		{name: "difference", operation: "difference", left: []string{"10.0.0.0/24"}, right: []string{"10.0.0.64/26", "10.0.0.192/26"}, want: []string{"10.0.0.0/26", "10.0.0.128/26"}},
		{name: "finite complement", operation: "complement", left: []string{"2001:db8::1/128"}, universe: []string{"2001:db8::/126"}, want: []string{"2001:db8::/128", "2001:db8::2/127"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			preview, err := PreviewAddressSetOperation(AddressSetOperationRequest{Operation: test.operation, Left: test.left, Right: test.right, Universe: test.universe})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(preview.Result, test.want) {
				t.Fatalf("result = %#v, want %#v", preview.Result, test.want)
			}
			if !preview.Lossless || preview.RequiresConfirmation {
				t.Fatalf("set operation must not be reported as an implicit expansion: %#v", preview)
			}
		})
	}
}

func TestPreviewAddressSetOperationCoverReportsExpansion(t *testing.T) {
	target := 24
	preview, err := PreviewAddressSetOperation(AddressSetOperationRequest{
		Operation: "cover", Left: []string{"203.0.113.64-203.0.113.127"}, TargetPrefixV4: &target,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(preview.Result, []string{"203.0.113.0/24"}) {
		t.Fatalf("result = %#v", preview.Result)
	}
	if preview.AddedAddressesV4 != "192" || preview.Lossless || !preview.RequiresConfirmation {
		t.Fatalf("expansion contract not reported: %#v", preview)
	}
}

func TestPreviewAddressSetOperationDetectsOverlaps(t *testing.T) {
	preview, err := PreviewAddressSetOperation(AddressSetOperationRequest{
		Operation: "union",
		Left:      []string{"10.0.0.0/24", "10.0.0.128/25"},
		Right:     []string{"10.0.0.192/26"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if preview.OverlapCount != 3 || len(preview.Overlaps) != 3 {
		t.Fatalf("overlaps = %d %#v, want 3", preview.OverlapCount, preview.Overlaps)
	}
}

func TestPreviewAddressSetOperationRejectsAmbiguousOrUnboundedRequests(t *testing.T) {
	tests := []AddressSetOperationRequest{
		{Operation: "normalize", Left: []string{"bad"}},
		{Operation: "complement", Left: []string{"10.0.0.0/24"}},
		{Operation: "difference", Left: []string{"10.0.0.0/24"}},
		{Operation: "intersection", Left: []string{"10.0.0.0/24"}},
		{Operation: "cover", Left: []string{"10.0.0.1"}},
		{Operation: "normalize", Left: []string{"10.0.0.2-10.0.0.1"}},
		{Operation: "normalize", Left: []string{"10.0.0.1-2001:db8::1"}},
	}
	for index, request := range tests {
		if _, err := PreviewAddressSetOperation(request); err == nil {
			t.Fatalf("request %d unexpectedly succeeded: %#v", index, request)
		}
	}
}

func TestPreviewAddressSetOperationHandlesFullIPv6Universe(t *testing.T) {
	preview, err := PreviewAddressSetOperation(AddressSetOperationRequest{
		Operation: "difference", Left: []string{"::/0"}, Right: []string{"::/1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(preview.Result, []string{"8000::/1"}) {
		t.Fatalf("result = %#v", preview.Result)
	}
	if preview.ResultAddressesV6 != "170141183460469231731687303715884105728" {
		t.Fatalf("v6 count = %s", preview.ResultAddressesV6)
	}
}
