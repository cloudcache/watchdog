package server

import "testing"

func TestNormalizeFlowCustomerRangesAcceptsNewlineCIDRsAndCanonicalizes(t *testing.T) {
	prefixes, err := normalizeFlowCustomerRanges([]string{
		"192.0.2.99/24",
		"198.51.100.0/25",
		"198.51.100.0/25",
		"2001:db8:100::/48",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"192.0.2.0/24": false, "198.51.100.0/25": false, "2001:db8:100::/48": false,
	}
	for _, prefix := range prefixes {
		if _, exists := want[prefix.String()]; exists {
			want[prefix.String()] = true
		}
	}
	for prefix, found := range want {
		if !found {
			t.Fatalf("normalized prefixes %v do not contain %s", prefixes, prefix)
		}
	}
}

func TestNormalizeFlowCustomerRangesRejectsEmptyAndInvalid(t *testing.T) {
	for name, values := range map[string][]string{
		"empty":   nil,
		"ip":      {"192.0.2.1"},
		"invalid": {"not-an-address"},
		"range":   {"192.0.2.1-192.0.2.10"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := normalizeFlowCustomerRanges(values); err == nil {
				t.Fatal("invalid customer source range was accepted")
			}
		})
	}
}
