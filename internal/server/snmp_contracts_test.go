package server

import (
	"net/url"
	"testing"
)

func TestParseSNMPEventVTableContract(t *testing.T) {
	query, err := parseSNMPEventTableQuery(url.Values{
		"q": {"link"}, "limit": {"50"}, "offset": {"100"}, "sort": {"severity"}, "order": {"asc"},
		"filter.severity": {"warning,error"}, "filter.source": {"trap"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if query.Limit != 50 || query.Offset != 100 || query.SortBy != "severity" || query.SortDirection != "ASC" || len(query.Severities) != 2 {
		t.Fatalf("query=%+v", query)
	}
	if _, err := parseSNMPEventTableQuery(url.Values{"tenant_id": {"removed"}}); err == nil {
		t.Fatal("removed tenant filter was accepted")
	}
}

func TestParseVMQueryCompatibilitySelector(t *testing.T) {
	selector, err := parseVMQuerySelector(`watchdog_snmp_if_in_bps{tenant_id="legacy",target_id="device-a",port_id="port-a"}`)
	if err != nil {
		t.Fatal(err)
	}
	if selector.Metric != "watchdog_snmp_if_in_bps" || selector.DeviceID != "device-a" || selector.PortID != "port-a" {
		t.Fatalf("selector=%+v", selector)
	}
	for _, invalid := range []string{"", `sum(watchdog_snmp_if_in_bps)`, `metric{device_id=~".*"}`, `metric{sql="drop"}`} {
		if _, err := parseVMQuerySelector(invalid); err == nil {
			t.Fatalf("unsafe selector %q was accepted", invalid)
		}
	}
}

func TestNormalizeTrapSource(t *testing.T) {
	for input, want := range map[string]string{"192.0.2.1:162": "192.0.2.1", "[2001:db8::1]:162": "2001:db8::1", "2001:db8::2": "2001:db8::2"} {
		if got := normalizeTrapSource(input); got != want {
			t.Fatalf("normalizeTrapSource(%q)=%q want=%q", input, got, want)
		}
	}
}
