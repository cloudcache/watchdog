package snmpdomain

import (
	"context"
	"testing"
)

func TestInterfaceAddressDiscoveryUsesIPMIBIndexesForBothFamilies(t *testing.T) {
	provider := snmpInterfaceAddressProvider{
		name: "test-ip-mib", ifIndex: ".1", prefix: ".2", origin: ".3",
	}
	query := fakeQueryEngine{walks: map[string]QueryResponse{
		"1": {VarBinds: []VarBind{
			{OID: "1.1.4.192.0.2.10", Value: uint32(7)},
			{OID: "1.2.16.32.1.13.184.0.0.0.0.0.0.0.0.0.0.0.16", Value: uint32(8)},
		}},
		"2": {VarBinds: []VarBind{
			{OID: "2.1.4.192.0.2.10", Value: ".1.3.6.1.2.1.4.32.1.5.7.1.4.192.0.2.0.24"},
			{OID: "2.2.16.32.1.13.184.0.0.0.0.0.0.0.0.0.0.0.16", Value: ".1.3.6.1.2.1.4.32.1.5.8.2.16.32.1.13.184.0.0.0.0.0.0.0.0.0.0.0.0.64"},
		}},
		"3": {VarBinds: []VarBind{
			{OID: "3.1.4.192.0.2.10", Value: uint32(2)},
			{OID: "3.2.16.32.1.13.184.0.0.0.0.0.0.0.0.0.0.0.16", Value: uint32(5)},
		}},
	}}
	addresses := discoverSNMPInterfaceAddressesWithProvider(context.Background(), DiscoveryContext{
		Device: Device{ID: "device-a"}, Query: query,
	}, provider)
	if len(addresses) != 2 {
		t.Fatalf("addresses = %#v", addresses)
	}
	byFamily := map[string]InterfaceAddress{}
	for _, address := range addresses {
		byFamily[address.Family] = address
	}
	if address := byFamily["ipv4"]; address.Address != "192.0.2.10" || address.PrefixLength != 24 || address.Origin != "manual" {
		t.Fatalf("unexpected IPv4 address: %#v", address)
	}
	if address := byFamily["ipv6"]; address.Address != "2001:db8::10" || address.PrefixLength != 64 || address.Origin != "linklayer" {
		t.Fatalf("unexpected IPv6 address: %#v", address)
	}
}
