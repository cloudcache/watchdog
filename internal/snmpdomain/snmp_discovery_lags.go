package snmpdomain

import (
	"context"
	"fmt"
)

const snmpCollectorModuleLAGs = "lags"

var (
	snmpOIDDot3adAggMACAddress = snmpMIBOID("IEEE8023-LAG-MIB::dot3adAggMACAddress")
	snmpOIDDot3adAggAggMode    = snmpMIBOID("IEEE8023-LAG-MIB::dot3adAggActorAdminKey")
)

type SNMPLAGsDiscoveryModule struct{}

func (SNMPLAGsDiscoveryModule) Name() string { return snmpCollectorModuleLAGs }

func (m SNMPLAGsDiscoveryModule) Discover(ctx context.Context, req DiscoveryContext) (DiscoveryResult, error) {
	if req.Query == nil {
		return DiscoveryResult{}, errSNMPCollectorQueryRequired
	}
	resp, err := req.Query.Walk(ctx, WalkRequest{
		Target: req.Target, Profile: req.Profile, BaseOID: snmpOIDDot3adAggMACAddress,
		Flags: QueryFlags{UseBulk: true, MaxRepetitions: 25},
	})
	if err != nil {
		return DiscoveryResult{}, err
	}
	macs := valuesByNumericSuffix(snmpOIDDot3adAggMACAddress, resp)
	resp2, _ := req.Query.Walk(ctx, WalkRequest{
		Target: req.Target, Profile: req.Profile, BaseOID: snmpOIDDot3adAggAggMode,
		Flags: QueryFlags{UseBulk: true, MaxRepetitions: 25},
	})
	modes := valuesByNumericSuffix(snmpOIDDot3adAggAggMode, resp2)
	lags := make([]LAGGroup, 0, len(macs))
	for index, mac := range macs {
		if index == 0 {
			continue
		}
		lags = append(lags, LAGGroup{
			AggregateIndex: index,
			MACAddress:     formatMacAddress(cleanSNMPValue(mac)),
			Mode:           lagMode(modes[index]),
		})
	}
	return DiscoveryResult{LAGs: lags}, nil
}

func lagMode(value string) string {
	switch parseUintValue(value) {
	case 0:
		return "unknown"
	case 1:
		return "active"
	case 2:
		return "passive"
	case 3:
		return "lacp"
	default:
		return ""
	}
}

func formatMacAddress(value string) string {
	if len(value) == 0 {
		return ""
	}
	hex := fmt.Sprintf("%x", []byte(value))
	if len(hex) < 12 {
		return value
	}
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s", hex[0:2], hex[2:4], hex[4:6], hex[6:8], hex[8:10], hex[10:12])
}
