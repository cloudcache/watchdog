package snmpdomain

import (
	"context"
	"strconv"
	"strings"
)

const snmpCollectorModuleVLANs = "vlans"

var (
	snmpOIDDot1qVlanStaticName      = snmpMIBOID("Q-BRIDGE-MIB::dot1qVlanStaticName")
	snmpOIDDot1qVlanStaticRowStatus = snmpMIBOID("Q-BRIDGE-MIB::dot1qVlanStaticRowStatus")
)

type SNMPVLANsDiscoveryModule struct{}

func (SNMPVLANsDiscoveryModule) Name() string { return snmpCollectorModuleVLANs }

func (m SNMPVLANsDiscoveryModule) Discover(ctx context.Context, req DiscoveryContext) (DiscoveryResult, error) {
	if req.Query == nil {
		return DiscoveryResult{}, errSNMPCollectorQueryRequired
	}

	// Walk dot1qVlanStaticName for VLAN names
	resp, err := req.Query.Walk(ctx, WalkRequest{
		Target: req.Target, Profile: req.Profile, BaseOID: snmpOIDDot1qVlanStaticName,
		Flags: QueryFlags{UseBulk: true, MaxRepetitions: 25},
	})
	nameMap := map[uint32]string{}
	if err == nil {
		names := valuesByNumericSuffix(snmpOIDDot1qVlanStaticName, resp)
		for index, name := range names {
			cleaned := cleanSNMPValue(name)
			if cleaned != "" {
				nameMap[uint32(index)] = cleaned
			}
		}
	}

	// Extract configured VLANs from Vlanif interfaces in ports
	configuredVLANs := map[uint32]bool{}
	for _, port := range []Port{} {
		_ = port
	}
	// Ports are discovered before vlans in the module order,
	// but they're in the same result set. We need to check ports
	// from the device's existing port list or from the discovery context.
	// Since ports module runs before vlans, the ports aren't available here.
	// Instead, walk ifDescr to find Vlanif interfaces.
	ifDescrResp, ifErr := req.Query.Walk(ctx, WalkRequest{
		Target: req.Target, Profile: req.Profile, BaseOID: snmpOIDIfDescr,
		Flags: QueryFlags{UseBulk: true, MaxRepetitions: 25},
	})
	if ifErr == nil {
		descrs := valuesByNumericSuffix(snmpOIDIfDescr, ifDescrResp)
		for _, descr := range descrs {
			name := cleanSNMPValue(descr)
			if vlanID, ok := extractVlanifID(name); ok {
				configuredVLANs[vlanID] = true
			}
		}
	}

	// Also check ifName
	ifNameResp, ifErr2 := req.Query.Walk(ctx, WalkRequest{
		Target: req.Target, Profile: req.Profile, BaseOID: snmpOIDIfName,
		Flags: QueryFlags{UseBulk: true, MaxRepetitions: 25},
	})
	if ifErr2 == nil {
		names := valuesByNumericSuffix(snmpOIDIfName, ifNameResp)
		for _, n := range names {
			name := cleanSNMPValue(n)
			if vlanID, ok := extractVlanifID(name); ok {
				configuredVLANs[vlanID] = true
			}
		}
	}

	vlans := make([]VLAN, 0, len(configuredVLANs))
	for vlanID := range configuredVLANs {
		name := nameMap[vlanID]
		if name == "" {
			name = "VLAN " + strconv.FormatUint(uint64(vlanID), 10)
		}
		vlans = append(vlans, VLAN{
			VLANID: vlanID,
			Name:   name,
			Status: "active",
		})
	}

	// If no Vlanif interfaces found, fall back to Q-BRIDGE-MIB names only
	if len(vlans) == 0 && len(nameMap) > 0 {
		for vlanID, name := range nameMap {
			if vlanID == 0 || vlanID == 1 || vlanID > 4094 {
				continue
			}
			vlans = append(vlans, VLAN{
				VLANID: vlanID,
				Name:   name,
				Status: "active",
			})
		}
	}

	return DiscoveryResult{VLANs: vlans}, nil
}

func extractVlanifID(name string) (uint32, bool) {
	lower := strings.ToLower(name)
	if !strings.HasPrefix(lower, "vlanif") {
		return 0, false
	}
	numStr := name[6:]
	id, err := strconv.ParseUint(numStr, 10, 32)
	if err != nil || id == 0 || id > 4094 {
		return 0, false
	}
	return uint32(id), true
}
