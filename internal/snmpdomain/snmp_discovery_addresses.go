package snmpdomain

import (
	"context"
	"net"
	"strconv"
	"strings"
)

// Interface address discovery is selected by MIB capability, not by the
// detected device OS. RFC 4293 IP-MIB is preferred because one table covers
// IPv4, IPv6 and scoped IPv6 addresses. The older IPv4/IPv6 tables are only
// compatibility providers for agents that do not implement ipAddressTable.
type snmpInterfaceAddressProvider struct {
	name     string
	ifIndex  string
	prefix   string
	origin   string
	legacyV4 bool
	legacyV6 bool
	netmask  string
}

var defaultSNMPInterfaceAddressProviders = []snmpInterfaceAddressProvider{
	{
		name:    "IP-MIB::ipAddressTable",
		ifIndex: "IP-MIB::ipAddressIfIndex",
		prefix:  "IP-MIB::ipAddressPrefix",
		origin:  "IP-MIB::ipAddressOrigin",
	},
	{
		name:     "IP-MIB::ipAddrTable",
		ifIndex:  "IP-MIB::ipAdEntIfIndex",
		netmask:  "IP-MIB::ipAdEntNetMask",
		legacyV4: true,
	},
	{
		name:     "IPV6-MIB::ipv6AddrTable",
		prefix:   "IPV6-MIB::ipv6AddrPfxLength",
		legacyV6: true,
	},
}

func discoverSNMPInterfaceAddresses(ctx context.Context, req DiscoveryContext) []InterfaceAddress {
	seenFamilies := map[string]bool{}
	var result []InterfaceAddress
	for _, provider := range defaultSNMPInterfaceAddressProviders {
		if provider.legacyV4 && seenFamilies["ipv4"] || provider.legacyV6 && seenFamilies["ipv6"] {
			continue
		}
		addresses := discoverSNMPInterfaceAddressesWithProvider(ctx, req, provider)
		for _, address := range addresses {
			seenFamilies[address.Family] = true
			result = append(result, address)
		}
	}
	return deduplicateSNMPInterfaceAddresses(result)
}

func discoverSNMPInterfaceAddressesWithProvider(ctx context.Context, req DiscoveryContext, provider snmpInterfaceAddressProvider) []InterfaceAddress {
	if provider.legacyV4 {
		return discoverLegacyIPv4Addresses(ctx, req, provider)
	}
	if provider.legacyV6 {
		return discoverLegacyIPv6Addresses(ctx, req, provider)
	}
	ifIndexOID, ok := resolveSNMPCapabilityOID(provider.ifIndex)
	if !ok {
		return nil
	}
	ifIndexes := walkSNMPColumn(ctx, req, ifIndexOID)
	if len(ifIndexes) == 0 {
		return nil
	}
	prefixOID, _ := resolveSNMPCapabilityOID(provider.prefix)
	originOID, _ := resolveSNMPCapabilityOID(provider.origin)
	prefixes := walkSNMPColumn(ctx, req, prefixOID)
	origins := walkSNMPColumn(ctx, req, originOID)
	addresses := make([]InterfaceAddress, 0, len(ifIndexes))
	for index, ifIndexValue := range ifIndexes {
		family, ip, ok := parseIPAddressTableIndex(index)
		if !ok {
			continue
		}
		ifIndex := parseUintValue(stringValue(ifIndexValue))
		if ifIndex == 0 {
			continue
		}
		prefixLength := prefixLengthFromRowPointer(prefixes[index], family)
		addresses = append(addresses, newSNMPInterfaceAddress(req, provider.name, ifIndex, family, ip, prefixLength, ipAddressOrigin(origins[index])))
	}
	return addresses
}

func discoverLegacyIPv4Addresses(ctx context.Context, req DiscoveryContext, provider snmpInterfaceAddressProvider) []InterfaceAddress {
	ifIndexOID, ok := resolveSNMPCapabilityOID(provider.ifIndex)
	if !ok {
		return nil
	}
	ifIndexes := walkSNMPColumn(ctx, req, ifIndexOID)
	if len(ifIndexes) == 0 {
		return nil
	}
	netmaskOID, _ := resolveSNMPCapabilityOID(provider.netmask)
	netmasks := walkSNMPColumn(ctx, req, netmaskOID)
	addresses := make([]InterfaceAddress, 0, len(ifIndexes))
	for index, ifIndexValue := range ifIndexes {
		ip := net.ParseIP(index).To4()
		ifIndex := parseUintValue(stringValue(ifIndexValue))
		if ip == nil || ifIndex == 0 {
			continue
		}
		prefixLength := uint8(32)
		if mask := net.ParseIP(stringValue(netmasks[index])).To4(); mask != nil {
			if ones, bits := net.IPMask(mask).Size(); bits == 32 && ones >= 0 {
				prefixLength = uint8(ones)
			}
		}
		addresses = append(addresses, newSNMPInterfaceAddress(req, provider.name, ifIndex, "ipv4", ip, prefixLength, ""))
	}
	return addresses
}

func discoverLegacyIPv6Addresses(ctx context.Context, req DiscoveryContext, provider snmpInterfaceAddressProvider) []InterfaceAddress {
	prefixOID, ok := resolveSNMPCapabilityOID(provider.prefix)
	if !ok {
		return nil
	}
	prefixes := walkSNMPColumn(ctx, req, prefixOID)
	addresses := make([]InterfaceAddress, 0, len(prefixes))
	for index, prefixValue := range prefixes {
		parts, ok := parseNumericOIDParts(index)
		if !ok || len(parts) < 17 {
			continue
		}
		ifIndex := parts[0]
		ip := make(net.IP, net.IPv6len)
		for i := 0; i < net.IPv6len; i++ {
			if parts[i+1] > 255 {
				ip = nil
				break
			}
			ip[i] = byte(parts[i+1])
		}
		if ip == nil || ifIndex == 0 {
			continue
		}
		prefixLength := parseUintValue(stringValue(prefixValue))
		if prefixLength > 128 {
			prefixLength = 128
		}
		addresses = append(addresses, newSNMPInterfaceAddress(req, provider.name, ifIndex, "ipv6", ip, uint8(prefixLength), ""))
	}
	return addresses
}

func newSNMPInterfaceAddress(req DiscoveryContext, source string, ifIndex uint64, family string, ip net.IP, prefixLength uint8, origin string) InterfaceAddress {
	address := ip.String()
	return InterfaceAddress{
		ID:           collectorStableID("interface-address", "", string(req.Device.ID), strconv.FormatUint(ifIndex, 10), family, address, strconv.Itoa(int(prefixLength))),
		DeviceID:     req.Device.ID,
		PortID:       collectorStableID("port", "", string(req.Device.ID), strconv.FormatUint(ifIndex, 10)),
		IfIndex:      ifIndex,
		Address:      address,
		Family:       family,
		PrefixLength: prefixLength,
		Origin:       origin,
		ContextName:  "",
	}
}

func resolveSNMPCapabilityOID(ref string) (string, bool) {
	if strings.TrimSpace(ref) == "" {
		return "", false
	}
	oid, err := DefaultSNMPMIBRegistry().OID(ref)
	return oid, err == nil && oid != ""
}

func walkSNMPColumn(ctx context.Context, req DiscoveryContext, oid string) map[string]any {
	if oid == "" {
		return nil
	}
	response, err := req.Query.Walk(ctx, WalkRequest{
		Target: req.Target, Profile: req.Profile, BaseOID: oid,
		Flags: QueryFlags{UseBulk: true, MaxRepetitions: 25},
	})
	if err != nil {
		return nil
	}
	values := make(map[string]any, len(response.VarBinds))
	prefix := strings.TrimPrefix(oid, ".") + "."
	for _, vb := range response.VarBinds {
		name := strings.TrimPrefix(vb.OID, ".")
		index := strings.TrimPrefix(name, prefix)
		if index != name && index != "" {
			values[index] = vb.Value
		}
	}
	return values
}

func parseIPAddressTableIndex(index string) (string, net.IP, bool) {
	parts, ok := parseNumericOIDParts(index)
	if !ok || len(parts) < 2 {
		return "", nil, false
	}
	addressType, addressLength := parts[0], parts[1]
	if uint64(len(parts)-2) < addressLength {
		return "", nil, false
	}
	bytes := make([]byte, addressLength)
	for i := uint64(0); i < addressLength; i++ {
		if parts[i+2] > 255 {
			return "", nil, false
		}
		bytes[i] = byte(parts[i+2])
	}
	switch addressType {
	case 1: // ipv4
		if len(bytes) == net.IPv4len {
			return "ipv4", net.IP(bytes), true
		}
	case 2: // ipv6
		if len(bytes) == net.IPv6len {
			return "ipv6", net.IP(bytes), true
		}
	case 3: // ipv4z: address followed by four-byte zone index
		if len(bytes) >= net.IPv4len {
			return "ipv4", net.IP(bytes[:net.IPv4len]), true
		}
	case 4: // ipv6z
		if len(bytes) >= net.IPv6len {
			return "ipv6", net.IP(bytes[:net.IPv6len]), true
		}
	}
	return "", nil, false
}

func parseNumericOIDParts(value string) ([]uint64, bool) {
	text := strings.Trim(value, ".")
	if text == "" {
		return nil, false
	}
	items := strings.Split(text, ".")
	parts := make([]uint64, len(items))
	for i, item := range items {
		value, err := strconv.ParseUint(item, 10, 64)
		if err != nil {
			return nil, false
		}
		parts[i] = value
	}
	return parts, true
}

func prefixLengthFromRowPointer(value any, family string) uint8 {
	maximum := uint64(128)
	if family == "ipv4" {
		maximum = 32
	}
	text := strings.Trim(stringValue(value), ". ")
	if text != "" && text != "0.0" {
		parts := strings.Split(text, ".")
		if prefix, err := strconv.ParseUint(parts[len(parts)-1], 10, 8); err == nil && prefix <= maximum {
			return uint8(prefix)
		}
	}
	return uint8(maximum)
}

func ipAddressOrigin(value any) string {
	switch strings.TrimSpace(stringValue(value)) {
	case "1", "other":
		return "other"
	case "2", "manual":
		return "manual"
	case "4", "dhcp":
		return "dhcp"
	case "5", "linklayer":
		return "linklayer"
	case "6", "random":
		return "random"
	default:
		return ""
	}
}

func deduplicateSNMPInterfaceAddresses(addresses []InterfaceAddress) []InterfaceAddress {
	seen := make(map[string]bool, len(addresses))
	result := make([]InterfaceAddress, 0, len(addresses))
	for _, address := range addresses {
		if seen[address.ID] {
			continue
		}
		seen[address.ID] = true
		result = append(result, address)
	}
	return result
}
