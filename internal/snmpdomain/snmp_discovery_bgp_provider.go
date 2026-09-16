package snmpdomain

import (
	"context"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
)

// bgpMIBProvider describes table semantics using MIB symbols. Providers are
// probed against the agent and the most complete successful result wins. No
// OS, vendor or model string participates in provider selection.
type bgpMIBProvider struct {
	Name               string
	State              string
	LocalAS            string
	RemoteAS           string
	RemoteAddressType  string
	RemoteAddress      string
	PeerIndex          string
	EstablishedSeconds string
	InUpdates          string
	OutUpdates         string
	InMessages         string
	OutMessages        string
	PrefixAFI          string
	PrefixSAFI         string
	AcceptedPrefixes   string
	DeniedPrefixes     string
	AdvertisedPrefixes string
}

var defaultBGPMIBProviders = []bgpMIBProvider{
	{
		Name:               "BGP4-V2-MIB-JUNIPER",
		State:              "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerState",
		LocalAS:            "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerLocalAs",
		RemoteAS:           "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerRemoteAs",
		RemoteAddressType:  "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerRemoteAddrType",
		RemoteAddress:      "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerRemoteAddr",
		PeerIndex:          "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerIndex",
		EstablishedSeconds: "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerFsmEstablishedTime",
		InUpdates:          "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerInUpdates",
		OutUpdates:         "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerOutUpdates",
		InMessages:         "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerInTotalMessages",
		OutMessages:        "BGP4-V2-MIB-JUNIPER::jnxBgpM2PeerOutTotalMessages",
		PrefixAFI:          "BGP4-V2-MIB-JUNIPER::jnxBgpM2PrefixCountersAfi",
		PrefixSAFI:         "BGP4-V2-MIB-JUNIPER::jnxBgpM2PrefixCountersSafi",
		AcceptedPrefixes:   "BGP4-V2-MIB-JUNIPER::jnxBgpM2PrefixInPrefixesAccepted",
		DeniedPrefixes:     "BGP4-V2-MIB-JUNIPER::jnxBgpM2PrefixInPrefixesRejected",
		AdvertisedPrefixes: "BGP4-V2-MIB-JUNIPER::jnxBgpM2PrefixOutPrefixes",
	},
	{
		Name:               "BGP4V2-MIB",
		State:              "BGP4V2-MIB::bgp4V2PeerState",
		LocalAS:            "BGP4V2-MIB::bgp4V2PeerLocalAs",
		RemoteAS:           "BGP4V2-MIB::bgp4V2PeerRemoteAs",
		RemoteAddressType:  "BGP4V2-MIB::bgp4V2PeerRemoteAddrType",
		RemoteAddress:      "BGP4V2-MIB::bgp4V2PeerRemoteAddr",
		EstablishedSeconds: "BGP4V2-MIB::bgp4V2PeerFsmEstablishedTime",
	},
}

type resolvedBGPMIBProvider struct {
	bgpMIBProvider
	oids map[string]string
}

type bgpProviderPeer struct {
	index      string
	peerIndex  string
	address    string
	addressAFI string
	peerAS     uint64
	localAS    uint64
	state      string
	uptime     time.Duration
}

type bgpProviderFamily struct {
	AFI        string
	SAFI       string
	Accepted   uint64
	Denied     uint64
	Advertised uint64
}

func (m SNMPBGPDiscoveryModule) discoverMIBProviders(ctx context.Context, req DiscoveryContext, contextName string) DiscoveryResult {
	providers := bgpMIBProvidersFromDefinition(req.Definition.Definition)
	if len(providers) == 0 {
		providers = defaultBGPMIBProviders
	}
	var best DiscoveryResult
	bestScore := 0
	for _, provider := range providers {
		resolved, ok := resolveBGPMIBProvider(provider)
		if !ok {
			continue
		}
		result := m.discoverMIBProvider(ctx, req, contextName, resolved)
		score := bgpProviderResultScore(result)
		if score > bestScore {
			best, bestScore = result, score
		}
	}
	return best
}

func resolveBGPMIBProvider(provider bgpMIBProvider) (resolvedBGPMIBProvider, bool) {
	stateOID, ok := resolveSNMPCapabilityOID(provider.State)
	if !ok {
		return resolvedBGPMIBProvider{}, false
	}
	fields := map[string]string{
		"local_as": provider.LocalAS, "remote_as": provider.RemoteAS,
		"remote_address_type": provider.RemoteAddressType, "remote_address": provider.RemoteAddress,
		"peer_index": provider.PeerIndex, "established_seconds": provider.EstablishedSeconds,
		"in_updates": provider.InUpdates, "out_updates": provider.OutUpdates,
		"in_messages": provider.InMessages, "out_messages": provider.OutMessages,
		"prefix_afi": provider.PrefixAFI, "prefix_safi": provider.PrefixSAFI,
		"accepted_prefixes": provider.AcceptedPrefixes, "denied_prefixes": provider.DeniedPrefixes,
		"advertised_prefixes": provider.AdvertisedPrefixes,
	}
	resolved := resolvedBGPMIBProvider{bgpMIBProvider: provider, oids: map[string]string{"state": stateOID}}
	for name, ref := range fields {
		if strings.TrimSpace(ref) == "" {
			continue
		}
		oid, ok := resolveSNMPCapabilityOID(ref)
		if !ok {
			if name == "state" || name == "remote_as" || name == "remote_address" {
				return resolved, false
			}
			continue
		}
		resolved.oids[name] = oid
	}
	return resolved, true
}

func (m SNMPBGPDiscoveryModule) discoverMIBProvider(ctx context.Context, req DiscoveryContext, contextName string, provider resolvedBGPMIBProvider) DiscoveryResult {
	columns := make(map[string]map[string]any, len(provider.oids))
	for name, oid := range provider.oids {
		columns[name] = walkSNMPColumnWithContext(ctx, req, contextName, oid)
	}
	if len(columns["state"]) == 0 || len(columns["remote_as"]) == 0 || len(columns["remote_address"]) == 0 {
		return DiscoveryResult{}
	}
	peers := make([]bgpProviderPeer, 0, len(columns["state"]))
	for index, stateValue := range columns["state"] {
		peerAS := parseUintValue(stringValue(columns["remote_as"][index]))
		addressType := parseUintValue(stringValue(columns["remote_address_type"][index]))
		address, family := parseSNMPInetAddress(columns["remote_address"][index], addressType)
		if address == "" || peerAS == 0 {
			continue
		}
		peerIndex := stringValue(columns["peer_index"][index])
		if peerIndex == "" {
			peerIndex = index
		}
		peers = append(peers, bgpProviderPeer{
			index: index, peerIndex: peerIndex, address: address, addressAFI: family,
			peerAS:  peerAS,
			localAS: parseUintValue(stringValue(columns["local_as"][index])),
			state:   bgpPeerState(stringValue(stateValue)),
			uptime:  time.Duration(parseUintValue(stringValue(columns["established_seconds"][index]))) * time.Second,
		})
	}
	families := bgpProviderFamilies(columns)
	var sessions []BGPSession
	var recipes []Recipe
	for _, peer := range peers {
		peerFamilies := families[peer.peerIndex]
		if len(peerFamilies) == 0 {
			peerFamilies = []bgpProviderFamily{{AFI: peer.addressAFI, SAFI: "unicast"}}
		}
		sort.Slice(peerFamilies, func(i, j int) bool {
			return peerFamilies[i].AFI+peerFamilies[i].SAFI < peerFamilies[j].AFI+peerFamilies[j].SAFI
		})
		for _, family := range peerFamilies {
			session := BGPSession{
				ID:       collectorStableID("bgp", "", string(req.Device.ID), peer.address, strconv.FormatUint(peer.peerAS, 10), family.AFI, family.SAFI),
				DeviceID: req.Device.ID, PeerAddr: peer.address,
				PeerAS: peer.peerAS, LocalAS: peer.localAS, AFI: family.AFI, SAFI: family.SAFI,
				State: peer.state, Uptime: peer.uptime, AcceptedPrefixes: family.Accepted,
				DeniedPrefixes: family.Denied, AdvertisedPrefixes: family.Advertised,
				Metadata: map[string]string{"source": provider.Name, "peer_index": peer.peerIndex},
			}
			sessions = append(sessions, session)
			if len(peerFamilies) > 0 && family == peerFamilies[0] {
				recipes = append(recipes, bgpProviderRecipes(req, session, peer.index, contextName, provider)...)
			}
		}
	}
	return DiscoveryResult{BGPSessions: sessions, Recipes: recipes}
}

func bgpProviderFamilies(columns map[string]map[string]any) map[string][]bgpProviderFamily {
	result := map[string][]bgpProviderFamily{}
	for index, afiValue := range columns["prefix_afi"] {
		parts := strings.Split(index, ".")
		if len(parts) < 3 {
			continue
		}
		peerIndex := parts[0]
		afi := bgpAFI(parseUintValue(stringValue(afiValue)))
		safi := bgpSAFI(parseUintValue(stringValue(columns["prefix_safi"][index])))
		if afi == "" || safi == "" {
			continue
		}
		result[peerIndex] = append(result[peerIndex], bgpProviderFamily{
			AFI: afi, SAFI: safi,
			Accepted:   parseUintValue(stringValue(columns["accepted_prefixes"][index])),
			Denied:     parseUintValue(stringValue(columns["denied_prefixes"][index])),
			Advertised: parseUintValue(stringValue(columns["advertised_prefixes"][index])),
		})
	}
	return result
}

func walkSNMPColumnWithContext(ctx context.Context, req DiscoveryContext, contextName, oid string) map[string]any {
	if oid == "" {
		return nil
	}
	response, err := req.Query.Walk(ctx, WalkRequest{
		Target: req.Target, Profile: req.Profile, Context: contextName, BaseOID: oid,
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

func parseSNMPInetAddress(value any, addressType uint64) (string, string) {
	var ip net.IP
	switch typed := value.(type) {
	case []byte:
		bytes := typed
		if addressType == 3 && len(bytes) >= 4 {
			bytes = bytes[:4]
		} else if addressType == 4 && len(bytes) >= 16 {
			bytes = bytes[:16]
		}
		ip = net.IP(bytes)
	default:
		ip = net.ParseIP(strings.TrimSpace(stringValue(value)))
	}
	if ip == nil {
		return "", ""
	}
	if addressType == 1 || addressType == 3 || ip.To4() != nil {
		if ipv4 := ip.To4(); ipv4 != nil {
			return ipv4.String(), "ipv4"
		}
	}
	if ipv6 := ip.To16(); ipv6 != nil {
		return ipv6.String(), "ipv6"
	}
	return "", ""
}

func bgpAFI(value uint64) string {
	switch value {
	case 1:
		return "ipv4"
	case 2:
		return "ipv6"
	case 25:
		return "l2vpn"
	default:
		return ""
	}
}

func bgpSAFI(value uint64) string {
	switch value {
	case 1:
		return "unicast"
	case 2:
		return "multicast"
	case 3:
		return "unicast_multicast"
	case 4:
		return "labeled_unicast"
	case 5:
		return "mvpn"
	case 65:
		return "vpls"
	case 70:
		return "evpn"
	case 128:
		return "vpn"
	case 132:
		return "route_target"
	case 133:
		return "flowspec"
	default:
		return ""
	}
}

func bgpProviderRecipes(req DiscoveryContext, session BGPSession, index, contextName string, provider resolvedBGPMIBProvider) []Recipe {
	definitions := []struct {
		metric    string
		field     string
		valueType ValueType
		unit      string
	}{
		{MetricBGPState, "state", ValueState, "state"},
		{MetricBGPPeerInUpdatesTotal, "in_updates", ValueCounter32, "updates"},
		{MetricBGPPeerOutUpdatesTotal, "out_updates", ValueCounter32, "updates"},
		{MetricBGPPeerInMessagesTotal, "in_messages", ValueCounter32, "messages"},
		{MetricBGPPeerOutMessagesTotal, "out_messages", ValueCounter32, "messages"},
		{MetricBGPPeerEstablishedSeconds, "established_seconds", ValueGauge, "seconds"},
	}
	var recipes []Recipe
	for _, definition := range definitions {
		oid := provider.oids[definition.field]
		if oid == "" {
			continue
		}
		recipes = append(recipes, Recipe{
			ID:       collectorStableID("snmp-recipe", "", string(req.Device.ID), snmpCollectorModuleBGP, string(EntityBGPPeer), string(session.ID), definition.metric, index, contextName),
			DeviceID: req.Device.ID, EntityType: EntityBGPPeer,
			EntityID: session.ID, ModuleName: snmpCollectorModuleBGP, MetricName: definition.metric,
			ValueType: definition.valueType, OID: snmpMIBDisplayOID(oid + "." + index),
			NumericOID: oid + "." + index, OIDIndex: index, MIB: provider.Name,
			ContextName: contextName, PollerType: "snmp", Unit: definition.unit,
			SampleIntervalSeconds: 60, Enabled: true,
			Labels: map[string]string{
				"target_id": string(req.TargetID), "bgp_session_id": string(session.ID),
				"peer_addr": session.PeerAddr, "peer_as": strconv.FormatUint(session.PeerAS, 10),
				"afi": session.AFI, "safi": session.SAFI, "module": snmpCollectorModuleBGP,
				"snmp_context": contextName, "mib_provider": provider.Name,
			},
		})
	}
	return recipes
}

func bgpProviderResultScore(result DiscoveryResult) int {
	families := map[string]bool{}
	for _, session := range result.BGPSessions {
		families[session.AFI+"/"+session.SAFI] = true
	}
	return len(families)*100000 + len(result.BGPSessions)
}

func bgpMIBProvidersFromDefinition(definition map[string]any) []bgpMIBProvider {
	raw, ok := definition["providers"].([]any)
	if !ok {
		return nil
	}
	providers := make([]bgpMIBProvider, 0, len(raw))
	for _, item := range raw {
		values, ok := item.(map[string]any)
		if !ok {
			continue
		}
		get := func(name string) string { return snmpCollectorDefinitionString(values, name) }
		provider := bgpMIBProvider{
			Name: get("name"), State: get("state"), LocalAS: get("local_as"), RemoteAS: get("remote_as"),
			RemoteAddressType: get("remote_address_type"), RemoteAddress: get("remote_address"), PeerIndex: get("peer_index"),
			EstablishedSeconds: get("established_seconds"), InUpdates: get("in_updates"), OutUpdates: get("out_updates"),
			InMessages: get("in_messages"), OutMessages: get("out_messages"), PrefixAFI: get("prefix_afi"),
			PrefixSAFI: get("prefix_safi"), AcceptedPrefixes: get("accepted_prefixes"), DeniedPrefixes: get("denied_prefixes"),
			AdvertisedPrefixes: get("advertised_prefixes"),
		}
		if provider.Name != "" && provider.State != "" && provider.RemoteAS != "" && provider.RemoteAddress != "" {
			providers = append(providers, provider)
		}
	}
	return providers
}
