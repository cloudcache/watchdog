package snmpdomain

import (
	"context"
	"strconv"
	"strings"
	"time"
)

const (
	snmpCollectorModuleBGP = "bgp"

	MetricBGPPeerInUpdatesTotal     = "watchdog_bgp_peer_in_updates_total"
	MetricBGPPeerOutUpdatesTotal    = "watchdog_bgp_peer_out_updates_total"
	MetricBGPPeerEstablishedSeconds = "watchdog_bgp_peer_established_seconds"
	MetricBGPPeerInMessagesTotal    = "watchdog_bgp_peer_in_messages_total"
	MetricBGPPeerOutMessagesTotal   = "watchdog_bgp_peer_out_messages_total"
)

var (
	snmpOIDBGPLocalAS                = snmpMIBOID("BGP4-MIB::bgpLocalAs.0")
	snmpOIDBGPPeerState              = snmpMIBOID("BGP4-MIB::bgpPeerState")
	snmpOIDBGPPeerRemoteAddr         = snmpMIBOID("BGP4-MIB::bgpPeerRemoteAddr")
	snmpOIDBGPPeerRemoteAS           = snmpMIBOID("BGP4-MIB::bgpPeerRemoteAs")
	snmpOIDBGPPeerInUpdates          = snmpMIBOID("BGP4-MIB::bgpPeerInUpdates")
	snmpOIDBGPPeerOutUpdates         = snmpMIBOID("BGP4-MIB::bgpPeerOutUpdates")
	snmpOIDBGPPeerInTotalMessages    = snmpMIBOID("BGP4-MIB::bgpPeerInTotalMessages")
	snmpOIDBGPPeerOutTotalMessages   = snmpMIBOID("BGP4-MIB::bgpPeerOutTotalMessages")
	snmpOIDBGPPeerEstablishedSeconds = snmpMIBOID("BGP4-MIB::bgpPeerFsmEstablishedTime")

	snmpOIDCbgpPeer2State          = snmpMIBOID("CISCO-BGP4-MIB::cbgpPeer2State")
	snmpOIDCbgpPeer2RemoteAddr     = snmpMIBOID("CISCO-BGP4-MIB::cbgpPeer2RemoteAddr")
	snmpOIDCbgpPeer2RemoteAS       = snmpMIBOID("CISCO-BGP4-MIB::cbgpPeer2RemoteAs")
	snmpOIDCbgpPeer2InUpdates      = snmpMIBOID("CISCO-BGP4-MIB::cbgpPeer2InUpdates")
	snmpOIDCbgpPeer2OutUpdates     = snmpMIBOID("CISCO-BGP4-MIB::cbgpPeer2OutUpdates")
	snmpOIDCbgpPeer2InMessages     = snmpMIBOID("CISCO-BGP4-MIB::cbgpPeer2InTotalMessages")
	snmpOIDCbgpPeer2OutMessages    = snmpMIBOID("CISCO-BGP4-MIB::cbgpPeer2OutTotalMessages")
	snmpOIDCbgpPeer2EstablishedSec = snmpMIBOID("CISCO-BGP4-MIB::cbgpPeer2FsmEstablishedTime")
)

type SNMPBGPDiscoveryModule struct{}

func (SNMPBGPDiscoveryModule) Name() string {
	return snmpCollectorModuleBGP
}

func (m SNMPBGPDiscoveryModule) Discover(ctx context.Context, req DiscoveryContext) (DiscoveryResult, error) {
	if req.Query == nil {
		return DiscoveryResult{}, errSNMPCollectorQueryRequired
	}
	contextName := snmpCollectorDefinitionString(req.Definition.Definition, "context_name")
	if result := m.discoverMIBProviders(ctx, req, contextName); len(result.BGPSessions) > 0 {
		return result, nil
	}
	result, err := m.discoverBGP4MIB(ctx, req, contextName)
	if err != nil {
		return DiscoveryResult{}, err
	}
	if len(result.BGPSessions) == 0 {
		ciscoResult, cerr := m.discoverCiscoBGP(ctx, req, contextName)
		if cerr == nil && len(ciscoResult.BGPSessions) > 0 {
			result = ciscoResult
		}
	}
	return result, nil
}

func (m SNMPBGPDiscoveryModule) discoverBGP4MIB(ctx context.Context, req DiscoveryContext, contextName string) (DiscoveryResult, error) {
	columns, err := m.walkBGPColumns(ctx, req, contextName)
	if err != nil {
		return DiscoveryResult{}, err
	}
	localAS := m.localAS(ctx, req, contextName)
	sessions := make([]BGPSession, 0, len(columns[snmpOIDBGPPeerState]))
	recipes := make([]Recipe, 0, len(columns[snmpOIDBGPPeerState])*5)
	for index, state := range columns[snmpOIDBGPPeerState] {
		peerAddr := firstNonEmptySNMPString(cleanSNMPValue(columns[snmpOIDBGPPeerRemoteAddr][index]), bgpPeerAddrFromIndex(index))
		peerAS := parseUintValue(columns[snmpOIDBGPPeerRemoteAS][index])
		if peerAddr == "" || peerAS == 0 {
			continue
		}
		session := BGPSession{
			ID:       collectorStableID("bgp", "", string(req.Device.ID), peerAddr, strconv.FormatUint(peerAS, 10), "ipv4", "unicast"),
			DeviceID: req.Device.ID,
			PeerAddr: peerAddr,
			PeerAS:   peerAS,
			LocalAS:  localAS,
			AFI:      "ipv4",
			SAFI:     "unicast",
			State:    bgpPeerState(state),
			Uptime:   time.Duration(parseUintValue(columns[snmpOIDBGPPeerEstablishedSeconds][index])) * time.Second,
			Metadata: map[string]string{"source": "BGP4-MIB"},
		}
		sessions = append(sessions, session)
		recipes = append(recipes, bgpPeerRecipes(req, session, index, contextName)...)
	}
	return DiscoveryResult{BGPSessions: sessions, Recipes: recipes}, nil
}

func (m SNMPBGPDiscoveryModule) discoverCiscoBGP(ctx context.Context, req DiscoveryContext, contextName string) (DiscoveryResult, error) {
	ciscoOIDs := map[string]string{
		"state":          snmpOIDCbgpPeer2State,
		"remoteAddr":     snmpOIDCbgpPeer2RemoteAddr,
		"remoteAS":       snmpOIDCbgpPeer2RemoteAS,
		"inUpdates":      snmpOIDCbgpPeer2InUpdates,
		"outUpdates":     snmpOIDCbgpPeer2OutUpdates,
		"inMessages":     snmpOIDCbgpPeer2InMessages,
		"outMessages":    snmpOIDCbgpPeer2OutMessages,
		"establishedSec": snmpOIDCbgpPeer2EstablishedSec,
	}
	columns := make(map[string]map[string]string, len(ciscoOIDs))
	for key, oid := range ciscoOIDs {
		resp, err := req.Query.Walk(ctx, WalkRequest{
			Target: req.Target, Profile: req.Profile, Context: contextName, BaseOID: oid,
			Flags: QueryFlags{UseBulk: true, MaxRepetitions: 25},
		})
		if err != nil {
			return DiscoveryResult{}, err
		}
		columns[key] = valuesBySNMPIndex(oid, resp)
	}
	localAS := m.localAS(ctx, req, contextName)
	sessions := make([]BGPSession, 0, len(columns["state"]))
	recipes := make([]Recipe, 0, len(columns["state"])*6)
	for index, state := range columns["state"] {
		peerAddr := cleanSNMPValue(columns["remoteAddr"][index])
		if peerAddr == "" {
			peerAddr = bgpPeerAddrFromIndex(index)
		}
		peerAS := parseUintValue(columns["remoteAS"][index])
		if peerAddr == "" || peerAS == 0 {
			continue
		}
		session := BGPSession{
			ID:       collectorStableID("bgp", "", string(req.Device.ID), peerAddr, strconv.FormatUint(peerAS, 10), "ipv4", "unicast"),
			DeviceID: req.Device.ID,
			PeerAddr: peerAddr,
			PeerAS:   peerAS,
			LocalAS:  localAS,
			AFI:      "ipv4",
			SAFI:     "unicast",
			State:    bgpPeerState(state),
			Uptime:   time.Duration(parseUintValue(columns["establishedSec"][index])) * time.Second,
			Metadata: map[string]string{"source": "CISCO-CGP-MIB"},
		}
		sessions = append(sessions, session)
		recipes = append(recipes, ciscoBGPPeerRecipes(req, session, index, contextName)...)
	}
	return DiscoveryResult{BGPSessions: sessions, Recipes: recipes}, nil
}

func (m SNMPBGPDiscoveryModule) walkBGPColumns(ctx context.Context, req DiscoveryContext, contextName string) (map[string]map[string]string, error) {
	oids := []string{
		snmpOIDBGPPeerState,
		snmpOIDBGPPeerRemoteAddr,
		snmpOIDBGPPeerRemoteAS,
		snmpOIDBGPPeerInUpdates,
		snmpOIDBGPPeerOutUpdates,
		snmpOIDBGPPeerInTotalMessages,
		snmpOIDBGPPeerOutTotalMessages,
		snmpOIDBGPPeerEstablishedSeconds,
	}
	columns := make(map[string]map[string]string, len(oids))
	for _, oid := range oids {
		response, err := req.Query.Walk(ctx, WalkRequest{
			Target:  req.Target,
			Profile: req.Profile,
			Context: contextName,
			BaseOID: oid,
			Flags: QueryFlags{
				UseBulk:        true,
				MaxRepetitions: 25,
			},
		})
		if err != nil {
			return nil, err
		}
		columns[oid] = valuesBySNMPIndex(oid, response)
	}
	return columns, nil
}

func (m SNMPBGPDiscoveryModule) localAS(ctx context.Context, req DiscoveryContext, contextName string) uint64 {
	response, err := req.Query.Get(ctx, GetRequest{
		Target:  req.Target,
		Profile: req.Profile,
		Context: contextName,
		OIDs:    []string{snmpOIDBGPLocalAS},
	})
	if err != nil || len(response.VarBinds) == 0 {
		return 0
	}
	return parseUintValue(stringValue(response.VarBinds[0].Value))
}

func valuesBySNMPIndex(baseOID string, response QueryResponse) map[string]string {
	values := make(map[string]string, len(response.VarBinds))
	prefix := strings.TrimPrefix(baseOID, ".") + "."
	for _, vb := range response.VarBinds {
		oid := strings.TrimPrefix(vb.OID, ".")
		suffix := strings.TrimPrefix(oid, prefix)
		if suffix == oid || suffix == "" {
			continue
		}
		values[suffix] = stringValue(vb.Value)
	}
	return values
}

func bgpPeerRecipes(req DiscoveryContext, session BGPSession, oidIndex string, contextName string) []Recipe {
	definitions := []struct {
		metric    string
		oid       string
		valueType ValueType
		unit      string
	}{
		{MetricBGPState, snmpOIDBGPPeerState, ValueState, "state"},
		{MetricBGPPeerInUpdatesTotal, snmpOIDBGPPeerInUpdates, ValueCounter32, "updates"},
		{MetricBGPPeerOutUpdatesTotal, snmpOIDBGPPeerOutUpdates, ValueCounter32, "updates"},
		{MetricBGPPeerInMessagesTotal, snmpOIDBGPPeerInTotalMessages, ValueCounter32, "messages"},
		{MetricBGPPeerOutMessagesTotal, snmpOIDBGPPeerOutTotalMessages, ValueCounter32, "messages"},
		{MetricBGPPeerEstablishedSeconds, snmpOIDBGPPeerEstablishedSeconds, ValueGauge, "seconds"},
	}
	recipes := make([]Recipe, 0, len(definitions))
	for _, definition := range definitions {
		recipeID := collectorStableID("snmp-recipe", "", string(req.Device.ID), snmpCollectorModuleBGP, string(EntityBGPPeer), string(session.ID), definition.metric, oidIndex, contextName)
		recipes = append(recipes, Recipe{
			ID:                    recipeID,
			DeviceID:              req.Device.ID,
			EntityType:            EntityBGPPeer,
			EntityID:              session.ID,
			ModuleName:            snmpCollectorModuleBGP,
			MetricName:            definition.metric,
			ValueType:             definition.valueType,
			OID:                   snmpMIBDisplayOID(definition.oid + "." + oidIndex),
			NumericOID:            definition.oid + "." + oidIndex,
			OIDIndex:              oidIndex,
			MIB:                   "BGP4-MIB",
			ContextName:           contextName,
			PollerType:            "snmp",
			Unit:                  definition.unit,
			SampleIntervalSeconds: 60,
			Labels: map[string]string{
				"target_id":       string(req.TargetID),
				"bgp_session_id":  string(session.ID),
				"peer_addr":       session.PeerAddr,
				"peer_as":         strconv.FormatUint(session.PeerAS, 10),
				"afi":             session.AFI,
				"safi":            session.SAFI,
				"module":          snmpCollectorModuleBGP,
				"snmp_context":    contextName,
				"bgp4_mib_column": definition.oid,
			},
			Enabled: true,
		})
	}
	return recipes
}

func ciscoBGPPeerRecipes(req DiscoveryContext, session BGPSession, oidIndex string, contextName string) []Recipe {
	definitions := []struct {
		metric string
		oid    string
	}{
		{MetricBGPState, snmpOIDCbgpPeer2State},
		{MetricBGPPeerInUpdatesTotal, snmpOIDCbgpPeer2InUpdates},
		{MetricBGPPeerOutUpdatesTotal, snmpOIDCbgpPeer2OutUpdates},
		{MetricBGPPeerInMessagesTotal, snmpOIDCbgpPeer2InMessages},
		{MetricBGPPeerOutMessagesTotal, snmpOIDCbgpPeer2OutMessages},
		{MetricBGPPeerEstablishedSeconds, snmpOIDCbgpPeer2EstablishedSec},
	}
	recipes := make([]Recipe, 0, len(definitions))
	for _, def := range definitions {
		recipeID := collectorStableID("snmp-recipe", "", string(req.Device.ID), snmpCollectorModuleBGP, string(EntityBGPPeer), string(session.ID), def.metric, oidIndex, contextName)
		recipes = append(recipes, Recipe{
			ID:                    recipeID,
			DeviceID:              req.Device.ID,
			EntityType:            EntityBGPPeer,
			EntityID:              session.ID,
			ModuleName:            snmpCollectorModuleBGP,
			MetricName:            def.metric,
			ValueType:             ValueCounter32,
			OID:                   snmpMIBDisplayOID(def.oid + "." + oidIndex),
			NumericOID:            def.oid + "." + oidIndex,
			OIDIndex:              oidIndex,
			MIB:                   "CISCO-BGP4-MIB",
			ContextName:           contextName,
			PollerType:            "snmp",
			SampleIntervalSeconds: 60,
			Labels: map[string]string{
				"target_id":      string(req.TargetID),
				"bgp_session_id": string(session.ID),
				"peer_addr":      session.PeerAddr,
				"peer_as":        strconv.FormatUint(session.PeerAS, 10),
				"afi":            session.AFI,
				"safi":           session.SAFI,
				"module":         snmpCollectorModuleBGP,
				"snmp_context":   contextName,
			},
			Enabled: true,
		})
	}
	return recipes
}

func snmpCollectorDefinitionString(definition map[string]any, key string) string {
	value, ok := definition[key]
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(text)
}
