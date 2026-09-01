package watchdog

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
)

var (
	SNMPTrapOIDLinkDown              = snmpMIBOID("IF-MIB::linkDown")
	SNMPTrapOIDLinkUp                = snmpMIBOID("IF-MIB::linkUp")
	SNMPTrapOIDAuthenticationFailure = snmpMIBOID("SNMPv2-MIB::authenticationFailure")
	SNMPTrapOIDColdStart             = snmpMIBOID("SNMPv2-MIB::coldStart")
	SNMPTrapOIDWarmStart             = snmpMIBOID("SNMPv2-MIB::warmStart")
	SNMPTrapOIDBGPBackwardTransition = snmpMIBOID("BGP4-MIB::bgpBackwardTransition")
)

type SNMPLinkTrapHandler struct {
	OperStatus string
	Ports      map[uint64]NetworkPort
	RecipeIDs  map[ID][]ID
}

func (h SNMPLinkTrapHandler) Handle(_ context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error) {
	ifIndex := trapIfIndex(trap)
	if ifIndex == 0 {
		return SNMPTrapHandleResult{
			Events:           []SNMPEvent{newSNMPTrapEvent(device, SNMPCollectorEntityPort, "", "trap", "warning", "unknown_interface_trap", "Interface trap did not include ifIndex", trap)},
			RediscoverDevice: true,
		}, nil
	}
	port, ok := h.Ports[ifIndex]
	if !ok {
		return SNMPTrapHandleResult{
			Events:           []SNMPEvent{newSNMPTrapEvent(device, SNMPCollectorEntityPort, "", "trap", "warning", "unknown_interface_trap", fmt.Sprintf("Trap referenced unknown ifIndex %d", ifIndex), trap)},
			RediscoverDevice: true,
		}, nil
	}
	port.OperStatus = h.OperStatus
	return SNMPTrapHandleResult{
		Events: []SNMPEvent{newSNMPTrapEvent(device, SNMPCollectorEntityPort, port.ID, "trap", "info", "interface_"+h.OperStatus, fmt.Sprintf("Interface %s is %s", port.IfName, h.OperStatus), trap)},
		PortUpdates: []NetworkPort{
			port,
		},
		ImmediatePollRecipe: h.RecipeIDs[port.ID],
	}, nil
}

type SNMPSimpleTrapHandler struct {
	EventType string
	Severity  string
	Message   string
}

func (h SNMPSimpleTrapHandler) Handle(_ context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error) {
	return SNMPTrapHandleResult{
		Events: []SNMPEvent{newSNMPTrapEvent(device, SNMPCollectorEntityDevice, device.ID, "trap", h.Severity, h.EventType, h.Message, trap)},
	}, nil
}

type SNMPBGPBackwardTransitionHandler struct {
	Sessions  map[string]BGPSession
	RecipeIDs map[ID][]ID
}

func (h SNMPBGPBackwardTransitionHandler) Handle(_ context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error) {
	peer := trapBGPPeer(trap)
	if peer == "" {
		return SNMPTrapHandleResult{
			Events:           []SNMPEvent{newSNMPTrapEvent(device, SNMPCollectorEntityBGPPeer, "", "trap", "warning", "unknown_bgp_peer_trap", "BGP trap did not include peer address", trap)},
			RediscoverDevice: true,
		}, nil
	}
	session, ok := h.Sessions[peer]
	if !ok {
		return SNMPTrapHandleResult{
			Events:           []SNMPEvent{newSNMPTrapEvent(device, SNMPCollectorEntityBGPPeer, "", "trap", "warning", "unknown_bgp_peer_trap", fmt.Sprintf("BGP trap referenced unknown peer %s", peer), trap)},
			RediscoverDevice: true,
		}, nil
	}
	session.State = "idle"
	return SNMPTrapHandleResult{
		Events:              []SNMPEvent{newSNMPTrapEvent(device, SNMPCollectorEntityBGPPeer, session.ID, "trap", "warning", "bgp_backward_transition", fmt.Sprintf("BGP peer %s moved backward", peer), trap)},
		BGPUpdates:          []BGPSession{session},
		ImmediatePollRecipe: h.RecipeIDs[session.ID],
	}, nil
}

func DefaultSNMPTrapHandlers(ports map[uint64]NetworkPort, portRecipeIDs map[ID][]ID, bgpSessions map[string]BGPSession, bgpRecipeIDs map[ID][]ID) map[string]SNMPTrapHandler {
	return map[string]SNMPTrapHandler{
		SNMPTrapOIDLinkDown: SNMPLinkTrapHandler{OperStatus: "down", Ports: ports, RecipeIDs: portRecipeIDs},
		SNMPTrapOIDLinkUp:   SNMPLinkTrapHandler{OperStatus: "up", Ports: ports, RecipeIDs: portRecipeIDs},
		SNMPTrapOIDAuthenticationFailure: SNMPSimpleTrapHandler{
			EventType: "authentication_failure",
			Severity:  "warning",
			Message:   "SNMP authentication failure",
		},
		SNMPTrapOIDColdStart: SNMPSimpleTrapHandler{
			EventType: "cold_start",
			Severity:  "info",
			Message:   "Device cold start",
		},
		SNMPTrapOIDWarmStart: SNMPSimpleTrapHandler{
			EventType: "warm_start",
			Severity:  "info",
			Message:   "Device warm start",
		},
		SNMPTrapOIDBGPBackwardTransition: SNMPBGPBackwardTransitionHandler{Sessions: bgpSessions, RecipeIDs: bgpRecipeIDs},
	}
}

var snmpOIDIfIndex = snmpMIBOID("IF-MIB::ifIndex")

func trapIfIndex(trap SNMPTrap) uint64 {
	for _, vb := range trap.VarBinds {
		oid := strings.TrimPrefix(vb.OID, ".")
		if strings.Contains(strings.ToLower(vb.OID), "ifindex") || strings.HasPrefix(oid, snmpOIDIfIndex+".") || oid == snmpOIDIfIndex {
			value, err := strconv.ParseUint(strings.TrimSpace(vb.Value), 10, 64)
			if err == nil {
				return value
			}
		}
	}
	return 0
}

func trapBGPPeer(trap SNMPTrap) string {
	for _, vb := range trap.VarBinds {
		parsed := net.ParseIP(strings.TrimSpace(vb.Value))
		if parsed != nil {
			return parsed.String()
		}
	}
	return ""
}
