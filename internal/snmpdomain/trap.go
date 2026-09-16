package snmpdomain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	TrapOIDLinkDown              = snmpMIBOID("IF-MIB::linkDown")
	TrapOIDLinkUp                = snmpMIBOID("IF-MIB::linkUp")
	TrapOIDAuthenticationFailure = snmpMIBOID("SNMPv2-MIB::authenticationFailure")
	TrapOIDColdStart             = snmpMIBOID("SNMPv2-MIB::coldStart")
	TrapOIDWarmStart             = snmpMIBOID("SNMPv2-MIB::warmStart")
	TrapOIDBGPBackwardTransition = snmpMIBOID("BGP4-MIB::bgpBackwardTransition")
	trapOIDIfIndex               = snmpMIBOID("IF-MIB::ifIndex")
)

type Trap struct {
	SourceIP   string
	Hostname   string
	TrapOID    string
	Uptime     uint64
	VarBinds   []TrapVarBind
	ReceivedAt time.Time
	RawText    string
}

// TrapVarBind intentionally retains the historical exported-field JSON names
// when included in an event's Raw payload. Request decoding remains
// case-insensitive for the existing lowercase oid/value body.
type TrapVarBind struct {
	OID   string
	Value string
}

type TrapDevice struct {
	ID string
}

type TrapPort struct {
	ID         string
	IfIndex    uint64
	IfName     string
	OperStatus string
}

type TrapBGPSession struct {
	ID       string
	PeerAddr string
	State    string
}

type TrapHandleResult struct {
	Events              []Event
	PortUpdates         []TrapPort
	BGPUpdates          []TrapBGPSession
	ImmediatePollRecipe []string
	RediscoverDevice    bool
}

type TrapHandler interface {
	Handle(ctx context.Context, device TrapDevice, trap Trap) (TrapHandleResult, error)
}

type TrapDispatcher struct {
	Handlers map[string]TrapHandler
}

func NewTrapDispatcher(handlers map[string]TrapHandler) TrapDispatcher {
	return TrapDispatcher{Handlers: handlers}
}

func (d TrapDispatcher) Dispatch(ctx context.Context, device TrapDevice, trap Trap) (TrapHandleResult, error) {
	if trap.TrapOID == "" {
		return TrapHandleResult{}, errors.New("trap oid is required")
	}
	handler, ok := d.Handlers[trap.TrapOID]
	if !ok {
		return TrapHandleResult{Events: []Event{newTrapEvent(
			device, "", "", "trap", "info", "unhandled_trap", fmt.Sprintf("Unhandled SNMP trap %s", trap.TrapOID), trap,
		)}}, nil
	}
	return handler.Handle(ctx, device, trap)
}

type linkTrapHandler struct {
	operStatus string
	ports      map[uint64]TrapPort
	recipeIDs  map[string][]string
}

func (h linkTrapHandler) Handle(_ context.Context, device TrapDevice, trap Trap) (TrapHandleResult, error) {
	ifIndex := trapIfIndex(trap)
	if ifIndex == 0 {
		return TrapHandleResult{
			Events:           []Event{newTrapEvent(device, EntityPort, "", "trap", "warning", "unknown_interface_trap", "Interface trap did not include ifIndex", trap)},
			RediscoverDevice: true,
		}, nil
	}
	port, ok := h.ports[ifIndex]
	if !ok {
		return TrapHandleResult{
			Events:           []Event{newTrapEvent(device, EntityPort, "", "trap", "warning", "unknown_interface_trap", fmt.Sprintf("Trap referenced unknown ifIndex %d", ifIndex), trap)},
			RediscoverDevice: true,
		}, nil
	}
	port.OperStatus = h.operStatus
	return TrapHandleResult{
		Events:              []Event{newTrapEvent(device, EntityPort, port.ID, "trap", "info", "interface_"+h.operStatus, fmt.Sprintf("Interface %s is %s", port.IfName, h.operStatus), trap)},
		PortUpdates:         []TrapPort{port},
		ImmediatePollRecipe: h.recipeIDs[port.ID],
	}, nil
}

type simpleTrapHandler struct {
	eventType string
	severity  string
	message   string
}

func (h simpleTrapHandler) Handle(_ context.Context, device TrapDevice, trap Trap) (TrapHandleResult, error) {
	return TrapHandleResult{Events: []Event{newTrapEvent(device, EntityDevice, device.ID, "trap", h.severity, h.eventType, h.message, trap)}}, nil
}

type bgpBackwardTransitionHandler struct {
	sessions  map[string]TrapBGPSession
	recipeIDs map[string][]string
}

func (h bgpBackwardTransitionHandler) Handle(_ context.Context, device TrapDevice, trap Trap) (TrapHandleResult, error) {
	peer := trapBGPPeer(trap)
	if peer == "" {
		return TrapHandleResult{
			Events:           []Event{newTrapEvent(device, EntityBGPPeer, "", "trap", "warning", "unknown_bgp_peer_trap", "BGP trap did not include peer address", trap)},
			RediscoverDevice: true,
		}, nil
	}
	session, ok := h.sessions[peer]
	if !ok {
		return TrapHandleResult{
			Events:           []Event{newTrapEvent(device, EntityBGPPeer, "", "trap", "warning", "unknown_bgp_peer_trap", fmt.Sprintf("BGP trap referenced unknown peer %s", peer), trap)},
			RediscoverDevice: true,
		}, nil
	}
	session.State = "idle"
	return TrapHandleResult{
		Events:              []Event{newTrapEvent(device, EntityBGPPeer, session.ID, "trap", "warning", "bgp_backward_transition", fmt.Sprintf("BGP peer %s moved backward", peer), trap)},
		BGPUpdates:          []TrapBGPSession{session},
		ImmediatePollRecipe: h.recipeIDs[session.ID],
	}, nil
}

func DefaultTrapHandlers(ports map[uint64]TrapPort, portRecipeIDs map[string][]string, sessions map[string]TrapBGPSession, bgpRecipeIDs map[string][]string) map[string]TrapHandler {
	return map[string]TrapHandler{
		TrapOIDLinkDown: linkTrapHandler{operStatus: "down", ports: ports, recipeIDs: portRecipeIDs},
		TrapOIDLinkUp:   linkTrapHandler{operStatus: "up", ports: ports, recipeIDs: portRecipeIDs},
		TrapOIDAuthenticationFailure: simpleTrapHandler{
			eventType: "authentication_failure", severity: "warning", message: "SNMP authentication failure",
		},
		TrapOIDColdStart:             simpleTrapHandler{eventType: "cold_start", severity: "info", message: "Device cold start"},
		TrapOIDWarmStart:             simpleTrapHandler{eventType: "warm_start", severity: "info", message: "Device warm start"},
		TrapOIDBGPBackwardTransition: bgpBackwardTransitionHandler{sessions: sessions, recipeIDs: bgpRecipeIDs},
	}
}

func newTrapEvent(device TrapDevice, entityType EntityType, entityID, source, severity, eventType, message string, trap Trap) Event {
	occurredAt := trap.ReceivedAt
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	return Event{
		// Keep the historical empty tenant component in the digest so event IDs
		// generated by the single-domain Gin path remain stable across upgrade.
		ID:       trapEventID("snmp-event", "", device.ID, eventType, trap.TrapOID, trap.SourceIP, occurredAt.String(), message),
		DeviceID: device.ID, EntityType: entityType, EntityID: entityID,
		Source: source, Severity: severity, EventType: eventType, Message: message,
		Raw: map[string]any{
			"trap_oid": trap.TrapOID, "source_ip": trap.SourceIP, "hostname": trap.Hostname, "varbinds": trap.VarBinds,
		},
		OccurredAt: occurredAt,
	}
}

func trapEventID(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strings.ToLower(strings.TrimSpace(part))))
		_, _ = hash.Write([]byte{0})
	}
	return "c_" + hex.EncodeToString(hash.Sum(nil))[:24]
}

func trapIfIndex(trap Trap) uint64 {
	for _, variable := range trap.VarBinds {
		oid := strings.TrimPrefix(variable.OID, ".")
		if strings.Contains(strings.ToLower(variable.OID), "ifindex") || strings.HasPrefix(oid, trapOIDIfIndex+".") || oid == trapOIDIfIndex {
			value, err := strconv.ParseUint(strings.TrimSpace(variable.Value), 10, 64)
			if err == nil {
				return value
			}
		}
	}
	return 0
}

func trapBGPPeer(trap Trap) string {
	for _, variable := range trap.VarBinds {
		if parsed := net.ParseIP(strings.TrimSpace(variable.Value)); parsed != nil {
			return parsed.String()
		}
	}
	return ""
}

func NormalizeIfStatus(value string) string {
	switch value {
	case "1":
		return "up"
	case "2":
		return "down"
	case "3":
		return "testing"
	case "4":
		return "unknown"
	case "5":
		return "dormant"
	case "6":
		return "notPresent"
	case "7":
		return "lowerLayerDown"
	default:
		return value
	}
}
