package watchdog

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type SNMPTrapHandler interface {
	Handle(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error)
}

type SNMPTrapDispatcher struct {
	Handlers map[string]SNMPTrapHandler
}

func NewSNMPTrapDispatcher(handlers map[string]SNMPTrapHandler) SNMPTrapDispatcher {
	return SNMPTrapDispatcher{Handlers: handlers}
}

func (d SNMPTrapDispatcher) Dispatch(ctx context.Context, device NetworkDevice, trap SNMPTrap) (SNMPTrapHandleResult, error) {
	if trap.TrapOID == "" {
		return SNMPTrapHandleResult{}, errors.New("trap oid is required")
	}
	handler, ok := d.Handlers[trap.TrapOID]
	if !ok {
		return SNMPTrapHandleResult{
			Events: []SNMPEvent{newSNMPTrapEvent(device, "", "", "trap", "info", "unhandled_trap", fmt.Sprintf("Unhandled SNMP trap %s", trap.TrapOID), trap)},
		}, nil
	}
	return handler.Handle(ctx, device, trap)
}

func newSNMPTrapEvent(device NetworkDevice, entityType SNMPCollectorEntityType, entityID ID, source string, severity string, eventType string, message string, trap SNMPTrap) SNMPEvent {
	occurredAt := trap.ReceivedAt
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	return SNMPEvent{
		ID:         collectorStableID("snmp-event", string(device.TenantID), string(device.ID), eventType, trap.TrapOID, trap.SourceIP, occurredAt.String(), message),
		TenantID:   device.TenantID,
		DeviceID:   device.ID,
		EntityType: entityType,
		EntityID:   entityID,
		Source:     source,
		Severity:   severity,
		EventType:  eventType,
		Message:    message,
		Raw: map[string]any{
			"trap_oid":  trap.TrapOID,
			"source_ip": trap.SourceIP,
			"hostname":  trap.Hostname,
			"varbinds":  trap.VarBinds,
		},
		OccurredAt: occurredAt,
	}
}
