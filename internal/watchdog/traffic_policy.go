package watchdog

import (
	"time"

	"github.com/cloudcache/watchdog/internal/snmpdomain"
)

// Deprecated: SNMP traffic-policy ownership moved to internal/snmpdomain.
// These aliases and conversions keep the legacy package buildable until the
// remaining internal/watchdog consumers are removed by KISS-08.
type PortSideType = snmpdomain.PortSideType

const (
	PortSideProvider = snmpdomain.PortSideProvider
	PortSideCustomer = snmpdomain.PortSideCustomer
)

type CorrectionDirection = snmpdomain.CorrectionDirection

const (
	CorrectionNone = snmpdomain.CorrectionNone
	CorrectionUp   = snmpdomain.CorrectionUp
	CorrectionDown = snmpdomain.CorrectionDown
)

const (
	ProviderBillingBaseBps = snmpdomain.ProviderBillingBaseBps
	CustomerBillingBaseBps = snmpdomain.CustomerBillingBaseBps
)

type TrafficPolicyDefaults struct {
	Provider TrafficPolicyDefault
	Customer TrafficPolicyDefault
}

type TrafficPolicyDefault struct {
	ID                  ID
	TenantID            ID
	SideType            PortSideType
	BillingBaseBps      uint64
	SampleStep          time.Duration
	CorrectionDirection CorrectionDirection
	CorrectionMin       int64
	CorrectionMax       int64
}

type PortPolicy struct {
	ID                  ID
	TenantID            ID
	PortID              ID
	SideType            PortSideType
	BillingBaseBps      uint64
	SampleStep          time.Duration
	CorrectionDirection CorrectionDirection
	CorrectionMin       int64
	CorrectionMax       int64
	Enabled             bool
}

var BuiltinTrafficPolicyDefaults = fromSNMPPolicyDefaults(snmpdomain.BuiltinTrafficPolicyDefaults)

func DefaultPortPolicy(tenantID, portID ID, side PortSideType) PortPolicy {
	return fromSNMPPortPolicy(tenantID, snmpdomain.DefaultPortPolicy(string(portID), side))
}

func DefaultPortPolicyWithDefaults(tenantID, portID ID, side PortSideType, defaults TrafficPolicyDefaults) PortPolicy {
	return fromSNMPPortPolicy(tenantID, snmpdomain.DefaultPortPolicyWithDefaults(string(portID), side, toSNMPPolicyDefaults(defaults)))
}

func portSideFromMetadata(port NetworkPort) PortSideType {
	if port.Metadata != nil && port.Metadata["side_type"] == string(PortSideProvider) {
		return PortSideProvider
	}
	return PortSideCustomer
}

func (d TrafficPolicyDefault) Normalize(side PortSideType) TrafficPolicyDefault {
	tenantID := d.TenantID
	normalized := toSNMPPolicyDefault(d).Normalize(side)
	return fromSNMPPolicyDefault(tenantID, normalized)
}

func (p PortPolicy) Normalize() PortPolicy {
	tenantID := p.TenantID
	normalized := toSNMPPortPolicy(p).Normalize()
	return fromSNMPPortPolicy(tenantID, normalized)
}

type CorrectionRandom = snmpdomain.CorrectionRandom

func DeterministicCorrectionRNG(portID string, t time.Time) CorrectionRandom {
	return snmpdomain.DeterministicCorrectionRNG(portID, t)
}

func ApplyCorrection(value int64, policy PortPolicy) int64 {
	return snmpdomain.ApplyCorrection(value, toSNMPPortPolicy(policy))
}

func ApplyCorrectionWithRand(value int64, policy PortPolicy, rng CorrectionRandom) int64 {
	return snmpdomain.ApplyCorrectionWithRand(value, toSNMPPortPolicy(policy), rng)
}

func ApplyCorrectionFloat(value float64, policy PortPolicy, rng CorrectionRandom) float64 {
	return snmpdomain.ApplyCorrectionFloat(value, toSNMPPortPolicy(policy), rng)
}

func toSNMPPolicyDefaults(value TrafficPolicyDefaults) snmpdomain.TrafficPolicyDefaults {
	return snmpdomain.TrafficPolicyDefaults{
		Provider: toSNMPPolicyDefault(value.Provider),
		Customer: toSNMPPolicyDefault(value.Customer),
	}
}

func fromSNMPPolicyDefaults(value snmpdomain.TrafficPolicyDefaults) TrafficPolicyDefaults {
	return TrafficPolicyDefaults{
		Provider: fromSNMPPolicyDefault("", value.Provider),
		Customer: fromSNMPPolicyDefault("", value.Customer),
	}
}

func toSNMPPolicyDefault(value TrafficPolicyDefault) snmpdomain.TrafficPolicyDefault {
	return snmpdomain.TrafficPolicyDefault{
		ID: string(value.ID), SideType: value.SideType, BillingBaseBps: value.BillingBaseBps,
		SampleStep: value.SampleStep, CorrectionDirection: value.CorrectionDirection,
		CorrectionMin: value.CorrectionMin, CorrectionMax: value.CorrectionMax,
	}
}

func fromSNMPPolicyDefault(tenantID ID, value snmpdomain.TrafficPolicyDefault) TrafficPolicyDefault {
	return TrafficPolicyDefault{
		ID: ID(value.ID), TenantID: tenantID, SideType: value.SideType, BillingBaseBps: value.BillingBaseBps,
		SampleStep: value.SampleStep, CorrectionDirection: value.CorrectionDirection,
		CorrectionMin: value.CorrectionMin, CorrectionMax: value.CorrectionMax,
	}
}

func toSNMPPortPolicy(value PortPolicy) snmpdomain.PortPolicy {
	return snmpdomain.PortPolicy{
		ID: string(value.ID), PortID: string(value.PortID), SideType: value.SideType,
		BillingBaseBps: value.BillingBaseBps, SampleStep: value.SampleStep,
		CorrectionDirection: value.CorrectionDirection, CorrectionMin: value.CorrectionMin,
		CorrectionMax: value.CorrectionMax, Enabled: value.Enabled,
	}
}

func fromSNMPPortPolicy(tenantID ID, value snmpdomain.PortPolicy) PortPolicy {
	return PortPolicy{
		ID: ID(value.ID), TenantID: tenantID, PortID: ID(value.PortID), SideType: value.SideType,
		BillingBaseBps: value.BillingBaseBps, SampleStep: value.SampleStep,
		CorrectionDirection: value.CorrectionDirection, CorrectionMin: value.CorrectionMin,
		CorrectionMax: value.CorrectionMax, Enabled: value.Enabled,
	}
}
