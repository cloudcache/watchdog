package snmpdomain

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand/v2"
	"time"
)

// ID is a storage/API identifier. It is an alias, not a tenant-scoped domain
// type; the single-domain SNMP package treats identifiers as opaque strings.
type ID = string

type PortSideType string

const (
	PortSideProvider PortSideType = "provider"
	PortSideCustomer PortSideType = "customer"
)

type CorrectionDirection string

const (
	CorrectionNone CorrectionDirection = "none"
	CorrectionUp   CorrectionDirection = "up"
	CorrectionDown CorrectionDirection = "down"
)

const (
	ProviderBillingBaseBps uint64 = 1024 * 1024 * 1024
	CustomerBillingBaseBps uint64 = 1000 * 1000 * 1000
)

// BuiltinTrafficPolicyDefaults are only a fallback used before admin-managed
// defaults have been configured in MySQL.
var BuiltinTrafficPolicyDefaults = TrafficPolicyDefaults{
	Provider: TrafficPolicyDefault{
		SideType:       PortSideProvider,
		BillingBaseBps: ProviderBillingBaseBps,
		SampleStep:     5 * time.Minute,
	},
	Customer: TrafficPolicyDefault{
		SideType:       PortSideCustomer,
		BillingBaseBps: CustomerBillingBaseBps,
		SampleStep:     5 * time.Minute,
	},
}

type TrafficPolicyDefaults struct {
	Provider TrafficPolicyDefault
	Customer TrafficPolicyDefault
}

type TrafficPolicyDefault struct {
	ID                  string
	SideType            PortSideType
	BillingBaseBps      uint64
	SampleStep          time.Duration
	CorrectionDirection CorrectionDirection
	CorrectionMin       int64
	CorrectionMax       int64
}

type PortPolicy struct {
	ID                  string
	PortID              string
	SideType            PortSideType
	BillingBaseBps      uint64
	SampleStep          time.Duration
	CorrectionDirection CorrectionDirection
	CorrectionMin       int64
	CorrectionMax       int64
	Enabled             bool
}

func DefaultPortPolicy(portID string, side PortSideType) PortPolicy {
	return DefaultPortPolicyWithDefaults(portID, side, BuiltinTrafficPolicyDefaults)
}

func DefaultPortPolicyWithDefaults(portID string, side PortSideType, defaults TrafficPolicyDefaults) PortPolicy {
	config := defaults.Customer.Normalize(PortSideCustomer)
	if side == PortSideProvider {
		config = defaults.Provider.Normalize(PortSideProvider)
	}
	return PortPolicy{
		PortID:              portID,
		SideType:            side,
		BillingBaseBps:      config.BillingBaseBps,
		SampleStep:          config.SampleStep,
		CorrectionDirection: config.CorrectionDirection,
		CorrectionMin:       config.CorrectionMin,
		CorrectionMax:       config.CorrectionMax,
		Enabled:             true,
	}
}

func (d TrafficPolicyDefault) Normalize(side PortSideType) TrafficPolicyDefault {
	d.SideType = side
	if d.BillingBaseBps == 0 {
		if side == PortSideProvider {
			d.BillingBaseBps = ProviderBillingBaseBps
		} else {
			d.BillingBaseBps = CustomerBillingBaseBps
		}
	}
	if d.SampleStep != time.Minute && d.SampleStep != 5*time.Minute {
		d.SampleStep = 5 * time.Minute
	}
	if d.CorrectionDirection == "" {
		d.CorrectionDirection = CorrectionNone
	}
	if d.CorrectionMax < d.CorrectionMin {
		d.CorrectionMax = d.CorrectionMin
	}
	return d
}

func (p PortPolicy) Normalize() PortPolicy {
	if p.SideType == "" {
		p.SideType = PortSideCustomer
	}
	if p.BillingBaseBps == 0 {
		if p.SideType == PortSideProvider {
			p.BillingBaseBps = ProviderBillingBaseBps
		} else {
			p.BillingBaseBps = CustomerBillingBaseBps
		}
	}
	if p.SampleStep != time.Minute && p.SampleStep != 5*time.Minute {
		p.SampleStep = 5 * time.Minute
	}
	if p.CorrectionMax < p.CorrectionMin {
		p.CorrectionMax = p.CorrectionMin
	}
	if p.CorrectionDirection == "" {
		p.CorrectionDirection = CorrectionNone
	}
	return p
}

type CorrectionRandom interface {
	Int64N(n int64) int64
}

type randV2Source struct{}

func (randV2Source) Int64N(n int64) int64 {
	return rand.Int64N(n)
}

type deterministicCorrectionSource struct{ seed uint64 }

func (d deterministicCorrectionSource) Int64N(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return int64(d.seed % uint64(n))
}

// DeterministicCorrectionRNG makes each corrected sample reproducible across
// reads while keeping the raw ClickHouse sample as the source of truth.
func DeterministicCorrectionRNG(portID string, t time.Time) CorrectionRandom {
	h := fnv.New64a()
	_, _ = h.Write([]byte(portID))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(t.UnixNano()))
	_, _ = h.Write(buf[:])
	return deterministicCorrectionSource{seed: h.Sum64()}
}

func ApplyCorrection(value int64, policy PortPolicy) int64 {
	return ApplyCorrectionWithRand(value, policy, randV2Source{})
}

func ApplyCorrectionWithRand(value int64, policy PortPolicy, rng CorrectionRandom) int64 {
	return int64(ApplyCorrectionFloat(float64(value), policy, rng))
}

// ApplyCorrectionFloat applies a policy delta without truncating rate values.
func ApplyCorrectionFloat(value float64, policy PortPolicy, rng CorrectionRandom) float64 {
	policy = policy.Normalize()
	if !policy.Enabled || policy.CorrectionDirection == CorrectionNone || policy.CorrectionMax <= 0 {
		return value
	}
	delta := policy.CorrectionMin
	if policy.CorrectionMax > policy.CorrectionMin {
		delta += rng.Int64N(policy.CorrectionMax - policy.CorrectionMin + 1)
	}
	switch policy.CorrectionDirection {
	case CorrectionUp:
		return value + float64(delta)
	case CorrectionDown:
		if float64(delta) >= value {
			return 0
		}
		return value - float64(delta)
	default:
		return value
	}
}
