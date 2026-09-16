package snmpdomain

import (
	"context"
	"time"
)

type Version string

const (
	Version1  Version = "1"
	Version2c Version = "2c"
	Version3  Version = "3"
)

type Profile struct {
	ID        string
	Name      string
	Version   Version
	Security  map[string]string
	Timeout   time.Duration
	Retries   uint8
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Device struct {
	ID            string
	TargetID      string
	Vendor        string
	Model         string
	Platform      string
	OSName        string
	OSVersion     string
	SysObjectID   string
	SysName       string
	SysDescr      string
	SysLocation   string
	Uptime        time.Duration
	SNMPProfileID string
	SNMPPort      uint16
	SNMPSecurity  map[string]string
	UpdatedAt     time.Time
}

type Target struct {
	ID        string
	Name      string
	Host      string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func ApplyDeviceOverrides(profile Profile, device Device) Profile {
	merged := profile
	merged.Security = make(map[string]string, len(profile.Security)+len(device.SNMPSecurity))
	for key, value := range profile.Security {
		merged.Security[key] = value
	}
	for key, value := range device.SNMPSecurity {
		if value != "" {
			merged.Security[key] = value
		}
	}
	return merged
}

type ValueType string

const (
	ValueCounter32 ValueType = "counter32"
	ValueCounter64 ValueType = "counter64"
	ValueGauge     ValueType = "gauge"
	ValueState     ValueType = "state"
	ValueString    ValueType = "string"
	ValueTimeTicks ValueType = "timeticks"
	ValueIPAddr    ValueType = "ipaddr"
	ValueMACAddr   ValueType = "macaddr"
)

type QueryFlags struct {
	UseBulk        bool
	MaxOids        int
	MaxRepetitions uint32
	NumericIndex   bool
	EnumStrings    bool
	AbortOnFailure bool
}

type QueryTarget struct {
	Host string
	Port uint16
}

type GetRequest struct {
	Target  QueryTarget
	Profile Profile
	Context string
	OIDs    []string
	Flags   QueryFlags
}

type WalkRequest struct {
	Target  QueryTarget
	Profile Profile
	Context string
	BaseOID string
	Flags   QueryFlags
}

type VarBind struct {
	OID       string
	Value     any
	ValueType ValueType
}

type QueryResponse struct {
	VarBinds []VarBind
}

type QueryEngine interface {
	Get(context.Context, GetRequest) (QueryResponse, error)
	Walk(context.Context, WalkRequest) (QueryResponse, error)
}

type Recipe struct {
	ID                    string
	DeviceID              string
	EntityType            EntityType
	EntityID              string
	ModuleName            string
	MetricName            string
	ValueType             ValueType
	OID                   string
	NumericOID            string
	OIDIndex              string
	MIB                   string
	ContextName           string
	Divisor               float64
	HasDivisor            bool
	Multiplier            float64
	HasMultiplier         bool
	SampleIntervalSeconds uint32
	Labels                map[string]string
	Options               map[string]string
	Enabled               bool
	DiscoveredAt          time.Time
	LastPolledAt          time.Time
	LastError             string
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type RawSample struct {
	TargetID     string
	DeviceID     string
	EntityType   EntityType
	EntityID     string
	RecipeID     string
	MetricName   string
	ValueType    ValueType
	FloatValue   float64
	CounterValue uint64
	CounterValid bool
	CounterWidth uint8
	StringValue  string
	SampledAt    time.Time
	IntervalMS   uint32
	QualityFlags uint32
	PollSequence uint64
	SourceRunID  string
	SampleIndex  uint32
	Labels       map[string]string
}
