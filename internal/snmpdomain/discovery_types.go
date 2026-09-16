package snmpdomain

import (
	"context"
	"time"
)

type Port struct {
	ID          string
	DeviceID    string
	IfIndex     uint64
	IfName      string
	IfAlias     string
	IfDescr     string
	AdminStatus string
	OperStatus  string
	SpeedBps    uint64
	Metadata    map[string]string
	UpdatedAt   time.Time
}

type InterfaceAddress struct {
	ID           string
	DeviceID     string
	PortID       string
	IfIndex      uint64
	Address      string
	Family       string
	PrefixLength uint8
	Origin       string
	ContextName  string
	UpdatedAt    time.Time
}

type Sensor struct {
	ID          string
	DeviceID    string
	PortID      string
	SensorIndex uint64
	Class       string
	Name        string
	OID         string
	Unit        string
	Value       float64
	WarnLimit   float64
	CritLimit   float64
	Status      string
	Metadata    map[string]string
	UpdatedAt   time.Time
}

type PhysicalEntity struct {
	Index            uint64
	Name             string
	Description      string
	Class            string
	VendorType       string
	ContainedIn      uint64
	ParentRelPos     int
	HardwareRevision string
	FirmwareRevision string
	SoftwareRevision string
	SerialNumber     string
	ManufacturerName string
	ModelName        string
	Alias            string
	AssetID          string
	IsFRU            bool
}

type VLAN struct {
	VLANID uint32
	Name   string
	Status string
}

type LAGGroup struct {
	AggregateIndex uint64
	MACAddress     string
	Mode           string
}

type BGPSession struct {
	ID                 string
	DeviceID           string
	PeerAddr           string
	PeerAS             uint64
	LocalAS            uint64
	AFI                string
	SAFI               string
	State              string
	AcceptedPrefixes   uint64
	DeniedPrefixes     uint64
	AdvertisedPrefixes uint64
	Uptime             time.Duration
	Metadata           map[string]string
	UpdatedAt          time.Time
}

type DiscoveryRequest struct {
	TargetID string
	Target   QueryTarget
	Device   Device
	Profile  Profile
}

type DiscoveryResult struct {
	DeviceUpdates      Device
	CompletedModules   []string
	Ports              []Port
	InterfaceAddresses []InterfaceAddress
	Sensors            []Sensor
	PhysicalEntities   []PhysicalEntity
	BGPSessions        []BGPSession
	VLANs              []VLAN
	LAGs               []LAGGroup
	Recipes            []Recipe
	Events             []Event
}

type Table struct {
	Rows map[string]map[string]VarBind
}

type PreCacheRequest struct {
	Module string
	MIB    string
	OID    string
	Mode   string
	Flags  QueryFlags
}

type PreCache struct {
	ByOID   map[string]QueryResponse
	ByTable map[string]Table
}

type DiscoveryModule interface {
	Name() string
	Discover(context.Context, DiscoveryContext) (DiscoveryResult, error)
}

type DiscoveryContext struct {
	TargetID   string
	Target     QueryTarget
	Device     Device
	Profile    Profile
	OS         OSMatch
	Query      QueryEngine
	PreCache   PreCache
	Definition ModuleDefinition
	// OSDiscovery is the matched OS's LibreNMS os_discovery "modules" map;
	// definition-driven modules walk what it declares instead of hardcoding
	// vendor branches.
	OSDiscovery map[string]any
}

type DiscoveryRunner interface {
	Discover(context.Context, DiscoveryRequest) (DiscoveryResult, error)
}
