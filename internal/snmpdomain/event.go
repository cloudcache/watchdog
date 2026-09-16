package snmpdomain

import "time"

type EntityType string

const (
	EntityDevice    EntityType = "device"
	EntityPort      EntityType = "port"
	EntitySensor    EntityType = "sensor"
	EntityProcessor EntityType = "processor"
	EntityMemory    EntityType = "memory"
	EntityStorage   EntityType = "storage"
	EntityBGPPeer   EntityType = "bgp_peer"
	EntityVLAN      EntityType = "vlan"
	EntityLAG       EntityType = "lag"
	EntityPhysical  EntityType = "physical_entity"
)

// Event is the single-domain API representation of an SNMP event. Events are
// stored and queried in ClickHouse; no tenant identity is carried in the row.
type Event struct {
	ID         string         `json:"ID"`
	DeviceID   string         `json:"DeviceID"`
	EntityType EntityType     `json:"EntityType"`
	EntityID   string         `json:"EntityID"`
	Source     string         `json:"Source"`
	Severity   string         `json:"Severity"`
	EventType  string         `json:"EventType"`
	Message    string         `json:"Message"`
	Raw        map[string]any `json:"Raw"`
	OccurredAt time.Time      `json:"OccurredAt"`
	CreatedAt  time.Time      `json:"CreatedAt"`
}
