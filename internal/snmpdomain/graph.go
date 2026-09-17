package snmpdomain

import (
	"strings"

	"github.com/cloudcache/watchdog/internal/metricdomain"
)

// GraphDashboard is the stable declarative contract consumed by the frontend
// graph renderer. It contains queries, not resolved time-series data.
type GraphDashboard struct {
	ID      string       `json:"id"`
	Title   string       `json:"title"`
	Panels  []GraphPanel `json:"panels"`
	Refresh int          `json:"refresh"`
}

type GraphPanel struct {
	ID           string            `json:"id"`
	Title        string            `json:"title"`
	Type         string            `json:"type"`
	Unit         string            `json:"unit,omitempty"`
	Stack        string            `json:"stack,omitempty"`
	Queries      []GraphQuery      `json:"queries"`
	Links        []GraphLink       `json:"links,omitempty"`
	QueryOptions GraphQueryOptions `json:"query_options"`
}

type GraphQuery struct {
	Metric      string              `json:"metric"`
	Scope       string              `json:"scope"`
	Aggregation string              `json:"aggregation,omitempty"`
	Label       string              `json:"label,omitempty"`
	PortID      string              `json:"port_id,omitempty"`
	PortIDs     []string            `json:"port_ids,omitempty"`
	Transform   GraphQueryTransform `json:"transform,omitempty"`
}

type GraphQueryTransform struct {
	Negative bool `json:"negative,omitempty"`
	Rate     bool `json:"rate,omitempty"`
}

type GraphLink struct {
	Href   string `json:"href"`
	Label  string `json:"label"`
	PortID string `json:"port_id,omitempty"`
	Status string `json:"status,omitempty"`
}

type GraphQueryOptions struct {
	MaxDataPoints int    `json:"max_data_points"`
	MinInterval   string `json:"min_interval"`
}

// GraphDevice, GraphPort and GraphSensor are deliberately limited to the
// inventory fields needed to compose dashboard schemas. They are not storage
// models and carry no tenant or credential state.
type GraphDevice struct {
	ID      string
	SysName string
	Model   string
}

type GraphPort struct {
	ID          string
	IfName      string
	IfDescr     string
	AdminStatus string
	OperStatus  string
	Disabled    bool
	Metadata    map[string]string
}

type GraphSensor struct {
	Class string
	Name  string
	Unit  string
}

const (
	graphScopeDevice          = "device"
	graphScopePort            = "port"
	graphScopePortsAggregate  = "ports-aggregate"
	defaultGraphMaxDataPoints = 1200
	defaultGraphMinInterval   = "1m"
)

func defaultGraphQueryOptions() GraphQueryOptions {
	return GraphQueryOptions{MaxDataPoints: defaultGraphMaxDataPoints, MinInterval: defaultGraphMinInterval}
}

var virtualIfTypes = map[string]bool{
	"1": true, "24": true, "53": true, "131": true, "135": true, "136": true, "150": true, "161": true,
}

var virtualIfNamePrefixes = []string{
	"vlanif", "vlan-interface", "null", "loopback", "inloopback",
	"eth-trunk", "port-channel", "bridge-aggregation", "tunnel",
}

func aggregatableGraphPorts(ports []GraphPort) []GraphPort {
	result := make([]GraphPort, 0, len(ports))
	for _, port := range ports {
		if virtualIfTypes[port.Metadata["if_type"]] {
			continue
		}
		name := strings.ToLower(firstGraphValue(port.IfName, port.IfDescr))
		virtual := false
		for _, prefix := range virtualIfNamePrefixes {
			if strings.HasPrefix(name, prefix) {
				virtual = true
				break
			}
		}
		if !virtual {
			result = append(result, port)
		}
	}
	return result
}

// NewDeviceOverviewDashboard composes the historical device overview schema
// from the current MySQL inventory and ClickHouse metric catalog.
func NewDeviceOverviewDashboard(device GraphDevice, ports []GraphPort, hasBGP bool, sensors []GraphSensor) GraphDashboard {
	links := make([]GraphLink, 0, len(ports))
	for _, port := range ports {
		links = append(links, GraphLink{
			Href: "/network/ports/" + port.ID, Label: graphPortName(port), PortID: port.ID, Status: graphPortStatus(port),
		})
	}
	panels := make([]GraphPanel, 0, 5)
	if len(ports) > 0 {
		trafficPortIDs := make([]string, 0, len(ports))
		for _, port := range aggregatableGraphPorts(ports) {
			trafficPortIDs = append(trafficPortIDs, port.ID)
		}
		panels = append(panels,
			GraphPanel{
				ID: "overall-traffic", Title: "Overall Traffic", Type: "line", Unit: "bps", Stack: "signed",
				Queries: []GraphQuery{
					{Metric: metricdomain.SNMPIfInBps, Scope: graphScopePortsAggregate, Aggregation: "sum", Label: "In", PortIDs: trafficPortIDs},
					{Metric: metricdomain.SNMPIfOutBps, Scope: graphScopePortsAggregate, Aggregation: "sum", Label: "Out", PortIDs: trafficPortIDs, Transform: GraphQueryTransform{Negative: true}},
				},
				Links: links, QueryOptions: defaultGraphQueryOptions(),
			},
			GraphPanel{
				ID: "interface-errors", Title: "Interface Errors", Type: "line", Unit: "pps",
				Queries: []GraphQuery{
					{Metric: metricdomain.SNMPIfInErrorsTotal, Scope: graphScopePortsAggregate, Aggregation: "sum", Label: "In errors", PortIDs: trafficPortIDs, Transform: GraphQueryTransform{Rate: true}},
					{Metric: metricdomain.SNMPIfOutErrorsTotal, Scope: graphScopePortsAggregate, Aggregation: "sum", Label: "Out errors", PortIDs: trafficPortIDs, Transform: GraphQueryTransform{Rate: true}},
				},
				QueryOptions: defaultGraphQueryOptions(),
			},
		)
	}
	panels = append(panels, GraphPanel{
		ID: "cpu-memory", Title: "CPU / Memory", Type: "line", Unit: "percent",
		Queries: []GraphQuery{
			{Metric: metricdomain.SNMPDeviceCPUPercent, Scope: graphScopeDevice, Label: "CPU"},
			{Metric: metricdomain.SNMPDeviceMemPercent, Scope: graphScopeDevice, Label: "Memory"},
		},
		QueryOptions: defaultGraphQueryOptions(),
	})
	if hasOpticalGraphSensor(sensors) {
		panels = append(panels, GraphPanel{
			ID: "optical-power", Title: "Optical Power", Type: "line", Unit: "dbm",
			Queries: []GraphQuery{
				{Metric: metricdomain.SNMPOpticalRxDBM, Scope: graphScopeDevice, Label: "RX"},
				{Metric: metricdomain.SNMPOpticalTxDBM, Scope: graphScopeDevice, Label: "TX"},
			},
			QueryOptions: defaultGraphQueryOptions(),
		})
	}
	if hasBGP {
		panels = append(panels, GraphPanel{
			ID: "bgp-prefixes", Title: "BGP Prefixes", Type: "line", Unit: "count",
			Queries: []GraphQuery{
				{Metric: metricdomain.BGPAcceptedPrefixes, Scope: graphScopeDevice, Label: "Accepted"},
				{Metric: metricdomain.BGPDeniedPrefixes, Scope: graphScopeDevice, Label: "Denied"},
				{Metric: metricdomain.BGPAdvertisedPrefixes, Scope: graphScopeDevice, Label: "Advertised"},
			},
			QueryOptions: defaultGraphQueryOptions(),
		})
	}
	return GraphDashboard{
		ID: "network-device-overview:" + device.ID, Title: firstGraphValue(device.SysName, device.Model, device.ID),
		Refresh: 30, Panels: panels,
	}
}

func NewPortOverviewDashboard(port GraphPort) GraphDashboard {
	return GraphDashboard{
		ID: "network-port-overview:" + port.ID, Title: graphPortName(port), Refresh: 30,
		Panels: []GraphPanel{
			{
				ID: "traffic", Title: "Traffic", Type: "line", Unit: "bps", Stack: "signed",
				Queries: []GraphQuery{
					{Metric: metricdomain.SNMPIfInBps, Scope: graphScopePort, PortID: port.ID, Label: "In"},
					{Metric: metricdomain.SNMPIfOutBps, Scope: graphScopePort, PortID: port.ID, Label: "Out", Transform: GraphQueryTransform{Negative: true}},
				},
				QueryOptions: defaultGraphQueryOptions(),
			},
			{
				ID: "errors", Title: "Errors / CRC", Type: "line", Unit: "pps",
				Queries: []GraphQuery{
					{Metric: metricdomain.SNMPIfInErrorsTotal, Scope: graphScopePort, PortID: port.ID, Label: "In errors", Transform: GraphQueryTransform{Rate: true}},
					{Metric: metricdomain.SNMPIfOutErrorsTotal, Scope: graphScopePort, PortID: port.ID, Label: "Out errors", Transform: GraphQueryTransform{Rate: true}},
					{Metric: metricdomain.SNMPIfInCRCTotal, Scope: graphScopePort, PortID: port.ID, Label: "In CRC", Transform: GraphQueryTransform{Rate: true}},
					{Metric: metricdomain.SNMPIfOutCRCTotal, Scope: graphScopePort, PortID: port.ID, Label: "Out CRC", Transform: GraphQueryTransform{Rate: true}},
				},
				QueryOptions: defaultGraphQueryOptions(),
			},
			{
				ID: "optical", Title: "Optical Power", Type: "line", Unit: "dbm",
				Queries: []GraphQuery{
					{Metric: metricdomain.SNMPOpticalRxDBM, Scope: graphScopePort, PortID: port.ID, Label: "RX"},
					{Metric: metricdomain.SNMPOpticalTxDBM, Scope: graphScopePort, PortID: port.ID, Label: "TX"},
				},
				QueryOptions: defaultGraphQueryOptions(),
			},
		},
	}
}

func hasOpticalGraphSensor(sensors []GraphSensor) bool {
	for _, sensor := range sensors {
		value := strings.ToLower(sensor.Class + " " + sensor.Unit + " " + sensor.Name)
		for _, marker := range []string{"dbm", "optical", "rx", "tx", "power", "dom", "transceiver"} {
			if strings.Contains(value, marker) {
				return true
			}
		}
	}
	return false
}

func graphPortName(port GraphPort) string {
	return firstGraphValue(port.IfName, port.IfDescr, port.ID)
}

func graphPortStatus(port GraphPort) string {
	admin, oper := strings.ToLower(port.AdminStatus), strings.ToLower(port.OperStatus)
	if port.Disabled || admin == "down" || admin == "2" {
		return "disabled"
	}
	if oper == "up" || oper == "1" {
		return "up"
	}
	if oper == "down" || oper == "2" {
		return "down"
	}
	return "unknown"
}

func firstGraphValue(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
