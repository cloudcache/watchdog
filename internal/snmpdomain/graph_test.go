package snmpdomain

import (
	"encoding/json"
	"slices"
	"testing"
	"time"
)

func TestDeviceOverviewDashboardUsesCapabilitiesAndPhysicalPorts(t *testing.T) {
	dashboard := NewDeviceOverviewDashboard(
		GraphDevice{ID: "device-a", SysName: "edge-a"},
		[]GraphPort{
			{ID: "physical", IfName: "xe-0/0/0", AdminStatus: "up", OperStatus: "up", Metadata: map[string]string{"if_type": "6"}},
			{ID: "vlan", IfName: "Vlanif10", AdminStatus: "up", OperStatus: "down", Metadata: map[string]string{"if_type": "6"}},
			{ID: "lag", IfName: "ae0", AdminStatus: "down", OperStatus: "down", Metadata: map[string]string{"if_type": "161"}},
		},
		true,
		[]GraphSensor{{Class: "optical", Name: "Rx power", Unit: "dBm"}},
	)
	if dashboard.ID != "network-device-overview:device-a" || dashboard.Title != "edge-a" || dashboard.Refresh != 30 {
		t.Fatalf("unexpected dashboard identity: %+v", dashboard)
	}
	panelIDs := make([]string, 0, len(dashboard.Panels))
	for _, panel := range dashboard.Panels {
		panelIDs = append(panelIDs, panel.ID)
	}
	for _, required := range []string{"overall-traffic", "interface-errors", "cpu-memory", "optical-power", "bgp-prefixes"} {
		if !slices.Contains(panelIDs, required) {
			t.Fatalf("missing panel %q in %v", required, panelIDs)
		}
	}
	if got := dashboard.Panels[0].Queries[0].PortIDs; !slices.Equal(got, []string{"physical"}) {
		t.Fatalf("device aggregate includes virtual ports: %v", got)
	}
	links := dashboard.Panels[0].Links
	if len(links) != 3 || links[0].PortID != "physical" || links[0].Status != "up" ||
		links[1].Status != "down" || links[2].Status != "disabled" {
		t.Fatalf("port link identity/status contract is incomplete: %+v", links)
	}
}

func TestGraphPortStatusDistinguishesOperationalAndDisabledPorts(t *testing.T) {
	tests := []struct {
		name string
		port GraphPort
		want string
	}{
		{name: "numeric up", port: GraphPort{AdminStatus: "1", OperStatus: "1"}, want: "up"},
		{name: "operational down", port: GraphPort{AdminStatus: "up", OperStatus: "down"}, want: "down"},
		{name: "administratively down", port: GraphPort{AdminStatus: "2", OperStatus: "1"}, want: "disabled"},
		{name: "explicitly disabled", port: GraphPort{Disabled: true, AdminStatus: "up", OperStatus: "up"}, want: "disabled"},
		{name: "unknown", port: GraphPort{AdminStatus: "testing", OperStatus: "testing"}, want: "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := graphPortStatus(test.port); got != test.want {
				t.Fatalf("graphPortStatus()=%q want %q", got, test.want)
			}
		})
	}
}

func TestPortOverviewDashboardPreservesFrontendContract(t *testing.T) {
	dashboard := NewPortOverviewDashboard(GraphPort{ID: "port-a", IfName: "xe-0/0/0"})
	if len(dashboard.Panels) != 3 || dashboard.Panels[0].Stack != "signed" {
		t.Fatalf("unexpected port panels: %+v", dashboard.Panels)
	}
	payload, err := json.Marshal(dashboard)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["id"] != "network-port-overview:port-a" || decoded["title"] != "xe-0/0/0" {
		t.Fatalf("unexpected JSON contract: %s", payload)
	}
}

func TestEventJSONPreservesExistingAPIFields(t *testing.T) {
	payload, err := json.Marshal(Event{ID: "event-a", DeviceID: "device-a", EntityType: EntityPort, EntityID: "port-a", OccurredAt: time.Unix(1, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"ID", "DeviceID", "EntityType", "EntityID", "OccurredAt", "CreatedAt"} {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("missing %q in %s", key, payload)
		}
	}
	if _, leaked := decoded["TenantID"]; leaked {
		t.Fatalf("legacy tenant field leaked in %s", payload)
	}
}
