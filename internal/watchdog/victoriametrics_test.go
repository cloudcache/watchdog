package watchdog

import "testing"

func TestSNMPPortMetricSelector(t *testing.T) {
	selector := SNMPPortMetricSelector(MetricSNMPIfInBps, MetricLabelSet{
		TenantID: "tenant-a",
		TargetID: "target-a",
		DeviceID: "device-a",
		PortID:   "port-a",
		IfIndex:  42,
		SideType: PortSideProvider,
	})
	want := `watchdog_snmp_if_in_bps{tenant_id="tenant-a",target_id="target-a",device_id="device-a",port_id="port-a",if_index="42",side_type="provider"}`
	if selector != want {
		t.Fatalf("selector = %s", selector)
	}
}

func TestLibreNMSBaselineMetricSelectorsUseStableIDs(t *testing.T) {
	portSelector := SNMPPortMetricSelector(MetricSNMPOpticalRxDBM, MetricLabelSet{
		TenantID: "tenant-a",
		TargetID: "target-a",
		DeviceID: "device-a",
		PortID:   "port-a",
		IfIndex:  42,
	})
	wantPort := `watchdog_snmp_optical_rx_dbm{tenant_id="tenant-a",target_id="target-a",device_id="device-a",port_id="port-a",if_index="42"}`
	if portSelector != wantPort {
		t.Fatalf("port selector = %s", portSelector)
	}
	bgpSelector := BGPMetricSelector(MetricBGPState, BGPMetricLabelSet{
		TenantID:  "tenant-a",
		TargetID:  "target-a",
		DeviceID:  "device-a",
		SessionID: "session-a",
		PeerAddr:  "203.0.113.1",
		PeerAS:    64500,
		AFI:       "ipv4",
		SAFI:      "unicast",
	})
	wantBGP := `watchdog_bgp_session_state{tenant_id="tenant-a",target_id="target-a",device_id="device-a",session_id="session-a",peer_addr="203.0.113.1",peer_as="64500",afi="ipv4",safi="unicast"}`
	if bgpSelector != wantBGP {
		t.Fatalf("bgp selector = %s", bgpSelector)
	}
}
