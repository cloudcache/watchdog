package server

import "testing"

func TestSNMPManagementAndCompatibilityRoutesAreMounted(t *testing.T) {
	routes := (&Server{}).newRouter().Routes()
	have := make(map[string]bool, len(routes))
	for _, route := range routes {
		have[route.Method+" "+route.Path] = true
	}
	for _, want := range []string{
		"GET /api/v1/devices/:id/events",
		"GET /api/v1/devices/:id/events/facets",
		"GET /api/v1/network/devices/:id/events",
		"GET /api/v1/network/devices/:id/events/facets",
		"GET /api/v1/network/ports/:port_id/policy",
		"PATCH /api/v1/network/ports/:port_id/policy",
		"GET /api/v1/network/traffic-policy-defaults",
		"PUT /api/v1/network/traffic-policy-defaults",
		"GET /api/v1/snmp/mib-modules",
		"PUT /api/v1/snmp/mib-modules",
		"DELETE /api/v1/snmp/mib-modules/:module_id",
		"GET /api/v1/retention/policies",
		"PUT /api/v1/retention/policies",
		"DELETE /api/v1/retention/policies/:policy_id",
		"POST /api/v1/snmp/traps",
		"GET /api/v1/metrics/vmquery",
	} {
		if !have[want] {
			t.Errorf("route %q is not mounted", want)
		}
	}
}
