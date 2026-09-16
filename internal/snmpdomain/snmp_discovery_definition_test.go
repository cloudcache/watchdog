package snmpdomain

import (
	"context"
	"testing"
)

// The processors section of a LibreNMS os_discovery definition (vrp-style)
// must drive collection through MIB-name resolution, honoring skip_values and
// descr templates — no vendor branches in Go.
func TestDefinitionDrivenProcessorsFromOSDiscovery(t *testing.T) {
	cpuOID := snmpMIBOID("HUAWEI-ENTITY-EXTENT-MIB::hwEntityCpuUsage")
	operOID := snmpMIBOID("HUAWEI-ENTITY-EXTENT-MIB::hwEntityOperStatus")
	nameOID := snmpMIBOID("ENTITY-MIB::entPhysicalName")
	query := fakeQueryEngine{
		walks: map[string]QueryResponse{
			cpuOID: {VarBinds: []VarBind{
				{OID: cpuOID + ".9", Value: "17"},
				{OID: cpuOID + ".12", Value: "44"},
			}},
			operOID: {VarBinds: []VarBind{
				{OID: operOID + ".9", Value: "3"},  // enabled -> kept
				{OID: operOID + ".12", Value: "2"}, // != 3 -> skipped
			}},
			nameOID: {VarBinds: []VarBind{
				{OID: nameOID + ".9", Value: "MPU Board"},
			}},
		},
	}
	req := DiscoveryContext{
		Device: Device{ID: "dev1"},
		Query:  query,
		OSDiscovery: map[string]any{
			"processors": map[string]any{
				"data": []any{map[string]any{
					"oid":   "HUAWEI-ENTITY-EXTENT-MIB::hwEntityStateEntry",
					"value": "HUAWEI-ENTITY-EXTENT-MIB::hwEntityCpuUsage",
					"descr": "{{ ENTITY-MIB::entPhysicalName }} Processor",
					"skip_values": []any{map[string]any{
						"oid":   "HUAWEI-ENTITY-EXTENT-MIB::hwEntityOperStatus",
						"op":    "!=",
						"value": 3,
					}},
				}},
			},
		},
	}
	recipes := discoverDefinitionProcessors(context.Background(), req)
	if len(recipes) != 2 {
		t.Fatalf("expected 2 recipes (usage + device percent) for the one kept row, got %d", len(recipes))
	}
	byMetric := map[string]Recipe{}
	for _, recipe := range recipes {
		byMetric[recipe.MetricName] = recipe
	}
	usage := byMetric[MetricSNMPProcessorUsage]
	if usage.NumericOID != cpuOID+".9" {
		t.Fatalf("usage polls %s, want %s.9 (row 12 must be skipped)", usage.NumericOID, cpuOID)
	}
	if usage.Labels["description"] != "MPU Board Processor" {
		t.Fatalf("descr template = %q", usage.Labels["description"])
	}
	if byMetric[MetricSNMPDeviceCPUPercent].NumericOID != cpuOID+".9" {
		t.Fatalf("device cpu percent recipe missing")
	}
	if usage.MIB != "HUAWEI-ENTITY-EXTENT-MIB" {
		t.Fatalf("mib label = %q", usage.MIB)
	}
}

// When the MIB module cannot be resolved, the entry's num_oid template is the
// fallback — collection must not depend on every vendor MIB being loadable.
func TestDefinitionValueOIDFallsBackToNumOIDTemplate(t *testing.T) {
	entry := map[string]any{
		"value":   "NOT-A-REAL-MIB::noSuchObject",
		"num_oid": ".1.3.6.1.4.1.2011.6.9.1.4.2.1.4.{{ $index }}",
	}
	if got := defValueOID(entry, "value"); got != "1.3.6.1.4.1.2011.6.9.1.4.2.1.4" {
		t.Fatalf("num_oid fallback = %q", got)
	}
}

func TestDefinitionSensorsApplyDivisorAndSkip(t *testing.T) {
	tempOID := snmpMIBOID("HUAWEI-ENTITY-EXTENT-MIB::hwEntityTemperature")
	query := fakeQueryEngine{
		walks: map[string]QueryResponse{
			tempOID: {VarBinds: []VarBind{
				{OID: tempOID + ".9", Value: "41"},
				{OID: tempOID + ".12", Value: "2147483647"}, // sentinel -> skipped
			}},
		},
	}
	req := DiscoveryContext{
		Device: Device{ID: "dev1"},
		Query:  query,
		OSDiscovery: map[string]any{
			"sensors": map[string]any{
				"temperature": map[string]any{
					"data": []any{map[string]any{
						"oid":   "HUAWEI-ENTITY-EXTENT-MIB::hwEntityStateTable",
						"value": "HUAWEI-ENTITY-EXTENT-MIB::hwEntityTemperature",
						"descr": "Board {{ $index }}",
						"skip_values": []any{map[string]any{
							"oid":   "HUAWEI-ENTITY-EXTENT-MIB::hwEntityTemperature",
							"op":    ">=",
							"value": "2147483646",
						}},
					}},
				},
			},
		},
	}
	sensors, recipes := discoverDefinitionSensors(context.Background(), req)
	if len(sensors) != 1 || len(recipes) != 1 {
		t.Fatalf("sensors=%d recipes=%d, want 1/1", len(sensors), len(recipes))
	}
	if sensors[0].Class != "temperature" || sensors[0].Value != 41 || sensors[0].Name != "Board 9" {
		t.Fatalf("sensor = %+v", sensors[0])
	}
}

// Candidates are probed, never vendor-gated: only sources the device answers
// produce recipes, and OIDs already claimed by the OS definition are skipped.
func TestCandidateProbingCollectsOnlyAnsweredSources(t *testing.T) {
	hwCPU := snmpMIBOID("HUAWEI-ENTITY-EXTENT-MIB::hwEntityCpuUsage")
	hrCPU := snmpMIBOID("HOST-RESOURCES-MIB::hrProcessorLoad")
	query := fakeQueryEngine{
		walks: map[string]QueryResponse{
			// Device answers the Huawei table only; hrProcessorLoad walks empty.
			hwCPU: {VarBinds: []VarBind{{OID: hwCPU + ".9", Value: "12"}}},
		},
	}
	req := DiscoveryContext{Device: Device{ID: "dev1"}, Query: query}
	recipes := probeCPUCandidates(context.Background(), req, map[string]bool{})
	if len(recipes) != 2 {
		t.Fatalf("expected 2 recipes from the one answering source, got %d", len(recipes))
	}
	if recipes[0].NumericOID != hwCPU+".9" {
		t.Fatalf("recipe polls %s", recipes[0].NumericOID)
	}
	// A claimed OID (already covered by the OS definition) must not duplicate.
	claimed := map[string]bool{hwCPU: true, hrCPU: true}
	if got := probeCPUCandidates(context.Background(), req, claimed); len(got) != 0 {
		t.Fatalf("claimed source produced %d duplicate recipes", len(got))
	}
}
