package snmpdomain

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseLibrenmsDefinitionsParsesOSDefinition(t *testing.T) {
	dir := t.TempDir()
	defsDir := filepath.Join(dir, "resources", "definitions", "os_detection")
	if err := os.MkdirAll(defsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	yamlContent := `os: cisco_ios
text: 'Cisco IOS'
type: network
group: cisco
icon: cisco
mib_dir: cisco
discovery:
    -
        sysObjectID:
            - .1.3.6.1.4.1.9.1
        sysDescr:
            - "/Cisco/"
discovery_modules:
    ports: true
    sensors: true
poller_modules:
    ports: true
bad_iftype:
    - 24
`
	if err := os.WriteFile(filepath.Join(defsDir, "cisco_ios.yaml"), []byte(yamlContent), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := ParseLibrenmsDefinitions(dir, "test-v1")
	if err != nil {
		t.Fatalf("ParseLibrenmsDefinitions error = %v", err)
	}
	if len(result.OSDefinitions) != 1 {
		t.Fatalf("os definitions = %d, want 1", len(result.OSDefinitions))
	}
	def := result.OSDefinitions[0]
	if def.OSName != "cisco_ios" || def.Vendor != "cisco" || def.Source != "librenms" || def.SourceVersion != "test-v1" {
		t.Fatalf("os def = %#v", def)
	}
	if def.Class != "network" {
		t.Fatalf("class = %v", def.Class)
	}
}

func TestParseLibrenmsDefinitionsAcceptsScalarDetectionValues(t *testing.T) {
	dir := t.TempDir()
	defsDir := filepath.Join(dir, "resources", "definitions", "os_detection")
	if err := os.MkdirAll(defsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	yamlContent := `os: junos
text: Juniper JunOS
type: network
group: juniper
discovery:
    -
        sysObjectID: .1.3.6.1.4.1.2636
    -
        sysDescr:
            - kernel JUNOS
        sysDescr_regex: '/Juniper Networks/i'
`
	if err := os.WriteFile(filepath.Join(defsDir, "junos.yaml"), []byte(yamlContent), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := ParseLibrenmsDefinitions(dir, "test-v1")
	if err != nil {
		t.Fatalf("ParseLibrenmsDefinitions error = %v", err)
	}
	if len(result.OSDefinitions) != 1 {
		t.Fatalf("os definitions = %d, want 1", len(result.OSDefinitions))
	}
	match, ok := DetectOS(OSFingerprint{
		SysObjectID: ".1.3.6.1.4.1.2636.1.1.1.2.25",
		SysDescr:    "Juniper Networks, Inc. mx480 internet router, kernel JUNOS 21.2R3-S8.5",
	}, result.OSDefinitions)
	if !ok || match.OSName != "junos" || match.Vendor != "juniper" {
		t.Fatalf("unexpected match: %#v, ok=%v", match, ok)
	}
}

func TestParseLibrenmsDefinitionsPreservesActiveProbeConditions(t *testing.T) {
	dir := t.TempDir()
	defsDir := filepath.Join(dir, "resources", "definitions", "os_detection")
	if err := os.MkdirAll(defsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	yamlContent := `os: supermicro-bmc
discovery:
    - sysObjectID: .1.3.6.1.4.1.
      snmpget:
        oid: ATEN-IPMI-MIB::bmcMajorVesion.0
        op: '!='
        value: false
`
	if err := os.WriteFile(filepath.Join(defsDir, "supermicro-bmc.yaml"), []byte(yamlContent), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := ParseLibrenmsDefinitions(dir, "test-v1")
	if err != nil {
		t.Fatalf("ParseLibrenmsDefinitions error = %v", err)
	}
	rules := snmpOSDetectionRules(result.OSDefinitions[0].Definition)
	if len(rules) != 1 || rules[0]["snmpget"] == nil {
		t.Fatalf("active probe condition was discarded: %#v", rules)
	}
}

func TestParseLibrenmsDefinitionsReportsInvalidDetectionFile(t *testing.T) {
	dir := t.TempDir()
	defsDir := filepath.Join(dir, "resources", "definitions", "os_detection")
	if err := os.MkdirAll(defsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(defsDir, "invalid.yaml")
	if err := os.WriteFile(path, []byte("os: invalid\ndiscovery:\n  - sysObjectID: {bad: value}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := ParseLibrenmsDefinitions(dir, "test-v1")
	if err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("error = %v, want source path", err)
	}
}

func TestParseLibrenmsDefinitionsParsesTrapHandlersFromPHP(t *testing.T) {
	dir := t.TempDir()
	configDir := filepath.Join(dir, "config")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatal(err)
	}
	phpContent := `<?php
$config['traps'] = [
    'IF-MIB::linkDown' => LibreNMS\Snmptrap\Handlers\LinkDown::class,
    'IF-MIB::linkUp' => LibreNMS\Snmptrap\Handlers\LinkUp::class,
];
`
	if err := os.WriteFile(filepath.Join(configDir, "snmptraps.php"), []byte(phpContent), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := ParseLibrenmsDefinitions(dir, "test-v1")
	if err != nil {
		t.Fatalf("ParseLibrenmsDefinitions error = %v", err)
	}
	if len(result.TrapHandlers) != 2 {
		t.Fatalf("trap handlers = %d, want 2", len(result.TrapHandlers))
	}
}
