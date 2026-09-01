package watchdog

import (
	"os"
	"path/filepath"
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
