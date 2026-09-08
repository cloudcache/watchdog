package server

import (
	"reflect"
	"strings"
	"testing"
)

func TestValidateDeviceGroupRule(t *testing.T) {
	rule, err := validateDeviceGroup("Core", "dynamic", &deviceGroupRule{
		Kind: []string{"network", "network"}, OS: []string{"junos"},
		Labels: map[string][]string{"site": {"dc-b", "dc-a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rule.Kind, []string{"network"}) || !reflect.DeepEqual(rule.Labels["site"], []string{"dc-a", "dc-b"}) {
		t.Fatalf("rule not canonicalized: %+v", rule)
	}
	for name, input := range map[string]struct {
		kind string
		rule *deviceGroupRule
	}{
		"empty dynamic": {"dynamic", &deviceGroupRule{}},
		"static rule":   {"static", &deviceGroupRule{OS: []string{"junos"}}},
		"invalid kind":  {"dynamic", &deviceGroupRule{Kind: []string{"router"}}},
		"unsafe label":  {"dynamic", &deviceGroupRule{Labels: map[string][]string{"a.b": {"x"}}}},
		"empty label":   {"dynamic", &deviceGroupRule{Labels: map[string][]string{"site": {}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := validateDeviceGroup("g", input.kind, input.rule); err == nil {
				t.Fatal("invalid rule accepted")
			}
		})
	}
}

func TestCompileDeviceGroupRuleUsesOnlyFixedColumns(t *testing.T) {
	where, args := compileDeviceGroupRule(deviceGroupRule{
		Kind: []string{"network"}, OS: []string{"junos", "iosxe"},
		Labels: map[string][]string{"site": {"dc-a"}},
	}, "d")
	if !strings.Contains(where, "d.kind IN (?)") || !strings.Contains(where, "d.os IN (?,?)") || !strings.Contains(where, "JSON_EXTRACT(d.labels_json, ?)") {
		t.Fatalf("unexpected predicate: %s", where)
	}
	want := []any{"network", "junos", "iosxe", `$."site"`, "dc-a"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args=%#v want=%#v", args, want)
	}
}

func TestValidateLocation(t *testing.T) {
	lat, lon, invalid := 1.5, 2.5, 200.0
	if err := validateLocation("Singapore", &lat, &lon); err != nil {
		t.Fatal(err)
	}
	if err := validateLocation("Singapore", &lat, nil); err == nil {
		t.Fatal("partial coordinate accepted")
	}
	if err := validateLocation("Singapore", &lat, &invalid); err == nil {
		t.Fatal("invalid longitude accepted")
	}
}
