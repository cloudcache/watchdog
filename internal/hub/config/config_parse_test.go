package config

import (
	"strings"
	"testing"
)

func TestDecodeConfigRejectsUnknownFields(t *testing.T) {
	_, err := decodeConfig([]byte("systems:\n  - name: edge-a\n    host: 192.0.2.1\n    prot: 45876\n"))
	if err == nil || !strings.Contains(err.Error(), "prot") {
		t.Fatalf("error = %v, want unknown field error", err)
	}
}

func TestDecodeConfigRejectsMultipleDocuments(t *testing.T) {
	_, err := decodeConfig([]byte("systems: []\n---\nsystems: []\n"))
	if err == nil || !strings.Contains(err.Error(), "multiple YAML documents") {
		t.Fatalf("error = %v, want multiple document error", err)
	}
}

func TestNormalizeSystemConfigs(t *testing.T) {
	systems := []systemConfig{{
		Name:  " edge-a ",
		Host:  " 192.0.2.1 ",
		Users: []string{" ADMIN@EXAMPLE.COM ", "admin@example.com", ""},
	}}
	if err := normalizeSystemConfigs(systems); err != nil {
		t.Fatal(err)
	}
	if systems[0].Name != "edge-a" || systems[0].Host != "192.0.2.1" || systems[0].Port != 45876 {
		t.Fatalf("system = %#v", systems[0])
	}
	if len(systems[0].Users) != 1 || systems[0].Users[0] != "admin@example.com" {
		t.Fatalf("users = %#v", systems[0].Users)
	}
}

func TestNormalizeSystemConfigsRejectsInvalidAndDuplicateEntries(t *testing.T) {
	tests := []struct {
		name    string
		systems []systemConfig
		want    string
	}{
		{
			name:    "missing host",
			systems: []systemConfig{{Name: "edge-a"}},
			want:    "name and host",
		},
		{
			name: "duplicate identity",
			systems: []systemConfig{
				{Name: "edge-a", Host: "192.0.2.1"},
				{Name: " edge-a ", Host: " 192.0.2.1 ", Port: 45876},
			},
			want: "duplicate system",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := normalizeSystemConfigs(tt.systems)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestSystemConfigIdentityDoesNotUseAmbiguousConcatenation(t *testing.T) {
	left := systemConfigIdentity{Name: "ab", Host: "c", Port: 45876}
	right := systemConfigIdentity{Name: "a", Host: "bc", Port: 45876}
	if left == right {
		t.Fatal("distinct system identities collided")
	}
}
