package watchdog

import (
	"context"
	"testing"
	"time"

	"github.com/gosnmp/gosnmp"
)

func TestNewGoSNMPSessionVersions(t *testing.T) {
	tests := []struct {
		name, version, community string
		wantVersion              gosnmp.SnmpVersion
	}{
		{name: "v1", version: "1", community: "v1-secret", wantVersion: gosnmp.Version1},
		{name: "v2c", version: "2c", community: "v2-secret", wantVersion: gosnmp.Version2c},
		{name: "v2c default", community: "public", wantVersion: gosnmp.Version2c},
		{name: "v3", version: "3", wantVersion: gosnmp.Version3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			security := map[string]string{"community": tt.community, "username": "operator", "security_level": "noAuthNoPriv"}
			session, err := newGoSNMPSession(context.Background(), SNMPCollectorTarget{Host: "[2001:db8::1]:1161"}, SNMPProfile{
				Version: SNMPVersion(tt.version), Security: security, Timeout: 7 * time.Second, Retries: 3,
			}, "vrf-blue", SNMPCollectorQueryFlags{MaxOids: 19, MaxRepetitions: 11})
			if err != nil {
				t.Fatal(err)
			}
			if session.Target != "2001:db8::1" || session.Port != 1161 || session.Version != tt.wantVersion {
				t.Fatalf("session target=%s port=%d version=%v", session.Target, session.Port, session.Version)
			}
			if session.Timeout != 7*time.Second || session.Retries != 3 || session.MaxOids != 19 || session.MaxRepetitions != 11 || session.ContextName != "vrf-blue" {
				t.Fatalf("session options were not preserved: %#v", session)
			}
			if tt.wantVersion != gosnmp.Version3 && session.Community != tt.community {
				t.Fatalf("community=%q, want %q", session.Community, tt.community)
			}
		})
	}
}

func TestNewGoSNMPSessionRejectsUnsupportedVersion(t *testing.T) {
	if _, err := newGoSNMPSession(context.Background(), SNMPCollectorTarget{Host: "192.0.2.1"}, SNMPProfile{Version: "4"}, "", SNMPCollectorQueryFlags{}); err == nil {
		t.Fatal("expected unsupported version error")
	}
}
