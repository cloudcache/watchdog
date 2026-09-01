package watchdog

import (
	"testing"
	"time"
)

func TestScanSNMPProfileSetsTimeout(t *testing.T) {
	now := time.Date(2026, 6, 17, 1, 2, 3, 0, time.UTC)
	row := fakeRow{values: []any{
		ID("profile_01"),
		ID("tenant_01"),
		"core-v3",
		"v3",
		[]byte(`{"auth_protocol":"SHA","username":"noc"}`),
		uint32(4500),
		uint8(3),
		now,
		now,
	}}

	profile, err := scanSNMPProfile(row)
	if err != nil {
		t.Fatalf("scanSNMPProfile() error = %v", err)
	}
	if profile.Timeout != 4500*time.Millisecond {
		t.Fatalf("Timeout = %v, want 4500ms", profile.Timeout)
	}
	if profile.Security["username"] != "noc" {
		t.Fatalf("Security username = %q, want noc", profile.Security["username"])
	}
	if profile.Retries != 3 {
		t.Fatalf("Retries = %d, want 3", profile.Retries)
	}
}

func TestScanMIBModule(t *testing.T) {
	now := time.Date(2026, 6, 17, 1, 2, 3, 0, time.UTC)
	row := fakeRow{values: []any{
		ID("mib_01"),
		"IF-MIB",
		"librenms",
		"2026.06",
		"sha256:a",
		true,
		now,
		now,
	}}
	module, err := scanMIBModule(row)
	if err != nil {
		t.Fatalf("scanMIBModule() error = %v", err)
	}
	if module.Name != "IF-MIB" || module.Source != "librenms" || !module.Enabled {
		t.Fatalf("module = %#v", module)
	}
}
