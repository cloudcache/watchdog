package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/watchdog"
	_ "github.com/go-sql-driver/mysql"
)

// TestRealSNMPDiscovery is an opt-in hardware test. Credentials are loaded
// directly from a local fixture database and are never printed or copied into
// source/test output. Persistence is covered independently by
// TestDeviceAndAgentAPI against a fresh v2 schema.
func TestRealSNMPDiscovery(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_TEST_SNMP_FIXTURE_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_TEST_SNMP_FIXTURE_DSN is not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	host := os.Getenv("WATCHDOG_TEST_SNMP_HOST")
	query := `SELECT nd.id,t.host,p.id,p.name,p.version,p.security_json,p.timeout_ms,p.retries,nd.snmp_port,
		COALESCE(nd.snmp_security_json,JSON_OBJECT())
		FROM network_devices nd JOIN targets t ON t.id=nd.target_id JOIN snmp_profiles p ON p.id=nd.snmp_profile_id`
	args := []any{}
	if host != "" {
		query += " WHERE t.host=?"
		args = append(args, host)
	}
	query += " ORDER BY nd.id LIMIT 1"
	var deviceID, targetHost, profileID, profileName, version string
	var profileRaw, overrideRaw json.RawMessage
	var timeoutMS uint32
	var retries uint8
	var port uint16
	if err := db.QueryRow(query, args...).Scan(&deviceID, &targetHost, &profileID, &profileName, &version, &profileRaw, &timeoutMS, &retries, &port, &overrideRaw); err != nil {
		t.Fatal(err)
	}
	security := map[string]string{}
	if err := json.Unmarshal(profileRaw, &security); err != nil {
		t.Fatal(err)
	}
	var overrides map[string]string
	if err := json.Unmarshal(overrideRaw, &overrides); err != nil {
		t.Fatal(err)
	}
	for key, value := range overrides {
		if value != "" {
			security[key] = value
		}
	}
	runner, err := newSNMPDiscoveryRunner(SNMPConfig{
		DefinitionsDir:     os.Getenv("WATCHDOG_TEST_LIBRENMS_DIR"),
		DefinitionsVersion: "hardware-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	result, err := runner.Discover(ctx, watchdog.SNMPDiscoveryEngineRequest{
		TargetID: watchdog.ID(deviceID),
		Target:   watchdog.SNMPCollectorTarget{Host: targetHost, Port: port},
		Device:   watchdog.NetworkDevice{ID: watchdog.ID(deviceID), TargetID: watchdog.ID(deviceID)},
		Profile: watchdog.SNMPProfile{
			ID: watchdog.ID(profileID), Name: profileName, Version: watchdog.SNMPVersion(version),
			Security: security, Timeout: time.Duration(timeoutMS) * time.Millisecond, Retries: retries,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.DeviceUpdates.SysObjectID == "" || result.DeviceUpdates.SysDescr == "" {
		t.Fatalf("incomplete core fingerprint: sysObjectID=%q sysDescr=%q", result.DeviceUpdates.SysObjectID, result.DeviceUpdates.SysDescr)
	}
	if len(result.Ports) == 0 {
		t.Fatal("real device returned no IF-MIB ports")
	}
}
