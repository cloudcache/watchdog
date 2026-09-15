package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type trapCaptureExecutor struct{ inserts int }

func (e *trapCaptureExecutor) Do(_ context.Context, query ch.Query) error {
	if query.Input != nil {
		e.inserts++
	}
	return nil
}

func TestSNMPManagementPolicyMIBAndTrapLifecycle(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.DBName = "watchdog_snmp_management_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	if err := ensureDatabase(dsn); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}
	userID, deviceID, portID := newID(), newID(), newID()
	if _, err := db.Exec(`INSERT INTO users (id,username,status) VALUES (?,?,'active')`, userID, "snmp-management-admin"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO devices (id,host,kind,sys_name) VALUES (?,?,'network',?)`, deviceID, "192.0.2.9", "edge-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ports (id,device_id,if_index,if_name,if_oper_status,metadata_json) VALUES (?,?,7,'xe-0/0/7','up',JSON_OBJECT('side_type','provider'))`, portID, deviceID); err != nil {
		t.Fatal(err)
	}
	executor := &trapCaptureExecutor{}
	store, _ := snmpch.New(executor)
	s := &Server{db: db, snmpMetrics: store}
	principal := &principal{UserID: userID, IsAdmin: true}
	if err := s.ensureBuiltinMIBModules(ctx); err != nil {
		t.Fatal(err)
	}
	var builtinMIBID string
	if err := db.QueryRow(`SELECT id FROM mib_modules WHERE name='IF-MIB' AND source='watchdog-builtin'`).Scan(&builtinMIBID); err != nil {
		t.Fatalf("built-in IF-MIB was not seeded: %v", err)
	}
	builtinList := snmpManagementRequest(t, s.listMIBModules, principal, http.MethodGet, "/api/v1/snmp/mib-modules", "", "", nil)
	if builtinList.Code != http.StatusOK || !strings.Contains(builtinList.Body.String(), `"Source":"watchdog-builtin"`) || !strings.Contains(builtinList.Body.String(), `"Builtin":true`) {
		t.Fatalf("built-in MIB inventory: status=%d body=%s", builtinList.Code, builtinList.Body.String())
	}
	protectedDelete := snmpManagementRequest(t, s.deleteMIBModule, principal, http.MethodDelete, "/api/v1/snmp/mib-modules/"+builtinMIBID, "module_id", builtinMIBID, nil)
	if protectedDelete.Code != http.StatusConflict {
		t.Fatalf("built-in MIB delete: status=%d body=%s", protectedDelete.Code, protectedDelete.Body.String())
	}
	if _, err := db.Exec(`UPDATE mib_modules SET enabled=0,version='stale',checksum='stale' WHERE id=?`, builtinMIBID); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureBuiltinMIBModules(ctx); err != nil {
		t.Fatal(err)
	}
	var builtinEnabled bool
	var builtinVersion, builtinChecksum string
	if err := db.QueryRow(`SELECT enabled,version,checksum FROM mib_modules WHERE id=?`, builtinMIBID).Scan(&builtinEnabled, &builtinVersion, &builtinChecksum); err != nil {
		t.Fatal(err)
	}
	if builtinEnabled || builtinVersion == "stale" || builtinChecksum == "stale" {
		t.Fatalf("built-in MIB reseed overwrote admin state or missed content: enabled=%t version=%q checksum=%q", builtinEnabled, builtinVersion, builtinChecksum)
	}

	defaultPolicy := snmpManagementRequest(t, s.getPortPolicy, principal, http.MethodGet, "/api/v1/network/ports/"+portID+"/policy", "port_id", portID, nil)
	if defaultPolicy.Code != http.StatusOK || !strings.Contains(defaultPolicy.Body.String(), `"SideType":"provider"`) || !strings.Contains(defaultPolicy.Body.String(), `"BillingBaseBps":1073741824`) {
		t.Fatalf("default policy: status=%d body=%s", defaultPolicy.Code, defaultPolicy.Body.String())
	}
	updatedDefaults := snmpManagementRequest(t, s.putTrafficPolicyDefaults, principal, http.MethodPut, "/api/v1/network/traffic-policy-defaults", "", "", map[string]any{
		"Provider": map[string]any{"BillingBaseBps": 2_000_000_000, "SampleStep": int64(5 * time.Minute), "CorrectionDirection": "none"},
		"Customer": map[string]any{"BillingBaseBps": 1_000_000_000, "SampleStep": int64(time.Minute), "CorrectionDirection": "none"},
	})
	if updatedDefaults.Code != http.StatusOK {
		t.Fatalf("update defaults: status=%d body=%s", updatedDefaults.Code, updatedDefaults.Body.String())
	}
	defaultPolicy = snmpManagementRequest(t, s.getPortPolicy, principal, http.MethodGet, "/api/v1/network/ports/"+portID+"/policy", "port_id", portID, nil)
	if !strings.Contains(defaultPolicy.Body.String(), `"BillingBaseBps":2000000000`) {
		t.Fatalf("admin default was not inherited: %s", defaultPolicy.Body.String())
	}
	override := snmpManagementRequest(t, s.patchPortPolicy, principal, http.MethodPatch, "/api/v1/network/ports/"+portID+"/policy", "port_id", portID, map[string]any{
		"SideType": "customer", "BillingBaseBps": 1_000_000_000, "SampleStep": int64(time.Minute),
		"CorrectionDirection": "up", "CorrectionMin": 10, "CorrectionMax": 10, "Enabled": true,
	})
	if override.Code != http.StatusOK || !strings.Contains(override.Body.String(), `"SideType":"customer"`) || !strings.Contains(override.Body.String(), `"CorrectionMin":10`) {
		t.Fatalf("port override: status=%d body=%s", override.Code, override.Body.String())
	}

	putMIB := snmpManagementRequest(t, s.putMIBModule, principal, http.MethodPut, "/api/v1/snmp/mib-modules", "", "", map[string]any{
		"Name": "IF-MIB", "Source": "librenms", "Version": "2026.09", "Checksum": "sha256:test", "Enabled": true,
	})
	if putMIB.Code != http.StatusOK || !strings.Contains(putMIB.Body.String(), `"Name":"IF-MIB"`) {
		t.Fatalf("put MIB: status=%d body=%s", putMIB.Code, putMIB.Body.String())
	}
	var mib watchdog.MIBModule
	if err := json.Unmarshal(putMIB.Body.Bytes(), &mib); err != nil || mib.ID == "" {
		t.Fatalf("decode MIB: value=%+v err=%v", mib, err)
	}
	listMIB := snmpManagementRequest(t, s.listMIBModules, principal, http.MethodGet, "/api/v1/snmp/mib-modules", "", "", nil)
	if listMIB.Code != http.StatusOK || !strings.Contains(listMIB.Body.String(), `"IF-MIB"`) {
		t.Fatalf("list MIB: status=%d body=%s", listMIB.Code, listMIB.Body.String())
	}

	trap := snmpManagementRequest(t, s.receiveSNMPTrap, principal, http.MethodPost, "/api/v1/snmp/traps", "", "", map[string]any{
		"source_ip": "192.0.2.9:162", "trap_oid": watchdog.SNMPTrapOIDLinkDown,
		"varbinds": []map[string]string{{"oid": "IF-MIB::ifIndex", "value": "7"}},
	})
	if trap.Code != http.StatusOK || !strings.Contains(trap.Body.String(), `"port_updates":1`) || executor.inserts != 1 {
		t.Fatalf("trap: status=%d body=%s inserts=%d", trap.Code, trap.Body.String(), executor.inserts)
	}
	var operStatus string
	if err := db.QueryRow(`SELECT if_oper_status FROM ports WHERE id=?`, portID).Scan(&operStatus); err != nil || operStatus != "down" {
		t.Fatalf("trap did not update port: status=%q err=%v", operStatus, err)
	}

	trapAgentID, trapToken := "snmp_trap_agent_test", "wda_snmp_trap_test_secret"
	if _, err := db.Exec(`INSERT INTO agents (id,name,kind,status,health,api_version,capabilities_json)
		VALUES (?,?,'snmp','active','ok','v1',JSON_ARRAY('snmp.poll/v2'))`, trapAgentID, "SNMP trap listener"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO agent_credentials (id,agent_id,auth_type,token_sha256) VALUES (?,?,'token',?)`,
		newID(), trapAgentID, sha256hex(trapToken)); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/api/v1/snmp/traps", s.authenticateSNMPTrapCaller, s.receiveSNMPTrap)
	machineBody, _ := json.Marshal(map[string]any{
		"source_ip": "192.0.2.9", "trap_oid": watchdog.SNMPTrapOIDLinkUp,
		"varbinds": []map[string]string{{"oid": "IF-MIB::ifIndex", "value": "7"}},
	})
	machineRequest := httptest.NewRequest(http.MethodPost, "/api/v1/snmp/traps", bytes.NewReader(machineBody))
	machineRequest.Header.Set("Content-Type", "application/json")
	machineRequest.Header.Set("Authorization", "Bearer "+trapToken)
	machineTrap := httptest.NewRecorder()
	router.ServeHTTP(machineTrap, machineRequest)
	if machineTrap.Code != http.StatusOK || !strings.Contains(machineTrap.Body.String(), `"port_updates":1`) {
		t.Fatalf("machine trap auth: status=%d body=%s", machineTrap.Code, machineTrap.Body.String())
	}
	if err := db.QueryRow(`SELECT if_oper_status FROM ports WHERE id=?`, portID).Scan(&operStatus); err != nil || operStatus != "up" {
		t.Fatalf("machine trap did not update port: status=%q err=%v", operStatus, err)
	}

	deletedMIB := snmpManagementRequest(t, s.deleteMIBModule, principal, http.MethodDelete, "/api/v1/snmp/mib-modules/"+string(mib.ID), "module_id", string(mib.ID), nil)
	if deletedMIB.Code != http.StatusOK {
		t.Fatalf("delete MIB: status=%d body=%s", deletedMIB.Code, deletedMIB.Body.String())
	}
}

func snmpManagementRequest(t *testing.T, handler gin.HandlerFunc, principal *principal, method, target, paramName, paramValue string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, bytes.NewReader(encoded))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(principalKey, principal)
	if paramName != "" {
		c.Params = gin.Params{{Key: paramName, Value: paramValue}}
	}
	handler(c)
	return recorder
}
