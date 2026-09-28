package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowquery"
	"github.com/cloudcache/watchdog/internal/flowvpn"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/cloudcache/watchdog/internal/snmpch"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestInstallRequestValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		request installRequest
		valid   bool
	}{
		{name: "valid", request: installRequest{Username: " admin ", Password: "password-123"}, valid: true},
		{name: "missing username", request: installRequest{Password: "password-123"}},
		{name: "short password", request: installRequest{Username: "admin", Password: "short"}},
		{name: "long password", request: installRequest{Username: "admin", Password: strings.Repeat("x", 73)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.request.normalizeAndValidate()
			if (err == nil) != test.valid {
				t.Fatalf("valid=%t error=%v", test.valid, err)
			}
		})
	}
}

// TestFreshInstallLifecycle proves the interactive install contract against a
// disposable real MySQL database. ClickHouse migrations are exercised by the
// deployment smoke test because the production database name is intentionally
// fixed to watchdog_flow.
func TestFreshInstallLifecycle(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_install_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })

	s, err := New(Config{MySQL: MySQLConfig{DSN: dsn}})
	if err != nil {
		t.Fatalf("start fresh server: %v", err)
	}
	defer s.Close()

	var tableCount int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE()`).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 0 {
		t.Fatalf("startup mutated empty database: tables=%d", tableCount)
	}

	status := requestJSON(t, s, http.MethodGet, "/api/v1/install-status", nil, nil)
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"requires_install":true`) {
		t.Fatalf("fresh status: code=%d body=%s", status.Code, status.Body.String())
	}
	health := requestJSON(t, s, http.MethodGet, "/api/v1/health", nil, nil)
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"status":"install_required"`) {
		t.Fatalf("fresh health: code=%d body=%s", health.Code, health.Body.String())
	}
	blockedLogin := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if blockedLogin.Code != http.StatusPreconditionRequired || !strings.Contains(blockedLogin.Body.String(), "install_required") {
		t.Fatalf("login before install: code=%d body=%s", blockedLogin.Code, blockedLogin.Body.String())
	}

	installed := requestJSON(t, s, http.MethodPost, "/api/v1/install", map[string]string{
		"username": "admin", "password": "install-password", "email": "admin@example.test", "display_name": "Administrator",
	}, nil)
	if installed.Code != http.StatusCreated || !strings.Contains(installed.Body.String(), `"installed":true`) || !strings.Contains(installed.Body.String(), `"runtime_ready":true`) {
		t.Fatalf("install: code=%d body=%s", installed.Code, installed.Body.String())
	}

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if login.Code != http.StatusOK || cookieValue(login.Result().Cookies(), sessionCookie) == "" {
		t.Fatalf("login after install: code=%d body=%s", login.Code, login.Body.String())
	}
	repeat := requestJSON(t, s, http.MethodPost, "/api/v1/install", map[string]string{
		"username": "other", "password": "other-password",
	}, nil)
	if repeat.Code != http.StatusConflict || !strings.Contains(repeat.Body.String(), "already_installed") {
		t.Fatalf("repeat install: code=%d body=%s", repeat.Code, repeat.Body.String())
	}
	var users int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&users); err != nil || users != 1 {
		t.Fatalf("administrator cardinality: users=%d error=%v", users, err)
	}
}

// TestInstallAndManagementPlaneSurviveUnavailableClickHouse freezes the
// deployment boundary: MySQL installation, authentication and device APIs do
// not depend on ClickHouse availability. Telemetry health/query surfaces stay
// explicit and fail closed until the dependency is fixed and the server is
// restarted. Opt-in via WATCHDOG_TEST_MYSQL_DSN.
func TestInstallAndManagementPlaneSurviveUnavailableClickHouse(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_install_ch_degraded_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })
	cfg := Config{
		MySQL: MySQLConfig{DSN: dsn},
		ClickHouse: ClickHouseConfig{
			Address: "127.0.0.1:1", Database: "watchdog_flow", Username: "default",
		},
		Address: AddressConfig{ArtifactDir: t.TempDir(), SnapshotDir: t.TempDir()},
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("start fresh server: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = s.Close()
		}
	}()
	installed := requestJSON(t, s, http.MethodPost, "/api/v1/install", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if installed.Code != http.StatusCreated || !strings.Contains(installed.Body.String(), `"runtime_ready":true`) {
		t.Fatalf("install with unavailable ClickHouse: code=%d body=%s", installed.Code, installed.Body.String())
	}

	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login with unavailable ClickHouse: code=%d body=%s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	devices := requestJSON(t, s, http.MethodGet, "/api/v1/devices?limit=1", nil, nil, cookies...)
	if devices.Code != http.StatusOK {
		t.Fatalf("management API with unavailable ClickHouse: code=%d body=%s", devices.Code, devices.Body.String())
	}
	health := requestJSON(t, s, http.MethodGet, "/api/v1/health", nil, nil)
	if health.Code != http.StatusServiceUnavailable ||
		!strings.Contains(health.Body.String(), `"clickhouse":false`) ||
		!strings.Contains(health.Body.String(), `"runtime_ready":true`) ||
		!strings.Contains(health.Body.String(), `"clickhouse_recovery"`) {
		t.Fatalf("degraded health: code=%d body=%s", health.Code, health.Body.String())
	}
	csrf := cookieValue(cookies, csrfCookie)
	flow := requestJSON(t, s, http.MethodPost, "/api/v1/flow/query", nil,
		map[string]string{"X-CSRF-Token": csrf}, cookies...)
	if flow.Code != http.StatusServiceUnavailable || !strings.Contains(flow.Body.String(), "flow_query_unavailable") {
		t.Fatalf("Flow query without ClickHouse: code=%d body=%s", flow.Code, flow.Body.String())
	}
	if s.jobs == nil {
		t.Fatal("management operation job store was not initialized")
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	restarted, err := New(cfg)
	if err != nil {
		t.Fatalf("restart installed server with unavailable ClickHouse: %v", err)
	}
	defer restarted.Close()
	relogin := requestJSON(t, restarted, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if relogin.Code != http.StatusOK {
		t.Fatalf("login after degraded restart: code=%d body=%s", relogin.Code, relogin.Body.String())
	}
}

// TestInstalledRuntimeStartsClickHouseWorkers proves the positive half of the
// same boundary against real MySQL and ClickHouse: once ClickHouse is ready,
// every query/service worker that depends on it is wired to the single shared
// operation-job store. Opt-in to avoid requiring ClickHouse for unit runs.
func TestInstalledRuntimeStartsClickHouseWorkers(t *testing.T) {
	if os.Getenv("WATCHDOG_SERVER_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("WATCHDOG_SERVER_CLICKHOUSE_INTEGRATION is not set")
	}
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Fatal("WATCHDOG_TEST_MYSQL_DSN is required")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_install_ch_ready_it"
	dsn := parsed.FormatDSN()
	dropTestDatabase(t, baseDSN, parsed.DBName)
	t.Cleanup(func() { dropTestDatabase(t, baseDSN, parsed.DBName) })

	root := t.TempDir()
	secret := root + "/clickhouse-password"
	if err := os.WriteFile(secret, []byte(os.Getenv("WATCHDOG_SNMP_CLICKHOUSE_PASSWORD")), 0o600); err != nil {
		t.Fatal(err)
	}
	clickHouseAddress := strings.TrimSpace(os.Getenv("WATCHDOG_TEST_CLICKHOUSE_ADDRESS"))
	if clickHouseAddress == "" {
		clickHouseAddress = "127.0.0.1:9000"
	}
	cfg := Config{
		MySQL: MySQLConfig{DSN: dsn},
		ClickHouse: ClickHouseConfig{
			Address: clickHouseAddress, Database: "watchdog_flow", Username: "default", PasswordFile: secret,
		},
		Address: AddressConfig{ArtifactDir: root + "/address-imports", SnapshotDir: root + "/address-snapshots"},
		Flow:    FlowConfig{Export: FlowExportConfig{Dir: root + "/flow-exports"}},
		SNMP:    SNMPConfig{ExportDir: root + "/snmp-exports"},
		Billing: BillingConfig{ExportDir: root + "/billing-exports"},
	}

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("start fresh server: %v", err)
	}
	deviceID := newID()
	sourceStreamID := "acceptance:" + deviceID
	defer func() {
		cleanupInstalledRuntimeTelemetry(t, s.clickHouse, deviceID, sourceStreamID)
		_ = s.Close()
	}()
	installed := requestJSON(t, s, http.MethodPost, "/api/v1/install", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if installed.Code != http.StatusCreated {
		t.Fatalf("install with ClickHouse: code=%d body=%s", installed.Code, installed.Body.String())
	}
	login := requestJSON(t, s, http.MethodPost, "/api/v1/session/login", map[string]string{
		"username": "admin", "password": "install-password",
	}, nil)
	if login.Code != http.StatusOK {
		t.Fatalf("login with ClickHouse: code=%d body=%s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	csrf := cookieValue(cookies, csrfCookie)
	enrollment := requestJSON(t, s, http.MethodPost, "/api/v1/agents/enrollment-tokens", map[string]any{
		"kind": "snmp", "expires_in_seconds": 300,
	}, map[string]string{"X-CSRF-Token": csrf}, cookies...)
	var enrollmentBody struct {
		Token string `json:"token"`
	}
	decodeJSON(t, enrollment, &enrollmentBody)
	if enrollment.Code != http.StatusCreated || enrollmentBody.Token == "" {
		t.Fatalf("clean-stack enrollment: code=%d body=%s", enrollment.Code, enrollment.Body.String())
	}
	registered := requestJSON(t, s, http.MethodPost, "/api/v1/agents/register", map[string]any{
		"id": "clean_stack_snmp", "enrollment_token": enrollmentBody.Token, "name": "Clean stack SNMP", "kind": "snmp",
		"capabilities": []string{"snmp.poll/v2"},
	}, nil)
	var registeredBody struct {
		Agent struct {
			ID string `json:"id"`
		} `json:"agent"`
		Credential struct {
			Token string `json:"token"`
		} `json:"credential"`
	}
	decodeJSON(t, registered, &registeredBody)
	if registered.Code != http.StatusCreated || registeredBody.Agent.ID != "clean_stack_snmp" || registeredBody.Credential.Token == "" {
		t.Fatalf("clean-stack register: code=%d body=%s", registered.Code, registered.Body.String())
	}
	heartbeat := requestJSON(t, s, http.MethodPost, "/api/v1/agents/clean_stack_snmp/heartbeat", map[string]any{
		"software_version": "clean-stack", "capabilities": []string{"snmp.poll/v2"},
	}, map[string]string{"X-Watchdog-Agent-Token": registeredBody.Credential.Token})
	if heartbeat.Code != http.StatusAccepted {
		t.Fatalf("clean-stack heartbeat: code=%d body=%s", heartbeat.Code, heartbeat.Body.String())
	}
	if s.jobs == nil || s.clickHouse == nil || s.snmpMetrics == nil || s.flowQuery == nil ||
		s.flowExportCancel == nil || s.snmpExportCancel == nil || s.billingService == nil {
		t.Fatalf("incomplete ClickHouse runtime: jobs=%t ch=%t snmp=%t flow=%t flow_export=%t snmp_export=%t billing=%t",
			s.jobs != nil, s.clickHouse != nil, s.snmpMetrics != nil, s.flowQuery != nil,
			s.flowExportCancel != nil, s.snmpExportCancel != nil, s.billingService != nil)
	}
	health := requestJSON(t, s, http.MethodGet, "/api/v1/health", nil, nil)
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"clickhouse":true`) ||
		!strings.Contains(health.Body.String(), `"runtime_ready":true`) {
		t.Fatalf("healthy runtime: code=%d body=%s", health.Code, health.Body.String())
	}

	created := requestJSON(t, s, http.MethodPost, "/api/v1/devices", map[string]any{
		"id": deviceID, "host": "acceptance-router.example", "kind": "network", "display_name": "Acceptance Router",
	}, map[string]string{"X-CSRF-Token": csrf}, cookies...)
	if created.Code != http.StatusCreated || !strings.Contains(created.Body.String(), `"id":"`+deviceID+`"`) {
		t.Fatalf("create acceptance device: code=%d body=%s", created.Code, created.Body.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	bucket := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Minute)
	if err := s.snmpMetrics.WriteSamples(ctx, []snmpch.Sample{{
		ObservedAt: bucket.Add(20 * time.Second), DeviceID: deviceID, AgentID: "clean_stack_snmp",
		EntityKind: "device", EntityID: deviceID, RecipeID: "acceptance-cpu",
		Metric: "watchdog_snmp_device_cpu_percent", ValueKind: "gauge", GaugeValue: 37.5,
		IntervalMS: 60_000, PollSequence: 1, SourceRunID: sourceStreamID + ":snmp", SampleIndex: 0,
	}}); err != nil {
		t.Fatalf("write acceptance SNMP sample: %v", err)
	}
	metricsPath := "/api/v1/metrics/query?device_id=" + deviceID +
		"&metric=watchdog_snmp_device_cpu_percent&time_mode=custom&start=" + bucket.Format(time.RFC3339) +
		"&end=" + bucket.Add(time.Minute).Format(time.RFC3339) + "&step=60&max_data_points=10"
	metrics := requestJSON(t, s, http.MethodGet, metricsPath, nil, nil, cookies...)
	if metrics.Code != http.StatusOK || !strings.Contains(metrics.Body.String(), `"status":"success"`) ||
		!strings.Contains(metrics.Body.String(), `"37.5"`) {
		t.Fatalf("query acceptance SNMP sample: code=%d body=%s", metrics.Code, metrics.Body.String())
	}

	record := flowworker.EnrichedRecord{
		RecordIndex: 0, EventTime: bucket.Add(30 * time.Second), TargetID: deviceID, DeviceID: deviceID,
		ObservationIfIndex: 7, SourceIP: netip.MustParseAddr("10.0.0.1"), DestinationIP: netip.MustParseAddr("203.0.113.8"),
		SourcePort: 49152, DestinationPort: 443, IPProtocol: 6, RawBytes: 123456, RawPackets: 12,
		EstimatedValid: true, EstimatedBytes: 1234560, EstimatedPackets: 120, LocalPort: 49152, RemotePort: 443,
		Dimensions: flowdimension.ClassifiedEndpoints{
			SnapshotID: "acceptance-snapshot", Version: 1, Direction: flowdimension.DirectionOut, Business: "acceptance",
			Local:  flowdimension.EndpointDimension{IP: netip.MustParseAddr("10.0.0.1"), Side: flowdimension.EndpointSrc, PrefixID: "local-acceptance", PrefixCIDR: "10.0.0.0/24"},
			Remote: flowdimension.EndpointDimension{IP: netip.MustParseAddr("203.0.113.8"), Side: flowdimension.EndpointDst, PrefixID: "remote-acceptance", PrefixCIDR: "203.0.113.0/24"},
		},
		RemoteGeo: flowdimension.GeoInfo{
			ContinentID: "Asia", RegionID: "EastAsia", CountryID: "CN", ProvinceID: "310000", CityID: "310100",
			Country: "CN", AdminCode: "310100", City: "Shanghai", Version: "acceptance-geo", Source: flowdimension.GeoSchemaV2,
		},
		RemoteASN: 64500, RemoteASNSource: flowworker.ASNSourceGeoV2, Category: flowdimension.CategoryOnNetCrossProvince,
		SupplierRemoteASNSource: flowworker.ASNSourceUnknown, SupplierCategory: flowdimension.CategoryUnknown,
		Disposition: flowdimension.DispositionCount, ClassificationVersion: 1,
	}
	flowRecord := func(index uint32, direction flowdimension.BusinessDirection, category flowdimension.Category, source, destination, country, region string, rawBytes uint64) flowworker.EnrichedRecord {
		item := record
		item.RecordIndex = index
		item.EventTime = bucket.Add(time.Duration(30+index) * time.Second)
		item.SourceIP = netip.MustParseAddr(source)
		item.DestinationIP = netip.MustParseAddr(destination)
		item.RawBytes, item.RawPackets = rawBytes, max(uint64(1), rawBytes/10_000)
		item.EstimatedBytes, item.EstimatedPackets = rawBytes*10, item.RawPackets*10
		item.Dimensions.Direction = direction
		item.Category = category
		item.RemoteGeo.Country, item.RemoteGeo.CountryID, item.RemoteGeo.RegionID = country, country, region
		item.RemoteGeo.Version = "acceptance-geo"
		item.RemoteASN = 64500 + index
		item.RemotePort = 443 + uint16(index)
		if direction == flowdimension.DirectionIn {
			item.Dimensions.Local = flowdimension.EndpointDimension{IP: item.DestinationIP, Side: flowdimension.EndpointDst, PrefixID: "local-acceptance", PrefixCIDR: "10.0.0.0/24"}
			item.Dimensions.Remote = flowdimension.EndpointDimension{IP: item.SourceIP, Side: flowdimension.EndpointSrc, PrefixID: "remote-acceptance", PrefixCIDR: "203.0.113.0/24"}
			item.SourcePort, item.DestinationPort = item.RemotePort, 49152+uint16(index)
			item.LocalPort = item.DestinationPort
		} else {
			item.Dimensions.Local = flowdimension.EndpointDimension{IP: item.SourceIP, Side: flowdimension.EndpointSrc, PrefixID: "local-acceptance", PrefixCIDR: "10.0.0.0/24"}
			item.Dimensions.Remote = flowdimension.EndpointDimension{IP: item.DestinationIP, Side: flowdimension.EndpointDst, PrefixID: "remote-acceptance", PrefixCIDR: "203.0.113.0/24"}
			item.SourcePort, item.DestinationPort = 49152+uint16(index), item.RemotePort
			item.LocalPort = item.SourcePort
		}
		return item
	}
	records := []flowworker.EnrichedRecord{
		record,
		flowRecord(1, flowdimension.DirectionIn, flowdimension.CategoryOnNetLocalCity, "203.0.113.9", "10.0.0.2", "CN", "EastAsia", 210_000),
		flowRecord(2, flowdimension.DirectionOut, flowdimension.CategoryOnNetCrossCity, "10.0.0.3", "203.0.113.10", "CN", "EastAsia", 320_000),
		flowRecord(3, flowdimension.DirectionIn, flowdimension.CategoryOffNetInProvince, "203.0.113.11", "10.0.0.4", "CN", "EastAsia", 430_000),
		flowRecord(4, flowdimension.DirectionOut, flowdimension.CategoryOffNetCrossProvince, "10.0.0.5", "203.0.113.12", "CN", "EastAsia", 540_000),
		flowRecord(5, flowdimension.DirectionOut, flowdimension.CategoryOverseas, "10.0.0.6", "198.51.100.6", "US", "NorthAmerica", 650_000),
	}
	batch := &flowworker.EnrichedBatch{
		SchemaVersion: flowworker.EnrichedBatchSchemaVersion, MessageDisposition: flowworker.MessageDispositionPersisted,
		SourceStreamID: sourceStreamID, KafkaTopic: "watchdog.flow.raw-v1", KafkaPartition: 0, KafkaOffset: time.Now().UnixNano(),
		CollectorID: "acceptance-flow-collect", ExporterID: "acceptance-exporter", RegistryVersion: 1,
		ReceivedAt: bucket.Add(40 * time.Second), SourceIP: netip.MustParseAddr("192.0.2.1"), Records: records,
	}
	blocks, err := flowch.PrepareBlocks([]*flowworker.EnrichedBatch{batch}, flowch.BatchLimits{})
	if err != nil || len(blocks) != 1 {
		t.Fatalf("prepare acceptance Flow block: blocks=%d err=%v", len(blocks), err)
	}
	if err := s.clickHouse.InsertFlowBlock(ctx, blocks[0]); err != nil {
		t.Fatalf("write acceptance Flow block: %v", err)
	}
	rollup, err := flowch.NewRollupRunner(s.clickHouse)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := rollup.LatestGeneration(ctx, flowch.RollupOneMinute, bucket)
	if err != nil {
		t.Fatalf("read acceptance Flow rollup generation: %v", err)
	}
	if err := rollup.Run(ctx, flowch.RollupRequest{
		Resolution: flowch.RollupOneMinute, Bucket: bucket, Generation: generation + 1, GeneratedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("build acceptance Flow rollup: %v", err)
	}
	flowQuery := requestJSON(t, s, http.MethodPost, "/api/v1/flow/query", map[string]any{
		"from": bucket, "to": bucket.Add(time.Minute), "step_seconds": 60, "value_layer": flowquery.ViewCustomer,
		"parameters": map[string]any{
			"metric": flowquery.MetricRawBytes, "dimension": flowquery.DimensionCategory,
			"filters": map[string]any{"device_ids": []string{deviceID}}, "top_n": 20, "target_points": 5, "timezone": "UTC",
		},
	}, map[string]string{"X-CSRF-Token": csrf}, cookies...)
	if flowQuery.Code != http.StatusOK || !strings.Contains(flowQuery.Body.String(), `"dimension_value":"on_net_cross_province"`) ||
		!strings.Contains(flowQuery.Body.String(), `"value":123456`) {
		t.Fatalf("query acceptance Flow data: code=%d body=%s", flowQuery.Code, flowQuery.Body.String())
	}

	// Materialize one real finding in the same window so the VPN report proves
	// the MySQL findings branch is non-empty while its denominator comes from CH.
	finding := scoredFixture()
	finding.Candidate.WindowStart, finding.Candidate.WindowEnd = bucket, bucket.Add(50*time.Second)
	finding.Candidate.ConversationKey = strings.Repeat("cd", 32)
	finding.Candidate.LocalIP, finding.Candidate.RemoteIP = netip.MustParseAddr("10.0.0.6"), netip.MustParseAddr("198.51.100.6")
	finding.Candidate.PrimaryProtocol, finding.Candidate.PrimaryLocalPort, finding.Candidate.PrimaryRemotePort = 6, 49157, 448
	finding.Candidate.LocalToRemoteBytes, finding.Candidate.RemoteToLocalBytes = 6_500_000, 3_250_000
	finding.Candidate.FlowRecordCount, finding.Candidate.ActiveBucketCount = 1, 1
	finding.Candidate.RemoteASN, finding.Candidate.RemoteCountry = 64505, "US"
	finding.Candidate.LocalPrefixID, finding.Candidate.RemotePrefixID = "local-acceptance", "remote-acceptance"
	finding.Candidate.CompleteRatio = 1
	finding.Candidate.DimensionSnapshotID, finding.Candidate.GeoVersion, finding.Candidate.ClassificationVersion = "acceptance-snapshot", "acceptance-geo", 1
	finding.Score.RuleSetVersion = "acceptance-vpn-v1"
	finding.Score.FamilyHints = []flowvpn.ProtocolFamily{flowvpn.FamilyTrojan}
	finding.GeneratedAt = bucket.Add(2 * time.Minute)
	if n, err := s.materializeVPNFindings(ctx, []flowvpn.ScoredCandidate{finding}); err != nil || n != 1 {
		t.Fatalf("materialize acceptance VPN finding: n=%d err=%v", n, err)
	}

	type reportEnvelope struct {
		Data struct {
			Kind   flowReportKind    `json:"kind"`
			Side   string            `json:"side"`
			Panels []flowReportPanel `json:"panels"`
		} `json:"data"`
	}
	reportRequest := func(kind flowReportKind, side string, metric flowquery.Metric, table map[string]any) map[string]any {
		reportSpec := map[string]any{"schema_version": 1, "kind": kind}
		if side != "" {
			reportSpec["side"] = side
		}
		payload := map[string]any{
			"from": bucket, "to": bucket.Add(time.Minute), "value_layer": flowquery.ViewCustomer,
			"metric": metric, "filters": map[string]any{"device_ids": []string{deviceID}},
			"top_n": 20, "target_points": 5, "timezone": "UTC", "report": reportSpec,
		}
		if kind == flowReportDimensions {
			reportSpec["group_by"] = flowquery.DimensionGeoCountry
		}
		if table != nil {
			payload["table"] = table
		}
		return payload
	}
	panelByID := func(t *testing.T, report reportEnvelope, id string) flowReportPanel {
		t.Helper()
		for _, panel := range report.Data.Panels {
			if panel.ID == id {
				return panel
			}
		}
		t.Fatalf("report %s/%s omitted panel %s", report.Data.Kind, report.Data.Side, id)
		return flowReportPanel{}
	}

	reportCases := []struct {
		name, side, panel string
		kind              flowReportKind
		metric            flowquery.Metric
		table             map[string]any
		want              string
	}{
		{name: "overview", kind: flowReportOverview, metric: flowquery.MetricRawBytes, panel: "category_out", want: `"dimension_value":"on_net_cross_province"`},
		{name: "dimensions", kind: flowReportDimensions, metric: flowquery.MetricRawBytes, panel: "dimension_out", want: `"dimension_value":"US"`},
		{name: "source", kind: flowReportEndpoints, side: "source", metric: flowquery.MetricEstimatedBPS, panel: "endpoint", table: map[string]any{"limit": 2, "offset": 0, "sort_by": "last", "sort_direction": "desc"}, want: `"limit":2`},
		{name: "destination", kind: flowReportEndpoints, side: "destination", metric: flowquery.MetricEstimatedBPS, panel: "endpoint", table: map[string]any{"limit": 2, "offset": 0, "sort_by": "last", "sort_direction": "desc"}, want: `"limit":2`},
		{name: "overseas", kind: flowReportOverseas, metric: flowquery.MetricRawBytes, panel: "country_out", want: `"dimension_value":"US"`},
		{name: "vpn", kind: flowReportVPN, metric: flowquery.MetricEstimatedBytes, panel: "vpn_findings", want: `"finding_count":1`},
	}
	for _, test := range reportCases {
		t.Run("nonempty_"+test.name, func(t *testing.T) {
			response := requestJSON(t, s, http.MethodPost, "/api/v1/flow/reports/query",
				reportRequest(test.kind, test.side, test.metric, test.table), map[string]string{"X-CSRF-Token": csrf}, cookies...)
			if response.Code != http.StatusOK {
				t.Fatalf("report: code=%d body=%s", response.Code, response.Body.String())
			}
			var report reportEnvelope
			if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil {
				t.Fatal(err)
			}
			panel := panelByID(t, report, test.panel)
			if panel.Status != "ready" || !strings.Contains(string(panel.Data), test.want) {
				t.Fatalf("panel %s: status=%s data=%s", panel.ID, panel.Status, panel.Data)
			}
			if test.kind == flowReportEndpoints {
				var data struct {
					Table struct {
						Items  []json.RawMessage `json:"items"`
						Total  int               `json:"total"`
						Limit  uint16            `json:"limit"`
						Offset uint32            `json:"offset"`
					} `json:"table"`
				}
				if err := json.Unmarshal(panel.Data, &data); err != nil {
					t.Fatal(err)
				}
				if data.Table.Total != len(records) || data.Table.Limit != 2 || data.Table.Offset != 0 || len(data.Table.Items) != 2 {
					t.Fatalf("first endpoint page: %+v", data.Table)
				}
				secondPayload := reportRequest(test.kind, test.side, test.metric,
					map[string]any{"limit": 2, "offset": 2, "sort_by": "maximum", "sort_direction": "desc"})
				second := requestJSON(t, s, http.MethodPost, "/api/v1/flow/reports/query", secondPayload,
					map[string]string{"X-CSRF-Token": csrf}, cookies...)
				var secondReport reportEnvelope
				decodeJSON(t, second, &secondReport)
				secondPanel := panelByID(t, secondReport, "endpoint")
				if err := json.Unmarshal(secondPanel.Data, &data); err != nil {
					t.Fatal(err)
				}
				if second.Code != http.StatusOK || data.Table.Total != len(records) || data.Table.Offset != 2 || len(data.Table.Items) != 2 {
					t.Fatalf("second endpoint page: code=%d table=%+v body=%s", second.Code, data.Table, second.Body.String())
				}
			}
		})
	}

	// The same report contract must complete through the asynchronous export
	// worker and produce a downloadable non-empty artifact.
	export := requestJSON(t, s, http.MethodPost, "/api/v1/flow/exports", map[string]any{
		"query": map[string]any{
			"dataset": "flow.traffic", "from": bucket, "to": bucket.Add(time.Minute), "value_layer": flowquery.ViewCustomer,
			"parameters": map[string]any{
				"metric": flowquery.MetricRawBytes, "filters": map[string]any{"device_ids": []string{deviceID}},
				"top_n": 20, "target_points": 5, "timezone": "UTC",
				"report": map[string]any{"schema_version": 1, "kind": "overview"},
			},
		},
		"format": "csv", "idempotency_key": "acceptance-flow-report-export",
	}, map[string]string{"X-CSRF-Token": csrf}, cookies...)
	if export.Code != http.StatusAccepted {
		t.Fatalf("create acceptance Flow export: code=%d body=%s", export.Code, export.Body.String())
	}
	var exportView flowExportResponse
	decodeJSON(t, export, &exportView)
	deadline := time.Now().Add(10 * time.Second)
	for {
		job, err := s.jobs.Get(ctx, exportView.ID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == opjob.StatusSucceeded {
			break
		}
		if job.Status == opjob.StatusFailed || time.Now().After(deadline) {
			t.Fatalf("acceptance Flow export status=%s error=%s", job.Status, job.LastErrorDetail)
		}
		time.Sleep(25 * time.Millisecond)
	}
	download := requestJSON(t, s, http.MethodGet, "/api/v1/flow/exports/"+exportView.ID+"/download", nil, nil, cookies...)
	if download.Code != http.StatusOK || !strings.Contains(download.Body.String(), "on_net_cross_province") || download.Body.Len() < 100 {
		t.Fatalf("download acceptance Flow export: code=%d bytes=%d body=%s", download.Code, download.Body.Len(), download.Body.String())
	}
}

func cleanupInstalledRuntimeTelemetry(t *testing.T, native *flowch.NativeInserter, deviceID, sourceStreamID string) {
	t.Helper()
	if native == nil || deviceID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	queries := []ch.Query{
		{Body: "ALTER TABLE snmp_samples DELETE WHERE device_id={device:String}", Parameters: ch.Parameters(map[string]any{"device": deviceID})},
		{Body: "ALTER TABLE flow_records DELETE WHERE device_id={device:String}", Parameters: ch.Parameters(map[string]any{"device": deviceID})},
		{Body: "ALTER TABLE flow_ingest_receipts DELETE WHERE source_stream_id={source:String}", Parameters: ch.Parameters(map[string]any{"source": sourceStreamID})},
		{Body: "ALTER TABLE flow_aggregate_1m DELETE WHERE device_id={device:String}", Parameters: ch.Parameters(map[string]any{"device": deviceID})},
	}
	for _, query := range queries {
		query.Settings = []ch.Setting{{Key: "mutations_sync", Value: "2"}}
		if err := native.Do(ctx, query); err != nil {
			t.Logf("cleanup acceptance telemetry: %v", err)
		}
	}
}
