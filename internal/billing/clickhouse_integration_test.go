// SPDX-License-Identifier: AGPL-3.0-only

package billing_test

import (
	"context"
	"database/sql"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/cloudcache/watchdog/internal/server"
	"github.com/cloudcache/watchdog/internal/snmpch"
	_ "github.com/go-sql-driver/mysql"
)

func TestRealClickHouseFlowAndSNMPBillingPeriod(t *testing.T) {
	if os.Getenv("WATCHDOG_BILLING_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_BILLING_CLICKHOUSE_INTEGRATION=1 to run")
	}
	dsn := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	passwordFile := os.Getenv("WATCHDOG_CLICKHOUSE_PASSWORD_FILE")
	if dsn == "" || passwordFile == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN and WATCHDOG_CLICKHOUSE_PASSWORD_FILE are required")
	}
	passwordBytes, err := os.ReadFile(passwordFile)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const chDatabase = "watchdog_billing_it"
	admin, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{
		Address: "127.0.0.1:9000", Database: "default", User: "default", Password: strings.TrimSpace(string(passwordBytes)),
		ClientName: "watchdog-billing-it-admin", OperationTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	_ = admin.Do(ctx, ch.Query{Body: "DROP DATABASE IF EXISTS " + chDatabase})
	if err := admin.Do(ctx, ch.Query{Body: "CREATE DATABASE " + chDatabase}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		_ = admin.Do(cleanup, ch.Query{Body: "DROP DATABASE IF EXISTS " + chDatabase})
	})
	migrations, err := flowch.LoadMigrations(os.DirFS("../../deploy/migration/clickhouse"), ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		for index, statement := range migration.Statements {
			statement = strings.ReplaceAll(statement, "watchdog_flow", chDatabase)
			if err := admin.Do(ctx, ch.Query{Body: statement, Settings: []ch.Setting{{Key: "async_insert", Value: "0"}, {Key: "wait_for_async_insert", Value: "1"}}}); err != nil {
				t.Fatalf("apply ClickHouse migration %d statement %d: %v", migration.Version, index+1, err)
			}
		}
	}
	native, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{
		Address: "127.0.0.1:9000", Database: chDatabase, User: "default", Password: strings.TrimSpace(string(passwordBytes)),
		ClientName: "watchdog-billing-it", OperationTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	snmpStore, err := snmpch.New(native)
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := server.ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}
	userID, deviceID, portID := billing.NewID(), billing.NewID(), billing.NewID()
	if _, err := db.ExecContext(ctx, `INSERT INTO users (id,username,status) VALUES (?,?,'active')`, userID, "kiss07-ch-"+userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO devices (id,host) VALUES (?,?)`, deviceID, "kiss07-ch-"+deviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO ports (id,device_id,if_index,if_name) VALUES (?,?,7,'xe-0/0/7')`, portID, deviceID); err != nil {
		t.Fatal(err)
	}
	var party billing.Party
	var account billing.Account
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM reconciliation_issues WHERE run_id IN (SELECT id FROM reconciliation_runs WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?))`, account.ID)
		_, _ = db.Exec(`DELETE FROM reconciliation_runs WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_adjustments WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_period_values WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_period_ports WHERE period_id IN (SELECT id FROM billing_periods WHERE account_id=?)`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_periods WHERE account_id=?`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_account_ports WHERE account_id=?`, account.ID)
		_, _ = db.Exec(`DELETE FROM billing_accounts WHERE id=?`, account.ID)
		_, _ = db.Exec(`DELETE FROM parties WHERE id=?`, party.ID)
		_, _ = db.Exec(`DELETE FROM audit_logs WHERE actor_id=?`, userID)
		_, _ = db.Exec(`DELETE FROM users WHERE id=?`, userID)
		_, _ = db.Exec(`DELETE FROM devices WHERE id=?`, deviceID)
	})

	from := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	for minute := -1; minute <= 10; minute++ {
		observed := from.Add(time.Duration(minute) * time.Minute)
		for direction, metric := range []string{snmpch.MetricIfInOctets, snmpch.MetricIfOutOctets} {
			value := uint64(0)
			if minute >= 0 {
				// The pre-period interval is deliberately huge. Correct [from,to)
				// rollup excludes (-1m,0] and includes the sample at to.
				value = uint64(6_000_000 + minute*6000)
			}
			if err := snmpStore.WriteSamples(ctx, []snmpch.Sample{{
				ObservedAt: observed, DeviceID: deviceID, EntityKind: "port", EntityID: portID, RecipeID: metric,
				Metric: metric, ValueKind: "counter", CounterValue: value, CounterWidth: 64, IntervalMS: 60_000,
				PollSequence: uint64((minute+1)*2 + direction + 1), SourceRunID: "billing-it", SampleIndex: 0,
			}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for index := 0; index < 2; index++ {
		if err := snmpStore.RebuildClosedInterfaceBucket(ctx, from.Add(time.Duration(index)*5*time.Minute), from.Add(time.Hour), 1); err != nil {
			t.Fatal(err)
		}
	}
	records := make([]flowworker.EnrichedRecord, 0, 2)
	for index := 0; index < 2; index++ {
		records = append(records, flowworker.EnrichedRecord{
			RecordIndex: uint32(index + 1), EventTime: from.Add(time.Duration(index*5+1) * time.Minute), TargetID: "target-it", DeviceID: deviceID,
			InIf: 7, OutIf: 8, SourceIP: netip.MustParseAddr("192.0.2.1"), DestinationIP: netip.MustParseAddr("198.51.100.1"),
			RawBytes: 3000, RawPackets: 3, SamplingMode: 1, SamplingRate: 10, EstimatedValid: true, EstimatedBytes: 30000, EstimatedPackets: 30,
			Dimensions: flowdimension.ClassifiedEndpoints{SnapshotID: "dimension-it", Direction: flowdimension.DirectionOut,
				Local: flowdimension.EndpointDimension{IP: netip.MustParseAddr("192.0.2.1")}, Remote: flowdimension.EndpointDimension{IP: netip.MustParseAddr("198.51.100.1")}},
			RemoteGeo:               flowdimension.GeoInfo{Country: "CN", Version: "geo-it", Source: flowdimension.GeoSchemaV2},
			RemoteASNSource:         flowworker.ASNSourceUnknown,
			Category:                flowdimension.CategoryOnNetLocalCity,
			SupplierRemoteASNSource: flowworker.ASNSourceUnknown,
			SupplierCategory:        flowdimension.CategoryUnknown,
			Disposition:             flowdimension.DispositionCount,
			ClassificationVersion:   3,
		})
	}
	batch := &flowworker.EnrichedBatch{
		SchemaVersion: flowworker.EnrichedBatchSchemaVersion, MessageDisposition: flowworker.MessageDispositionPersisted,
		SourceStreamID: "billing-it:raw-v1:1", KafkaTopic: "watchdog.flow.raw-v1", KafkaPartition: 1, KafkaOffset: 1,
		CollectorID: "collector-it", ExporterID: "exporter-it", ReceivedAt: from.Add(15 * time.Minute), SourceIP: netip.MustParseAddr("203.0.113.1"), Records: records,
	}
	blocks, err := flowch.PrepareBlocks([]*flowworker.EnrichedBatch{batch}, flowch.BatchLimits{})
	if err != nil || len(blocks) != 1 {
		t.Fatalf("prepare Flow billing fixture: blocks=%d err=%v", len(blocks), err)
	}
	if err := native.InsertFlowBlock(ctx, blocks[0]); err != nil {
		t.Fatal(err)
	}

	store := billing.NewStore(db)
	party, err = store.CreateParty(ctx, billing.Party{Kind: "customer", Status: "active", Name: "CH customer " + userID}, userID)
	if err != nil {
		t.Fatal(err)
	}
	cdr := uint64(1000)
	account, err = store.CreateAccount(ctx, billing.Account{
		PartyID: party.ID, Name: "CH account " + userID, Status: "active", BillType: "cdr", Algorithm: billing.Algorithm95th,
		BillingDay: 1, Timezone: "UTC", Direction: billing.DirectionIn, DefaultLayer: billing.LayerCustomer, CDRBPS: &cdr,
	}, userID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceAccountPorts(ctx, account.ID, []billing.AccountPort{{PortID: portID, Direction: billing.DirectionIn}}, account.RowVersion, userID); err != nil {
		t.Fatal(err)
	}
	account, _ = store.GetAccount(ctx, account.ID)
	period, err := store.CreatePeriod(ctx, account.ID, from, from.Add(10*time.Minute), from.Add(time.Hour), userID)
	if err != nil {
		t.Fatal(err)
	}
	service, err := billing.NewService(store, snmpStore, native)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.CalculateForOperation(ctx, period.ID, period.RowVersion, userID, billing.NewID())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Values) != 4 {
		t.Fatalf("values=%+v", result.Values)
	}
	for _, value := range result.Values {
		if value.AlgorithmValue != 800 || value.SelectedBytes != 60000 || value.Coverage != 1 || value.ExpectedBuckets != 2 || value.ObservedBuckets != 2 {
			t.Fatalf("layer %s=%+v", value.Layer, value)
		}
	}
	if len(result.Issues) != 0 {
		t.Fatalf("equal SNMP/Flow facts created issues: %+v", result.Issues)
	}
	repeat, err := service.CalculateForOperation(ctx, period.ID, result.Period.RowVersion, userID, billing.NewID())
	if err != nil || !sameLayerValues(result.Values, repeat.Values) {
		t.Fatalf("repeat=%+v err=%v", repeat.Values, err)
	}
}
