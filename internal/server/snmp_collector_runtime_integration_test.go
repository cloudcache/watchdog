package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/snmpdomain"
	"github.com/go-sql-driver/mysql"
)

func TestRealSNMPV2RecipePollWritesClickHouseWithoutVM(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_SNMP_RUNTIME_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("WATCHDOG_SNMP_RUNTIME_TEST_MYSQL_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil || !strings.HasPrefix(parsed.DBName, "watchdog_") {
		t.Fatalf("integration DSN must name a disposable watchdog_* database: %v", err)
	}
	database := parsed.DBName
	parsed.DBName = ""
	admin, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + database + "`")
		_ = admin.Close()
	}()
	if _, err := admin.Exec("DROP DATABASE IF EXISTS `" + database + "`"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec("CREATE DATABASE `" + database + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := ApplyMySQLSchema(ctx, db, schema.MySQL); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO snmp_profiles (id,name,version,security_json) VALUES ('profile-a','fixture','v2c',JSON_OBJECT('community','public'))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO devices (id,host,kind,snmp_profile_id,status) VALUES ('device-a','192.0.2.10','network','profile-a','up')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO snmp_collection_recipes
		(id,device_id,entity_kind,entity_id,module_name,metric,value_kind,oid,numeric_oid,labels_json,options_json,enabled,discovered_at)
		VALUES ('recipe-a','device-a','port','port-a','ports','watchdog_snmp_if_in_octets_total','counter64','IF-MIB::ifHCInOctets.1','1.3.6.1.2.1.31.1.1.1.6.1',JSON_OBJECT(),JSON_OBJECT(),1,UTC_TIMESTAMP(3))`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	chPassword := os.Getenv("WATCHDOG_SNMP_CLICKHOUSE_PASSWORD")
	chDatabase := "watchdog_snmp_runtime_it"
	chAdmin, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{Address: "127.0.0.1:9000", Database: "default", User: "default", Password: chPassword, ClientName: "watchdog-snmp-runtime-it-admin", OperationTimeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = chAdmin.Do(context.Background(), ch.Query{Body: "DROP DATABASE IF EXISTS " + chDatabase})
		chAdmin.Close()
	}()
	_ = chAdmin.Do(ctx, ch.Query{Body: "DROP DATABASE IF EXISTS " + chDatabase})
	if err := chAdmin.Do(ctx, ch.Query{Body: "CREATE DATABASE " + chDatabase}); err != nil {
		t.Fatal(err)
	}
	migrations, err := flowch.LoadMigrations(os.DirFS("../../deploy/migration/clickhouse"), ".")
	if err != nil {
		t.Fatal(err)
	}
	var snmpMigrations []flowch.Migration
	for _, migration := range migrations {
		if migration.Name == "012_snmp_telemetry.sql" || migration.Name == "014_snmp_events.sql" {
			snmpMigrations = append(snmpMigrations, migration)
		}
	}
	if len(snmpMigrations) != 2 {
		t.Fatal("SNMP ClickHouse migrations are missing")
	}
	for _, migration := range snmpMigrations {
		for _, statement := range migration.Statements {
			if err := chAdmin.Do(ctx, ch.Query{Body: strings.ReplaceAll(statement, "watchdog_flow", chDatabase)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	secret := t.TempDir() + "/clickhouse-password"
	if err := os.WriteFile(secret, []byte(chPassword), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		MySQL:      MySQLConfig{DSN: dsn},
		ClickHouse: ClickHouseConfig{Address: "127.0.0.1:9000", Database: chDatabase, Username: "default", PasswordFile: secret},
		SNMP:       SNMPConfig{PollConcurrency: 1},
	}
	runtime, err := openSNMPCollectorRuntime(ctx, cfg, "snmp-agent-a", fixedSNMPQuery{value: 1<<53 + 33})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	result, err := runtime.RunDue(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeviceCount != 1 || result.RecipeCount != 1 || result.SampleCount != 1 || result.FailedCount != 0 {
		t.Fatalf("poll result=%+v", result)
	}
	var count, counter proto.ColUInt64
	err = runtime.clickHouse.Do(ctx, ch.Query{Body: `SELECT count(),max(counter_value) FROM snmp_samples FINAL WHERE device_id='device-a' AND recipe_id='recipe-a'`, Result: proto.Results{{Name: "count()", Data: &count}, {Name: "max(counter_value)", Data: &counter}}})
	if err != nil {
		t.Fatal(err)
	}
	if count.Rows() != 1 || count[0] != 1 || counter[0] != 1<<53+33 {
		t.Fatalf("ClickHouse count=%v counter=%v", count, counter)
	}
	var polled sql.NullTime
	if err := runtime.db.QueryRowContext(ctx, "SELECT last_polled_at FROM snmp_collection_recipes WHERE id='recipe-a'").Scan(&polled); err != nil || !polled.Valid {
		t.Fatalf("recipe poll state=%v err=%v", polled, err)
	}
	var tenantColumns int
	if err := runtime.db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='snmp_collection_recipes' AND column_name='tenant_id'`).Scan(&tenantColumns); err != nil || tenantColumns != 0 {
		t.Fatalf("tenant columns=%d err=%v", tenantColumns, err)
	}
}

type fixedSNMPQuery struct{ value uint64 }

func (q fixedSNMPQuery) Get(_ context.Context, request snmpdomain.GetRequest) (snmpdomain.QueryResponse, error) {
	if len(request.OIDs) != 1 {
		return snmpdomain.QueryResponse{}, fmt.Errorf("unexpected OIDs: %v", request.OIDs)
	}
	return snmpdomain.QueryResponse{VarBinds: []snmpdomain.VarBind{{OID: request.OIDs[0], Value: q.value, ValueType: snmpdomain.ValueCounter64}}}, nil
}

func (q fixedSNMPQuery) Walk(context.Context, snmpdomain.WalkRequest) (snmpdomain.QueryResponse, error) {
	return snmpdomain.QueryResponse{}, errors.New("unexpected walk")
}
