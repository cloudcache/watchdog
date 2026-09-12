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
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/deploy/schema"
	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/gin-gonic/gin"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type aggregateGraphExecutor struct{}

func (aggregateGraphExecutor) Do(ctx context.Context, query ch.Query) error {
	result := query.Result.(proto.Results)
	result[0].Data.(*proto.ColDateTime).Append(time.Date(2026, 9, 10, 1, 0, 0, 0, time.UTC))
	result[1].Data.(*proto.ColStr).Append("device-a")
	result[2].Data.(*proto.ColLowCardinality[string]).Append("port")
	result[3].Data.(*proto.ColStr).Append("port-a")
	result[4].Data.(*proto.ColFloat64).Append(8_000)
	return query.OnResult(ctx, proto.Block{Rows: 1})
}

func TestAggregateGraphMySQLAPI(t *testing.T) {
	baseDSN := os.Getenv("WATCHDOG_TEST_MYSQL_DSN")
	if baseDSN == "" {
		t.Skip("WATCHDOG_TEST_MYSQL_DSN is not set")
	}
	parsed, err := mysqldriver.ParseDSN(baseDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	parsed.DBName = "watchdog_snmp_aggregate_it"
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
	userID, deviceID, portID, graphID := newID(), newID(), newID(), "snmp-aggregate-integration"
	cleanup := func() {
		_, _ = db.Exec("DELETE FROM aggregate_graphs WHERE id=?", graphID)
		_, _ = db.Exec("DELETE FROM devices WHERE id=?", deviceID)
		_, _ = db.Exec("DELETE FROM users WHERE id=?", userID)
	}
	cleanup()
	t.Cleanup(cleanup)
	if _, err := db.Exec("INSERT INTO users (id,username,status) VALUES (?,?,'active')", userID, "aggregate-"+userID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO devices (id,host,kind) VALUES (?,?,'network')", deviceID, "aggregate-"+deviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ports (id,device_id,if_index,if_name,metadata_json) VALUES (?,?,1,'xe-0/0/0',JSON_OBJECT('side_type','provider'))`, portID, deviceID); err != nil {
		t.Fatal(err)
	}
	store, err := snmpch.New(aggregateGraphExecutor{})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{db: db, snmpMetrics: store}
	p := &principal{UserID: userID, IsAdmin: true}

	created := aggregateGraphRequest(t, s.createAggregateGraph, p, http.MethodPost, "/api/v1/aggregate-graphs", "", map[string]any{
		"id": graphID, "Name": "Cross-device traffic", "Aggregation": "sum", "ValueMode": "both", "Unit": "bps",
	})
	if created.Code != http.StatusCreated {
		t.Fatalf("create: status=%d body=%s", created.Code, created.Body.String())
	}
	items := aggregateGraphRequest(t, s.replaceAggregateGraphItems, p, http.MethodPut, "/api/v1/aggregate-graphs/"+graphID+"/items", graphID, map[string]any{
		"items": []map[string]any{{"id": "item-in", "metric": snmpch.MetricIfInBPS, "direction": "in", "label": "In", "total": true}},
	})
	ports := aggregateGraphRequest(t, s.replaceAggregateGraphPorts, p, http.MethodPut, "/api/v1/aggregate-graphs/"+graphID+"/ports", graphID, map[string]any{
		"ports": []map[string]any{{"PortID": portID}},
	})
	if items.Code != http.StatusOK || ports.Code != http.StatusOK {
		t.Fatalf("members: items=%d/%s ports=%d/%s", items.Code, items.Body.String(), ports.Code, ports.Body.String())
	}
	listed := aggregateGraphRequest(t, s.listAggregateGraphs, p, http.MethodGet, "/api/v1/aggregate-graphs?q=Cross-device", "", nil)
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), graphID) || !strings.Contains(listed.Body.String(), deviceID) {
		t.Fatalf("list: status=%d body=%s", listed.Code, listed.Body.String())
	}
	series := aggregateGraphRequest(t, s.aggregateGraphSeries, p, http.MethodGet,
		"/api/v1/aggregate-graphs/"+graphID+"/series?time_mode=fixed&window=1h&max_data_points=10&value_mode=both&split_side_type=true", graphID, nil)
	if series.Code != http.StatusOK || !strings.Contains(series.Body.String(), `"8000"`) ||
		!strings.Contains(series.Body.String(), `"value_mode":"corrected"`) ||
		!strings.Contains(series.Body.String(), `"value_mode":"raw"`) ||
		!strings.Contains(series.Body.String(), `"side_type":"provider"`) {
		t.Fatalf("series: status=%d body=%s", series.Code, series.Body.String())
	}
	viewer := &principal{UserID: userID, Abilities: map[string]bool{"device.viewAll": true}}
	data := aggregateGraphRequest(t, s.aggregateGraphData, viewer, http.MethodGet,
		"/api/v1/aggregate-graphs/"+graphID+"/data?start=2026-09-10T00:00:00Z&end=2026-09-11T00:00:00Z", graphID, nil)
	if data.Code != http.StatusOK || !strings.Contains(data.Body.String(), `"GraphID":"`+graphID+`"`) ||
		!strings.Contains(data.Body.String(), `"ItemID":"item-in"`) || !strings.Contains(data.Body.String(), `"Value":8000`) {
		t.Fatalf("data contract: status=%d body=%s", data.Code, data.Body.String())
	}
	if strings.Count(data.Body.String(), `"GraphID":"`+graphID+`"`) != 1 {
		t.Fatalf("data contract duplicated raw/corrected rows: body=%s", data.Body.String())
	}
	summary := aggregateGraphRequest(t, s.aggregateGraphSummary, viewer, http.MethodGet,
		"/api/v1/aggregate-graphs/"+graphID+"/summary?start=2026-09-10T00:00:00Z&end=2026-09-11T00:00:00Z", graphID, nil)
	if summary.Code != http.StatusOK || !strings.Contains(summary.Body.String(), `"samples":1`) ||
		!strings.Contains(summary.Body.String(), `"total_bytes":144000`) {
		t.Fatalf("summary contract: status=%d body=%s", summary.Code, summary.Body.String())
	}
}

func aggregateGraphRequest(t *testing.T, handler gin.HandlerFunc, p *principal, method, target, graphID string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var input []byte
	if body != nil {
		var err error
		input, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, target, bytes.NewReader(input))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(principalKey, p)
	if graphID != "" {
		c.Params = gin.Params{{Key: "id", Value: graphID}}
	}
	handler(c)
	return recorder
}
