package snmpch

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const (
	// HardMaxAggregateScopes and HardMaxAggregateRows are process safety
	// ceilings, not chart or billing defaults. Runtime configuration may lower
	// them, but raising them requires a reviewed memory/query-plan change.
	HardMaxAggregateScopes = 1000
	HardMaxAggregateRows   = 250000
)

// Scope is one authorized device or port included in an aggregate. An empty
// PortID selects every matching metric entity on the device.
type Scope struct {
	DeviceID string `json:"device_id"`
	PortID   string `json:"port_id,omitempty"`
}

type AggregateRequest struct {
	Scopes  []Scope       `json:"scopes"`
	Metric  string        `json:"metric"`
	Method  string        `json:"method"`
	From    time.Time     `json:"from"`
	To      time.Time     `json:"to"`
	Step    time.Duration `json:"-"`
	MaxRows uint32        `json:"max_rows"`
}

type AggregateResult struct {
	Metric string  `json:"metric"`
	Method string  `json:"method"`
	Points []Point `json:"points"`
}

// QueryScopes returns the per-entity series selected by an authorized scope
// set. It is used when presentation policy must be applied before aggregation;
// raw SNMP samples and counters remain immutable in ClickHouse.
func (s *Store) QueryScopes(ctx context.Context, req AggregateRequest) ([]Series, error) {
	if s == nil || s.exec == nil {
		return nil, errors.New("SNMP ClickHouse store is not initialized")
	}
	scopes, err := normalizeScopes(req.Scopes)
	if err != nil {
		return nil, err
	}
	if req.Metric == "" || !req.To.After(req.From) || req.To.Sub(req.From) > 400*24*time.Hour || req.Step < time.Second || req.Step > 24*time.Hour || !s.allowsRows(req.MaxRows) {
		return nil, errors.New("invalid SNMP scoped query")
	}
	metric, rate := req.Metric, false
	switch metric {
	case MetricIfInBPS:
		metric, rate = MetricIfInOctets, true
	case MetricIfOutBPS:
		metric, rate = MetricIfOutOctets, true
	}
	body := valueScopedSQL
	if rate {
		body = rateScopedSQL
	}
	body = strings.NewReplacer(
		"{from_ms:Int64}", strconv.FormatInt(req.From.UTC().UnixMilli(), 10),
		"{to_ms:Int64}", strconv.FormatInt(req.To.UTC().UnixMilli(), 10),
		"{step:UInt64}", strconv.FormatInt(int64(req.Step/time.Second), 10),
		"{limit:UInt64}", strconv.FormatUint(uint64(req.MaxRows)+1, 10),
	).Replace(body)
	devices := new(proto.ColStr)
	ports := new(proto.ColStr)
	for _, scope := range scopes {
		devices.Append(scope.DeviceID)
		ports.Append(scope.PortID)
	}
	var buckets proto.ColDateTime
	var resultDevices, entities proto.ColStr
	kinds := new(proto.ColStr).LowCardinality()
	var values proto.ColFloat64
	query := ch.Query{
		Body: body, Parameters: ch.Parameters(map[string]any{"metric": metric}),
		ExternalTable: "snmp_scope",
		ExternalData:  []proto.InputColumn{{Name: "device_id", Data: devices}, {Name: "port_id", Data: ports}},
		Result: proto.Results{
			{Name: "bucket", Data: &buckets}, {Name: "device_id", Data: &resultDevices},
			{Name: "entity_kind", Data: kinds}, {Name: "entity_id", Data: &entities}, {Name: "value", Data: &values},
		},
		Settings: s.querySettings(req.MaxRows),
	}
	byKey := make(map[string]*Series)
	rows := uint32(0)
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for i := 0; i < block.Rows; i++ {
			rows++
			if rows > req.MaxRows {
				return errors.New("SNMP scoped query exceeds row budget")
			}
			key := resultDevices.Row(i) + "\x00" + kinds.Row(i) + "\x00" + entities.Row(i)
			series := byKey[key]
			if series == nil {
				series = &Series{DeviceID: resultDevices.Row(i), EntityKind: kinds.Row(i), EntityID: entities.Row(i), Metric: req.Metric}
				byKey[key] = series
			}
			series.Points = append(series.Points, Point{Time: buckets.Row(i).UTC(), Value: values[i]})
		}
		return nil
	}
	if err := s.exec.Do(ctx, query); err != nil {
		return nil, fmt.Errorf("query scoped SNMP ClickHouse data: %w", err)
	}
	result := make([]Series, 0, len(byKey))
	for _, series := range byKey {
		result = append(result, *series)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].DeviceID != result[j].DeviceID {
			return result[i].DeviceID < result[j].DeviceID
		}
		return result[i].EntityID < result[j].EntityID
	})
	return result, nil
}

// Aggregate performs one bounded ClickHouse query across the already
// authorized scopes. The scope is passed as an external table, never rendered
// into SQL, and the aggregation function comes from a fixed allowlist.
func (s *Store) Aggregate(ctx context.Context, req AggregateRequest) (AggregateResult, error) {
	if s == nil || s.exec == nil {
		return AggregateResult{}, errors.New("SNMP ClickHouse store is not initialized")
	}
	scopes, err := normalizeScopes(req.Scopes)
	if err != nil {
		return AggregateResult{}, err
	}
	method, err := aggregateFunction(req.Method)
	if err != nil {
		return AggregateResult{}, err
	}
	if req.Metric == "" || !req.To.After(req.From) || req.To.Sub(req.From) > 400*24*time.Hour || req.Step < time.Second || req.Step > 24*time.Hour || !s.allowsRows(req.MaxRows) {
		return AggregateResult{}, errors.New("invalid SNMP aggregate query")
	}

	metric, rate := req.Metric, false
	switch metric {
	case MetricIfInBPS:
		metric, rate = MetricIfInOctets, true
	case MetricIfOutBPS:
		metric, rate = MetricIfOutOctets, true
	}
	body := fmt.Sprintf(valueAggregateSQL, method)
	if rate {
		body = fmt.Sprintf(rateAggregateSQL, method)
	}
	body = strings.NewReplacer(
		"{from_ms:Int64}", strconv.FormatInt(req.From.UTC().UnixMilli(), 10),
		"{to_ms:Int64}", strconv.FormatInt(req.To.UTC().UnixMilli(), 10),
		"{step:UInt64}", strconv.FormatInt(int64(req.Step/time.Second), 10),
		"{limit:UInt64}", strconv.FormatUint(uint64(req.MaxRows)+1, 10),
	).Replace(body)

	devices := new(proto.ColStr)
	ports := new(proto.ColStr)
	for _, scope := range scopes {
		devices.Append(scope.DeviceID)
		ports.Append(scope.PortID)
	}
	var buckets proto.ColDateTime
	var values proto.ColFloat64
	query := ch.Query{
		Body:          body,
		Parameters:    ch.Parameters(map[string]any{"metric": metric}),
		ExternalTable: "snmp_scope",
		ExternalData:  []proto.InputColumn{{Name: "device_id", Data: devices}, {Name: "port_id", Data: ports}},
		Result:        proto.Results{{Name: "bucket", Data: &buckets}, {Name: "value", Data: &values}},
		Settings:      s.querySettings(req.MaxRows),
	}
	result := AggregateResult{Metric: req.Metric, Method: req.Method}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if uint64(len(result.Points)+block.Rows) > uint64(req.MaxRows) {
			return errors.New("SNMP aggregate exceeds row budget")
		}
		for i := 0; i < block.Rows; i++ {
			result.Points = append(result.Points, Point{Time: buckets.Row(i).UTC(), Value: values[i]})
		}
		return nil
	}
	if err := s.exec.Do(ctx, query); err != nil {
		return AggregateResult{}, fmt.Errorf("aggregate SNMP ClickHouse data: %w", err)
	}
	return result, nil
}

func normalizeScopes(input []Scope) ([]Scope, error) {
	if len(input) == 0 || len(input) > HardMaxAggregateScopes {
		return nil, errors.New("SNMP aggregate requires 1..1000 scopes")
	}
	deviceWide := make(map[string]bool, len(input))
	seen := make(map[string]Scope, len(input))
	for _, scope := range input {
		scope.DeviceID = strings.TrimSpace(scope.DeviceID)
		scope.PortID = strings.TrimSpace(scope.PortID)
		if scope.DeviceID == "" {
			return nil, errors.New("SNMP aggregate scope device_id is required")
		}
		if scope.PortID == "" {
			deviceWide[scope.DeviceID] = true
		}
		seen[scope.DeviceID+"\x00"+scope.PortID] = scope
	}
	out := make([]Scope, 0, len(seen))
	for _, scope := range seen {
		if scope.PortID != "" && deviceWide[scope.DeviceID] {
			continue
		}
		out = append(out, scope)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DeviceID != out[j].DeviceID {
			return out[i].DeviceID < out[j].DeviceID
		}
		return out[i].PortID < out[j].PortID
	})
	return out, nil
}

func aggregateFunction(method string) (string, error) {
	switch method {
	case "", "sum":
		return "sum", nil
	case "avg":
		return "avg", nil
	case "min":
		return "min", nil
	case "max":
		return "max", nil
	case "count":
		return "count", nil
	default:
		return "", errors.New("unsupported SNMP aggregate method")
	}
}

const valueAggregateSQL = `WITH scoped AS (
  SELECT toStartOfInterval(s.observed_at,toIntervalSecond({step:UInt64})) bucket,
    s.device_id,s.entity_kind,s.entity_id,
    if(argMax(s.value_kind,tuple(s.observed_at,s.ingested_at))='counter',
       toFloat64(argMax(s.counter_value,tuple(s.observed_at,s.ingested_at))),
       argMax(s.gauge_value,tuple(s.observed_at,s.ingested_at))) value
  FROM snmp_samples AS s FINAL
  INNER JOIN snmp_scope AS scope
    ON s.device_id=scope.device_id
  WHERE (scope.port_id='' OR s.entity_id=scope.port_id) AND s.metric={metric:String}
    AND s.observed_at>=fromUnixTimestamp64Milli({from_ms:Int64})
    AND s.observed_at<fromUnixTimestamp64Milli({to_ms:Int64})
  GROUP BY bucket,s.device_id,s.entity_kind,s.entity_id
)
SELECT bucket,%s(value) value
FROM scoped GROUP BY bucket ORDER BY bucket LIMIT {limit:UInt64}`

const rateAggregateSQL = `WITH dedup AS (
  SELECT s.observed_at,s.device_id,s.entity_kind,s.entity_id,
    argMax(s.counter_value,s.ingested_at) counter_value,
    argMax(s.counter_width,s.ingested_at) counter_width,
    argMax(s.interval_ms,s.ingested_at) interval_ms
  FROM snmp_samples AS s FINAL
  INNER JOIN snmp_scope AS scope
    ON s.device_id=scope.device_id
  WHERE (scope.port_id='' OR s.entity_id=scope.port_id) AND s.metric={metric:String} AND s.entity_kind='port'
    AND s.observed_at>=subtractMinutes(fromUnixTimestamp64Milli({from_ms:Int64}),15)
    AND s.observed_at<fromUnixTimestamp64Milli({to_ms:Int64})
  GROUP BY s.observed_at,s.device_id,s.entity_kind,s.entity_id
), ordered AS (
  SELECT *,row_number() OVER w rn,
    lagInFrame(observed_at) OVER w previous_at,
    lagInFrame(counter_value) OVER w previous_value,
    lagInFrame(counter_width) OVER w previous_width
  FROM dedup WINDOW w AS (PARTITION BY device_id,entity_kind,entity_id ORDER BY observed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), segments AS (
  SELECT *,dateDiff('millisecond',previous_at,observed_at) elapsed_ms,
    (counter_width=previous_width AND (counter_value>=previous_value OR (counter_width=32 AND previous_value>=3865470566 AND counter_value<=429496729))) accepted,
    multiIf(counter_width!=previous_width,0,counter_value>=previous_value,counter_value-previous_value,4294967296-previous_value+counter_value) delta
  FROM ordered
), per_entity AS (
  SELECT toStartOfInterval(observed_at,toIntervalSecond({step:UInt64})) bucket,
    device_id,entity_kind,entity_id,
    sumIf(toFloat64(delta)*8000,rn>1 AND elapsed_ms>0 AND elapsed_ms<=greatest(toInt64(interval_ms)*3,900000) AND accepted)
      / sumIf(elapsed_ms,rn>1 AND elapsed_ms>0 AND elapsed_ms<=greatest(toInt64(interval_ms)*3,900000) AND accepted) value
  FROM segments WHERE observed_at>=fromUnixTimestamp64Milli({from_ms:Int64})
  GROUP BY bucket,device_id,entity_kind,entity_id
  HAVING sumIf(elapsed_ms,rn>1 AND elapsed_ms>0 AND elapsed_ms<=greatest(toInt64(interval_ms)*3,900000) AND accepted)>0
)
SELECT bucket,%s(value) value
FROM per_entity GROUP BY bucket ORDER BY bucket LIMIT {limit:UInt64}`

const valueScopedSQL = `SELECT
  toStartOfInterval(s.observed_at,toIntervalSecond({step:UInt64})) bucket,
  s.device_id,s.entity_kind,s.entity_id,
  if(argMax(s.value_kind,tuple(s.observed_at,s.ingested_at))='counter',
     toFloat64(argMax(s.counter_value,tuple(s.observed_at,s.ingested_at))),
     argMax(s.gauge_value,tuple(s.observed_at,s.ingested_at))) value
FROM snmp_samples AS s FINAL
INNER JOIN snmp_scope AS scope
  ON s.device_id=scope.device_id
WHERE (scope.port_id='' OR s.entity_id=scope.port_id) AND s.metric={metric:String}
  AND s.observed_at>=fromUnixTimestamp64Milli({from_ms:Int64})
  AND s.observed_at<fromUnixTimestamp64Milli({to_ms:Int64})
GROUP BY bucket,s.device_id,s.entity_kind,s.entity_id
ORDER BY bucket,s.device_id,s.entity_kind,s.entity_id
LIMIT {limit:UInt64}`

const rateScopedSQL = `WITH dedup AS (
  SELECT s.observed_at,s.device_id,s.entity_kind,s.entity_id,
    argMax(s.counter_value,s.ingested_at) counter_value,
    argMax(s.counter_width,s.ingested_at) counter_width,
    argMax(s.interval_ms,s.ingested_at) interval_ms
  FROM snmp_samples AS s FINAL
  INNER JOIN snmp_scope AS scope
    ON s.device_id=scope.device_id
  WHERE (scope.port_id='' OR s.entity_id=scope.port_id) AND s.metric={metric:String} AND s.entity_kind='port'
    AND s.observed_at>=subtractMinutes(fromUnixTimestamp64Milli({from_ms:Int64}),15)
    AND s.observed_at<fromUnixTimestamp64Milli({to_ms:Int64})
  GROUP BY s.observed_at,s.device_id,s.entity_kind,s.entity_id
), ordered AS (
  SELECT *,row_number() OVER w rn,
    lagInFrame(observed_at) OVER w previous_at,
    lagInFrame(counter_value) OVER w previous_value,
    lagInFrame(counter_width) OVER w previous_width
  FROM dedup WINDOW w AS (PARTITION BY device_id,entity_kind,entity_id ORDER BY observed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), segments AS (
  SELECT *,dateDiff('millisecond',previous_at,observed_at) elapsed_ms,
    (counter_width=previous_width AND (counter_value>=previous_value OR (counter_width=32 AND previous_value>=3865470566 AND counter_value<=429496729))) accepted,
    multiIf(counter_width!=previous_width,0,counter_value>=previous_value,counter_value-previous_value,4294967296-previous_value+counter_value) delta
  FROM ordered
)
SELECT toStartOfInterval(observed_at,toIntervalSecond({step:UInt64})) bucket,
  device_id,entity_kind,entity_id,
  sumIf(toFloat64(delta)*8000,rn>1 AND elapsed_ms>0 AND elapsed_ms<=greatest(toInt64(interval_ms)*3,900000) AND accepted)
    / sumIf(elapsed_ms,rn>1 AND elapsed_ms>0 AND elapsed_ms<=greatest(toInt64(interval_ms)*3,900000) AND accepted) value
FROM segments WHERE observed_at>=fromUnixTimestamp64Milli({from_ms:Int64})
GROUP BY bucket,device_id,entity_kind,entity_id
HAVING sumIf(elapsed_ms,rn>1 AND elapsed_ms>0 AND elapsed_ms<=greatest(toInt64(interval_ms)*3,900000) AND accepted)>0
ORDER BY bucket,device_id,entity_kind,entity_id
LIMIT {limit:UInt64}`
