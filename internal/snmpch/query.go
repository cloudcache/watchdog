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
	MetricIfInBPS     = "watchdog_snmp_if_in_bps"
	MetricIfOutBPS    = "watchdog_snmp_if_out_bps"
	MetricIfInOctets  = "watchdog_snmp_if_in_octets_total"
	MetricIfOutOctets = "watchdog_snmp_if_out_octets_total"
)

type QueryRequest struct {
	DeviceID, EntityID, Metric string
	From, To                   time.Time
	Step                       time.Duration
	MaxRows                    uint32
}

type Point struct {
	Time  time.Time
	Value float64
}

type Series struct {
	DeviceID, EntityKind, EntityID, Metric string
	Points                                 []Point
}

func (s *Store) Query(ctx context.Context, req QueryRequest) ([]Series, error) {
	if s == nil || s.exec == nil {
		return nil, errors.New("SNMP ClickHouse store is not initialized")
	}
	if req.DeviceID == "" || req.Metric == "" || !req.To.After(req.From) || req.Step < time.Second || !s.allowsRows(req.MaxRows) {
		return nil, errors.New("invalid SNMP query")
	}
	metric, rate := req.Metric, false
	switch metric {
	case MetricIfInBPS:
		metric, rate = MetricIfInOctets, true
	case MetricIfOutBPS:
		metric, rate = MetricIfOutOctets, true
	}
	entityClause := ""
	params := map[string]any{
		"device": req.DeviceID, "metric": metric,
	}
	if req.EntityID != "" {
		entityClause = " AND entity_id={entity:String}"
		params["entity"] = req.EntityID
	}
	body := fmt.Sprintf(valueQuerySQL, entityClause)
	if rate {
		body = fmt.Sprintf(rateQuerySQL, entityClause)
	}
	body = strings.NewReplacer(
		"{from_ms:Int64}", strconv.FormatInt(req.From.UTC().UnixMilli(), 10),
		"{to_ms:Int64}", strconv.FormatInt(req.To.UTC().UnixMilli(), 10),
		"{step:UInt64}", strconv.FormatInt(int64(req.Step/time.Second), 10),
		"{limit:UInt64}", strconv.FormatUint(uint64(req.MaxRows)+1, 10),
	).Replace(body)
	var buckets proto.ColDateTime
	var devices, entities proto.ColStr
	kinds := new(proto.ColStr).LowCardinality()
	var values proto.ColFloat64
	query := ch.Query{
		Body: body, Parameters: ch.Parameters(params),
		Result: proto.Results{
			{Name: "bucket", Data: &buckets}, {Name: "device_id", Data: &devices},
			{Name: "entity_kind", Data: kinds}, {Name: "entity_id", Data: &entities},
			{Name: "value", Data: &values},
		},
		Settings: s.querySettings(req.MaxRows),
	}
	byKey := map[string]*Series{}
	rows := uint32(0)
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for i := 0; i < block.Rows; i++ {
			rows++
			if rows > req.MaxRows {
				return errors.New("SNMP query exceeds row budget")
			}
			key := devices.Row(i) + "\x00" + kinds.Row(i) + "\x00" + entities.Row(i)
			series := byKey[key]
			if series == nil {
				series = &Series{DeviceID: devices.Row(i), EntityKind: kinds.Row(i), EntityID: entities.Row(i), Metric: req.Metric}
				byKey[key] = series
			}
			series.Points = append(series.Points, Point{Time: buckets.Row(i).UTC(), Value: values[i]})
		}
		return nil
	}
	if err := s.exec.Do(ctx, query); err != nil {
		return nil, fmt.Errorf("query SNMP ClickHouse data: %w", err)
	}
	result := make([]Series, 0, len(byKey))
	for _, series := range byKey {
		result = append(result, *series)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].EntityKind != result[j].EntityKind {
			return result[i].EntityKind < result[j].EntityKind
		}
		return result[i].EntityID < result[j].EntityID
	})
	return result, nil
}

const valueQuerySQL = `SELECT
  toStartOfInterval(observed_at,toIntervalSecond({step:UInt64})) bucket,
  device_id,entity_kind,entity_id,
  if(argMax(value_kind,tuple(observed_at,ingested_at))='counter',
     toFloat64(argMax(counter_value,tuple(observed_at,ingested_at))),
     argMax(gauge_value,tuple(observed_at,ingested_at))) value
FROM snmp_samples FINAL
WHERE device_id={device:String} AND metric={metric:String}
  AND observed_at>=fromUnixTimestamp64Milli({from_ms:Int64}) AND observed_at<fromUnixTimestamp64Milli({to_ms:Int64})%s
GROUP BY bucket,device_id,entity_kind,entity_id
ORDER BY bucket,device_id,entity_kind,entity_id
LIMIT {limit:UInt64}`

const rateQuerySQL = `WITH dedup AS (
  SELECT observed_at,device_id,entity_kind,entity_id,
    argMax(counter_value,ingested_at) counter_value,
    argMax(counter_width,ingested_at) counter_width,
    argMax(interval_ms,ingested_at) interval_ms
  FROM snmp_samples FINAL
  WHERE device_id={device:String} AND metric={metric:String}
    AND observed_at>=subtractMinutes(fromUnixTimestamp64Milli({from_ms:Int64}),15) AND observed_at<fromUnixTimestamp64Milli({to_ms:Int64})%s
  GROUP BY observed_at,device_id,entity_kind,entity_id
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
FROM segments
WHERE observed_at>=fromUnixTimestamp64Milli({from_ms:Int64})
GROUP BY bucket,device_id,entity_kind,entity_id
HAVING sumIf(elapsed_ms,rn>1 AND elapsed_ms>0 AND elapsed_ms<=greatest(toInt64(interval_ms)*3,900000) AND accepted)>0
ORDER BY bucket,device_id,entity_kind,entity_id
LIMIT {limit:UInt64}`
