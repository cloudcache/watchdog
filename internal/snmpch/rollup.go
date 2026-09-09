package snmpch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go"
)

const interfaceBucket = 5 * time.Minute

// RebuildClosedInterfaceBucket derives one immutable five-minute interface
// traffic generation from raw counters. Values are written before the marker;
// readers accept only the marker's generation, so a crash cannot publish a
// partial rebuild and an empty repair can supersede old values.
func (s *Store) RebuildClosedInterfaceBucket(ctx context.Context, bucketStart, now time.Time, generation uint64) error {
	if s == nil || s.exec == nil {
		return errors.New("SNMP ClickHouse store is not initialized")
	}
	bucketStart = bucketStart.UTC()
	now = now.UTC()
	if generation == 0 || !bucketStart.Equal(bucketStart.Truncate(interfaceBucket)) || bucketStart.Add(interfaceBucket).After(now.Truncate(interfaceBucket)) {
		return errors.New("SNMP interface rollup requires an aligned closed bucket and non-zero generation")
	}
	parameters := ch.Parameters(map[string]any{
		"bucket_ms": bucketStart.UnixMilli(), "end_ms": bucketStart.Add(interfaceBucket).UnixMilli(),
		"lookback_ms": bucketStart.Add(-15 * time.Minute).UnixMilli(), "generation": generation,
		"generated_ms": now.UnixMilli(),
	})
	if err := s.exec.Do(ctx, ch.Query{Body: interfaceRollupValuesSQL, Parameters: parameters}); err != nil {
		return fmt.Errorf("write SNMP interface rollup values: %w", err)
	}
	if err := s.exec.Do(ctx, ch.Query{Body: interfaceRollupMarkerSQL, Parameters: parameters}); err != nil {
		return fmt.Errorf("publish SNMP interface rollup generation: %w", err)
	}
	return nil
}

const interfaceRollupValuesSQL = `INSERT INTO snmp_interface_traffic_5m
  (bucket_start,row_kind,device_id,port_id,in_bytes,out_bytes,in_bps,out_bps,reset_flag,gap_flag,coverage,generation,generated_at)
WITH dedup AS (
  SELECT observed_at,device_id,entity_id,metric,
    argMax(counter_value,ingested_at) counter_value,
    argMax(counter_width,ingested_at) counter_width,
    argMax(interval_ms,ingested_at) interval_ms
  FROM snmp_samples FINAL
  WHERE entity_kind='port'
    AND metric IN ('watchdog_snmp_if_in_octets_total','watchdog_snmp_if_out_octets_total')
    AND observed_at>=fromUnixTimestamp64Milli({lookback_ms:Int64}) AND observed_at<=fromUnixTimestamp64Milli({end_ms:Int64})
  GROUP BY observed_at,device_id,entity_id,metric
), ordered AS (
  SELECT *,row_number() OVER w rn,
    lagInFrame(observed_at) OVER w previous_at,
    lagInFrame(counter_value) OVER w previous_value
  FROM dedup WINDOW w AS (PARTITION BY device_id,entity_id,metric ORDER BY observed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), segments AS (
  SELECT *,dateDiff('millisecond',previous_at,observed_at) elapsed_ms,
    counter_width=32 AND previous_value>=3865470566 AND counter_value<=429496729 wrap32,
    counter_value>=previous_value forward,
    multiIf(counter_value>=previous_value,counter_value-previous_value,
      counter_width=32 AND previous_value>=3865470566 AND counter_value<=429496729,
      4294967296-previous_value+counter_value,0) delta
  FROM ordered
), per_metric AS (
  SELECT device_id,entity_id,metric,
    sumIf(delta,rn>1 AND elapsed_ms>0 AND elapsed_ms<=greatest(toInt64(interval_ms)*3,900000) AND (forward OR wrap32)) bytes,
    sumIf(elapsed_ms,rn>1 AND elapsed_ms>0 AND elapsed_ms<=greatest(toInt64(interval_ms)*3,900000) AND (forward OR wrap32)) accepted_ms,
    countIf(rn>1 AND elapsed_ms>0 AND NOT forward AND NOT wrap32)>0 reset_flag,
    countIf(rn>1 AND elapsed_ms>greatest(toInt64(interval_ms)*3,900000))>0 gap_flag
  FROM segments
  WHERE previous_at>=fromUnixTimestamp64Milli({bucket_ms:Int64}) AND observed_at<=fromUnixTimestamp64Milli({end_ms:Int64})
  GROUP BY device_id,entity_id,metric
)
SELECT toDateTime(fromUnixTimestamp64Milli({bucket_ms:Int64})),'value',device_id,entity_id,
  sumIf(bytes,metric='watchdog_snmp_if_in_octets_total'),
  sumIf(bytes,metric='watchdog_snmp_if_out_octets_total'),
  sumIf(if(accepted_ms=0,0,toFloat64(bytes)*8000/accepted_ms),metric='watchdog_snmp_if_in_octets_total'),
  sumIf(if(accepted_ms=0,0,toFloat64(bytes)*8000/accepted_ms),metric='watchdog_snmp_if_out_octets_total'),
  max(reset_flag),max(gap_flag),least(sum(accepted_ms)/600000.0,1.0),
  {generation:UInt64},fromUnixTimestamp64Milli({generated_ms:Int64})
FROM per_metric GROUP BY device_id,entity_id`

const interfaceRollupMarkerSQL = `INSERT INTO snmp_interface_traffic_5m
  (bucket_start,row_kind,device_id,port_id,in_bytes,out_bytes,in_bps,out_bps,reset_flag,gap_flag,coverage,generation,generated_at)
VALUES (toDateTime(fromUnixTimestamp64Milli({bucket_ms:Int64})),'generation','','',0,0,0,0,0,0,0,{generation:UInt64},fromUnixTimestamp64Milli({generated_ms:Int64}))`
