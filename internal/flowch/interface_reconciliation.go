// SPDX-License-Identifier: AGPL-3.0-only

package flowch

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
	hardMaxInterfaceReconciliationScopes = 1000
	hardMaxInterfaceReconciliationRows   = 250000
)

type InterfaceReconciliationScope struct {
	DeviceID string `json:"device_id"`
	PortID   string `json:"port_id"`
	IfIndex  uint32 `json:"if_index"`
}

type InterfaceReconciliationBudget struct {
	MaxResultRows    uint32
	MaxExecutionTime time.Duration
	MaxRowsToRead    uint64
	MaxBytesToRead   uint64
	MaxMemoryBytes   uint64
}

type InterfaceReconciliationRequest struct {
	Scopes []InterfaceReconciliationScope
	From   time.Time
	To     time.Time
	Budget InterfaceReconciliationBudget
}

type InterfaceReconciliationBucket struct {
	Bucket   time.Time `json:"bucket"`
	DeviceID string    `json:"device_id"`
	PortID   string    `json:"port_id"`
	IfIndex  uint32    `json:"if_index"`

	FlowInBytes             uint64  `json:"flow_in_bytes"`
	FlowOutBytes            uint64  `json:"flow_out_bytes"`
	FlowRecords             uint64  `json:"flow_records"`
	FlowUnknownSampling     uint64  `json:"flow_unknown_sampling_records"`
	SFlowCounterInBytes     uint64  `json:"sflow_counter_in_bytes"`
	SFlowCounterOutBytes    uint64  `json:"sflow_counter_out_bytes"`
	SFlowCounterInCoverage  float64 `json:"sflow_counter_in_coverage"`
	SFlowCounterOutCoverage float64 `json:"sflow_counter_out_coverage"`
	SFlowCounterInReset     bool    `json:"sflow_counter_in_reset"`
	SFlowCounterOutReset    bool    `json:"sflow_counter_out_reset"`
	SNMPInBytes             uint64  `json:"snmp_in_bytes"`
	SNMPOutBytes            uint64  `json:"snmp_out_bytes"`
	SNMPCoverage            float64 `json:"snmp_coverage"`
	SNMPReset               bool    `json:"snmp_reset"`
	SNMPGap                 bool    `json:"snmp_gap"`
	FlowToCounterInRatio    float64 `json:"flow_to_counter_in_ratio"`
	FlowToCounterInValid    bool    `json:"flow_to_counter_in_valid"`
	FlowToCounterOutRatio   float64 `json:"flow_to_counter_out_ratio"`
	FlowToCounterOutValid   bool    `json:"flow_to_counter_out_valid"`
	FlowToSNMPInRatio       float64 `json:"flow_to_snmp_in_ratio"`
	FlowToSNMPInValid       bool    `json:"flow_to_snmp_in_valid"`
	FlowToSNMPOutRatio      float64 `json:"flow_to_snmp_out_ratio"`
	FlowToSNMPOutValid      bool    `json:"flow_to_snmp_out_valid"`
	CounterToSNMPInRatio    float64 `json:"counter_to_snmp_in_ratio"`
	CounterToSNMPInValid    bool    `json:"counter_to_snmp_in_valid"`
	CounterToSNMPOutRatio   float64 `json:"counter_to_snmp_out_ratio"`
	CounterToSNMPOutValid   bool    `json:"counter_to_snmp_out_valid"`
}

type InterfaceReconciliationResult struct {
	From    time.Time                       `json:"from"`
	To      time.Time                       `json:"to"`
	Buckets []InterfaceReconciliationBucket `json:"buckets"`
}

func DefaultInterfaceReconciliationBudget() InterfaceReconciliationBudget {
	return InterfaceReconciliationBudget{
		MaxResultRows: 50000, MaxExecutionTime: 30 * time.Second,
		MaxRowsToRead: 100_000_000, MaxBytesToRead: 8 << 30, MaxMemoryBytes: 2 << 30,
	}
}

// ReadInterfaceReconciliation returns evidence only. It never modifies or
// calibrates Flow, sFlow counter, or SNMP facts.
func (s *NativeInserter) ReadInterfaceReconciliation(ctx context.Context, request InterfaceReconciliationRequest) (InterfaceReconciliationResult, error) {
	if s == nil || s.executor == nil {
		return InterfaceReconciliationResult{}, errors.New("Flow ClickHouse store is not initialized")
	}
	scopes, err := normalizeInterfaceReconciliationScopes(request.Scopes)
	if err != nil {
		return InterfaceReconciliationResult{}, err
	}
	from, to := request.From.UTC(), request.To.UTC()
	if !to.After(from) || !from.Equal(from.Truncate(5*time.Minute)) || !to.Equal(to.Truncate(5*time.Minute)) || to.Sub(from) > 7*24*time.Hour {
		return InterfaceReconciliationResult{}, errors.New("interface reconciliation requires an aligned interval of at most 7 days")
	}
	budget := request.Budget
	if budget == (InterfaceReconciliationBudget{}) {
		budget = DefaultInterfaceReconciliationBudget()
	}
	if err := validateInterfaceReconciliationBudget(budget); err != nil {
		return InterfaceReconciliationResult{}, err
	}
	expectedRows := uint64(to.Sub(from)/(5*time.Minute)) * uint64(len(scopes))
	if expectedRows == 0 || expectedRows > uint64(budget.MaxResultRows) {
		return InterfaceReconciliationResult{}, errors.New("interface reconciliation result exceeds configured row budget")
	}

	devices, ports := new(proto.ColStr), new(proto.ColStr)
	indexes := new(proto.ColUInt32)
	for _, scope := range scopes {
		devices.Append(scope.DeviceID)
		ports.Append(scope.PortID)
		indexes.Append(scope.IfIndex)
	}
	var buckets proto.ColDateTime
	var resultDevices, resultPorts proto.ColStr
	var resultIndexes proto.ColUInt32
	var flowIn, flowOut, flowRecords, flowUnknown, counterIn, counterOut, snmpIn, snmpOut proto.ColUInt64
	var counterInCoverage, counterOutCoverage, snmpCoverage proto.ColFloat64
	var counterInReset, counterOutReset, snmpReset, snmpGap proto.ColUInt8
	query := ch.Query{
		Body: strings.NewReplacer(
			"{from_ms:Int64}", strconv.FormatInt(from.UnixMilli(), 10),
			"{to_ms:Int64}", strconv.FormatInt(to.UnixMilli(), 10),
			"{limit:UInt64}", strconv.FormatUint(uint64(budget.MaxResultRows)+1, 10),
		).Replace(interfaceReconciliationSQL),
		ExternalTable: "interface_reconciliation_scope",
		ExternalData: []proto.InputColumn{
			{Name: "device_id", Data: devices}, {Name: "port_id", Data: ports}, {Name: "if_index", Data: indexes},
		},
		Result: proto.Results{
			{Name: "bucket", Data: &buckets}, {Name: "device_id", Data: &resultDevices}, {Name: "port_id", Data: &resultPorts}, {Name: "if_index", Data: &resultIndexes},
			{Name: "flow_in_bytes", Data: &flowIn}, {Name: "flow_out_bytes", Data: &flowOut}, {Name: "flow_records", Data: &flowRecords}, {Name: "flow_unknown", Data: &flowUnknown},
			{Name: "counter_in_bytes", Data: &counterIn}, {Name: "counter_out_bytes", Data: &counterOut},
			{Name: "counter_in_coverage", Data: &counterInCoverage}, {Name: "counter_out_coverage", Data: &counterOutCoverage},
			{Name: "counter_in_reset", Data: &counterInReset}, {Name: "counter_out_reset", Data: &counterOutReset},
			{Name: "snmp_in_bytes", Data: &snmpIn}, {Name: "snmp_out_bytes", Data: &snmpOut}, {Name: "snmp_coverage", Data: &snmpCoverage},
			{Name: "snmp_reset", Data: &snmpReset}, {Name: "snmp_gap", Data: &snmpGap},
		},
		Settings: interfaceReconciliationSettings(budget),
	}
	result := InterfaceReconciliationResult{From: from, To: to}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if uint64(len(result.Buckets)+block.Rows) > uint64(budget.MaxResultRows) {
			return errors.New("interface reconciliation exceeded result row budget")
		}
		for row := 0; row < block.Rows; row++ {
			item := InterfaceReconciliationBucket{
				Bucket: buckets.Row(row).UTC(), DeviceID: resultDevices.Row(row), PortID: resultPorts.Row(row), IfIndex: resultIndexes[row],
				FlowInBytes: flowIn[row], FlowOutBytes: flowOut[row], FlowRecords: flowRecords[row], FlowUnknownSampling: flowUnknown[row],
				SFlowCounterInBytes: counterIn[row], SFlowCounterOutBytes: counterOut[row],
				SFlowCounterInCoverage: counterInCoverage[row], SFlowCounterOutCoverage: counterOutCoverage[row],
				SFlowCounterInReset: counterInReset[row] != 0, SFlowCounterOutReset: counterOutReset[row] != 0,
				SNMPInBytes: snmpIn[row], SNMPOutBytes: snmpOut[row], SNMPCoverage: snmpCoverage[row],
				SNMPReset: snmpReset[row] != 0, SNMPGap: snmpGap[row] != 0,
			}
			item.FlowToCounterInRatio, item.FlowToCounterInValid = evidenceRatio(item.FlowInBytes, item.SFlowCounterInBytes)
			item.FlowToCounterOutRatio, item.FlowToCounterOutValid = evidenceRatio(item.FlowOutBytes, item.SFlowCounterOutBytes)
			item.FlowToSNMPInRatio, item.FlowToSNMPInValid = evidenceRatio(item.FlowInBytes, item.SNMPInBytes)
			item.FlowToSNMPOutRatio, item.FlowToSNMPOutValid = evidenceRatio(item.FlowOutBytes, item.SNMPOutBytes)
			item.CounterToSNMPInRatio, item.CounterToSNMPInValid = evidenceRatio(item.SFlowCounterInBytes, item.SNMPInBytes)
			item.CounterToSNMPOutRatio, item.CounterToSNMPOutValid = evidenceRatio(item.SFlowCounterOutBytes, item.SNMPOutBytes)
			result.Buckets = append(result.Buckets, item)
		}
		return nil
	}
	if err := s.executor.Do(ctx, query); err != nil {
		return InterfaceReconciliationResult{}, fmt.Errorf("read interface reconciliation evidence: %w", err)
	}
	return result, nil
}

func normalizeInterfaceReconciliationScopes(input []InterfaceReconciliationScope) ([]InterfaceReconciliationScope, error) {
	if len(input) == 0 || len(input) > hardMaxInterfaceReconciliationScopes {
		return nil, fmt.Errorf("interface reconciliation requires 1..%d scopes", hardMaxInterfaceReconciliationScopes)
	}
	seen := make(map[string]InterfaceReconciliationScope, len(input))
	for _, scope := range input {
		scope.DeviceID, scope.PortID = strings.TrimSpace(scope.DeviceID), strings.TrimSpace(scope.PortID)
		if scope.DeviceID == "" || scope.PortID == "" || scope.IfIndex == 0 {
			return nil, errors.New("interface reconciliation scope requires device_id, port_id, and if_index")
		}
		key := scope.DeviceID + "\x00" + strconv.FormatUint(uint64(scope.IfIndex), 10)
		if previous, ok := seen[key]; ok && previous.PortID != scope.PortID {
			return nil, errors.New("interface reconciliation scope maps one device/ifIndex to multiple ports")
		}
		seen[key] = scope
	}
	result := make([]InterfaceReconciliationScope, 0, len(seen))
	for _, scope := range seen {
		result = append(result, scope)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].DeviceID != result[j].DeviceID {
			return result[i].DeviceID < result[j].DeviceID
		}
		return result[i].IfIndex < result[j].IfIndex
	})
	return result, nil
}

func validateInterfaceReconciliationBudget(budget InterfaceReconciliationBudget) error {
	if budget.MaxResultRows == 0 || budget.MaxResultRows > hardMaxInterfaceReconciliationRows ||
		budget.MaxExecutionTime <= 0 || budget.MaxExecutionTime > 2*time.Minute ||
		budget.MaxRowsToRead == 0 || budget.MaxRowsToRead > 500_000_000 ||
		budget.MaxBytesToRead == 0 || budget.MaxBytesToRead > 64<<30 ||
		budget.MaxMemoryBytes == 0 || budget.MaxMemoryBytes > 8<<30 {
		return errors.New("interface reconciliation budget is invalid")
	}
	return nil
}

func interfaceReconciliationSettings(budget InterfaceReconciliationBudget) []ch.Setting {
	return []ch.Setting{
		{Key: "readonly", Value: "1", Important: true},
		{Key: "max_execution_time", Value: strconv.FormatInt(int64((budget.MaxExecutionTime+time.Second-1)/time.Second), 10), Important: true},
		{Key: "max_result_rows", Value: strconv.FormatUint(uint64(budget.MaxResultRows)+1, 10), Important: true},
		{Key: "result_overflow_mode", Value: "throw", Important: true},
		{Key: "max_rows_to_read", Value: strconv.FormatUint(budget.MaxRowsToRead, 10), Important: true},
		{Key: "read_overflow_mode", Value: "throw", Important: true},
		{Key: "max_bytes_to_read", Value: strconv.FormatUint(budget.MaxBytesToRead, 10), Important: true},
		{Key: "max_memory_usage", Value: strconv.FormatUint(budget.MaxMemoryBytes, 10), Important: true},
	}
}

func evidenceRatio(numerator, denominator uint64) (float64, bool) {
	if denominator == 0 {
		return 0, false
	}
	return float64(numerator) / float64(denominator), true
}

const interfaceReconciliationSQL = `WITH flow_values AS (
  SELECT toStartOfFiveMinutes(records.event_time) bucket,scope.device_id,scope.port_id,scope.if_index,
    sumIf(records.estimated_bytes,records.estimated_valid AND records.ingress_if_index=scope.if_index) flow_in_bytes,
    sumIf(records.estimated_bytes,records.estimated_valid AND records.egress_if_index=scope.if_index) flow_out_bytes,
    countIf(records.ingress_if_index=scope.if_index OR records.egress_if_index=scope.if_index) flow_records,
    countIf(NOT records.estimated_valid AND (records.ingress_if_index=scope.if_index OR records.egress_if_index=scope.if_index)) flow_unknown
  FROM flow_records AS records FINAL
  INNER JOIN interface_reconciliation_scope AS scope
    ON records.device_id=scope.device_id AND (records.ingress_if_index=scope.if_index OR records.egress_if_index=scope.if_index)
  WHERE records.event_time>=fromUnixTimestamp64Milli({from_ms:Int64})
    AND records.event_time<fromUnixTimestamp64Milli({to_ms:Int64})
  GROUP BY bucket,scope.device_id,scope.port_id,scope.if_index
), counter_values AS (
  SELECT counters.*,scope.port_id
  FROM sflow_interface_counters AS counters FINAL
  INNER JOIN interface_reconciliation_scope AS scope ON counters.device_id=scope.device_id AND counters.if_index=scope.if_index
  WHERE counters.event_time>=subtractMinutes(fromUnixTimestamp64Milli({from_ms:Int64}),15)
    AND counters.event_time<=fromUnixTimestamp64Milli({to_ms:Int64})
), counter_ordered AS (
  SELECT *,row_number() OVER window AS rn,
    lagInFrame(event_time) OVER window previous_at,
    lagInFrame(if_in_octets) OVER window previous_in,
    lagInFrame(if_out_octets) OVER window previous_out
  FROM counter_values
  WINDOW window AS (PARTITION BY device_id,if_index ORDER BY event_time,kafka_partition,kafka_offset,sample_index,record_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), counter_segments AS (
  SELECT *,dateDiff('millisecond',previous_at,event_time) elapsed_ms,
    if_in_octets>=previous_in in_forward,if_out_octets>=previous_out out_forward,
    if(if_in_octets>=previous_in,if_in_octets-previous_in,0) in_delta,
    if(if_out_octets>=previous_out,if_out_octets-previous_out,0) out_delta
  FROM counter_ordered
), counter_rollup AS (
  SELECT toStartOfFiveMinutes(subtractMilliseconds(event_time,1)) bucket,device_id,port_id,if_index,
    sumIf(in_delta,rn>1 AND elapsed_ms>0 AND elapsed_ms<=900000 AND in_forward) counter_in_bytes,
    sumIf(out_delta,rn>1 AND elapsed_ms>0 AND elapsed_ms<=900000 AND out_forward) counter_out_bytes,
    least(sumIf(elapsed_ms,rn>1 AND elapsed_ms>0 AND elapsed_ms<=900000 AND in_forward)/300000.0,1.0) counter_in_coverage,
    least(sumIf(elapsed_ms,rn>1 AND elapsed_ms>0 AND elapsed_ms<=900000 AND out_forward)/300000.0,1.0) counter_out_coverage,
    toUInt8(countIf(rn>1 AND elapsed_ms>0 AND NOT in_forward)>0) counter_in_reset,
    toUInt8(countIf(rn>1 AND elapsed_ms>0 AND NOT out_forward)>0) counter_out_reset
  FROM counter_segments
  WHERE event_time>fromUnixTimestamp64Milli({from_ms:Int64}) AND event_time<=fromUnixTimestamp64Milli({to_ms:Int64})
  GROUP BY bucket,device_id,port_id,if_index
), snmp_published AS (
  SELECT bucket_start,generation FROM snmp_interface_traffic_5m FINAL
  WHERE row_kind='generation' AND bucket_start>=fromUnixTimestamp64Milli({from_ms:Int64}) AND bucket_start<fromUnixTimestamp64Milli({to_ms:Int64})
), snmp_values AS (
  SELECT values.bucket_start bucket,scope.device_id,scope.port_id,scope.if_index,
    values.in_bytes snmp_in_bytes,values.out_bytes snmp_out_bytes,values.coverage snmp_coverage,
    values.reset_flag snmp_reset,values.gap_flag snmp_gap
  FROM snmp_interface_traffic_5m AS values FINAL
  INNER JOIN snmp_published AS published ON values.bucket_start=published.bucket_start AND values.generation=published.generation
  INNER JOIN interface_reconciliation_scope AS scope ON values.device_id=scope.device_id AND values.port_id=scope.port_id
  WHERE values.row_kind='value' AND values.bucket_start>=fromUnixTimestamp64Milli({from_ms:Int64}) AND values.bucket_start<fromUnixTimestamp64Milli({to_ms:Int64})
), measurements AS (
  SELECT bucket,device_id,port_id,if_index,flow_in_bytes,flow_out_bytes,flow_records,flow_unknown,
    toUInt64(0) counter_in_bytes,toUInt64(0) counter_out_bytes,toFloat64(0) counter_in_coverage,toFloat64(0) counter_out_coverage,
    toUInt8(0) counter_in_reset,toUInt8(0) counter_out_reset,toUInt64(0) snmp_in_bytes,toUInt64(0) snmp_out_bytes,
    toFloat64(0) snmp_coverage,toUInt8(0) snmp_reset,toUInt8(0) snmp_gap FROM flow_values
  UNION ALL
  SELECT bucket,device_id,port_id,if_index,0,0,0,0,counter_in_bytes,counter_out_bytes,counter_in_coverage,counter_out_coverage,
    counter_in_reset,counter_out_reset,0,0,0,0,0 FROM counter_rollup
  UNION ALL
  SELECT bucket,device_id,port_id,if_index,0,0,0,0,0,0,0,0,0,0,snmp_in_bytes,snmp_out_bytes,snmp_coverage,snmp_reset,snmp_gap FROM snmp_values
)
SELECT bucket,device_id,port_id,if_index,
  sum(flow_in_bytes) flow_in_bytes,sum(flow_out_bytes) flow_out_bytes,sum(flow_records) flow_records,sum(flow_unknown) flow_unknown,
  sum(counter_in_bytes) counter_in_bytes,sum(counter_out_bytes) counter_out_bytes,max(counter_in_coverage) counter_in_coverage,max(counter_out_coverage) counter_out_coverage,
  max(counter_in_reset) counter_in_reset,max(counter_out_reset) counter_out_reset,
  sum(snmp_in_bytes) snmp_in_bytes,sum(snmp_out_bytes) snmp_out_bytes,max(snmp_coverage) snmp_coverage,max(snmp_reset) snmp_reset,max(snmp_gap) snmp_gap
FROM measurements GROUP BY bucket,device_id,port_id,if_index
ORDER BY bucket,device_id,if_index LIMIT {limit:UInt64}`
