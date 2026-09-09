package snmpch

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

const maxBillingBuckets = 120000

type BillingPort struct {
	PortID    string `json:"port_id"`
	Direction string `json:"direction"` // in | out | agg
}

type BillingRequest struct {
	Ports      []BillingPort `json:"ports"`
	From       time.Time     `json:"from"`
	To         time.Time     `json:"to"`
	Now        time.Time     `json:"-"`
	MaxBuckets uint32        `json:"max_buckets"`
}

type BillingBucket struct {
	Time          time.Time `json:"time"`
	InBytes       uint64    `json:"in_bytes"`
	OutBytes      uint64    `json:"out_bytes"`
	SelectedBytes uint64    `json:"selected_bytes"`
	InBPS         float64   `json:"in_bps"`
	OutBPS        float64   `json:"out_bps"`
	SelectedBPS   float64   `json:"selected_bps"`
	Coverage      float64   `json:"coverage"`
	Reset         bool      `json:"reset"`
	Gap           bool      `json:"gap"`
	Generation    uint64    `json:"generation"`
	PresentPorts  uint32    `json:"present_ports"`
}

// BillingResult is immutable evidence derived only from published, closed 5m
// buckets. KISS-07 may persist it into billing_period_values, but must not
// recompute its counter math or query the raw sample table independently.
type BillingResult struct {
	From                time.Time       `json:"from"`
	To                  time.Time       `json:"to"`
	Buckets             []BillingBucket `json:"buckets,omitempty"`
	ExpectedBuckets     uint32          `json:"expected_buckets"`
	ObservedBuckets     uint32          `json:"observed_buckets"`
	Coverage            float64         `json:"coverage"`
	ResetBuckets        uint32          `json:"reset_buckets"`
	GapBuckets          uint32          `json:"gap_buckets"`
	TotalInBytes        uint64          `json:"total_in_bytes"`
	TotalOutBytes       uint64          `json:"total_out_bytes"`
	TotalSelectedBytes  uint64          `json:"total_selected_bytes"`
	Rate95In            float64         `json:"rate_95th_in_bps"`
	Rate95Out           float64         `json:"rate_95th_out_bps"`
	Rate95Selected      float64         `json:"rate_95th_selected_bps"`
	RateAverageIn       float64         `json:"rate_average_in_bps"`
	RateAverageOut      float64         `json:"rate_average_out_bps"`
	RateAverageSelected float64         `json:"rate_average_selected_bps"`
	GenerationMin       uint64          `json:"generation_min"`
	GenerationMax       uint64          `json:"generation_max"`
	ExpectedPorts       uint32          `json:"expected_ports"`
}

func (s *Store) ReadBilling(ctx context.Context, req BillingRequest) (BillingResult, error) {
	if s == nil || s.exec == nil {
		return BillingResult{}, errors.New("SNMP ClickHouse store is not initialized")
	}
	ports, err := normalizeBillingPorts(req.Ports)
	if err != nil {
		return BillingResult{}, err
	}
	from, to := req.From.UTC(), req.To.UTC()
	now := req.Now.UTC()
	if req.Now.IsZero() {
		now = time.Now().UTC()
	}
	if !to.After(from) || !from.Equal(from.Truncate(interfaceBucket)) || !to.Equal(to.Truncate(interfaceBucket)) || to.After(now.Truncate(interfaceBucket)) || to.Sub(from) > 400*24*time.Hour {
		return BillingResult{}, errors.New("SNMP billing range must be an aligned, closed interval of at most 400 days")
	}
	expected := uint32(to.Sub(from) / interfaceBucket)
	maxBuckets := req.MaxBuckets
	if maxBuckets == 0 {
		maxBuckets = maxBillingBuckets
	}
	if expected == 0 || expected > maxBuckets || maxBuckets > maxBillingBuckets {
		return BillingResult{}, errors.New("SNMP billing range exceeds bucket budget")
	}

	portIDs := new(proto.ColStr)
	directions := new(proto.ColStr)
	for _, port := range ports {
		portIDs.Append(port.PortID)
		directions.Append(port.Direction)
	}
	var buckets proto.ColDateTime
	var inBytes, outBytes, selectedBytes, generations, presentPorts proto.ColUInt64
	var inBPS, outBPS, selectedBPS, coverage proto.ColFloat64
	var reset, gap proto.ColUInt8
	query := ch.Query{
		Body: strings.NewReplacer(
			"{from_ms:Int64}", strconv.FormatInt(from.UnixMilli(), 10),
			"{to_ms:Int64}", strconv.FormatInt(to.UnixMilli(), 10),
			"{limit:UInt64}", strconv.FormatUint(uint64(maxBuckets)+1, 10),
		).Replace(billingQuerySQL),
		ExternalTable: "snmp_billing_scope",
		ExternalData:  []proto.InputColumn{{Name: "port_id", Data: portIDs}, {Name: "direction", Data: directions}},
		Result: proto.Results{
			{Name: "bucket", Data: &buckets}, {Name: "in_bytes", Data: &inBytes}, {Name: "out_bytes", Data: &outBytes},
			{Name: "selected_bytes", Data: &selectedBytes}, {Name: "in_bps", Data: &inBPS}, {Name: "out_bps", Data: &outBPS},
			{Name: "selected_bps", Data: &selectedBPS}, {Name: "coverage", Data: &coverage}, {Name: "reset_flag", Data: &reset},
			{Name: "gap_flag", Data: &gap}, {Name: "generation", Data: &generations}, {Name: "present_ports", Data: &presentPorts},
		},
		Settings: snmpQuerySettings(maxBuckets),
	}
	result := BillingResult{From: from, To: to, ExpectedBuckets: expected, ExpectedPorts: uint32(len(ports))}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if uint64(len(result.Buckets)+block.Rows) > uint64(maxBuckets) {
			return errors.New("SNMP billing query exceeds bucket budget")
		}
		for i := 0; i < block.Rows; i++ {
			result.Buckets = append(result.Buckets, BillingBucket{
				Time: buckets.Row(i).UTC(), InBytes: inBytes[i], OutBytes: outBytes[i], SelectedBytes: selectedBytes[i],
				InBPS: inBPS[i], OutBPS: outBPS[i], SelectedBPS: selectedBPS[i], Coverage: coverage[i],
				Reset: reset[i] != 0, Gap: gap[i] != 0, Generation: generations[i], PresentPorts: uint32(presentPorts[i]),
			})
		}
		return nil
	}
	if err := s.exec.Do(ctx, query); err != nil {
		return BillingResult{}, fmt.Errorf("read SNMP billing data: %w", err)
	}
	finishBillingResult(&result)
	return result, nil
}

func normalizeBillingPorts(input []BillingPort) ([]BillingPort, error) {
	if len(input) == 0 || len(input) > maxAggregateScopes {
		return nil, errors.New("SNMP billing requires 1..1000 ports")
	}
	seen := make(map[string]string, len(input))
	for _, item := range input {
		item.PortID = strings.TrimSpace(item.PortID)
		item.Direction = strings.TrimSpace(item.Direction)
		if item.PortID == "" || (item.Direction != "in" && item.Direction != "out" && item.Direction != "agg") {
			return nil, errors.New("SNMP billing port and direction are invalid")
		}
		if previous, ok := seen[item.PortID]; ok && previous != item.Direction {
			return nil, errors.New("SNMP billing port has conflicting directions")
		}
		seen[item.PortID] = item.Direction
	}
	out := make([]BillingPort, 0, len(seen))
	for portID, direction := range seen {
		out = append(out, BillingPort{PortID: portID, Direction: direction})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PortID < out[j].PortID })
	return out, nil
}

func finishBillingResult(result *BillingResult) {
	if result == nil {
		return
	}
	inRates := make([]float64, 0, len(result.Buckets))
	outRates := make([]float64, 0, len(result.Buckets))
	selectedRates := make([]float64, 0, len(result.Buckets))
	var coverage float64
	for i, bucket := range result.Buckets {
		if bucket.PresentPorts > 0 {
			result.ObservedBuckets++
		}
		result.TotalInBytes += bucket.InBytes
		result.TotalOutBytes += bucket.OutBytes
		result.TotalSelectedBytes += bucket.SelectedBytes
		inRates = append(inRates, bucket.InBPS)
		outRates = append(outRates, bucket.OutBPS)
		selectedRates = append(selectedRates, bucket.SelectedBPS)
		coverage += math.Max(0, math.Min(1, bucket.Coverage))
		if bucket.Reset {
			result.ResetBuckets++
		}
		if bucket.Gap {
			result.GapBuckets++
		}
		if i == 0 || bucket.Generation < result.GenerationMin {
			result.GenerationMin = bucket.Generation
		}
		if bucket.Generation > result.GenerationMax {
			result.GenerationMax = bucket.Generation
		}
	}
	if result.ExpectedBuckets > 0 {
		result.Coverage = coverage / float64(result.ExpectedBuckets)
	}
	result.Rate95In, result.RateAverageIn = exact95AndAverage(inRates)
	result.Rate95Out, result.RateAverageOut = exact95AndAverage(outRates)
	result.Rate95Selected, result.RateAverageSelected = exact95AndAverage(selectedRates)
}

func exact95AndAverage(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	index := int(math.Ceil(0.95*float64(len(ordered)))) - 1
	if index < 0 {
		index = 0
	}
	var sum float64
	for _, value := range values {
		sum += value
	}
	return ordered[index], sum / float64(len(values))
}

const billingQuerySQL = `WITH buckets AS (
  SELECT addMinutes(toDateTime(fromUnixTimestamp64Milli({from_ms:Int64})),number*5) bucket_start
  FROM numbers(toUInt64(dateDiff('minute',fromUnixTimestamp64Milli({from_ms:Int64}),fromUnixTimestamp64Milli({to_ms:Int64}))/5))
), published AS (
  SELECT bucket_start,generation
  FROM snmp_interface_traffic_5m FINAL
  WHERE row_kind='generation'
    AND bucket_start>=fromUnixTimestamp64Milli({from_ms:Int64})
    AND bucket_start<fromUnixTimestamp64Milli({to_ms:Int64})
), values AS (
  SELECT bucket_start,device_id,port_id,in_bytes,out_bytes,in_bps,out_bps,
    reset_flag,gap_flag,coverage,generation
  FROM snmp_interface_traffic_5m FINAL
  WHERE row_kind='value'
    AND bucket_start>=fromUnixTimestamp64Milli({from_ms:Int64})
    AND bucket_start<fromUnixTimestamp64Milli({to_ms:Int64})
)
SELECT buckets.bucket_start bucket,
  sum(values.in_bytes) in_bytes,sum(values.out_bytes) out_bytes,
  sum(multiIf(scope.direction='in',values.in_bytes,scope.direction='out',values.out_bytes,values.in_bytes+values.out_bytes)) selected_bytes,
  sum(values.in_bps) in_bps,sum(values.out_bps) out_bps,
  sum(multiIf(scope.direction='in',values.in_bps,scope.direction='out',values.out_bps,values.in_bps+values.out_bps)) selected_bps,
  avg(if(values.port_id='',0,values.coverage)) coverage,
  max(values.reset_flag) reset_flag,
  toUInt8(countIf(values.port_id!='')<count() OR max(values.gap_flag)>0 OR min(if(values.port_id='',0,values.coverage))<0.999999) gap_flag,
  max(values.generation) generation,
  countIf(values.port_id!='') present_ports
FROM buckets
CROSS JOIN snmp_billing_scope AS scope
LEFT JOIN published ON buckets.bucket_start=published.bucket_start
LEFT JOIN values ON values.bucket_start=buckets.bucket_start
  AND values.generation=published.generation AND values.port_id=scope.port_id
GROUP BY buckets.bucket_start ORDER BY buckets.bucket_start LIMIT {limit:UInt64}`
