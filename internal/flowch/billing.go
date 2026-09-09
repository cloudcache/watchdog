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

const maxFlowBillingBuckets = 120000

type BillingPortScope struct {
	DeviceID  string `json:"device_id"`
	IfIndex   uint32 `json:"if_index"`
	Direction string `json:"direction"`
}

type BillingLayerResult struct {
	Layer                  string              `json:"layer"`
	InBytes                uint64              `json:"in_bytes"`
	OutBytes               uint64              `json:"out_bytes"`
	SelectedBytes          uint64              `json:"selected_bytes"`
	SelectedRates          []float64           `json:"selected_rates"`
	Coverage               float64             `json:"coverage"`
	ExpectedBuckets        uint32              `json:"expected_buckets"`
	ObservedBuckets        uint32              `json:"observed_buckets"`
	UnknownSamplingRecords uint64              `json:"unknown_sampling_records"`
	RateBuckets            []BillingRateBucket `json:"rate_buckets,omitempty"`
}

// BillingRateBucket preserves the fixed five-minute grid and whether every
// contributing flow had usable sampling and layer facts. Billing uses only the
// common complete bucket intersection across Flow layers and SNMP.
type BillingRateBucket struct {
	Time        time.Time `json:"time"`
	SelectedBPS float64   `json:"selected_bps"`
	Observed    bool      `json:"observed"`
	Complete    bool      `json:"complete"`
}

type FlowBillingResult struct {
	From                   time.Time            `json:"from"`
	To                     time.Time            `json:"to"`
	Layers                 []BillingLayerResult `json:"layers"`
	SourceGenerationMin    uint64               `json:"source_generation_min"`
	SourceGenerationMax    uint64               `json:"source_generation_max"`
	DimensionSnapshotRefs  []string             `json:"dimension_snapshot_refs"`
	GeoVersionRefs         []string             `json:"geo_version_refs"`
	ClassificationVersions []string             `json:"classification_versions"`
}

type FlowBillingRequest struct {
	Ports      []BillingPortScope
	From       time.Time
	To         time.Time
	Now        time.Time
	MaxBuckets uint32
}

type flowBillingAccumulator struct {
	result   BillingLayerResult
	coverage float64
}

func (s *NativeInserter) ReadBilling(ctx context.Context, req FlowBillingRequest) (FlowBillingResult, error) {
	if s == nil || s.executor == nil {
		return FlowBillingResult{}, errors.New("Flow ClickHouse store is not initialized")
	}
	ports, err := normalizeFlowBillingPorts(req.Ports)
	if err != nil {
		return FlowBillingResult{}, err
	}
	from, to := req.From.UTC(), req.To.UTC()
	now := req.Now.UTC()
	if req.Now.IsZero() {
		now = time.Now().UTC()
	}
	if !to.After(from) || !from.Equal(from.Truncate(5*time.Minute)) || !to.Equal(to.Truncate(5*time.Minute)) || to.After(now.Truncate(5*time.Minute)) || to.Sub(from) > 400*24*time.Hour {
		return FlowBillingResult{}, errors.New("Flow billing range must be an aligned, closed interval of at most 400 days")
	}
	expected := uint32(to.Sub(from) / (5 * time.Minute))
	maxBuckets := req.MaxBuckets
	if maxBuckets == 0 {
		maxBuckets = maxFlowBillingBuckets
	}
	if expected == 0 || expected > maxBuckets || maxBuckets > maxFlowBillingBuckets {
		return FlowBillingResult{}, errors.New("Flow billing range exceeds bucket budget")
	}

	deviceIDs, ifIndexes, directions := new(proto.ColStr), new(proto.ColUInt32), new(proto.ColStr)
	for _, port := range ports {
		deviceIDs.Append(port.DeviceID)
		ifIndexes.Append(port.IfIndex)
		directions.Append(port.Direction)
	}
	var buckets proto.ColDateTime
	var rawIn, rawOut, rawSelected, supplierIn, supplierOut, supplierSelected, customerIn, customerOut, customerSelected proto.ColUInt64
	var rawObservations, rawKnown, supplierObservations, supplierKnown, customerObservations, customerKnown, generationMin, generationMax proto.ColUInt64
	var dimensionRefs, geoRefs, classificationRefs proto.ColStr
	query := ch.Query{
		Body: strings.NewReplacer(
			"{from_ms:Int64}", strconv.FormatInt(from.UnixMilli(), 10),
			"{to_ms:Int64}", strconv.FormatInt(to.UnixMilli(), 10),
			"{limit:UInt64}", strconv.FormatUint(uint64(maxBuckets)+1, 10),
		).Replace(flowBillingQuerySQL),
		ExternalTable: "flow_billing_scope",
		ExternalData: []proto.InputColumn{
			{Name: "device_id", Data: deviceIDs}, {Name: "if_index", Data: ifIndexes}, {Name: "direction", Data: directions},
		},
		Result: proto.Results{
			{Name: "bucket", Data: &buckets},
			{Name: "raw_in_bytes", Data: &rawIn}, {Name: "raw_out_bytes", Data: &rawOut}, {Name: "raw_selected_bytes", Data: &rawSelected},
			{Name: "supplier_in_bytes", Data: &supplierIn}, {Name: "supplier_out_bytes", Data: &supplierOut}, {Name: "supplier_selected_bytes", Data: &supplierSelected},
			{Name: "customer_in_bytes", Data: &customerIn}, {Name: "customer_out_bytes", Data: &customerOut}, {Name: "customer_selected_bytes", Data: &customerSelected},
			{Name: "raw_observations", Data: &rawObservations}, {Name: "raw_known", Data: &rawKnown},
			{Name: "supplier_observations", Data: &supplierObservations}, {Name: "supplier_known", Data: &supplierKnown},
			{Name: "customer_observations", Data: &customerObservations}, {Name: "customer_known", Data: &customerKnown},
			{Name: "generation_min", Data: &generationMin}, {Name: "generation_max", Data: &generationMax},
			{Name: "dimension_refs", Data: &dimensionRefs}, {Name: "geo_refs", Data: &geoRefs}, {Name: "classification_refs", Data: &classificationRefs},
		},
		Settings: []ch.Setting{
			{Key: "readonly", Value: "1", Important: true}, {Key: "max_execution_time", Value: "120", Important: true},
			{Key: "max_result_rows", Value: strconv.FormatUint(uint64(maxBuckets)+1, 10), Important: true}, {Key: "result_overflow_mode", Value: "throw", Important: true},
			{Key: "max_threads", Value: "8", Important: true}, {Key: "max_memory_usage", Value: "1073741824", Important: true},
		},
	}
	result := FlowBillingResult{From: from, To: to}
	layers := map[string]*flowBillingAccumulator{
		"raw":      {result: BillingLayerResult{Layer: "raw", ExpectedBuckets: expected}},
		"supplier": {result: BillingLayerResult{Layer: "supplier", ExpectedBuckets: expected}},
		"customer": {result: BillingLayerResult{Layer: "customer", ExpectedBuckets: expected}},
	}
	dimensionSet, geoSet, classificationSet := map[string]bool{}, map[string]bool{}, map[string]bool{}
	rowCount := 0
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if uint64(rowCount+block.Rows) > uint64(maxBuckets) {
			return errors.New("Flow billing query exceeds bucket budget")
		}
		for i := 0; i < block.Rows; i++ {
			rowCount++
			bucketTime := buckets.Row(i).UTC()
			accumulateFlowBillingLayer(layers["raw"], bucketTime, rawIn[i], rawOut[i], rawSelected[i], rawObservations[i], rawKnown[i])
			accumulateFlowBillingLayer(layers["supplier"], bucketTime, supplierIn[i], supplierOut[i], supplierSelected[i], supplierObservations[i], supplierKnown[i])
			accumulateFlowBillingLayer(layers["customer"], bucketTime, customerIn[i], customerOut[i], customerSelected[i], customerObservations[i], customerKnown[i])
			if generationMin[i] > 0 && (result.SourceGenerationMin == 0 || generationMin[i] < result.SourceGenerationMin) {
				result.SourceGenerationMin = generationMin[i]
			}
			if generationMax[i] > result.SourceGenerationMax {
				result.SourceGenerationMax = generationMax[i]
			}
			addCommaValues(dimensionSet, dimensionRefs.Row(i))
			addCommaValues(geoSet, geoRefs.Row(i))
			addCommaValues(classificationSet, classificationRefs.Row(i))
		}
		return nil
	}
	if err := s.executor.Do(ctx, query); err != nil {
		return FlowBillingResult{}, fmt.Errorf("read Flow billing data: %w", err)
	}
	for _, name := range []string{"raw", "supplier", "customer"} {
		item := layers[name]
		if expected > 0 {
			item.result.Coverage = item.coverage / float64(expected)
		}
		result.Layers = append(result.Layers, item.result)
	}
	result.DimensionSnapshotRefs = sortedSet(dimensionSet)
	result.GeoVersionRefs = sortedSet(geoSet)
	result.ClassificationVersions = sortedSet(classificationSet)
	return result, nil
}

func accumulateFlowBillingLayer(acc *flowBillingAccumulator, bucket time.Time, inBytes, outBytes, selectedBytes, observations, known uint64) {
	acc.result.InBytes += inBytes
	acc.result.OutBytes += outBytes
	acc.result.SelectedBytes += selectedBytes
	complete := observations > 0 && known == observations
	acc.result.RateBuckets = append(acc.result.RateBuckets, BillingRateBucket{
		Time: bucket, SelectedBPS: float64(selectedBytes) * 8 / 300, Observed: observations > 0, Complete: complete,
	})
	if observations > 0 {
		acc.result.ObservedBuckets++
		if complete {
			acc.result.SelectedRates = append(acc.result.SelectedRates, float64(selectedBytes)*8/300)
		}
		acc.coverage += float64(known) / float64(observations)
		if observations > known {
			acc.result.UnknownSamplingRecords += observations - known
		}
	}
}

func normalizeFlowBillingPorts(input []BillingPortScope) ([]BillingPortScope, error) {
	if len(input) == 0 || len(input) > 1000 {
		return nil, errors.New("Flow billing requires 1..1000 ports")
	}
	seen := map[string]BillingPortScope{}
	for _, item := range input {
		item.DeviceID, item.Direction = strings.TrimSpace(item.DeviceID), strings.TrimSpace(item.Direction)
		if item.DeviceID == "" || item.IfIndex == 0 || (item.Direction != "in" && item.Direction != "out" && item.Direction != "agg") {
			return nil, errors.New("Flow billing port scope is invalid")
		}
		key := item.DeviceID + "\x00" + strconv.FormatUint(uint64(item.IfIndex), 10)
		if previous, ok := seen[key]; ok && previous.Direction != item.Direction {
			return nil, errors.New("Flow billing port has conflicting directions")
		}
		seen[key] = item
	}
	out := make([]BillingPortScope, 0, len(seen))
	for _, item := range seen {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DeviceID != out[j].DeviceID {
			return out[i].DeviceID < out[j].DeviceID
		}
		return out[i].IfIndex < out[j].IfIndex
	})
	return out, nil
}

func addCommaValues(set map[string]bool, input string) {
	for _, item := range strings.Split(input, ",") {
		if item = strings.TrimSpace(item); item != "" {
			set[item] = true
		}
	}
}

func sortedSet(set map[string]bool) []string {
	result := make([]string, 0, len(set))
	for item := range set {
		result = append(result, item)
	}
	sort.Strings(result)
	return result
}

const flowBillingQuerySQL = `WITH buckets AS (
  SELECT addMinutes(toDateTime(fromUnixTimestamp64Milli({from_ms:Int64})),number*5) bucket_start
  FROM numbers(toUInt64(dateDiff('minute',fromUnixTimestamp64Milli({from_ms:Int64}),fromUnixTimestamp64Milli({to_ms:Int64}))/5))
), values AS (
  SELECT toStartOfFiveMinutes(records.event_time) bucket_start,
    sumIf(records.estimated_bytes,records.estimated_valid AND records.ingress_if_index=scope.if_index) raw_in_bytes,
    sumIf(records.estimated_bytes,records.estimated_valid AND records.egress_if_index=scope.if_index) raw_out_bytes,
    sumIf(records.estimated_bytes,records.estimated_valid AND ((scope.direction IN ('in','agg') AND records.ingress_if_index=scope.if_index) OR (scope.direction IN ('out','agg') AND records.egress_if_index=scope.if_index))) raw_selected_bytes,
    sumIf(records.estimated_bytes,records.estimated_valid AND records.fact_schema>=2 AND records.ingress_if_index=scope.if_index) supplier_in_bytes,
    sumIf(records.estimated_bytes,records.estimated_valid AND records.fact_schema>=2 AND records.egress_if_index=scope.if_index) supplier_out_bytes,
    sumIf(records.estimated_bytes,records.estimated_valid AND records.fact_schema>=2 AND ((scope.direction IN ('in','agg') AND records.ingress_if_index=scope.if_index) OR (scope.direction IN ('out','agg') AND records.egress_if_index=scope.if_index))) supplier_selected_bytes,
    sumIf(records.estimated_bytes,records.estimated_valid AND records.disposition='count' AND records.ingress_if_index=scope.if_index) customer_in_bytes,
    sumIf(records.estimated_bytes,records.estimated_valid AND records.disposition='count' AND records.egress_if_index=scope.if_index) customer_out_bytes,
    sumIf(records.estimated_bytes,records.estimated_valid AND records.disposition='count' AND ((scope.direction IN ('in','agg') AND records.ingress_if_index=scope.if_index) OR (scope.direction IN ('out','agg') AND records.egress_if_index=scope.if_index))) customer_selected_bytes,
    sum(toUInt64(scope.direction IN ('in','agg') AND records.ingress_if_index=scope.if_index)+toUInt64(scope.direction IN ('out','agg') AND records.egress_if_index=scope.if_index)) raw_observations,
    sumIf(toUInt64(scope.direction IN ('in','agg') AND records.ingress_if_index=scope.if_index)+toUInt64(scope.direction IN ('out','agg') AND records.egress_if_index=scope.if_index),records.estimated_valid) raw_known,
    raw_observations supplier_observations,
    sumIf(toUInt64(scope.direction IN ('in','agg') AND records.ingress_if_index=scope.if_index)+toUInt64(scope.direction IN ('out','agg') AND records.egress_if_index=scope.if_index),records.fact_schema>=2 AND records.estimated_valid) supplier_known,
    raw_observations customer_observations,raw_known customer_known,
    min(records.ingest_generation) generation_min,max(records.ingest_generation) generation_max,
    arrayStringConcat(arraySort(groupUniqArray(records.dimension_snapshot_id)),',') dimension_refs,
    arrayStringConcat(arraySort(groupUniqArray(records.geo_version)),',') geo_refs,
    arrayStringConcat(arrayMap(x -> toString(x),arraySort(groupUniqArray(records.classification_version))),',') classification_refs
  FROM flow_records AS records FINAL
  CROSS JOIN flow_billing_scope AS scope
  WHERE records.event_time>=fromUnixTimestamp64Milli({from_ms:Int64})
    AND records.event_time<fromUnixTimestamp64Milli({to_ms:Int64})
    AND records.device_id=scope.device_id
    AND (records.ingress_if_index=scope.if_index OR records.egress_if_index=scope.if_index)
  GROUP BY bucket_start
)
SELECT buckets.bucket_start bucket,
  ifNull(values.raw_in_bytes,0) raw_in_bytes,ifNull(values.raw_out_bytes,0) raw_out_bytes,ifNull(values.raw_selected_bytes,0) raw_selected_bytes,
  ifNull(values.supplier_in_bytes,0) supplier_in_bytes,ifNull(values.supplier_out_bytes,0) supplier_out_bytes,ifNull(values.supplier_selected_bytes,0) supplier_selected_bytes,
  ifNull(values.customer_in_bytes,0) customer_in_bytes,ifNull(values.customer_out_bytes,0) customer_out_bytes,ifNull(values.customer_selected_bytes,0) customer_selected_bytes,
  ifNull(values.raw_observations,0) raw_observations,ifNull(values.raw_known,0) raw_known,
  ifNull(values.supplier_observations,0) supplier_observations,ifNull(values.supplier_known,0) supplier_known,
  ifNull(values.customer_observations,0) customer_observations,ifNull(values.customer_known,0) customer_known,
  ifNull(values.generation_min,0) generation_min,ifNull(values.generation_max,0) generation_max,
  ifNull(values.dimension_refs,'') dimension_refs,ifNull(values.geo_refs,'') geo_refs,ifNull(values.classification_refs,'') classification_refs
FROM buckets LEFT JOIN values ON values.bucket_start=buckets.bucket_start
ORDER BY buckets.bucket_start LIMIT {limit:UInt64}`
