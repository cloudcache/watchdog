// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type RollupResolution string

const (
	RollupOneMinute RollupResolution = "1m"
	RollupOneHour   RollupResolution = "1h"
)

type RollupRequest struct {
	TenantID    string
	Resolution  RollupResolution
	Bucket      time.Time
	Generation  uint64
	GeneratedAt time.Time
}

type RollupRunner struct {
	executor queryExecutor
}

// LatestGeneration reads the authoritative generation marker from
// ClickHouse. operation_jobs has finite retention, so historical repair must
// not infer generations from old job rows.
func (r *RollupRunner) LatestGeneration(ctx context.Context, tenantID string, resolution RollupResolution, bucket time.Time) (uint64, error) {
	duration, _, err := rollupTarget(resolution)
	if err != nil {
		return 0, Permanent(err)
	}
	request := RollupRequest{
		TenantID: tenantID, Resolution: resolution, Bucket: bucket,
		Generation: 1, GeneratedAt: bucket.UTC().Add(duration),
	}
	if err := ValidateRollupRequest(request); err != nil {
		return 0, Permanent(err)
	}
	if r == nil || r.executor == nil {
		return 0, Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	_, table, _ := rollupTarget(resolution)
	var generations proto.ColUInt64
	rows := 0
	query := ch.Query{
		Body: fmt.Sprintf(`SELECT max(generation) AS generation
FROM %s FINAL
WHERE tenant_id = {tenant:String}
  AND bucket = {bucket:DateTime('UTC')}
  AND dimension_kind = '_generation'`, table),
		Parameters: ch.Parameters(map[string]any{
			"tenant": tenantID, "bucket": bucket.UTC().Format("2006-01-02 15:04:05"),
		}),
		Result: proto.Results{{Name: "generation", Data: &generations}},
	}
	query.OnResult = func(_ context.Context, _ proto.Block) error {
		if generations.Rows() != 1 || rows != 0 {
			return Permanent(errors.New("ClickHouse rollup generation query returned an invalid row count"))
		}
		rows++
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return 0, classifyClickHouseError(fmt.Errorf("read ClickHouse %s rollup generation: %w", resolution, err))
	}
	if rows != 1 {
		return 0, Permanent(errors.New("ClickHouse rollup generation query returned no result"))
	}
	return generations[0], nil
}

func NewRollupRunner(native *NativeInserter) (*RollupRunner, error) {
	if native == nil || native.executor == nil {
		return nil, errors.New("ClickHouse native connection is required")
	}
	return &RollupRunner{executor: native.executor}, nil
}

// Run rebuilds one closed tenant bucket in one INSERT SELECT. Every public
// dimension and an internal generation marker are inserted atomically. A
// repair reuses the same request, or supplies a greater generation after late
// base records arrive.
func (r *RollupRunner) Run(ctx context.Context, request RollupRequest) error {
	query, err := buildRollupQuery(request)
	if err != nil {
		return Permanent(err)
	}
	if r == nil || r.executor == nil {
		return Permanent(errors.New("ClickHouse rollup runner is not initialized"))
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return classifyClickHouseError(fmt.Errorf("rebuild ClickHouse %s bucket: %w", request.Resolution, err))
	}
	return nil
}

// ValidateRollupRequest validates the public rollup contract without issuing
// ClickHouse I/O. Operation-job producers use it before enqueueing so a bad
// bucket can never consume the retry budget.
func ValidateRollupRequest(request RollupRequest) error {
	duration, _, err := rollupTarget(request.Resolution)
	if err != nil {
		return err
	}
	if !validRollupTenant(request.TenantID) || request.Generation == 0 || request.GeneratedAt.IsZero() {
		return errors.New("rollup tenant, generation, and generated_at are required")
	}
	bucket := request.Bucket.UTC()
	end := bucket.Add(duration)
	_, bucketOffset := request.Bucket.Zone()
	_, generatedOffset := request.GeneratedAt.Zone()
	if bucket.IsZero() || bucketOffset != 0 || generatedOffset != 0 || bucket.Truncate(duration) != bucket {
		return fmt.Errorf("%s rollup bucket and generated_at must be UTC and bucket-aligned", request.Resolution)
	}
	if request.GeneratedAt.UTC().Before(end) {
		return fmt.Errorf("%s rollup generated_at precedes the closed bucket end", request.Resolution)
	}
	return nil
}

func buildRollupQuery(request RollupRequest) (ch.Query, error) {
	duration, table, err := rollupTarget(request.Resolution)
	if err != nil {
		return ch.Query{}, err
	}
	if err := ValidateRollupRequest(request); err != nil {
		return ch.Query{}, err
	}
	bucket := request.Bucket.UTC()
	generatedAt := request.GeneratedAt.UTC()
	end := bucket.Add(duration)
	tokenInput := fmt.Sprintf("watchdog-flow-rollup-v1\x00%s\x00%s\x00%d\x00%d", request.TenantID, request.Resolution, bucket.Unix(), request.Generation)
	token := sha256.Sum256([]byte(tokenInput))
	query := ch.Query{
		Body: fmt.Sprintf(rollupSQL, table),
		Parameters: ch.Parameters(map[string]any{
			"tenant": request.TenantID, "bucket_start": bucket.Format("2006-01-02 15:04:05"),
			"bucket_end": end.Format("2006-01-02 15:04:05"), "generation": request.Generation,
			"generated_at": generatedAt.Format("2006-01-02 15:04:05.000"),
		}),
		Settings: []ch.Setting{
			{Key: "async_insert", Value: "0", Important: true},
			{Key: "wait_for_async_insert", Value: "1", Important: true},
			{Key: "insert_deduplication_token", Value: hex.EncodeToString(token[:]), Important: true},
		},
	}
	return query, nil
}

func rollupTarget(resolution RollupResolution) (time.Duration, string, error) {
	switch resolution {
	case RollupOneMinute:
		return time.Minute, "flow_aggregate_1m", nil
	case RollupOneHour:
		return time.Hour, "flow_aggregate_1h", nil
	default:
		return 0, "", fmt.Errorf("unsupported rollup resolution %q", resolution)
	}
}

func validRollupTenant(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("._:-", character) {
			continue
		}
		return false
	}
	return true
}

// The marker row makes an empty repair visible as the latest complete
// generation, so dimensions that disappeared do not leak from an older run.
// Public queries must filter dimension_kind and select max(generation) per
// tenant+bucket; _generation is internal and never exposed by the registry.
const rollupSQL = `INSERT INTO %s (
  bucket, tenant_id, target_id, device_id, exporter_id,
  business_direction, category, business, dimension_kind, dimension_value,
  dimension_snapshot_id, geo_version, classification_version,
  raw_bytes, raw_packets, estimated_bytes, estimated_packets,
  received_records, unknown_sampling_records, quality_records,
  generation, generated_at)
WITH
  {bucket_start:DateTime('UTC')} AS rollup_start,
  {bucket_end:DateTime('UTC')} AS rollup_end,
  {generation:UInt64} AS rollup_generation,
  {generated_at:DateTime64(3, 'UTC')} AS rollup_generated_at
SELECT
  rollup_start AS bucket,
  tenant_id,
  target_id,
  device_id,
  exporter_id,
  toString(business_direction) AS business_direction,
  toString(category) AS category,
  business,
  tupleElement(dimension, 1) AS dimension_kind,
  tupleElement(dimension, 2) AS dimension_value,
  dimension_snapshot_id,
  geo_version,
  classification_version,
  sum(raw_bytes) AS raw_bytes,
  sum(raw_packets) AS raw_packets,
  sum(estimated_bytes) AS estimated_bytes,
  sum(estimated_packets) AS estimated_packets,
  count() AS received_records,
  countIf(NOT estimated_valid) AS unknown_sampling_records,
  countIf(quality_flags != 0) AS quality_records,
  rollup_generation AS generation,
  rollup_generated_at AS generated_at
FROM flow_records FINAL
ARRAY JOIN arrayFilter(item -> tupleElement(item, 2) != '', arrayConcat(
  [
    tuple('total', 'total'),
    tuple('category', toString(category)),
    tuple('geo.continent', if(empty(remote_geo_continent_id), '_unassigned', remote_geo_continent_id)),
    tuple('geo.region', if(empty(remote_geo_region_id), '_unassigned', remote_geo_region_id)),
    tuple('geo.country', if(empty(remote_geo_country_id), '_unassigned', remote_geo_country_id)),
    tuple('geo.province', if(empty(remote_geo_province_id), '_unassigned', remote_geo_province_id)),
    tuple('geo.city', if(empty(remote_geo_city_id), '_unassigned', remote_geo_city_id)),
    tuple('isp', if(remote_isp_id = 0, '_unassigned', toString(remote_isp_id))),
    tuple('asn', if(remote_asn = 0, '_unassigned', toString(remote_asn))),
    tuple('business', if(empty(business), '_unassigned', business)),
    tuple('local_prefix', if(empty(local_prefix_id), '_unassigned', local_prefix_id)),
    tuple('remote_prefix', if(empty(remote_prefix_id), '_unassigned', remote_prefix_id)),
    tuple('src_ip', toString(src_ip)),
    tuple('dst_ip', toString(dst_ip)),
    tuple('remote_port', if(remote_port = 0, '_unassigned', toString(remote_port))),
    tuple('protocol', toString(ip_protocol)),
    tuple('observation_interface', if(observation_if_index = 0, '_unassigned', toString(observation_if_index)))
  ],
  arrayMap(value -> tuple('address_set', value), arrayDistinct(arrayConcat(local_address_set_ids, remote_address_set_ids)))
)) AS dimension
WHERE tenant_id = {tenant:String}
  AND event_time >= rollup_start
  AND event_time < rollup_end
  AND disposition = 'count'
GROUP BY
  tenant_id, target_id, device_id, exporter_id, business_direction, category,
  business, dimension_kind, dimension_value, dimension_snapshot_id,
  geo_version, classification_version
UNION ALL
SELECT
  rollup_start, {tenant:String}, '', '', '', 'ambiguous', 'unknown', '',
  '_generation', '', '', '', 0,
  0, 0, 0, 0, 0, 0, 0, rollup_generation, rollup_generated_at`
