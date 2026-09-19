// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

const (
	defaultReclassificationPageRows = 5_000
	maxReclassificationPageRows     = 50_000
)

type ReclassificationSpec struct {
	ID                          string
	Generation                  uint64
	SourcePublicationID         string
	TargetPublicationID         string
	View                        string
	WindowStart                 time.Time
	WindowEnd                   time.Time
	SourceDimensionSnapshotID   string
	SourceClassificationVersion uint32
}

type ReclassificationCursor struct {
	Set            bool      `json:"set"`
	EventTime      time.Time `json:"event_time,omitempty"`
	SourceStreamID string    `json:"source_stream_id,omitempty"`
	KafkaPartition uint32    `json:"kafka_partition,omitempty"`
	KafkaOffset    uint64    `json:"kafka_offset,omitempty"`
	RecordIndex    uint32    `json:"record_index,omitempty"`
}

type ReclassificationEvidence struct {
	RecordCount           uint64 `json:"record_count"`
	RawBytes              string `json:"raw_bytes"`
	RawPackets            string `json:"raw_packets"`
	EstimatedBytes        string `json:"estimated_bytes"`
	EstimatedPackets      string `json:"estimated_packets"`
	EstimatedValidRecords uint64 `json:"estimated_valid_records"`
}

func (e ReclassificationEvidence) Equal(other ReclassificationEvidence) bool {
	return e.RecordCount == other.RecordCount && e.RawBytes == other.RawBytes &&
		e.RawPackets == other.RawPackets && e.EstimatedBytes == other.EstimatedBytes &&
		e.EstimatedPackets == other.EstimatedPackets && e.EstimatedValidRecords == other.EstimatedValidRecords
}

type ReclassificationPage struct {
	Batches []*flowworker.EnrichedBatch
	Records []*flowworker.Record
	Cursor  ReclassificationCursor
	Done    bool
}

type ReclassificationRunner struct{ executor queryExecutor }

func NewReclassificationRunner(native *NativeInserter) (*ReclassificationRunner, error) {
	if native == nil || native.executor == nil {
		return nil, errors.New("ClickHouse native connection is required")
	}
	return &ReclassificationRunner{executor: native.executor}, nil
}

func (r *ReclassificationRunner) Ready(ctx context.Context) error {
	if r == nil || r.executor == nil {
		return errors.New("ClickHouse reclassification runner is not initialized")
	}
	var records, markers proto.ColUInt64
	found := false
	query := ch.Query{
		Body: `SELECT
countIf(table='flow_reclassified_records' AND name IN ('reclassification_id','reclassification_generation','source_stream_id','kafka_partition','kafka_offset','record_index')) AS required_records,
countIf(table='flow_reclassification_generations' AND name IN ('reclassification_id','generation','record_count','raw_bytes','raw_packets','estimated_bytes','estimated_packets','estimated_valid_records')) AS required_markers
FROM system.columns WHERE database=currentDatabase() AND table IN ('flow_reclassified_records','flow_reclassification_generations')`,
		Result: proto.Results{{Name: "required_records", Data: &records}, {Name: "required_markers", Data: &markers}},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if found || block.Rows != 1 {
			return errors.New("reclassification readiness returned an invalid row count")
		}
		found = true
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return fmt.Errorf("verify ClickHouse reclassification schema: %w", err)
	}
	if !found || records[0] != 6 || markers[0] != 8 {
		return fmt.Errorf("ClickHouse reclassification schema is incomplete (records=%d/6 markers=%d/8)", columnOrZero(records), columnOrZero(markers))
	}
	return nil
}

func ValidateReclassificationSpec(spec ReclassificationSpec) error {
	start, end := spec.WindowStart.UTC(), spec.WindowEnd.UTC()
	_, startOffset := spec.WindowStart.Zone()
	_, endOffset := spec.WindowEnd.Zone()
	if !validCandidateIdentifier(spec.ID) || spec.Generation == 0 || !validCandidateIdentifier(spec.SourcePublicationID) ||
		!validCandidateIdentifier(spec.TargetPublicationID) || !validCandidateIdentifier(spec.SourceDimensionSnapshotID) ||
		spec.SourceClassificationVersion == 0 || (spec.View != "customer" && spec.View != "supplier") {
		return errors.New("reclassification identity, publications, source version, generation, and view are required")
	}
	if start.IsZero() || end.IsZero() || startOffset != 0 || endOffset != 0 || !start.Before(end) || end.Sub(start) > 31*24*time.Hour {
		return errors.New("reclassification window must be UTC, non-empty, and no longer than 31 days")
	}
	return nil
}

func (r *ReclassificationRunner) SourceEvidence(ctx context.Context, spec ReclassificationSpec) (ReclassificationEvidence, error) {
	if err := ValidateReclassificationSpec(spec); err != nil {
		return ReclassificationEvidence{}, Permanent(err)
	}
	return r.evidence(ctx, `flow_records FINAL`, `dimension_snapshot_id = {source_dimension:String} AND classification_version = {source_classification:UInt32}`, spec)
}

func (r *ReclassificationRunner) OutputEvidence(ctx context.Context, spec ReclassificationSpec) (ReclassificationEvidence, error) {
	if err := ValidateReclassificationSpec(spec); err != nil {
		return ReclassificationEvidence{}, Permanent(err)
	}
	return r.evidence(ctx, `flow_reclassified_records FINAL`, `reclassification_id = {id:String} AND reclassification_generation = {generation:UInt64}`, spec)
}

func (r *ReclassificationRunner) evidence(ctx context.Context, table, extra string, spec ReclassificationSpec) (ReclassificationEvidence, error) {
	if r == nil || r.executor == nil {
		return ReclassificationEvidence{}, Permanent(errors.New("ClickHouse reclassification runner is not initialized"))
	}
	var count, valid proto.ColUInt64
	rawBytes, rawPackets, estimatedBytes, estimatedPackets := new(proto.ColStr), new(proto.ColStr), new(proto.ColStr), new(proto.ColStr)
	found := false
	query := ch.Query{
		Body: `SELECT count() AS record_count, toString(sum(toDecimal256(raw_bytes,0))) AS raw_bytes,
  toString(sum(toDecimal256(raw_packets,0))) AS raw_packets,
  toString(sumIf(toDecimal256(estimated_bytes,0), estimated_valid)) AS estimated_bytes,
  toString(sumIf(toDecimal256(estimated_packets,0), estimated_valid)) AS estimated_packets,
  countIf(estimated_valid) AS estimated_valid_records
FROM ` + table + `
WHERE event_time >= {window_start:DateTime64(3,'UTC')} AND event_time < {window_end:DateTime64(3,'UTC')}
  AND ` + extra,
		Parameters: reclassificationParameters(spec),
		Result: proto.Results{
			{Name: "record_count", Data: &count}, {Name: "raw_bytes", Data: rawBytes}, {Name: "raw_packets", Data: rawPackets},
			{Name: "estimated_bytes", Data: estimatedBytes}, {Name: "estimated_packets", Data: estimatedPackets},
			{Name: "estimated_valid_records", Data: &valid},
		},
	}
	var result ReclassificationEvidence
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if found || block.Rows != 1 {
			return errors.New("reclassification evidence returned an invalid row count")
		}
		found = true
		result = ReclassificationEvidence{RecordCount: count[0], RawBytes: rawBytes.Row(0), RawPackets: rawPackets.Row(0),
			EstimatedBytes: estimatedBytes.Row(0), EstimatedPackets: estimatedPackets.Row(0), EstimatedValidRecords: valid[0]}
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return ReclassificationEvidence{}, classifyClickHouseError(fmt.Errorf("read reclassification evidence: %w", err))
	}
	if !found {
		return ReclassificationEvidence{}, Permanent(errors.New("reclassification evidence returned no row"))
	}
	return result, nil
}

func (r *ReclassificationRunner) ReadSourcePage(ctx context.Context, spec ReclassificationSpec, cursor ReclassificationCursor, limit int) (ReclassificationPage, error) {
	if err := ValidateReclassificationSpec(spec); err != nil {
		return ReclassificationPage{}, Permanent(err)
	}
	if limit == 0 {
		limit = defaultReclassificationPageRows
	}
	if limit < 1 || limit > maxReclassificationPageRows {
		return ReclassificationPage{}, Permanent(errors.New("reclassification page limit must be 1..50000"))
	}
	if r == nil || r.executor == nil {
		return ReclassificationPage{}, Permanent(errors.New("ClickHouse reclassification runner is not initialized"))
	}
	columns := newReclassificationSourceColumns()
	params := reclassificationParameters(spec)
	params = append(params, ch.Parameters(map[string]any{
		"cursor_set": boolToUInt8(cursor.Set), "cursor_time": cursorTime(spec, cursor), "cursor_stream": cursor.SourceStreamID,
		"cursor_partition": cursor.KafkaPartition, "cursor_offset": cursor.KafkaOffset, "cursor_index": cursor.RecordIndex,
	})...)
	query := ch.Query{
		Body: reclassificationSourcePageSQL + strconv.Itoa(limit), Parameters: params,
		Settings: []ch.Setting{{Key: "readonly", Value: "1", Important: true}, {Key: "max_threads", Value: "4", Important: true}},
		Result:   columns.results(),
	}
	page := ReclassificationPage{Batches: make([]*flowworker.EnrichedBatch, 0, limit), Records: make([]*flowworker.Record, 0, limit)}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		for row := 0; row < block.Rows; row++ {
			batch, record := columns.batch(row)
			page.Batches = append(page.Batches, batch)
			page.Records = append(page.Records, record)
			page.Cursor = columns.cursor(row)
		}
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return ReclassificationPage{}, classifyClickHouseError(fmt.Errorf("read reclassification source page: %w", err))
	}
	page.Done = len(page.Batches) < limit
	return page, nil
}

func (r *ReclassificationRunner) InsertPage(ctx context.Context, spec ReclassificationSpec, pageNumber uint64, batches []*flowworker.EnrichedBatch) error {
	if err := ValidateReclassificationSpec(spec); err != nil {
		return Permanent(err)
	}
	if len(batches) == 0 || pageNumber == 0 {
		return Permanent(errors.New("reclassification insert page and rows are required"))
	}
	block := PreparedBlock{SourceStreamID: "reclassification", KafkaPartition: 0, FirstOffset: 0, LastOffset: 0}
	for _, batch := range batches {
		if batch == nil || len(batch.Records) != 1 {
			return Permanent(errors.New("reclassification page contains an invalid source row"))
		}
		block.Records = append(block.Records, RecordRef{Batch: batch, Record: &batch.Records[0]})
	}
	input, err := buildRecordInput(block)
	if err != nil {
		return Permanent(err)
	}
	ids := new(proto.ColStr)
	var generations proto.ColUInt64
	for range batches {
		ids.Append(spec.ID)
		generations.Append(spec.Generation)
	}
	input = append(proto.Input{{Name: "reclassification_id", Data: ids}, {Name: "reclassification_generation", Data: &generations}}, input...)
	token := fmt.Sprintf("flow-reclass-v1:%s:%d:%d", spec.ID, spec.Generation, pageNumber)
	if err := r.executor.Do(ctx, insertQuery("flow_reclassified_records", token, input)); err != nil {
		return classifyClickHouseError(fmt.Errorf("insert reclassified Flow page: %w", err))
	}
	return nil
}

// CoordinateDiff proves one-to-one coverage with the natural Kafka identity.
// It deliberately does not add a per-record content hash to the ingest path.
func (r *ReclassificationRunner) CoordinateDiff(ctx context.Context, spec ReclassificationSpec) (uint64, error) {
	if err := ValidateReclassificationSpec(spec); err != nil {
		return 0, Permanent(err)
	}
	var count proto.ColUInt64
	found := false
	query := ch.Query{Body: reclassificationCoordinateDiffSQL, Parameters: reclassificationParameters(spec), Result: proto.Results{{Name: "mismatches", Data: &count}}}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if found || block.Rows != 1 {
			return errors.New("reclassification coordinate comparison returned an invalid row count")
		}
		found = true
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return 0, classifyClickHouseError(fmt.Errorf("compare reclassification coordinates: %w", err))
	}
	if !found {
		return 0, Permanent(errors.New("reclassification coordinate comparison returned no row"))
	}
	return count[0], nil
}

func (r *ReclassificationRunner) WriteCompletionMarker(ctx context.Context, spec ReclassificationSpec) error {
	if err := ValidateReclassificationSpec(spec); err != nil {
		return Permanent(err)
	}
	query := ch.Query{Body: reclassificationMarkerSQL, Parameters: reclassificationParameters(spec), Settings: []ch.Setting{
		{Key: "async_insert", Value: "0", Important: true}, {Key: "wait_for_async_insert", Value: "1", Important: true},
		{Key: "insert_deduplication_token", Value: fmt.Sprintf("flow-reclass-marker-v1:%s:%d", spec.ID, spec.Generation), Important: true},
	}}
	if err := r.executor.Do(ctx, query); err != nil {
		return classifyClickHouseError(fmt.Errorf("write reclassification completion marker: %w", err))
	}
	return nil
}

func (r *ReclassificationRunner) MarkerExists(ctx context.Context, spec ReclassificationSpec) (bool, error) {
	var count proto.ColUInt64
	query := ch.Query{Body: `SELECT count() FROM flow_reclassification_generations FINAL WHERE reclassification_id={id:String} AND generation={generation:UInt64}`,
		Parameters: reclassificationParameters(spec), Result: proto.Results{{Name: "count()", Data: &count}}}
	found := false
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if found || block.Rows != 1 {
			return errors.New("reclassification marker lookup returned an invalid row count")
		}
		found = true
		return nil
	}
	if err := r.executor.Do(ctx, query); err != nil {
		return false, classifyClickHouseError(err)
	}
	return found && count[0] == 1, nil
}

func reclassificationParameters(spec ReclassificationSpec) []proto.Parameter {
	return ch.Parameters(map[string]any{
		"id": spec.ID, "generation": spec.Generation, "source_publication": spec.SourcePublicationID,
		"target_publication": spec.TargetPublicationID, "view": spec.View,
		"window_start":     spec.WindowStart.UTC().Format("2006-01-02 15:04:05.000"),
		"window_end":       spec.WindowEnd.UTC().Format("2006-01-02 15:04:05.000"),
		"source_dimension": spec.SourceDimensionSnapshotID, "source_classification": spec.SourceClassificationVersion,
	})
}

func boolToUInt8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}
func cursorTime(spec ReclassificationSpec, cursor ReclassificationCursor) string {
	if !cursor.Set {
		return spec.WindowStart.UTC().Format("2006-01-02 15:04:05.000")
	}
	return cursor.EventTime.UTC().Format("2006-01-02 15:04:05.000")
}

type reclassificationSourceColumns struct {
	eventTime, receivedTime *proto.ColDateTime64
	strings                 map[string]*proto.ColStr
	u64                     map[string]*proto.ColUInt64
	u32                     map[string]*proto.ColUInt32
	u16                     map[string]*proto.ColUInt16
	u8                      map[string]*proto.ColUInt8
	bools                   map[string]*proto.ColBool
	ips                     map[string]*proto.ColIPv6
}

func newReclassificationSourceColumns() *reclassificationSourceColumns {
	return &reclassificationSourceColumns{eventTime: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli), receivedTime: new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli),
		strings: map[string]*proto.ColStr{}, u64: map[string]*proto.ColUInt64{}, u32: map[string]*proto.ColUInt32{}, u16: map[string]*proto.ColUInt16{}, u8: map[string]*proto.ColUInt8{}, bools: map[string]*proto.ColBool{}, ips: map[string]*proto.ColIPv6{}}
}

func (c *reclassificationSourceColumns) results() proto.Results {
	result := proto.Results{{Name: "event_time", Data: c.eventTime}, {Name: "received_time", Data: c.receivedTime}}
	for _, name := range reclassStringColumns {
		col := new(proto.ColStr)
		c.strings[name] = col
		result = append(result, proto.ResultColumn{Name: name, Data: col})
	}
	for _, name := range reclassUInt64Columns {
		col := new(proto.ColUInt64)
		c.u64[name] = col
		result = append(result, proto.ResultColumn{Name: name, Data: col})
	}
	for _, name := range reclassUInt32Columns {
		col := new(proto.ColUInt32)
		c.u32[name] = col
		result = append(result, proto.ResultColumn{Name: name, Data: col})
	}
	for _, name := range reclassUInt16Columns {
		col := new(proto.ColUInt16)
		c.u16[name] = col
		result = append(result, proto.ResultColumn{Name: name, Data: col})
	}
	for _, name := range reclassUInt8Columns {
		col := new(proto.ColUInt8)
		c.u8[name] = col
		result = append(result, proto.ResultColumn{Name: name, Data: col})
	}
	for _, name := range reclassBoolColumns {
		col := new(proto.ColBool)
		c.bools[name] = col
		result = append(result, proto.ResultColumn{Name: name, Data: col})
	}
	for _, name := range reclassIPColumns {
		col := new(proto.ColIPv6)
		c.ips[name] = col
		result = append(result, proto.ResultColumn{Name: name, Data: col})
	}
	return result
}

func (c *reclassificationSourceColumns) batch(row int) (*flowworker.EnrichedBatch, *flowworker.Record) {
	agentIP := c.ips["agent_ip"].Row(row).ToIP()
	if !c.bools["agent_ip_valid"].Row(row) {
		agentIP = netip.Addr{}
	}
	decoded := &flowworker.Record{RecordIndex: c.u32["record_index"].Row(row), EventTimeUnixMS: c.eventTime.Row(row).UnixMilli(), TargetID: c.strings["target_id"].Row(row), DeviceID: c.strings["device_id"].Row(row),
		ObservationIfIndex: c.u32["observation_if_index"].Row(row), ObservationDirection: uint32(c.u8["observation_direction"].Row(row)), InIf: c.u32["ingress_if_index"].Row(row), OutIf: c.u32["egress_if_index"].Row(row),
		SourceIP: c.ips["src_ip"].Row(row).ToIP().AsSlice(), DestinationIP: c.ips["dst_ip"].Row(row).ToIP().AsSlice(), SourcePort: uint32(c.u16["src_port"].Row(row)), DestinationPort: uint32(c.u16["dst_port"].Row(row)),
		IPProtocol: uint32(c.u8["ip_protocol"].Row(row)), TCPFlags: uint32(c.u8["tcp_flags"].Row(row)), SourceASN: c.u32["source_asn"].Row(row), DestinationASN: c.u32["destination_asn"].Row(row),
		RawBytes: c.u64["raw_bytes"].Row(row), RawPackets: c.u64["raw_packets"].Row(row), SamplingMode: uint32(c.u8["sampling_mode"].Row(row)), SamplingRate: c.u64["sampling_rate"].Row(row), SamplingSource: uint32(c.u8["sampling_source"].Row(row)), EstimatedBytesScalePPM: c.u32["estimated_bytes_scale_ppm"].Row(row),
		EstimatedValid: c.bools["estimated_valid"].Row(row), EstimatedBytes: c.u64["estimated_bytes"].Row(row), EstimatedPackets: c.u64["estimated_packets"].Row(row), FlowDurationMS: c.u64["flow_duration_ms"].Row(row), QualityFlags: c.u64["quality_flags"].Row(row),
		SourceIDType: c.u32["source_id_type"].Row(row), SourceIDValue: c.u32["source_id_value"].Row(row), SampleSequence: c.u32["sample_sequence"].Row(row), SamplePool: c.u64["sample_pool"].Row(row), ExporterDrops: c.u64["exporter_drops"].Row(row), SampleIndex: c.u32["sample_index"].Row(row), QualityEpoch: c.u64["quality_epoch"].Row(row)}
	batch := &flowworker.EnrichedBatch{SchemaVersion: flowworker.EnrichedBatchSchemaVersion, MessageDisposition: flowworker.MessageDispositionPersisted,
		SourceStreamID: c.strings["source_stream_id"].Row(row), KafkaTopic: c.strings["kafka_topic"].Row(row), KafkaPartition: int32(c.u32["kafka_partition"].Row(row)), KafkaOffset: int64(c.u64["kafka_offset"].Row(row)),
		CollectorID: c.strings["collector_id"].Row(row), ExporterID: c.strings["exporter_id"].Row(row), RegistryVersion: c.u64["registry_version"].Row(row), ReceivedAt: c.receivedTime.Row(row), Protocol: c.u8["flow_protocol"].Row(row),
		SourceIP: c.ips["exporter_source_ip"].Row(row).ToIP(), ObservationDomainID: c.u64["observation_domain_id"].Row(row), SubAgentID: c.u32["sub_agent_id"].Row(row), DatagramSequence: c.u32["datagram_sequence"].Row(row), AgentIP: agentIP, ExporterEpoch: c.u64["exporter_epoch"].Row(row),
		Records: make([]flowworker.EnrichedRecord, 1)}
	return batch, decoded
}

func (c *reclassificationSourceColumns) cursor(row int) ReclassificationCursor {
	return ReclassificationCursor{Set: true, EventTime: c.eventTime.Row(row), SourceStreamID: c.strings["source_stream_id"].Row(row), KafkaPartition: c.u32["kafka_partition"].Row(row), KafkaOffset: c.u64["kafka_offset"].Row(row), RecordIndex: c.u32["record_index"].Row(row)}
}

var reclassStringColumns = []string{"source_stream_id", "kafka_topic", "collector_id", "exporter_id", "target_id", "device_id"}
var reclassUInt64Columns = []string{"kafka_offset", "registry_version", "exporter_epoch", "observation_domain_id", "raw_bytes", "raw_packets", "sampling_rate", "estimated_bytes", "estimated_packets", "flow_duration_ms", "quality_flags", "sample_pool", "exporter_drops", "quality_epoch"}
var reclassUInt32Columns = []string{"kafka_partition", "record_index", "sub_agent_id", "datagram_sequence", "observation_if_index", "ingress_if_index", "egress_if_index", "source_asn", "destination_asn", "estimated_bytes_scale_ppm", "source_id_type", "source_id_value", "sample_sequence", "sample_index"}
var reclassUInt16Columns = []string{"src_port", "dst_port"}
var reclassUInt8Columns = []string{"flow_protocol", "observation_direction", "ip_protocol", "tcp_flags", "sampling_mode", "sampling_source"}
var reclassBoolColumns = []string{"agent_ip_valid", "estimated_valid"}
var reclassIPColumns = []string{"exporter_source_ip", "agent_ip", "src_ip", "dst_ip"}

const reclassificationSourcePageSQL = `SELECT
event_time,received_time,
CAST(source_stream_id AS String) source_stream_id,CAST(kafka_topic AS String) kafka_topic,CAST(collector_id AS String) collector_id,CAST(exporter_id AS String) exporter_id,CAST(target_id AS String) target_id,CAST(device_id AS String) device_id,
kafka_offset,registry_version,exporter_epoch,observation_domain_id,raw_bytes,raw_packets,sampling_rate,estimated_bytes,estimated_packets,flow_duration_ms,quality_flags,sample_pool,exporter_drops,quality_epoch,
kafka_partition,record_index,sub_agent_id,datagram_sequence,observation_if_index,ingress_if_index,egress_if_index,source_asn,destination_asn,estimated_bytes_scale_ppm,source_id_type,source_id_value,sample_sequence,sample_index,
src_port,dst_port,
flow_protocol,toUInt8(observation_direction) observation_direction,ip_protocol,tcp_flags,toUInt8(sampling_mode) sampling_mode,toUInt8(sampling_source) sampling_source,
agent_ip_valid,estimated_valid,
exporter_source_ip,agent_ip,src_ip,dst_ip
FROM flow_records FINAL
WHERE event_time >= {window_start:DateTime64(3,'UTC')} AND event_time < {window_end:DateTime64(3,'UTC')}
AND dimension_snapshot_id={source_dimension:String} AND classification_version={source_classification:UInt32}
AND ({cursor_set:UInt8}=0 OR tuple(event_time,CAST(source_stream_id AS String),kafka_partition,kafka_offset,record_index) > tuple({cursor_time:DateTime64(3,'UTC')},{cursor_stream:String},{cursor_partition:UInt32},{cursor_offset:UInt64},{cursor_index:UInt32}))
ORDER BY event_time,source_stream_id,kafka_partition,kafka_offset,record_index LIMIT `

const reclassificationCoordinateDiffSQL = `SELECT count() AS mismatches FROM (
(SELECT event_time,source_stream_id,kafka_partition,kafka_offset,record_index FROM flow_records FINAL WHERE event_time >= {window_start:DateTime64(3,'UTC')} AND event_time < {window_end:DateTime64(3,'UTC')} AND dimension_snapshot_id={source_dimension:String} AND classification_version={source_classification:UInt32}
 EXCEPT DISTINCT SELECT event_time,source_stream_id,kafka_partition,kafka_offset,record_index FROM flow_reclassified_records FINAL WHERE reclassification_id={id:String} AND reclassification_generation={generation:UInt64})
UNION ALL
(SELECT event_time,source_stream_id,kafka_partition,kafka_offset,record_index FROM flow_reclassified_records FINAL WHERE reclassification_id={id:String} AND reclassification_generation={generation:UInt64}
 EXCEPT DISTINCT SELECT event_time,source_stream_id,kafka_partition,kafka_offset,record_index FROM flow_records FINAL WHERE event_time >= {window_start:DateTime64(3,'UTC')} AND event_time < {window_end:DateTime64(3,'UTC')} AND dimension_snapshot_id={source_dimension:String} AND classification_version={source_classification:UInt32}))`

const reclassificationMarkerSQL = `INSERT INTO flow_reclassification_generations SELECT {id:String},{generation:UInt64},{source_publication:String},{target_publication:String},CAST({view:String},'Enum8(\'customer\'=1,\'supplier\'=2)'),{window_start:DateTime64(3,'UTC')},{window_end:DateTime64(3,'UTC')},count(),sum(toDecimal256(raw_bytes,0)),sum(toDecimal256(raw_packets,0)),sumIf(toDecimal256(estimated_bytes,0),estimated_valid),sumIf(toDecimal256(estimated_packets,0),estimated_valid),countIf(estimated_valid),now64(3)
FROM flow_reclassified_records FINAL WHERE reclassification_id={id:String} AND reclassification_generation={generation:UInt64}`
