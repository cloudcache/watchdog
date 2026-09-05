package flowworker

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	"github.com/cloudcache/watchdog/internal/flowcollect"
	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
	"github.com/cloudcache/watchdog/internal/flowdimension"
)

const (
	NormalizedBatchSchemaVersion = 1
	EnrichedBatchSchemaVersion   = 1
	defaultMaxRecordsPerBatch    = 1_024
	hardMaxRecordsPerBatch       = 65_535
	defaultMaxFutureSkew         = 5 * time.Minute
)

var (
	ErrInvalidNormalizedBatch = errors.New("invalid normalized flow batch")
	ErrVersionUnavailable     = errors.New("event-time dimension version unavailable")
	ErrVersionSkew            = errors.New("classification and dimension snapshots are inconsistent")
)

type ObservationDirection uint8

const (
	ObservationUnknown ObservationDirection = iota
	ObservationIngress
	ObservationEgress
)

type ASNSource string

const (
	ASNSourceUnknown  ASNSource = "unknown"
	ASNSourceExporter ASNSource = "exporter"
	ASNSourceGeo      ASNSource = flowdimension.GeoSchema
	ASNSourceOverride ASNSource = "flow_geo_override"
)

type EnrichmentLimits struct {
	MaxRecordsPerBatch int
	MaxFutureSkew      time.Duration
}

type Enricher struct {
	dimensions     *flowdimension.SnapshotCatalog
	geo            *flowdimension.GeoCatalog
	classification *flowdimension.ClassificationCatalog
	limits         EnrichmentLimits
}

type EnrichedBatch struct {
	SchemaVersion       uint32
	NormalizedBatchID   [32]byte
	DatagramID          [32]byte
	VirtualShard        uint32
	PartitionMapVersion uint32
	PhysicalPartition   uint32
	ReplayGeneration    uint32
	TenantID            string
	CollectorID         string
	ExporterID          string
	RegistryVersion     uint64
	ReceivedAt          time.Time
	Protocol            uint8
	SourceIP            netip.Addr
	ObservationDomainID uint64
	SubAgentID          uint32
	DatagramSequence    uint32
	AgentIP             netip.Addr
	ExporterEpoch       uint64
	Records             []EnrichedRecord
}

type EnrichedRecord struct {
	NormalizedRecordID    [32]byte
	RecordIndex           uint32
	EventTime             time.Time
	TargetID              string
	DeviceID              string
	ObservationIfIndex    uint32
	ObservationDirection  ObservationDirection
	InIf                  uint32
	OutIf                 uint32
	SourceIP              netip.Addr
	DestinationIP         netip.Addr
	SourcePort            uint16
	DestinationPort       uint16
	IPProtocol            uint8
	TCPFlags              uint8
	SourceASN             uint32
	DestinationASN        uint32
	RawBytes              uint64
	RawPackets            uint64
	SamplingMode          uint8
	SamplingRate          uint64
	EstimatedBytes        uint64
	EstimatedPackets      uint64
	FlowDurationMS        uint64
	QualityFlags          uint64
	SourceIDType          uint32
	SourceIDValue         uint32
	SampleSequence        uint32
	SamplePool            uint64
	ExporterDrops         uint64
	SampleIndex           uint32
	QualityEpoch          uint64
	Dimensions            flowdimension.ClassifiedEndpoints
	LocalPort             uint16
	RemotePort            uint16
	RemoteGeo             flowdimension.GeoInfo
	RemoteASN             uint32
	RemoteASNSource       ASNSource
	Category              flowdimension.Category
	Disposition           flowdimension.RecordDisposition
	ClassificationVersion uint32
	DimensionFingerprint  uint64
}

// VersionBlockedError is the consumer-loop signal to pause a Kafka partition
// without committing its offset. It contains only stable identifiers and never
// causes a fallback to an active/current snapshot.
type VersionBlockedError struct {
	Dependency  string
	TenantID    string
	BatchID     [32]byte
	RecordIndex uint32
	EventTime   time.Time
	Cause       error
}

func (e *VersionBlockedError) Error() string {
	return fmt.Sprintf("%s: dependency=%s tenant=%s record=%d event_time=%s", ErrVersionUnavailable, e.Dependency, e.TenantID, e.RecordIndex, e.EventTime.UTC().Format(time.RFC3339Nano))
}

func (e *VersionBlockedError) Unwrap() error {
	return e.Cause
}

func (e *VersionBlockedError) Is(target error) bool {
	return target == ErrVersionUnavailable
}

func NewEnricher(dimensions *flowdimension.SnapshotCatalog, geo *flowdimension.GeoCatalog, classification *flowdimension.ClassificationCatalog, limits EnrichmentLimits) (*Enricher, error) {
	if dimensions == nil || geo == nil || classification == nil {
		return nil, errors.New("dimension, Geo, and classification catalogs are required")
	}
	if limits.MaxRecordsPerBatch == 0 {
		limits.MaxRecordsPerBatch = defaultMaxRecordsPerBatch
	}
	if limits.MaxRecordsPerBatch < 1 || limits.MaxRecordsPerBatch > hardMaxRecordsPerBatch {
		return nil, errors.New("max records per enriched batch must be 1..65535")
	}
	if limits.MaxFutureSkew == 0 {
		limits.MaxFutureSkew = defaultMaxFutureSkew
	}
	if limits.MaxFutureSkew < 0 || limits.MaxFutureSkew > 24*time.Hour {
		return nil, errors.New("max event-time future skew must be 1ns..24h")
	}
	return &Enricher{dimensions: dimensions, geo: geo, classification: classification, limits: limits}, nil
}

// EnrichBatch is atomic at the normalized Kafka message boundary: callers get
// either every validated record or no result. It performs no network, database,
// Kafka, or ClickHouse I/O.
func (e *Enricher) EnrichBatch(batch *flowpb.NormalizedRecordBatch) (*EnrichedBatch, error) {
	result := &EnrichedBatch{}
	if err := e.EnrichBatchInto(batch, result); err != nil {
		return nil, err
	}
	return result, nil
}

// EnrichBatchInto lets the aggregate worker reuse one record buffer per
// partition/shard. On any error Records is reset to length zero, so partially
// enriched input cannot be mistaken for a complete Kafka message.
func (e *Enricher) EnrichBatchInto(batch *flowpb.NormalizedRecordBatch, result *EnrichedBatch) error {
	if result == nil {
		return invalid("enriched batch destination is required")
	}
	if e == nil || e.dimensions == nil || e.geo == nil || e.classification == nil {
		result.Records = result.Records[:0]
		return invalid("enricher is not initialized")
	}
	validated, err := validateBatch(batch, e.limits)
	if err != nil {
		result.Records = result.Records[:0]
		return err
	}
	records := result.Records[:0]
	*result = EnrichedBatch{
		SchemaVersion:     EnrichedBatchSchemaVersion,
		NormalizedBatchID: validated.batchID, DatagramID: validated.datagramID,
		VirtualShard: batch.VirtualShard, PartitionMapVersion: batch.PartitionMapVersion,
		PhysicalPartition: batch.PhysicalPartition, ReplayGeneration: batch.ReplayGeneration,
		TenantID: batch.TenantId, CollectorID: batch.CollectorId, ExporterID: batch.ExporterId,
		RegistryVersion: batch.RegistryVersion, ReceivedAt: time.UnixMilli(batch.ReceivedAtUnixMs).UTC(),
		Protocol: uint8(batch.Protocol), SourceIP: validated.sourceIP,
		ObservationDomainID: batch.ObservationDomainId,
		SubAgentID:          batch.SubAgentId, DatagramSequence: batch.DatagramSequence,
		AgentIP: validated.agentIP, ExporterEpoch: batch.ExporterEpoch,
		Records: records,
	}
	if cap(result.Records) < len(batch.Records) {
		result.Records = make([]EnrichedRecord, 0, len(batch.Records))
	}
	for _, normalized := range batch.Records {
		record, err := e.enrichRecord(batch.TenantId, validated.datagramID, validated.batchID, normalized)
		if err != nil {
			result.Records = result.Records[:0]
			return err
		}
		result.Records = append(result.Records, record)
	}
	return nil
}

func (e *Enricher) enrichRecord(tenantID string, datagramID, batchID [32]byte, normalized *flowpb.NormalizedRecord) (EnrichedRecord, error) {
	eventTime := time.UnixMilli(normalized.EventTimeUnixMs).UTC()
	source, _ := parseAddress16(normalized.SrcIp)
	destination, _ := parseAddress16(normalized.DstIp)
	dimensionSnapshot, err := e.dimensions.Select(tenantID, eventTime)
	if err != nil {
		return EnrichedRecord{}, blocked("dimension", tenantID, batchID, normalized, eventTime, err)
	}
	classificationSnapshot, err := e.classification.Select(tenantID, eventTime)
	if err != nil {
		return EnrichedRecord{}, blocked("classification", tenantID, batchID, normalized, eventTime, err)
	}
	dimensionMetadata := dimensionSnapshot.Metadata()
	classificationMetadata := classificationSnapshot.Metadata()
	if classificationMetadata.DimensionSnapshotID != dimensionMetadata.SnapshotID {
		return EnrichedRecord{}, blocked("classification_dimension_pair", tenantID, batchID, normalized, eventTime, ErrVersionSkew)
	}
	geoIndex, err := e.geo.Select(eventTime)
	if err != nil {
		return EnrichedRecord{}, blocked("geo", tenantID, batchID, normalized, eventTime, err)
	}

	dimensions := dimensionSnapshot.ClassifyEndpoints(source, destination)
	geoMetadata := geoIndex.Metadata()
	remoteGeo := flowdimension.GeoInfo{Country: flowdimension.GeoUnknownCountry, Version: geoMetadata.Version, Source: flowdimension.GeoSchema}
	geoMatched := false
	if dimensions.Remote.IP.IsValid() {
		if resolved, matched := geoIndex.Lookup(dimensions.Remote.IP); matched {
			remoteGeo, geoMatched = resolved, true
		}
	}
	remoteGeo, overrideFields, _ := dimensionSnapshot.ApplyGeoOverride(dimensions.Remote.IP, remoteGeo)
	remoteASN, remoteASNSource := selectRemoteASN(normalized, dimensions.Remote.Side, remoteGeo, geoMatched, overrideFields)
	localPort, remotePort := endpointPorts(normalized, dimensions)
	record := EnrichedRecord{
		NormalizedRecordID: normalizedRecordID(datagramID, normalized.RecordIndex),
		RecordIndex:        normalized.RecordIndex, EventTime: eventTime, TargetID: normalized.TargetId, DeviceID: normalized.DeviceId,
		ObservationIfIndex: normalized.ObservationIfIndex, ObservationDirection: ObservationDirection(normalized.ObservationDirection),
		InIf: normalized.InIf, OutIf: normalized.OutIf, SourceIP: source, DestinationIP: destination,
		SourcePort: uint16(normalized.SrcPort), DestinationPort: uint16(normalized.DstPort),
		IPProtocol: uint8(normalized.IpProto), TCPFlags: uint8(normalized.TcpFlags),
		SourceASN: normalized.SrcAs, DestinationASN: normalized.DstAs,
		RawBytes: normalized.RawBytes, RawPackets: normalized.RawPackets,
		SamplingMode: uint8(normalized.SamplingMode), SamplingRate: normalized.SamplingRate,
		EstimatedBytes: normalized.EstimatedBytes, EstimatedPackets: normalized.EstimatedPackets,
		FlowDurationMS: normalized.FlowDurationMs, QualityFlags: normalized.QualityFlags,
		SourceIDType: normalized.SourceIdType, SourceIDValue: normalized.SourceIdValue,
		SampleSequence: normalized.SampleSequence, SamplePool: normalized.SamplePool,
		ExporterDrops: normalized.ExporterDrops, SampleIndex: normalized.SampleIndex, QualityEpoch: normalized.QualityEpoch,
		Dimensions: dimensions, LocalPort: localPort, RemotePort: remotePort,
		RemoteGeo: remoteGeo, RemoteASN: remoteASN, RemoteASNSource: remoteASNSource,
		Category:              classificationSnapshot.Classify(dimensions.Direction, remoteGeo),
		Disposition:           classificationSnapshot.Disposition(dimensions.Direction),
		ClassificationVersion: classificationMetadata.Version,
	}
	record.DimensionFingerprint = dimensionFingerprint(record.Dimensions)
	return record, nil
}

type validatedBatch struct {
	batchID    [32]byte
	datagramID [32]byte
	sourceIP   netip.Addr
	agentIP    netip.Addr
}

func validateBatch(batch *flowpb.NormalizedRecordBatch, limits EnrichmentLimits) (validatedBatch, error) {
	var result validatedBatch
	if batch == nil {
		return result, invalid("batch is required")
	}
	if batch.BatchSchemaVersion != NormalizedBatchSchemaVersion {
		return result, invalid("unsupported batch schema version")
	}
	if len(batch.NormalizedBatchId) != len(result.batchID) || len(batch.DatagramId) != len(result.datagramID) {
		return result, invalid("batch and datagram IDs must be 32 bytes")
	}
	copy(result.batchID[:], batch.NormalizedBatchId)
	copy(result.datagramID[:], batch.DatagramId)
	if batch.VirtualShard >= flowcollect.VirtualShardCount || batch.PhysicalPartition > math.MaxInt32 || batch.PartitionMapVersion == 0 {
		return result, invalid("partition identity is invalid")
	}
	if !validIdentifier(batch.TenantId, 64) || !validIdentifier(batch.CollectorId, 128) || !validIdentifier(batch.ExporterId, 128) || batch.RegistryVersion == 0 {
		return result, invalid("tenant, collector, exporter, and registry identity are required")
	}
	if batch.ReceivedAtUnixMs <= 0 || batch.Protocol < 1 || batch.Protocol > 4 {
		return result, invalid("receive time or protocol is invalid")
	}
	var ok bool
	if result.sourceIP, ok = parseAddress16(batch.SourceIp); !ok {
		return result, invalid("exporter source IP must be a 16-byte address")
	}
	if len(batch.AgentIp) != 0 {
		if result.agentIP, ok = parseAddress16(batch.AgentIp); !ok {
			return result, invalid("sFlow agent IP must be empty or a 16-byte address")
		}
	}
	if len(batch.Records) == 0 || len(batch.Records) > limits.MaxRecordsPerBatch {
		return result, invalid("record count exceeds the configured boundary")
	}
	for position, record := range batch.Records {
		if err := validateRecord(batch, record, limits); err != nil {
			return result, fmt.Errorf("%w: record[%d]: %v", ErrInvalidNormalizedBatch, position, err)
		}
		if position > 0 && batch.Records[position-1].RecordIndex >= record.RecordIndex {
			return result, fmt.Errorf("%w: record_index must be strictly increasing", ErrInvalidNormalizedBatch)
		}
	}
	return result, nil
}

func validateRecord(batch *flowpb.NormalizedRecordBatch, record *flowpb.NormalizedRecord, limits EnrichmentLimits) error {
	if record == nil {
		return errors.New("record is required")
	}
	if record.EventTimeUnixMs <= 0 || !validIdentifier(record.TargetId, 128) || !validOptionalIdentifier(record.DeviceId, 128) {
		return errors.New("event time, target, or device identity is invalid")
	}
	if record.EventTimeUnixMs > batch.ReceivedAtUnixMs && record.EventTimeUnixMs-batch.ReceivedAtUnixMs > limits.MaxFutureSkew.Milliseconds() {
		return errors.New("event time exceeds the allowed receive-time future skew")
	}
	source, sourceOK := parseAddress16(record.SrcIp)
	destination, destinationOK := parseAddress16(record.DstIp)
	if !sourceOK || !destinationOK {
		return errors.New("source and destination must be 16-byte addresses")
	}
	if flowcollect.VirtualShard(batch.TenantId, source, destination) != batch.VirtualShard {
		return errors.New("record does not belong to the declared virtual shard")
	}
	if record.ObservationDirection > uint32(ObservationEgress) || record.SrcPort > math.MaxUint16 || record.DstPort > math.MaxUint16 || record.IpProto > math.MaxUint8 || record.TcpFlags > math.MaxUint8 {
		return errors.New("direction, port, protocol, or TCP flags exceed the normalized schema")
	}
	switch record.SamplingMode {
	case 1:
		if record.SamplingRate == 0 || !scaledCountersMatch(record) {
			return errors.New("sampled counters do not match the authoritative sampling rate")
		}
	case 2:
		if record.EstimatedBytes != record.RawBytes || record.EstimatedPackets != record.RawPackets {
			return errors.New("pre-scaled counters must not be multiplied again")
		}
	default:
		return errors.New("sampling mode is invalid")
	}
	return nil
}

func scaledCountersMatch(record *flowpb.NormalizedRecord) bool {
	if record.RawBytes != 0 && record.SamplingRate > math.MaxUint64/record.RawBytes {
		return false
	}
	if record.RawPackets != 0 && record.SamplingRate > math.MaxUint64/record.RawPackets {
		return false
	}
	return record.EstimatedBytes == record.RawBytes*record.SamplingRate && record.EstimatedPackets == record.RawPackets*record.SamplingRate
}

func parseAddress16(value []byte) (netip.Addr, bool) {
	if len(value) != 16 {
		return netip.Addr{}, false
	}
	var bytes [16]byte
	copy(bytes[:], value)
	return netip.AddrFrom16(bytes).Unmap(), true
}

func endpointPorts(record *flowpb.NormalizedRecord, dimensions flowdimension.ClassifiedEndpoints) (uint16, uint16) {
	port := func(side flowdimension.EndpointSide) uint16 {
		switch side {
		case flowdimension.EndpointSrc:
			return uint16(record.SrcPort)
		case flowdimension.EndpointDst:
			return uint16(record.DstPort)
		default:
			return 0
		}
	}
	return port(dimensions.Local.Side), port(dimensions.Remote.Side)
}

func selectRemoteASN(record *flowpb.NormalizedRecord, side flowdimension.EndpointSide, geo flowdimension.GeoInfo, geoMatched bool, overrideFields flowdimension.GeoOverrideFields) (uint32, ASNSource) {
	if overrideFields&flowdimension.GeoOverrideASN != 0 {
		return geo.ASN, ASNSourceOverride
	}
	if geoMatched && geo.ASN != 0 {
		return geo.ASN, ASNSourceGeo
	}
	var exporterASN uint32
	switch side {
	case flowdimension.EndpointSrc:
		exporterASN = record.SrcAs
	case flowdimension.EndpointDst:
		exporterASN = record.DstAs
	}
	if exporterASN != 0 {
		return exporterASN, ASNSourceExporter
	}
	return 0, ASNSourceUnknown
}

func dimensionFingerprint(dimensions flowdimension.ClassifiedEndpoints) uint64 {
	hash := xxhash.New()
	writeHashString(hash, dimensions.SnapshotID)
	writeHashUint64(hash, dimensions.Version)
	writeHashString(hash, string(dimensions.Direction))
	writeHashString(hash, dimensions.Business)
	writeEndpointHash(hash, dimensions.Local)
	writeEndpointHash(hash, dimensions.Remote)
	return hash.Sum64()
}

func writeEndpointHash(hash *xxhash.Digest, endpoint flowdimension.EndpointDimension) {
	writeHashString(hash, string(endpoint.Side))
	writeHashString(hash, endpoint.PrefixID)
	writeHashString(hash, endpoint.PrefixCIDR)
	writeHashUint64(hash, uint64(endpoint.AddressSets.Count()))
	for index := 0; index < endpoint.AddressSets.Count(); index++ {
		id, _ := endpoint.AddressSets.At(index)
		writeHashString(hash, id)
	}
}

func writeHashString(hash *xxhash.Digest, value string) {
	writeHashUint64(hash, uint64(len(value)))
	_, _ = hash.WriteString(value)
}

func writeHashUint64(hash *xxhash.Digest, value uint64) {
	var encoded [10]byte
	length := binary.PutUvarint(encoded[:], value)
	_, _ = hash.Write(encoded[:length])
}

func normalizedRecordID(datagramID [32]byte, recordIndex uint32) [32]byte {
	var input [36]byte
	copy(input[:32], datagramID[:])
	binary.BigEndian.PutUint32(input[32:], recordIndex)
	return sha256.Sum256(input[:])
}

func blocked(dependency, tenantID string, batchID [32]byte, record *flowpb.NormalizedRecord, eventTime time.Time, cause error) error {
	return &VersionBlockedError{Dependency: dependency, TenantID: tenantID, BatchID: batchID, RecordIndex: record.RecordIndex, EventTime: eventTime, Cause: cause}
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidNormalizedBatch, message)
}

func validIdentifier(value string, maximum int) bool {
	return validText(value, maximum) && !strings.ContainsAny(value, " /\\")
}

func validOptionalIdentifier(value string, maximum int) bool {
	return value == "" || validIdentifier(value, maximum)
}

func validText(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}
