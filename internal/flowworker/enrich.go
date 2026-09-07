package flowworker

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

const (
	RecordBatchSchemaVersion   = 3
	EnrichedBatchSchemaVersion = 5
	defaultMaxRecordsPerBatch  = 1_024
	hardMaxRecordsPerBatch     = 65_535
	defaultMaxFutureSkew       = 5 * time.Minute
)

var (
	ErrInvalidRecordBatch = errors.New("invalid flow record batch")
	ErrVersionUnavailable = errors.New("event-time dimension version unavailable")
	ErrVersionSkew        = errors.New("classification and dimension snapshots are inconsistent")
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
	ASNSourceGeoV1    ASNSource = flowdimension.GeoSchemaV1
	ASNSourceGeoV2    ASNSource = flowdimension.GeoSchemaV2
	ASNSourceGeo      ASNSource = ASNSourceGeoV1
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
	versions       *EnrichmentVersionCatalog
	limits         EnrichmentLimits
}

type EnrichedBatch struct {
	SchemaVersion       uint32
	MessageDisposition  MessageDisposition
	SourceStreamID      string
	KafkaTopic          string
	KafkaPartition      int32
	KafkaOffset         int64
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
	RecordIndex               uint32
	EventTime                 time.Time
	TargetID                  string
	DeviceID                  string
	ObservationIfIndex        uint32
	ObservationDirection      ObservationDirection
	InIf                      uint32
	OutIf                     uint32
	SourceIP                  netip.Addr
	DestinationIP             netip.Addr
	SourcePort                uint16
	DestinationPort           uint16
	IPProtocol                uint8
	TCPFlags                  uint8
	SourceASN                 uint32
	DestinationASN            uint32
	RawBytes                  uint64
	RawPackets                uint64
	SamplingMode              uint8
	SamplingRate              uint64
	SamplingSource            uint8
	EstimatedValid            bool
	EstimatedBytes            uint64
	EstimatedPackets          uint64
	FlowDurationMS            uint64
	QualityFlags              uint64
	SourceIDType              uint32
	SourceIDValue             uint32
	SampleSequence            uint32
	SamplePool                uint64
	ExporterDrops             uint64
	SampleIndex               uint32
	QualityEpoch              uint64
	Dimensions                flowdimension.ClassifiedEndpoints
	LocalPort                 uint16
	RemotePort                uint16
	RemoteGeo                 flowdimension.GeoInfo
	RemoteASN                 uint32
	RemoteASNSource           ASNSource
	Category                  flowdimension.Category
	SupplierRemoteGeo         flowdimension.GeoInfo
	SupplierRemoteASN         uint32
	SupplierRemoteASNSource   ASNSource
	SupplierCategory          flowdimension.Category
	CustomerGeoOverrideFields flowdimension.GeoOverrideFields
	Disposition               flowdimension.RecordDisposition
	ClassificationVersion     uint32
}

// VersionBlockedError is the consumer-loop signal to pause a Kafka partition
// without committing its offset. It contains only stable identifiers and never
// causes a fallback to an active/current snapshot.
type VersionBlockedError struct {
	Dependency     string
	TenantID       string
	SourceStreamID string
	KafkaPartition int32
	KafkaOffset    int64
	RecordIndex    uint32
	EventTime      time.Time
	Cause          error
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
	limits, err := normalizeEnrichmentLimits(limits)
	if err != nil {
		return nil, err
	}
	return &Enricher{dimensions: dimensions, geo: geo, classification: classification, limits: limits}, nil
}

// NewEnricherWithVersionCatalog is the production constructor. It consumes
// atomically published dimension/classification pairs. NewEnricher is retained
// for callers that still own the two legacy catalogs independently.
func NewEnricherWithVersionCatalog(versions *EnrichmentVersionCatalog, geo *flowdimension.GeoCatalog, limits EnrichmentLimits) (*Enricher, error) {
	if versions == nil {
		return nil, errors.New("enrichment version catalog is required")
	}
	limits, err := normalizeEnrichmentLimits(limits)
	if err != nil {
		return nil, err
	}
	return &Enricher{versions: versions, geo: geo, limits: limits}, nil
}

func normalizeEnrichmentLimits(limits EnrichmentLimits) (EnrichmentLimits, error) {
	if limits.MaxRecordsPerBatch == 0 {
		limits.MaxRecordsPerBatch = defaultMaxRecordsPerBatch
	}
	if limits.MaxRecordsPerBatch < 1 || limits.MaxRecordsPerBatch > hardMaxRecordsPerBatch {
		return EnrichmentLimits{}, errors.New("max records per enriched batch must be 1..65535")
	}
	if limits.MaxFutureSkew == 0 {
		limits.MaxFutureSkew = defaultMaxFutureSkew
	}
	if limits.MaxFutureSkew < 0 || limits.MaxFutureSkew > 24*time.Hour {
		return EnrichmentLimits{}, errors.New("max event-time future skew must be 1ns..24h")
	}
	return limits, nil
}

// EnrichBatch is atomic at the decoded RawFlow batch boundary: callers get
// either every validated record or no result. It performs no network, database,
// Kafka, or ClickHouse I/O.
func (e *Enricher) EnrichBatch(batch *RecordBatch) (*EnrichedBatch, error) {
	result := &EnrichedBatch{}
	if err := e.EnrichBatchInto(batch, result); err != nil {
		return nil, err
	}
	return result, nil
}

// EnrichBatchInto lets the worker reuse one record buffer per partition.
// On any error Records is reset to length zero, so partially
// enriched input cannot be mistaken for a complete source batch.
func (e *Enricher) EnrichBatchInto(batch *RecordBatch, result *EnrichedBatch) error {
	if result == nil {
		return invalid("enriched batch destination is required")
	}
	if e == nil || (e.versions == nil && (e.geo == nil || e.dimensions == nil || e.classification == nil)) {
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
		SchemaVersion:      EnrichedBatchSchemaVersion,
		MessageDisposition: batch.MessageDisposition,
		SourceStreamID:     batch.SourceStreamID, KafkaTopic: batch.KafkaTopic,
		KafkaPartition: batch.KafkaPartition, KafkaOffset: batch.KafkaOffset,
		TenantID: batch.TenantID, CollectorID: batch.CollectorID, ExporterID: batch.ExporterID,
		RegistryVersion: batch.RegistryVersion, ReceivedAt: time.UnixMilli(batch.ReceivedAtUnixMS).UTC(),
		Protocol: uint8(batch.Protocol), SourceIP: validated.sourceIP,
		ObservationDomainID: batch.ObservationDomainID,
		SubAgentID:          batch.SubAgentID, DatagramSequence: batch.DatagramSequence,
		AgentIP: validated.agentIP, ExporterEpoch: batch.ExporterEpoch,
		Records: records,
	}
	if batch.MessageDisposition != MessageDispositionPersisted {
		return nil
	}
	if cap(result.Records) < len(batch.Records) {
		result.Records = make([]EnrichedRecord, 0, len(batch.Records))
	}
	for _, decoded := range batch.Records {
		record, err := e.enrichRecord(batch.TenantID, batch.SourceStreamID, batch.KafkaPartition, batch.KafkaOffset, decoded)
		if err != nil {
			result.Records = result.Records[:0]
			return err
		}
		result.Records = append(result.Records, record)
	}
	return nil
}

func (e *Enricher) enrichRecord(tenantID, sourceStreamID string, kafkaPartition int32, kafkaOffset int64, decoded *Record) (EnrichedRecord, error) {
	eventTime := time.UnixMilli(decoded.EventTimeUnixMS).UTC()
	source, _ := parseAddress16(decoded.SourceIP)
	destination, _ := parseAddress16(decoded.DestinationIP)
	dimensionSnapshot, classificationSnapshot, dependency, err := e.selectVersions(tenantID, eventTime)
	if err != nil {
		return EnrichedRecord{}, blocked(dependency, tenantID, sourceStreamID, kafkaPartition, kafkaOffset, decoded, eventTime, err)
	}
	dimensionMetadata := dimensionSnapshot.Metadata()
	classificationMetadata := classificationSnapshot.Metadata()
	if classificationMetadata.DimensionSnapshotID != dimensionMetadata.SnapshotID {
		return EnrichedRecord{}, blocked("classification_dimension_pair", tenantID, sourceStreamID, kafkaPartition, kafkaOffset, decoded, eventTime, ErrVersionSkew)
	}
	dimensions := dimensionSnapshot.ClassifyEndpoints(source, destination)
	var supplierRemoteGeo, remoteGeo flowdimension.GeoInfo
	var overrideFields flowdimension.GeoOverrideFields
	geoMatched := false
	addressSnapshotResolved := false
	if addressSnapshot, ok := dimensionSnapshot.(interface {
		ResolveAddress(netip.Addr) (flowdimension.AddressSnapshotResolution, bool)
	}); ok {
		supplierRemoteGeo = flowdimension.GeoInfo{Country: flowdimension.GeoUnknownCountry, Version: dimensionMetadata.SnapshotID, Source: flowdimension.GeoSchemaV2}
		remoteGeo = supplierRemoteGeo
		if dimensions.Remote.IP.IsValid() {
			if resolved, matched := addressSnapshot.ResolveAddress(dimensions.Remote.IP); matched {
				supplierRemoteGeo, remoteGeo = resolved.SupplierGeo, resolved.CustomerGeo
				overrideFields, geoMatched, addressSnapshotResolved = resolved.CustomerOverrideFields, true, true
			}
		}
	} else {
		if e.geo == nil {
			return EnrichedRecord{}, blocked("geo", tenantID, sourceStreamID, kafkaPartition, kafkaOffset, decoded, eventTime, flowdimension.ErrNoGeoIndex)
		}
		geoIndex, err := e.geo.Select(eventTime)
		if err != nil {
			return EnrichedRecord{}, blocked("geo", tenantID, sourceStreamID, kafkaPartition, kafkaOffset, decoded, eventTime, err)
		}
		geoMetadata := geoIndex.Metadata()
		supplierRemoteGeo = flowdimension.GeoInfo{Country: flowdimension.GeoUnknownCountry, Version: geoMetadata.Version, Source: geoMetadata.Schema}
		if dimensions.Remote.IP.IsValid() {
			if resolved, matched := geoIndex.Lookup(dimensions.Remote.IP); matched {
				supplierRemoteGeo, geoMatched = resolved, true
			}
		}
		legacySnapshot, ok := dimensionSnapshot.(*flowdimension.CompiledSnapshot)
		if !ok {
			return EnrichedRecord{}, blocked("dimension", tenantID, sourceStreamID, kafkaPartition, kafkaOffset, decoded, eventTime, ErrVersionSkew)
		}
		remoteGeo, overrideFields, _ = legacySnapshot.ApplyGeoOverride(dimensions.Remote.IP, supplierRemoteGeo)
	}
	supplierRemoteASN, supplierRemoteASNSource := selectRemoteASN(decoded, dimensions.Remote.Side, supplierRemoteGeo, geoMatched, 0)
	remoteGeoForASN := remoteGeo
	if addressSnapshotResolved && overrideFields&flowdimension.GeoOverrideASN == 0 {
		// A customer Geo/ISP correction must not hide the inherited WADS ASN's
		// supplier-derived provenance.
		remoteGeoForASN.Source = flowdimension.GeoSchemaV2
	}
	remoteASN, remoteASNSource := selectRemoteASN(decoded, dimensions.Remote.Side, remoteGeoForASN, geoMatched, overrideFields)
	localPort, remotePort := endpointPorts(decoded, dimensions)
	record := EnrichedRecord{
		RecordIndex: decoded.RecordIndex, EventTime: eventTime, TargetID: decoded.TargetID, DeviceID: decoded.DeviceID,
		ObservationIfIndex: decoded.ObservationIfIndex, ObservationDirection: ObservationDirection(decoded.ObservationDirection),
		InIf: decoded.InIf, OutIf: decoded.OutIf, SourceIP: source, DestinationIP: destination,
		SourcePort: uint16(decoded.SourcePort), DestinationPort: uint16(decoded.DestinationPort),
		IPProtocol: uint8(decoded.IPProtocol), TCPFlags: uint8(decoded.TCPFlags),
		SourceASN: decoded.SourceASN, DestinationASN: decoded.DestinationASN,
		RawBytes: decoded.RawBytes, RawPackets: decoded.RawPackets,
		SamplingMode: uint8(decoded.SamplingMode), SamplingRate: decoded.SamplingRate,
		SamplingSource: uint8(decoded.SamplingSource), EstimatedValid: decoded.EstimatedValid,
		EstimatedBytes: decoded.EstimatedBytes, EstimatedPackets: decoded.EstimatedPackets,
		FlowDurationMS: decoded.FlowDurationMS, QualityFlags: decoded.QualityFlags,
		SourceIDType: decoded.SourceIDType, SourceIDValue: decoded.SourceIDValue,
		SampleSequence: decoded.SampleSequence, SamplePool: decoded.SamplePool,
		ExporterDrops: decoded.ExporterDrops, SampleIndex: decoded.SampleIndex, QualityEpoch: decoded.QualityEpoch,
		Dimensions: dimensions, LocalPort: localPort, RemotePort: remotePort,
		RemoteGeo: remoteGeo, RemoteASN: remoteASN, RemoteASNSource: remoteASNSource,
		SupplierRemoteGeo: supplierRemoteGeo, SupplierRemoteASN: supplierRemoteASN, SupplierRemoteASNSource: supplierRemoteASNSource,
		SupplierCategory: classificationSnapshot.Classify(dimensions.Direction, supplierRemoteGeo), CustomerGeoOverrideFields: overrideFields,
		Category:              classificationSnapshot.Classify(dimensions.Direction, remoteGeo),
		Disposition:           classificationSnapshot.Disposition(dimensions.Direction),
		ClassificationVersion: classificationMetadata.Version,
	}
	return record, nil
}

func (e *Enricher) selectVersions(tenantID string, eventTime time.Time) (DimensionSnapshot, *flowdimension.ClassificationSnapshot, string, error) {
	if e.versions != nil {
		version, err := e.versions.Select(tenantID, eventTime)
		if err != nil {
			return nil, nil, "dimension_classification_pair", err
		}
		return version.Dimension, version.Classification, "", nil
	}
	dimension, err := e.dimensions.Select(tenantID, eventTime)
	if err != nil {
		return nil, nil, "dimension", err
	}
	classification, err := e.classification.Select(tenantID, eventTime)
	if err != nil {
		return nil, nil, "classification", err
	}
	return dimension, classification, "", nil
}

type validatedBatch struct {
	sourceIP netip.Addr
	agentIP  netip.Addr
}

func validateBatch(batch *RecordBatch, limits EnrichmentLimits) (validatedBatch, error) {
	var result validatedBatch
	if batch == nil {
		return result, invalid("batch is required")
	}
	if batch.BatchSchemaVersion != RecordBatchSchemaVersion {
		return result, invalid("unsupported batch schema version")
	}
	if !ValidSourceStreamID(batch.SourceStreamID) || !validText(batch.KafkaTopic, 249) || batch.KafkaPartition < 0 || batch.KafkaOffset < 0 {
		return result, invalid("Kafka source identity is invalid")
	}
	if batch.ReceivedAtUnixMS <= 0 {
		return result, invalid("receive time is invalid")
	}
	if batch.MessageDisposition < MessageDispositionPersisted || batch.MessageDisposition > MessageDispositionMappingRejected {
		return result, invalid("message disposition is invalid")
	}
	if batch.MessageDisposition != MessageDispositionPersisted {
		if len(batch.Records) != 0 {
			return result, invalid("non-persisted message must not carry records")
		}
		return result, nil
	}
	if !validIdentifier(batch.TenantID, 64) || !validIdentifier(batch.CollectorID, 128) || !validIdentifier(batch.ExporterID, 128) || batch.RegistryVersion == 0 {
		return result, invalid("tenant, collector, exporter, and registry identity are required")
	}
	if batch.Protocol < 1 || batch.Protocol > 4 {
		return result, invalid("protocol is invalid")
	}
	var ok bool
	if result.sourceIP, ok = parseAddress16(batch.SourceIP); !ok {
		return result, invalid("exporter source IP must be a 16-byte address")
	}
	if len(batch.AgentIP) != 0 {
		if result.agentIP, ok = parseAddress16(batch.AgentIP); !ok {
			return result, invalid("sFlow agent IP must be empty or a 16-byte address")
		}
	}
	if len(batch.Records) == 0 || len(batch.Records) > limits.MaxRecordsPerBatch {
		return result, invalid("record count exceeds the configured boundary")
	}
	for position, record := range batch.Records {
		if err := validateRecord(batch, record, limits); err != nil {
			return result, fmt.Errorf("%w: record[%d]: %v", ErrInvalidRecordBatch, position, err)
		}
		if record.RecordIndex != uint32(position) {
			return result, fmt.Errorf("%w: record_index must start at zero and be contiguous", ErrInvalidRecordBatch)
		}
	}
	return result, nil
}

func validateRecord(batch *RecordBatch, record *Record, limits EnrichmentLimits) error {
	if record == nil {
		return errors.New("record is required")
	}
	if record.EventTimeUnixMS <= 0 || !validIdentifier(record.TargetID, 128) || !validOptionalIdentifier(record.DeviceID, 128) {
		return errors.New("event time, target, or device identity is invalid")
	}
	if record.EventTimeUnixMS > batch.ReceivedAtUnixMS && record.EventTimeUnixMS-batch.ReceivedAtUnixMS > limits.MaxFutureSkew.Milliseconds() {
		return errors.New("event time exceeds the allowed receive-time future skew")
	}
	_, sourceOK := parseAddress16(record.SourceIP)
	_, destinationOK := parseAddress16(record.DestinationIP)
	if !sourceOK || !destinationOK {
		return errors.New("source and destination must be 16-byte addresses")
	}
	if record.ObservationDirection > uint32(ObservationEgress) || record.SourcePort > math.MaxUint16 || record.DestinationPort > math.MaxUint16 || record.IPProtocol > math.MaxUint8 || record.TCPFlags > math.MaxUint8 {
		return errors.New("direction, port, protocol, or TCP flags exceed the normalized schema")
	}
	switch record.SamplingMode {
	case 0:
		if record.EstimatedValid || record.SamplingRate != 0 || record.EstimatedBytes != 0 || record.EstimatedPackets != 0 {
			return errors.New("unknown sampling must not carry estimated counters")
		}
	case 1:
		if record.SamplingRate == 0 {
			return errors.New("sampled counters do not match the authoritative sampling rate")
		}
		if record.EstimatedValid {
			if !scaledCountersMatch(record) {
				return errors.New("sampled counters do not match the authoritative sampling rate")
			}
		} else if record.QualityFlags&QualityCounterOverflow == 0 || record.EstimatedBytes != 0 || record.EstimatedPackets != 0 {
			return errors.New("invalid sampled counters require an explicit overflow marker")
		}
	case 2:
		if !record.EstimatedValid || record.EstimatedBytes != record.RawBytes || record.EstimatedPackets != record.RawPackets {
			return errors.New("pre-scaled counters must not be multiplied again")
		}
	default:
		return errors.New("sampling mode is invalid")
	}
	return nil
}

func scaledCountersMatch(record *Record) bool {
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

func endpointPorts(record *Record, dimensions flowdimension.ClassifiedEndpoints) (uint16, uint16) {
	port := func(side flowdimension.EndpointSide) uint16 {
		switch side {
		case flowdimension.EndpointSrc:
			return uint16(record.SourcePort)
		case flowdimension.EndpointDst:
			return uint16(record.DestinationPort)
		default:
			return 0
		}
	}
	return port(dimensions.Local.Side), port(dimensions.Remote.Side)
}

func selectRemoteASN(record *Record, side flowdimension.EndpointSide, geo flowdimension.GeoInfo, geoMatched bool, overrideFields flowdimension.GeoOverrideFields) (uint32, ASNSource) {
	if overrideFields&flowdimension.GeoOverrideASN != 0 {
		return geo.ASN, ASNSourceOverride
	}
	if geoMatched && geo.ASN != 0 {
		switch geo.Source {
		case flowdimension.GeoSchemaV1:
			return geo.ASN, ASNSourceGeoV1
		case flowdimension.GeoSchemaV2:
			return geo.ASN, ASNSourceGeoV2
		}
	}
	var exporterASN uint32
	switch side {
	case flowdimension.EndpointSrc:
		exporterASN = record.SourceASN
	case flowdimension.EndpointDst:
		exporterASN = record.DestinationASN
	}
	if exporterASN != 0 {
		return exporterASN, ASNSourceExporter
	}
	return 0, ASNSourceUnknown
}

func blocked(dependency, tenantID, sourceStreamID string, kafkaPartition int32, kafkaOffset int64, record *Record, eventTime time.Time, cause error) error {
	return &VersionBlockedError{Dependency: dependency, TenantID: tenantID, SourceStreamID: sourceStreamID, KafkaPartition: kafkaPartition, KafkaOffset: kafkaOffset, RecordIndex: record.RecordIndex, EventTime: eventTime, Cause: cause}
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidRecordBatch, message)
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
