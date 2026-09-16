// SPDX-FileCopyrightText: 2025 Free Mobile
// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

const (
	defaultClickHouseAddress  = "127.0.0.1:9000"
	defaultClickHouseDatabase = "watchdog_flow"
	defaultOperationTimeout   = 2 * time.Minute
	flowRecordsTable          = "flow_records"
	flowReceiptsTable         = "flow_ingest_receipts"
	flowQuarantineTable       = "flow_quarantined_datagrams"
	receiptSchemaVersion      = 4
	factSchemaVersion         = 3
)

type NativeConfig struct {
	Address     string
	Database    string
	User        string
	Password    string
	ClientName  string
	DialTimeout time.Duration
	ReadTimeout time.Duration
	// OperationTimeout bounds one complete query. ch-go's ReadTimeout only
	// bounds a packet read before it polls again, so it is not a query deadline.
	OperationTimeout time.Duration
	MaxConns         int32
	MinConns         int32
	TLS              *tls.Config
}

type queryExecutor interface {
	Do(context.Context, ch.Query) error
}

type NativeInserter struct {
	executor queryExecutor
	close    func()
}

type operationTimeoutExecutor struct {
	next    queryExecutor
	timeout time.Duration
}

func (e operationTimeoutExecutor) Do(ctx context.Context, query ch.Query) error {
	bounded, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	return e.next.Do(bounded, query)
}

// NewNativeInserter creates the bounded connection pool used by partition
// workers. ch-go clients represent one connection and cannot run Do
// concurrently; chpool owns that concurrency boundary.
func NewNativeInserter(ctx context.Context, config NativeConfig) (*NativeInserter, error) {
	if config.Address == "" {
		config.Address = defaultClickHouseAddress
	}
	if config.Database == "" {
		config.Database = defaultClickHouseDatabase
	}
	if config.ClientName == "" {
		config.ClientName = "watchdog-flow-worker"
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = defaultOperationTimeout
	}
	if !validClickHouseIdentifier(config.Database) {
		return nil, errors.New("ClickHouse database name is invalid")
	}
	if config.MaxConns < 0 || config.MinConns < 0 || (config.MaxConns != 0 && config.MinConns > config.MaxConns) {
		return nil, errors.New("ClickHouse connection limits are invalid")
	}
	if config.DialTimeout < 0 || config.ReadTimeout < 0 || config.OperationTimeout < 0 {
		return nil, errors.New("ClickHouse timeouts must not be negative")
	}
	var tlsConfig *tls.Config
	if config.TLS != nil {
		tlsConfig = config.TLS.Clone()
	}
	dialCtx, cancelDial := context.WithTimeout(ctx, config.OperationTimeout)
	defer cancelDial()
	pool, err := chpool.Dial(dialCtx, chpool.Options{
		ClientOptions: ch.Options{
			Address: config.Address, Database: config.Database, User: config.User, Password: config.Password,
			DialTimeout: config.DialTimeout, ReadTimeout: config.ReadTimeout, TLS: tlsConfig,
			Compression: ch.CompressionLZ4, ClientName: config.ClientName,
		},
		MaxConns: config.MaxConns,
		MinConns: config.MinConns,
	})
	if err != nil {
		return nil, fmt.Errorf("connect ClickHouse native endpoint: %w", err)
	}
	return &NativeInserter{
		executor: operationTimeoutExecutor{next: pool, timeout: config.OperationTimeout},
		close:    pool.Close,
	}, nil
}

func (n *NativeInserter) Close() {
	if n != nil && n.close != nil {
		n.close()
		n.close = nil
	}
}

// Do executes an already compiled ClickHouse query on the same bounded native
// pool used by Flow ingestion and rollups. SQL construction remains owned by
// the typed flowquery/flowch compilers; the hub only shares the connection
// lifecycle instead of opening a second query-only pool.
func (n *NativeInserter) Do(ctx context.Context, query ch.Query) error {
	if n == nil || n.executor == nil {
		return errors.New("ClickHouse native connection is not initialized")
	}
	return n.executor.Do(ctx, query)
}

// Ready verifies the selected database and the Flow Storage V2 contract without
// reading application data. A V1 table must fail before a worker consumes Kafka
// or the hub advertises a query provider as ready.
func (n *NativeInserter) Ready(ctx context.Context) error {
	if n == nil || n.executor == nil {
		return errors.New("ClickHouse native connection is not initialized")
	}
	var requiredRecords, requiredReceipts, requiredQuarantine, forbiddenRecords, forbiddenReceipts proto.ColUInt64
	seen := false
	query := ch.Query{
		Body: `SELECT
  countIf(table = 'flow_records' AND name IN (
    'source_stream_id', 'kafka_partition', 'kafka_offset', 'record_index', 'ingest_generation'
  )) AS required_records,
  countIf(table = 'flow_ingest_receipts' AND name IN (
    'source_stream_id', 'kafka_partition', 'kafka_offset', 'message_disposition',
    'record_count', 'raw_bytes', 'raw_packets', 'estimated_bytes',
    'estimated_packets', 'estimated_valid_records', 'generation'
  )) AS required_receipts,
	countIf(table = 'flow_quarantined_datagrams' AND name IN (
	  'source_stream_id', 'kafka_partition', 'kafka_offset', 'barrier_revision',
	  'raw_payload', 'decoded_record_count', 'matched_event_day'
	)) + countIf(table = 'flow_ingest_receipts' AND name = 'message_disposition' AND position(type, 'late_quarantined') > 0) AS required_quarantine,
  countIf(table = 'flow_records' AND name IN ('record_id', 'ingest_batch_id', 'dimension_fingerprint')) AS forbidden_records,
  countIf(table = 'flow_ingest_receipts' AND name IN ('ingest_batch_id', 'checksum', 'first_offset', 'last_offset')) AS forbidden_receipts
FROM system.columns
WHERE database = currentDatabase() AND table IN ('flow_records', 'flow_ingest_receipts', 'flow_quarantined_datagrams')`,
		Result: proto.Results{
			{Name: "required_records", Data: &requiredRecords},
			{Name: "required_receipts", Data: &requiredReceipts},
			{Name: "required_quarantine", Data: &requiredQuarantine},
			{Name: "forbidden_records", Data: &forbiddenRecords},
			{Name: "forbidden_receipts", Data: &forbiddenReceipts},
		},
	}
	query.OnResult = func(_ context.Context, block proto.Block) error {
		if block.Rows == 0 {
			return nil
		}
		if seen || block.Rows != 1 || requiredRecords.Rows() != 1 || requiredReceipts.Rows() != 1 || requiredQuarantine.Rows() != 1 || forbiddenRecords.Rows() != 1 || forbiddenReceipts.Rows() != 1 {
			return errors.New("ClickHouse Flow schema readiness returned an invalid row count")
		}
		seen = true
		return nil
	}
	if err := n.executor.Do(ctx, query); err != nil {
		return fmt.Errorf("verify ClickHouse Flow Storage V2 schema: %w", err)
	}
	if !seen || requiredRecords[0] != 5 || requiredReceipts[0] != 11 || requiredQuarantine[0] != 8 || forbiddenRecords[0] != 0 || forbiddenReceipts[0] != 0 {
		return fmt.Errorf("ClickHouse Flow schema is not Storage V2 (records=%d/5 receipts=%d/11 quarantine=%d/8 forbidden=%d/%d)",
			columnOrZero(requiredRecords), columnOrZero(requiredReceipts), columnOrZero(requiredQuarantine), columnOrZero(forbiddenRecords), columnOrZero(forbiddenReceipts))
	}
	return nil
}

func columnOrZero(column proto.ColUInt64) uint64 {
	if column.Rows() == 0 {
		return 0
	}
	return column[0]
}

// InsertFlowBlock is synchronous. A retry uses exactly the same block and
// deduplication tokens; the ReplacingMergeTree generation is also stable.
func (n *NativeInserter) InsertFlowBlock(ctx context.Context, block PreparedBlock) error {
	if n == nil || n.executor == nil {
		return Permanent(errors.New("ClickHouse native inserter is not initialized"))
	}
	if err := validatePreparedReceipts(block); err != nil {
		return Permanent(err)
	}
	token := blockDeduplicationToken(block)
	if len(block.Records) > 0 {
		records, err := buildRecordInput(block)
		if err != nil {
			return Permanent(err)
		}
		if err := n.executor.Do(ctx, insertQuery(flowRecordsTable, token, records)); err != nil {
			return classifyClickHouseError(fmt.Errorf("insert flow records: %w", err))
		}
	} else if len(block.Receipts) == 0 {
		return Permanent(fmt.Errorf("%w: prepared block has neither records nor receipts", ErrInvalidBatchGroup))
	}
	if len(block.Receipts) > 0 {
		receipt := buildReceiptInput(block)
		if err := n.executor.Do(ctx, insertQuery(flowReceiptsTable, token+":receipts", receipt)); err != nil {
			return classifyClickHouseError(fmt.Errorf("insert flow receipts: %w", err))
		}
	}
	return nil
}

func blockDeduplicationToken(block PreparedBlock) string {
	return fmt.Sprintf("flow-v2:%s:%d:%d:%d:%d:%d:%d:%d", block.SourceStreamID, block.KafkaPartition,
		block.FirstOffset, block.FirstRecordIndex, block.LastOffset, block.LastRecordIndex, len(block.Records), len(block.Receipts))
}

func insertQuery(table, token string, input proto.Input) ch.Query {
	return ch.Query{
		Body:  input.Into(table),
		Input: input,
		Settings: []ch.Setting{
			{Key: "async_insert", Value: "0", Important: true},
			{Key: "wait_for_async_insert", Value: "1", Important: true},
			{Key: "insert_deduplication_token", Value: token, Important: true},
		},
	}
}

func buildRecordInput(block PreparedBlock) (proto.Input, error) {
	if !flowworker.ValidSourceStreamID(block.SourceStreamID) || block.KafkaPartition < 0 || block.FirstOffset < 0 || block.LastOffset < block.FirstOffset || len(block.Records) == 0 {
		return nil, fmt.Errorf("%w: prepared block identity or records are missing", ErrInvalidBatchGroup)
	}
	var (
		eventTime                 = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		receivedTime              = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		sourceStreamID            = new(proto.ColStr).LowCardinality()
		ingestGeneration          proto.ColUInt64
		kafkaTopic                = new(proto.ColStr).LowCardinality()
		kafkaPartition            proto.ColUInt32
		kafkaOffset               proto.ColUInt64
		recordIndex               proto.ColUInt32
		collectorID               = new(proto.ColStr).LowCardinality()
		exporterID                = new(proto.ColStr).LowCardinality()
		targetID                  = new(proto.ColStr).LowCardinality()
		deviceID                  = new(proto.ColStr).LowCardinality()
		registryVersion           proto.ColUInt64
		exporterEpoch             proto.ColUInt64
		exporterSourceIP          proto.ColIPv6
		flowProtocol              proto.ColUInt8
		observationDomainID       proto.ColUInt64
		subAgentID                proto.ColUInt32
		datagramSequence          proto.ColUInt32
		agentIP                   proto.ColIPv6
		agentIPValid              proto.ColBool
		observationIfIndex        proto.ColUInt32
		ingressIfIndex            proto.ColUInt32
		egressIfIndex             proto.ColUInt32
		observationDirection      proto.ColEnum
		srcIP                     proto.ColIPv6
		dstIP                     proto.ColIPv6
		srcPort                   proto.ColUInt16
		dstPort                   proto.ColUInt16
		ipProtocol                proto.ColUInt8
		tcpFlags                  proto.ColUInt8
		sourceASN                 proto.ColUInt32
		destinationASN            proto.ColUInt32
		rawBytes                  proto.ColUInt64
		rawPackets                proto.ColUInt64
		samplingMode              proto.ColEnum
		samplingRate              proto.ColUInt64
		samplingSource            proto.ColEnum
		estimatedValid            proto.ColBool
		estimatedBytes            proto.ColUInt64
		estimatedPackets          proto.ColUInt64
		flowDurationMS            proto.ColUInt64
		qualityFlags              proto.ColUInt64
		sourceIDType              proto.ColUInt32
		sourceIDValue             proto.ColUInt32
		sampleSequence            proto.ColUInt32
		samplePool                proto.ColUInt64
		exporterDrops             proto.ColUInt64
		sampleIndex               proto.ColUInt32
		qualityEpoch              proto.ColUInt64
		dimensionSnapshotID       = new(proto.ColStr).LowCardinality()
		dimensionVersion          proto.ColUInt64
		businessDirection         proto.ColEnum
		business                  = new(proto.ColStr).LowCardinality()
		localIP                   proto.ColIPv6
		localIPValid              proto.ColBool
		remoteIP                  proto.ColIPv6
		remoteIPValid             proto.ColBool
		localPort                 proto.ColUInt16
		remotePort                proto.ColUInt16
		localPrefixID             = new(proto.ColStr).LowCardinality()
		remotePrefixID            = new(proto.ColStr).LowCardinality()
		localAddressSetIDs        = new(proto.ColStr).Array()
		remoteAddressSetIDs       = new(proto.ColStr).Array()
		remoteCountry             = &proto.ColFixedStr{Size: 2}
		remoteAdminCode           = new(proto.ColStr).LowCardinality()
		remoteSubdivision         = new(proto.ColStr).LowCardinality()
		remoteCity                = new(proto.ColStr).LowCardinality()
		remoteGeoContinentID      = new(proto.ColStr).LowCardinality()
		remoteGeoRegionID         = new(proto.ColStr).LowCardinality()
		remoteGeoCountryID        = new(proto.ColStr).LowCardinality()
		remoteGeoProvinceID       = new(proto.ColStr).LowCardinality()
		remoteGeoCityID           = new(proto.ColStr).LowCardinality()
		remoteISPID               proto.ColUInt16
		remoteASN                 proto.ColUInt32
		remoteASNSource           proto.ColEnum
		geoVersion                = new(proto.ColStr).LowCardinality()
		category                  proto.ColEnum
		disposition               proto.ColEnum
		classificationVersion     proto.ColUInt32
		factSchema                proto.ColUInt16
		supplierRemoteCountry     = &proto.ColFixedStr{Size: 2}
		supplierRemoteAdminCode   = new(proto.ColStr).LowCardinality()
		supplierRemoteSubdivision = new(proto.ColStr).LowCardinality()
		supplierRemoteCity        = new(proto.ColStr).LowCardinality()
		supplierRemoteContinentID = new(proto.ColStr).LowCardinality()
		supplierRemoteRegionID    = new(proto.ColStr).LowCardinality()
		supplierRemoteCountryID   = new(proto.ColStr).LowCardinality()
		supplierRemoteProvinceID  = new(proto.ColStr).LowCardinality()
		supplierRemoteCityID      = new(proto.ColStr).LowCardinality()
		supplierRemoteISPID       proto.ColUInt16
		supplierRemoteASN         proto.ColUInt32
		supplierRemoteASNSource   proto.ColEnum
		supplierGeoVersion        = new(proto.ColStr).LowCardinality()
		supplierCategory          proto.ColEnum
		customerGeoOverrideFields proto.ColUInt8
	)

	var localAddressSetScratch, remoteAddressSetScratch []string
	for index, ref := range block.Records {
		if ref.Batch == nil || ref.Record == nil || ref.Batch.ReceivedAt.UnixMilli() <= 0 || ref.Record.EventTime.UnixMilli() <= 0 {
			return nil, fmt.Errorf("%w: record %d has incomplete timestamps", ErrInvalidBatchGroup, index)
		}
		observation, ok := observationDirectionName(ref.Record.ObservationDirection)
		if !ok {
			return nil, fmt.Errorf("%w: record %d has invalid observation direction", ErrInvalidBatchGroup, index)
		}
		mode, ok := samplingModeName(ref.Record.SamplingMode)
		if !ok {
			return nil, fmt.Errorf("%w: record %d has invalid sampling mode", ErrInvalidBatchGroup, index)
		}
		source, ok := samplingSourceName(ref.Record.SamplingSource)
		if !ok {
			return nil, fmt.Errorf("%w: record %d has invalid sampling source", ErrInvalidBatchGroup, index)
		}
		direction, ok := businessDirectionName(ref.Record.Dimensions.Direction)
		if !ok {
			return nil, fmt.Errorf("%w: record %d has invalid business direction", ErrInvalidBatchGroup, index)
		}
		customerCategoryName, ok := categoryName(ref.Record.Category)
		if !ok {
			return nil, fmt.Errorf("%w: record %d has invalid category", ErrInvalidBatchGroup, index)
		}
		dispositionName, ok := dispositionName(ref.Record.Disposition)
		if !ok {
			return nil, fmt.Errorf("%w: record %d has invalid disposition", ErrInvalidBatchGroup, index)
		}
		asnSource, ok := asnSourceName(ref.Record.RemoteASNSource)
		if !ok {
			return nil, fmt.Errorf("%w: record %d has invalid remote ASN source", ErrInvalidBatchGroup, index)
		}
		supplierASNSource, ok := asnSourceName(ref.Record.SupplierRemoteASNSource)
		if !ok || supplierASNSource == "flow_geo_override" {
			return nil, fmt.Errorf("%w: record %d has invalid supplier remote ASN source", ErrInvalidBatchGroup, index)
		}
		supplierCategoryName, ok := categoryName(ref.Record.SupplierCategory)
		if !ok {
			return nil, fmt.Errorf("%w: record %d has invalid supplier category", ErrInvalidBatchGroup, index)
		}
		if !ref.Record.CustomerGeoOverrideFields.Valid() {
			return nil, fmt.Errorf("%w: record %d has invalid customer Geo override fields", ErrInvalidBatchGroup, index)
		}
		received := ref.Batch.ReceivedAt.UTC()
		rowGeneration := uint64(received.UnixMilli())

		eventTime.Append(ref.Record.EventTime.UTC())
		receivedTime.Append(received)
		sourceStreamID.Append(ref.Batch.SourceStreamID)
		ingestGeneration.Append(rowGeneration)
		kafkaTopic.Append(ref.Batch.KafkaTopic)
		kafkaPartition.Append(uint32(ref.Batch.KafkaPartition))
		kafkaOffset.Append(uint64(ref.Batch.KafkaOffset))
		recordIndex.Append(ref.Record.RecordIndex)
		collectorID.Append(ref.Batch.CollectorID)
		exporterID.Append(ref.Batch.ExporterID)
		targetID.Append(ref.Record.TargetID)
		deviceID.Append(ref.Record.DeviceID)
		registryVersion.Append(ref.Batch.RegistryVersion)
		exporterEpoch.Append(ref.Batch.ExporterEpoch)
		exporterSourceIP.Append(clickHouseIP(ref.Batch.SourceIP))
		flowProtocol.Append(ref.Batch.Protocol)
		observationDomainID.Append(ref.Batch.ObservationDomainID)
		subAgentID.Append(ref.Batch.SubAgentID)
		datagramSequence.Append(ref.Batch.DatagramSequence)
		agentIP.Append(clickHouseIP(ref.Batch.AgentIP))
		agentIPValid.Append(ref.Batch.AgentIP.IsValid())
		observationIfIndex.Append(ref.Record.ObservationIfIndex)
		ingressIfIndex.Append(ref.Record.InIf)
		egressIfIndex.Append(ref.Record.OutIf)
		observationDirection.Append(observation)
		srcIP.Append(clickHouseIP(ref.Record.SourceIP))
		dstIP.Append(clickHouseIP(ref.Record.DestinationIP))
		srcPort.Append(ref.Record.SourcePort)
		dstPort.Append(ref.Record.DestinationPort)
		ipProtocol.Append(ref.Record.IPProtocol)
		tcpFlags.Append(ref.Record.TCPFlags)
		sourceASN.Append(ref.Record.SourceASN)
		destinationASN.Append(ref.Record.DestinationASN)
		rawBytes.Append(ref.Record.RawBytes)
		rawPackets.Append(ref.Record.RawPackets)
		samplingMode.Append(mode)
		samplingRate.Append(ref.Record.SamplingRate)
		samplingSource.Append(source)
		estimatedValid.Append(ref.Record.EstimatedValid)
		estimatedBytes.Append(ref.Record.EstimatedBytes)
		estimatedPackets.Append(ref.Record.EstimatedPackets)
		flowDurationMS.Append(ref.Record.FlowDurationMS)
		qualityFlags.Append(ref.Record.QualityFlags)
		sourceIDType.Append(ref.Record.SourceIDType)
		sourceIDValue.Append(ref.Record.SourceIDValue)
		sampleSequence.Append(ref.Record.SampleSequence)
		samplePool.Append(ref.Record.SamplePool)
		exporterDrops.Append(ref.Record.ExporterDrops)
		sampleIndex.Append(ref.Record.SampleIndex)
		qualityEpoch.Append(ref.Record.QualityEpoch)
		dimensionSnapshotID.Append(ref.Record.Dimensions.SnapshotID)
		dimensionVersion.Append(ref.Record.Dimensions.Version)
		businessDirection.Append(direction)
		business.Append(ref.Record.Dimensions.Business)
		localIP.Append(clickHouseIP(ref.Record.Dimensions.Local.IP))
		localIPValid.Append(ref.Record.Dimensions.Local.IP.IsValid())
		remoteIP.Append(clickHouseIP(ref.Record.Dimensions.Remote.IP))
		remoteIPValid.Append(ref.Record.Dimensions.Remote.IP.IsValid())
		localPort.Append(ref.Record.LocalPort)
		remotePort.Append(ref.Record.RemotePort)
		localPrefixID.Append(ref.Record.Dimensions.Local.PrefixID)
		remotePrefixID.Append(ref.Record.Dimensions.Remote.PrefixID)
		localAddressSetScratch = ref.Record.Dimensions.Local.AddressSets.AppendTo(localAddressSetScratch[:0])
		remoteAddressSetScratch = ref.Record.Dimensions.Remote.AddressSets.AppendTo(remoteAddressSetScratch[:0])
		localAddressSetIDs.Append(localAddressSetScratch)
		remoteAddressSetIDs.Append(remoteAddressSetScratch)
		remoteCountry.Append(countryCode(ref.Record.RemoteGeo.Country))
		remoteAdminCode.Append(ref.Record.RemoteGeo.AdminCode)
		remoteSubdivision.Append(ref.Record.RemoteGeo.Subdivision)
		remoteCity.Append(ref.Record.RemoteGeo.City)
		remoteGeoContinentID.Append(ref.Record.RemoteGeo.ContinentID)
		remoteGeoRegionID.Append(ref.Record.RemoteGeo.RegionID)
		remoteGeoCountryID.Append(ref.Record.RemoteGeo.CountryID)
		remoteGeoProvinceID.Append(ref.Record.RemoteGeo.ProvinceID)
		remoteGeoCityID.Append(ref.Record.RemoteGeo.CityID)
		remoteISPID.Append(ref.Record.RemoteGeo.ISPID)
		remoteASN.Append(ref.Record.RemoteASN)
		remoteASNSource.Append(asnSource)
		geoVersion.Append(ref.Record.RemoteGeo.Version)
		category.Append(customerCategoryName)
		disposition.Append(dispositionName)
		classificationVersion.Append(ref.Record.ClassificationVersion)
		factSchema.Append(factSchemaVersion)
		supplierRemoteCountry.Append(countryCode(ref.Record.SupplierRemoteGeo.Country))
		supplierRemoteAdminCode.Append(ref.Record.SupplierRemoteGeo.AdminCode)
		supplierRemoteSubdivision.Append(ref.Record.SupplierRemoteGeo.Subdivision)
		supplierRemoteCity.Append(ref.Record.SupplierRemoteGeo.City)
		supplierRemoteContinentID.Append(ref.Record.SupplierRemoteGeo.ContinentID)
		supplierRemoteRegionID.Append(ref.Record.SupplierRemoteGeo.RegionID)
		supplierRemoteCountryID.Append(ref.Record.SupplierRemoteGeo.CountryID)
		supplierRemoteProvinceID.Append(ref.Record.SupplierRemoteGeo.ProvinceID)
		supplierRemoteCityID.Append(ref.Record.SupplierRemoteGeo.CityID)
		supplierRemoteISPID.Append(ref.Record.SupplierRemoteGeo.ISPID)
		supplierRemoteASN.Append(ref.Record.SupplierRemoteASN)
		supplierRemoteASNSource.Append(supplierASNSource)
		supplierGeoVersion.Append(ref.Record.SupplierRemoteGeo.Version)
		supplierCategory.Append(supplierCategoryName)
		customerGeoOverrideFields.Append(uint8(ref.Record.CustomerGeoOverrideFields))
	}

	return proto.Input{
		{Name: "event_time", Data: eventTime}, {Name: "received_time", Data: receivedTime},
		{Name: "source_stream_id", Data: sourceStreamID}, {Name: "ingest_generation", Data: ingestGeneration},
		{Name: "kafka_topic", Data: kafkaTopic}, {Name: "kafka_partition", Data: kafkaPartition}, {Name: "kafka_offset", Data: kafkaOffset}, {Name: "record_index", Data: recordIndex},
		{Name: "collector_id", Data: collectorID}, {Name: "exporter_id", Data: exporterID}, {Name: "target_id", Data: targetID}, {Name: "device_id", Data: deviceID},
		{Name: "registry_version", Data: registryVersion}, {Name: "exporter_epoch", Data: exporterEpoch}, {Name: "exporter_source_ip", Data: exporterSourceIP}, {Name: "flow_protocol", Data: flowProtocol},
		{Name: "observation_domain_id", Data: observationDomainID}, {Name: "sub_agent_id", Data: subAgentID}, {Name: "datagram_sequence", Data: datagramSequence}, {Name: "agent_ip", Data: agentIP}, {Name: "agent_ip_valid", Data: agentIPValid},
		{Name: "observation_if_index", Data: observationIfIndex}, {Name: "ingress_if_index", Data: ingressIfIndex}, {Name: "egress_if_index", Data: egressIfIndex}, {Name: "observation_direction", Data: &observationDirection},
		{Name: "src_ip", Data: srcIP}, {Name: "dst_ip", Data: dstIP}, {Name: "src_port", Data: srcPort}, {Name: "dst_port", Data: dstPort}, {Name: "ip_protocol", Data: ipProtocol}, {Name: "tcp_flags", Data: tcpFlags},
		{Name: "source_asn", Data: sourceASN}, {Name: "destination_asn", Data: destinationASN}, {Name: "raw_bytes", Data: rawBytes}, {Name: "raw_packets", Data: rawPackets},
		{Name: "sampling_mode", Data: &samplingMode}, {Name: "sampling_rate", Data: samplingRate}, {Name: "sampling_source", Data: &samplingSource},
		{Name: "estimated_valid", Data: estimatedValid}, {Name: "estimated_bytes", Data: estimatedBytes}, {Name: "estimated_packets", Data: estimatedPackets},
		{Name: "flow_duration_ms", Data: flowDurationMS}, {Name: "quality_flags", Data: qualityFlags}, {Name: "source_id_type", Data: sourceIDType}, {Name: "source_id_value", Data: sourceIDValue},
		{Name: "sample_sequence", Data: sampleSequence}, {Name: "sample_pool", Data: samplePool}, {Name: "exporter_drops", Data: exporterDrops}, {Name: "sample_index", Data: sampleIndex}, {Name: "quality_epoch", Data: qualityEpoch},
		{Name: "dimension_snapshot_id", Data: dimensionSnapshotID}, {Name: "dimension_version", Data: dimensionVersion},
		{Name: "business_direction", Data: &businessDirection}, {Name: "business", Data: business},
		{Name: "local_ip", Data: localIP}, {Name: "local_ip_valid", Data: localIPValid}, {Name: "remote_ip", Data: remoteIP}, {Name: "remote_ip_valid", Data: remoteIPValid},
		{Name: "local_port", Data: localPort}, {Name: "remote_port", Data: remotePort}, {Name: "local_prefix_id", Data: localPrefixID}, {Name: "remote_prefix_id", Data: remotePrefixID},
		{Name: "local_address_set_ids", Data: localAddressSetIDs}, {Name: "remote_address_set_ids", Data: remoteAddressSetIDs},
		{Name: "remote_country", Data: remoteCountry}, {Name: "remote_admin_code", Data: remoteAdminCode}, {Name: "remote_subdivision", Data: remoteSubdivision}, {Name: "remote_city", Data: remoteCity},
		{Name: "remote_geo_continent_id", Data: remoteGeoContinentID}, {Name: "remote_geo_region_id", Data: remoteGeoRegionID}, {Name: "remote_geo_country_id", Data: remoteGeoCountryID},
		{Name: "remote_geo_province_id", Data: remoteGeoProvinceID}, {Name: "remote_geo_city_id", Data: remoteGeoCityID},
		{Name: "remote_isp_id", Data: remoteISPID}, {Name: "remote_asn", Data: remoteASN}, {Name: "remote_asn_source", Data: &remoteASNSource}, {Name: "geo_version", Data: geoVersion},
		{Name: "category", Data: &category}, {Name: "disposition", Data: &disposition}, {Name: "classification_version", Data: classificationVersion},
		{Name: "fact_schema", Data: factSchema},
		{Name: "supplier_remote_country", Data: supplierRemoteCountry}, {Name: "supplier_remote_admin_code", Data: supplierRemoteAdminCode},
		{Name: "supplier_remote_subdivision", Data: supplierRemoteSubdivision}, {Name: "supplier_remote_city", Data: supplierRemoteCity},
		{Name: "supplier_remote_geo_continent_id", Data: supplierRemoteContinentID}, {Name: "supplier_remote_geo_region_id", Data: supplierRemoteRegionID},
		{Name: "supplier_remote_geo_country_id", Data: supplierRemoteCountryID}, {Name: "supplier_remote_geo_province_id", Data: supplierRemoteProvinceID},
		{Name: "supplier_remote_geo_city_id", Data: supplierRemoteCityID}, {Name: "supplier_remote_isp_id", Data: supplierRemoteISPID},
		{Name: "supplier_remote_asn", Data: supplierRemoteASN}, {Name: "supplier_remote_asn_source", Data: &supplierRemoteASNSource},
		{Name: "supplier_geo_version", Data: supplierGeoVersion}, {Name: "supplier_category", Data: &supplierCategory},
		{Name: "customer_geo_override_fields", Data: customerGeoOverrideFields},
	}, nil
}

func buildReceiptInput(block PreparedBlock) proto.Input {
	var (
		sourceStreamID     = new(proto.ColStr).LowCardinality()
		workerSchema       proto.ColUInt32
		receiptSchema      proto.ColUInt16
		receiptDisposition proto.ColEnum
		kafkaTopic         = new(proto.ColStr).LowCardinality()
		kafkaPartition     proto.ColUInt32
		kafkaOffset        proto.ColUInt64
		recordCount        proto.ColUInt64
		rawBytes           proto.ColUInt64
		rawPackets         proto.ColUInt64
		estimatedBytes     proto.ColUInt64
		estimatedPackets   proto.ColUInt64
		estimatedValid     proto.ColUInt64
		minEventTime       = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		maxEventTime       = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		generationCol      proto.ColUInt64
		legacyInsertedAt   = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
	)
	for _, receipt := range block.Receipts {
		disposition, _ := messageDispositionName(receipt.Disposition)
		sourceStreamID.Append(receipt.SourceStreamID)
		workerSchema.Append(WorkerSchemaVersion)
		receiptSchema.Append(receiptSchemaVersion)
		receiptDisposition.Append(disposition)
		kafkaTopic.Append(receipt.KafkaTopic)
		kafkaPartition.Append(uint32(receipt.KafkaPartition))
		kafkaOffset.Append(uint64(receipt.KafkaOffset))
		recordCount.Append(receipt.RecordCount)
		rawBytes.Append(receipt.RawBytes)
		rawPackets.Append(receipt.RawPackets)
		estimatedBytes.Append(receipt.EstimatedBytes)
		estimatedPackets.Append(receipt.EstimatedPackets)
		estimatedValid.Append(receipt.EstimatedValidRecords)
		minEventTime.Append(receipt.MinEventTime.UTC())
		maxEventTime.Append(receipt.MaxEventTime.UTC())
		rowGeneration := receipt.Generation
		if rowGeneration == 0 {
			rowGeneration = uint64(receipt.ReceivedAt.UnixMilli())
		}
		generationCol.Append(rowGeneration)
		legacyInsertedAt.Append(receipt.ReceivedAt.UTC())
	}
	return proto.Input{
		{Name: "source_stream_id", Data: sourceStreamID}, {Name: "worker_schema", Data: workerSchema}, {Name: "receipt_schema", Data: receiptSchema}, {Name: "message_disposition", Data: &receiptDisposition}, {Name: "kafka_topic", Data: kafkaTopic},
		{Name: "kafka_partition", Data: kafkaPartition}, {Name: "kafka_offset", Data: kafkaOffset},
		{Name: "record_count", Data: recordCount}, {Name: "raw_bytes", Data: rawBytes}, {Name: "raw_packets", Data: rawPackets},
		{Name: "estimated_bytes", Data: estimatedBytes}, {Name: "estimated_packets", Data: estimatedPackets}, {Name: "estimated_valid_records", Data: estimatedValid},
		{Name: "min_event_time", Data: minEventTime}, {Name: "max_event_time", Data: maxEventTime}, {Name: "generation", Data: generationCol}, {Name: "inserted_at", Data: legacyInsertedAt},
	}
}

func validatePreparedReceipts(block PreparedBlock) error {
	if !flowworker.ValidSourceStreamID(block.SourceStreamID) || block.KafkaPartition < 0 || block.FirstOffset < 0 || block.LastOffset < block.FirstOffset {
		return fmt.Errorf("%w: prepared block identity is invalid", ErrInvalidBatchGroup)
	}
	for index, receipt := range block.Receipts {
		_, validDisposition := messageDispositionName(receipt.Disposition)
		if !validDisposition || receipt.SourceStreamID != block.SourceStreamID || receipt.KafkaTopic != block.KafkaTopic ||
			receipt.KafkaPartition != block.KafkaPartition || receipt.KafkaOffset < block.FirstOffset || receipt.KafkaOffset > block.LastOffset ||
			receipt.ReceivedAt.UnixMilli() <= 0 {
			return fmt.Errorf("%w: prepared receipt %d identity is invalid", ErrInvalidBatchGroup, index)
		}
		if receipt.Disposition == flowworker.MessageDispositionPersisted && receipt.RecordCount == 0 {
			return fmt.Errorf("%w: persisted receipt %d has no records", ErrInvalidBatchGroup, index)
		}
		if receipt.Disposition != flowworker.MessageDispositionPersisted && receipt.Disposition != flowworker.MessageDispositionLateQuarantined &&
			(receipt.RecordCount != 0 || receipt.RawBytes != 0 || receipt.RawPackets != 0 || receipt.EstimatedBytes != 0 || receipt.EstimatedPackets != 0 || receipt.EstimatedValidRecords != 0) {
			return fmt.Errorf("%w: non-persisted receipt %d has counters", ErrInvalidBatchGroup, index)
		}
	}
	return nil
}

func messageDispositionName(value flowworker.MessageDisposition) (string, bool) {
	switch value {
	case flowworker.MessageDispositionPersisted:
		return "persisted", true
	case flowworker.MessageDispositionTemplateMissing:
		return "template_missing", true
	case flowworker.MessageDispositionEmpty:
		return "empty", true
	case flowworker.MessageDispositionDecodeRejected:
		return "decode_rejected", true
	case flowworker.MessageDispositionMappingRejected:
		return "mapping_rejected", true
	case flowworker.MessageDispositionLateQuarantined:
		return "late_quarantined", true
	default:
		return "", false
	}
}

func clickHouseIP(address netip.Addr) proto.IPv6 {
	if !address.IsValid() {
		return proto.IPv6{}
	}
	return proto.ToIPv6(address)
}

func countryCode(value string) []byte {
	value = strings.ToUpper(strings.TrimSpace(value))
	if len(value) != 2 {
		value = flowdimension.GeoUnknownCountry
	}
	return []byte(value)
}

func observationDirectionName(value flowworker.ObservationDirection) (string, bool) {
	switch value {
	case flowworker.ObservationUnknown:
		return "unknown", true
	case flowworker.ObservationIngress:
		return "ingress", true
	case flowworker.ObservationEgress:
		return "egress", true
	default:
		return "", false
	}
}

func samplingModeName(value uint8) (string, bool) {
	switch uint32(value) {
	case flowworker.SamplingUnknown:
		return "unknown", true
	case flowworker.SamplingSampled:
		return "sampled", true
	case flowworker.SamplingPreScaled:
		return "pre_scaled", true
	default:
		return "", false
	}
}

func samplingSourceName(value uint8) (string, bool) {
	names := [...]string{"unknown", "protocol", "plan_rule", "exporter_default", "counter_mode"}
	if int(value) >= len(names) {
		return "", false
	}
	return names[value], true
}

func businessDirectionName(value flowdimension.BusinessDirection) (string, bool) {
	switch value {
	case flowdimension.DirectionAmbiguous, flowdimension.DirectionIn, flowdimension.DirectionOut, flowdimension.DirectionInternal, flowdimension.DirectionTransit:
		return string(value), true
	default:
		return "", false
	}
}

func categoryName(value flowdimension.Category) (string, bool) {
	switch value {
	case flowdimension.CategoryUnknown, flowdimension.CategoryOnNetLocalCity, flowdimension.CategoryOnNetCrossCity,
		flowdimension.CategoryOnNetCrossProvince, flowdimension.CategoryOffNetInProvince, flowdimension.CategoryOffNetCrossProvince,
		flowdimension.CategoryOverseas, flowdimension.CategoryInternal, flowdimension.CategoryTransit, flowdimension.CategoryAmbiguous:
		return string(value), true
	default:
		return "", false
	}
}

func dispositionName(value flowdimension.RecordDisposition) (string, bool) {
	switch value {
	case flowdimension.DispositionDrop, flowdimension.DispositionCount:
		return string(value), true
	default:
		return "", false
	}
}

func asnSourceName(value flowworker.ASNSource) (string, bool) {
	switch value {
	case flowworker.ASNSourceUnknown, flowworker.ASNSourceExporter, flowworker.ASNSourceGeoV1, flowworker.ASNSourceGeoV2, flowworker.ASNSourceOverride:
		return string(value), true
	default:
		return "", false
	}
}

func classifyClickHouseError(err error) error {
	var exception *ch.Exception
	if !errors.As(err, &exception) {
		return err
	}
	if exception.IsCode(
		proto.ErrUnknownDatabase, proto.ErrUnknownTable, proto.ErrThereIsNoColumn, proto.ErrNoSuchColumnInTable,
		proto.ErrIncorrectNumberOfColumns, proto.ErrNumberOfColumnsDoesntMatch, proto.ErrTypeMismatch,
		proto.ErrSizesOfColumnsDoesntMatch, proto.ErrSizeOfFixedStringDoesntMatch, proto.ErrUnknownElementOfEnum,
		proto.ErrUnknownUser, proto.ErrWrongPassword, proto.ErrRequiredPassword, proto.ErrIPAddressNotAllowed,
		proto.ErrDatabaseAccessDenied, proto.ErrAccessDenied, proto.ErrAuthenticationFailed,
		proto.ErrUnknownSetting, proto.ErrInvalidSettingValue,
	) {
		return Permanent(err)
	}
	return err
}

func validClickHouseIdentifier(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for index, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || character == '_' || (index > 0 && character >= '0' && character <= '9') {
			continue
		}
		return false
	}
	return true
}
