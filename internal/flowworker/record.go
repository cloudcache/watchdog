package flowworker

// RecordBatch is the in-memory handoff between RawFlow decode and enrichment.
// It is not a Kafka schema and is never persisted independently of RawFlow.
type RecordBatch struct {
	BatchSchemaVersion  uint32
	SourceID            []byte
	KafkaTopic          string
	KafkaPartition      int32
	KafkaOffset         int64
	TenantID            string
	CollectorID         string
	ExporterID          string
	RegistryVersion     uint64
	ReceivedAtUnixMS    int64
	Protocol            uint32
	SourceIP            []byte
	ObservationDomainID uint64
	Records             []*Record
	SubAgentID          uint32
	DatagramSequence    uint32
	AgentIP             []byte
	ExporterEpoch       uint64
}

type Record struct {
	RecordIndex          uint32
	EventTimeUnixMS      int64
	TargetID             string
	DeviceID             string
	ObservationIfIndex   uint32
	ObservationDirection uint32
	InIf                 uint32
	OutIf                uint32
	SourceIP             []byte
	DestinationIP        []byte
	SourcePort           uint32
	DestinationPort      uint32
	IPProtocol           uint32
	TCPFlags             uint32
	RawBytes             uint64
	RawPackets           uint64
	SamplingMode         uint32
	SamplingRate         uint64
	SamplingSource       uint32
	EstimatedValid       bool
	EstimatedBytes       uint64
	EstimatedPackets     uint64
	FlowDurationMS       uint64
	QualityFlags         uint64
	SourceASN            uint32
	DestinationASN       uint32
	SourceIDType         uint32
	SourceIDValue        uint32
	SampleSequence       uint32
	SamplePool           uint64
	ExporterDrops        uint64
	SampleIndex          uint32
	QualityEpoch         uint64
}
