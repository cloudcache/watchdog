package watchdog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"time"
)

const (
	CollectorHeartbeatEnvelopeSchemaVersion = 1
	CollectorRuntimeHeartbeatSchemaVersion  = 1
	collectorRuntimeJSONMaxBytes            = 8 << 10
	collectorRuntimeValueMax                = uint64(1 << 60)
)

var (
	ErrCollectorHeartbeatConflict = errors.New("collector heartbeat is stale or conflicts with stored runtime state")
	ErrCollectorHeartbeatFenced   = errors.New("collector heartbeat process incarnation is fenced")
	collectorRuntimeProtocols     = []string{"ipfix", "netflow5", "netflow9", "sflow5"}
)

// CollectorRuntimeCapabilities is deliberately limited to stable process
// capabilities. Queue depths, health and counters belong to Observation so a
// changing runtime value never creates false capability drift.
type CollectorRuntimeCapabilities struct {
	SchemaVersion        uint16   `json:"schema_version"`
	Protocols            []string `json:"protocols"`
	PlanEnvelopeVersions []uint16 `json:"plan_envelope_versions"`
}

type CollectorQueueObservation struct {
	Depth    uint64 `json:"depth"`
	Capacity uint64 `json:"capacity"`
}

type CollectorRuntimeQueues struct {
	Kafka CollectorQueueObservation `json:"kafka"`
}

type CollectorRuntimeCounters struct {
	ReceivedDatagrams uint64 `json:"received_datagrams"`
	RejectedSources   uint64 `json:"rejected_sources"`
	InvalidDatagrams  uint64 `json:"invalid_datagrams"`
	UDPKernelDrops    uint64 `json:"udp_kernel_drops"`
	KafkaRecords      uint64 `json:"kafka_records"`
	KafkaBytes        uint64 `json:"kafka_bytes"`
	PublishFailures   uint64 `json:"publish_failures"`
}

type CollectorRuntimeObservation struct {
	Running             bool                     `json:"running"`
	UptimeSeconds       uint64                   `json:"uptime_seconds"`
	PlanAccepting       bool                     `json:"plan_accepting"`
	PlanUsingLKG        bool                     `json:"plan_using_lkg"`
	ControlPlaneHealthy bool                     `json:"control_plane_healthy"`
	KafkaHealthy        bool                     `json:"kafka_healthy"`
	Queues              CollectorRuntimeQueues   `json:"queues"`
	Counters            CollectorRuntimeCounters `json:"counters"`
}

type CollectorRuntimeHeartbeatReport struct {
	SchemaVersion       uint16                       `json:"schema_version"`
	Sequence            uint64                       `json:"sequence"`
	SentAtUnixMilli     int64                        `json:"sent_at_unix_ms"`
	BootID              string                       `json:"boot_id"`
	SoftwareVersion     string                       `json:"software_version"`
	AgentAPIVersion     uint16                       `json:"agent_api_version"`
	PlanSchemaMin       uint16                       `json:"plan_schema_min"`
	PlanSchemaMax       uint16                       `json:"plan_schema_max"`
	ActiveConfigVersion uint64                       `json:"active_config_version"`
	ActiveSpecHash      string                       `json:"active_spec_hash,omitempty"`
	Capabilities        CollectorRuntimeCapabilities `json:"capabilities"`
	Observation         CollectorRuntimeObservation  `json:"observation"`
}

type CollectorRuntimeHeartbeat struct {
	TenantID    ID
	CollectorID ID
	CollectorRuntimeHeartbeatReport
	ReceivedAt              time.Time
	ClockOffsetMilliseconds int64
	CapabilitiesJSON        json.RawMessage
	CapabilitiesHash        string
	ObservationJSON         json.RawMessage
	ObservationHash         string
	PayloadHash             string
}

func prepareCollectorRuntimeHeartbeat(identity CollectorMachineIdentity, report CollectorRuntimeHeartbeatReport, receivedAt time.Time) (CollectorRuntimeHeartbeat, error) {
	if identity.TenantID == "" || identity.CollectorID == "" {
		return CollectorRuntimeHeartbeat{}, errors.New("collector heartbeat identity is required")
	}
	if err := validateCollectorRuntimeHeartbeatReport(report); err != nil {
		return CollectorRuntimeHeartbeat{}, err
	}
	capabilitiesJSON, capabilitiesHash, err := marshalCollectorRuntimeValue(report.Capabilities)
	if err != nil {
		return CollectorRuntimeHeartbeat{}, err
	}
	observationJSON, observationHash, err := marshalCollectorRuntimeValue(report.Observation)
	if err != nil {
		return CollectorRuntimeHeartbeat{}, err
	}
	_, payloadHash, err := marshalCollectorRuntimeValue(report)
	if err != nil {
		return CollectorRuntimeHeartbeat{}, err
	}
	receivedAt = receivedAt.UTC().Truncate(time.Millisecond)
	return CollectorRuntimeHeartbeat{
		TenantID: identity.TenantID, CollectorID: identity.CollectorID,
		CollectorRuntimeHeartbeatReport: report,
		ReceivedAt:                      receivedAt, ClockOffsetMilliseconds: receivedAt.UnixMilli() - report.SentAtUnixMilli,
		CapabilitiesJSON: capabilitiesJSON, CapabilitiesHash: capabilitiesHash,
		ObservationJSON: observationJSON, ObservationHash: observationHash, PayloadHash: payloadHash,
	}, nil
}

func validateCollectorRuntimeHeartbeatReport(report CollectorRuntimeHeartbeatReport) error {
	if report.SchemaVersion != CollectorRuntimeHeartbeatSchemaVersion || report.Sequence == 0 ||
		report.SentAtUnixMilli <= 0 || report.SentAtUnixMilli > 253402300799999 ||
		report.BootID == "" || len(report.BootID) > 64 || !isPrintableASCII(report.BootID) ||
		report.SoftwareVersion == "" || len(report.SoftwareVersion) > 64 || !isPrintableASCII(report.SoftwareVersion) ||
		report.AgentAPIVersion == 0 || report.PlanSchemaMin == 0 || report.PlanSchemaMax < report.PlanSchemaMin ||
		report.ActiveConfigVersion == 0 || (report.ActiveSpecHash != "" && !validSHA256Hex(report.ActiveSpecHash)) {
		return errors.New("collector runtime heartbeat header is invalid")
	}
	if err := validateCollectorRuntimeCapabilities(report.Capabilities); err != nil {
		return err
	}
	return validateCollectorRuntimeObservation(report.Observation)
}

func validateCollectorRuntimeCapabilities(capabilities CollectorRuntimeCapabilities) error {
	if capabilities.SchemaVersion != 1 || len(capabilities.Protocols) == 0 || len(capabilities.PlanEnvelopeVersions) == 0 ||
		!slices.IsSorted(capabilities.Protocols) || !slices.IsSorted(capabilities.PlanEnvelopeVersions) {
		return errors.New("collector runtime capabilities are unsupported or not canonical")
	}
	for index, protocol := range capabilities.Protocols {
		if !slices.Contains(collectorRuntimeProtocols, protocol) || (index > 0 && protocol == capabilities.Protocols[index-1]) {
			return errors.New("collector runtime capabilities are unsupported or not canonical")
		}
	}
	hasEnvelopeV2 := false
	for index, version := range capabilities.PlanEnvelopeVersions {
		if version == 0 || version > 2 || (index > 0 && version == capabilities.PlanEnvelopeVersions[index-1]) {
			return errors.New("collector runtime capabilities are unsupported or not canonical")
		}
		hasEnvelopeV2 = hasEnvelopeV2 || version == 2
	}
	if !hasEnvelopeV2 {
		return errors.New("collector runtime capabilities are unsupported or not canonical")
	}
	return nil
}

func validateCollectorRuntimeObservation(observation CollectorRuntimeObservation) error {
	queue := observation.Queues.Kafka
	if queue.Depth > queue.Capacity || queue.Capacity > collectorRuntimeValueMax {
		return errors.New("collector runtime Kafka queue observation is invalid")
	}
	if observation.UptimeSeconds > collectorRuntimeValueMax {
		return errors.New("collector runtime uptime observation is invalid")
	}
	counters := observation.Counters
	for _, value := range []uint64{
		counters.ReceivedDatagrams, counters.RejectedSources, counters.InvalidDatagrams,
		counters.UDPKernelDrops, counters.KafkaRecords, counters.KafkaBytes, counters.PublishFailures,
	} {
		if value > collectorRuntimeValueMax {
			return errors.New("collector runtime counter observation is invalid")
		}
	}
	return nil
}

func marshalCollectorRuntimeValue(value any) (json.RawMessage, string, error) {
	payload, err := json.Marshal(value)
	if err != nil || len(payload) == 0 || len(payload) > collectorRuntimeJSONMaxBytes {
		return nil, "", errors.New("collector runtime heartbeat payload is invalid")
	}
	digest := sha256.Sum256(payload)
	return payload, hex.EncodeToString(digest[:]), nil
}

func validateCollectorHeartbeatSequence(previousBootID string, previousSequence uint64, previousPayloadHash, bootID string, sequence uint64, payloadHash string) (bool, error) {
	if previousBootID == bootID {
		if sequence < previousSequence || (sequence == previousSequence && payloadHash != previousPayloadHash) {
			return false, ErrCollectorHeartbeatFenced
		}
		return sequence == previousSequence, nil
	}
	if sequence != 1 {
		return false, ErrCollectorHeartbeatFenced
	}
	return false, nil
}

func collectorRuntimeCountersMonotonic(previous, current CollectorRuntimeCounters) bool {
	return current.ReceivedDatagrams >= previous.ReceivedDatagrams &&
		current.RejectedSources >= previous.RejectedSources &&
		current.InvalidDatagrams >= previous.InvalidDatagrams &&
		current.UDPKernelDrops >= previous.UDPKernelDrops &&
		current.KafkaRecords >= previous.KafkaRecords &&
		current.KafkaBytes >= previous.KafkaBytes &&
		current.PublishFailures >= previous.PublishFailures
}
