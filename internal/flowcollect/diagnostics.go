package flowcollect

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"net/netip"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/cloudcache/watchdog/internal/flowcollect/flowpb"
)

const (
	diagnosticSchemaVersion = 1
	decodeRejectedCode      = "FLOW_DECODE_REJECTED"
	normalizeRejectedCode   = "FLOW_NORMALIZE_REJECTED"
	unknownSourceCode       = "FLOW_EXPORTER_UNKNOWN"
	maxErrorSummaryRunes    = 512
)

type permanentProcessingError struct {
	code string
	err  error
}

func (e *permanentProcessingError) Error() string { return e.code + ": " + e.err.Error() }
func (e *permanentProcessingError) Unwrap() error { return e.err }

func permanentProcessingFailure(code string, err error) error {
	return &permanentProcessingError{code: code, err: err}
}

func permanentFailureDetails(err error) (string, error, bool) {
	var failure *permanentProcessingError
	if errors.As(err, &failure) {
		return failure.code, failure.err, true
	}
	return "", nil, false
}

func BuildDecodeFailure(record WALRecord, collectorID, code string, cause error, attempts uint32, payloadMaxBytes int) *flowpb.DecodeFailure {
	payloadDigest := sha256.Sum256(record.Payload)
	eventID := diagnosticEventID(record.DatagramID[:], []byte(code))
	payloadSize := len(record.Payload)
	if payloadMaxBytes < 0 {
		payloadMaxBytes = 0
	}
	if payloadSize > payloadMaxBytes {
		payloadSize = payloadMaxBytes
	}
	payload := bytes.Clone(record.Payload[:payloadSize])
	return &flowpb.DecodeFailure{
		FailureSchemaVersion: diagnosticSchemaVersion,
		EventId:              eventID[:],
		DatagramId:           bytes.Clone(record.DatagramID[:]),
		TenantId:             record.TenantID,
		CollectorId:          collectorID,
		ExporterId:           record.ExporterID,
		RegistryVersion:      record.RegistryVersion,
		ReceivedAtUnixMs:     record.ReceivedAt.UnixMilli(),
		Protocol:             uint32(record.Protocol),
		SourceIp:             address16(record.Source.Addr()),
		ObservationDomainId:  record.ObservationDomainID,
		ErrorCode:            code,
		ErrorSummary:         sanitizeErrorSummary(cause),
		Attempts:             attempts,
		PayloadSha256:        payloadDigest[:],
		Payload:              payload,
		PayloadTruncated:     payloadSize < len(record.Payload),
	}
}

func BuildQuarantineEvent(datagram Datagram, collectorID string, protocol Protocol, domain uint64) *flowpb.QuarantineEvent {
	payloadDigest := sha256.Sum256(datagram.Payload)
	var received [8]byte
	binary.BigEndian.PutUint64(received[:], uint64(datagram.ReceivedAt.UnixNano()))
	var domainBytes [8]byte
	binary.BigEndian.PutUint64(domainBytes[:], domain)
	eventID := diagnosticEventID([]byte(collectorID), address16(datagram.Source.Addr()), []byte{byte(protocol)}, domainBytes[:], received[:], payloadDigest[:])
	return &flowpb.QuarantineEvent{
		EventSchemaVersion:  diagnosticSchemaVersion,
		EventId:             eventID[:],
		CollectorId:         collectorID,
		ReceivedAtUnixMs:    datagram.ReceivedAt.UnixMilli(),
		Protocol:            uint32(protocol),
		SourceIp:            address16(datagram.Source.Addr()),
		ObservationDomainId: domain,
		ReasonCode:          unknownSourceCode,
		PayloadBytes:        uint32(len(datagram.Payload)),
		PayloadSha256:       payloadDigest[:],
	}
}

func diagnosticEventID(parts ...[]byte) [sha256.Size]byte {
	hash := sha256.New()
	for _, part := range parts {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(part)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(part)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func sanitizeErrorSummary(err error) string {
	if err == nil {
		return ""
	}
	value := strings.Map(func(value rune) rune {
		if unicode.IsControl(value) {
			return ' '
		}
		return value
	}, err.Error())
	value = strings.Join(strings.Fields(value), " ")
	runes := []rune(value)
	if len(runes) > maxErrorSummaryRunes {
		return string(runes[:maxErrorSummaryRunes])
	}
	return value
}

type quarantineLimiter struct {
	mu           sync.Mutex
	second       int64
	total        int
	bySource     map[[16]byte]int
	globalMax    int
	perSourceMax int
}

func newQuarantineLimiter(globalMax, perSourceMax int) *quarantineLimiter {
	return &quarantineLimiter{globalMax: globalMax, perSourceMax: perSourceMax, bySource: make(map[[16]byte]int)}
}

func (l *quarantineLimiter) Allow(source netip.Addr, now time.Time) bool {
	if l == nil || !source.IsValid() || l.globalMax <= 0 || l.perSourceMax <= 0 {
		return false
	}
	key := source.As16()
	second := now.Unix()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.second != second {
		l.second = second
		l.total = 0
		clear(l.bySource)
	}
	if l.total >= l.globalMax || l.bySource[key] >= l.perSourceMax {
		return false
	}
	l.total++
	l.bySource[key]++
	return true
}

func retryBackoff(config DiagnosticsConfig, failedAttempt uint32) time.Duration {
	delay := config.RetryInitial
	for count := uint32(1); count < failedAttempt && delay < config.RetryMax; count++ {
		if delay > config.RetryMax/2 {
			return config.RetryMax
		}
		delay *= 2
	}
	if delay > config.RetryMax {
		return config.RetryMax
	}
	return delay
}
