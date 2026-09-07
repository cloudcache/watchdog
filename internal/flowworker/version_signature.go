package flowworker

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
)

const (
	EnrichmentVersionEnvelopeSchemaVersion = uint16(1)
	EnrichmentVersionSignatureAlgorithm    = "ed25519"
	maxEnrichmentVersionEnvelopeBytes      = 64 << 10
)

// SignedEnrichmentVersionPublication binds the two immutable objects as one
// install unit. Workers resolve SigningKeyID through the same monotonic trust
// bundle used by collector plans; no second trust root is introduced.
type SignedEnrichmentVersionPublication struct {
	SchemaVersion      uint16                       `json:"schema_version"`
	Publication        EnrichmentVersionPublication `json:"publication"`
	SignatureAlgorithm string                       `json:"signature_algorithm"`
	SigningKeyID       string                       `json:"signing_key_id"`
	SignedAtUnixMilli  int64                        `json:"signed_at_unix_ms"`
	Signature          []byte                       `json:"signature"`
}

// EnrichmentVersionSigningPayload is the canonical byte sequence signed by
// the control plane. The signature bytes themselves are deliberately absent.
func EnrichmentVersionSigningPayload(envelope SignedEnrichmentVersionPublication) ([]byte, error) {
	if err := validateSignedEnrichmentVersionMetadata(envelope); err != nil {
		return nil, err
	}
	payload := struct {
		EnvelopeVersion    uint16                       `json:"envelope_version"`
		Publication        EnrichmentVersionPublication `json:"publication"`
		SignatureAlgorithm string                       `json:"signature_algorithm"`
		SigningKeyID       string                       `json:"signing_key_id"`
		SignedAtUnixMilli  int64                        `json:"signed_at_unix_ms"`
	}{
		EnvelopeVersion: envelope.SchemaVersion, Publication: normalizedVersionPublication(envelope.Publication),
		SignatureAlgorithm: envelope.SignatureAlgorithm, SigningKeyID: envelope.SigningKeyID,
		SignedAtUnixMilli: envelope.SignedAtUnixMilli,
	}
	return json.Marshal(payload)
}

func MarshalSignedEnrichmentVersionPublication(envelope SignedEnrichmentVersionPublication) ([]byte, error) {
	if len(envelope.Signature) != ed25519.SignatureSize {
		return nil, errors.New("enrichment version signature size is invalid")
	}
	if _, err := EnrichmentVersionSigningPayload(envelope); err != nil {
		return nil, err
	}
	envelope.Publication = normalizedVersionPublication(envelope.Publication)
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if len(data) > maxEnrichmentVersionEnvelopeBytes {
		return nil, errors.New("signed enrichment version publication is too large")
	}
	return data, nil
}

func VerifySignedEnrichmentVersionPublication(data []byte, trust *flowplan.TrustStore, now time.Time) (SignedEnrichmentVersionPublication, error) {
	if len(data) == 0 || len(data) > maxEnrichmentVersionEnvelopeBytes || trust == nil || now.IsZero() {
		return SignedEnrichmentVersionPublication{}, errors.New("signed enrichment version publication or trust store is invalid")
	}
	var envelope SignedEnrichmentVersionPublication
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return SignedEnrichmentVersionPublication{}, fmt.Errorf("decode signed enrichment version publication: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return SignedEnrichmentVersionPublication{}, errors.New("signed enrichment version publication must contain exactly one JSON document")
	}
	canonical, err := MarshalSignedEnrichmentVersionPublication(envelope)
	if err != nil {
		return SignedEnrichmentVersionPublication{}, err
	}
	if !bytes.Equal(canonical, data) {
		return SignedEnrichmentVersionPublication{}, errors.New("signed enrichment version publication is not canonical JSON")
	}
	key, err := trust.ResolveAt(envelope.SigningKeyID, now)
	if err != nil {
		return SignedEnrichmentVersionPublication{}, err
	}
	payload, err := EnrichmentVersionSigningPayload(envelope)
	if err != nil {
		return SignedEnrichmentVersionPublication{}, err
	}
	if !ed25519.Verify(key, payload, envelope.Signature) {
		return SignedEnrichmentVersionPublication{}, errors.New("enrichment version publication signature verification failed")
	}
	return envelope, nil
}

func validateSignedEnrichmentVersionMetadata(envelope SignedEnrichmentVersionPublication) error {
	if envelope.SchemaVersion != EnrichmentVersionEnvelopeSchemaVersion || envelope.SignatureAlgorithm != EnrichmentVersionSignatureAlgorithm ||
		envelope.SigningKeyID == "" || len(envelope.SigningKeyID) > 64 || strings.TrimSpace(envelope.SigningKeyID) != envelope.SigningKeyID ||
		envelope.SignedAtUnixMilli <= 0 {
		return errors.New("signed enrichment version metadata is invalid")
	}
	if err := validateVersionPublication(envelope.Publication); err != nil {
		return err
	}
	return nil
}

func normalizedVersionPublication(publication EnrichmentVersionPublication) EnrichmentVersionPublication {
	publication.DimensionEffectiveFrom = publication.DimensionEffectiveFrom.UTC()
	publication.ClassificationEffectiveFrom = publication.ClassificationEffectiveFrom.UTC()
	return publication
}
