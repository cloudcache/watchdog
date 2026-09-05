package flowplan

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const signedPlanMaxBytes = 8 << 20

type signedPlanEnvelope struct {
	SchemaVersion           uint32 `json:"schema_version"`
	Payload                 string `json:"payload"`
	Signature               string `json:"signature"`
	PlanID                  string `json:"plan_id,omitempty"`
	TenantID                string `json:"tenant_id,omitempty"`
	CollectorID             string `json:"collector_id,omitempty"`
	ConfigVersion           uint64 `json:"config_version,omitempty"`
	PlanSchemaVersion       uint16 `json:"plan_schema_version,omitempty"`
	SpecHash                string `json:"spec_hash,omitempty"`
	SigningKeyID            string `json:"signing_key_id,omitempty"`
	NotBeforeUnixMilli      int64  `json:"not_before_unix_ms,omitempty"`
	ExpiresAtUnixMilli      int64  `json:"expires_at_unix_ms,omitempty"`
	SupersedesConfigVersion uint64 `json:"supersedes_config_version,omitempty"`
}

type PlanSignatureMetadata struct {
	PlanID                  string
	TenantID                string
	CollectorID             string
	ConfigVersion           uint64
	PlanSchemaVersion       uint16
	SpecHash                string
	SigningKeyID            string
	NotBeforeUnixMilli      int64
	ExpiresAtUnixMilli      int64
	SupersedesConfigVersion uint64
}

func LoadSignedPlan(planPath, publicKeyPath string, now time.Time) (*Registry, error) {
	planData, err := readBoundedFile(planPath, signedPlanMaxBytes)
	if err != nil {
		return nil, fmt.Errorf("read flow plan: %w", err)
	}
	keyData, err := readBoundedFile(publicKeyPath, 64<<10)
	if err != nil {
		return nil, fmt.Errorf("read flow plan public key: %w", err)
	}
	return VerifySignedPlan(planData, keyData, now)
}

// LoadHistoricalSignedPlan verifies a retained plan without requiring its
// validity window to include wall-clock now. Workers use this only to process
// RawFlow records carrying that exact historical registry version.
func LoadHistoricalSignedPlan(planPath, publicKeyPath string) (*Registry, error) {
	planData, err := readBoundedFile(planPath, signedPlanMaxBytes)
	if err != nil {
		return nil, fmt.Errorf("read historical flow plan: %w", err)
	}
	keyData, err := readBoundedFile(publicKeyPath, 64<<10)
	if err != nil {
		return nil, fmt.Errorf("read flow plan public key: %w", err)
	}
	verified, err := verifySignedPlanPayload(planData, keyData)
	if err != nil {
		return nil, err
	}
	return compileHistoricalPlan(verified.plan)
}

func VerifySignedPlan(envelopeData, publicKeyData []byte, now time.Time) (*Registry, error) {
	if len(envelopeData) == 0 || len(envelopeData) > signedPlanMaxBytes || len(publicKeyData) == 0 || len(publicKeyData) > 64<<10 {
		return nil, errors.New("signed flow plan or public key size is invalid")
	}
	verified, err := verifySignedPlanPayload(envelopeData, publicKeyData)
	if err != nil {
		return nil, err
	}
	return CompilePlan(verified.plan, now)
}

// VerifyControlPlaneSignedPlan verifies a version-2 delivery envelope and
// returns the signed metadata needed for an exact activation acknowledgement.
func VerifyControlPlaneSignedPlan(envelopeData, publicKeyData []byte, now time.Time) (*Registry, PlanSignatureMetadata, error) {
	if len(envelopeData) == 0 || len(envelopeData) > signedPlanMaxBytes || len(publicKeyData) == 0 || len(publicKeyData) > 64<<10 {
		return nil, PlanSignatureMetadata{}, errors.New("signed flow plan or public key size is invalid")
	}
	verified, err := verifySignedPlanPayload(envelopeData, publicKeyData)
	if err != nil {
		return nil, PlanSignatureMetadata{}, err
	}
	if verified.envelopeVersion != 2 {
		return nil, PlanSignatureMetadata{}, errors.New("remote flow plan must use control-plane envelope version 2")
	}
	registry, err := CompilePlan(verified.plan, now)
	if err != nil {
		return nil, PlanSignatureMetadata{}, err
	}
	return registry, verified.metadata, nil
}

type verifiedSignedPlan struct {
	plan            Plan
	payload         []byte
	envelopeVersion uint32
	metadata        PlanSignatureMetadata
}

func verifySignedPlanPayload(envelopeData, publicKeyData []byte) (verifiedSignedPlan, error) {
	if len(envelopeData) == 0 || len(envelopeData) > signedPlanMaxBytes || len(publicKeyData) == 0 || len(publicKeyData) > 64<<10 {
		return verifiedSignedPlan{}, errors.New("signed flow plan or public key size is invalid")
	}
	var envelope signedPlanEnvelope
	decoder := json.NewDecoder(strings.NewReader(string(envelopeData)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return verifiedSignedPlan{}, fmt.Errorf("decode signed flow plan: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return verifiedSignedPlan{}, errors.New("signed flow plan must contain exactly one JSON document")
	}
	if envelope.SchemaVersion != 1 && envelope.SchemaVersion != 2 {
		return verifiedSignedPlan{}, fmt.Errorf("unsupported signed plan envelope version %d", envelope.SchemaVersion)
	}
	if envelope.SchemaVersion == 1 && controlPlaneEnvelopeFieldsPresent(envelope) {
		return verifiedSignedPlan{}, errors.New("legacy signed flow plan cannot carry control-plane metadata")
	}
	payload, err := base64.StdEncoding.DecodeString(envelope.Payload)
	if err != nil {
		return verifiedSignedPlan{}, errors.New("decode signed plan payload")
	}
	signature, err := base64.StdEncoding.DecodeString(envelope.Signature)
	if err != nil {
		return verifiedSignedPlan{}, errors.New("decode signed plan signature")
	}
	publicKey, err := parseEd25519PublicKey(publicKeyData)
	if err != nil {
		return verifiedSignedPlan{}, err
	}
	var plan Plan
	decoder = json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return verifiedSignedPlan{}, fmt.Errorf("decode flow plan payload: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return verifiedSignedPlan{}, errors.New("flow plan payload must contain exactly one JSON document")
	}
	if envelope.SchemaVersion == 1 {
		if !ed25519.Verify(publicKey, payload, signature) {
			return verifiedSignedPlan{}, errors.New("flow plan signature verification failed")
		}
	} else {
		metadata := planEnvelopeMetadata(envelope)
		if err := validateControlPlanePlanPayload(metadata, payload, plan); err != nil {
			return verifiedSignedPlan{}, err
		}
		signingPayload, err := BuildPlanSignaturePayload(metadata)
		if err != nil {
			return verifiedSignedPlan{}, err
		}
		if !ed25519.Verify(publicKey, signingPayload, signature) {
			return verifiedSignedPlan{}, errors.New("flow control-plane plan signature verification failed")
		}
	}
	return verifiedSignedPlan{
		plan: plan, payload: payload, envelopeVersion: envelope.SchemaVersion,
		metadata: planEnvelopeMetadata(envelope),
	}, nil
}

func BuildPlanSignaturePayload(metadata PlanSignatureMetadata) ([]byte, error) {
	if err := validatePlanSignatureMetadata(metadata); err != nil {
		return nil, err
	}
	payload := struct {
		EnvelopeVersion         uint16 `json:"envelope_version"`
		PlanID                  string `json:"plan_id"`
		TenantID                string `json:"tenant_id"`
		CollectorID             string `json:"collector_id"`
		ConfigVersion           uint64 `json:"config_version"`
		PlanSchemaVersion       uint16 `json:"plan_schema_version"`
		SpecHash                string `json:"spec_hash"`
		SigningKeyID            string `json:"signing_key_id"`
		NotBeforeUnixMilli      int64  `json:"not_before_unix_ms"`
		ExpiresAtUnixMilli      int64  `json:"expires_at_unix_ms"`
		SupersedesConfigVersion uint64 `json:"supersedes_config_version"`
	}{
		EnvelopeVersion: 1, PlanID: metadata.PlanID, TenantID: metadata.TenantID,
		CollectorID: metadata.CollectorID, ConfigVersion: metadata.ConfigVersion,
		PlanSchemaVersion: metadata.PlanSchemaVersion, SpecHash: metadata.SpecHash,
		SigningKeyID: metadata.SigningKeyID, NotBeforeUnixMilli: metadata.NotBeforeUnixMilli,
		ExpiresAtUnixMilli: metadata.ExpiresAtUnixMilli, SupersedesConfigVersion: metadata.SupersedesConfigVersion,
	}
	return json.Marshal(payload)
}

func MarshalControlPlaneSignedPlan(metadata PlanSignatureMetadata, planJSON, signature []byte) ([]byte, error) {
	if len(signature) != ed25519.SignatureSize || len(planJSON) == 0 || len(planJSON) > 4<<20 {
		return nil, errors.New("control-plane signed plan payload or signature size is invalid")
	}
	var plan Plan
	decoder := json.NewDecoder(bytes.NewReader(planJSON))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return nil, fmt.Errorf("decode control-plane flow plan: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("control-plane flow plan must contain exactly one JSON document")
	}
	if err := validateControlPlanePlanPayload(metadata, planJSON, plan); err != nil {
		return nil, err
	}
	envelope := signedPlanEnvelope{
		SchemaVersion: 2, Payload: base64.StdEncoding.EncodeToString(planJSON), Signature: base64.StdEncoding.EncodeToString(signature),
		PlanID: metadata.PlanID, TenantID: metadata.TenantID, CollectorID: metadata.CollectorID,
		ConfigVersion: metadata.ConfigVersion, PlanSchemaVersion: metadata.PlanSchemaVersion,
		SpecHash: metadata.SpecHash, SigningKeyID: metadata.SigningKeyID,
		NotBeforeUnixMilli: metadata.NotBeforeUnixMilli, ExpiresAtUnixMilli: metadata.ExpiresAtUnixMilli,
		SupersedesConfigVersion: metadata.SupersedesConfigVersion,
	}
	return json.Marshal(envelope)
}

func planEnvelopeMetadata(envelope signedPlanEnvelope) PlanSignatureMetadata {
	return PlanSignatureMetadata{
		PlanID: envelope.PlanID, TenantID: envelope.TenantID, CollectorID: envelope.CollectorID,
		ConfigVersion: envelope.ConfigVersion, PlanSchemaVersion: envelope.PlanSchemaVersion,
		SpecHash: envelope.SpecHash, SigningKeyID: envelope.SigningKeyID,
		NotBeforeUnixMilli: envelope.NotBeforeUnixMilli, ExpiresAtUnixMilli: envelope.ExpiresAtUnixMilli,
		SupersedesConfigVersion: envelope.SupersedesConfigVersion,
	}
}

func validatePlanSignatureMetadata(metadata PlanSignatureMetadata) error {
	if !validPlanEnvelopeID(metadata.PlanID) || !validPlanEnvelopeID(metadata.TenantID) || !validPlanEnvelopeID(metadata.CollectorID) || metadata.ConfigVersion == 0 || metadata.PlanSchemaVersion == 0 {
		return errors.New("control-plane plan identity and versions are invalid")
	}
	if strings.TrimSpace(metadata.SigningKeyID) != metadata.SigningKeyID || metadata.SigningKeyID == "" || len(metadata.SigningKeyID) > 64 {
		return errors.New("control-plane plan signing key is invalid")
	}
	if len(metadata.SpecHash) != sha256.Size*2 || metadata.SpecHash != strings.ToLower(metadata.SpecHash) {
		return errors.New("control-plane plan spec hash is invalid")
	}
	if decoded, err := hex.DecodeString(metadata.SpecHash); err != nil || len(decoded) != sha256.Size {
		return errors.New("control-plane plan spec hash is invalid")
	}
	if metadata.ExpiresAtUnixMilli <= 0 || (metadata.NotBeforeUnixMilli != 0 && metadata.NotBeforeUnixMilli >= metadata.ExpiresAtUnixMilli) || (metadata.SupersedesConfigVersion != 0 && metadata.SupersedesConfigVersion >= metadata.ConfigVersion) {
		return errors.New("control-plane plan validity or lineage is invalid")
	}
	return nil
}

func validateControlPlanePlanPayload(metadata PlanSignatureMetadata, payload []byte, plan Plan) error {
	if err := validatePlanSignatureMetadata(metadata); err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > 4<<20 {
		return errors.New("control-plane flow plan payload size is invalid")
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != metadata.SpecHash {
		return errors.New("control-plane plan payload hash does not match signed metadata")
	}
	if plan.CollectorID != metadata.CollectorID || plan.Revision != metadata.ConfigVersion || plan.SchemaVersion != uint32(metadata.PlanSchemaVersion) || plan.ExpiresAt.UnixMilli() != metadata.ExpiresAtUnixMilli {
		return errors.New("control-plane plan payload does not match signed identity, version, schema, or expiry")
	}
	if (plan.NotBefore.IsZero() && metadata.NotBeforeUnixMilli != 0) || (!plan.NotBefore.IsZero() && plan.NotBefore.UnixMilli() != metadata.NotBeforeUnixMilli) {
		return errors.New("control-plane plan payload does not match signed not_before")
	}
	return nil
}

func controlPlaneEnvelopeFieldsPresent(envelope signedPlanEnvelope) bool {
	return envelope.PlanID != "" || envelope.TenantID != "" || envelope.CollectorID != "" || envelope.ConfigVersion != 0 || envelope.PlanSchemaVersion != 0 || envelope.SpecHash != "" || envelope.SigningKeyID != "" || envelope.NotBeforeUnixMilli != 0 || envelope.ExpiresAtUnixMilli != 0 || envelope.SupersedesConfigVersion != 0
}

func validPlanEnvelopeID(value string) bool {
	if value == "" || len(value) > 26 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func parseEd25519PublicKey(data []byte) (ed25519.PublicKey, error) {
	trimmed := strings.TrimSpace(string(data))
	if block, _ := pem.Decode([]byte(trimmed)); block != nil {
		value, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse flow plan public key: %w", err)
		}
		key, ok := value.(ed25519.PublicKey)
		if !ok {
			return nil, errors.New("flow plan public key must be Ed25519")
		}
		return key, nil
	}
	raw, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil {
		return nil, errors.New("flow plan public key must be PEM or base64 Ed25519")
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, errors.New("flow plan public key has invalid size")
	}
	return ed25519.PublicKey(raw), nil
}

func readBoundedFile(path string, maxBytes int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || int64(len(data)) > maxBytes {
		return nil, errors.New("file size is invalid")
	}
	return data, nil
}
