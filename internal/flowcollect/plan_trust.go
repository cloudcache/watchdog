package flowcollect

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	planTrustBundleMaxBytes = 256 << 10
	planTrustBundleMaxKeys  = 64
)

var (
	ErrPlanTrustBundleInvalid      = errors.New("flow plan trust bundle is invalid")
	ErrPlanSigningKeyUnknown       = errors.New("flow plan signing key is not trusted")
	ErrPlanSigningKeyRevoked       = errors.New("flow plan signing key is revoked")
	ErrPlanSigningKeyNotAcceptable = errors.New("flow plan signing key is not acceptable")
)

type planTrustUse uint8

const (
	planTrustExisting planTrustUse = iota
	planTrustDelivery
)

type planTrustBundle struct {
	SchemaVersion uint32         `json:"schema_version"`
	Keys          []planTrustKey `json:"keys"`
	byID          map[string]planTrustKey
}

type planTrustKey struct {
	ID                   string `json:"id"`
	PublicKey            string `json:"public_key"`
	Status               string `json:"status"`
	NotBeforeUnixMilli   int64  `json:"not_before_unix_ms"`
	NotAfterUnixMilli    int64  `json:"not_after_unix_ms"`
	AcceptUntilUnixMilli int64  `json:"accept_until_unix_ms,omitempty"`
	parsed               ed25519.PublicKey
}

func parsePlanTrustBundle(data []byte) (*planTrustBundle, error) {
	if len(data) == 0 || len(data) > planTrustBundleMaxBytes {
		return nil, errors.New("flow plan trust bundle size is invalid")
	}
	var bundle planTrustBundle
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return nil, fmt.Errorf("decode flow plan trust bundle: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("flow plan trust bundle must contain exactly one JSON document")
	}
	if bundle.SchemaVersion != 1 || len(bundle.Keys) == 0 || len(bundle.Keys) > planTrustBundleMaxKeys {
		return nil, errors.New("flow plan trust bundle schema or key count is invalid")
	}
	bundle.byID = make(map[string]planTrustKey, len(bundle.Keys))
	for index := range bundle.Keys {
		key := &bundle.Keys[index]
		if !validSigningKeyID(key.ID) {
			return nil, errors.New("flow plan trust bundle key id is invalid")
		}
		if _, exists := bundle.byID[key.ID]; exists {
			return nil, fmt.Errorf("flow plan trust bundle key %q is duplicated", key.ID)
		}
		parsed, err := parseEd25519PublicKey([]byte(key.PublicKey))
		if err != nil {
			return nil, fmt.Errorf("flow plan trust bundle key %q: %w", key.ID, err)
		}
		key.parsed = parsed
		if key.NotAfterUnixMilli <= 0 || (key.NotBeforeUnixMilli != 0 && key.NotBeforeUnixMilli >= key.NotAfterUnixMilli) {
			return nil, fmt.Errorf("flow plan trust bundle key %q validity is invalid", key.ID)
		}
		switch key.Status {
		case "active":
			if key.AcceptUntilUnixMilli != 0 {
				return nil, fmt.Errorf("active flow plan trust key %q cannot have accept_until", key.ID)
			}
		case "retiring":
			if key.AcceptUntilUnixMilli <= 0 || key.AcceptUntilUnixMilli > key.NotAfterUnixMilli || (key.NotBeforeUnixMilli != 0 && key.AcceptUntilUnixMilli < key.NotBeforeUnixMilli) {
				return nil, fmt.Errorf("retiring flow plan trust key %q accept_until is invalid", key.ID)
			}
		case "revoked":
			if key.AcceptUntilUnixMilli != 0 {
				return nil, fmt.Errorf("revoked flow plan trust key %q cannot have accept_until", key.ID)
			}
		default:
			return nil, fmt.Errorf("flow plan trust bundle key %q status is invalid", key.ID)
		}
		bundle.byID[key.ID] = *key
	}
	return &bundle, nil
}

func (b *planTrustBundle) resolve(metadata PlanSignatureMetadata, use planTrustUse, now time.Time) (ed25519.PublicKey, error) {
	if b == nil || now.IsZero() {
		return nil, errors.New("flow plan trust bundle and current time are required")
	}
	key, ok := b.byID[metadata.SigningKeyID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPlanSigningKeyUnknown, metadata.SigningKeyID)
	}
	if key.Status == "revoked" {
		return nil, fmt.Errorf("%w: %s", ErrPlanSigningKeyRevoked, metadata.SigningKeyID)
	}
	if metadata.ExpiresAtUnixMilli > key.NotAfterUnixMilli || (key.NotBeforeUnixMilli != 0 && (metadata.NotBeforeUnixMilli == 0 || metadata.NotBeforeUnixMilli < key.NotBeforeUnixMilli)) {
		return nil, fmt.Errorf("%w: plan validity is outside the key validity", ErrPlanSigningKeyNotAcceptable)
	}
	if key.Status == "retiring" {
		if metadata.NotBeforeUnixMilli == 0 || metadata.NotBeforeUnixMilli > key.AcceptUntilUnixMilli {
			return nil, fmt.Errorf("%w: plan was issued after the key acceptance cutoff", ErrPlanSigningKeyNotAcceptable)
		}
		if use == planTrustDelivery && now.UnixMilli() > key.AcceptUntilUnixMilli {
			return nil, fmt.Errorf("%w: retiring key no longer accepts deliveries", ErrPlanSigningKeyNotAcceptable)
		}
	}
	if use == planTrustDelivery && (now.UnixMilli() < key.NotBeforeUnixMilli || now.UnixMilli() > key.NotAfterUnixMilli) {
		return nil, fmt.Errorf("%w: key is not currently valid for delivery", ErrPlanSigningKeyNotAcceptable)
	}
	return key.parsed, nil
}

func verifySignedPlanPayloadWithTrust(envelopeData, legacyPublicKeyData, trustBundleData []byte, use planTrustUse, now time.Time) (verifiedSignedPlan, error) {
	var header signedPlanEnvelope
	if err := json.Unmarshal(envelopeData, &header); err != nil {
		return verifiedSignedPlan{}, fmt.Errorf("decode signed flow plan header: %w", err)
	}
	if header.SchemaVersion == 1 {
		if len(legacyPublicKeyData) == 0 {
			return verifiedSignedPlan{}, errors.New("legacy flow plan requires plan_public_key_file")
		}
		return verifySignedPlanPayload(envelopeData, legacyPublicKeyData)
	}
	if header.SchemaVersion != 2 {
		return verifiedSignedPlan{}, fmt.Errorf("unsupported signed plan envelope version %d", header.SchemaVersion)
	}
	if len(trustBundleData) == 0 {
		if len(legacyPublicKeyData) == 0 {
			return verifiedSignedPlan{}, errors.New("flow plan trust material is unavailable")
		}
		return verifySignedPlanPayload(envelopeData, legacyPublicKeyData)
	}
	bundle, err := parsePlanTrustBundle(trustBundleData)
	if err != nil {
		return verifiedSignedPlan{}, err
	}
	key, err := bundle.resolve(planEnvelopeMetadata(header), use, now)
	if err != nil {
		return verifiedSignedPlan{}, err
	}
	return verifySignedPlanPayload(envelopeData, []byte(base64.StdEncoding.EncodeToString(key)))
}

func validSigningKeyID(value string) bool {
	if value == "" || len(value) > 64 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '-' && character != '_' && character != '.' && character != ':' {
			return false
		}
	}
	return true
}
