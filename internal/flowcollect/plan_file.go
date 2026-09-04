package flowcollect

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type signedPlanEnvelope struct {
	SchemaVersion uint32 `json:"schema_version"`
	Payload       string `json:"payload"`
	Signature     string `json:"signature"`
}

func LoadSignedPlan(planPath, publicKeyPath string, now time.Time) (*Registry, error) {
	planData, err := os.ReadFile(planPath)
	if err != nil {
		return nil, fmt.Errorf("read flow plan: %w", err)
	}
	keyData, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read flow plan public key: %w", err)
	}
	return VerifySignedPlan(planData, keyData, now)
}

func VerifySignedPlan(envelopeData, publicKeyData []byte, now time.Time) (*Registry, error) {
	verified, err := verifySignedPlanPayload(envelopeData, publicKeyData)
	if err != nil {
		return nil, err
	}
	return CompilePlan(verified.plan, now)
}

type verifiedSignedPlan struct {
	plan    Plan
	payload []byte
}

func verifySignedPlanPayload(envelopeData, publicKeyData []byte) (verifiedSignedPlan, error) {
	var envelope signedPlanEnvelope
	decoder := json.NewDecoder(strings.NewReader(string(envelopeData)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return verifiedSignedPlan{}, fmt.Errorf("decode signed flow plan: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return verifiedSignedPlan{}, errors.New("signed flow plan must contain exactly one JSON document")
	}
	if envelope.SchemaVersion != 1 {
		return verifiedSignedPlan{}, fmt.Errorf("unsupported signed plan envelope version %d", envelope.SchemaVersion)
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
	if !ed25519.Verify(publicKey, payload, signature) {
		return verifiedSignedPlan{}, errors.New("flow plan signature verification failed")
	}
	var plan Plan
	decoder = json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return verifiedSignedPlan{}, fmt.Errorf("decode flow plan payload: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return verifiedSignedPlan{}, errors.New("flow plan payload must contain exactly one JSON document")
	}
	return verifiedSignedPlan{plan: plan, payload: payload}, nil
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
