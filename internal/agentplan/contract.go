// Package agentplan implements the single-domain signed immutable plan
// contract shared by Watchdog agents and the Gin control plane.
package agentplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

const (
	SchemaVersion  = uint16(1)
	MaxPayloadSize = 4 << 20
)

type Spec struct {
	Kind                 string          `json:"kind"`
	APIVersion           string          `json:"api_version"`
	RequiredCapabilities []string        `json:"required_capabilities"`
	Config               json.RawMessage `json:"config"`
}

type Metadata struct {
	PlanID                string `json:"plan_id"`
	AgentID               string `json:"agent_id"`
	PlanVersion           uint64 `json:"plan_version"`
	PlanSchemaVersion     uint16 `json:"plan_schema_version"`
	Kind                  string `json:"kind"`
	APIVersion            string `json:"api_version"`
	PayloadSHA256         string `json:"payload_sha256"`
	SigningKeyID          string `json:"signing_key_id"`
	NotBeforeUnixMilli    int64  `json:"not_before_unix_ms,omitempty"`
	ExpiresAtUnixMilli    int64  `json:"expires_at_unix_ms"`
	SupersedesPlanVersion uint64 `json:"supersedes_plan_version,omitempty"`
}

type Envelope struct {
	SchemaVersion uint16          `json:"schema_version"`
	Metadata      Metadata        `json:"metadata"`
	Payload       json.RawMessage `json:"payload"`
	Signature     string          `json:"signature"`
}

func CanonicalSpec(data []byte) (json.RawMessage, string, Spec, error) {
	if len(data) == 0 || len(data) > MaxPayloadSize {
		return nil, "", Spec{}, errors.New("agent plan payload size is invalid")
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, "", Spec{}, fmt.Errorf("decode agent plan payload: %w", err)
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, "", Spec{}, errors.New("agent plan payload must be a JSON object")
	}
	if err := rejectTrailing(decoder); err != nil {
		return nil, "", Spec{}, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, "", Spec{}, fmt.Errorf("canonicalize agent plan payload: %w", err)
	}
	var spec Spec
	decoder = json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&spec); err != nil {
		return nil, "", Spec{}, fmt.Errorf("decode agent plan contract: %w", err)
	}
	if err := ValidateSpec(spec); err != nil {
		return nil, "", Spec{}, err
	}
	digest := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(digest[:]), spec, nil
}

func ValidateSpec(spec Spec) error {
	if !ValidKind(spec.Kind) || spec.APIVersion != "v1" {
		return errors.New("agent plan kind or API version is unsupported")
	}
	if len(spec.RequiredCapabilities) == 0 || len(spec.RequiredCapabilities) > 128 {
		return errors.New("agent plan required_capabilities must contain 1..128 entries")
	}
	if !slices.IsSorted(spec.RequiredCapabilities) {
		return errors.New("agent plan required_capabilities must be sorted")
	}
	for i, capability := range spec.RequiredCapabilities {
		if !ValidCapability(capability) || (i > 0 && spec.RequiredCapabilities[i-1] == capability) {
			return errors.New("agent plan required_capabilities are invalid or duplicated")
		}
	}
	if len(spec.Config) == 0 || len(spec.Config) > MaxPayloadSize || !json.Valid(spec.Config) || spec.Config[0] != '{' {
		return errors.New("agent plan config must be a JSON object")
	}
	return nil
}

func ValidateCompatibility(spec Spec, agentID, kind, apiVersion string, capabilities []string) error {
	if strings.TrimSpace(agentID) == "" || spec.Kind != kind || spec.APIVersion != apiVersion {
		return errors.New("agent plan identity, kind, or API version is incompatible")
	}
	available := make(map[string]struct{}, len(capabilities))
	for _, capability := range capabilities {
		available[capability] = struct{}{}
	}
	for _, required := range spec.RequiredCapabilities {
		if _, ok := available[required]; !ok {
			return fmt.Errorf("agent plan requires unavailable capability %q", required)
		}
	}
	return nil
}

func ValidKind(value string) bool {
	switch value {
	case "system", "snmp", "flow_collect", "flow_worker", "probe":
		return true
	}
	return false
}

func ValidCapability(value string) bool {
	return value != "" && len(value) <= 128 && strings.TrimSpace(value) == value && strings.Contains(value, "/v")
}

func ValidateMetadata(metadata Metadata) error {
	if !validID(metadata.PlanID) || !validID(metadata.AgentID) || metadata.PlanVersion == 0 || metadata.PlanSchemaVersion != SchemaVersion || !ValidKind(metadata.Kind) || metadata.APIVersion != "v1" {
		return errors.New("agent plan metadata identity or version is invalid")
	}
	if metadata.SigningKeyID == "" || len(metadata.SigningKeyID) > 64 || strings.TrimSpace(metadata.SigningKeyID) != metadata.SigningKeyID {
		return errors.New("agent plan signing key id is invalid")
	}
	if len(metadata.PayloadSHA256) != sha256.Size*2 || metadata.PayloadSHA256 != strings.ToLower(metadata.PayloadSHA256) {
		return errors.New("agent plan payload digest is invalid")
	}
	if decoded, err := hex.DecodeString(metadata.PayloadSHA256); err != nil || len(decoded) != sha256.Size {
		return errors.New("agent plan payload digest is invalid")
	}
	if metadata.ExpiresAtUnixMilli <= 0 || (metadata.NotBeforeUnixMilli != 0 && metadata.NotBeforeUnixMilli >= metadata.ExpiresAtUnixMilli) || metadata.SupersedesPlanVersion >= metadata.PlanVersion {
		return errors.New("agent plan validity or lineage is invalid")
	}
	return nil
}

func SigningPayload(metadata Metadata) ([]byte, error) {
	if err := ValidateMetadata(metadata); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		EnvelopeVersion uint16 `json:"envelope_version"`
		Metadata
	}{EnvelopeVersion: SchemaVersion, Metadata: metadata})
}

func MetadataValidAt(metadata Metadata, now time.Time) error {
	if now.IsZero() {
		return errors.New("agent plan verification time is required")
	}
	millis := now.UTC().UnixMilli()
	if metadata.NotBeforeUnixMilli != 0 && millis < metadata.NotBeforeUnixMilli {
		return errors.New("agent plan is not active yet")
	}
	if millis >= metadata.ExpiresAtUnixMilli {
		return errors.New("agent plan has expired")
	}
	return nil
}

func rejectTrailing(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("agent plan contains multiple JSON values")
	}
	return fmt.Errorf("decode agent plan trailing data: %w", err)
}

func validID(value string) bool {
	return strings.TrimSpace(value) == value && value != "" && len(value) <= 26
}
