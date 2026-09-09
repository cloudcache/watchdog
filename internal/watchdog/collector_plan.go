package watchdog

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
)

const collectorPlanMaxSpecBytes = 4 << 20

type CollectorPlanStatus string

const (
	CollectorPlanValidated CollectorPlanStatus = "validated"
	CollectorPlanActive    CollectorPlanStatus = "active"
	CollectorPlanRetired   CollectorPlanStatus = "retired"
)

type CollectorPlanRevision struct {
	ID                      ID
	TenantID                ID
	CollectorID             ID
	ConfigVersion           uint64
	PlanSchemaVersion       uint16
	Status                  CollectorPlanStatus
	SpecJSON                json.RawMessage
	SpecHash                string
	SigningKeyID            string
	Signature               []byte
	ValidationJSON          json.RawMessage
	NotBefore               time.Time
	ExpiresAt               time.Time
	SupersedesConfigVersion uint64
	CreatedBy               ID
	UpdatedBy               ID
	RowVersion              uint64
	CreatedAt               time.Time
	UpdatedAt               time.Time
	ActivatedAt             time.Time
	RetiredAt               time.Time
	verifiedEnvelopeSHA256  [sha256.Size]byte
}

type CollectorPlanActivation struct {
	TenantID                    ID
	CollectorID                 ID
	ConfigVersion               uint64
	ExpectedCollectorRowVersion uint64
	ExpectedPlanRowVersion      uint64
	ActorID                     ID
}

type CollectorPlanAcknowledgement struct {
	TenantID        ID
	CollectorID     ID
	ConfigVersion   uint64
	SpecHash        string
	BootID          string
	SoftwareVersion string
}

func CanonicalCollectorPlanJSON(spec []byte) (json.RawMessage, string, error) {
	if len(spec) == 0 || len(spec) > collectorPlanMaxSpecBytes {
		return nil, "", errors.New("collector plan spec size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(spec))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, "", fmt.Errorf("decode collector plan spec: %w", err)
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, "", errors.New("collector plan spec must be a JSON object")
	}
	if err := rejectTrailingCollectorPlanJSON(decoder); err != nil {
		return nil, "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, "", fmt.Errorf("canonicalize collector plan spec: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(digest[:]), nil
}

func rejectTrailingCollectorPlanJSON(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("collector plan spec contains multiple JSON values")
	}
	return fmt.Errorf("decode collector plan trailing data: %w", err)
}

func ValidateNewCollectorPlanRevision(plan CollectorPlanRevision) error {
	payload, err := CollectorPlanSigningPayload(plan)
	if err != nil {
		return err
	}
	if len(plan.Signature) != ed25519.SignatureSize {
		return errors.New("validated collector plan Ed25519 signature is required")
	}
	if verifiedCollectorPlanEnvelopeDigest(payload, plan.Signature) != plan.verifiedEnvelopeSHA256 {
		return errors.New("collector plan signature was not cryptographically verified")
	}
	if len(plan.ValidationJSON) > collectorPlanMaxSpecBytes || (len(plan.ValidationJSON) != 0 && !json.Valid(plan.ValidationJSON)) {
		return errors.New("collector plan validation result is invalid")
	}
	return nil
}

func VerifyCollectorPlanRevisionSignature(plan CollectorPlanRevision, publicKey ed25519.PublicKey) (CollectorPlanRevision, error) {
	payload, err := CollectorPlanSigningPayload(plan)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	if len(publicKey) != ed25519.PublicKeySize || len(plan.Signature) != ed25519.SignatureSize || !ed25519.Verify(publicKey, payload, plan.Signature) {
		return CollectorPlanRevision{}, errors.New("collector plan signature verification failed")
	}
	plan.verifiedEnvelopeSHA256 = verifiedCollectorPlanEnvelopeDigest(payload, plan.Signature)
	return plan, nil
}

func verifiedCollectorPlanEnvelopeDigest(payload, signature []byte) [sha256.Size]byte {
	digest := sha256.New()
	_, _ = digest.Write(payload)
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(signature)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func CollectorPlanSigningPayload(plan CollectorPlanRevision) ([]byte, error) {
	if plan.ID == "" || len(plan.ID) > 26 || plan.TenantID == "" || len(plan.TenantID) > 26 || plan.CollectorID == "" || len(plan.CollectorID) > 26 || plan.CreatedBy == "" || len(plan.CreatedBy) > 26 || plan.ConfigVersion == 0 || plan.PlanSchemaVersion == 0 {
		return nil, errors.New("collector plan identity, tenant, collector, actor, and versions are required")
	}
	if plan.Status != CollectorPlanValidated {
		return nil, errors.New("new collector plan revision must be validated")
	}
	if strings.TrimSpace(plan.SigningKeyID) == "" || len(plan.SigningKeyID) > 64 {
		return nil, errors.New("validated collector plan signing key is required")
	}
	if plan.ExpiresAt.IsZero() || (!plan.NotBefore.IsZero() && !plan.NotBefore.Before(plan.ExpiresAt)) {
		return nil, errors.New("collector plan validity interval is invalid")
	}
	if plan.ExpiresAt.Nanosecond()%int(time.Millisecond) != 0 || (!plan.NotBefore.IsZero() && plan.NotBefore.Nanosecond()%int(time.Millisecond) != 0) {
		return nil, errors.New("collector plan validity timestamps must use millisecond precision")
	}
	if plan.SupersedesConfigVersion >= plan.ConfigVersion && plan.SupersedesConfigVersion != 0 {
		return nil, errors.New("collector plan superseded version must be lower than config version")
	}
	canonical, hash, err := CanonicalCollectorPlanJSON(plan.SpecJSON)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, plan.SpecJSON) {
		return nil, errors.New("collector plan spec is not canonical JSON")
	}
	if !validSHA256Hex(plan.SpecHash) || plan.SpecHash != hash {
		return nil, errors.New("collector plan spec hash does not match canonical JSON")
	}
	metadata := flowplan.PlanSignatureMetadata{
		PlanID: string(plan.ID), CollectorID: string(plan.CollectorID),
		ConfigVersion: plan.ConfigVersion, PlanSchemaVersion: plan.PlanSchemaVersion,
		SpecHash: plan.SpecHash, SigningKeyID: plan.SigningKeyID,
		ExpiresAtUnixMilli: plan.ExpiresAt.UnixMilli(), SupersedesConfigVersion: plan.SupersedesConfigVersion,
	}
	if !plan.NotBefore.IsZero() {
		metadata.NotBeforeUnixMilli = plan.NotBefore.UnixMilli()
	}
	encoded, err := flowplan.BuildPlanSignaturePayload(metadata)
	if err != nil {
		return nil, fmt.Errorf("encode collector plan signing payload: %w", err)
	}
	return encoded, nil
}
