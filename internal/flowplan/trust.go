package flowplan

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

const (
	TrustBundleSchemaVersion = uint16(1)
	trustBundleMaxBytes      = 1 << 20
)

var (
	ErrTrustBundleRollback = errors.New("collector plan trust bundle generation rollback")
	ErrTrustBundleConflict = errors.New("collector plan trust bundle generation conflict")
	ErrTrustKeyUnavailable = errors.New("collector plan signing key is unavailable")
)

type TrustBundleKey struct {
	KeyID               string `json:"key_id"`
	Algorithm           string `json:"algorithm"`
	PublicKey           string `json:"public_key"`
	Status              string `json:"status"`
	TrustUntilUnixMilli int64  `json:"trust_until_unix_ms,omitempty"`
}

type TrustBundle struct {
	SchemaVersion     uint16           `json:"schema_version"`
	Generation        uint64           `json:"generation"`
	IssuedAtUnixMilli int64            `json:"issued_at_unix_ms"`
	Keys              []TrustBundleKey `json:"keys"`
	RevokedKeyIDs     []string         `json:"revoked_key_ids"`
}

type trustBundleState struct {
	bundle   TrustBundle
	checksum string
	keys     map[string]trustedKey
	revoked  map[string]struct{}
}

type trustedKey struct {
	publicKey           ed25519.PublicKey
	status              string
	trustUntilUnixMilli int64
}

// TrustStore is the collector-side monotonic cache for control-plane signing
// keys. Install accepts an identical generation idempotently, but never lets a
// stale or conflicting bundle replace the current trust root.
type TrustStore struct {
	state atomic.Pointer[trustBundleState]
}

func ParseTrustBundle(data []byte) (TrustBundle, string, error) {
	if len(data) == 0 || len(data) > trustBundleMaxBytes {
		return TrustBundle{}, "", errors.New("collector plan trust bundle size is invalid")
	}
	var bundle TrustBundle
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return TrustBundle{}, "", fmt.Errorf("decode collector plan trust bundle: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return TrustBundle{}, "", errors.New("collector plan trust bundle must contain exactly one JSON document")
	}
	if err := validateTrustBundle(bundle); err != nil {
		return TrustBundle{}, "", err
	}
	canonical, err := json.Marshal(bundle)
	if err != nil {
		return TrustBundle{}, "", err
	}
	if !bytes.Equal(canonical, data) {
		return TrustBundle{}, "", errors.New("collector plan trust bundle is not canonical JSON")
	}
	digest := sha256.Sum256(canonical)
	return bundle, hex.EncodeToString(digest[:]), nil
}

func MarshalTrustBundle(bundle TrustBundle) ([]byte, string, error) {
	if err := validateTrustBundle(bundle); err != nil {
		return nil, "", err
	}
	data, err := json.Marshal(bundle)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(data)
	return data, hex.EncodeToString(digest[:]), nil
}

func validateTrustBundle(bundle TrustBundle) error {
	if bundle.SchemaVersion != TrustBundleSchemaVersion || bundle.Generation == 0 || bundle.IssuedAtUnixMilli <= 0 {
		return errors.New("collector plan trust bundle metadata is invalid")
	}
	seen := make(map[string]struct{}, len(bundle.Keys)+len(bundle.RevokedKeyIDs))
	seenPublicKeys := make(map[string]struct{}, len(bundle.Keys))
	active := 0
	for index, key := range bundle.Keys {
		if !validTrustKeyID(key.KeyID) || key.Algorithm != "ed25519" || (key.Status != "active" && key.Status != "retiring") {
			return fmt.Errorf("collector plan trust bundle key %d is invalid", index)
		}
		if index > 0 && bundle.Keys[index-1].KeyID >= key.KeyID {
			return errors.New("collector plan trust bundle keys are not strictly sorted")
		}
		if _, duplicate := seen[key.KeyID]; duplicate {
			return errors.New("collector plan trust bundle contains duplicate key ids")
		}
		seen[key.KeyID] = struct{}{}
		publicKey, err := base64.StdEncoding.DecodeString(key.PublicKey)
		if err != nil || len(publicKey) != ed25519.PublicKeySize {
			return fmt.Errorf("collector plan trust bundle key %q is not Ed25519", key.KeyID)
		}
		publicKeyFingerprint := string(publicKey)
		if _, duplicate := seenPublicKeys[publicKeyFingerprint]; duplicate {
			return errors.New("collector plan trust bundle reuses public key material")
		}
		seenPublicKeys[publicKeyFingerprint] = struct{}{}
		if key.Status == "active" {
			active++
			if key.TrustUntilUnixMilli != 0 {
				return errors.New("active collector plan trust key cannot have trust_until")
			}
		} else if key.TrustUntilUnixMilli <= bundle.IssuedAtUnixMilli {
			return errors.New("retiring collector plan trust key must outlive the bundle issue time")
		}
	}
	if active > 1 {
		return errors.New("collector plan trust bundle contains multiple active keys")
	}
	for index, keyID := range bundle.RevokedKeyIDs {
		if !validTrustKeyID(keyID) || (index > 0 && bundle.RevokedKeyIDs[index-1] >= keyID) {
			return errors.New("collector plan trust bundle revoked keys are invalid or unsorted")
		}
		if _, duplicate := seen[keyID]; duplicate {
			return errors.New("collector plan trust bundle key is both trusted and revoked")
		}
		seen[keyID] = struct{}{}
	}
	return nil
}

func (s *TrustStore) Install(data []byte) error {
	bundle, checksum, err := ParseTrustBundle(data)
	if err != nil {
		return err
	}
	keys := make(map[string]trustedKey, len(bundle.Keys))
	for _, entry := range bundle.Keys {
		raw, _ := base64.StdEncoding.DecodeString(entry.PublicKey)
		keys[entry.KeyID] = trustedKey{
			publicKey: append(ed25519.PublicKey(nil), raw...), status: entry.Status,
			trustUntilUnixMilli: entry.TrustUntilUnixMilli,
		}
	}
	revoked := make(map[string]struct{}, len(bundle.RevokedKeyIDs))
	for _, keyID := range bundle.RevokedKeyIDs {
		revoked[keyID] = struct{}{}
	}
	next := &trustBundleState{bundle: cloneTrustBundle(bundle), checksum: checksum, keys: keys, revoked: revoked}
	for {
		current := s.state.Load()
		if current != nil {
			if bundle.Generation < current.bundle.Generation {
				return ErrTrustBundleRollback
			}
			if bundle.Generation == current.bundle.Generation {
				if checksum == current.checksum {
					return nil
				}
				return ErrTrustBundleConflict
			}
		}
		if s.state.CompareAndSwap(current, next) {
			return nil
		}
	}
}

func (s *TrustStore) Resolve(keyID string) (ed25519.PublicKey, error) {
	return s.ResolveAt(keyID, time.Now())
}

func (s *TrustStore) ResolveAt(keyID string, now time.Time) (ed25519.PublicKey, error) {
	state := s.state.Load()
	if state == nil || !validTrustKeyID(keyID) || now.IsZero() {
		return nil, ErrTrustKeyUnavailable
	}
	if _, revoked := state.revoked[keyID]; revoked {
		return nil, ErrTrustKeyUnavailable
	}
	key, ok := state.keys[keyID]
	if !ok || len(key.publicKey) != ed25519.PublicKeySize ||
		(key.status == "retiring" && now.UnixMilli() >= key.trustUntilUnixMilli) {
		return nil, ErrTrustKeyUnavailable
	}
	return append(ed25519.PublicKey(nil), key.publicKey...), nil
}

func (s *TrustStore) Generation() uint64 {
	if state := s.state.Load(); state != nil {
		return state.bundle.Generation
	}
	return 0
}

func (s *TrustStore) VerifyControlPlanePlan(envelope []byte, now time.Time) (*Registry, PlanSignatureMetadata, error) {
	var metadata struct {
		SigningKeyID string `json:"signing_key_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(envelope))
	if err := decoder.Decode(&metadata); err != nil {
		return nil, PlanSignatureMetadata{}, fmt.Errorf("decode signed flow plan key id: %w", err)
	}
	key, err := s.ResolveAt(metadata.SigningKeyID, now)
	if err != nil {
		return nil, PlanSignatureMetadata{}, err
	}
	return VerifyControlPlaneSignedPlan(envelope, []byte(base64.StdEncoding.EncodeToString(key)), now)
}

func validTrustKeyID(value string) bool {
	if value == "" || len(value) > 64 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && character != '-' && character != '_' && character != '.' {
			return false
		}
	}
	return true
}

func cloneTrustBundle(bundle TrustBundle) TrustBundle {
	bundle.Keys = append([]TrustBundleKey(nil), bundle.Keys...)
	bundle.RevokedKeyIDs = append([]string(nil), bundle.RevokedKeyIDs...)
	return bundle
}

func SortTrustBundle(bundle *TrustBundle) {
	sort.Slice(bundle.Keys, func(i, j int) bool { return bundle.Keys[i].KeyID < bundle.Keys[j].KeyID })
	sort.Strings(bundle.RevokedKeyIDs)
}
