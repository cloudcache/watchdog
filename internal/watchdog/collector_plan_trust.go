package watchdog

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

var (
	ErrCollectorPlanSigningKeyUnavailable  = errors.New("collector plan signing key is unavailable")
	ErrCollectorPlanSigningKeyReuse        = errors.New("collector plan signing key id or material cannot be reused")
	ErrCollectorPlanTrustBundleUnavailable = errors.New("collector plan trust bundle is unavailable")
)

type CollectorPlanSigningKeyStatus string

const (
	CollectorPlanSigningKeyActive   CollectorPlanSigningKeyStatus = "active"
	CollectorPlanSigningKeyRetiring CollectorPlanSigningKeyStatus = "retiring"
	CollectorPlanSigningKeyRevoked  CollectorPlanSigningKeyStatus = "revoked"
)

type CollectorPlanSigningKey struct {
	KeyID            string
	Algorithm        string
	PublicKey        ed25519.PublicKey
	PublicKeySHA256  string
	Status           CollectorPlanSigningKeyStatus
	ActivatedAt      time.Time
	RetiringAt       time.Time
	TrustUntil       time.Time
	RevokedAt        time.Time
	RevocationReason string
	RowVersion       uint64
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type CollectorPlanTrustBundlePublication struct {
	Generation uint64
	BundleJSON []byte
	Checksum   string
	IssuedAt   time.Time
	RowVersion uint64
}

type CollectorPlanTrustRepository interface {
	ActivateCollectorPlanSigningKey(context.Context, string, ed25519.PublicKey, time.Time) (CollectorPlanTrustBundlePublication, error)
	GetCollectorPlanSigningKey(context.Context, string) (CollectorPlanSigningKey, error)
	GetCollectorPlanTrustBundle(context.Context) (CollectorPlanTrustBundlePublication, error)
	RevokeCollectorPlanSigningKey(context.Context, string, string, time.Time) (CollectorPlanTrustBundlePublication, error)
	ExpireRetiringCollectorPlanSigningKeys(context.Context, time.Time) (CollectorPlanTrustBundlePublication, bool, error)
}

// CollectorPlanSigner is the only private-key capability exposed to plan
// creation. Implementations never return or serialize private key bytes.
type CollectorPlanSigner interface {
	KeyID() string
	PublicKey() ed25519.PublicKey
	Sign(context.Context, CollectorPlanRevision) (CollectorPlanRevision, error)
}

// ControlPlanePayloadSigner reuses the platform signing key for small,
// canonical control-plane payloads such as an enrichment version pair. It does
// not expose private key bytes or create a second trust lifecycle.
type ControlPlanePayloadSigner interface {
	KeyID() string
	PublicKey() ed25519.PublicKey
	SignControlPlanePayload(context.Context, []byte) ([]byte, error)
}

// collectorPlanTransactionSigner lets the MySQL repository sign only after it
// has locked and revalidated the registered key in the same transaction. It is
// intentionally package-private so HTTP adapters and external implementations
// cannot bypass CollectorPlanSigner.Sign's trust-state check.
type collectorPlanTransactionSigner interface {
	CollectorPlanSigner
	signVerified(CollectorPlanRevision) (CollectorPlanRevision, error)
}

type controlPlanePayloadTransactionSigner interface {
	ControlPlanePayloadSigner
	signControlPlanePayloadVerified([]byte) ([]byte, error)
}

type ed25519CollectorPlanSigner struct {
	keyID      string
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
	trust      CollectorPlanTrustRepository
}

func LoadCollectorPlanSigner(cfg CollectorPlanSigningConfig, trust CollectorPlanTrustRepository) (CollectorPlanSigner, error) {
	if trust == nil || !validCollectorPlanSigningKeyID(cfg.KeyID) || strings.TrimSpace(cfg.PrivateKeyFile) == "" {
		return nil, errors.New("collector plan signing key id, private key file, and trust repository are required")
	}
	privateKey, err := readCollectorPlanPrivateKey(cfg.PrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load collector plan private signing key: %w", err)
	}
	publicKey := append(ed25519.PublicKey(nil), privateKey.Public().(ed25519.PublicKey)...)
	return &ed25519CollectorPlanSigner{
		keyID: cfg.KeyID, privateKey: privateKey, publicKey: publicKey, trust: trust,
	}, nil
}

func (s *ed25519CollectorPlanSigner) KeyID() string { return s.keyID }

func (s *ed25519CollectorPlanSigner) PublicKey() ed25519.PublicKey {
	return append(ed25519.PublicKey(nil), s.publicKey...)
}

func (s *ed25519CollectorPlanSigner) Sign(ctx context.Context, plan CollectorPlanRevision) (CollectorPlanRevision, error) {
	if s == nil || ctx == nil || len(s.privateKey) != ed25519.PrivateKeySize || len(s.publicKey) != ed25519.PublicKeySize {
		return CollectorPlanRevision{}, ErrCollectorPlanSigningKeyUnavailable
	}
	registered, err := s.trust.GetCollectorPlanSigningKey(ctx, s.keyID)
	if err != nil {
		return CollectorPlanRevision{}, fmt.Errorf("read collector plan signing key state: %w", err)
	}
	if registered.Status != CollectorPlanSigningKeyActive || !bytes.Equal(registered.PublicKey, s.publicKey) {
		return CollectorPlanRevision{}, ErrCollectorPlanSigningKeyUnavailable
	}
	return s.signVerified(plan)
}

func (s *ed25519CollectorPlanSigner) signVerified(plan CollectorPlanRevision) (CollectorPlanRevision, error) {
	if s == nil || len(s.privateKey) != ed25519.PrivateKeySize || len(s.publicKey) != ed25519.PublicKeySize {
		return CollectorPlanRevision{}, ErrCollectorPlanSigningKeyUnavailable
	}
	plan.SigningKeyID = s.keyID
	plan.Signature = nil
	plan.verifiedEnvelopeSHA256 = [32]byte{}
	payload, err := CollectorPlanSigningPayload(plan)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	plan.Signature = ed25519.Sign(s.privateKey, payload)
	verified, err := VerifyCollectorPlanRevisionSignature(plan, s.publicKey)
	if err != nil {
		return CollectorPlanRevision{}, err
	}
	return verified, nil
}

func (s *ed25519CollectorPlanSigner) SignControlPlanePayload(ctx context.Context, payload []byte) ([]byte, error) {
	if s == nil || ctx == nil || len(s.privateKey) != ed25519.PrivateKeySize || len(s.publicKey) != ed25519.PublicKeySize {
		return nil, ErrCollectorPlanSigningKeyUnavailable
	}
	registered, err := s.trust.GetCollectorPlanSigningKey(ctx, s.keyID)
	if err != nil {
		return nil, fmt.Errorf("read control-plane signing key state: %w", err)
	}
	if registered.Status != CollectorPlanSigningKeyActive || !bytes.Equal(registered.PublicKey, s.publicKey) {
		return nil, ErrCollectorPlanSigningKeyUnavailable
	}
	return s.signControlPlanePayloadVerified(payload)
}

func (s *ed25519CollectorPlanSigner) signControlPlanePayloadVerified(payload []byte) ([]byte, error) {
	if s == nil || len(s.privateKey) != ed25519.PrivateKeySize || len(s.publicKey) != ed25519.PublicKeySize || len(payload) == 0 || len(payload) > 64<<10 {
		return nil, ErrCollectorPlanSigningKeyUnavailable
	}
	return ed25519.Sign(s.privateKey, payload), nil
}

type CollectorPlanTrustBundleDelivery struct {
	Payload    []byte
	ETag       string
	Generation uint64
	Checksum   string
}

type CollectorPlanTrustBundleController interface {
	FetchTrustBundle(context.Context, ID, CollectorMachineCredential) (CollectorPlanTrustBundleDelivery, error)
}

type CollectorPlanTrustBundleService struct {
	authenticator CollectorMachineAuthenticator
	repository    CollectorPlanTrustRepository
}

func NewCollectorPlanTrustBundleService(authenticator CollectorMachineAuthenticator, repository CollectorPlanTrustRepository) (*CollectorPlanTrustBundleService, error) {
	if authenticator == nil || repository == nil {
		return nil, errors.New("collector plan trust authenticator and repository are required")
	}
	return &CollectorPlanTrustBundleService{authenticator: authenticator, repository: repository}, nil
}

func (s *CollectorPlanTrustBundleService) FetchTrustBundle(ctx context.Context, collectorID ID, credential CollectorMachineCredential) (CollectorPlanTrustBundleDelivery, error) {
	if s == nil || ctx == nil || !validCollectorEvidenceID(collectorID) {
		return CollectorPlanTrustBundleDelivery{}, ErrCollectorMachineUnauthorized
	}
	identity, err := s.authenticator.AuthenticateCollector(ctx, collectorID, credential)
	if err != nil || identity.CollectorID != collectorID || identity.TenantID == "" {
		return CollectorPlanTrustBundleDelivery{}, ErrCollectorMachineUnauthorized
	}
	publication, err := s.repository.GetCollectorPlanTrustBundle(ctx)
	if err != nil {
		return CollectorPlanTrustBundleDelivery{}, err
	}
	return CollectorPlanTrustBundleDelivery{
		Payload:    append([]byte(nil), publication.BundleJSON...),
		ETag:       fmt.Sprintf(`"g%d-%s"`, publication.Generation, publication.Checksum),
		Generation: publication.Generation, Checksum: publication.Checksum,
	}, nil
}

func readCollectorPlanPrivateKey(path string) (ed25519.PrivateKey, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("collector plan private key file must be regular and accessible only by its owner")
	}
	data, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil {
		return nil, err
	}
	defer clear(data)
	if len(data) == 0 || len(data) > 64<<10 {
		return nil, errors.New("collector plan private key file size is invalid")
	}
	if block, rest := pem.Decode(data); block != nil {
		if len(bytes.TrimSpace(rest)) != 0 {
			return nil, errors.New("collector plan private key file contains trailing PEM data")
		}
		value, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse PKCS8 private key: %w", err)
		}
		key, ok := value.(ed25519.PrivateKey)
		if !ok {
			return nil, errors.New("collector plan private key must be Ed25519")
		}
		return append(ed25519.PrivateKey(nil), key...), nil
	}
	trimmed := bytes.TrimSpace(data)
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(trimmed)))
	decodedBytes, err := base64.StdEncoding.Decode(decoded, trimmed)
	if err != nil {
		clear(decoded)
		return nil, errors.New("collector plan private key must be PKCS8 PEM or base64 Ed25519")
	}
	decoded = decoded[:decodedBytes]
	defer clear(decoded)
	switch len(decoded) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(decoded), nil
	case ed25519.PrivateKeySize:
		return append(ed25519.PrivateKey(nil), decoded...), nil
	default:
		return nil, errors.New("collector plan private key has invalid size")
	}
}

func validCollectorPlanSigningKeyID(value string) bool {
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

func NewCollectorPlanTrustMaintenance(repository CollectorPlanTrustRepository, logf func(string, ...any)) *PeriodicMaintenance {
	maintenance := &PeriodicMaintenance{Logf: logf}
	maintenance.Register(MaintenanceTask{
		Name:     "collector_plan_trust_keys",
		Interval: time.Hour,
		Run: func(ctx context.Context) (int64, error) {
			_, changed, err := repository.ExpireRetiringCollectorPlanSigningKeys(ctx, time.Now())
			if errors.Is(err, ErrCollectorPlanTrustBundleUnavailable) {
				return 0, nil
			}
			if err != nil || !changed {
				return 0, err
			}
			return 1, nil
		},
	})
	return maintenance
}

var _ CollectorPlanSigner = (*ed25519CollectorPlanSigner)(nil)
var _ collectorPlanTransactionSigner = (*ed25519CollectorPlanSigner)(nil)
var _ ControlPlanePayloadSigner = (*ed25519CollectorPlanSigner)(nil)
var _ controlPlanePayloadTransactionSigner = (*ed25519CollectorPlanSigner)(nil)
var _ CollectorPlanTrustBundleController = (*CollectorPlanTrustBundleService)(nil)
