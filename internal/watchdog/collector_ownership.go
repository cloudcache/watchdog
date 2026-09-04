package watchdog

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

const collectorEvidenceMaxReceiptBytes = 1 << 20

var (
	ErrCollectorEvidenceConflict = errors.New("collector ownership evidence conflict")
	ErrCollectorEvidenceNotReady = errors.New("collector ownership evidence is not ready")
)

type CollectorServicePrincipalGrant struct {
	ID                  ID
	TenantID            ID
	CollectorID         ID
	ServiceType         string
	PrincipalRef        string
	CredentialSecretRef string
	Provider            string
	GrantOperationKey   string
	GrantRequestHash    string
	GrantReceiptRef     string
	GrantReceipt        []byte
	ACLPropagationDelay time.Duration
	ActorID             ID
}

type CollectorPrincipalRevocation struct {
	TenantID           ID
	PrincipalID        ID
	ExpectedRowVersion uint64
	Provider           string
	OperationKey       string
	RevokeReceiptRef   string
	RevokeReceipt      []byte
	ActorID            ID
}

type CollectorServicePrincipal struct {
	ID                  ID
	TenantID            ID
	CollectorID         ID
	ServiceType         string
	PrincipalRef        string
	CredentialSecretRef string
	Provider            string
	GrantOperationKey   string
	GrantRequestHash    string
	Status              string
	RevokeOperationKey  string
	ACLPropagationDelay time.Duration
	RowVersion          uint64
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type CollectorOwnershipTransfer struct {
	ID                      ID
	TenantID                ID
	ExporterID              ID
	OldCollectorID          ID
	NewCollectorID          ID
	OldPlanRevision         uint64
	OldRevokePlanRevision   uint64
	NewPlanRevision         uint64
	OldOwnershipEpoch       uint64
	NewOwnershipEpoch       uint64
	OldPrincipalID          ID
	MaxClockSkew            time.Duration
	ApprovalID              ID
	RequestedBy             ID
	ExpectedOldCollectorRow uint64
	ExpectedNewCollectorRow uint64
}

type CollectorDrainReceipt struct {
	TenantID                 ID
	TransferID               ID
	AuthenticatedCollectorID ID
	BootID                   string
	AppliedConfigVersion     uint64
	ReceiptNonce             string
}

type CollectorStateRestoreReceipt struct {
	TenantID                   ID
	TransferID                 ID
	AuthenticatedCollectorID   ID
	Kind                       flowcollect.StateCheckpointKind
	StateIdentityKey           []byte
	BootID                     string
	AppliedConfigVersion       uint64
	RestoredOldOwnershipEpoch  uint64
	RestoredOldGeneration      uint64
	NewEpochBaselineGeneration uint64
	ReceiptNonce               string
}

func validateCollectorServicePrincipalGrant(grant CollectorServicePrincipalGrant) error {
	if grant.ID == "" || len(grant.ID) > 26 || grant.TenantID == "" || len(grant.TenantID) > 26 || grant.CollectorID == "" || len(grant.CollectorID) > 26 || grant.ServiceType != "kafka" || strings.TrimSpace(grant.PrincipalRef) == "" || len(grant.PrincipalRef) > 190 || !isPrintableASCII(grant.PrincipalRef) || strings.TrimSpace(grant.CredentialSecretRef) == "" || len(grant.CredentialSecretRef) > 255 || !isPrintableASCII(grant.CredentialSecretRef) || strings.TrimSpace(grant.Provider) == "" || len(grant.Provider) > 64 || !isPrintableASCII(grant.Provider) || !validSHA256Hex(grant.GrantOperationKey) || !validSHA256Hex(grant.GrantRequestHash) || strings.TrimSpace(grant.GrantReceiptRef) == "" || len(grant.GrantReceiptRef) > 512 || !isPrintableASCII(grant.GrantReceiptRef) || len(grant.GrantReceipt) == 0 || len(grant.GrantReceipt) > collectorEvidenceMaxReceiptBytes || grant.ActorID == "" || len(grant.ActorID) > 26 {
		return errors.New("collector service principal grant is incomplete")
	}
	if !validEvidenceDuration(grant.ACLPropagationDelay) {
		return errors.New("collector principal ACL propagation delay is invalid")
	}
	return nil
}

func validateCollectorOwnershipTransfer(transfer CollectorOwnershipTransfer) error {
	if transfer.ID == "" || len(transfer.ID) > 26 || transfer.TenantID == "" || len(transfer.TenantID) > 26 || transfer.ExporterID == "" || len(transfer.ExporterID) > 26 || transfer.OldCollectorID == "" || len(transfer.OldCollectorID) > 26 || transfer.NewCollectorID == "" || len(transfer.NewCollectorID) > 26 || transfer.OldCollectorID == transfer.NewCollectorID || transfer.OldPlanRevision == 0 || transfer.OldRevokePlanRevision <= transfer.OldPlanRevision || transfer.NewPlanRevision == 0 || transfer.OldOwnershipEpoch == 0 || transfer.NewOwnershipEpoch <= transfer.OldOwnershipEpoch || transfer.OldPrincipalID == "" || len(transfer.OldPrincipalID) > 26 || transfer.ApprovalID == "" || len(transfer.ApprovalID) > 26 || transfer.RequestedBy == "" || len(transfer.RequestedBy) > 26 || transfer.ExpectedOldCollectorRow == 0 || transfer.ExpectedNewCollectorRow == 0 {
		return errors.New("collector ownership transfer is incomplete")
	}
	if !validEvidenceDuration(transfer.MaxClockSkew) {
		return errors.New("collector ownership transfer clock skew is invalid")
	}
	return nil
}

func isPrintableASCII(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func validEvidenceDuration(value time.Duration) bool {
	return value >= 0 && value <= 24*time.Hour && value%time.Millisecond == 0
}

func evidencePayloadSHA256(payload []byte) (string, error) {
	if len(payload) == 0 || len(payload) > collectorEvidenceMaxReceiptBytes {
		return "", errors.New("collector evidence receipt size is invalid")
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}
