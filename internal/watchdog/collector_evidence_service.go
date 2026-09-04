package watchdog

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

var ErrCollectorMachineUnauthorized = errors.New("collector machine authentication failed")

type CollectorMachineCredential struct {
	Token                  string
	CertificateFingerprint string
}

type CollectorMachineIdentity struct {
	TenantID    ID
	CollectorID ID
	BootID      string
}

type CollectorMachineAuthenticator interface {
	AuthenticateCollector(context.Context, ID, CollectorMachineCredential) (CollectorMachineIdentity, error)
}

type CollectorEvidenceRecorder interface {
	RecordCollectorDrain(context.Context, CollectorDrainReceipt) error
	RecordCollectorStateRestore(context.Context, CollectorStateRestoreReceipt) error
}

type CollectorDrainReport struct {
	TransferID           ID
	AppliedConfigVersion uint64
	ReceiptNonce         string
}

type CollectorStateRestoreReport struct {
	TransferID                 ID
	Kind                       flowcollect.StateCheckpointKind
	StateIdentityKey           []byte
	AppliedConfigVersion       uint64
	RestoredOldOwnershipEpoch  uint64
	RestoredOldGeneration      uint64
	NewEpochBaselineGeneration uint64
	ReceiptNonce               string
}

type CollectorEvidenceController interface {
	RecordDrain(context.Context, ID, CollectorMachineCredential, CollectorDrainReport) error
	RecordStateRestore(context.Context, ID, CollectorMachineCredential, CollectorStateRestoreReport) error
}

type CollectorEvidenceService struct {
	authenticator CollectorMachineAuthenticator
	recorder      CollectorEvidenceRecorder
}

func NewCollectorEvidenceService(authenticator CollectorMachineAuthenticator, recorder CollectorEvidenceRecorder) (*CollectorEvidenceService, error) {
	if authenticator == nil || recorder == nil {
		return nil, errors.New("collector evidence authenticator and recorder are required")
	}
	return &CollectorEvidenceService{authenticator: authenticator, recorder: recorder}, nil
}

func (s *CollectorEvidenceService) RecordDrain(ctx context.Context, collectorID ID, credential CollectorMachineCredential, report CollectorDrainReport) error {
	if s == nil || s.authenticator == nil || s.recorder == nil || ctx == nil || !validCollectorEvidenceID(collectorID) || !validCollectorEvidenceID(report.TransferID) || report.AppliedConfigVersion == 0 || !validCollectorReceiptNonce(report.ReceiptNonce) {
		return errors.New("collector drain report is incomplete")
	}
	identity, err := s.authenticator.AuthenticateCollector(ctx, collectorID, credential)
	if err != nil {
		return err
	}
	if identity.CollectorID != collectorID || identity.TenantID == "" || identity.BootID == "" {
		return ErrCollectorMachineUnauthorized
	}
	return s.recorder.RecordCollectorDrain(ctx, CollectorDrainReceipt{
		TenantID: identity.TenantID, TransferID: report.TransferID,
		AuthenticatedCollectorID: identity.CollectorID, BootID: identity.BootID,
		AppliedConfigVersion: report.AppliedConfigVersion, ReceiptNonce: report.ReceiptNonce,
	})
}

func (s *CollectorEvidenceService) RecordStateRestore(ctx context.Context, collectorID ID, credential CollectorMachineCredential, report CollectorStateRestoreReport) error {
	if s == nil || s.authenticator == nil || s.recorder == nil || ctx == nil || !validCollectorEvidenceID(collectorID) || !validCollectorEvidenceID(report.TransferID) || len(report.StateIdentityKey) != sha256.Size || report.AppliedConfigVersion == 0 || report.RestoredOldOwnershipEpoch == 0 || report.RestoredOldGeneration == 0 || !validCollectorReceiptNonce(report.ReceiptNonce) || (report.Kind != flowcollect.StateCheckpointDecoder && report.Kind != flowcollect.StateCheckpointQuality) {
		return errors.New("collector state restore report is incomplete")
	}
	identity, err := s.authenticator.AuthenticateCollector(ctx, collectorID, credential)
	if err != nil {
		return err
	}
	if identity.CollectorID != collectorID || identity.TenantID == "" || identity.BootID == "" {
		return ErrCollectorMachineUnauthorized
	}
	return s.recorder.RecordCollectorStateRestore(ctx, CollectorStateRestoreReceipt{
		TenantID: identity.TenantID, TransferID: report.TransferID,
		AuthenticatedCollectorID: identity.CollectorID, Kind: report.Kind,
		StateIdentityKey: append([]byte(nil), report.StateIdentityKey...), BootID: identity.BootID,
		AppliedConfigVersion:       report.AppliedConfigVersion,
		RestoredOldOwnershipEpoch:  report.RestoredOldOwnershipEpoch,
		RestoredOldGeneration:      report.RestoredOldGeneration,
		NewEpochBaselineGeneration: report.NewEpochBaselineGeneration,
		ReceiptNonce:               report.ReceiptNonce,
	})
}

func validCollectorEvidenceID(value ID) bool {
	return value != "" && len(value) <= 26 && string(value) == strings.TrimSpace(string(value)) && isPrintableASCII(string(value))
}

func validCollectorReceiptNonce(value string) bool {
	return value != "" && len(value) <= 128 && isPrintableASCII(value)
}

var _ CollectorEvidenceController = (*CollectorEvidenceService)(nil)
