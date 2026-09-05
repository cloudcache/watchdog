package watchdog

import (
	"context"
	"errors"
	"strings"
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
}

type CollectorDrainReport struct {
	TransferID           ID
	AppliedConfigVersion uint64
	ReceiptNonce         string
}

type CollectorEvidenceController interface {
	RecordDrain(context.Context, ID, CollectorMachineCredential, CollectorDrainReport) error
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

func validCollectorEvidenceID(value ID) bool {
	return value != "" && len(value) <= 26 && string(value) == strings.TrimSpace(string(value)) && isPrintableASCII(string(value))
}

func validCollectorReceiptNonce(value string) bool {
	return value != "" && len(value) <= 128 && isPrintableASCII(value)
}

var _ CollectorEvidenceController = (*CollectorEvidenceService)(nil)
