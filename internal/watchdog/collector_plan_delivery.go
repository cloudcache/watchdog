package watchdog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

var ErrCollectorPlanUnavailable = errors.New("collector active plan is unavailable")

type CollectorPlanDelivery struct {
	Envelope      []byte
	ETag          string
	ConfigVersion uint64
	SpecHash      string
	ExpiresAt     time.Time
}

type CollectorPlanFailureReport struct {
	FailedConfigVersion uint64
	BootID              string
	SoftwareVersion     string
	Stage               string
	Code                string
	Detail              string
}

type CollectorPlanFailure struct {
	TenantID    ID
	CollectorID ID
	CollectorPlanFailureReport
}

type CollectorPlanRuntimeRepository interface {
	GetActiveCollectorPlan(context.Context, ID, ID) (CollectorPlanRevision, error)
	AcknowledgeCollectorPlan(context.Context, CollectorPlanAcknowledgement) error
	RecordCollectorPlanFailure(context.Context, CollectorPlanFailure) error
	RecordCollectorRuntimeHeartbeat(context.Context, CollectorRuntimeHeartbeat) error
}

type CollectorPlanDeliveryController interface {
	Fetch(context.Context, ID, CollectorMachineCredential) (CollectorPlanDelivery, error)
	Acknowledge(context.Context, ID, CollectorMachineCredential, CollectorPlanAcknowledgement) error
	ReportFailure(context.Context, ID, CollectorMachineCredential, CollectorPlanFailureReport) error
	ReportRuntime(context.Context, ID, CollectorMachineCredential, CollectorRuntimeHeartbeatReport) error
}

type CollectorPlanDeliveryService struct {
	authenticator CollectorMachineAuthenticator
	repository    CollectorPlanRuntimeRepository
	now           func() time.Time
}

func NewCollectorPlanDeliveryService(authenticator CollectorMachineAuthenticator, repository CollectorPlanRuntimeRepository) (*CollectorPlanDeliveryService, error) {
	if authenticator == nil || repository == nil {
		return nil, errors.New("collector plan authenticator and repository are required")
	}
	return &CollectorPlanDeliveryService{authenticator: authenticator, repository: repository, now: time.Now}, nil
}

func (s *CollectorPlanDeliveryService) Fetch(ctx context.Context, collectorID ID, credential CollectorMachineCredential) (CollectorPlanDelivery, error) {
	identity, err := s.authenticate(ctx, collectorID, credential)
	if err != nil {
		return CollectorPlanDelivery{}, err
	}
	plan, err := s.repository.GetActiveCollectorPlan(ctx, identity.TenantID, identity.CollectorID)
	if err != nil {
		return CollectorPlanDelivery{}, err
	}
	now := s.now().UTC()
	if plan.Status != CollectorPlanActive || (!plan.NotBefore.IsZero() && now.Before(plan.NotBefore)) || !now.Before(plan.ExpiresAt) {
		return CollectorPlanDelivery{}, ErrCollectorPlanUnavailable
	}
	metadata := flowcollect.PlanSignatureMetadata{
		PlanID: string(plan.ID), TenantID: string(plan.TenantID), CollectorID: string(plan.CollectorID),
		ConfigVersion: plan.ConfigVersion, PlanSchemaVersion: plan.PlanSchemaVersion,
		SpecHash: plan.SpecHash, SigningKeyID: plan.SigningKeyID,
		ExpiresAtUnixMilli: plan.ExpiresAt.UnixMilli(), SupersedesConfigVersion: plan.SupersedesConfigVersion,
	}
	if !plan.NotBefore.IsZero() {
		metadata.NotBeforeUnixMilli = plan.NotBefore.UnixMilli()
	}
	envelope, err := flowcollect.MarshalControlPlaneSignedPlan(metadata, plan.SpecJSON, plan.Signature)
	if err != nil {
		return CollectorPlanDelivery{}, fmt.Errorf("encode active collector plan delivery: %w", err)
	}
	return CollectorPlanDelivery{
		Envelope: envelope, ETag: collectorPlanETag(plan.ConfigVersion, plan.SpecHash),
		ConfigVersion: plan.ConfigVersion, SpecHash: plan.SpecHash, ExpiresAt: plan.ExpiresAt,
	}, nil
}

func (s *CollectorPlanDeliveryService) Acknowledge(ctx context.Context, collectorID ID, credential CollectorMachineCredential, acknowledgement CollectorPlanAcknowledgement) error {
	identity, err := s.authenticate(ctx, collectorID, credential)
	if err != nil {
		return err
	}
	acknowledgement.TenantID = identity.TenantID
	acknowledgement.CollectorID = identity.CollectorID
	if err := validateCollectorPlanAcknowledgement(acknowledgement); err != nil {
		return err
	}
	return s.repository.AcknowledgeCollectorPlan(ctx, acknowledgement)
}

func (s *CollectorPlanDeliveryService) ReportFailure(ctx context.Context, collectorID ID, credential CollectorMachineCredential, report CollectorPlanFailureReport) error {
	identity, err := s.authenticate(ctx, collectorID, credential)
	if err != nil {
		return err
	}
	if err := validateCollectorPlanFailureReport(report); err != nil {
		return err
	}
	return s.repository.RecordCollectorPlanFailure(ctx, CollectorPlanFailure{
		TenantID: identity.TenantID, CollectorID: identity.CollectorID, CollectorPlanFailureReport: report,
	})
}

func (s *CollectorPlanDeliveryService) ReportRuntime(ctx context.Context, collectorID ID, credential CollectorMachineCredential, report CollectorRuntimeHeartbeatReport) error {
	identity, err := s.authenticate(ctx, collectorID, credential)
	if err != nil {
		return err
	}
	heartbeat, err := prepareCollectorRuntimeHeartbeat(identity, report, s.now())
	if err != nil {
		return err
	}
	return s.repository.RecordCollectorRuntimeHeartbeat(ctx, heartbeat)
}

func (s *CollectorPlanDeliveryService) authenticate(ctx context.Context, collectorID ID, credential CollectorMachineCredential) (CollectorMachineIdentity, error) {
	if s == nil || s.authenticator == nil || s.repository == nil || ctx == nil || !validCollectorEvidenceID(collectorID) {
		return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
	}
	identity, err := s.authenticator.AuthenticateCollector(ctx, collectorID, credential)
	if err != nil {
		return CollectorMachineIdentity{}, err
	}
	if identity.TenantID == "" || identity.CollectorID != collectorID {
		return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
	}
	return identity, nil
}

func validateCollectorPlanFailureReport(report CollectorPlanFailureReport) error {
	validStages := map[string]struct{}{
		"transport": {}, "decode": {}, "verify": {}, "compatibility": {},
		"dependency": {}, "persist": {}, "activate": {},
	}
	if _, ok := validStages[report.Stage]; !ok || report.BootID == "" || len(report.BootID) > 64 || !isPrintableASCII(report.BootID) || !validCollectorPlanFailureCode(report.Code, 64-len(report.Stage)-1) || len(report.SoftwareVersion) > 64 || !isPrintableASCII(report.SoftwareVersion) || len(report.Detail) > 1024 || !isPrintableASCIIText(report.Detail) {
		return errors.New("collector plan failure report is invalid")
	}
	return nil
}

func validateCollectorPlanAcknowledgement(acknowledgement CollectorPlanAcknowledgement) error {
	if acknowledgement.TenantID == "" || acknowledgement.CollectorID == "" {
		return errors.New("collector plan acknowledgement identity is required")
	}
	return validateCollectorPlanAcknowledgementReport(acknowledgement)
}

func validateCollectorPlanAcknowledgementReport(acknowledgement CollectorPlanAcknowledgement) error {
	if acknowledgement.ConfigVersion == 0 || !validSHA256Hex(acknowledgement.SpecHash) || acknowledgement.BootID == "" || len(acknowledgement.BootID) > 64 || !isPrintableASCII(acknowledgement.BootID) || len(acknowledgement.SoftwareVersion) > 64 || !isPrintableASCII(acknowledgement.SoftwareVersion) {
		return errors.New("collector plan acknowledgement is incomplete")
	}
	return nil
}

func validCollectorPlanFailureCode(value string, max int) bool {
	if value == "" || len(value) > max || value != strings.ToUpper(value) {
		return false
	}
	for _, character := range value {
		if (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func isPrintableASCIIText(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func collectorPlanETag(configVersion uint64, specHash string) string {
	return fmt.Sprintf(`"v%d-%s"`, configVersion, specHash)
}

var _ CollectorPlanDeliveryController = (*CollectorPlanDeliveryService)(nil)
