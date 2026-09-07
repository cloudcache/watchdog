package watchdog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cloudcache/watchdog/internal/flowworker"
)

const (
	FlowEnrichmentAckDownloaded = "downloaded"
	FlowEnrichmentAckInstalled  = "installed"
	FlowEnrichmentAckFailed     = "failed"

	flowEnrichmentPublicationPageLimit = 100
	flowClassificationObjectMaxBytes   = 64 << 10
)

var (
	ErrFlowEnrichmentDeliveryInvalid = errors.New("flow enrichment delivery request is invalid")
	ErrFlowEnrichmentAckConflict     = errors.New("flow enrichment acknowledgement conflicts with publication")
	ErrFlowEnrichmentObjectMissing   = errors.New("flow enrichment publication object is unavailable")
)

type FlowEnrichmentPublicationPage struct {
	Items   []FlowEnrichmentPublication
	HasMore bool
}

type FlowEnrichmentDeliveryItem struct {
	ClassificationVersion uint32
	Envelope              []byte
}

type FlowEnrichmentDeliveryPage struct {
	Items       []FlowEnrichmentDeliveryItem
	NextVersion uint32
	HasMore     bool
}

type FlowEnrichmentObjectDelivery struct {
	Path        string
	Name        string
	ContentType string
	Checksum    string
	Size        int64
}

type FlowEnrichmentAcknowledgementReport struct {
	State                  string
	BootID                 string
	SoftwareVersion        string
	DimensionSnapshotID    ID
	DimensionVersion       uint64
	DimensionChecksum      string
	ClassificationVersion  uint32
	ClassificationChecksum string
	FailureStage           string
	FailureCode            string
	FailureMessage         string
}

type FlowEnrichmentAcknowledgement struct {
	TenantID      ID
	PublicationID ID
	WorkerID      ID
	FlowEnrichmentAcknowledgementReport
	AttemptedAt time.Time
}

type FlowEnrichmentDeliveryRepository interface {
	ListFlowEnrichmentPublications(context.Context, ID, uint32, int) (FlowEnrichmentPublicationPage, error)
	GetFlowEnrichmentPublication(context.Context, ID, ID) (FlowEnrichmentPublication, error)
	RecordFlowEnrichmentAcknowledgement(context.Context, FlowEnrichmentAcknowledgement) error
}

type FlowEnrichmentDeliveryController interface {
	FetchDesired(context.Context, ID, CollectorMachineCredential, uint32, int) (FlowEnrichmentDeliveryPage, error)
	ResolveObject(context.Context, ID, CollectorMachineCredential, ID, string) (FlowEnrichmentObjectDelivery, error)
	Acknowledge(context.Context, ID, CollectorMachineCredential, ID, FlowEnrichmentAcknowledgementReport) error
}

type FlowEnrichmentDeliveryService struct {
	authenticator     CollectorMachineAuthenticator
	repository        FlowEnrichmentDeliveryRepository
	objects           DimensionObjectStore
	maxDimensionBytes int64
	now               func() time.Time
}

func NewFlowEnrichmentDeliveryService(authenticator CollectorMachineAuthenticator, repository FlowEnrichmentDeliveryRepository, objects DimensionObjectStore, maxDimensionBytes int64) (*FlowEnrichmentDeliveryService, error) {
	if authenticator == nil || repository == nil || objects == nil {
		return nil, errors.New("flow worker authenticator, publication repository, and object store are required")
	}
	if maxDimensionBytes <= 0 {
		maxDimensionBytes = 512 << 20
	}
	return &FlowEnrichmentDeliveryService{
		authenticator: authenticator, repository: repository, objects: objects,
		maxDimensionBytes: maxDimensionBytes, now: time.Now,
	}, nil
}

func (s *FlowEnrichmentDeliveryService) FetchDesired(ctx context.Context, workerID ID, credential CollectorMachineCredential, afterVersion uint32, limit int) (FlowEnrichmentDeliveryPage, error) {
	identity, err := s.authenticate(ctx, workerID, credential)
	if err != nil {
		return FlowEnrichmentDeliveryPage{}, err
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > flowEnrichmentPublicationPageLimit {
		return FlowEnrichmentDeliveryPage{}, ErrFlowEnrichmentDeliveryInvalid
	}
	page, err := s.repository.ListFlowEnrichmentPublications(ctx, identity.TenantID, afterVersion, limit)
	if err != nil {
		return FlowEnrichmentDeliveryPage{}, err
	}
	delivery := FlowEnrichmentDeliveryPage{Items: make([]FlowEnrichmentDeliveryItem, 0, len(page.Items)), NextVersion: afterVersion, HasMore: page.HasMore}
	for _, publication := range page.Items {
		if publication.TenantID != identity.TenantID || publication.ClassificationVersion <= delivery.NextVersion {
			return FlowEnrichmentDeliveryPage{}, ErrFlowEnrichmentDeliveryInvalid
		}
		envelope, err := flowworker.MarshalSignedEnrichmentVersionPublication(publication.SignedEnvelope())
		if err != nil {
			return FlowEnrichmentDeliveryPage{}, fmt.Errorf("encode flow enrichment publication: %w", err)
		}
		delivery.Items = append(delivery.Items, FlowEnrichmentDeliveryItem{
			ClassificationVersion: publication.ClassificationVersion,
			Envelope:              envelope,
		})
		delivery.NextVersion = publication.ClassificationVersion
	}
	return delivery, nil
}

func (s *FlowEnrichmentDeliveryService) ResolveObject(ctx context.Context, workerID ID, credential CollectorMachineCredential, publicationID ID, kind string) (FlowEnrichmentObjectDelivery, error) {
	identity, err := s.authenticate(ctx, workerID, credential)
	if err != nil {
		return FlowEnrichmentObjectDelivery{}, err
	}
	if !validCollectorEvidenceID(publicationID) {
		return FlowEnrichmentObjectDelivery{}, ErrFlowEnrichmentDeliveryInvalid
	}
	publication, err := s.repository.GetFlowEnrichmentPublication(ctx, identity.TenantID, publicationID)
	if err != nil {
		return FlowEnrichmentObjectDelivery{}, err
	}
	var ref, checksum, name, contentType string
	var maximum int64
	switch kind {
	case "dimension":
		ref, checksum, name, contentType, maximum = publication.DimensionObjectRef, publication.DimensionChecksum, "address-snapshot.wads", "application/octet-stream", s.maxDimensionBytes
	case "classification":
		ref, checksum, name, contentType, maximum = publication.ClassificationObjectRef, publication.ClassificationChecksum, "classification.json", "application/json", flowClassificationObjectMaxBytes
	default:
		return FlowEnrichmentObjectDelivery{}, ErrFlowEnrichmentDeliveryInvalid
	}
	path, err := s.objects.ResolveDimensionObject(ref)
	if err != nil {
		return FlowEnrichmentObjectDelivery{}, errors.Join(ErrFlowEnrichmentObjectMissing, err)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		if err == nil {
			err = fmt.Errorf("object size must be 1..%d bytes", maximum)
		}
		return FlowEnrichmentObjectDelivery{}, errors.Join(ErrFlowEnrichmentObjectMissing, err)
	}
	return FlowEnrichmentObjectDelivery{
		Path: path, Name: name, ContentType: contentType, Checksum: checksum, Size: info.Size(),
	}, nil
}

func (s *FlowEnrichmentDeliveryService) Acknowledge(ctx context.Context, workerID ID, credential CollectorMachineCredential, publicationID ID, report FlowEnrichmentAcknowledgementReport) error {
	identity, err := s.authenticate(ctx, workerID, credential)
	if err != nil {
		return err
	}
	if !validCollectorEvidenceID(publicationID) || validateFlowEnrichmentAcknowledgementReport(report) != nil {
		return ErrFlowEnrichmentDeliveryInvalid
	}
	return s.repository.RecordFlowEnrichmentAcknowledgement(ctx, FlowEnrichmentAcknowledgement{
		TenantID: identity.TenantID, PublicationID: publicationID, WorkerID: identity.CollectorID,
		FlowEnrichmentAcknowledgementReport: report, AttemptedAt: s.now().UTC().Truncate(time.Millisecond),
	})
}

func (s *FlowEnrichmentDeliveryService) authenticate(ctx context.Context, workerID ID, credential CollectorMachineCredential) (CollectorMachineIdentity, error) {
	if s == nil || s.authenticator == nil || s.repository == nil || s.objects == nil || ctx == nil || !validCollectorEvidenceID(workerID) {
		return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
	}
	identity, err := s.authenticator.AuthenticateCollector(ctx, workerID, credential)
	if err != nil || identity.TenantID == "" || identity.CollectorID != workerID {
		return CollectorMachineIdentity{}, ErrCollectorMachineUnauthorized
	}
	return identity, nil
}

func validateFlowEnrichmentAcknowledgementReport(report FlowEnrichmentAcknowledgementReport) error {
	if (report.State != FlowEnrichmentAckDownloaded && report.State != FlowEnrichmentAckInstalled && report.State != FlowEnrichmentAckFailed) ||
		report.BootID == "" || len(report.BootID) > 64 || !isPrintableASCII(report.BootID) ||
		report.SoftwareVersion == "" || len(report.SoftwareVersion) > 64 || !isPrintableASCII(report.SoftwareVersion) ||
		!validCollectorEvidenceID(report.DimensionSnapshotID) || report.DimensionVersion == 0 || !validSHA256Digest(report.DimensionChecksum) ||
		report.ClassificationVersion == 0 || !validSHA256Digest(report.ClassificationChecksum) {
		return ErrFlowEnrichmentDeliveryInvalid
	}
	if report.State != FlowEnrichmentAckFailed {
		if report.FailureStage != "" || report.FailureCode != "" || report.FailureMessage != "" {
			return ErrFlowEnrichmentDeliveryInvalid
		}
		return nil
	}
	validStages := map[string]struct{}{"transport": {}, "verify": {}, "compile": {}, "persist": {}, "activate": {}, "ack": {}}
	if _, ok := validStages[report.FailureStage]; !ok || !validCollectorPlanFailureCode(report.FailureCode, 48) ||
		len(report.FailureMessage) > 512 || !isPrintableASCIIText(report.FailureMessage) {
		return ErrFlowEnrichmentDeliveryInvalid
	}
	return nil
}

func flowEnrichmentObjectBaseName(delivery FlowEnrichmentObjectDelivery) string {
	if delivery.Name != "" {
		return delivery.Name
	}
	return filepath.Base(delivery.Path)
}

var _ FlowEnrichmentDeliveryController = (*FlowEnrichmentDeliveryService)(nil)
