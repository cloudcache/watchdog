package flowworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
)

const (
	versionHTTPMaxDesiredBytes = (100 * maxEnrichmentVersionEnvelopeBytes) + (64 << 10)
	versionHTTPMaxErrorBytes   = 8 << 10
	versionHTTPDefaultPageSize = 20
)

var (
	ErrVersionRemoteInvalid     = errors.New("flow enrichment control-plane response is invalid")
	ErrVersionRemoteUnavailable = errors.New("flow enrichment control-plane request failed")
)

type VersionHTTPClientConfig struct {
	BaseURL    string
	AgentToken string
	Identity   VersionWorkerIdentity
	Client     *http.Client
}

type VersionHTTPClient struct {
	base     *url.URL
	token    string
	identity VersionWorkerIdentity
	client   *http.Client
}

type VersionPublicationHTTPPage struct {
	Items       []json.RawMessage
	NextVersion uint32
	HasMore     bool
}

type versionHTTPAckRequest struct {
	State                  string `json:"state"`
	BootID                 string `json:"boot_id"`
	SoftwareVersion        string `json:"software_version"`
	DimensionSnapshotID    string `json:"dimension_snapshot_id"`
	DimensionVersion       uint64 `json:"dimension_version"`
	DimensionChecksum      string `json:"dimension_checksum"`
	ClassificationVersion  uint32 `json:"classification_version"`
	ClassificationChecksum string `json:"classification_checksum"`
	FailureStage           string `json:"failure_stage,omitempty"`
	FailureCode            string `json:"failure_code,omitempty"`
	FailureMessage         string `json:"failure_message,omitempty"`
}

func NewVersionHTTPClient(config VersionHTTPClientConfig) (*VersionHTTPClient, error) {
	base, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || base == nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" ||
		base.User != nil || base.RawQuery != "" || base.Fragment != "" || base.Opaque != "" {
		return nil, errors.New("flow enrichment control-plane base URL is invalid")
	}
	if !validIdentifier(config.Identity.WorkerID, 26) || !validASCII(config.Identity.BootID, 64) ||
		!validASCII(config.Identity.SoftwareVersion, 64) || !validASCII(config.AgentToken, 256) {
		return nil, errors.New("flow worker HTTP identity or token is invalid")
	}
	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Minute}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base.Path = strings.TrimRight(base.Path, "/")
	base.RawPath = ""
	return &VersionHTTPClient{base: base, token: config.AgentToken, identity: config.Identity, client: &clientCopy}, nil
}

func (c *VersionHTTPClient) FetchTrustBundle(ctx context.Context) ([]byte, error) {
	response, err := c.get(ctx, "api", "v1", "flow-workers", c.identity.WorkerID, "trust-bundle")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, versionHTTPStatusError("fetch trust bundle", response)
	}
	data, err := readVersionHTTPBody(response.Body, versionLKGMaxTrustBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: trust bundle body: %v", ErrVersionRemoteInvalid, err)
	}
	bundle, checksum, err := flowplan.ParseTrustBundle(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVersionRemoteInvalid, err)
	}
	generation, err := strconv.ParseUint(response.Header.Get("X-Watchdog-Trust-Generation"), 10, 64)
	if err != nil || generation != bundle.Generation || response.Header.Get("X-Watchdog-Trust-Checksum") != checksum {
		return nil, fmt.Errorf("%w: trust bundle headers do not match payload", ErrVersionRemoteInvalid)
	}
	return data, nil
}

func (c *VersionHTTPClient) FetchDesired(ctx context.Context, afterVersion uint32, limit int) (VersionPublicationHTTPPage, error) {
	if limit < 1 || limit > 100 {
		return VersionPublicationHTTPPage{}, errors.New("flow enrichment page limit must be 1..100")
	}
	query := make(url.Values)
	query.Set("after_version", strconv.FormatUint(uint64(afterVersion), 10))
	query.Set("limit", strconv.Itoa(limit))
	request, err := c.newRequest(ctx, http.MethodGet, query, nil, "api", "v1", "flow-workers", c.identity.WorkerID, "enrichment-publications")
	if err != nil {
		return VersionPublicationHTTPPage{}, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return VersionPublicationHTTPPage{}, fmt.Errorf("%w: fetch desired publications: %v", ErrVersionRemoteUnavailable, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return VersionPublicationHTTPPage{}, versionHTTPStatusError("fetch desired publications", response)
	}
	data, err := readVersionHTTPBody(response.Body, versionHTTPMaxDesiredBytes)
	if err != nil {
		return VersionPublicationHTTPPage{}, fmt.Errorf("%w: desired publication body: %v", ErrVersionRemoteInvalid, err)
	}
	var wire struct {
		Items       []json.RawMessage `json:"items"`
		NextVersion uint32            `json:"next_version"`
		HasMore     bool              `json:"has_more"`
	}
	if err := decodeVersionHTTPJSON(data, &wire); err != nil || len(wire.Items) > limit ||
		(len(wire.Items) == 0 && (wire.HasMore || wire.NextVersion != afterVersion)) {
		return VersionPublicationHTTPPage{}, fmt.Errorf("%w: desired publication page", ErrVersionRemoteInvalid)
	}
	return VersionPublicationHTTPPage{Items: wire.Items, NextVersion: wire.NextVersion, HasMore: wire.HasMore}, nil
}

func (c *VersionHTTPClient) OpenObject(ctx context.Context, publicationID, kind, checksum string, maxBytes int) (io.ReadCloser, error) {
	if !validIdentifier(publicationID, 128) || (kind != "dimension" && kind != "classification") || !validSHA256(checksum) || maxBytes < 1 {
		return nil, errors.New("flow enrichment object request is invalid")
	}
	request, err := c.newRequest(ctx, http.MethodGet, nil, nil, "api", "v1", "flow-workers", c.identity.WorkerID, "enrichment-publications", publicationID, "objects", kind)
	if err != nil {
		return nil, err
	}
	if kind == "dimension" {
		request.Header.Set("Accept", "application/octet-stream")
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: fetch %s object: %v", ErrVersionRemoteUnavailable, kind, err)
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		return nil, versionHTTPStatusError("fetch "+kind+" object", response)
	}
	if response.Header.Get("Content-Encoding") != "" || response.Header.Get("X-Watchdog-Object-Checksum") != checksum ||
		response.ContentLength <= 0 || response.ContentLength > int64(maxBytes) {
		response.Body.Close()
		return nil, fmt.Errorf("%w: %s object headers", ErrVersionRemoteInvalid, kind)
	}
	return response.Body, nil
}

func (c *VersionHTTPClient) Acknowledge(ctx context.Context, acknowledgement EnrichmentVersionAcknowledgement) error {
	if acknowledgement.WorkerID != c.identity.WorkerID || acknowledgement.BootID != c.identity.BootID || acknowledgement.SoftwareVersion != c.identity.SoftwareVersion {
		return errors.New("flow enrichment acknowledgement identity mismatch")
	}
	return c.acknowledge(ctx, acknowledgement.PublicationID, versionHTTPAckRequest{
		State: "installed", BootID: acknowledgement.BootID, SoftwareVersion: acknowledgement.SoftwareVersion,
		DimensionSnapshotID: acknowledgement.DimensionSnapshotID, DimensionVersion: acknowledgement.DimensionVersion,
		DimensionChecksum: acknowledgement.DimensionChecksum, ClassificationVersion: acknowledgement.ClassificationVersion,
		ClassificationChecksum: acknowledgement.ClassificationChecksum,
	})
}

func (c *VersionHTTPClient) AcknowledgeDownloaded(ctx context.Context, publication EnrichmentVersionPublication) error {
	return c.acknowledgePublication(ctx, publication, versionHTTPAckRequest{State: "downloaded"})
}

func (c *VersionHTTPClient) AcknowledgeFailed(ctx context.Context, publication EnrichmentVersionPublication, stage, code string, cause error) error {
	if cause == nil || !validVersionFailureStage(stage) || !validVersionFailureCode(code) {
		return errors.New("flow enrichment failure acknowledgement is invalid")
	}
	return c.acknowledgePublication(ctx, publication, versionHTTPAckRequest{
		State: "failed", FailureStage: stage, FailureCode: code, FailureMessage: printableVersionFailure(cause.Error()),
	})
}

func (c *VersionHTTPClient) acknowledgePublication(ctx context.Context, publication EnrichmentVersionPublication, request versionHTTPAckRequest) error {
	request.BootID = c.identity.BootID
	request.SoftwareVersion = c.identity.SoftwareVersion
	request.DimensionSnapshotID = publication.DimensionSnapshotID
	request.DimensionVersion = publication.DimensionVersion
	request.DimensionChecksum = publication.Dimension.Checksum
	request.ClassificationVersion = publication.ClassificationVersion
	request.ClassificationChecksum = publication.Classification.Checksum
	return c.acknowledge(ctx, publication.PublicationID, request)
}

func (c *VersionHTTPClient) acknowledge(ctx context.Context, publicationID string, payload versionHTTPAckRequest) error {
	if !validIdentifier(publicationID, 128) {
		return errors.New("flow enrichment publication ID is invalid")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := c.newRequest(ctx, http.MethodPost, nil, bytes.NewReader(data), "api", "v1", "flow-workers", c.identity.WorkerID, "enrichment-publications", publicationID, "ack")
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: acknowledge publication: %v", ErrVersionRemoteUnavailable, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		return versionHTTPStatusError("acknowledge publication", response)
	}
	_, err = io.Copy(io.Discard, io.LimitReader(response.Body, versionHTTPMaxErrorBytes+1))
	return err
}

func (c *VersionHTTPClient) get(ctx context.Context, path ...string) (*http.Response, error) {
	request, err := c.newRequest(ctx, http.MethodGet, nil, nil, path...)
	if err != nil {
		return nil, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: GET: %v", ErrVersionRemoteUnavailable, err)
	}
	return response, nil
}

func (c *VersionHTTPClient) newRequest(ctx context.Context, method string, query url.Values, body io.Reader, path ...string) (*http.Request, error) {
	if c == nil || c.base == nil || c.client == nil || ctx == nil {
		return nil, errors.New("flow enrichment HTTP client is not initialized")
	}
	endpoint := c.base.JoinPath(path...)
	if query != nil {
		endpoint.RawQuery = query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("X-Watchdog-Agent-Token", c.token)
	request.Header.Set("Accept", "application/json")
	return request, nil
}

func versionHTTPStatusError(operation string, response *http.Response) error {
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, versionHTTPMaxErrorBytes+1))
	return fmt.Errorf("%w: %s returned HTTP %d", ErrVersionRemoteUnavailable, operation, response.StatusCode)
}

func readVersionHTTPBody(reader io.Reader, maximum int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, int64(maximum)+1))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > maximum {
		return nil, errors.New("response body exceeds its byte boundary")
	}
	return data, nil
}

func decodeVersionHTTPJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("response must contain exactly one JSON document")
	}
	return nil
}

func validASCII(value string, maximum int) bool {
	if value == "" || len(value) > maximum || strings.TrimSpace(value) != value {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func validVersionFailureStage(value string) bool {
	switch value {
	case "transport", "verify", "compile", "persist", "activate", "ack":
		return true
	default:
		return false
	}
}

func validVersionFailureCode(value string) bool {
	if value == "" || len(value) > 48 || value != strings.ToUpper(value) {
		return false
	}
	for _, character := range value {
		if (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func printableVersionFailure(value string) string {
	var result strings.Builder
	result.Grow(min(len(value), 512))
	for index := 0; index < len(value) && result.Len() < 512; index++ {
		character := value[index]
		if character < 0x20 || character > 0x7e {
			character = '?'
		}
		result.WriteByte(character)
	}
	return result.String()
}

type RemoteVersionSync struct {
	mu       sync.Mutex
	client   *VersionHTTPClient
	lkg      *DiskVersionLKG
	trust    *flowplan.TrustStore
	loader   *VersionLoader
	limits   VersionLoaderLimits
	now      func() time.Time
	pageSize int
}

type RemoteVersionSyncResult struct {
	PreviousVersion uint32
	HighestVersion  uint32
	Installed       uint32
}

type versionObjectSyncError struct {
	stage string
	code  string
	err   error
}

func (e *versionObjectSyncError) Error() string { return e.err.Error() }
func (e *versionObjectSyncError) Unwrap() error { return e.err }

func NewRemoteVersionSync(client *VersionHTTPClient, lkg *DiskVersionLKG, trust *flowplan.TrustStore, catalog *EnrichmentVersionCatalog, limits VersionLoaderLimits) (*RemoteVersionSync, error) {
	if client == nil || lkg == nil || trust == nil || catalog == nil {
		return nil, errors.New("flow enrichment remote sync dependencies are required")
	}
	loader, err := NewPersistentVersionLoader(lkg, client, catalog, client.identity, limits, lkg)
	if err != nil {
		return nil, err
	}
	return &RemoteVersionSync{
		client: client, lkg: lkg, trust: trust, loader: loader, limits: loader.limits,
		now: time.Now, pageSize: versionHTTPDefaultPageSize,
	}, nil
}

func (s *RemoteVersionSync) SyncOnce(ctx context.Context, afterVersion uint32) (RemoteVersionSyncResult, error) {
	if s == nil || ctx == nil {
		return RemoteVersionSyncResult{}, errors.New("flow enrichment remote sync is not initialized")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := RemoteVersionSyncResult{PreviousVersion: afterVersion, HighestVersion: afterVersion}
	trustData, err := s.client.FetchTrustBundle(ctx)
	if err != nil {
		return result, err
	}
	if err := s.trust.Install(trustData); err != nil {
		return result, fmt.Errorf("install flow enrichment trust bundle: %w", err)
	}
	if err := s.lkg.SaveTrustBundle(ctx, trustData); err != nil {
		return result, fmt.Errorf("persist flow enrichment trust bundle: %w", err)
	}

	cursor := afterVersion
	totalItems := 0
	for pageNumber := 0; pageNumber < versionLKGMaxPublicationCount; pageNumber++ {
		page, err := s.client.FetchDesired(ctx, cursor, s.pageSize)
		if err != nil {
			return result, err
		}
		totalItems += len(page.Items)
		if totalItems > versionLKGMaxPublicationCount {
			return result, fmt.Errorf("%w: desired publication count exceeds limit", ErrVersionRemoteInvalid)
		}
		for _, raw := range page.Items {
			envelope, err := VerifySignedEnrichmentVersionPublication(raw, s.trust, s.now().UTC())
			if err != nil {
				return result, fmt.Errorf("%w: verify signed enrichment publication: %v", ErrVersionRemoteInvalid, err)
			}
			publication := envelope.Publication
			if publication.ClassificationVersion <= cursor {
				return result, fmt.Errorf("%w: desired publication versions are not strictly increasing", ErrVersionRemoteInvalid)
			}
			if err := s.lkg.RegisterVerifiedPublication(envelope, raw); err != nil {
				return result, s.fail(ctx, publication, "persist", "LKG_REGISTER_FAILED", err)
			}
			if err := s.ensureObject(ctx, publication, "dimension", publication.Dimension, s.dimensionLimit(publication.Dimension)); err != nil {
				return result, s.failObject(ctx, publication, err)
			}
			if err := s.ensureObject(ctx, publication, "classification", publication.Classification, s.limits.MaxClassificationObjectBytes); err != nil {
				return result, s.failObject(ctx, publication, err)
			}
			downloadAckErr := s.client.AcknowledgeDownloaded(ctx, publication)
			if err := s.loader.Install(ctx, publication); err != nil {
				stage, code := versionInstallFailure(err)
				if downloadAckErr != nil {
					err = errors.Join(err, downloadAckErr)
				}
				return result, s.fail(ctx, publication, stage, code, err)
			}
			cursor = publication.ClassificationVersion
			result.HighestVersion = cursor
			result.Installed++
		}
		if page.NextVersion != cursor || (page.HasMore && len(page.Items) == 0) {
			return result, fmt.Errorf("%w: desired publication cursor is inconsistent", ErrVersionRemoteInvalid)
		}
		if !page.HasMore {
			return result, nil
		}
	}
	return result, fmt.Errorf("%w: desired publication pagination did not terminate", ErrVersionRemoteInvalid)
}

func (s *RemoteVersionSync) ensureObject(ctx context.Context, publication EnrichmentVersionPublication, kind string, reference VersionObjectReference, maxBytes int) error {
	prefix := strings.ToUpper(kind)
	available, err := s.lkg.objectAvailable(ctx, reference.Checksum, maxBytes)
	if err != nil {
		stage, code := "persist", prefix+"_CACHE_FAILED"
		if errors.Is(err, ErrVersionObjectIntegrity) {
			stage, code = "verify", prefix+"_CACHE_INVALID"
		}
		return &versionObjectSyncError{stage: stage, code: code, err: err}
	}
	if available {
		return nil
	}
	reader, err := s.client.OpenObject(ctx, publication.PublicationID, kind, reference.Checksum, maxBytes)
	if err != nil {
		stage, code := "transport", prefix+"_FETCH_FAILED"
		if errors.Is(err, ErrVersionRemoteInvalid) {
			stage, code = "verify", prefix+"_HEADER_INVALID"
		}
		return &versionObjectSyncError{stage: stage, code: code, err: err}
	}
	defer reader.Close()
	if err := s.lkg.StoreObject(ctx, reference.ObjectRef, reference.Checksum, maxBytes, reader); err != nil {
		stage, code := "persist", prefix+"_STORE_FAILED"
		if errors.Is(err, ErrVersionObjectIntegrity) {
			stage, code = "verify", prefix+"_OBJECT_INVALID"
		} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) {
			stage, code = "transport", prefix+"_TRANSFER_FAILED"
		}
		return &versionObjectSyncError{stage: stage, code: code, err: err}
	}
	return nil
}

func (s *RemoteVersionSync) dimensionLimit(reference VersionObjectReference) int {
	if reference.ObjectFormat == VersionObjectFormatWADS {
		return s.limits.MaxAddressSnapshotObjectBytes
	}
	return s.limits.MaxDimensionObjectBytes
}

func (s *RemoteVersionSync) fail(ctx context.Context, publication EnrichmentVersionPublication, stage, code string, cause error) error {
	if ackErr := s.client.AcknowledgeFailed(ctx, publication, stage, code, cause); ackErr != nil {
		return errors.Join(cause, fmt.Errorf("failure acknowledgement: %w", ackErr))
	}
	return cause
}

func (s *RemoteVersionSync) failObject(ctx context.Context, publication EnrichmentVersionPublication, cause error) error {
	var failure *versionObjectSyncError
	if !errors.As(cause, &failure) {
		return s.fail(ctx, publication, "transport", "OBJECT_FETCH_FAILED", cause)
	}
	return s.fail(ctx, publication, failure.stage, failure.code, cause)
}

func versionInstallFailure(err error) (string, string) {
	switch {
	case errors.Is(err, ErrVersionPersistence):
		return "persist", "LKG_MANIFEST_FAILED"
	case errors.Is(err, ErrInvalidVersionPublication):
		return "compile", "PUBLICATION_COMPILE_FAILED"
	case errors.Is(err, ErrVersionAcknowledgement):
		return "ack", "INSTALLED_ACK_FAILED"
	default:
		return "activate", "CATALOG_ACTIVATE_FAILED"
	}
}

var _ EnrichmentVersionAcknowledger = (*VersionHTTPClient)(nil)
