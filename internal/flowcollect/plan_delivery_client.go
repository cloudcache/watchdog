package flowcollect

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const planDeliveryResponseMaxBytes = signedPlanMaxBytes

type planDeliveryFailure struct {
	Stage         string
	Code          string
	Detail        string
	ConfigVersion uint64
	Err           error
}

func (f planDeliveryFailure) Error() string {
	if f.Err == nil {
		return f.Stage + ":" + f.Code
	}
	return f.Stage + ":" + f.Code + ": " + f.Err.Error()
}

func (f planDeliveryFailure) Unwrap() error { return f.Err }

type planDeliveryPending struct {
	ConfigVersion uint64
	SpecHash      string
}

type planDeliveryHeartbeatRequest struct {
	SchemaVersion uint16                            `json:"schema_version"`
	Kind          string                            `json:"kind"`
	Runtime       *planDeliveryRuntimeHeartbeat     `json:"runtime,omitempty"`
	PlanFailure   *planDeliveryFailureHeartbeatBody `json:"plan_failure,omitempty"`
}

type planDeliveryFailureHeartbeatBody struct {
	FailedConfigVersion uint64 `json:"failed_config_version"`
	BootID              string `json:"boot_id"`
	SoftwareVersion     string `json:"software_version"`
	Stage               string `json:"stage"`
	Code                string `json:"code"`
	Detail              string `json:"detail"`
}

type planDeliveryRuntimeHeartbeat struct {
	SchemaVersion       uint16                          `json:"schema_version"`
	Sequence            uint64                          `json:"sequence"`
	SentAtUnixMilli     int64                           `json:"sent_at_unix_ms"`
	BootID              string                          `json:"boot_id"`
	SoftwareVersion     string                          `json:"software_version"`
	AgentAPIVersion     uint16                          `json:"agent_api_version"`
	PlanSchemaMin       uint16                          `json:"plan_schema_min"`
	PlanSchemaMax       uint16                          `json:"plan_schema_max"`
	ActiveConfigVersion uint64                          `json:"active_config_version"`
	ActiveSpecHash      string                          `json:"active_spec_hash,omitempty"`
	Capabilities        planDeliveryRuntimeCapabilities `json:"capabilities"`
	Observation         planDeliveryRuntimeObservation  `json:"observation"`
}

type planDeliveryRuntimeCapabilities struct {
	SchemaVersion        uint16   `json:"schema_version"`
	Protocols            []string `json:"protocols"`
	PlanEnvelopeVersions []uint16 `json:"plan_envelope_versions"`
}

type planDeliveryQueueObservation struct {
	Depth    uint64 `json:"depth"`
	Capacity uint64 `json:"capacity"`
}

type planDeliveryRuntimeQueues struct {
	Receive    planDeliveryQueueObservation `json:"receive"`
	Decode     planDeliveryQueueObservation `json:"decode"`
	Quarantine planDeliveryQueueObservation `json:"quarantine"`
}

type planDeliveryWALObservation struct {
	Bytes                 uint64 `json:"bytes"`
	MaxBytes              uint64 `json:"max_bytes"`
	OldestAgeMilliseconds uint64 `json:"oldest_age_ms"`
	Writable              bool   `json:"writable"`
	SoftWatermark         bool   `json:"soft_watermark"`
	HardWatermark         bool   `json:"hard_watermark"`
}

type planDeliveryRuntimeCounters struct {
	ReceivedDatagrams    uint64 `json:"received_datagrams"`
	ReceiveQueueDrops    uint64 `json:"receive_queue_drops"`
	QuarantineQueueDrops uint64 `json:"quarantine_queue_drops"`
	UDPKernelDrops       uint64 `json:"udp_kernel_drops"`
	WALHardStops         uint64 `json:"wal_hard_stops"`
	DecodeFailures       uint64 `json:"decode_failures"`
	PublishFailures      uint64 `json:"publish_failures"`
}

type planDeliveryRuntimeObservation struct {
	Running             bool                        `json:"running"`
	UptimeSeconds       uint64                      `json:"uptime_seconds"`
	PlanAccepting       bool                        `json:"plan_accepting"`
	PlanUsingLKG        bool                        `json:"plan_using_lkg"`
	ControlPlaneHealthy bool                        `json:"control_plane_healthy"`
	KafkaHealthy        bool                        `json:"kafka_healthy"`
	Queues              planDeliveryRuntimeQueues   `json:"queues"`
	WAL                 planDeliveryWALObservation  `json:"wal"`
	Counters            planDeliveryRuntimeCounters `json:"counters"`
}

type PlanDeliveryClient struct {
	config            Config
	collectorID       string
	bootID            string
	softwareVersion   string
	publicKey         []byte
	httpClient        *http.Client
	metrics           *Metrics
	runtime           *RuntimeState
	wal               *WAL
	delivered         chan struct{}
	activated         chan uint64
	failures          chan error
	activeRevision    uint64
	pending           planDeliveryPending
	acknowledged      planDeliveryPending
	etag              string
	heartbeatSequence uint64
	pendingHeartbeat  []byte
	now               func() time.Time
	OnError           func(error)
}

func NewPlanDeliveryClient(config Config, collectorID, bootID, softwareVersion string, activeRevision uint64, metrics *Metrics, runtime *RuntimeState, wal *WAL) (*PlanDeliveryClient, error) {
	if config.ControlPlaneURL == "" || collectorID == "" || len(collectorID) > 26 || bootID == "" || len(bootID) > 64 || !printableToken(bootID) || softwareVersion == "" || len(softwareVersion) > 64 || !printableToken(softwareVersion) || activeRevision == 0 || metrics == nil || runtime == nil || wal == nil {
		return nil, errors.New("remote plan delivery configuration and runtime identity are required")
	}
	var publicKey []byte
	var err error
	if config.PlanPublicKeyFile != "" {
		publicKey, err = readBoundedFile(config.PlanPublicKeyFile, 64<<10)
		if err != nil {
			return nil, fmt.Errorf("read remote plan compatibility key: %w", err)
		}
	}
	if config.PlanTrustBundleFile != "" {
		trustBundle, err := readBoundedFile(config.PlanTrustBundleFile, planTrustBundleMaxBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: read remote plan trust bundle: %v", ErrPlanTrustBundleInvalid, err)
		}
		if _, err := parsePlanTrustBundle(trustBundle); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrPlanTrustBundleInvalid, err)
		}
	}
	httpClient, err := newPlanDeliveryHTTPClient(config)
	if err != nil {
		return nil, err
	}
	client := &PlanDeliveryClient{
		config: config, collectorID: collectorID, bootID: bootID, softwareVersion: softwareVersion,
		activeRevision: activeRevision, publicKey: publicKey, httpClient: httpClient,
		metrics: metrics, runtime: runtime, wal: wal, delivered: make(chan struct{}, 1),
		activated: make(chan uint64, 1), failures: make(chan error, 1), now: time.Now,
	}
	client.restoreLocalDeliveryState()
	client.runtime.enableControlPlane()
	return client, nil
}

func (c *PlanDeliveryClient) Delivered() <-chan struct{} { return c.delivered }

func (c *PlanDeliveryClient) NotifyActivated(registry *Registry) {
	if c == nil || registry == nil || registry.Plan().CollectorID != c.collectorID {
		return
	}
	select {
	case c.activated <- registry.Plan().Revision:
	default:
		select {
		case <-c.activated:
		default:
		}
		select {
		case c.activated <- registry.Plan().Revision:
		default:
		}
	}
}

func (c *PlanDeliveryClient) NotifyRefreshFailure(err error) {
	if c == nil || err == nil {
		return
	}
	select {
	case c.failures <- err:
	default:
	}
}

func (c *PlanDeliveryClient) Run(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errors.New("remote plan delivery client is required")
	}
	planTimer := time.NewTimer(0)
	heartbeatTimer := time.NewTimer(0)
	defer planTimer.Stop()
	defer heartbeatTimer.Stop()
	planFailureCount := 0
	heartbeatFailureCount := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case revision := <-c.activated:
			c.activeRevision = revision
			if !c.planNeedsAcknowledgement() {
				continue
			}
			err := c.acknowledge(ctx)
			c.observe(err)
			if err != nil {
				planFailureCount++
				resetPlanDeliveryTimer(planTimer, c.retryDelay(planFailureCount))
			}
		case refreshErr := <-c.failures:
			failure := classifyPlanRefreshFailure(refreshErr, c.pending.ConfigVersion)
			_ = c.reportFailure(ctx, failure)
			c.observe(failure)
		case <-planTimer.C:
			err := c.sync(ctx)
			c.observe(err)
			if err == nil {
				planFailureCount = 0
				resetPlanDeliveryTimer(planTimer, c.config.PlanRefreshInterval)
			} else {
				planFailureCount++
				resetPlanDeliveryTimer(planTimer, c.retryDelay(planFailureCount))
			}
		case <-heartbeatTimer.C:
			err := c.reportRuntime(ctx)
			if err != nil && c.OnError != nil {
				c.OnError(err)
			}
			var failure planDeliveryFailure
			if errors.As(err, &failure) && failure.Code == "HEARTBEAT_FENCED" {
				return failure
			}
			if err == nil {
				heartbeatFailureCount = 0
				resetPlanDeliveryTimer(heartbeatTimer, c.config.HeartbeatInterval)
			} else {
				heartbeatFailureCount++
				resetPlanDeliveryTimer(heartbeatTimer, c.retryDelay(heartbeatFailureCount))
			}
		}
	}
}

func (c *PlanDeliveryClient) sync(ctx context.Context) error {
	if c.planNeedsAcknowledgement() {
		if err := c.acknowledge(ctx); err != nil {
			return err
		}
	}
	return c.fetch(ctx)
}

func (c *PlanDeliveryClient) fetch(ctx context.Context) error {
	request, err := c.newRequest(ctx, http.MethodGet, "plan", nil)
	if err != nil {
		return planDeliveryFailure{Stage: "transport", Code: "REQUEST_BUILD_FAILED", Detail: "plan request could not be created", Err: err}
	}
	request.Header.Set("Accept", "application/json")
	if c.etag != "" {
		request.Header.Set("If-None-Match", c.etag)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		c.metrics.PlanDeliveryFetchFailures.Add(1)
		return planDeliveryFailure{Stage: "transport", Code: "CONTROL_PLANE_UNAVAILABLE", Detail: "control plane request failed", Err: err}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotModified {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		c.metrics.PlanDeliveryNotModified.Add(1)
		return nil
	}
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		c.metrics.PlanDeliveryFetchFailures.Add(1)
		return planDeliveryStatusFailure("fetch", response.StatusCode, c.pending.ConfigVersion)
	}
	envelope, err := io.ReadAll(io.LimitReader(response.Body, planDeliveryResponseMaxBytes+1))
	if err != nil {
		c.metrics.PlanDeliveryFetchFailures.Add(1)
		return planDeliveryFailure{Stage: "transport", Code: "RESPONSE_READ_FAILED", Detail: "plan response could not be read", Err: err}
	}
	if len(envelope) == 0 || len(envelope) > planDeliveryResponseMaxBytes {
		c.metrics.PlanDeliveryFetchFailures.Add(1)
		return planDeliveryFailure{Stage: "decode", Code: "RESPONSE_SIZE_INVALID", Detail: "plan response size is invalid"}
	}
	registry, metadata, err := c.verifyPlan(envelope, planTrustDelivery)
	if err != nil {
		failure := classifyDeliveredPlanVerificationFailure(err)
		c.metrics.PlanDeliveryFetchFailures.Add(1)
		_ = c.reportFailure(ctx, failure)
		return failure
	}
	version, versionErr := strconv.ParseUint(response.Header.Get("X-Watchdog-Plan-Version"), 10, 64)
	expectedETag := planDeliveryETag(metadata.ConfigVersion, metadata.SpecHash)
	if registry.Plan().CollectorID != c.collectorID || metadata.CollectorID != c.collectorID || versionErr != nil || version != metadata.ConfigVersion || response.Header.Get("ETag") != expectedETag {
		failure := planDeliveryFailure{Stage: "verify", Code: "DELIVERY_METADATA_MISMATCH", Detail: "plan response headers do not match signed metadata", ConfigVersion: metadata.ConfigVersion}
		c.metrics.PlanDeliveryFetchFailures.Add(1)
		_ = c.reportFailure(ctx, failure)
		return failure
	}
	if metadata.ConfigVersion < c.activeRevision {
		failure := planDeliveryFailure{Stage: "compatibility", Code: "PLAN_REVISION_ROLLBACK", Detail: "downloaded plan revision is older than the active revision", ConfigVersion: metadata.ConfigVersion}
		c.metrics.PlanDeliveryFetchFailures.Add(1)
		_ = c.reportFailure(ctx, failure)
		return failure
	}
	if err := writeDeliveredPlan(c.config.PlanFile, envelope); err != nil {
		failure := planDeliveryFailure{Stage: "persist", Code: "PLAN_FILE_PERSIST_FAILED", Detail: "downloaded plan could not be made durable", ConfigVersion: metadata.ConfigVersion, Err: err}
		c.metrics.PlanDeliveryPersistFailures.Add(1)
		_ = c.reportFailure(ctx, failure)
		return failure
	}
	c.pending = planDeliveryPending{ConfigVersion: metadata.ConfigVersion, SpecHash: metadata.SpecHash}
	c.etag = expectedETag
	c.metrics.PlanDeliveryFetchSuccesses.Add(1)
	select {
	case c.delivered <- struct{}{}:
	default:
	}
	return nil
}

func (c *PlanDeliveryClient) acknowledge(ctx context.Context) error {
	body, err := json.Marshal(struct {
		ConfigVersion   uint64 `json:"config_version"`
		SpecHash        string `json:"spec_hash"`
		BootID          string `json:"boot_id"`
		SoftwareVersion string `json:"software_version"`
	}{c.pending.ConfigVersion, c.pending.SpecHash, c.bootID, c.softwareVersion})
	if err != nil {
		return err
	}
	request, err := c.newRequest(ctx, http.MethodPost, "plan-ack", body)
	if err != nil {
		return err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		c.metrics.PlanDeliveryAckFailures.Add(1)
		return planDeliveryFailure{Stage: "transport", Code: "CONTROL_PLANE_UNAVAILABLE", Detail: "plan acknowledgement request failed", ConfigVersion: c.pending.ConfigVersion, Err: err}
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode == http.StatusConflict {
		c.pending = planDeliveryPending{}
		return nil
	}
	if response.StatusCode != http.StatusAccepted {
		c.metrics.PlanDeliveryAckFailures.Add(1)
		return planDeliveryStatusFailure("acknowledge", response.StatusCode, c.pending.ConfigVersion)
	}
	c.acknowledged = c.pending
	c.metrics.PlanDeliveryAckSuccesses.Add(1)
	return nil
}

func (c *PlanDeliveryClient) reportFailure(ctx context.Context, failure planDeliveryFailure) error {
	body, err := json.Marshal(planDeliveryHeartbeatRequest{SchemaVersion: 1, Kind: "plan_failure", PlanFailure: &planDeliveryFailureHeartbeatBody{
		FailedConfigVersion: failure.ConfigVersion, BootID: c.bootID, SoftwareVersion: c.softwareVersion,
		Stage: failure.Stage, Code: failure.Code, Detail: failure.Detail,
	}})
	if err != nil {
		return err
	}
	request, err := c.newRequest(ctx, http.MethodPost, "heartbeat", body)
	if err != nil {
		return err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusAccepted {
		return planDeliveryStatusFailure("report failure", response.StatusCode, failure.ConfigVersion)
	}
	return nil
}

func (c *PlanDeliveryClient) reportRuntime(ctx context.Context) error {
	if len(c.pendingHeartbeat) == 0 {
		c.heartbeatSequence++
		heartbeat := c.runtimeHeartbeat(c.heartbeatSequence, c.now())
		body, err := json.Marshal(planDeliveryHeartbeatRequest{SchemaVersion: 1, Kind: "runtime", Runtime: &heartbeat})
		if err != nil {
			c.metrics.HeartbeatFailures.Add(1)
			return err
		}
		c.pendingHeartbeat = body
	}
	request, err := c.newRequest(ctx, http.MethodPost, "heartbeat", c.pendingHeartbeat)
	if err != nil {
		c.metrics.HeartbeatFailures.Add(1)
		return err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		c.metrics.HeartbeatFailures.Add(1)
		return planDeliveryFailure{Stage: "transport", Code: "CONTROL_PLANE_UNAVAILABLE", Detail: "runtime heartbeat request failed", ConfigVersion: c.activeRevision, Err: err}
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode == http.StatusPreconditionFailed {
		c.metrics.HeartbeatFailures.Add(1)
		return planDeliveryFailure{Stage: "transport", Code: "HEARTBEAT_FENCED", Detail: "collector process incarnation was fenced by the control plane", ConfigVersion: c.activeRevision}
	}
	if response.StatusCode == http.StatusConflict {
		c.pendingHeartbeat = nil
		c.heartbeatSequence--
		c.metrics.HeartbeatFailures.Add(1)
		return planDeliveryFailure{Stage: "compatibility", Code: "HEARTBEAT_STATE_CONFLICT", Detail: "runtime state no longer matches the active or last-good plan", ConfigVersion: c.activeRevision}
	}
	if response.StatusCode != http.StatusAccepted {
		c.metrics.HeartbeatFailures.Add(1)
		return planDeliveryStatusFailure("report runtime heartbeat", response.StatusCode, c.activeRevision)
	}
	c.pendingHeartbeat = nil
	c.metrics.HeartbeatSuccesses.Add(1)
	return nil
}

func (c *PlanDeliveryClient) runtimeHeartbeat(sequence uint64, now time.Time) planDeliveryRuntimeHeartbeat {
	runtime := c.runtime.Snapshot()
	wal := c.wal.State()
	kafkaHealthy := true
	for _, component := range runtime.Kafka {
		kafkaHealthy = kafkaHealthy && component.Healthy
	}
	uptime := uint64(0)
	if runtime.Running && runtime.StartedAt > 0 && now.Unix() > runtime.StartedAt {
		uptime = uint64(now.Unix() - runtime.StartedAt)
	}
	return planDeliveryRuntimeHeartbeat{
		SchemaVersion: 1, Sequence: sequence, SentAtUnixMilli: now.UTC().UnixMilli(),
		BootID: c.bootID, SoftwareVersion: c.softwareVersion, AgentAPIVersion: 1,
		PlanSchemaMin: 1, PlanSchemaMax: 2, ActiveConfigVersion: c.activeRevision,
		ActiveSpecHash: c.activeSpecHash(),
		Capabilities: planDeliveryRuntimeCapabilities{
			SchemaVersion: 1, Protocols: []string{"ipfix", "netflow5", "netflow9", "sflow5"},
			PlanEnvelopeVersions: []uint16{1, 2},
		},
		Observation: planDeliveryRuntimeObservation{
			Running: runtime.Running, UptimeSeconds: uptime, PlanAccepting: runtime.Plan.Accepting,
			PlanUsingLKG: runtime.Plan.UsedLKG, ControlPlaneHealthy: runtime.ControlPlane.Healthy,
			KafkaHealthy: kafkaHealthy,
			Queues: planDeliveryRuntimeQueues{
				Receive:    planDeliveryQueueObservation{Depth: nonNegativeRuntimeValue(c.metrics.ReceiveQueueDepth.Load()), Capacity: nonNegativeRuntimeValue(c.metrics.ReceiveQueueCapacity.Load())},
				Decode:     planDeliveryQueueObservation{Depth: nonNegativeRuntimeValue(c.metrics.DecodeQueueDepth.Load()), Capacity: nonNegativeRuntimeValue(c.metrics.DecodeQueueCapacity.Load())},
				Quarantine: planDeliveryQueueObservation{Depth: nonNegativeRuntimeValue(c.metrics.QuarantineQueueDepth.Load()), Capacity: nonNegativeRuntimeValue(c.metrics.QuarantineQueueCapacity.Load())},
			},
			WAL: planDeliveryWALObservation{
				Bytes: nonNegativeRuntimeValue(wal.Bytes), MaxBytes: nonNegativeRuntimeValue(wal.MaxBytes),
				OldestAgeMilliseconds: nonNegativeRuntimeValue(wal.OldestAge.Milliseconds()),
				Writable:              wal.Writable, SoftWatermark: wal.SoftWatermark, HardWatermark: wal.HardWatermark,
			},
			Counters: planDeliveryRuntimeCounters{
				ReceivedDatagrams: c.metrics.ReceivedDatagrams.Load(), ReceiveQueueDrops: c.metrics.ReceiveQueueDrops.Load(),
				QuarantineQueueDrops: c.metrics.QuarantineQueueDrops.Load(),
				UDPKernelDrops:       c.metrics.UDPKernelDropsSFlow.Load() + c.metrics.UDPKernelDropsNetFlow.Load(),
				WALHardStops:         c.metrics.WALHardStops.Load(), DecodeFailures: c.metrics.DecodeFailures.Load(),
				PublishFailures: c.metrics.PublishFailures.Load() + c.metrics.QuarantinePublishFailures.Load() +
					c.metrics.DLQPublishFailures.Load() + c.metrics.CollectStateFailures.Load() + c.metrics.QualityCheckpointFailures.Load(),
			},
		},
	}
}

func (c *PlanDeliveryClient) activeSpecHash() string {
	if c.pending.ConfigVersion == c.activeRevision {
		return c.pending.SpecHash
	}
	if c.acknowledged.ConfigVersion == c.activeRevision {
		return c.acknowledged.SpecHash
	}
	return ""
}

func nonNegativeRuntimeValue(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

func (c *PlanDeliveryClient) newRequest(ctx context.Context, method, action string, body []byte) (*http.Request, error) {
	endpoint := c.config.ControlPlaneURL + "/api/v1/collectors/" + url.PathEscape(c.collectorID) + "/" + action
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("User-Agent", "watchdog-flow-collect/"+c.softwareVersion)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.config.ControlPlaneTokenFile != "" {
		tokenData, err := readBoundedFile(c.config.ControlPlaneTokenFile, 4096)
		if err != nil {
			return nil, fmt.Errorf("read control plane token: %w", err)
		}
		token := strings.TrimSpace(string(tokenData))
		if token == "" || len(token) > 256 || !printableToken(token) {
			return nil, errors.New("control plane token is invalid")
		}
		request.Header.Set("X-Watchdog-Agent-Token", token)
	}
	return request, nil
}

func (c *PlanDeliveryClient) planNeedsAcknowledgement() bool {
	return c.pending.ConfigVersion != 0 && c.pending.ConfigVersion == c.activeRevision && c.pending != c.acknowledged
}

func (c *PlanDeliveryClient) restoreLocalDeliveryState() {
	envelope, err := readBoundedFile(c.config.PlanFile, signedPlanMaxBytes)
	if err != nil {
		return
	}
	registry, metadata, err := c.verifyPlan(envelope, planTrustExisting)
	if err != nil || registry.Plan().CollectorID != c.collectorID || metadata.ConfigVersion != c.activeRevision {
		return
	}
	c.pending = planDeliveryPending{ConfigVersion: metadata.ConfigVersion, SpecHash: metadata.SpecHash}
	c.etag = planDeliveryETag(metadata.ConfigVersion, metadata.SpecHash)
}

func (c *PlanDeliveryClient) verifyPlan(envelope []byte, use planTrustUse) (*Registry, PlanSignatureMetadata, error) {
	var trustBundle []byte
	if c.config.PlanTrustBundleFile != "" {
		var err error
		trustBundle, err = readBoundedFile(c.config.PlanTrustBundleFile, planTrustBundleMaxBytes)
		if err != nil {
			return nil, PlanSignatureMetadata{}, fmt.Errorf("%w: read remote plan trust bundle: %v", ErrPlanTrustBundleInvalid, err)
		}
		if _, err := parsePlanTrustBundle(trustBundle); err != nil {
			return nil, PlanSignatureMetadata{}, fmt.Errorf("%w: %v", ErrPlanTrustBundleInvalid, err)
		}
	}
	now := c.now()
	verified, err := verifySignedPlanPayloadWithTrust(envelope, c.publicKey, trustBundle, use, now)
	if err != nil {
		return nil, PlanSignatureMetadata{}, err
	}
	if verified.envelopeVersion != 2 {
		return nil, PlanSignatureMetadata{}, errors.New("remote flow plan must use control-plane envelope version 2")
	}
	registry, err := CompilePlan(verified.plan, now)
	if err != nil {
		return nil, PlanSignatureMetadata{}, err
	}
	return registry, verified.metadata, nil
}

func classifyDeliveredPlanVerificationFailure(err error) planDeliveryFailure {
	failure := planDeliveryFailure{Stage: "verify", Code: "PLAN_SIGNATURE_INVALID", Detail: "downloaded plan signature or metadata is invalid", Err: err}
	switch {
	case errors.Is(err, ErrPlanTrustBundleInvalid):
		failure.Code, failure.Detail = "PLAN_TRUST_BUNDLE_INVALID", "plan trust bundle is unavailable or invalid"
	case errors.Is(err, ErrPlanSigningKeyUnknown):
		failure.Code, failure.Detail = "PLAN_SIGNING_KEY_UNKNOWN", "downloaded plan signing key is not trusted"
	case errors.Is(err, ErrPlanSigningKeyRevoked):
		failure.Code, failure.Detail = "PLAN_SIGNING_KEY_REVOKED", "downloaded plan signing key is revoked"
	case errors.Is(err, ErrPlanSigningKeyNotAcceptable):
		failure.Code, failure.Detail = "PLAN_SIGNING_KEY_NOT_ACCEPTED", "downloaded plan signing key is outside its acceptance policy"
	}
	return failure
}

func (c *PlanDeliveryClient) observe(err error) {
	c.runtime.observeControlPlane(err, c.now())
	if err != nil && c.OnError != nil {
		c.OnError(err)
	}
}

func (c *PlanDeliveryClient) retryDelay(failures int) time.Duration {
	delay := c.config.ControlPlaneRetryMin
	for attempt := 1; attempt < failures && delay < c.config.ControlPlaneRetryMax; attempt++ {
		if delay > c.config.ControlPlaneRetryMax/2 {
			return c.config.ControlPlaneRetryMax
		}
		delay *= 2
	}
	if delay > c.config.ControlPlaneRetryMax {
		return c.config.ControlPlaneRetryMax
	}
	return delay
}

func newPlanDeliveryHTTPClient(config Config) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: config.ControlPlaneTLSServerName}
	if config.ControlPlaneTLSCAFile != "" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		ca, err := readBoundedFile(config.ControlPlaneTLSCAFile, 4<<20)
		if err != nil {
			return nil, fmt.Errorf("read control plane CA: %w", err)
		}
		if !roots.AppendCertsFromPEM(ca) {
			return nil, errors.New("control plane CA contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if config.ControlPlaneTLSCertFile != "" {
		_, err := tls.LoadX509KeyPair(config.ControlPlaneTLSCertFile, config.ControlPlaneTLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load control plane client identity: %w", err)
		}
		tlsConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			certificate, err := tls.LoadX509KeyPair(config.ControlPlaneTLSCertFile, config.ControlPlaneTLSKeyFile)
			if err != nil {
				return nil, fmt.Errorf("reload control plane client identity: %w", err)
			}
			return &certificate, nil
		}
		// A reused TLS connection would keep presenting the previous certificate.
		// Heartbeats are low-frequency, so one handshake per request is the
		// bounded cost of making external certificate rotation deterministic.
		transport.DisableKeepAlives = true
	}
	transport.TLSClientConfig = tlsConfig
	return &http.Client{
		Transport: transport,
		Timeout:   config.ControlPlaneRequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("control plane redirects are not allowed")
		},
	}, nil
}

func writeDeliveredPlan(path string, envelope []byte) error {
	if len(envelope) == 0 || len(envelope) > signedPlanMaxBytes {
		return errors.New("delivered plan size is invalid")
	}
	directoryPath := filepath.Dir(path)
	temporary, err := os.CreateTemp(directoryPath, ".plan-delivery-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(envelope); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directory, err := os.Open(directoryPath)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func classifyPlanRefreshFailure(err error, configVersion uint64) planDeliveryFailure {
	var refreshFailure planRefreshFailure
	if errors.As(err, &refreshFailure) {
		return planDeliveryFailure{Stage: refreshFailure.stage, Code: refreshFailure.code, Detail: refreshFailure.detail, ConfigVersion: configVersion, Err: err}
	}
	return planDeliveryFailure{Stage: "compatibility", Code: "PLAN_ACTIVATION_REJECTED", Detail: "plan could not be activated by this collector", ConfigVersion: configVersion, Err: err}
}

func planDeliveryStatusFailure(operation string, status int, configVersion uint64) planDeliveryFailure {
	code, detail := "CONTROL_PLANE_RESPONSE_INVALID", "control plane returned an unexpected response"
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		code, detail = "AUTHENTICATION_REJECTED", "collector credentials were rejected"
	case http.StatusConflict, http.StatusNotFound:
		code, detail = "PLAN_UNAVAILABLE", "active plan is unavailable or no longer applicable"
	case http.StatusTooManyRequests:
		code, detail = "CONTROL_PLANE_THROTTLED", "control plane throttled the collector"
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		code, detail = "CONTROL_PLANE_UNAVAILABLE", "control plane is temporarily unavailable"
	}
	return planDeliveryFailure{Stage: "transport", Code: code, Detail: detail, ConfigVersion: configVersion, Err: fmt.Errorf("%s returned HTTP %d", operation, status)}
}

func planDeliveryETag(configVersion uint64, specHash string) string {
	return fmt.Sprintf(`"v%d-%s"`, configVersion, specHash)
}

func printableToken(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func resetPlanDeliveryTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}
