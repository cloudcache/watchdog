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

type PlanDeliveryClient struct {
	config          Config
	collectorID     string
	bootID          string
	softwareVersion string
	publicKey       []byte
	httpClient      *http.Client
	metrics         *Metrics
	runtime         *RuntimeState
	delivered       chan struct{}
	activated       chan uint64
	failures        chan error
	activeRevision  uint64
	pending         planDeliveryPending
	acknowledged    planDeliveryPending
	etag            string
	now             func() time.Time
	OnError         func(error)
}

func NewPlanDeliveryClient(config Config, collectorID, bootID, softwareVersion string, activeRevision uint64, metrics *Metrics, runtime *RuntimeState) (*PlanDeliveryClient, error) {
	if config.ControlPlaneURL == "" || collectorID == "" || len(collectorID) > 26 || bootID == "" || len(bootID) > 64 || !printableToken(bootID) || len(softwareVersion) > 64 || !printableToken(softwareVersion) || activeRevision == 0 || metrics == nil || runtime == nil {
		return nil, errors.New("remote plan delivery configuration and runtime identity are required")
	}
	publicKey, err := readBoundedFile(config.PlanPublicKeyFile, 64<<10)
	if err != nil {
		return nil, fmt.Errorf("read remote plan trust key: %w", err)
	}
	httpClient, err := newPlanDeliveryHTTPClient(config)
	if err != nil {
		return nil, err
	}
	client := &PlanDeliveryClient{
		config: config, collectorID: collectorID, bootID: bootID, softwareVersion: softwareVersion,
		activeRevision: activeRevision, publicKey: publicKey, httpClient: httpClient,
		metrics: metrics, runtime: runtime, delivered: make(chan struct{}, 1),
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
	timer := time.NewTimer(0)
	defer timer.Stop()
	failureCount := 0
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
				failureCount++
				resetPlanDeliveryTimer(timer, c.retryDelay(failureCount))
			}
		case refreshErr := <-c.failures:
			failure := classifyPlanRefreshFailure(refreshErr, c.pending.ConfigVersion)
			_ = c.reportFailure(ctx, failure)
			c.observe(failure)
		case <-timer.C:
			err := c.sync(ctx)
			c.observe(err)
			if err == nil {
				failureCount = 0
				resetPlanDeliveryTimer(timer, c.config.PlanRefreshInterval)
			} else {
				failureCount++
				resetPlanDeliveryTimer(timer, c.retryDelay(failureCount))
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
	registry, metadata, err := VerifyControlPlaneSignedPlan(envelope, c.publicKey, c.now())
	if err != nil {
		failure := planDeliveryFailure{Stage: "verify", Code: "PLAN_SIGNATURE_INVALID", Detail: "downloaded plan signature or metadata is invalid", Err: err}
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
	body, err := json.Marshal(struct {
		FailedConfigVersion uint64 `json:"failed_config_version"`
		BootID              string `json:"boot_id"`
		SoftwareVersion     string `json:"software_version"`
		Stage               string `json:"stage"`
		Code                string `json:"code"`
		Detail              string `json:"detail"`
	}{failure.ConfigVersion, c.bootID, c.softwareVersion, failure.Stage, failure.Code, failure.Detail})
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
	registry, metadata, err := VerifyControlPlaneSignedPlan(envelope, c.publicKey, c.now())
	if err != nil || registry.Plan().CollectorID != c.collectorID || metadata.ConfigVersion != c.activeRevision {
		return
	}
	c.pending = planDeliveryPending{ConfigVersion: metadata.ConfigVersion, SpecHash: metadata.SpecHash}
	c.etag = planDeliveryETag(metadata.ConfigVersion, metadata.SpecHash)
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
		certificate, err := tls.LoadX509KeyPair(config.ControlPlaneTLSCertFile, config.ControlPlaneTLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load control plane client identity: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
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
