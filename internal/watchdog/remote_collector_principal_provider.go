package watchdog

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrCollectorPrincipalProviderCircuitOpen = errors.New("collector principal provider circuit is open")

const collectorPrincipalProviderResponseOverhead = 16 << 10
const collectorPrincipalProviderRetentionHeader = "X-Watchdog-Operation-Retention-Seconds"

type remoteCollectorPrincipalProvider struct {
	config RemoteCollectorPrincipalProviderConfig
	client *http.Client
	now    func() time.Time

	breakerMu           sync.Mutex
	consecutiveFailure  uint32
	openUntil           time.Time
	halfOpenProbe       bool
	requestSuccessTotal uint64
	requestFailureTotal uint64
	remoteRejectTotal   uint64
	circuitRejectTotal  uint64
	lastSuccessAt       time.Time
	lastFailureAt       time.Time
	lastRemoteRejectAt  time.Time
}

type CollectorPrincipalProviderRuntimeHealth struct {
	AcceptingRequests   bool      `json:"accepting_requests"`
	CircuitOpen         bool      `json:"circuit_open"`
	HalfOpenProbe       bool      `json:"half_open_probe"`
	ConsecutiveFailures uint32    `json:"consecutive_failures"`
	RequestSuccessTotal uint64    `json:"request_success_total"`
	RequestFailureTotal uint64    `json:"request_failure_total"`
	RemoteRejectTotal   uint64    `json:"remote_reject_total"`
	CircuitRejectTotal  uint64    `json:"circuit_reject_total"`
	LastSuccessAt       time.Time `json:"last_success_at,omitempty"`
	LastFailureAt       time.Time `json:"last_failure_at,omitempty"`
	LastRemoteRejectAt  time.Time `json:"last_remote_reject_at,omitempty"`
}

type CollectorPrincipalProviderRuntimeStatus struct {
	Enabled bool                                    `json:"enabled"`
	Health  CollectorPrincipalProviderRuntimeHealth `json:"health"`
}

func boolMetricValue(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

type remoteCollectorPrincipalGrantRequest struct {
	OperationKey        string `json:"operation_key"`
	RequestHash         string `json:"request_hash"`
	PolicyRevision      string `json:"policy_revision"`
	PrincipalID         ID     `json:"principal_id"`
	TenantID            ID     `json:"tenant_id"`
	CollectorID         ID     `json:"collector_id"`
	ServiceType         string `json:"service_type"`
	ACLPropagationDelay uint64 `json:"acl_propagation_delay_ms"`
}

type remoteCollectorPrincipalRevokeRequest struct {
	OperationKey   string `json:"operation_key"`
	RequestHash    string `json:"request_hash"`
	PolicyRevision string `json:"policy_revision"`
	TenantID       ID     `json:"tenant_id"`
	CollectorID    ID     `json:"collector_id"`
	PrincipalID    ID     `json:"principal_id"`
	ServiceType    string `json:"service_type"`
	PrincipalRef   string `json:"principal_ref"`
}

type remoteCollectorPrincipalGrantResponse struct {
	OperationKey        string          `json:"operation_key"`
	RequestHash         string          `json:"request_hash"`
	Provider            string          `json:"provider"`
	PrincipalRef        string          `json:"principal_ref"`
	CredentialSecretRef string          `json:"credential_secret_ref"`
	ReceiptRef          string          `json:"receipt_ref"`
	Receipt             json.RawMessage `json:"receipt"`
}

type remoteCollectorPrincipalRevokeResponse struct {
	OperationKey string          `json:"operation_key"`
	RequestHash  string          `json:"request_hash"`
	Provider     string          `json:"provider"`
	PrincipalRef string          `json:"principal_ref"`
	ReceiptRef   string          `json:"receipt_ref"`
	Receipt      json.RawMessage `json:"receipt"`
}

func NewRemoteCollectorPrincipalProvider(config RemoteCollectorPrincipalProviderConfig) (CollectorPrincipalProvider, error) {
	normalizeRemoteCollectorPrincipalProviderConfig(&config)
	if err := validateRemoteCollectorPrincipalProviderConfig(config); err != nil {
		return nil, err
	}
	client, err := newRemoteCollectorPrincipalHTTPClient(config)
	if err != nil {
		return nil, err
	}
	return newRemoteCollectorPrincipalProviderWithClient(config, client, time.Now)
}

func newRemoteCollectorPrincipalProviderWithClient(config RemoteCollectorPrincipalProviderConfig, client *http.Client, now func() time.Time) (*remoteCollectorPrincipalProvider, error) {
	normalizeRemoteCollectorPrincipalProviderConfig(&config)
	if err := validateRemoteCollectorPrincipalProviderConfig(config); err != nil {
		return nil, err
	}
	if client == nil || now == nil {
		return nil, errors.New("collector principal provider HTTP client and clock are required")
	}
	return &remoteCollectorPrincipalProvider{config: config, client: client, now: now}, nil
}

func (p *remoteCollectorPrincipalProvider) PolicyRevision() string {
	return p.config.PolicyRevision
}

func (p *remoteCollectorPrincipalProvider) LookupGrant(ctx context.Context, operationKey string) (CollectorPrincipalGrantProviderResult, bool, error) {
	var response remoteCollectorPrincipalGrantResponse
	found, err := p.request(ctx, http.MethodGet, "grants", operationKey, nil, &response, true)
	if err != nil || !found {
		return CollectorPrincipalGrantProviderResult{}, found, err
	}
	result := response.grantResult()
	if err := validateCollectorPrincipalGrantProviderResult(result, p.config.Name, operationKey, result.RequestHash); err != nil || !remoteProviderReceiptIsObject(result.Receipt) {
		p.recordFailure()
		return CollectorPrincipalGrantProviderResult{}, false, ErrCollectorPrincipalProviderResultInvalid
	}
	p.recordSuccess()
	return result, true, nil
}

func (p *remoteCollectorPrincipalProvider) Grant(ctx context.Context, request CollectorPrincipalGrantProviderRequest) (CollectorPrincipalGrantProviderResult, error) {
	if request.PolicyRevision != p.config.PolicyRevision || !validSHA256Hex(request.OperationKey) || !validSHA256Hex(request.RequestHash) || !validCollectorEvidenceID(request.PrincipalID) || !validCollectorEvidenceID(request.TenantID) || !validCollectorEvidenceID(request.CollectorID) || request.ServiceType != "kafka" || !validEvidenceDuration(request.ACLPropagationDelay) {
		return CollectorPrincipalGrantProviderResult{}, errors.New("remote collector principal grant request is invalid")
	}
	body := remoteCollectorPrincipalGrantRequest{
		OperationKey: request.OperationKey, RequestHash: request.RequestHash,
		PolicyRevision: request.PolicyRevision, PrincipalID: request.PrincipalID,
		TenantID: request.TenantID, CollectorID: request.CollectorID,
		ServiceType:         request.ServiceType,
		ACLPropagationDelay: uint64(request.ACLPropagationDelay / time.Millisecond),
	}
	var response remoteCollectorPrincipalGrantResponse
	if _, err := p.request(ctx, http.MethodPut, "grants", request.OperationKey, body, &response, false); err != nil {
		return CollectorPrincipalGrantProviderResult{}, err
	}
	result := response.grantResult()
	if err := validateCollectorPrincipalGrantProviderResult(result, p.config.Name, request.OperationKey, request.RequestHash); err != nil || !remoteProviderReceiptIsObject(result.Receipt) {
		p.recordFailure()
		return CollectorPrincipalGrantProviderResult{}, ErrCollectorPrincipalProviderResultInvalid
	}
	p.recordSuccess()
	return result, nil
}

func (p *remoteCollectorPrincipalProvider) LookupRevoke(ctx context.Context, operationKey string) (CollectorPrincipalRevokeProviderResult, bool, error) {
	var response remoteCollectorPrincipalRevokeResponse
	found, err := p.request(ctx, http.MethodGet, "revocations", operationKey, nil, &response, true)
	if err != nil || !found {
		return CollectorPrincipalRevokeProviderResult{}, found, err
	}
	result := response.revokeResult()
	if err := validateCollectorPrincipalRevokeProviderResult(result, p.config.Name, result.PrincipalRef, operationKey, result.RequestHash); err != nil || !remoteProviderReceiptIsObject(result.Receipt) {
		p.recordFailure()
		return CollectorPrincipalRevokeProviderResult{}, false, ErrCollectorPrincipalProviderResultInvalid
	}
	p.recordSuccess()
	return result, true, nil
}

func (p *remoteCollectorPrincipalProvider) RevokeWrite(ctx context.Context, request CollectorPrincipalRevokeProviderRequest) (CollectorPrincipalRevokeProviderResult, error) {
	if request.PolicyRevision != p.config.PolicyRevision || !validSHA256Hex(request.OperationKey) || !validSHA256Hex(request.RequestHash) || !validCollectorEvidenceID(request.TenantID) || !validCollectorEvidenceID(request.CollectorID) || !validCollectorEvidenceID(request.PrincipalID) || request.ServiceType != "kafka" || request.PrincipalRef == "" || len(request.PrincipalRef) > 190 || !isPrintableASCII(request.PrincipalRef) {
		return CollectorPrincipalRevokeProviderResult{}, errors.New("remote collector principal revocation request is invalid")
	}
	body := remoteCollectorPrincipalRevokeRequest{
		OperationKey: request.OperationKey, RequestHash: request.RequestHash,
		PolicyRevision: request.PolicyRevision, TenantID: request.TenantID,
		CollectorID: request.CollectorID, PrincipalID: request.PrincipalID,
		ServiceType: request.ServiceType, PrincipalRef: request.PrincipalRef,
	}
	var response remoteCollectorPrincipalRevokeResponse
	if _, err := p.request(ctx, http.MethodPut, "revocations", request.OperationKey, body, &response, false); err != nil {
		return CollectorPrincipalRevokeProviderResult{}, err
	}
	result := response.revokeResult()
	if err := validateCollectorPrincipalRevokeProviderResult(result, p.config.Name, request.PrincipalRef, request.OperationKey, request.RequestHash); err != nil || !remoteProviderReceiptIsObject(result.Receipt) {
		p.recordFailure()
		return CollectorPrincipalRevokeProviderResult{}, ErrCollectorPrincipalProviderResultInvalid
	}
	p.recordSuccess()
	return result, nil
}

func (p *remoteCollectorPrincipalProvider) request(ctx context.Context, method, operationType, operationKey string, body, response any, lookup bool) (bool, error) {
	if p == nil || p.client == nil || ctx == nil || !validSHA256Hex(operationKey) {
		return false, errors.New("remote collector principal provider request is invalid")
	}
	var requestBody io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return false, err
		}
		requestBody = bytes.NewReader(payload)
	}
	requestURL := p.config.BaseURL + "/v1/collector-principal-operations/" + operationType + "/" + operationKey
	req, err := http.NewRequestWithContext(ctx, method, requestURL, requestBody)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Watchdog-Provider-Revision", p.config.PolicyRevision)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if err := p.allowRequest(); err != nil {
		return false, err
	}
	result, err := p.client.Do(req)
	if err != nil {
		p.recordFailure()
		return false, errors.New("collector principal provider request failed")
	}
	defer result.Body.Close()
	if result.StatusCode != http.StatusTooManyRequests && result.StatusCode < http.StatusInternalServerError {
		if err := p.verifyOperationRetention(result.Header); err != nil {
			p.recordFailure()
			return false, err
		}
	}
	if lookup && result.StatusCode == http.StatusNotFound {
		drainCollectorPrincipalProviderResponse(result.Body)
		p.recordSuccess()
		return false, nil
	}
	if result.StatusCode != http.StatusOK {
		drainCollectorPrincipalProviderResponse(result.Body)
		if result.StatusCode == http.StatusTooManyRequests || result.StatusCode >= http.StatusInternalServerError {
			p.recordFailure()
		} else {
			p.recordRemoteReject()
		}
		return false, fmt.Errorf("collector principal provider returned HTTP %d", result.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(result.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		p.recordFailure()
		return false, errors.New("collector principal provider returned a non-JSON response")
	}
	decoder := json.NewDecoder(io.LimitReader(result.Body, collectorEvidenceMaxReceiptBytes+collectorPrincipalProviderResponseOverhead+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(response); err != nil {
		p.recordFailure()
		return false, errors.New("collector principal provider returned an invalid response")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		p.recordFailure()
		return false, errors.New("collector principal provider returned more than one JSON value")
	}
	return true, nil
}

func (p *remoteCollectorPrincipalProvider) verifyOperationRetention(header http.Header) error {
	seconds, err := strconv.ParseUint(strings.TrimSpace(header.Get(collectorPrincipalProviderRetentionHeader)), 10, 64)
	if err != nil || seconds < uint64(p.config.MinOperationRetention/time.Second) {
		return errors.New("collector principal provider operation retention is below the required recovery window")
	}
	return nil
}

func drainCollectorPrincipalProviderResponse(body io.Reader) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 4<<10))
}

func (p *remoteCollectorPrincipalProvider) allowRequest() error {
	p.breakerMu.Lock()
	defer p.breakerMu.Unlock()
	now := p.now()
	if !p.openUntil.IsZero() && now.Before(p.openUntil) {
		p.circuitRejectTotal++
		return ErrCollectorPrincipalProviderCircuitOpen
	}
	if !p.openUntil.IsZero() {
		if p.halfOpenProbe {
			p.circuitRejectTotal++
			return ErrCollectorPrincipalProviderCircuitOpen
		}
		p.halfOpenProbe = true
	}
	return nil
}

func (p *remoteCollectorPrincipalProvider) recordSuccess() {
	p.breakerMu.Lock()
	p.consecutiveFailure = 0
	p.openUntil = time.Time{}
	p.halfOpenProbe = false
	p.requestSuccessTotal++
	p.lastSuccessAt = p.now().UTC()
	p.breakerMu.Unlock()
}

func (p *remoteCollectorPrincipalProvider) recordRemoteReject() {
	p.breakerMu.Lock()
	p.consecutiveFailure = 0
	p.openUntil = time.Time{}
	p.halfOpenProbe = false
	p.remoteRejectTotal++
	p.lastRemoteRejectAt = p.now().UTC()
	p.breakerMu.Unlock()
}

func (p *remoteCollectorPrincipalProvider) recordFailure() {
	p.breakerMu.Lock()
	p.consecutiveFailure++
	p.halfOpenProbe = false
	p.requestFailureTotal++
	p.lastFailureAt = p.now().UTC()
	if p.consecutiveFailure >= p.config.FailureThreshold {
		p.openUntil = p.now().Add(p.config.CircuitOpenInterval)
	}
	p.breakerMu.Unlock()
}

func (p *remoteCollectorPrincipalProvider) Health() CollectorPrincipalProviderRuntimeHealth {
	if p == nil {
		return CollectorPrincipalProviderRuntimeHealth{}
	}
	p.breakerMu.Lock()
	defer p.breakerMu.Unlock()
	circuitOpen := !p.openUntil.IsZero() && p.now().Before(p.openUntil)
	return CollectorPrincipalProviderRuntimeHealth{
		AcceptingRequests: !circuitOpen && !p.halfOpenProbe,
		CircuitOpen:       circuitOpen, HalfOpenProbe: p.halfOpenProbe,
		ConsecutiveFailures: p.consecutiveFailure,
		RequestSuccessTotal: p.requestSuccessTotal, RequestFailureTotal: p.requestFailureTotal,
		RemoteRejectTotal: p.remoteRejectTotal, CircuitRejectTotal: p.circuitRejectTotal,
		LastSuccessAt: p.lastSuccessAt, LastFailureAt: p.lastFailureAt,
		LastRemoteRejectAt: p.lastRemoteRejectAt,
	}
}

func (p *remoteCollectorPrincipalProvider) PrometheusText() []byte {
	health := p.Health()
	var out strings.Builder
	out.WriteString("# TYPE watchdog_collector_principal_provider_accepting_requests gauge\n")
	out.WriteString("watchdog_collector_principal_provider_accepting_requests ")
	out.WriteString(boolMetricValue(health.AcceptingRequests))
	out.WriteByte('\n')
	out.WriteString("# TYPE watchdog_collector_principal_provider_circuit_open gauge\n")
	out.WriteString("watchdog_collector_principal_provider_circuit_open ")
	out.WriteString(boolMetricValue(health.CircuitOpen))
	out.WriteByte('\n')
	out.WriteString("# TYPE watchdog_collector_principal_provider_half_open_probe gauge\n")
	out.WriteString("watchdog_collector_principal_provider_half_open_probe ")
	out.WriteString(boolMetricValue(health.HalfOpenProbe))
	out.WriteByte('\n')
	out.WriteString("# TYPE watchdog_collector_principal_provider_consecutive_failures gauge\n")
	out.WriteString("watchdog_collector_principal_provider_consecutive_failures ")
	out.WriteString(strconv.FormatUint(uint64(health.ConsecutiveFailures), 10))
	out.WriteByte('\n')
	out.WriteString("# TYPE watchdog_collector_principal_provider_request_total counter\n")
	for _, item := range []struct {
		result string
		value  uint64
	}{
		{result: "success", value: health.RequestSuccessTotal},
		{result: "failure", value: health.RequestFailureTotal},
		{result: "remote_reject", value: health.RemoteRejectTotal},
		{result: "circuit_reject", value: health.CircuitRejectTotal},
	} {
		out.WriteString("watchdog_collector_principal_provider_request_total{result=\"")
		out.WriteString(item.result)
		out.WriteString("\"} ")
		out.WriteString(strconv.FormatUint(item.value, 10))
		out.WriteByte('\n')
	}
	for _, item := range []struct {
		name  string
		value time.Time
	}{
		{name: "last_success", value: health.LastSuccessAt},
		{name: "last_failure", value: health.LastFailureAt},
		{name: "last_remote_reject", value: health.LastRemoteRejectAt},
	} {
		out.WriteString("# TYPE watchdog_collector_principal_provider_")
		out.WriteString(item.name)
		out.WriteString("_timestamp_seconds gauge\nwatchdog_collector_principal_provider_")
		out.WriteString(item.name)
		out.WriteString("_timestamp_seconds ")
		value := int64(0)
		if !item.value.IsZero() {
			value = item.value.Unix()
		}
		out.WriteString(strconv.FormatInt(value, 10))
		out.WriteByte('\n')
	}
	return []byte(out.String())
}

func (p *remoteCollectorPrincipalProvider) CloseIdleConnections() {
	if p != nil && p.client != nil {
		p.client.CloseIdleConnections()
	}
}

func (response remoteCollectorPrincipalGrantResponse) grantResult() CollectorPrincipalGrantProviderResult {
	return CollectorPrincipalGrantProviderResult{
		OperationKey: response.OperationKey, RequestHash: response.RequestHash,
		Provider: response.Provider, PrincipalRef: response.PrincipalRef,
		CredentialSecretRef: response.CredentialSecretRef,
		ReceiptRef:          response.ReceiptRef, Receipt: bytes.Clone(response.Receipt),
	}
}

func (response remoteCollectorPrincipalRevokeResponse) revokeResult() CollectorPrincipalRevokeProviderResult {
	return CollectorPrincipalRevokeProviderResult{
		OperationKey: response.OperationKey, RequestHash: response.RequestHash,
		Provider: response.Provider, PrincipalRef: response.PrincipalRef,
		ReceiptRef: response.ReceiptRef, Receipt: bytes.Clone(response.Receipt),
	}
}

func remoteProviderReceiptIsObject(receipt []byte) bool {
	receipt = bytes.TrimSpace(receipt)
	return len(receipt) >= 2 && receipt[0] == '{' && receipt[len(receipt)-1] == '}'
}

func newRemoteCollectorPrincipalHTTPClient(config RemoteCollectorPrincipalProviderConfig) (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: config.TLSServerName}
	if config.TLSCAFile != "" {
		pem, err := readCollectorPrincipalProviderFile(config.TLSCAFile, 4<<20)
		if err != nil {
			return nil, fmt.Errorf("read collector principal provider TLS CA: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("collector principal provider TLS CA contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	certPEM, err := readCollectorPrincipalProviderFile(config.TLSCertFile, 4<<20)
	if err != nil {
		return nil, fmt.Errorf("read collector principal provider TLS certificate: %w", err)
	}
	keyPEM, err := readCollectorPrincipalProviderFile(config.TLSKeyFile, 4<<20)
	if err != nil {
		return nil, fmt.Errorf("read collector principal provider TLS key: %w", err)
	}
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load collector principal provider TLS identity: %w", err)
	}
	tlsConfig.Certificates = []tls.Certificate{certificate}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = tlsConfig
	transport.DisableCompression = true
	transport.MaxIdleConns = 8
	transport.MaxIdleConnsPerHost = 8
	transport.MaxConnsPerHost = 16
	transport.ResponseHeaderTimeout = config.RequestTimeout
	if transport.TLSHandshakeTimeout > config.RequestTimeout {
		transport.TLSHandshakeTimeout = config.RequestTimeout
	}
	return &http.Client{
		Transport: transport, Timeout: config.RequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

func readCollectorPrincipalProviderFile(path string, maxBytes int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxBytes {
		return nil, errors.New("file must be regular and within the size limit")
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > maxBytes {
		return nil, errors.New("file exceeds the size limit")
	}
	return payload, nil
}

func validateRemoteCollectorPrincipalProviderConfig(config RemoteCollectorPrincipalProviderConfig) error {
	if config.Name == "" || len(config.Name) > 64 || !isPrintableASCII(config.Name) || config.PolicyRevision == "" || len(config.PolicyRevision) > 128 || !isPrintableASCII(config.PolicyRevision) {
		return errors.New("collector_principal_provider name and policy_revision are required printable ASCII")
	}
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return errors.New("collector_principal_provider.base_url must be an absolute HTTPS URL without user info, query, or fragment")
	}
	if config.RequestTimeout <= 0 || config.RequestTimeout > time.Minute || config.FailureThreshold == 0 || config.FailureThreshold > 100 || config.CircuitOpenInterval <= 0 || config.CircuitOpenInterval > time.Hour || config.MinOperationRetention < 24*time.Hour || config.MinOperationRetention > 365*24*time.Hour || config.MinOperationRetention%time.Second != 0 {
		return errors.New("collector_principal_provider timeout and circuit breaker values are invalid")
	}
	if config.TLSCertFile == "" || config.TLSKeyFile == "" {
		return errors.New("collector_principal_provider requires a dedicated mTLS certificate and key")
	}
	if config.TLSCertFile == config.TLSKeyFile {
		return errors.New("collector_principal_provider TLS certificate and key must be different files")
	}
	return nil
}

func normalizeRemoteCollectorPrincipalProviderConfig(config *RemoteCollectorPrincipalProviderConfig) {
	config.Name = strings.TrimSpace(config.Name)
	config.BaseURL = strings.TrimRight(strings.TrimSpace(config.BaseURL), "/")
	config.PolicyRevision = strings.TrimSpace(config.PolicyRevision)
	config.TLSCAFile = cleanOptionalConfigPath(config.TLSCAFile)
	config.TLSCertFile = cleanOptionalConfigPath(config.TLSCertFile)
	config.TLSKeyFile = cleanOptionalConfigPath(config.TLSKeyFile)
	config.TLSServerName = strings.TrimSpace(config.TLSServerName)
}

var _ CollectorPrincipalProvider = (*remoteCollectorPrincipalProvider)(nil)
var _ collectorPrincipalRuntimeProvider = (*remoteCollectorPrincipalProvider)(nil)
