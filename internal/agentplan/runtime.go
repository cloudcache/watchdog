package agentplan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// RuntimeConfig contains only the control-plane bootstrap shared by all agent
// processes. Process-specific configuration remains owned and validated by
// the process that applies Spec.Config.
type RuntimeConfig struct {
	BaseURL         string
	AgentID         string
	Name            string
	Kind            string
	DeviceID        string
	Role            string
	Mode            string
	Endpoint        string
	SoftwareVersion string
	APIVersion      string
	Capabilities    []string
	Token           string
	TokenFile       string
	EnrollmentFile  string
	PublicKeyFile   string
	LKGFile         string
	BootID          string
	HTTPClient      *http.Client
}

type HeartbeatState struct {
	DesiredPlanVersion uint64 `json:"desired_plan_version"`
	AckedPlanVersion   uint64 `json:"acked_plan_version"`
}

func (r RuntimeConfig) Heartbeat(ctx context.Context, token string) error {
	_, err := r.heartbeat(ctx, token)
	return err
}

func (r RuntimeConfig) heartbeat(ctx context.Context, token string) (HeartbeatState, error) {
	payload, err := json.Marshal(map[string]any{
		"software_version": r.SoftwareVersion,
		"api_version":      r.APIVersion,
		"capabilities":     r.Capabilities,
		"sent_at":          time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return HeartbeatState{}, err
	}
	endpoint := strings.TrimRight(strings.TrimSpace(r.BaseURL), "/") + "/api/v1/agents/" + url.PathEscape(r.AgentID) + "/heartbeat"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return HeartbeatState{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	client := r.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return HeartbeatState{}, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return HeartbeatState{}, ErrUnauthorized
	}
	if response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return HeartbeatState{}, errors.New("agent heartbeat failed: " + response.Status + ": " + strings.TrimSpace(string(body)))
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<10))
	if err != nil {
		return HeartbeatState{}, err
	}
	// Older control-plane versions returned an empty 202 response. Accepting it
	// keeps rolling upgrades safe; plan convergence becomes active as soon as
	// the server starts returning the version fields.
	if len(bytes.TrimSpace(data)) == 0 {
		return HeartbeatState{}, nil
	}
	var state HeartbeatState
	if err := json.Unmarshal(data, &state); err != nil {
		return HeartbeatState{}, errors.New("agent heartbeat response is invalid")
	}
	return state, nil
}

func (r RuntimeConfig) RunHeartbeats(ctx context.Context, token string, interval time.Duration, appliedPlanVersion uint64, report func(error)) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		state, err := r.heartbeat(ctx, token)
		if err == nil && state.DesiredPlanVersion > appliedPlanVersion {
			err = ErrPlanChanged
		}
		if err != nil {
			if report != nil {
				report(err)
			}
			if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrPlanChanged) {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunReport is one periodic processing summary a streaming collector/worker
// sends so the registry reflects real workload, not just liveness. Summary
// carries per-window counters (records in/out, bytes, drops, errors), gauges
// (Kafka lag, buffered records),
// current version, per-target counts); the server stores it as
// agent_runs.summary_json.
type RunReport struct {
	Status     string
	Error      string
	StartedAt  time.Time
	EndedAt    time.Time
	DurationMS uint64
	Summary    map[string]any
}

// ReportRun posts one processing summary to the agent status endpoint.
func (r RuntimeConfig) ReportRun(ctx context.Context, token string, report RunReport) error {
	status := strings.TrimSpace(report.Status)
	if status == "" {
		status = "success"
	}
	ended := report.EndedAt
	if ended.IsZero() {
		ended = time.Now().UTC()
	}
	started := report.StartedAt
	if started.IsZero() {
		started = ended
	}
	body := map[string]any{
		"status":      status,
		"started_at":  started.UTC().Format(time.RFC3339Nano),
		"ended_at":    ended.UTC().Format(time.RFC3339Nano),
		"duration_ms": report.DurationMS,
	}
	if strings.TrimSpace(report.Error) != "" {
		body["error"] = report.Error
	}
	if len(report.Summary) > 0 {
		body["summary"] = report.Summary
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(strings.TrimSpace(r.BaseURL), "/") + "/api/v1/agents/" + url.PathEscape(r.AgentID) + "/status"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	client := r.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return ErrUnauthorized
	}
	if response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return errors.New("agent status report failed: " + response.Status + ": " + strings.TrimSpace(string(b)))
	}
	return nil
}

// RunReports periodically collects and posts a processing summary until ctx is
// done. collect snapshots the current counters each interval.
func (r RuntimeConfig) RunReports(ctx context.Context, token string, interval time.Duration, collect func() RunReport, report func(error)) {
	if collect == nil {
		return
	}
	if interval <= 0 {
		interval = 60 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := r.ReportRun(ctx, token, collect()); err != nil && report != nil {
			report(err)
		}
	}
}

func (r RuntimeConfig) Enabled() bool { return strings.TrimSpace(r.BaseURL) != "" }

// Sync registers the process once when necessary, then fetches, verifies,
// applies, persists and acknowledges its immutable plan. The returned token is
// only intended for the caller's in-memory client and must not be logged.
func (r RuntimeConfig) Sync(ctx context.Context, apply ApplyFunc) (SyncResult, string, error) {
	if !r.Enabled() {
		return SyncResult{}, "", errors.New("agent plan control-plane URL is required")
	}
	if r.APIVersion == "" {
		r.APIVersion = "v1"
	}
	if r.Mode == "" {
		r.Mode = "push"
	}
	publicKey, err := LoadPublicKey(r.PublicKeyFile)
	if err != nil {
		return SyncResult{}, "", err
	}
	token := strings.TrimSpace(r.Token)
	if token == "" && strings.TrimSpace(r.TokenFile) != "" {
		token, err = ReadSecretFile(r.TokenFile)
		if err != nil && !os.IsNotExist(err) {
			return SyncResult{}, "", err
		}
	}
	if token == "" {
		enrollmentToken, readErr := ReadSecretFile(r.EnrollmentFile)
		if readErr != nil {
			return SyncResult{}, "", readErr
		}
		token, err = Register(ctx, r.BaseURL, r.HTTPClient, Registration{
			EnrollmentToken: enrollmentToken, AgentID: r.AgentID, Name: r.Name,
			Kind: r.Kind, DeviceID: r.DeviceID, Role: r.Role, Mode: r.Mode,
			Endpoint: r.Endpoint, SoftwareVersion: r.SoftwareVersion,
			APIVersion: r.APIVersion, Capabilities: r.Capabilities,
			CredentialFile: r.TokenFile,
		})
		if err != nil {
			return SyncResult{}, "", err
		}
		// The enrollment secret is single-use. Once the durable machine
		// credential has been installed, keeping the bootstrap secret serves no
		// recovery purpose and only leaves misleading deployment state behind.
		if err := os.Remove(r.EnrollmentFile); err != nil && !os.IsNotExist(err) {
			return SyncResult{}, "", err
		}
	}
	client := Client{
		BaseURL: r.BaseURL, AgentID: r.AgentID, Token: token, HTTPClient: r.HTTPClient,
		PublicKey: publicKey, LKG: DiskLKG{Path: r.LKGFile}, Kind: r.Kind,
		APIVersion: r.APIVersion, Capabilities: r.Capabilities, BootID: r.BootID,
		Software: r.SoftwareVersion,
	}
	result, err := client.Sync(ctx, apply)
	if errors.Is(err, ErrUnauthorized) && token != "" {
		// A configured installation-wide token authenticates only after the agent
		// row exists. Register idempotently, then retry the original plan fetch.
		// Per-agent credentials cannot pass this registration endpoint and retain
		// the same unauthorized failure semantics.
		token, registerErr := Register(ctx, r.BaseURL, r.HTTPClient, Registration{
			SharedToken: token, AgentID: r.AgentID, Name: r.Name,
			Kind: r.Kind, DeviceID: r.DeviceID, Role: r.Role, Mode: r.Mode,
			Endpoint: r.Endpoint, SoftwareVersion: r.SoftwareVersion,
			APIVersion: r.APIVersion, Capabilities: r.Capabilities,
		})
		if registerErr != nil {
			return SyncResult{}, "", registerErr
		}
		client.Token = token
		result, err = client.Sync(ctx, apply)
	}
	return result, token, err
}

// DecodeConfig is the common strict decoder used by each process-specific
// plan adapter. It rejects unknown fields and multiple JSON values.
func DecodeConfig(spec Spec, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(spec.Config))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("agent plan config must contain exactly one JSON object")
	}
	return nil
}
