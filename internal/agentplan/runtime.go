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

func (r RuntimeConfig) Heartbeat(ctx context.Context, token string) error {
	payload, err := json.Marshal(map[string]any{
		"software_version": r.SoftwareVersion,
		"api_version":      r.APIVersion,
		"capabilities":     r.Capabilities,
		"sent_at":          time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(strings.TrimSpace(r.BaseURL), "/") + "/api/v1/agents/" + url.PathEscape(r.AgentID) + "/heartbeat"
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
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return errors.New("agent heartbeat failed: " + response.Status + ": " + strings.TrimSpace(string(body)))
	}
	return nil
}

func (r RuntimeConfig) RunHeartbeats(ctx context.Context, token string, interval time.Duration, report func(error)) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Heartbeat(ctx, token); err != nil {
				if report != nil {
					report(err)
				}
				if errors.Is(err, ErrUnauthorized) {
					return
				}
			}
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
	}
	client := Client{
		BaseURL: r.BaseURL, AgentID: r.AgentID, Token: token, HTTPClient: r.HTTPClient,
		PublicKey: publicKey, LKG: DiskLKG{Path: r.LKGFile}, Kind: r.Kind,
		APIVersion: r.APIVersion, Capabilities: r.Capabilities, BootID: r.BootID,
		Software: r.SoftwareVersion,
	}
	result, err := client.Sync(ctx, apply)
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
