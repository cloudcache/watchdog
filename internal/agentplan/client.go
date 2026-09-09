package agentplan

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

var (
	ErrUnauthorized = errors.New("agent credential is invalid or revoked")
	ErrNoPlan       = errors.New("agent has no desired plan")
)

type Client struct {
	BaseURL      string
	AgentID      string
	Token        string
	HTTPClient   *http.Client
	PublicKey    ed25519.PublicKey
	LKG          DiskLKG
	Kind         string
	APIVersion   string
	Capabilities []string
	BootID       string
	Software     string
	Now          func() time.Time
}

type SyncResult struct {
	Envelope Envelope
	Spec     Spec
	Source   string
	AckError error
}

type ApplyFunc func(context.Context, Spec) error

func (c Client) Sync(ctx context.Context, apply ApplyFunc) (SyncResult, error) {
	if err := c.validate(); err != nil {
		return SyncResult{}, err
	}
	if apply == nil {
		return SyncResult{}, errors.New("agent plan apply callback is required")
	}
	data, status, err := c.fetch(ctx)
	if err != nil {
		if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrNoPlan) {
			return SyncResult{}, err
		}
		if status == 0 || status >= http.StatusInternalServerError {
			return c.restore(ctx, apply, err)
		}
		return SyncResult{}, err
	}
	if status == http.StatusNotModified {
		return c.restore(ctx, apply, nil)
	}
	now := c.now()
	envelope, spec, err := Verify(data, c.PublicKey, now, true)
	if err == nil && envelope.Metadata.AgentID != c.AgentID {
		err = errors.New("agent plan belongs to a different agent")
	}
	if err == nil {
		err = ValidateCompatibility(spec, c.AgentID, c.Kind, c.APIVersion, c.Capabilities)
	}
	if err != nil {
		_ = c.ack(ctx, EnvelopeMetadata(data), "rejected", "VERIFY_FAILED", err.Error())
		return SyncResult{}, err
	}
	if err := apply(ctx, spec); err != nil {
		_ = c.ack(ctx, envelope.Metadata, "rejected", "ACTIVATE_FAILED", err.Error())
		return SyncResult{}, fmt.Errorf("apply agent plan: %w", err)
	}
	if _, _, err := c.LKG.InstallAt(data, c.PublicKey, c.AgentID, c.Kind, c.APIVersion, c.Capabilities, now); err != nil {
		_ = c.ack(ctx, envelope.Metadata, "rejected", "PERSIST_FAILED", err.Error())
		return SyncResult{}, err
	}
	result := SyncResult{Envelope: envelope, Spec: spec, Source: "remote"}
	result.AckError = c.ack(ctx, envelope.Metadata, "applied", "", "")
	return result, nil
}

func (c Client) restore(ctx context.Context, apply ApplyFunc, cause error) (SyncResult, error) {
	_, envelope, spec, err := c.LKG.Load(c.PublicKey, c.AgentID, c.Kind, c.APIVersion, c.Capabilities)
	if err != nil {
		if cause != nil {
			return SyncResult{}, errors.Join(cause, err)
		}
		return SyncResult{}, err
	}
	if err := apply(ctx, spec); err != nil {
		return SyncResult{}, fmt.Errorf("apply agent plan LKG: %w", err)
	}
	return SyncResult{Envelope: envelope, Spec: spec, Source: "lkg"}, nil
}

func (c Client) fetch(ctx context.Context) ([]byte, int, error) {
	endpoint, err := c.endpoint("/api/v1/agents/" + url.PathEscape(c.AgentID) + "/plan")
	if err != nil {
		return nil, 0, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	c.authorize(request)
	if etag := c.lkgETag(); etag != "" {
		request.Header.Set("If-None-Match", etag)
	}
	response, err := c.httpClient().Do(request)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch agent plan: %w", err)
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK:
		data, err := io.ReadAll(io.LimitReader(response.Body, MaxPayloadSize+(64<<10)+1))
		if err != nil {
			return nil, 0, err
		}
		if len(data) > MaxPayloadSize+(64<<10) {
			return nil, 0, errors.New("agent plan response is too large")
		}
		return data, response.StatusCode, nil
	case http.StatusNotModified:
		return nil, response.StatusCode, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, response.StatusCode, ErrUnauthorized
	case http.StatusNotFound, http.StatusNoContent:
		return nil, response.StatusCode, ErrNoPlan
	default:
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return nil, response.StatusCode, fmt.Errorf("fetch agent plan: HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
}

func (c Client) lkgETag() string {
	_, envelope, _, err := c.LKG.Load(c.PublicKey, c.AgentID, c.Kind, c.APIVersion, c.Capabilities)
	if err != nil {
		return ""
	}
	return fmt.Sprintf(`"agent-plan-%d-%s"`, envelope.Metadata.PlanVersion, envelope.Metadata.PayloadSHA256)
}

func (c Client) ack(ctx context.Context, metadata Metadata, status, code, detail string) error {
	if metadata.PlanVersion == 0 || metadata.PayloadSHA256 == "" {
		return errors.New("agent plan ACK metadata is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"plan_version": metadata.PlanVersion, "payload_sha256": metadata.PayloadSHA256,
		"status": status, "boot_id": c.BootID, "software_version": c.Software,
		"error_code": code, "error": detail,
	})
	if err != nil {
		return err
	}
	endpoint, err := c.endpoint("/api/v1/agents/" + url.PathEscape(c.AgentID) + "/plan-acks")
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	c.authorize(request)
	response, err := c.httpClient().Do(request)
	if err != nil {
		return fmt.Errorf("acknowledge agent plan: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return ErrUnauthorized
	}
	if response.StatusCode != http.StatusAccepted && response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return fmt.Errorf("acknowledge agent plan: HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c Client) validate() error {
	if strings.TrimSpace(c.BaseURL) == "" || strings.TrimSpace(c.AgentID) == "" || !ValidKind(c.Kind) || c.APIVersion != "v1" || len(c.PublicKey) != ed25519.PublicKeySize || strings.TrimSpace(c.BootID) == "" {
		return errors.New("agent plan client identity, endpoint, public key, and boot id are required")
	}
	if len(c.Capabilities) == 0 {
		return errors.New("agent plan client capabilities are required")
	}
	return nil
}

func (c Client) endpoint(path string) (string, error) {
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(c.BaseURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", errors.New("agent plan control-plane URL is invalid")
	}
	return base.String() + path, nil
}

func (c Client) authorize(request *http.Request) {
	if token := strings.TrimSpace(c.Token); token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
}

func (c Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func (c Client) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

// EnvelopeMetadata extracts only enough metadata to send a rejected ACK after
// a verification error. Invalid input returns zero metadata and no ACK is sent.
func EnvelopeMetadata(data []byte) Metadata {
	var envelope Envelope
	if json.Unmarshal(data, &envelope) != nil {
		return Metadata{}
	}
	return envelope.Metadata
}

type Registration struct {
	EnrollmentToken string
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
	CredentialFile  string
}

func Register(ctx context.Context, baseURL string, client *http.Client, registration Registration) (string, error) {
	if registration.EnrollmentToken == "" || registration.AgentID == "" || !ValidKind(registration.Kind) || registration.CredentialFile == "" {
		return "", errors.New("agent enrollment token, identity, kind, and credential file are required")
	}
	payload, _ := json.Marshal(map[string]any{
		"enrollment_token": registration.EnrollmentToken, "id": registration.AgentID,
		"name": registration.Name, "kind": registration.Kind, "device_id": registration.DeviceID,
		"role": registration.Role, "mode": registration.Mode, "endpoint": registration.Endpoint,
		"software_version": registration.SoftwareVersion, "api_version": registration.APIVersion,
		"capabilities": registration.Capabilities,
	})
	endpoint := strings.TrimRight(strings.TrimSpace(baseURL), "/") + "/api/v1/agents/register"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return "", fmt.Errorf("register agent: HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var result struct {
		Credential struct {
			Token string `json:"token"`
		} `json:"credential"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	if err := decoder.Decode(&result); err != nil || result.Credential.Token == "" {
		return "", errors.New("register agent response did not contain a credential")
	}
	if err := atomicWriteFile(registration.CredentialFile, []byte(result.Credential.Token+"\n"), 0o600); err != nil {
		return "", err
	}
	return result.Credential.Token, nil
}

func ReadSecretFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if len(data) > 64<<10 {
		return "", errors.New("agent secret file is too large")
	}
	secret := strings.TrimSpace(string(data))
	if secret == "" {
		return "", errors.New("agent secret file is empty")
	}
	return secret, nil
}
