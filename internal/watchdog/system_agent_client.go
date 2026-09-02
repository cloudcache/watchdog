package watchdog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"
)

type SystemAgentClient struct {
	HubURL     string
	AgentID    ID
	AgentToken string
	HTTPClient *http.Client
}

func (c SystemAgentClient) FetchPlan(ctx context.Context) (SystemAgentPlan, error) {
	var plan SystemAgentPlan
	if err := c.getJSON(ctx, "plan", &plan); err != nil {
		return SystemAgentPlan{}, err
	}
	return plan, nil
}

func (c SystemAgentClient) Heartbeat(ctx context.Context) error {
	endpoint, err := c.endpoint("heartbeat")
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Watchdog-Agent-Token", c.AgentToken)
	res, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return errors.New("system agent heartbeat failed")
	}
	return nil
}

func (c SystemAgentClient) PushSamples(ctx context.Context, batch SystemSampleBatch) error {
	endpoint, err := c.endpoint("samples")
	if err != nil {
		return err
	}
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Watchdog-Agent-Token", c.AgentToken)
	res, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return errors.New("system agent sample push failed")
	}
	return nil
}

func (c SystemAgentClient) ReportStatus(ctx context.Context, report AgentRunReport) error {
	if report.Status == "" {
		report.Status = AgentRunSuccess
	}
	return c.postReport(ctx, "status", report)
}

func (c SystemAgentClient) ReportError(ctx context.Context, report AgentRunReport) error {
	report.Status = AgentRunFailure
	return c.postReport(ctx, "errors", report)
}

func (c SystemAgentClient) postReport(ctx context.Context, action string, report AgentRunReport) error {
	endpoint, err := c.genericAgentEndpoint(action)
	if err != nil {
		return err
	}
	body, err := json.Marshal(report)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Watchdog-Agent-Token", c.AgentToken)
	res, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return errors.New("system agent status report failed")
	}
	return nil
}

func (c SystemAgentClient) getJSON(ctx context.Context, action string, out any) error {
	endpoint, err := c.endpoint(action)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Watchdog-Agent-Token", c.AgentToken)
	res, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return errors.New("system agent request failed")
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func (c SystemAgentClient) endpoint(action string) (string, error) {
	if c.HubURL == "" {
		return "", errors.New("hub url is required")
	}
	if c.AgentID == "" {
		return "", errors.New("agent id is required")
	}
	return url.JoinPath(c.HubURL, "api/v1/system-agents", string(c.AgentID), action)
}

func (c SystemAgentClient) genericAgentEndpoint(action string) (string, error) {
	if c.HubURL == "" {
		return "", errors.New("hub url is required")
	}
	if c.AgentID == "" {
		return "", errors.New("agent id is required")
	}
	return url.JoinPath(c.HubURL, "api/v1/agents", string(c.AgentID), action)
}

func (c SystemAgentClient) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}
