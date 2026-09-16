package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"
)

const (
	agentRunSuccess agentRunStatus = "success"
	agentRunFailure agentRunStatus = "failure"
)

type agentRunStatus string

type agentRunReport struct {
	AgentID   string
	Status    agentRunStatus
	Error     string
	Seen      bool
	StartedAt time.Time
	EndedAt   time.Time
}

type systemAgentClient struct {
	HubURL     string
	AgentID    string
	AgentToken string
	HTTPClient *http.Client
}

func (c systemAgentClient) FetchPlan(ctx context.Context) (systemAgentPlan, error) {
	var plan systemAgentPlan
	if err := c.getJSON(ctx, "plan", &plan); err != nil {
		return systemAgentPlan{}, err
	}
	return plan, nil
}

func (c systemAgentClient) Heartbeat(ctx context.Context) error {
	endpoint, err := c.genericAgentEndpoint("heartbeat")
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

func (c systemAgentClient) PushSamples(ctx context.Context, batch systemSampleBatch) error {
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

func (c systemAgentClient) ReportStatus(ctx context.Context, report agentRunReport) error {
	if report.Status == "" {
		report.Status = agentRunSuccess
	}
	return c.postReport(ctx, "status", report)
}

func (c systemAgentClient) ReportError(ctx context.Context, report agentRunReport) error {
	report.Status = agentRunFailure
	return c.postReport(ctx, "errors", report)
}

func (c systemAgentClient) postReport(ctx context.Context, action string, report agentRunReport) error {
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

func (c systemAgentClient) getJSON(ctx context.Context, action string, out any) error {
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

func (c systemAgentClient) endpoint(action string) (string, error) {
	if c.HubURL == "" {
		return "", errors.New("hub url is required")
	}
	if c.AgentID == "" {
		return "", errors.New("agent id is required")
	}
	return url.JoinPath(c.HubURL, "api/v1/system-agents", c.AgentID, action)
}

func (c systemAgentClient) genericAgentEndpoint(action string) (string, error) {
	if c.HubURL == "" {
		return "", errors.New("hub url is required")
	}
	if c.AgentID == "" {
		return "", errors.New("agent id is required")
	}
	return url.JoinPath(c.HubURL, "api/v1/agents", c.AgentID, action)
}

func (c systemAgentClient) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}
