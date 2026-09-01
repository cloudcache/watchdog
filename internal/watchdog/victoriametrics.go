package watchdog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

var ErrVictoriaMetricsUnavailable = errors.New("victoriametrics unavailable")

type VictoriaMetricsClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

type RangeQuery struct {
	Query string
	Start time.Time
	End   time.Time
	Step  time.Duration
}

type VictoriaMetricsResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string             `json:"resultType"`
		Result     []VMRangeQueryItem `json:"result"`
	} `json:"data"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

type VMRangeQueryItem struct {
	Metric map[string]string `json:"metric"`
	Values []VMValue         `json:"values"`
}

type VMValue struct {
	Time  time.Time
	Value float64
}

func (v VMValue) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{float64(v.Time.UnixMilli()) / 1000, strconv.FormatFloat(v.Value, 'f', -1, 64)})
}

func (v *VMValue) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) != 2 {
		return fmt.Errorf("invalid VictoriaMetrics value length %d", len(raw))
	}
	var ts float64
	if err := json.Unmarshal(raw[0], &ts); err != nil {
		return err
	}
	var valueString string
	if err := json.Unmarshal(raw[1], &valueString); err != nil {
		return err
	}
	value, err := strconv.ParseFloat(valueString, 64)
	if err != nil {
		return err
	}
	v.Time = time.UnixMilli(int64(ts * 1000)).UTC()
	v.Value = value
	return nil
}

func (c VictoriaMetricsClient) QueryRange(ctx context.Context, query RangeQuery) (VictoriaMetricsResponse, error) {
	var response VictoriaMetricsResponse
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	endpoint, err := url.JoinPath(strings.TrimRight(c.BaseURL, "/"), "api/v1/query_range")
	if err != nil {
		return response, err
	}
	values := url.Values{}
	values.Set("query", query.Query)
	values.Set("start", strconv.FormatInt(query.Start.Unix(), 10))
	values.Set("end", strconv.FormatInt(query.End.Unix(), 10))
	values.Set("step", strconv.FormatFloat(query.Step.Seconds(), 'f', -1, 64))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+values.Encode(), nil)
	if err != nil {
		return response, err
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return response, fmt.Errorf("%w: %v", ErrVictoriaMetricsUnavailable, err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return response, fmt.Errorf("victoriametrics query_range failed: %s", res.Status)
	}
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return response, err
	}
	if response.Status != "success" {
		return response, fmt.Errorf("victoriametrics query_range failed: %s", response.Error)
	}
	return response, nil
}

func (c VictoriaMetricsClient) ImportPrometheus(ctx context.Context, payload []byte) error {
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	endpoint, err := url.JoinPath(strings.TrimRight(c.BaseURL, "/"), "api/v1/import/prometheus")
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; version=0.0.4")
	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("victoriametrics import failed: %s", res.Status)
	}
	return nil
}

func (c VictoriaMetricsClient) DeleteSeries(ctx context.Context, matchers []string) error {
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	endpoint, err := url.JoinPath(strings.TrimRight(c.BaseURL, "/"), "api/v1/admin/tsdb/delete_series")
	if err != nil {
		return err
	}
	values := url.Values{}
	for _, matcher := range matchers {
		values.Add("match[]", matcher)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"?"+values.Encode(), nil)
	if err != nil {
		return err
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVictoriaMetricsUnavailable, err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("victoriametrics delete_series failed: %s", res.Status)
	}
	return nil
}

func RenderSystemBatchPrometheus(batch SystemSampleBatch) []byte {
	var b strings.Builder
	timestamp := batch.SampledAt.UnixMilli()
	writePrometheusTargetSample(&b, MetricSystemCPUPercent, batch.TenantID, batch.TargetID, nil, batch.System.CPUPercent, timestamp)
	writePrometheusTargetSample(&b, MetricSystemMemoryPercent, batch.TenantID, batch.TargetID, nil, batch.System.MemoryPercent, timestamp)
	writePrometheusTargetSample(&b, MetricSystemDiskPercent, batch.TenantID, batch.TargetID, nil, batch.System.DiskPercent, timestamp)
	writePrometheusTargetSample(&b, MetricSystemNetInBps, batch.TenantID, batch.TargetID, nil, batch.System.NetInBps, timestamp)
	writePrometheusTargetSample(&b, MetricSystemNetOutBps, batch.TenantID, batch.TargetID, nil, batch.System.NetOutBps, timestamp)
	writePrometheusTargetSample(&b, MetricGPUCount, batch.TenantID, batch.TargetID, nil, float64(len(batch.GPUs)), timestamp)
	for _, sample := range batch.GPUs {
		labels := map[string]string{}
		if sample.Index != "" {
			labels["gpu_index"] = sample.Index
		}
		if sample.Name != "" {
			labels["gpu_name"] = sample.Name
		}
		up := 0.0
		if sample.Up {
			up = 1
		}
		writePrometheusTargetSample(&b, MetricGPUUp, batch.TenantID, batch.TargetID, labels, up, timestamp)
		writePrometheusTargetSample(&b, MetricGPUUtilPercent, batch.TenantID, batch.TargetID, labels, sample.UtilizationPercent, timestamp)
	}
	for _, sample := range batch.Containers {
		labels := map[string]string{"container_name": sample.Name}
		writePrometheusTargetSample(&b, MetricContainerCPUPercent, batch.TenantID, batch.TargetID, labels, sample.CPUPercent, timestamp)
		writePrometheusTargetSample(&b, MetricContainerMemory, batch.TenantID, batch.TargetID, labels, float64(sample.MemoryBytes), timestamp)
		writePrometheusTargetSample(&b, MetricContainerNetTxBps, batch.TenantID, batch.TargetID, labels, sample.NetTxBps, timestamp)
		writePrometheusTargetSample(&b, MetricContainerNetRxBps, batch.TenantID, batch.TargetID, labels, sample.NetRxBps, timestamp)
	}
	return []byte(b.String())
}

func writePrometheusTargetSample(b *strings.Builder, metric string, tenantID, targetID ID, extraLabels map[string]string, value float64, timestamp int64) {
	b.WriteString(metric)
	b.WriteByte('{')
	parts := []string{
		labelMatcher("tenant_id", string(tenantID)),
		labelMatcher("target_id", string(targetID)),
	}
	keys := make([]string, 0, len(extraLabels))
	for key := range extraLabels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if extraLabels[key] != "" {
			parts = append(parts, labelMatcher(key, extraLabels[key]))
		}
	}
	b.WriteString(strings.Join(parts, ","))
	b.WriteString("} ")
	b.WriteString(strconv.FormatFloat(value, 'f', -1, 64))
	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(timestamp, 10))
	b.WriteByte('\n')
}

func writePrometheusSample(b *strings.Builder, metric string, labels MetricLabelSet, value float64, timestamp int64) {
	b.WriteString(metric)
	b.WriteByte('{')
	parts := []string{
		labelMatcher("tenant_id", string(labels.TenantID)),
		labelMatcher("target_id", string(labels.TargetID)),
		labelMatcher("device_id", string(labels.DeviceID)),
		labelMatcher("port_id", string(labels.PortID)),
		labelMatcher("if_index", strconv.FormatUint(labels.IfIndex, 10)),
	}
	if labels.SideType != "" {
		parts = append(parts, labelMatcher("side_type", string(labels.SideType)))
	}
	if labels.Vendor != "" {
		parts = append(parts, labelMatcher("vendor", labels.Vendor))
	}
	if labels.Model != "" {
		parts = append(parts, labelMatcher("model", labels.Model))
	}
	if labels.IfName != "" {
		parts = append(parts, labelMatcher("if_name", labels.IfName))
	}
	b.WriteString(strings.Join(parts, ","))
	b.WriteString("} ")
	b.WriteString(strconv.FormatFloat(value, 'f', -1, 64))
	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(timestamp, 10))
	b.WriteByte('\n')
}

func SNMPPortMetricSelector(metric string, labels MetricLabelSet) string {
	parts := []string{
		labelMatcher("tenant_id", string(labels.TenantID)),
		labelMatcher("target_id", string(labels.TargetID)),
		labelMatcher("device_id", string(labels.DeviceID)),
		labelMatcher("port_id", string(labels.PortID)),
	}
	if labels.IfIndex != 0 {
		parts = append(parts, labelMatcher("if_index", strconv.FormatUint(labels.IfIndex, 10)))
	}
	if labels.SideType != "" {
		parts = append(parts, labelMatcher("side_type", string(labels.SideType)))
	}
	return metric + "{" + strings.Join(parts, ",") + "}"
}

func BGPMetricSelector(metric string, labels BGPMetricLabelSet) string {
	parts := []string{
		labelMatcher("tenant_id", string(labels.TenantID)),
		labelMatcher("target_id", string(labels.TargetID)),
		labelMatcher("device_id", string(labels.DeviceID)),
	}
	if labels.SessionID != "" {
		parts = append(parts, labelMatcher("session_id", string(labels.SessionID)))
	}
	if labels.PeerAddr != "" {
		parts = append(parts, labelMatcher("peer_addr", labels.PeerAddr))
	}
	if labels.PeerAS != 0 {
		parts = append(parts, labelMatcher("peer_as", strconv.FormatUint(labels.PeerAS, 10)))
	}
	if labels.AFI != "" {
		parts = append(parts, labelMatcher("afi", labels.AFI))
	}
	if labels.SAFI != "" {
		parts = append(parts, labelMatcher("safi", labels.SAFI))
	}
	return metric + "{" + strings.Join(parts, ",") + "}"
}

func labelMatcher(key, value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return key + `="` + escaped + `"`
}
