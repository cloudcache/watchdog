package watchdog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

var errSNMPDerivedMetric = errors.New("snmp raw writer rejects derived metric")

type VictoriaMetricsSNMPRawWriter struct {
	Client VictoriaMetricsClient
}

func (w VictoriaMetricsSNMPRawWriter) WriteSNMPRawSamples(ctx context.Context, samples []SNMPRawSample) error {
	payload, err := RenderSNMPRawSamplesPrometheus(samples)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	return w.Client.ImportPrometheus(ctx, payload)
}

func RenderSNMPRawSamplesPrometheus(samples []SNMPRawSample) ([]byte, error) {
	var b strings.Builder
	for _, sample := range samples {
		if err := validateRawSample(sample); err != nil {
			return nil, err
		}
		if strings.Contains(sample.MetricName, "_bps") {
			return nil, fmt.Errorf("%w: %s", errSNMPDerivedMetric, sample.MetricName)
		}
		if sample.ValueType == SNMPCollectorValueString || sample.ValueType == SNMPCollectorValueMACAddr || sample.ValueType == SNMPCollectorValueIPAddr {
			continue
		}
		writeSNMPRawPrometheusSample(&b, sample)
	}
	return []byte(b.String()), nil
}

func writeSNMPRawPrometheusSample(b *strings.Builder, sample SNMPRawSample) {
	b.WriteString(sample.MetricName)
	b.WriteByte('{')
	labels := make(map[string]string, len(sample.Labels)+6)
	for key, value := range sample.Labels {
		if value != "" && allowedSNMPRawLabel(key) {
			labels[key] = value
		}
	}
	labels["tenant_id"] = string(sample.TenantID)
	if sample.TargetID != "" {
		labels["target_id"] = string(sample.TargetID)
	}
	labels["device_id"] = string(sample.DeviceID)
	labels["recipe_id"] = string(sample.RecipeID)
	labels["entity_type"] = string(sample.EntityType)
	labels["entity_id"] = string(sample.EntityID)
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, labelMatcher(key, labels[key]))
	}
	b.WriteString(strings.Join(parts, ","))
	b.WriteString("} ")
	b.WriteString(strconv.FormatFloat(sample.FloatValue, 'f', -1, 64))
	if !sample.SampledAt.IsZero() {
		b.WriteByte(' ')
		b.WriteString(strconv.FormatInt(sample.SampledAt.UnixMilli(), 10))
	}
	b.WriteByte('\n')
}

func allowedSNMPRawLabel(key string) bool {
	switch key {
	case "tenant_id", "target_id", "device_id", "recipe_id", "entity_type", "entity_id", "module",
		"port_id", "if_index", "if_name", "sensor_id", "sensor_class", "sensor_name", "bgp_session_id", "afi", "safi":
		return true
	default:
		return false
	}
}
