package watchdog

import (
	"errors"
	"strconv"
	"strings"
)

type SNMPTrafficDirection string

const (
	SNMPTrafficIn  SNMPTrafficDirection = "in"
	SNMPTrafficOut SNMPTrafficDirection = "out"
)

type SNMPTrafficRateProjectionRequest struct {
	TenantID  ID
	DeviceID  ID
	EntityID  ID
	RecipeID  ID
	IfIndex   uint64
	Direction SNMPTrafficDirection
	Window    string
}

func SNMPTrafficRateQuery(req SNMPTrafficRateProjectionRequest) (string, error) {
	if req.TenantID == "" {
		return "", errors.New("tenant_id is required")
	}
	if req.DeviceID == "" && req.EntityID == "" {
		return "", errors.New("device_id or entity_id is required")
	}
	metric, err := rawOctetsMetricForTrafficDirection(req.Direction)
	if err != nil {
		return "", err
	}
	window := strings.TrimSpace(req.Window)
	if window == "" {
		window = "5m"
	}
	labels := []string{
		labelMatcher("tenant_id", string(req.TenantID)),
		labelMatcher("entity_type", string(SNMPCollectorEntityPort)),
	}
	if req.DeviceID != "" {
		labels = append(labels, labelMatcher("device_id", string(req.DeviceID)))
	}
	if req.EntityID != "" {
		labels = append(labels, labelMatcher("entity_id", string(req.EntityID)))
	}
	if req.IfIndex != 0 {
		labels = append(labels, labelMatcher("if_index", strconv.FormatUint(req.IfIndex, 10)))
	}
	if req.RecipeID != "" {
		labels = append(labels, labelMatcher("recipe_id", string(req.RecipeID)))
	}
	return "rate(" + metric + "{" + strings.Join(labels, ",") + "}[" + window + "]) * 8", nil
}

func rawOctetsMetricForTrafficDirection(direction SNMPTrafficDirection) (string, error) {
	switch direction {
	case SNMPTrafficIn:
		return MetricSNMPIfInOctetsTotal, nil
	case SNMPTrafficOut:
		return MetricSNMPIfOutOctetsTotal, nil
	default:
		return "", errors.New("traffic direction must be in or out")
	}
}
