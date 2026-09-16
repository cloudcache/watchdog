package server

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/watchdog"
)

var (
	errSNMPRawForbidden = errors.New("raw metric value mode requires admin permission")
	errSNMPValueMode    = errors.New("value_mode must be corrected, raw or both")
	errSNMPTrafficView  = errors.New("traffic_view must be raw, supplier or customer")
)

func snmpValueModes(raw string, admin bool) ([]string, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		mode = "corrected"
	}
	if mode != "corrected" && mode != "raw" && mode != "both" {
		return nil, errSNMPValueMode
	}
	if mode != "corrected" && !admin {
		return nil, errSNMPRawForbidden
	}
	if mode == "both" {
		return []string{"corrected", "raw"}, nil
	}
	return []string{mode}, nil
}

func snmpTrafficSide(raw string) (watchdog.PortSideType, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "raw":
		return "", nil
	case "supplier":
		return watchdog.PortSideProvider, nil
	case "customer":
		return watchdog.PortSideCustomer, nil
	default:
		return "", errSNMPTrafficView
	}
}

func snmpSeriesPortIDs(series []snmpch.Series) []string {
	seen := make(map[string]bool)
	for _, item := range series {
		if item.EntityKind == "port" && item.EntityID != "" {
			seen[item.EntityID] = true
		}
	}
	result := make([]string, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func correctedSNMPSeries(series []snmpch.Series, policies map[string]watchdog.PortPolicy, corrected bool) []snmpch.Series {
	result := make([]snmpch.Series, 0, len(series))
	for _, item := range series {
		copyItem := item
		copyItem.Points = append([]snmpch.Point(nil), item.Points...)
		if corrected && item.EntityKind == "port" {
			if policy, ok := policies[item.EntityID]; ok {
				for index := range copyItem.Points {
					point := &copyItem.Points[index]
					point.Value = watchdog.ApplyCorrectionFloat(point.Value, policy, watchdog.DeterministicCorrectionRNG(item.EntityID, point.Time))
				}
			}
		}
		result = append(result, copyItem)
	}
	return result
}

func aggregateSNMPSeries(series []snmpch.Series, method string) []snmpch.Point {
	type accumulator struct {
		sum, min, max float64
		count         int
	}
	byTime := make(map[time.Time]accumulator)
	for _, item := range series {
		for _, point := range item.Points {
			value := byTime[point.Time]
			if value.count == 0 || point.Value < value.min {
				value.min = point.Value
			}
			if value.count == 0 || point.Value > value.max {
				value.max = point.Value
			}
			value.sum += point.Value
			value.count++
			byTime[point.Time] = value
		}
	}
	result := make([]snmpch.Point, 0, len(byTime))
	for observedAt, value := range byTime {
		var output float64
		switch method {
		case "avg":
			output = value.sum / float64(value.count)
		case "min":
			output = value.min
		case "max":
			output = value.max
		case "count":
			output = float64(value.count)
		default:
			output = value.sum
		}
		result = append(result, snmpch.Point{Time: observedAt, Value: output})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Time.Before(result[j].Time) })
	return result
}
