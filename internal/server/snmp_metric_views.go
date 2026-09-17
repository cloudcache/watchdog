package server

import (
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/metricdomain"
	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/snmpdomain"
)

const defaultSNMPChartPointBudget uint32 = 1200

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

func snmpTrafficSide(raw string) (snmpdomain.PortSideType, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "raw":
		return "", nil
	case "supplier":
		return snmpdomain.PortSideProvider, nil
	case "customer":
		return snmpdomain.PortSideCustomer, nil
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

func correctedSNMPSeries(series []snmpch.Series, policies map[string]snmpdomain.PortPolicy, corrected bool) []snmpch.Series {
	result := make([]snmpch.Series, 0, len(series))
	for _, item := range series {
		copyItem := item
		copyItem.Points = append([]snmpch.Point(nil), item.Points...)
		if corrected && item.EntityKind == "port" {
			if policy, ok := policies[item.EntityID]; ok {
				for index := range copyItem.Points {
					point := &copyItem.Points[index]
					point.Value = snmpdomain.ApplyCorrectionFloat(point.Value, policy, snmpdomain.DeterministicCorrectionRNG(item.EntityID, point.Time))
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

// snmpIntermediateRowBudget separates two different limits which used to be
// conflated: max_data_points bounds the final rendered series, while this
// budget bounds the per-port rows needed before correction and aggregation.
func snmpIntermediateRowBudget(finalPoints uint32, scopes []snmpch.Scope, from, to time.Time, step time.Duration, ceiling uint32) uint32 {
	if ceiling == 0 || ceiling > snmpch.HardMaxAggregateRows {
		ceiling = snmpch.HardMaxAggregateRows
	}
	if finalPoints == 0 {
		finalPoints = defaultSNMPChartPointBudget
	}
	if len(scopes) == 0 || step <= 0 || !to.After(from) {
		return min(finalPoints, ceiling)
	}
	explicitPorts := uint64(0)
	for _, scope := range scopes {
		if scope.PortID == "" {
			return ceiling
		}
		explicitPorts++
	}
	buckets := uint64(math.Ceil(float64(to.Sub(from)) / float64(step)))
	// One row of slack per port covers boundary rounding without turning the
	// final chart budget into an unbounded ClickHouse read.
	estimated := (buckets + 1) * explicitPorts
	if estimated < uint64(finalPoints) {
		estimated = uint64(finalPoints)
	}
	if estimated > uint64(ceiling) {
		return ceiling
	}
	return uint32(estimated)
}

func (s *Server) snmpQueryRowCeiling() uint32 {
	if s != nil && s.cfg.SNMP.QueryMaxIntermediateRows > 0 && s.cfg.SNMP.QueryMaxIntermediateRows <= snmpch.HardMaxAggregateRows {
		return s.cfg.SNMP.QueryMaxIntermediateRows
	}
	return snmpch.HardMaxAggregateRows
}

// downsampleSNMPPoints is applied only after correction and aggregation. Rate
// and gauge samples use a mean per display bucket; monotonic counters and
// state metrics use the last observation so their semantics are preserved.
func downsampleSNMPPoints(points []snmpch.Point, maxPoints uint32, useLast bool) []snmpch.Point {
	if maxPoints == 0 || len(points) <= int(maxPoints) {
		return points
	}
	result := make([]snmpch.Point, 0, maxPoints)
	n := len(points)
	for bucket := 0; bucket < int(maxPoints); bucket++ {
		start := bucket * n / int(maxPoints)
		end := (bucket + 1) * n / int(maxPoints)
		last := points[end-1]
		if useLast {
			result = append(result, last)
			continue
		}
		var total float64
		for _, point := range points[start:end] {
			total += point.Value
		}
		last.Value = total / float64(end-start)
		result = append(result, last)
	}
	return result
}

func snmpMetricUsesLastSample(metric string) bool {
	if strings.HasSuffix(metric, "_total") {
		return true
	}
	switch metric {
	case metricdomain.SNMPIfOperStatus, metricdomain.SNMPIfAdminStatus, metricdomain.BGPState:
		return true
	default:
		return false
	}
}
