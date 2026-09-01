package watchdog

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Declarative dashboard model (Grafana-style schema, no pre-resolved data).
// The backend emits this schema; the frontend GraphPanelRenderer executes
// each panel's Queries against the metrics API and applies Transforms.
// ---------------------------------------------------------------------------

// GraphDashboard is a set of render-ready panel definitions for an entity.
type GraphDashboard struct {
	ID     string       `json:"id"`
	Title  string       `json:"title"`
	Panels []GraphPanel `json:"panels"`
	Refresh int         `json:"refresh"` // suggested auto-refresh, seconds (0 = off)
}

// GraphPanel is one panel schema. Stack values: "" (none), "normal", "signed"
// (LibreNMS-style: In above axis, Out mirrored below).
type GraphPanel struct {
	ID           string            `json:"id"`
	Title        string            `json:"title"`
	Type         string            `json:"type"` // line | area | bar | stat
	Unit         string            `json:"unit,omitempty"`
	Stack        string            `json:"stack,omitempty"`
	Queries      []GraphQuery      `json:"queries"`
	Links        []GraphLink       `json:"links,omitempty"`
	QueryOptions GraphQueryOptions `json:"query_options"`
}

// GraphQuery is a declarative metric query. Scope selects the selector family:
// "device" (target+device labels), "port" (single port, requires PortID),
// "ports-aggregate" (aggregate across the parent entity's port set).
type GraphQuery struct {
	Metric      string              `json:"metric"`
	Scope       string              `json:"scope"`
	Aggregation string              `json:"aggregation,omitempty"` // sum|avg|max|min|count for ports-aggregate
	Label       string              `json:"label,omitempty"`
	PortID      ID                  `json:"port_id,omitempty"`
	PortIDs     []ID                `json:"port_ids,omitempty"` // explicit ports-aggregate membership; overrides the renderer's context port set
	Transform   GraphQueryTransform `json:"transform,omitempty"`
}

type GraphQueryTransform struct {
	Negative bool `json:"negative,omitempty"` // mirror below axis (Out traffic)
	Rate     bool `json:"rate,omitempty"`     // apply rate() to counters
}

type GraphLink struct {
	Href  string `json:"href"`
	Label string `json:"label"`
}

type GraphQueryOptions struct {
	MaxDataPoints int    `json:"max_data_points"`
	MinInterval   string `json:"min_interval"`
}

const (
	graphScopeDevice          = "device"
	graphScopePort            = "port"
	graphScopePortsAggregate  = "ports-aggregate"
	defaultGraphMaxDataPoints = 1200
	defaultGraphMinInterval   = "1m"
)

func defaultGraphQueryOptions() GraphQueryOptions {
	return GraphQueryOptions{MaxDataPoints: defaultGraphMaxDataPoints, MinInterval: defaultGraphMinInterval}
}

// virtualIfTypes are IANAifType values whose octet counters duplicate traffic
// already counted on physical members: other(1), softwareLoopback(24),
// propVirtual(53), tunnel(131), l2vlan(135), l3ipvlan(136), mplsTunnel(150),
// ieee8023adLag(161).
var virtualIfTypes = map[string]bool{
	"1": true, "24": true, "53": true, "131": true, "135": true, "136": true, "150": true, "161": true,
}

var virtualIfNamePrefixes = []string{
	"vlanif", "vlan-interface", "null", "loopback", "inloopback",
	"eth-trunk", "port-channel", "bridge-aggregation", "tunnel",
}

// aggregatablePorts filters ports whose counters can be summed into a device
// total without double counting. Huawei reports Eth-Trunk with ifType 6, so
// well-known virtual interface names are excluded alongside virtual ifTypes.
func aggregatablePorts(ports []NetworkPort) []NetworkPort {
	result := make([]NetworkPort, 0, len(ports))
	for _, port := range ports {
		if virtualIfTypes[port.Metadata["if_type"]] {
			continue
		}
		name := strings.ToLower(firstNonEmptySNMPString(port.IfName, port.IfDescr))
		virtual := false
		for _, prefix := range virtualIfNamePrefixes {
			if strings.HasPrefix(name, prefix) {
				virtual = true
				break
			}
		}
		if virtual {
			continue
		}
		result = append(result, port)
	}
	return result
}

// NewNetworkDeviceOverviewDashboard builds a device-specific dashboard: only
// panels for capabilities the device actually has (discovered via SNMP readback).
// A device without BGP sessions or optical sensors won't get those panels.
func NewNetworkDeviceOverviewDashboard(device NetworkDevice, ports []NetworkPort, bgpSessions []BGPSession, sensors []NetworkDeviceSensor) GraphDashboard {
	links := make([]GraphLink, 0, len(ports))
	for _, p := range ports {
		links = append(links, GraphLink{Href: "/network/ports/" + string(p.ID), Label: portDisplayName(p)})
	}
	panels := make([]GraphPanel, 0, 5)
	if len(ports) > 0 {
		// Device totals sum physical interfaces only: VLAN SVIs, LAG bundles
		// and loopbacks re-report traffic that already flows through their
		// member ports, which double-counts the device total (LibreNMS
		// excludes them from device graphs the same way).
		trafficPortIDs := make([]ID, 0, len(ports))
		for _, port := range aggregatablePorts(ports) {
			trafficPortIDs = append(trafficPortIDs, port.ID)
		}
		panels = append(panels,
			GraphPanel{
				ID:    "overall-traffic",
				Title: "Overall Traffic",
				Type:  "line",
				Unit:  "bps",
				// A switch's summed In and Out totals are near-identical by
				// definition, so the two lines coincide; mirror Out below the
				// axis (LibreNMS-style) so both stay readable.
				Stack: "signed",
				Queries: []GraphQuery{
					{Metric: MetricSNMPIfInBps, Scope: graphScopePortsAggregate, Aggregation: "sum", Label: "In", PortIDs: trafficPortIDs},
					{Metric: MetricSNMPIfOutBps, Scope: graphScopePortsAggregate, Aggregation: "sum", Label: "Out", PortIDs: trafficPortIDs, Transform: GraphQueryTransform{Negative: true}},
				},
				Links:        links,
				QueryOptions: defaultGraphQueryOptions(),
			},
			GraphPanel{
				ID:    "interface-errors",
				Title: "Interface Errors",
				Type:  "line",
				Unit:  "pps",
				Queries: []GraphQuery{
					{Metric: MetricSNMPIfInErrorsTotal, Scope: graphScopePortsAggregate, Aggregation: "sum", Label: "In errors", Transform: GraphQueryTransform{Rate: true}, PortIDs: trafficPortIDs},
					{Metric: MetricSNMPIfOutErrorsTotal, Scope: graphScopePortsAggregate, Aggregation: "sum", Label: "Out errors", Transform: GraphQueryTransform{Rate: true}, PortIDs: trafficPortIDs},
				},
				QueryOptions: defaultGraphQueryOptions(),
			},
		)
	}
	// CPU / Memory — always include (core metric; renderer shows no_data if absent).
	panels = append(panels, GraphPanel{
		ID:    "cpu-memory",
		Title: "CPU / Memory",
		Type:  "line",
		Unit:  "percent",
		Queries: []GraphQuery{
			{Metric: MetricSNMPDeviceCPUPercent, Scope: graphScopeDevice, Label: "CPU"},
			{Metric: MetricSNMPDeviceMemPercent, Scope: graphScopeDevice, Label: "Memory"},
		},
		QueryOptions: defaultGraphQueryOptions(),
	})
	if hasOpticalSensor(sensors) {
		panels = append(panels, GraphPanel{
			ID:    "optical-power",
			Title: "Optical Power",
			Type:  "line",
			Unit:  "dbm",
			Queries: []GraphQuery{
				// Optical series carry sensor labels, not port_id, so a
				// device-scope selector is the one that matches them.
				{Metric: MetricSNMPOpticalRxDBM, Scope: graphScopeDevice, Label: "RX"},
				{Metric: MetricSNMPOpticalTxDBM, Scope: graphScopeDevice, Label: "TX"},
			},
			QueryOptions: defaultGraphQueryOptions(),
		})
	}
	if len(bgpSessions) > 0 {
		panels = append(panels, GraphPanel{
			ID:    "bgp-prefixes",
			Title: "BGP Prefixes",
			Type:  "line",
			Unit:  "count",
			Queries: []GraphQuery{
				{Metric: MetricBGPAcceptedPrefixes, Scope: graphScopeDevice, Label: "Accepted"},
				{Metric: MetricBGPDeniedPrefixes, Scope: graphScopeDevice, Label: "Denied"},
				{Metric: MetricBGPAdvertisedPrefixes, Scope: graphScopeDevice, Label: "Advertised"},
			},
			QueryOptions: defaultGraphQueryOptions(),
		})
	}
	return GraphDashboard{
		ID:      "network-device-overview:" + string(device.ID),
		Title:   deviceDisplayTitle(device),
		Refresh: 30,
		Panels:  panels,
	}
}

// hasOpticalSensor reports whether the device has sensors indicating optical
// power monitoring (transceiver DOM). The class or unit must reference dBm,
// optical, power, or similar. This avoids showing an Optical Power panel for
// devices without transceivers.
func hasOpticalSensor(sensors []NetworkDeviceSensor) bool {
	for _, s := range sensors {
		combined := strings.ToLower(s.Class + " " + s.Unit + " " + s.Name)
		if strings.Contains(combined, "dbm") || strings.Contains(combined, "optical") || strings.Contains(combined, "rx") || strings.Contains(combined, "tx") || strings.Contains(combined, "power") || strings.Contains(combined, "dom") || strings.Contains(combined, "transceiver") {
			return true
		}
	}
	return false
}

func deviceDisplayTitle(device NetworkDevice) string {
	if device.SysName != "" {
		return device.SysName
	}
	if device.Model != "" {
		return device.Model
	}
	return string(device.ID)
}

// NewNetworkPortOverviewDashboard builds the per-port dashboard (LibreNMS port
// graphs): Traffic, Errors/CRC, Optical Power. Queries are port-scoped.
func NewNetworkPortOverviewDashboard(port NetworkPort) GraphDashboard {
	return GraphDashboard{
		ID:      "network-port-overview:" + string(port.ID),
		Title:   portDisplayName(port),
		Refresh: 30,
		Panels: []GraphPanel{
		{
			ID:    "traffic",
			Title: "Traffic",
			Type:  "line",
			Unit:  "bps",
			Stack: "signed",
			Queries: []GraphQuery{
				{Metric: MetricSNMPIfInBps, Scope: graphScopePort, PortID: port.ID, Label: "In"},
				{Metric: MetricSNMPIfOutBps, Scope: graphScopePort, PortID: port.ID, Label: "Out", Transform: GraphQueryTransform{Negative: true}},
			},
			QueryOptions: defaultGraphQueryOptions(),
		},
			{
				ID:    "errors",
				Title: "Errors / CRC",
				Type:  "line",
				Unit:  "pps",
				Queries: []GraphQuery{
					{Metric: MetricSNMPIfInErrorsTotal, Scope: graphScopePort, PortID: port.ID, Label: "In errors", Transform: GraphQueryTransform{Rate: true}},
					{Metric: MetricSNMPIfOutErrorsTotal, Scope: graphScopePort, PortID: port.ID, Label: "Out errors", Transform: GraphQueryTransform{Rate: true}},
					{Metric: MetricSNMPIfInCRCTotal, Scope: graphScopePort, PortID: port.ID, Label: "In CRC", Transform: GraphQueryTransform{Rate: true}},
					{Metric: MetricSNMPIfOutCRCTotal, Scope: graphScopePort, PortID: port.ID, Label: "Out CRC", Transform: GraphQueryTransform{Rate: true}},
				},
				QueryOptions: defaultGraphQueryOptions(),
			},
			{
				ID:    "optical",
				Title: "Optical Power",
				Type:  "line",
				Unit:  "dbm",
				Queries: []GraphQuery{
					{Metric: MetricSNMPOpticalRxDBM, Scope: graphScopePort, PortID: port.ID, Label: "RX"},
					{Metric: MetricSNMPOpticalTxDBM, Scope: graphScopePort, PortID: port.ID, Label: "TX"},
				},
				QueryOptions: defaultGraphQueryOptions(),
			},
		},
	}
}

func portDisplayName(port NetworkPort) string {
	if port.IfName != "" {
		return port.IfName
	}
	if port.IfDescr != "" {
		return port.IfDescr
	}
	return string(port.ID)
}

// ---------------------------------------------------------------------------
// Resolve support (used by the frontend renderer via the metrics API, or by a
// future /graph/resolve endpoint). Kept here so the time/step + gap-aware
// series conversion is standardized in one place.
// ---------------------------------------------------------------------------

// GraphSeries is resolved data for one query.
type GraphSeries struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Points []GraphPoint      `json:"points"`
	Stats  GraphSeriesStats  `json:"stats,omitempty"`
}

// GraphPoint is one sample. Time is unix seconds; Value is nil for a gap.
type GraphPoint struct {
	Time  int64    `json:"time"`
	Value *float64 `json:"value"`
}

type GraphSeriesStats struct {
	Last float64 `json:"last"`
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Avg  float64 `json:"avg"`
}

// GraphTimeParams carries Grafana-style time inputs.
type GraphTimeParams struct {
	From          time.Time
	To            time.Time
	Range         time.Duration
	MaxDataPoints int
	MinInterval   time.Duration
}

const (
	graphLegacyDefaultRange         = 24 * time.Hour
	graphLegacyDefaultMaxDataPoints = 600
)

// parseGraphTimeParams parses from/to/range/max_data_points/min_interval.
func parseGraphTimeParams(r *http.Request, now time.Time) (GraphTimeParams, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	p := GraphTimeParams{Range: graphLegacyDefaultRange, MaxDataPoints: graphLegacyDefaultMaxDataPoints}
	q := r.URL.Query()
	if s := q.Get("from"); s != "" {
		t, err := parseGraphTime(s)
		if err != nil {
			return p, fmt.Errorf("invalid from: %w", err)
		}
		p.From = t
	}
	if s := q.Get("to"); s != "" {
		t, err := parseGraphTime(s)
		if err != nil {
			return p, fmt.Errorf("invalid to: %w", err)
		}
		p.To = t
	}
	if s := q.Get("range"); s != "" {
		d, err := parseRangeDuration(s)
		if err != nil {
			return p, fmt.Errorf("invalid range: %w", err)
		}
		p.Range = d
	}
	if s := q.Get("max_data_points"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return p, fmt.Errorf("invalid max_data_points")
		}
		p.MaxDataPoints = n
	}
	if s := q.Get("min_interval"); s != "" {
		d, err := parseRangeDuration(s)
		if err != nil {
			return p, fmt.Errorf("invalid min_interval: %w", err)
		}
		p.MinInterval = d
	}
	if p.From.IsZero() && p.To.IsZero() {
		p.To = now
		p.From = now.Add(-p.Range)
	} else if p.From.IsZero() {
		p.From = p.To.Add(-p.Range)
	} else if p.To.IsZero() {
		p.To = p.From.Add(p.Range)
	}
	if !p.To.After(p.From) {
		return p, fmt.Errorf("to must be after from")
	}
	return p, nil
}

// Step computes max(range/maxDataPoints, minInterval), snapped to an allowed
// query step. scrapeInterval is an optional floor (device sample step).
func (p GraphTimeParams) Step(scrapeInterval time.Duration) time.Duration {
	step := AutoQueryStep(p.To.Sub(p.From), p.MaxDataPoints)
	floor := p.MinInterval
	if scrapeInterval > floor {
		floor = scrapeInterval
	}
	if floor > step {
		for _, allowed := range AllowedQuerySteps {
			if allowed >= floor {
				step = allowed
				break
			}
		}
	}
	if step <= 0 {
		step = time.Minute
	}
	return step
}

func parseGraphTime(value string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	if epoch, err := strconv.ParseInt(value, 10, 64); err == nil {
		seconds := epoch
		if epoch > 10_000_000_000 {
			seconds = epoch / 1000
		}
		return time.Unix(seconds, 0).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("not RFC3339 or epoch")
}

// parseRangeDuration accepts "5m", "1h", "24h", "7d", "2h30m", or seconds.
func parseRangeDuration(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("empty")
	}
	if d, err := time.ParseDuration(value); err == nil {
		return d, nil
	}
	if strings.HasSuffix(value, "d") {
		days, err := strconv.ParseFloat(strings.TrimSuffix(value, "d"), 64)
		if err == nil {
			return time.Duration(days * 24 * float64(time.Hour)), nil
		}
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Duration(seconds) * time.Second, nil
	}
	return 0, fmt.Errorf("unsupported duration %q", value)
}

// vmResponseToGraphSeries converts a VM range response into gap-aware series
// on a uniform step grid. Missing grid points become nil.
func vmResponseToGraphSeries(response VictoriaMetricsResponse, from, to time.Time, step time.Duration) []GraphSeries {
	if len(response.Data.Result) == 0 {
		return nil
	}
	grid := timeGrid(from, to, step)
	out := make([]GraphSeries, 0, len(response.Data.Result))
	for _, item := range response.Data.Result {
		byTime := make(map[int64]float64, len(item.Values))
		for _, v := range item.Values {
			byTime[v.Time.Unix()] = v.Value
		}
		points := make([]GraphPoint, 0, len(grid))
		var acc seriesStatsAcc
		for _, t := range grid {
			ts := t.Unix()
			if value, ok := byTime[ts]; ok {
				v := value
				points = append(points, GraphPoint{Time: ts, Value: &v})
				acc.add(value)
			} else {
				points = append(points, GraphPoint{Time: ts})
			}
		}
		out = append(out, GraphSeries{
			Name:   graphSeriesName(item.Metric),
			Labels: item.Metric,
			Points: points,
			Stats:  acc.finalize(),
		})
	}
	return out
}

func timeGrid(from, to time.Time, step time.Duration) []time.Time {
	if step <= 0 {
		return nil
	}
	align := from.UTC().Truncate(step)
	if align.Before(from) {
		align = align.Add(step)
	}
	var grid []time.Time
	for t := align; !t.After(to); t = t.Add(step) {
		grid = append(grid, t)
	}
	return grid
}

type seriesStatsAcc struct {
	count int
	sum   float64
	min   float64
	max   float64
	last  float64
	has   bool
}

func (a *seriesStatsAcc) add(v float64) {
	if !a.has {
		a.min, a.max, a.has = v, v, true
	} else {
		if v < a.min {
			a.min = v
		}
		if v > a.max {
			a.max = v
		}
	}
	a.sum += v
	a.count++
	a.last = v
}

func (a *seriesStatsAcc) finalize() GraphSeriesStats {
	if !a.has {
		return GraphSeriesStats{}
	}
	return GraphSeriesStats{Last: a.last, Min: a.min, Max: a.max, Avg: a.sum / float64(a.count)}
}

func graphSeriesName(metric map[string]string) string {
	for _, key := range []string{"label", "direction", "value_mode", "__name__"} {
		if v := metric[key]; v != "" {
			return v
		}
	}
	return "series"
}
