package watchdog

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/netsampler/goflow2/v3/decoders/sflow"
	"github.com/netsampler/goflow2/v3/decoders/utils"
)

type SFlowCollector struct {
	ListenAddr         string
	Network            NetworkRepository
	AddressSets        AddressSetRepository
	VMClient           VictoriaMetricsClient
	TenantID           ID
	AggInterval        time.Duration
	PrefixSyncInterval time.Duration
	Matcher            *PrefixMatcher
	aggregator         map[string]*flowAggregate
	aggMu              sync.Mutex
	lastPrefixSync     time.Time
}

type flowAggregate struct {
	DeviceID    string
	PortID      string
	IfName      string
	AddressSets string
	Direction   string
	Proto       string
	Bytes       uint64
	Samples     uint64
}

type sflowFlowRecord struct {
	TimeReceived time.Time
	AgentIP      string
	DeviceID     string
	PortID       string
	IfName       string
	IfIndex      uint32
	Direction    string
	SrcIP        string
	DstIP        string
	SrcPort      uint16
	DstPort      uint16
	Proto        uint8
	Bytes        uint64
	SamplingRate uint64
	AddressSets  []string
	SrcLabels    map[string]string
	DstLabels    map[string]string
}

func (c *SFlowCollector) Run(ctx context.Context) error {
	if c.Matcher == nil {
		c.Matcher = NewPrefixMatcher()
	}
	if c.AggInterval <= 0 {
		c.AggInterval = 60 * time.Second
	}
	if c.PrefixSyncInterval <= 0 {
		c.PrefixSyncInterval = 5 * time.Minute
	}
	c.aggregator = make(map[string]*flowAggregate)

	addr, err := net.ResolveUDPAddr("udp", c.ListenAddr)
	if err != nil {
		return fmt.Errorf("resolve udp addr: %w", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listen udp: %w", err)
	}
	defer conn.Close()

	log.Printf("sflow collector listening on %s", c.ListenAddr)

	go c.aggLoop(ctx)

	buf := make([]byte, 65536)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("sflow read error: %v", err)
			continue
		}
		payload := bytes.NewBuffer(buf[:n])
		var packet sflow.Packet
		if err := sflow.DecodeMessage(payload, &packet); err != nil {
			continue
		}
		c.syncPrefixesIfNeeded(ctx)
		agentIP := net.IP(utils.IPAddress(packet.AgentIP)).String()
		c.processPacket(ctx, packet, agentIP)
	}
}

func (c *SFlowCollector) processPacket(ctx context.Context, packet sflow.Packet, agentIP string) {
	device, err := c.findDevice(ctx, agentIP)
	if err != nil {
		return
	}
	for _, sample := range packet.Samples {
		var records []sflow.FlowRecord
		var samplingRate uint64
		var inIf, outIf uint32

		switch fs := sample.(type) {
		case sflow.FlowSample:
			records = fs.Records
			samplingRate = uint64(fs.SamplingRate)
			inIf = fs.Input & 0x3fffffff
			outIf = fs.Output & 0x3fffffff
		case sflow.ExpandedFlowSample:
			records = fs.Records
			samplingRate = uint64(fs.SamplingRate)
			if fs.InputIfFormat == 0 {
				inIf = fs.InputIfValue
			}
			if fs.OutputIfFormat == 0 {
				outIf = fs.OutputIfValue
			}
		default:
			continue
		}

		if samplingRate == 0 {
			samplingRate = 1
		}

		port, portID, ifName, direction := c.resolvePort(device.ID, inIf, outIf)

		for _, record := range records {
			var flow sflowFlowRecord
			flow.TimeReceived = time.Now().UTC()
			flow.AgentIP = agentIP
			flow.DeviceID = string(device.ID)
			flow.PortID = portID
			flow.IfName = ifName
			flow.IfIndex = func() uint32 {
				if direction == "in" {
					return inIf
				}
				return outIf
			}()
			flow.Direction = direction
			flow.SamplingRate = samplingRate

			switch rd := record.Data.(type) {
			case sflow.SampledIPv4:
				flow.SrcIP = ipBytesToString(rd.SrcIP)
				flow.DstIP = ipBytesToString(rd.DstIP)
				flow.SrcPort = uint16(rd.SrcPort)
				flow.DstPort = uint16(rd.DstPort)
				flow.Proto = uint8(rd.Protocol)
				flow.Bytes = uint64(rd.Length)
			case sflow.SampledIPv6:
				flow.SrcIP = ipBytesToString(rd.SrcIP)
				flow.DstIP = ipBytesToString(rd.DstIP)
				flow.SrcPort = uint16(rd.SrcPort)
				flow.DstPort = uint16(rd.DstPort)
				flow.Proto = uint8(rd.Protocol)
				flow.Bytes = uint64(rd.Length)
			default:
				continue
			}

			if flow.SrcIP != "" {
				flow.SrcLabels = c.Matcher.Match(flow.SrcIP)
			}
			if flow.DstIP != "" {
				flow.DstLabels = c.Matcher.Match(flow.DstIP)
			}
			flow.AddressSets = c.matchAddressSets(flow.SrcLabels, flow.DstLabels)

			c.accumulate(flow)
		}
		_ = port
	}
}

func (c *SFlowCollector) findDevice(ctx context.Context, agentIP string) (NetworkDevice, error) {
	devices, err := c.Network.ListDevices(ctx, c.TenantID)
	if err != nil {
		return NetworkDevice{}, err
	}
	for _, d := range devices {
		if d.SysName == agentIP || d.SysDescr == agentIP {
			return d, nil
		}
	}
	if len(devices) == 1 {
		return devices[0], nil
	}
	return NetworkDevice{}, fmt.Errorf("device not found for agent %s", agentIP)
}

func (c *SFlowCollector) resolvePort(deviceID ID, inIf, outIf uint32) (NetworkPort, string, string, string) {
	direction := "out"
	ifIndex := outIf
	if ifIndex == 0 && inIf != 0 {
		ifIndex = inIf
		direction = "in"
	}
	ports, _ := c.Network.ListPorts(context.Background(), c.TenantID, deviceID)
	for _, p := range ports {
		if uint32(p.IfIndex) == ifIndex {
			return p, string(p.ID), p.IfName, direction
		}
	}
	return NetworkPort{}, "", "", direction
}

func (c *SFlowCollector) matchAddressSets(srcLabels, dstLabels map[string]string) []string {
	var allMatched []string
	seen := map[string]bool{}
	if srcLabels != nil {
		for _, name := range c.Matcher.MatchSets(srcLabels) {
			if !seen[name] {
				allMatched = append(allMatched, name)
				seen[name] = true
			}
		}
	}
	if dstLabels != nil {
		for _, name := range c.Matcher.MatchSets(dstLabels) {
			if !seen[name] {
				allMatched = append(allMatched, name)
				seen[name] = true
			}
		}
	}
	return allMatched
}

func (c *SFlowCollector) accumulate(flow sflowFlowRecord) {
	c.aggMu.Lock()
	defer c.aggMu.Unlock()
	setsKey := "unknown"
	if len(flow.AddressSets) > 0 {
		setsKey = flow.AddressSets[0]
	}
	key := fmt.Sprintf("%s|%s|%s|%s|%d", flow.DeviceID, flow.PortID, setsKey, flow.Direction, flow.Proto)
	agg, ok := c.aggregator[key]
	if !ok {
		agg = &flowAggregate{
			DeviceID:    flow.DeviceID,
			PortID:      flow.PortID,
			IfName:      flow.IfName,
			AddressSets: setsKey,
			Direction:   flow.Direction,
			Proto:       fmt.Sprintf("%d", flow.Proto),
		}
		c.aggregator[key] = agg
	}
	agg.Bytes += flow.Bytes * flow.SamplingRate
	agg.Samples++
}

func (c *SFlowCollector) aggLoop(ctx context.Context) {
	ticker := time.NewTicker(c.AggInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.flushAggregation(ctx)
		}
	}
}

func (c *SFlowCollector) flushAggregation(ctx context.Context) {
	c.aggMu.Lock()
	snapshot := c.aggregator
	c.aggregator = make(map[string]*flowAggregate)
	c.aggMu.Unlock()

	if len(snapshot) == 0 {
		return
	}
	now := time.Now().UTC()
	var sb bytes.Buffer
	ts := now.UnixMilli()
	for _, agg := range snapshot {
		metric := fmt.Sprintf("watchdog_sflow_traffic_bytes{tenant_id=\"%s\",device_id=\"%s\",port_id=\"%s\",if_name=\"%s\",address_set=\"%s\",direction=\"%s\",proto=\"%s\"} %d %d\n",
			c.TenantID, agg.DeviceID, agg.PortID, agg.IfName, agg.AddressSets, agg.Direction, agg.Proto, agg.Bytes, ts)
		sb.WriteString(metric)
	}
	if c.VMClient.BaseURL != "" && sb.Len() > 0 {
		if err := c.VMClient.ImportPrometheus(ctx, sb.Bytes()); err != nil {
			log.Printf("sflow vm write error: %v", err)
		}
	}
	log.Printf("sflow aggregated: %d series flushed", len(snapshot))
}

func (c *SFlowCollector) syncPrefixesIfNeeded(ctx context.Context) {
	if time.Since(c.lastPrefixSync) < c.PrefixSyncInterval {
		return
	}
	c.lastPrefixSync = time.Now()
	prefixes, err := c.AddressSets.ListAddressPrefixes(ctx, c.TenantID)
	if err != nil {
		log.Printf("sflow prefix sync error: %v", err)
		return
	}
	c.Matcher.LoadPrefixes(prefixes)
	sets, err := c.AddressSets.ListAddressSets(ctx, c.TenantID)
	if err != nil {
		log.Printf("sflow set sync error: %v", err)
		return
	}
	c.Matcher.LoadSets(sets)
	log.Printf("sflow prefix sync: %d prefixes, %d sets loaded", len(prefixes), len(sets))
}

func ipBytesToString(b []byte) string {
	if len(b) == 4 {
		return net.IP(b).String()
	}
	if len(b) == 16 {
		return net.IP(b).String()
	}
	return ""
}
