// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

const (
	proxyForward int32 = iota
	proxyWaitServerResponse
	proxyWaitClientWrite
	proxyDropResponses
	proxyCutResponse
)

// TestRealClickHouseSilentResponseHonorsOperationTimeout uses a transparent TCP
// proxy that becomes a one-way black hole after a successful control query.
// This proves that the production operation deadline bounds ch-go's packet
// polling and that a migration statement is not retried implicitly.
func TestRealClickHouseSilentResponseHonorsOperationTimeout(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_FAULT_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_FAULT_INTEGRATION=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	proxy := newSilentResponseProxy(t, "127.0.0.1:9000")
	defer proxy.Close()
	config := realMigrationConfig(t, "watchdog-flow-silent-timeout", 250*time.Millisecond)
	config.ReadTimeout = 50 * time.Millisecond
	config.Address = proxy.Address()
	config.MaxConns = 1
	config.MinConns = 1
	native, err := NewNativeInserter(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()

	probe := synchronousMigrationQuery("INSERT INTO TABLE FUNCTION null('value UInt8') SELECT 1")
	if err := native.executor.Do(ctx, probe); err != nil {
		t.Fatalf("ClickHouse control query through proxy: %v", err)
	}
	if got := proxy.AcceptedConnections(); got != 1 {
		t.Fatalf("ClickHouse control query used %d connections, want 1", got)
	}

	proxy.DropServerResponses()
	started := time.Now()
	err = native.executor.Do(ctx, probe)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("ClickHouse query unexpectedly succeeded after responses were dropped")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("silent ClickHouse response did not reach operation deadline: %T %v", err, err)
	}
	if elapsed < 150*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("ClickHouse operation timeout elapsed=%s, want 150ms..2s", elapsed)
	}
	if got := proxy.AcceptedConnections(); got != 1 {
		t.Fatalf("silent migration statement was retried on %d connections", got)
	}

	recoveryConfig := realMigrationConfig(t, "watchdog-flow-silent-timeout-recovery", time.Second)
	recoveryConfig.MaxConns = 1
	recoveryConfig.MinConns = 1
	recovered, err := NewNativeInserter(ctx, recoveryConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if err := recovered.executor.Do(ctx, probe); err != nil {
		t.Fatalf("ClickHouse query did not recover with a fresh pool: %v", err)
	}
}

// TestRealClickHouseLostInsertResponseConvergesAfterReplay models the
// ambiguous commit boundary: ClickHouse receives and commits the fact insert,
// but the client never sees its final response and therefore cannot write the
// receipt. Replaying the exact prepared block must produce one logical fact
// set and one matching receipt.
func TestRealClickHouseLostInsertResponseConvergesAfterReplay(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_FAULT_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_FAULT_INTEGRATION=1 to run")
	}
	ctx, direct := openDataIntegrationClickHouse(t, "watchdog_flow_it_lost_insert_response")
	eventTime := time.Date(2026, 9, 6, 1, 2, 0, 0, time.UTC)
	batch := integrationBatch(99, eventTime.Add(time.Minute),
		integrationRecord(1, eventTime, "geo-city-a", 100),
		integrationRecord(2, eventTime.Add(time.Second), "geo-city-b", 200),
	)
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{batch}, BatchLimits{})
	if err != nil || len(blocks) != 1 {
		t.Fatalf("prepare blocks=%d error=%v", len(blocks), err)
	}
	block := blocks[0]

	proxy := newSilentResponseProxy(t, "127.0.0.1:9000")
	defer proxy.Close()
	faultConfig := realMigrationConfig(t, "watchdog-flow-lost-insert-response", 250*time.Millisecond)
	faultConfig.Address = proxy.Address()
	faultConfig.Database = "watchdog_flow_it_lost_insert_response"
	faultConfig.ReadTimeout = 50 * time.Millisecond
	faulty, err := NewNativeInserter(ctx, faultConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := faulty.executor.Do(ctx, synchronousMigrationQuery("INSERT INTO TABLE FUNCTION null('value UInt8') SELECT 1")); err != nil {
		faulty.Close()
		t.Fatalf("ClickHouse control query through proxy: %v", err)
	}
	// The first server read carries INSERT column metadata. Forwarding it lets
	// the client send the block; all later responses, including EOS, disappear.
	proxy.DropServerResponsesAfterNextClientWrite()
	err = faulty.InsertFlowBlock(ctx, block)
	faulty.Close()
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost INSERT response error=%v, want operation deadline", err)
	}
	if got := proxy.AcceptedConnections(); got != 1 {
		t.Fatalf("ambiguous INSERT used %d connections, want no implicit retry", got)
	}

	before := readReplayState(t, ctx, direct, block)
	if before.factCount != uint64(len(block.Records)) || before.receiptCount != 0 {
		t.Fatalf("ambiguous commit state=%+v, want committed facts without receipt", before)
	}
	if err := direct.InsertFlowBlock(ctx, block); err != nil {
		t.Fatalf("replay exact prepared block: %v", err)
	}
	after := readReplayState(t, ctx, direct, block)
	if after.factCount != uint64(len(block.Records)) || after.rawBytes != 300 || after.rawPackets != 2 ||
		after.receiptCount != 1 || after.receiptChecksum != block.Checksum {
		t.Fatalf("replayed block did not converge: %+v", after)
	}
}

func TestRealClickHousePartialResponseFailsWithoutRetry(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_FAULT_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_FAULT_INTEGRATION=1 to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	proxy := newSilentResponseProxy(t, "127.0.0.1:9000")
	defer proxy.Close()
	config := realMigrationConfig(t, "watchdog-flow-partial-response", 2*time.Second)
	config.Address = proxy.Address()
	config.ReadTimeout = 50 * time.Millisecond
	native, err := NewNativeInserter(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	if err := native.executor.Do(ctx, synchronousMigrationQuery("INSERT INTO TABLE FUNCTION null('value UInt8') SELECT 1")); err != nil {
		t.Fatalf("ClickHouse control query through proxy: %v", err)
	}

	var values proto.ColStr
	proxy.CutNextServerResponseAfterBytes(64)
	started := time.Now()
	err = native.executor.Do(ctx, ch.Query{
		Body:   "SELECT randomString(1048576) AS value",
		Result: proto.Results{{Name: "value", Data: &values}},
	})
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("partial ClickHouse response error=%v, want immediate transport/decode failure", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("partial ClickHouse response took %s, want immediate failure", elapsed)
	}
	if values.Rows() != 0 {
		t.Fatalf("partial ClickHouse response exposed %d result rows", values.Rows())
	}
	if got := proxy.AcceptedConnections(); got != 1 {
		t.Fatalf("partial response query used %d connections, want no implicit retry", got)
	}

	recoveryConfig := realMigrationConfig(t, "watchdog-flow-partial-response-recovery", 2*time.Second)
	recovered, err := NewNativeInserter(ctx, recoveryConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if err := recovered.executor.Do(ctx, synchronousMigrationQuery("INSERT INTO TABLE FUNCTION null('value UInt8') SELECT 1")); err != nil {
		t.Fatalf("ClickHouse query did not recover with a fresh pool: %v", err)
	}
}

func TestRealClickHouseReplayConvergesWithoutServerDedup(t *testing.T) {
	if os.Getenv("WATCHDOG_FLOW_CLICKHOUSE_FAULT_INTEGRATION") != "1" {
		t.Skip("set WATCHDOG_FLOW_CLICKHOUSE_FAULT_INTEGRATION=1 to run")
	}
	ctx, native := openDataIntegrationClickHouse(t, "watchdog_flow_it_no_server_dedup")
	for _, table := range []string{flowRecordsTable, flowReceiptsTable} {
		if err := native.executor.Do(ctx, synchronousMigrationQuery(
			"ALTER TABLE "+table+" MODIFY SETTING non_replicated_deduplication_window = 0",
		)); err != nil {
			t.Fatalf("disable %s server dedup: %v", table, err)
		}
		if err := native.executor.Do(ctx, synchronousMigrationQuery("SYSTEM STOP MERGES "+table)); err != nil {
			t.Fatalf("stop %s merges: %v", table, err)
		}
	}

	eventTime := time.Date(2026, 9, 6, 2, 3, 0, 0, time.UTC)
	batch := integrationBatch(100, eventTime.Add(time.Minute),
		integrationRecord(3, eventTime, "geo-city-a", 400),
		integrationRecord(4, eventTime.Add(time.Second), "geo-city-b", 500),
	)
	blocks, err := PrepareBlocks([]*flowworker.EnrichedBatch{batch}, BatchLimits{})
	if err != nil || len(blocks) != 1 {
		t.Fatalf("prepare blocks=%d error=%v", len(blocks), err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := native.InsertFlowBlock(ctx, blocks[0]); err != nil {
			t.Fatalf("insert attempt %d: %v", attempt+1, err)
		}
	}
	physicalFacts, physicalReceipts := readReplayPhysicalCounts(t, ctx, native, blocks[0])
	if physicalFacts != 4 || physicalReceipts != 2 {
		t.Fatalf("physical fact/receipt counts=%d/%d, want 4/2 with dedup disabled and merges stopped", physicalFacts, physicalReceipts)
	}
	logical := readReplayState(t, ctx, native, blocks[0])
	if logical.factCount != 2 || logical.rawBytes != 900 || logical.rawPackets != 2 ||
		logical.receiptCount != 1 || logical.receiptChecksum != blocks[0].Checksum {
		t.Fatalf("dedup-window-independent replay did not converge: %+v", logical)
	}
}

type replayState struct {
	factCount       uint64
	rawBytes        uint64
	rawPackets      uint64
	receiptCount    uint64
	receiptChecksum [32]byte
}

func readReplayState(t testing.TB, ctx context.Context, native *NativeInserter, block PreparedBlock) replayState {
	t.Helper()
	id := hex.EncodeToString(block.ID[:])
	state := replayState{}
	var factCounts, rawBytes, rawPackets proto.ColUInt64
	factQuery := ch.Query{
		Body: `SELECT count(), sum(raw_bytes), sum(raw_packets)
FROM flow_records FINAL
WHERE ingest_batch_id = unhex({batch_id:String})`,
		Parameters: ch.Parameters(map[string]any{"batch_id": id}),
		Result: proto.Results{
			{Name: "count()", Data: &factCounts},
			{Name: "sum(raw_bytes)", Data: &rawBytes},
			{Name: "sum(raw_packets)", Data: &rawPackets},
		},
	}
	if err := native.executor.Do(ctx, factQuery); err != nil {
		t.Fatalf("read replay facts: %v", err)
	}
	if len(factCounts) != 1 || len(rawBytes) != 1 || len(rawPackets) != 1 {
		t.Fatalf("fact aggregate rows count=%d raw_bytes=%d raw_packets=%d", len(factCounts), len(rawBytes), len(rawPackets))
	}
	state.factCount, state.rawBytes, state.rawPackets = factCounts[0], rawBytes[0], rawPackets[0]
	var (
		checksums proto.ColFixedStr32
		counts    proto.ColUInt64
	)
	receiptQuery := ch.Query{
		Body: `SELECT checksum, record_count
FROM flow_ingest_batches FINAL
WHERE ingest_batch_id = unhex({batch_id:String})`,
		Parameters: ch.Parameters(map[string]any{"batch_id": id}),
		Result: proto.Results{
			{Name: "checksum", Data: &checksums},
			{Name: "record_count", Data: &counts},
		},
	}
	if err := native.executor.Do(ctx, receiptQuery); err != nil {
		t.Fatalf("read replay receipt: %v", err)
	}
	if len(checksums) != counts.Rows() || len(checksums) > 1 {
		t.Fatalf("receipt rows checksum=%d count=%d", len(checksums), counts.Rows())
	}
	state.receiptCount = uint64(len(checksums))
	if len(checksums) == 1 {
		if counts[0] != uint64(len(block.Records)) {
			t.Fatalf("receipt record count=%d want=%d", counts[0], len(block.Records))
		}
		state.receiptChecksum = checksums[0]
	}
	return state
}

func readReplayPhysicalCounts(t testing.TB, ctx context.Context, native *NativeInserter, block PreparedBlock) (uint64, uint64) {
	t.Helper()
	id := hex.EncodeToString(block.ID[:])
	count := func(table string) uint64 {
		var values proto.ColUInt64
		query := ch.Query{
			Body:       "SELECT count() FROM " + table + " WHERE ingest_batch_id = unhex({batch_id:String})",
			Parameters: ch.Parameters(map[string]any{"batch_id": id}),
			Result:     proto.Results{{Name: "count()", Data: &values}},
		}
		if err := native.executor.Do(ctx, query); err != nil {
			t.Fatalf("read physical %s count: %v", table, err)
		}
		if len(values) != 1 {
			t.Fatalf("physical %s count rows=%d", table, len(values))
		}
		return values[0]
	}
	return count(flowRecordsTable), count(flowReceiptsTable)
}

type silentResponseProxy struct {
	listener net.Listener
	backend  string
	// responseMode controls the fault transition constants above.
	responseMode atomic.Int32
	cutRemaining atomic.Int64
	accepted     atomic.Uint64

	mu          sync.Mutex
	connections map[net.Conn]struct{}
	wait        sync.WaitGroup
	closeOnce   sync.Once
}

func newSilentResponseProxy(t testing.TB, backend string) *silentResponseProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &silentResponseProxy{listener: listener, backend: backend, connections: make(map[net.Conn]struct{})}
	proxy.wait.Add(1)
	go proxy.acceptLoop()
	return proxy
}

func (p *silentResponseProxy) Address() string { return p.listener.Addr().String() }

func (p *silentResponseProxy) DropServerResponses() { p.responseMode.Store(proxyDropResponses) }

func (p *silentResponseProxy) DropServerResponsesAfterNextClientWrite() {
	if !p.responseMode.CompareAndSwap(proxyForward, proxyWaitServerResponse) {
		panic("response drop transition is already active")
	}
}

func (p *silentResponseProxy) CutNextServerResponseAfterBytes(count int64) {
	if count <= 0 || !p.responseMode.CompareAndSwap(proxyForward, proxyCutResponse) {
		panic("response cut transition is invalid or already active")
	}
	p.cutRemaining.Store(count)
}

func (p *silentResponseProxy) AcceptedConnections() uint64 { return p.accepted.Load() }

func (p *silentResponseProxy) acceptLoop() {
	defer p.wait.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		backend, err := net.DialTimeout("tcp", p.backend, time.Second)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.accepted.Add(1)
		p.track(client, true)
		p.track(backend, true)
		p.wait.Add(1)
		go p.proxyConnection(client, backend)
	}
}

func (p *silentResponseProxy) proxyConnection(client, backend net.Conn) {
	defer p.wait.Done()
	defer func() {
		p.track(client, false)
		p.track(backend, false)
		_ = client.Close()
		_ = backend.Close()
	}()
	clientDone := make(chan struct{})
	go func() {
		buffer := make([]byte, 32<<10)
		for {
			count, err := client.Read(buffer)
			if count > 0 {
				// Mode 2 means the server's column metadata reached the
				// client. Drop responses before forwarding the subsequent
				// input bytes, so the server can commit but EOS cannot pass.
				p.responseMode.CompareAndSwap(proxyWaitClientWrite, proxyDropResponses)
				if writeErr := writeProxyBytes(backend, buffer[:count]); writeErr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		_ = backend.SetReadDeadline(time.Now())
		close(clientDone)
	}()

	buffer := make([]byte, 32<<10)
	for {
		count, err := backend.Read(buffer)
		if count > 0 {
			switch p.responseMode.Load() {
			case proxyDropResponses:
			case proxyCutResponse:
				remaining := p.cutRemaining.Load()
				if remaining <= int64(count) {
					if remaining > 0 {
						_ = writeProxyBytes(client, buffer[:remaining])
					}
					return
				}
				if err := writeProxyBytes(client, buffer[:count]); err != nil {
					return
				}
				p.cutRemaining.Add(-int64(count))
			default:
				if writeErr := writeProxyBytes(client, buffer[:count]); writeErr != nil {
					return
				}
				p.responseMode.CompareAndSwap(proxyWaitServerResponse, proxyWaitClientWrite)
			}
		}
		if err != nil {
			return
		}
		select {
		case <-clientDone:
			return
		default:
		}
	}
}

func writeProxyBytes(connection net.Conn, data []byte) error {
	for len(data) > 0 {
		count, err := connection.Write(data)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		data = data[count:]
	}
	return nil
}

func (p *silentResponseProxy) track(connection net.Conn, add bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if add {
		p.connections[connection] = struct{}{}
	} else {
		delete(p.connections, connection)
	}
}

func (p *silentResponseProxy) Close() {
	p.closeOnce.Do(func() {
		_ = p.listener.Close()
		p.mu.Lock()
		connections := make([]net.Conn, 0, len(p.connections))
		for connection := range p.connections {
			connections = append(connections, connection)
		}
		p.mu.Unlock()
		for _, connection := range connections {
			_ = connection.Close()
		}
		p.wait.Wait()
	})
}
