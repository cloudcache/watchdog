// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowch

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

type silentResponseProxy struct {
	listener net.Listener
	backend  string
	drop     atomic.Bool
	accepted atomic.Uint64

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

func (p *silentResponseProxy) DropServerResponses() { p.drop.Store(true) }

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
		_, _ = io.Copy(backend, client)
		_ = backend.SetReadDeadline(time.Now())
		close(clientDone)
	}()

	buffer := make([]byte, 32<<10)
	for {
		count, err := backend.Read(buffer)
		if count > 0 && !p.drop.Load() {
			if _, writeErr := client.Write(buffer[:count]); writeErr != nil {
				return
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
