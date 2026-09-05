// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowmetrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

func ValidateListenAddress(address string) error {
	address = strings.TrimSpace(address)
	if address == "" {
		return nil
	}
	host, portText, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) != host || strings.ContainsAny(host, "/\\\t\r\n ") {
		return errors.New("metrics listen address must be host:port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("metrics listen port must be 1..65535")
	}
	return nil
}

type Server struct {
	server   *http.Server
	listener net.Listener
	done     chan struct{}
	mu       sync.Mutex
	err      error
}

// StartServer binds synchronously so configuration and port conflicts fail
// startup instead of becoming a silent background goroutine failure.
func StartServer(address string, handler http.Handler) (*Server, error) {
	address = strings.TrimSpace(address)
	if address == "" || handler == nil {
		return nil, errors.New("metrics listen address and handler are required")
	}
	if err := ValidateListenAddress(address); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return startServerOnListener(listener, handler), nil
}

func startServerOnListener(listener net.Listener, handler http.Handler) *Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", handler)
	server := &Server{
		listener: listener,
		server: &http.Server{
			Handler: mux, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second,
			IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10,
		},
		done: make(chan struct{}),
	}
	go func() {
		err := server.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		server.mu.Lock()
		server.err = err
		server.mu.Unlock()
		close(server.done)
	}()
	return server
}

func (s *Server) Address() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) Done() <-chan struct{} {
	if s == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return s.done
}

func (s *Server) Wait() error {
	if s == nil {
		return nil
	}
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	shutdownErr := s.server.Shutdown(ctx)
	return errors.Join(shutdownErr, s.Wait())
}
