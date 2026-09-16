// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream"
	"github.com/cloudcache/watchdog/internal/flowtombstone"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

func TestMetricsListenerCanBeDisabledWithoutBinding(t *testing.T) {
	server, err := startMetricsServer("", http.NotFoundHandler())
	if err != nil || server != nil {
		t.Fatalf("server=%v error=%v", server, err)
	}
}

func TestBuildConsumerConfigMapsSecurityAndTemplateReplay(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "kafka-password")
	if err := os.WriteFile(secret, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := buildConsumerConfig(options{
		brokers: "kafka-1:9093,kafka-2:9093", topic: "watchdog.flow.raw", clientID: "worker-a", consumerGroup: "worker-v1",
		fetchMinBytes: 1024, fetchMaxWait: time.Second, templateReplayRecords: 2_000_000,
		kafkaTLS: true, kafkaCAFile: "ca.pem", kafkaServerName: "kafka.internal",
		saslMechanism: string(flowstream.SASLSCRAMSHA256), saslUsername: "worker", saslPasswordFile: secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Kafka.Brokers) != 2 || config.Kafka.SASL.Password != "secret" || config.TemplateReplayRecords != 2_000_000 {
		t.Fatalf("unexpected consumer config: %+v", config)
	}
}

func TestBuildClickHouseConfigRejectsHiddenTLSAndInlinePassword(t *testing.T) {
	base := options{
		clickHouseAddress: "clickhouse:9000", clickHouseDatabase: "watchdog_flow", clickHouseUser: "flow",
		clickHouseMaxConns: 8, clickHouseMinConns: 1, clickHouseDialTimeout: time.Second, clickHouseReadTimeout: time.Second,
		clickHouseOperationTimeout: 2 * time.Second,
	}
	withHiddenTLS := base
	withHiddenTLS.clickHouseCAFile = "ca.pem"
	if _, err := buildClickHouseConfig(withHiddenTLS); err == nil {
		t.Fatal("ClickHouse TLS files were accepted while TLS was disabled")
	}
	secret := filepath.Join(t.TempDir(), "clickhouse-password")
	if err := os.WriteFile(secret, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	base.clickHousePasswordFile = secret
	config, err := buildClickHouseConfig(base)
	if err != nil || config.Password != "secret" || config.OperationTimeout != 2*time.Second {
		t.Fatalf("config=%+v error=%v", config, err)
	}
}

func TestLoadPublicationIsStrictAndResolvesObjectPaths(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "publication.json")
	publication := flowworker.EnrichmentVersionPublication{
		PublicationID: "publication-1", DimensionSnapshotID: "snapshot-a", DimensionVersion: 1,
		DimensionEffectiveFrom: time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC),
		Dimension:              flowworker.VersionObjectReference{ObjectRef: "dimension.json", Checksum: "sha256:" + string(make([]byte, 64))},
		ClassificationVersion:  1, ClassificationEffectiveFrom: time.Date(2026, 9, 5, 1, 0, 0, 0, time.UTC),
		Classification: flowworker.VersionObjectReference{ObjectRef: "classification.json", Checksum: "sha256:" + string(make([]byte, 64))},
	}
	data, err := json.Marshal(publication)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadPublication(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Dimension.ObjectRef != filepath.Join(directory, "dimension.json") || loaded.Classification.ObjectRef != filepath.Join(directory, "classification.json") {
		t.Fatalf("relative refs were not resolved: %+v", loaded)
	}
	data[len(data)-1] = ','
	data = append(data, []byte(`"unexpected":true}`)...)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPublication(path); err == nil {
		t.Fatal("unknown publication field was accepted")
	}
}

func TestLoadRemoteVersionsCheckIsOffline(t *testing.T) {
	directory := t.TempDir()
	token := filepath.Join(directory, "agent-token")
	if err := os.WriteFile(token, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	versions, syncer, cursor, err := loadRemoteVersions(context.Background(), options{
		controlPlaneURL: "http://127.0.0.1:1", agentTokenFile: token,
		versionLKGDir: filepath.Join(directory, "lkg"), versionRefreshInterval: time.Minute,
		controlPlaneTimeout: 5 * time.Second, check: true,
	}, flowworker.VersionWorkerIdentity{WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "test"})
	if err != nil || versions == nil || syncer == nil || cursor != 0 {
		t.Fatalf("versions=%v syncer=%v cursor=%d error=%v", versions, syncer, cursor, err)
	}
}

func TestBuildVersionHTTPClientRejectsCleartextRemoteHost(t *testing.T) {
	token := filepath.Join(t.TempDir(), "agent-token")
	if err := os.WriteFile(token, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := buildVersionHTTPClient(options{
		controlPlaneURL: "http://watchdog.example", agentTokenFile: token, controlPlaneTimeout: 5 * time.Second,
	}, flowworker.VersionWorkerIdentity{WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "test"})
	if err == nil {
		t.Fatal("cleartext non-loopback control plane was accepted")
	}
}

func TestLoadRawDeleteBarrierPersistsThenFallsBackToLKG(t *testing.T) {
	directory := t.TempDir()
	token := filepath.Join(directory, "agent-token")
	if err := os.WriteFile(token, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	barrier, err := flowtombstone.Advance(nil, time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), "barrier-startup", 2, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	acks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Watchdog-Agent-Token") != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/raw-delete-barrier"):
			_ = json.NewEncoder(w).Encode(barrier)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/raw-delete-barriers/"+barrier.ID+"/ack"):
			acks++
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	identity := flowworker.VersionWorkerIdentity{WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "test"}
	opt := options{
		controlPlaneURL: server.URL, agentTokenFile: token, versionLKGDir: filepath.Join(directory, "lkg"),
		controlPlaneTimeout: 5 * time.Second,
	}
	guard, _, err := loadRawDeleteBarrier(context.Background(), opt, identity)
	if err != nil || acks != 1 {
		server.Close()
		t.Fatalf("load barrier: acks=%d err=%v", acks, err)
	}
	if current, ok := guard.Current(); !ok || current.ID != barrier.ID {
		server.Close()
		t.Fatalf("installed barrier=%+v ok=%t", current, ok)
	}
	server.Close()
	guard, _, err = loadRawDeleteBarrier(context.Background(), opt, identity)
	if err != nil {
		t.Fatalf("LKG fallback: %v", err)
	}
	if current, ok := guard.Current(); !ok || current.ID != barrier.ID {
		t.Fatalf("restored barrier=%+v ok=%t", current, ok)
	}
}

func TestLoadRawDeleteBarrierFailsClosedWithoutLKG(t *testing.T) {
	directory := t.TempDir()
	token := filepath.Join(directory, "agent-token")
	if err := os.WriteFile(token, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err := loadRawDeleteBarrier(context.Background(), options{
		controlPlaneURL: "http://127.0.0.1:1", agentTokenFile: token, versionLKGDir: filepath.Join(directory, "lkg"),
		controlPlaneTimeout: 5 * time.Second,
	}, flowworker.VersionWorkerIdentity{WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "test"})
	if err == nil || !strings.Contains(err.Error(), "without LKG") {
		t.Fatalf("startup error=%v", err)
	}
}

func TestLoadRawDeleteBarrierCanConsumeAfterDurableInstallWhileACKRetries(t *testing.T) {
	directory := t.TempDir()
	token := filepath.Join(directory, "agent-token")
	if err := os.WriteFile(token, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	barrier, err := flowtombstone.Advance(nil, time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), "barrier-ack-retry", 4, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	ackAttempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/raw-delete-barrier"):
			_ = json.NewEncoder(w).Encode(barrier)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/raw-delete-barriers/"+barrier.ID+"/ack"):
			ackAttempts++
			if ackAttempts == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	opt := options{
		controlPlaneURL: server.URL, agentTokenFile: token, versionLKGDir: filepath.Join(directory, "lkg"),
		controlPlaneTimeout: 5 * time.Second,
	}
	guard, syncBarrier, err := loadRawDeleteBarrier(context.Background(), opt,
		flowworker.VersionWorkerIdentity{WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "test"})
	if err != nil {
		t.Fatalf("durably installed barrier should allow startup while ACK retries: %v", err)
	}
	if current, ok := guard.Current(); !ok || current.ID != barrier.ID || ackAttempts != 1 {
		t.Fatalf("installed barrier=%+v ok=%t attempts=%d", current, ok, ackAttempts)
	}
	if err := syncBarrier(context.Background()); err != nil || ackAttempts != 2 {
		t.Fatalf("ACK retry attempts=%d err=%v", ackAttempts, err)
	}
}
