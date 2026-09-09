// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowstream"
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
