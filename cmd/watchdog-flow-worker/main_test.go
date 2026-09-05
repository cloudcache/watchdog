// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
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
	if err != nil || config.Password != "secret" {
		t.Fatalf("config=%+v error=%v", config, err)
	}
}

func TestLoadPublicationIsStrictAndResolvesObjectPaths(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "publication.json")
	publication := flowworker.EnrichmentVersionPublication{
		PublicationID: "publication-1", TenantID: "tenant-a", DimensionSnapshotID: "snapshot-a", DimensionVersion: 1,
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
