// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/cloudcache/watchdog/internal/watchdog"
)

type workerTrustIntegrationController struct {
	delivery watchdog.CollectorPlanTrustBundleDelivery
}

func (c workerTrustIntegrationController) FetchTrustBundle(_ context.Context, workerID watchdog.ID, credential watchdog.CollectorMachineCredential) (watchdog.CollectorPlanTrustBundleDelivery, error) {
	if workerID != "worker-a" || credential.Token != "agent-secret" {
		return watchdog.CollectorPlanTrustBundleDelivery{}, watchdog.ErrCollectorMachineUnauthorized
	}
	return c.delivery, nil
}

type workerDeliveryIntegrationController struct {
	mu       sync.Mutex
	envelope []byte
	objects  map[string]watchdog.FlowEnrichmentObjectDelivery
	acks     []watchdog.FlowEnrichmentAcknowledgementReport
}

func (c *workerDeliveryIntegrationController) FetchDesired(_ context.Context, workerID watchdog.ID, credential watchdog.CollectorMachineCredential, afterVersion uint32, limit int) (watchdog.FlowEnrichmentDeliveryPage, error) {
	if workerID != "worker-a" || credential.Token != "agent-secret" || limit != 20 {
		return watchdog.FlowEnrichmentDeliveryPage{}, watchdog.ErrFlowEnrichmentDeliveryInvalid
	}
	if afterVersion >= 1 {
		return watchdog.FlowEnrichmentDeliveryPage{NextVersion: afterVersion}, nil
	}
	return watchdog.FlowEnrichmentDeliveryPage{
		Items: []watchdog.FlowEnrichmentDeliveryItem{{ClassificationVersion: 1, Envelope: c.envelope}}, NextVersion: 1,
	}, nil
}

func (c *workerDeliveryIntegrationController) ResolveObject(_ context.Context, workerID watchdog.ID, credential watchdog.CollectorMachineCredential, publicationID watchdog.ID, kind string) (watchdog.FlowEnrichmentObjectDelivery, error) {
	if workerID != "worker-a" || credential.Token != "agent-secret" || publicationID != "publication-a" {
		return watchdog.FlowEnrichmentObjectDelivery{}, watchdog.ErrFlowEnrichmentDeliveryInvalid
	}
	object, ok := c.objects[kind]
	if !ok {
		return watchdog.FlowEnrichmentObjectDelivery{}, watchdog.ErrFlowEnrichmentDeliveryInvalid
	}
	return object, nil
}

func (c *workerDeliveryIntegrationController) Acknowledge(_ context.Context, workerID watchdog.ID, credential watchdog.CollectorMachineCredential, publicationID watchdog.ID, report watchdog.FlowEnrichmentAcknowledgementReport) error {
	if workerID != "worker-a" || credential.Token != "agent-secret" || publicationID != "publication-a" {
		return watchdog.ErrFlowEnrichmentDeliveryInvalid
	}
	c.mu.Lock()
	c.acks = append(c.acks, report)
	c.mu.Unlock()
	return nil
}

func TestProductionRouterAndWorkerSyncInstallWADSAndColdRestore(t *testing.T) {
	now := time.Date(2026, 9, 7, 16, 0, 0, 0, time.UTC)
	dimensionData, dimensionChecksum := buildWorkerIntegrationWADS(t, now)
	classificationData, err := json.Marshal(flowdimension.ClassificationBundle{
		SchemaVersion: flowdimension.ClassificationSchemaVersion, Version: 1,
		EffectiveFrom: now, DimensionSnapshotID: "snapshot-a", HomeProvince: "330000", HomeCity: "330100",
		HomeISPIDs: []uint16{3}, OverseasIncludesHMT: true,
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	publication := flowworker.EnrichmentVersionPublication{
		PublicationID: "publication-a", DimensionSnapshotID: "snapshot-a",
		DimensionVersion: 1, DimensionEffectiveFrom: now,
		Dimension: flowworker.VersionObjectReference{
			ObjectRef: "objects/snapshot-a/address-snapshot.wads", Checksum: dimensionChecksum,
			ObjectFormat: flowworker.VersionObjectFormatWADS, ObjectFormatVersion: flowdimension.AddressSnapshotFormatVersion,
		},
		ClassificationVersion: 1, ClassificationEffectiveFrom: now,
		Classification: flowworker.VersionObjectReference{
			ObjectRef: "objects/publication-a/classification.json", Checksum: workerIntegrationChecksum(classificationData),
			ObjectFormat: flowworker.VersionObjectFormatJSON,
		},
	}
	trustData, envelope := signWorkerIntegrationPublication(t, now, publication)
	directory := t.TempDir()
	dimensionPath := filepath.Join(directory, "address-snapshot.wads")
	classificationPath := filepath.Join(directory, "classification.json")
	if err := os.WriteFile(dimensionPath, dimensionData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(classificationPath, classificationData, 0o600); err != nil {
		t.Fatal(err)
	}
	_, trustChecksum, err := flowplan.ParseTrustBundle(trustData)
	if err != nil {
		t.Fatal(err)
	}
	delivery := &workerDeliveryIntegrationController{
		envelope: envelope,
		objects: map[string]watchdog.FlowEnrichmentObjectDelivery{
			"dimension": {
				Path: dimensionPath, Name: "address-snapshot.wads", ContentType: "application/octet-stream",
				Checksum: dimensionChecksum, Size: int64(len(dimensionData)),
			},
			"classification": {
				Path: classificationPath, Name: "classification.json", ContentType: "application/json",
				Checksum: publication.Classification.Checksum, Size: int64(len(classificationData)),
			},
		},
	}
	router := watchdog.NewAPIV1Router(watchdog.APIV1RouterConfig{
		FlowWorkerTrust: workerTrustIntegrationController{delivery: watchdog.CollectorPlanTrustBundleDelivery{
			Payload: trustData, ETag: `"g1-` + trustChecksum + `"`, Generation: 1, Checksum: trustChecksum,
		}},
		FlowEnrichmentDelivery: delivery,
	})
	server := httptest.NewServer(router)

	identity := flowworker.VersionWorkerIdentity{WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "integration"}
	client, err := flowworker.NewVersionHTTPClient(flowworker.VersionHTTPClientConfig{
		BaseURL: server.URL, AgentToken: "agent-secret", Identity: identity, Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	lkgDirectory := filepath.Join(directory, "lkg")
	lkg, _ := flowworker.NewDiskVersionLKG(lkgDirectory)
	catalog, _ := flowworker.NewEnrichmentVersionCatalog()
	syncer, err := flowworker.NewRemoteVersionSync(client, lkg, &flowplan.TrustStore{}, catalog, flowworker.VersionLoaderLimits{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := syncer.SyncOnce(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.HighestVersion != 1 || result.Installed != 1 {
		t.Fatalf("sync result = %+v", result)
	}
	installed, exists := catalog.DimensionVersion(1)
	if !exists {
		t.Fatal("WADS version was not installed")
	}
	if _, ok := installed.(*flowdimension.AddressSnapshotIndex); !ok {
		t.Fatalf("installed dimension type = %T", installed)
	}
	delivery.mu.Lock()
	if len(delivery.acks) != 2 || delivery.acks[0].State != watchdog.FlowEnrichmentAckDownloaded || delivery.acks[1].State != watchdog.FlowEnrichmentAckInstalled {
		t.Fatalf("ACKs = %+v", delivery.acks)
	}
	delivery.mu.Unlock()
	server.Close()

	restartedLKG, _ := flowworker.NewDiskVersionLKG(lkgDirectory)
	restartedCatalog, _ := flowworker.NewEnrichmentVersionCatalog()
	restored, err := restartedLKG.Restore(context.Background(), &flowplan.TrustStore{}, restartedCatalog, identity, flowworker.VersionLoaderLimits{}, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if restored.PublicationCount != 1 || restored.HighestVersion != 1 {
		t.Fatalf("cold restore = %+v", restored)
	}
	if _, err := restartedCatalog.Select(now.Add(time.Minute)); err != nil {
		t.Fatalf("cold-restored catalog is unavailable: %v", err)
	}
}

func buildWorkerIntegrationWADS(t testing.TB, effectiveFrom time.Time) ([]byte, string) {
	t.Helper()
	definition, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: "snapshot-a",
		Version: 1, EffectiveFrom: effectiveFrom,
		Prefixes: []flowdimension.PrefixDefinition{{
			ID: "local", CIDR: "10.0.0.0/8", Labels: map[string]string{"flow": "local", "business": "customer"},
		}},
	}, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	built, err := flowdimension.BuildAddressSnapshot(flowdimension.AddressSnapshotBuildInput{
		Definition: definition, BuilderVersion: "integration-test",
	}, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	return built.Data, built.ChecksumSHA256
}

func signWorkerIntegrationPublication(t testing.TB, now time.Time, publication flowworker.EnrichmentVersionPublication) ([]byte, []byte) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	trustData, _, err := flowplan.MarshalTrustBundle(flowplan.TrustBundle{
		SchemaVersion: flowplan.TrustBundleSchemaVersion, Generation: 1, IssuedAtUnixMilli: now.Add(-time.Hour).UnixMilli(),
		Keys: []flowplan.TrustBundleKey{{
			KeyID: "integration-key", Algorithm: flowworker.EnrichmentVersionSignatureAlgorithm,
			PublicKey: base64.StdEncoding.EncodeToString(public), Status: "active",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := flowworker.SignedEnrichmentVersionPublication{
		SchemaVersion: flowworker.EnrichmentVersionEnvelopeSchemaVersion, Publication: publication,
		SignatureAlgorithm: flowworker.EnrichmentVersionSignatureAlgorithm, SigningKeyID: "integration-key",
		SignedAtUnixMilli: now.Add(-time.Minute).UnixMilli(),
	}
	payload, err := flowworker.EnrichmentVersionSigningPayload(envelope)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Signature = ed25519.Sign(private, payload)
	data, err := flowworker.MarshalSignedEnrichmentVersionPublication(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return trustData, data
}

func workerIntegrationChecksum(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}
