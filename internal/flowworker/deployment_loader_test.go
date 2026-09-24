package flowworker

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

type recordingDeploymentAcknowledger struct {
	states []string
}

func (a *recordingDeploymentAcknowledger) AcknowledgeDeployment(_ context.Context, _ WorkerDeploymentManifest, _ string, state string, _ map[string]DeploymentInstalledArtifact, _, _ string, _ error) error {
	a.states = append(a.states, state)
	return nil
}

type recordingDeploymentPersistence struct {
	manifests []WorkerDeploymentManifest
	err       error
}

func (p *recordingDeploymentPersistence) PersistDeployment(_ context.Context, manifest WorkerDeploymentManifest) error {
	if p.err != nil {
		return p.err
	}
	p.manifests = append(p.manifests, manifest)
	return nil
}

func TestDeploymentLoaderInstallsIndependentArtifactsAtomically(t *testing.T) {
	manifest, checksum, source := testWorkerDeployment(t, 1, testMinute(12, 0), nil)
	catalog, err := NewEnrichmentVersionCatalog()
	if err != nil {
		t.Fatal(err)
	}
	acks := &recordingDeploymentAcknowledger{}
	persistence := &recordingDeploymentPersistence{}
	loader, err := NewDeploymentLoader(source, acks, catalog, DeploymentLoaderLimits{}, persistence)
	if err != nil {
		t.Fatal(err)
	}
	if err := loader.Install(context.Background(), manifest, checksum); err != nil {
		t.Fatal(err)
	}

	installed, err := catalog.Select(testMinute(12, 30))
	if err != nil {
		t.Fatal(err)
	}
	if direction, customer, ok := installed.Classification.DeviceDirectionAttribution(
		"device-a", netip.MustParseAddr("192.0.2.10"), netip.MustParseAddr("203.0.113.10"),
	); !ok || direction != flowdimension.DirectionOut || customer != "Customer A" {
		t.Fatalf("IPv4 customer direction = %v, %q, %t", direction, customer, ok)
	}
	if direction, customer, ok := installed.Classification.DeviceDirectionAttribution(
		"device-a", netip.MustParseAddr("2409:8000::10"), netip.MustParseAddr("2001:db8::10"),
	); !ok || direction != flowdimension.DirectionOut || customer != "Customer A" {
		t.Fatalf("IPv6 customer direction = %v, %q, %t", direction, customer, ok)
	}
	if len(persistence.manifests) != 1 || len(acks.states) != 2 || acks.states[0] != "verified" || acks.states[1] != "installed" {
		t.Fatalf("persistence=%d ACKs=%v", len(persistence.manifests), acks.states)
	}
}

func TestDeploymentLoaderReusesAddressCatalogForBoundaryOnlyChange(t *testing.T) {
	first, firstChecksum, source := testWorkerDeployment(t, 1, testMinute(12, 0), nil)
	catalog, _ := NewEnrichmentVersionCatalog()
	loader, _ := NewDeploymentLoader(source, &recordingDeploymentAcknowledger{}, catalog, DeploymentLoaderLimits{}, &recordingDeploymentPersistence{})
	if err := loader.Install(context.Background(), first, firstChecksum); err != nil {
		t.Fatal(err)
	}
	firstInstalled, _ := catalog.ClassificationVersion(1)

	second, secondChecksum, secondSource := testWorkerDeployment(t, 2, testMinute(13, 0), &first.Artifacts[0])
	for key, value := range secondSource.objects {
		source.objects[key] = value
	}
	loader.source = source
	if err := loader.Install(context.Background(), second, secondChecksum); err != nil {
		t.Fatal(err)
	}
	secondInstalled, _ := catalog.ClassificationVersion(2)
	if firstInstalled.Dimension != secondInstalled.Dimension {
		t.Fatal("boundary-only deployment rebuilt the immutable address catalog")
	}
	addressFetches := 0
	for _, call := range source.calls {
		if call == first.Artifacts[0].ArtifactID {
			addressFetches++
		}
	}
	if addressFetches != 1 {
		t.Fatalf("address catalog fetches = %d, calls=%v", addressFetches, source.calls)
	}
}

func TestDeploymentLoaderFailureLeavesCurrentCatalogUntouched(t *testing.T) {
	first, firstChecksum, source := testWorkerDeployment(t, 1, testMinute(12, 0), nil)
	catalog, _ := NewEnrichmentVersionCatalog()
	loader, _ := NewDeploymentLoader(source, &recordingDeploymentAcknowledger{}, catalog, DeploymentLoaderLimits{}, &recordingDeploymentPersistence{})
	if err := loader.Install(context.Background(), first, firstChecksum); err != nil {
		t.Fatal(err)
	}

	second, secondChecksum, secondSource := testWorkerDeployment(t, 2, testMinute(13, 0), &first.Artifacts[0])
	for key, value := range secondSource.objects {
		source.objects[key] = value
	}
	for index := range second.Artifacts {
		if second.Artifacts[index].Kind == ArtifactKindDeviceBoundary {
			second.Artifacts[index].Checksum = checksumOf('f')
		}
	}
	if err := loader.Install(context.Background(), second, secondChecksum); !errors.Is(err, ErrInvalidWorkerDeployment) {
		t.Fatalf("invalid boundary error = %v", err)
	}
	if _, exists := catalog.ClassificationVersion(2); exists {
		t.Fatal("invalid deployment became visible")
	}
	if _, err := catalog.Select(testMinute(12, 30)); err != nil {
		t.Fatalf("previous deployment disappeared: %v", err)
	}
}

func TestDeploymentLoaderPersistenceFailurePreventsActivation(t *testing.T) {
	manifest, checksum, source := testWorkerDeployment(t, 1, testMinute(12, 0), nil)
	catalog, _ := NewEnrichmentVersionCatalog()
	acks := &recordingDeploymentAcknowledger{}
	loader, _ := NewDeploymentLoader(source, acks, catalog, DeploymentLoaderLimits{}, &recordingDeploymentPersistence{err: errors.New("disk full")})
	if err := loader.Install(context.Background(), manifest, checksum); !errors.Is(err, ErrInvalidWorkerDeployment) || !errors.Is(err, ErrDeploymentPersistence) {
		t.Fatalf("persistence error = %v", err)
	}
	if _, exists := catalog.ClassificationVersion(1); exists || len(acks.states) != 0 {
		t.Fatalf("failed deployment visible=%t ACKs=%v", exists, acks.states)
	}
}

func testWorkerDeployment(t testing.TB, generation uint64, effective time.Time, addressReference *DeploymentArtifactReference) (WorkerDeploymentManifest, string, *memoryVersionObjectSource) {
	t.Helper()
	objects := make(map[string][]byte)
	var address DeploymentArtifactReference
	if addressReference == nil {
		bundle, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
			SchemaVersion: flowdimension.BundleSchemaVersion,
			SnapshotID:    "address-snapshot-a",
			Version:       1,
			EffectiveFrom: testMinute(11, 0),
			Prefixes: []flowdimension.PrefixDefinition{
				{ID: "remote-v4", CIDR: "203.0.113.0/24", Labels: map[string]string{"business": "remote"}},
				{ID: "remote-v6", CIDR: "2001:db8::/32", Labels: map[string]string{"business": "remote"}},
			},
		}, flowdimension.CompileLimits{})
		if err != nil {
			t.Fatal(err)
		}
		built, err := flowdimension.BuildAddressSnapshot(flowdimension.AddressSnapshotBuildInput{
			Definition: bundle, BuilderVersion: "deployment-loader-test",
		}, flowdimension.AddressSnapshotLimits{})
		if err != nil {
			t.Fatal(err)
		}
		address = DeploymentArtifactReference{
			ArtifactID: "address-artifact-1", Kind: ArtifactKindAddressCatalog, ScopeID: "global", Version: 1,
			Format: VersionObjectFormatWADS, FormatVersion: flowdimension.AddressSnapshotFormatVersion,
			Checksum: built.ChecksumSHA256, SizeBytes: uint64(len(built.Data)),
		}
		objects[address.ArtifactID] = built.Data
	} else {
		address = *addressReference
	}

	policyData, policyChecksum, err := flowdimension.EncodeClassificationPolicy(flowdimension.ClassificationPolicyArtifact{
		SchemaVersion: flowdimension.ClassificationPolicySchemaVersion,
		Revision:      generation, Algorithm: flowdimension.ClassificationPolicyAlgorithm,
		UnmatchedPolicy: "unknown", EffectiveFrom: effective,
	})
	if err != nil {
		t.Fatal(err)
	}
	boundaryData, boundaryChecksum, err := flowdimension.EncodeDeviceCustomerBoundary(flowdimension.DeviceCustomerBoundary{
		SchemaVersion: flowdimension.DeviceBoundarySchemaVersion,
		DeviceID:      "device-a", Revision: generation, EffectiveFrom: effective,
		Customers: []flowdimension.DeviceBoundaryCustomer{{
			CustomerID: "customer-a", CustomerName: "Customer A",
			Prefixes: []flowdimension.DeviceBoundaryPrefix{
				{ID: "customer-a-v4", CIDR: "192.0.2.0/24"},
				{ID: "customer-a-v6", CIDR: "2409:8000::/32"},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	policyID := "policy-artifact-" + string(rune('0'+generation))
	boundaryID := "boundary-artifact-" + string(rune('0'+generation))
	objects[policyID] = policyData
	objects[boundaryID] = boundaryData
	manifest := WorkerDeploymentManifest{
		SchemaVersion: WorkerDeploymentManifestSchemaVersion,
		DeploymentID:  "deployment-" + string(rune('0'+generation)),
		Generation:    generation,
		WorkerID:      "worker-a",
		EffectiveFrom: effective,
		Artifacts: []DeploymentArtifactReference{
			address,
			{ArtifactID: policyID, Kind: ArtifactKindClassificationPolicy, ScopeID: "global", Version: generation,
				Format: VersionObjectFormatJSON, FormatVersion: flowdimension.ClassificationPolicySchemaVersion,
				Checksum: policyChecksum, SizeBytes: uint64(len(policyData))},
			{ArtifactID: boundaryID, Kind: ArtifactKindDeviceBoundary, ScopeID: "device-a", Version: generation,
				Format: VersionObjectFormatJSON, FormatVersion: flowdimension.DeviceBoundarySchemaVersion,
				Checksum: boundaryChecksum, SizeBytes: uint64(len(boundaryData))},
		},
	}
	return manifest, versionObjectChecksum([]byte(manifest.DeploymentID)), &memoryVersionObjectSource{objects: objects}
}
