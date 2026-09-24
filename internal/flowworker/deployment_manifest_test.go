package flowworker

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
)

func TestSignedWorkerDeploymentManifestRoundTrip(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	now := time.Date(2026, 9, 19, 4, 0, 0, 0, time.UTC)
	manifest := WorkerDeploymentManifest{
		SchemaVersion: WorkerDeploymentManifestSchemaVersion,
		DeploymentID:  "deployment-1",
		Generation:    2,
		WorkerID:      "worker-1",
		EffectiveFrom: now,
		Artifacts: []DeploymentArtifactReference{
			{ArtifactID: "boundary-1", Kind: ArtifactKindDeviceBoundary, ScopeID: "device-1", Version: 1, Format: VersionObjectFormatJSON, FormatVersion: flowdimension.DeviceBoundarySchemaVersion, Checksum: checksumOf('b'), SizeBytes: 20},
			{ArtifactID: "policy-1", Kind: ArtifactKindClassificationPolicy, ScopeID: "global", Version: 1, Format: VersionObjectFormatJSON, FormatVersion: flowdimension.ClassificationPolicySchemaVersion, Checksum: checksumOf('c'), SizeBytes: 30},
			{ArtifactID: "address-1", Kind: ArtifactKindAddressCatalog, ScopeID: "global", Version: 9, Format: VersionObjectFormatWADS, FormatVersion: flowdimension.AddressSnapshotFormatVersion, Checksum: checksumOf('a'), SizeBytes: 40},
		},
	}
	envelope := SignedWorkerDeploymentManifest{
		SchemaVersion:      WorkerDeploymentEnvelopeSchemaVersion,
		Manifest:           manifest,
		SignatureAlgorithm: WorkerDeploymentSignatureAlgorithm,
		SigningKeyID:       "key-1",
		SignedAtUnixMilli:  now.UnixMilli(),
	}
	payload, err := WorkerDeploymentSigningPayload(envelope)
	if err != nil {
		t.Fatalf("signing payload: %v", err)
	}
	envelope.Signature = ed25519.Sign(privateKey, payload)
	encoded, err := MarshalSignedWorkerDeploymentManifest(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	trustBundle, _, err := flowplan.MarshalTrustBundle(flowplan.TrustBundle{
		SchemaVersion:     flowplan.TrustBundleSchemaVersion,
		Generation:        1,
		IssuedAtUnixMilli: now.UnixMilli(),
		Keys: []flowplan.TrustBundleKey{{
			KeyID: "key-1", Algorithm: "ed25519", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "active",
		}},
	})
	if err != nil {
		t.Fatalf("marshal trust bundle: %v", err)
	}
	trust := &flowplan.TrustStore{}
	if err := trust.Install(trustBundle); err != nil {
		t.Fatalf("install trust bundle: %v", err)
	}
	verified, err := VerifySignedWorkerDeploymentManifest(encoded, trust, now)
	if err != nil {
		t.Fatalf("verify envelope: %v", err)
	}
	if verified.Manifest.Artifacts[0].Kind != ArtifactKindAddressCatalog || verified.Manifest.Artifacts[2].Kind != ArtifactKindDeviceBoundary {
		t.Fatalf("manifest was not canonicalized: %+v", verified.Manifest.Artifacts)
	}

	encoded[len(encoded)-2] ^= 1
	if _, err := VerifySignedWorkerDeploymentManifest(encoded, trust, now); err == nil {
		t.Fatal("expected tampered manifest to fail")
	}
}

func TestWorkerDeploymentManifestRequiresOneGlobalAddressAndPolicy(t *testing.T) {
	manifest := WorkerDeploymentManifest{
		SchemaVersion: WorkerDeploymentManifestSchemaVersion,
		DeploymentID:  "deployment-1",
		Generation:    1,
		WorkerID:      "worker-1",
		EffectiveFrom: time.Date(2026, 9, 19, 4, 0, 0, 0, time.UTC),
		Artifacts: []DeploymentArtifactReference{
			{ArtifactID: "address-1", Kind: ArtifactKindAddressCatalog, ScopeID: "global", Version: 1, Format: VersionObjectFormatWADS, FormatVersion: flowdimension.AddressSnapshotFormatVersion, Checksum: checksumOf('a'), SizeBytes: 1},
			{ArtifactID: "address-2", Kind: ArtifactKindAddressCatalog, ScopeID: "second", Version: 1, Format: VersionObjectFormatWADS, FormatVersion: flowdimension.AddressSnapshotFormatVersion, Checksum: checksumOf('b'), SizeBytes: 1},
		},
	}
	if err := ValidateWorkerDeploymentManifest(manifest); err == nil {
		t.Fatal("expected invalid artifact composition")
	}
}

func checksumOf(value byte) string {
	data := make([]byte, 64)
	for index := range data {
		data[index] = value
	}
	return "sha256:" + string(data)
}
