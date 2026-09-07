package flowworker

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
)

func TestSignedEnrichmentVersionPublicationUsesMonotonicTrustBundle(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	trustData, _, err := flowplan.MarshalTrustBundle(flowplan.TrustBundle{
		SchemaVersion: flowplan.TrustBundleSchemaVersion, Generation: 1,
		IssuedAtUnixMilli: now.Add(-time.Minute).UnixMilli(),
		Keys: []flowplan.TrustBundleKey{{
			KeyID: "enrichment-key", Algorithm: EnrichmentVersionSignatureAlgorithm,
			PublicKey: encodePublicKey(publicKey), Status: "active",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	trust := &flowplan.TrustStore{}
	if err := trust.Install(trustData); err != nil {
		t.Fatal(err)
	}
	envelope := SignedEnrichmentVersionPublication{
		SchemaVersion: EnrichmentVersionEnvelopeSchemaVersion,
		Publication: EnrichmentVersionPublication{
			PublicationID: "publication-1", TenantID: "tenant-1",
			DimensionSnapshotID: "snapshot-1", DimensionVersion: 7,
			DimensionEffectiveFrom: now.Add(-2 * time.Hour),
			Dimension: VersionObjectReference{
				ObjectRef: "dimension-snapshots/tenant-1/snapshot-1/address-snapshot.wads",
				Checksum:  "sha256:" + strings.Repeat("a", 64), ObjectFormat: VersionObjectFormatWADS, ObjectFormatVersion: 1,
			},
			ClassificationVersion: 3, ClassificationEffectiveFrom: now.Add(-time.Hour),
			Classification: VersionObjectReference{
				ObjectRef: "dimension-snapshots/tenant-1/publication-1/bundle.json",
				Checksum:  "sha256:" + strings.Repeat("b", 64), ObjectFormat: VersionObjectFormatJSON,
			},
		},
		SignatureAlgorithm: EnrichmentVersionSignatureAlgorithm,
		SigningKeyID:       "enrichment-key",
		SignedAtUnixMilli:  now.Add(-30 * time.Minute).UnixMilli(),
	}
	payload, err := EnrichmentVersionSigningPayload(envelope)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Signature = ed25519.Sign(privateKey, payload)
	data, err := MarshalSignedEnrichmentVersionPublication(envelope)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifySignedEnrichmentVersionPublication(data, trust, now)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Publication.ClassificationVersion != 3 || verified.SigningKeyID != "enrichment-key" {
		t.Fatalf("verified envelope = %+v", verified)
	}

	var tampered SignedEnrichmentVersionPublication
	if err := json.Unmarshal(data, &tampered); err != nil {
		t.Fatal(err)
	}
	tampered.Publication.ClassificationVersion = 4
	tamperedData, err := MarshalSignedEnrichmentVersionPublication(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifySignedEnrichmentVersionPublication(tamperedData, trust, now); err == nil || !strings.Contains(err.Error(), "verification failed") {
		t.Fatalf("tampered verification error = %v", err)
	}

	retiringData, _, err := flowplan.MarshalTrustBundle(flowplan.TrustBundle{
		SchemaVersion: flowplan.TrustBundleSchemaVersion, Generation: 2,
		IssuedAtUnixMilli: now.UnixMilli(),
		Keys: []flowplan.TrustBundleKey{{
			KeyID: "enrichment-key", Algorithm: EnrichmentVersionSignatureAlgorithm,
			PublicKey: encodePublicKey(publicKey), Status: "retiring", TrustUntilUnixMilli: now.Add(time.Minute).UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := trust.Install(retiringData); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifySignedEnrichmentVersionPublication(data, trust, now.Add(time.Minute)); !errors.Is(err, flowplan.ErrTrustKeyUnavailable) {
		t.Fatalf("expired key verification error = %v", err)
	}
}

func encodePublicKey(key ed25519.PublicKey) string {
	return base64.StdEncoding.EncodeToString(key)
}
