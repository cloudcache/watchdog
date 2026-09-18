package server

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
)

func TestNormalizeFlowClassificationProfileCanonicalizesDefinition(t *testing.T) {
	draft, digest, err := normalizeFlowClassificationProfile(flowClassificationProfileDraft{
		DeviceProfiles: []flowClassificationDeviceProfileDraft{{
			DeviceID: " device-b ", SourcePrefixIDs: []string{" prefix-b ", "prefix-a", "prefix-b"},
		}, {
			DeviceID: "device-a", SourcePrefixIDs: []string{"prefix-c"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(draft.DeviceProfiles) != 2 || draft.DeviceProfiles[0].DeviceID != "device-a" ||
		draft.DeviceProfiles[1].DeviceID != "device-b" || len(draft.DeviceProfiles[1].SourcePrefixIDs) != 2 ||
		draft.DeviceProfiles[1].SourcePrefixIDs[0] != "prefix-a" || draft.DeviceProfiles[1].SourcePrefixIDs[1] != "prefix-b" ||
		!flowSHA256("sha256:"+digest) {
		t.Fatalf("profile was not canonicalized: %+v digest=%q", draft, digest)
	}

	again, againDigest, err := normalizeFlowClassificationProfile(draft)
	if err != nil || againDigest != digest || len(again.DeviceProfiles) != 2 || len(again.DeviceProfiles[1].SourcePrefixIDs) != 2 {
		t.Fatalf("normalization is not idempotent: %+v digest=%q err=%v", again, againDigest, err)
	}
}

func TestNormalizeFlowClassificationProfileRejectsInvalidDeviceSources(t *testing.T) {
	for _, draft := range []flowClassificationProfileDraft{
		{},
		{DeviceProfiles: []flowClassificationDeviceProfileDraft{{DeviceID: "device-a"}}},
		{DeviceProfiles: []flowClassificationDeviceProfileDraft{{DeviceID: "", SourcePrefixIDs: []string{"prefix-a"}}}},
		{DeviceProfiles: []flowClassificationDeviceProfileDraft{{DeviceID: "device-a", SourcePrefixIDs: []string{"prefix-a"}}, {DeviceID: "device-a", SourcePrefixIDs: []string{"prefix-b"}}}},
	} {
		if _, _, err := normalizeFlowClassificationProfile(draft); err == nil {
			t.Fatalf("invalid profile accepted: %+v", draft)
		}
	}
}

func TestLoadFlowAddressSnapshotPrefixesUsesPublishedWADS(t *testing.T) {
	definition, err := flowdimension.CompileBundle(flowdimension.SnapshotBundle{
		SchemaVersion: flowdimension.BundleSchemaVersion, SnapshotID: "snapshot-prefix-catalog", Version: 1,
		EffectiveFrom: time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC),
		Prefixes:      []flowdimension.PrefixDefinition{{ID: "customer-prefix", CIDR: "10.0.0.0/8", Labels: map[string]string{"business": "customer"}}},
	}, flowdimension.CompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	built, err := flowdimension.BuildAddressSnapshot(flowdimension.AddressSnapshotBuildInput{
		Definition: definition, BuilderVersion: "server-test",
	}, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "address-snapshot.wads")
	if err := os.WriteFile(path, built.Data, 0o600); err != nil {
		t.Fatal(err)
	}
	prefixes, err := loadFlowAddressSnapshotPrefixes(path, built.ChecksumSHA256)
	if err != nil {
		t.Fatal(err)
	}
	if got := prefixes["customer-prefix"]; got.CIDR != "10.0.0.0/8" || got.HasCity || got.HasOperator {
		t.Fatalf("published prefix = %+v", got)
	}
	if _, err := loadFlowAddressSnapshotPrefixes(path, "sha256:"+strings.Repeat("0", 64)); err == nil {
		t.Fatal("checksum mismatch was accepted")
	}
}

func TestFlowTrustKeyStateDistinguishesMatchReuseAndRotation(t *testing.T) {
	publicA, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicB, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	bundle := newFlowTrustBundle(1, time.Now().UTC(), "key-a", publicA)
	if matching, reused := flowTrustKeyState(bundle, "key-a", publicA); !matching || reused {
		t.Fatalf("active key not matched: matching=%v reused=%v", matching, reused)
	}
	if matching, reused := flowTrustKeyState(bundle, "key-a", publicB); matching || !reused {
		t.Fatalf("key id reuse was not rejected: matching=%v reused=%v", matching, reused)
	}
	if matching, reused := flowTrustKeyState(bundle, "key-b", publicB); matching || reused {
		t.Fatalf("new key was not recognized as a rotation: matching=%v reused=%v", matching, reused)
	}
	bundle.RevokedKeyIDs = []string{"key-b"}
	if matching, reused := flowTrustKeyState(bundle, "key-b", publicB); matching || !reused {
		t.Fatalf("revoked key id was not fenced: matching=%v reused=%v", matching, reused)
	}
}

func TestNewFlowTrustBundleIsCanonicalAndInstallable(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	bundle := newFlowTrustBundle(1, time.Now().UTC(), "flow-key-v1", publicKey)
	data, _, err := flowplan.MarshalTrustBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	var store flowplan.TrustStore
	if err := store.Install(data); err != nil || store.Generation() != 1 {
		t.Fatalf("trust bundle cannot be installed: generation=%d err=%v", store.Generation(), err)
	}
	if !strings.Contains(string(data), base64.StdEncoding.EncodeToString(publicKey)) {
		t.Fatal("trust bundle does not contain the configured public key")
	}
}

func TestValidateFlowEnrichmentACK(t *testing.T) {
	valid := flowEnrichmentAckRequest{
		State: "installed", BootID: "boot-1", SoftwareVersion: "1.2.3", DimensionSnapshotID: "snapshot-a",
		DimensionVersion: 1, DimensionChecksum: "sha256:" + strings.Repeat("a", 64),
		ClassificationVersion: 1, ClassificationChecksum: "sha256:" + strings.Repeat("b", 64),
	}
	if err := validateFlowEnrichmentACK(valid); err != nil {
		t.Fatal(err)
	}
	failed := valid
	failed.State = "failed"
	failed.FailureStage = "activate"
	failed.FailureCode = "RUNTIME_SWAP_FAILED"
	failed.FailureMessage = "swap failed"
	if err := validateFlowEnrichmentACK(failed); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*flowEnrichmentAckRequest){
		"unknown state":      func(value *flowEnrichmentAckRequest) { value.State = "ready" },
		"bad checksum":       func(value *flowEnrichmentAckRequest) { value.DimensionChecksum = "sha256:bad" },
		"failure on success": func(value *flowEnrichmentAckRequest) { value.FailureCode = "FAILED" },
		"missing failure": func(value *flowEnrichmentAckRequest) {
			value.State = "failed"
		},
		"bad failure code": func(value *flowEnrichmentAckRequest) {
			value.State = "failed"
			value.FailureStage, value.FailureCode = "verify", "bad-code"
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			if err := validateFlowEnrichmentACK(request); err == nil {
				t.Fatalf("invalid ACK accepted: %+v", request)
			}
		})
	}
}

func TestFlowUTCMinute(t *testing.T) {
	if !flowUTCMinute(time.Date(2026, 9, 15, 1, 2, 0, 0, time.UTC)) {
		t.Fatal("UTC minute boundary rejected")
	}
	if flowUTCMinute(time.Date(2026, 9, 15, 1, 2, 1, 0, time.UTC)) ||
		flowUTCMinute(time.Date(2026, 9, 15, 1, 2, 0, 0, time.FixedZone("UTC+8", 8*60*60))) {
		t.Fatal("non-UTC minute boundary accepted")
	}
}
