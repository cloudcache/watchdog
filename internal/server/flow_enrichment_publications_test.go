package server

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
)

func TestNormalizeFlowClassificationProfileCanonicalizesDefinition(t *testing.T) {
	draft, digest, err := normalizeFlowClassificationProfile(flowClassificationProfileDraft{
		HomeProvince: " 330000 ", HomeCity: " 330100 ",
		HomeISPIDs: []uint16{4, 3, 4}, HomeASNs: []uint32{4812, 4134, 4812},
	})
	if err != nil {
		t.Fatal(err)
	}
	if draft.HomeProvince != "330000" || draft.HomeCity != "330100" ||
		len(draft.HomeISPIDs) != 2 || draft.HomeISPIDs[0] != 3 || draft.HomeISPIDs[1] != 4 ||
		len(draft.HomeASNs) != 2 || draft.HomeASNs[0] != 4134 || draft.HomeASNs[1] != 4812 ||
		draft.InternalPolicy != flowdimension.RecordPolicyCount || draft.TransitPolicy != flowdimension.RecordPolicyCount ||
		!flowSHA256("sha256:"+digest) {
		t.Fatalf("profile was not canonicalized: %+v digest=%q", draft, digest)
	}

	again, againDigest, err := normalizeFlowClassificationProfile(draft)
	if err != nil || againDigest != digest || len(again.HomeISPIDs) != 2 || len(again.HomeASNs) != 2 {
		t.Fatalf("normalization is not idempotent: %+v digest=%q err=%v", again, againDigest, err)
	}
}

func TestNormalizeFlowClassificationProfileRejectsInvalidHome(t *testing.T) {
	for _, draft := range []flowClassificationProfileDraft{
		{HomeProvince: "330100", InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount},
		{HomeProvince: "330000", HomeCity: "320100", InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount},
		{HomeISPIDs: []uint16{0}, InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount},
		{InternalPolicy: "ignore", TransitPolicy: flowdimension.RecordPolicyCount},
	} {
		if _, _, err := normalizeFlowClassificationProfile(draft); err == nil {
			t.Fatalf("invalid profile accepted: %+v", draft)
		}
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
