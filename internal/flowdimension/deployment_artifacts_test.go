package flowdimension

import (
	"strings"
	"testing"
	"time"
)

func TestDeviceCustomerBoundaryCanonicalRoundTrip(t *testing.T) {
	effective := time.Date(2026, 9, 19, 4, 0, 0, 0, time.UTC)
	input := DeviceCustomerBoundary{
		SchemaVersion: DeviceBoundarySchemaVersion,
		DeviceID:      "device-b",
		Revision:      7,
		EffectiveFrom: effective,
		Customers: []DeviceBoundaryCustomer{
			{CustomerID: "customer-b", CustomerName: "Customer B", Prefixes: []DeviceBoundaryPrefix{{ID: "prefix-v6", CIDR: "2409:8000::1/32"}}},
			{CustomerID: "customer-a", CustomerName: "Customer A", Prefixes: []DeviceBoundaryPrefix{{ID: "prefix-v4", CIDR: "::ffff:192.0.2.1/120"}}},
		},
	}

	encoded, checksum, err := EncodeDeviceCustomerBoundary(input)
	if err != nil {
		t.Fatalf("encode boundary: %v", err)
	}
	decoded, err := DecodeDeviceCustomerBoundary(encoded, checksum, DeploymentArtifactLimits{})
	if err != nil {
		t.Fatalf("decode boundary: %v", err)
	}
	if decoded.Customers[0].CustomerID != "customer-a" || decoded.Customers[0].Prefixes[0].CIDR != "192.0.2.0/24" {
		t.Fatalf("unexpected canonical IPv4 boundary: %+v", decoded.Customers[0])
	}
	if decoded.Customers[1].Prefixes[0].CIDR != "2409:8000::/32" {
		t.Fatalf("unexpected canonical IPv6 boundary: %+v", decoded.Customers[1])
	}

	if _, err := DecodeDeviceCustomerBoundary(encoded, "sha256:"+strings.Repeat("0", 64), DeploymentArtifactLimits{}); err == nil {
		t.Fatal("expected checksum mismatch")
	}
}

func TestDeviceCustomerBoundaryRejectsOverlapAcrossCustomers(t *testing.T) {
	boundary := DeviceCustomerBoundary{
		SchemaVersion: DeviceBoundarySchemaVersion,
		DeviceID:      "device-a",
		Revision:      1,
		EffectiveFrom: time.Date(2026, 9, 19, 4, 0, 0, 0, time.UTC),
		Customers: []DeviceBoundaryCustomer{
			{CustomerID: "customer-a", CustomerName: "A", Prefixes: []DeviceBoundaryPrefix{{ID: "a", CIDR: "192.0.2.0/24"}}},
			{CustomerID: "customer-b", CustomerName: "B", Prefixes: []DeviceBoundaryPrefix{{ID: "b", CIDR: "192.0.2.128/25"}}},
		},
	}
	if _, _, err := EncodeDeviceCustomerBoundary(boundary); err == nil {
		t.Fatal("expected overlapping customer prefixes to be rejected")
	}
}

func TestEmptyDeviceCustomerBoundaryIsCanonicalTombstone(t *testing.T) {
	effective := time.Date(2026, 9, 19, 4, 0, 0, 0, time.UTC)
	encoded, checksum, err := EncodeDeviceCustomerBoundary(DeviceCustomerBoundary{
		SchemaVersion: DeviceBoundarySchemaVersion,
		DeviceID:      "device-a",
		Revision:      8,
		EffectiveFrom: effective,
	})
	if err != nil {
		t.Fatalf("encode empty boundary: %v", err)
	}
	if !strings.Contains(string(encoded), `"customers":[]`) {
		t.Fatalf("empty boundary is not canonical: %s", encoded)
	}
	decoded, err := DecodeDeviceCustomerBoundary(encoded, checksum, DeploymentArtifactLimits{})
	if err != nil || decoded.DeviceID != "device-a" || len(decoded.Customers) != 0 {
		t.Fatalf("decode empty boundary: boundary=%+v err=%v", decoded, err)
	}
	policy := ClassificationPolicyArtifact{
		SchemaVersion: ClassificationPolicySchemaVersion,
		Revision:      1, Algorithm: ClassificationPolicyAlgorithm,
		UnmatchedPolicy: "unknown", EffectiveFrom: effective,
	}
	compiled, err := CompileDeploymentClassification("snapshot-a", 9, effective, policy, []DeviceCustomerBoundary{decoded}, checksum)
	if err != nil {
		t.Fatalf("compile empty boundary deployment: %v", err)
	}
	if !compiled.UsesDeviceSources() {
		t.Fatal("empty v2 deployment fell back to legacy global classification")
	}
}

func TestClassificationPolicyCanonicalRoundTrip(t *testing.T) {
	policy := ClassificationPolicyArtifact{
		SchemaVersion:   ClassificationPolicySchemaVersion,
		Revision:        3,
		Algorithm:       ClassificationPolicyAlgorithm,
		UnmatchedPolicy: "unknown",
		EffectiveFrom:   time.Date(2026, 9, 19, 4, 0, 0, 0, time.UTC),
	}
	encoded, checksum, err := EncodeClassificationPolicy(policy)
	if err != nil {
		t.Fatalf("encode policy: %v", err)
	}
	decoded, err := DecodeClassificationPolicy(encoded, checksum, DeploymentArtifactLimits{})
	if err != nil {
		t.Fatalf("decode policy: %v", err)
	}
	if decoded != policy {
		t.Fatalf("policy mismatch: got %+v want %+v", decoded, policy)
	}
}
