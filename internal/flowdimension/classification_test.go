package flowdimension

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestDecodeAndCompileClassificationBundleVerifiesWireContract(t *testing.T) {
	bundle := ClassificationBundle{
		SchemaVersion: LegacyClassificationSchemaVersion, Version: 7,
		EffectiveFrom: testMinute(12, 0), DimensionSnapshotID: "snapshot-7",
		HomeProvince: "330000", HomeCity: "330100", HomeISPIDs: []uint16{3, 4}, HomeASNs: []uint32{4134, 4812},
		OverseasIncludesHMT: true, InternalPolicy: RecordPolicyDrop, TransitPolicy: RecordPolicyCount,
	}
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	checksum := classificationChecksum(data)
	snapshot, err := DecodeAndCompileClassificationBundle(data, checksum, ClassificationCompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	metadata := snapshot.Metadata()
	if metadata.Version != bundle.Version || metadata.DimensionSnapshotID != bundle.DimensionSnapshotID || metadata.Checksum != checksum {
		t.Fatalf("metadata = %+v", metadata)
	}
	badChecksum := "sha256:" + strings.Repeat("0", 64)
	if _, err := DecodeAndCompileClassificationBundle(data, badChecksum, ClassificationCompileLimits{}); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("checksum error = %v", err)
	}
	unknown := append(data[:len(data)-1], []byte(`,"unexpected":true}`)...)
	if _, err := DecodeAndCompileClassificationBundle(unknown, classificationChecksum(unknown), ClassificationCompileLimits{}); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown-field error = %v", err)
	}
	if _, err := DecodeAndCompileClassificationBundle(data, checksum, ClassificationCompileLimits{MaxBundleBytes: len(data) - 1}); err == nil || !strings.Contains(err.Error(), "bundle size") {
		t.Fatalf("size error = %v", err)
	}
}

func TestEncodeClassificationBundleCanonicalizesSets(t *testing.T) {
	definition := testClassificationDefinition(7, testMinute(12, 0), "snapshot-7")
	definition.HomeISPIDs = []uint16{9, 3, 4}
	definition.HomeASNs = []uint32{4837, 4134}
	data, checksum, err := EncodeClassificationBundle(definition)
	if err != nil {
		t.Fatal(err)
	}
	definition.HomeISPIDs = []uint16{4, 9, 3}
	definition.HomeASNs = []uint32{4134, 4837}
	reordered, reorderedChecksum, err := EncodeClassificationBundle(definition)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(reordered) || checksum != reorderedChecksum {
		t.Fatalf("classification object changed with input order:\n%s\n%s", data, reordered)
	}
	if _, err := DecodeAndCompileClassificationBundle(data, checksum, ClassificationCompileLimits{}); err != nil {
		t.Fatal(err)
	}
	definition.HomeISPIDs = nil
	definition.HomeASNs = nil
	empty, _, err := EncodeClassificationBundle(definition)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(empty), `"home_isp_ids":[]`) || !strings.Contains(string(empty), `"home_asns":[]`) {
		t.Fatalf("empty classification sets are not JSON arrays: %s", empty)
	}
}

func TestClassificationSchemaV2SelectsPerDeviceContext(t *testing.T) {
	definition := ClassificationDefinition{
		Version: 9, EffectiveFrom: testMinute(12, 0), DimensionSnapshotID: "snapshot-9",
		DeviceProfiles: []ClassificationDeviceProfile{
			{DeviceID: "device-zhejiang", SourcePrefixes: []ClassificationSourcePrefix{{ID: "zhejiang-customer", CIDR: "10.0.0.0/8"}}},
			{DeviceID: "device-jiangsu", SourcePrefixes: []ClassificationSourcePrefix{{ID: "jiangsu-customer", CIDR: "172.16.0.0/12"}}},
		},
		InternalPolicy: RecordPolicyCount, TransitPolicy: RecordPolicyCount,
	}
	data, checksum, err := EncodeClassificationBundle(definition)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"schema_version":2`) || !strings.Contains(string(data), `"device_profiles"`) {
		t.Fatalf("schema v2 bundle = %s", data)
	}
	snapshot, err := DecodeAndCompileClassificationBundle(data, checksum, ClassificationCompileLimits{})
	if err != nil {
		t.Fatal(err)
	}
	source := netip.MustParseAddr("10.1.2.3")
	destination := netip.MustParseAddr("203.0.113.20")
	if direction, ok := snapshot.DeviceDirection("device-zhejiang", source, destination); !ok || direction != DirectionOut {
		t.Fatalf("zhejiang direction = %q, configured=%v", direction, ok)
	}
	if direction, ok := snapshot.DeviceDirection("device-jiangsu", source, destination); !ok || direction != DirectionTransit {
		t.Fatalf("jiangsu direction = %q, configured=%v", direction, ok)
	}
	if direction, ok := snapshot.DeviceDirection("device-jiangsu", destination, netip.MustParseAddr("172.16.1.1")); !ok || direction != DirectionIn {
		t.Fatalf("jiangsu inbound direction = %q, configured=%v", direction, ok)
	}
	if direction, ok := snapshot.DeviceDirection("missing-device", source, destination); ok || direction != DirectionAmbiguous {
		t.Fatalf("missing direction = %q, configured=%v", direction, ok)
	}
	local := GeoInfo{Country: "CN", AdminCode: "330100", ISPID: 3, ASN: 4134}
	remote := GeoInfo{Country: "CN", AdminCode: "330100", ISPID: 3, ASN: 4134}
	if got := snapshot.ClassifyResolvedEndpoints("device-zhejiang", DirectionOut, local, remote); got != CategoryOnNetLocalCity {
		t.Fatalf("zhejiang category = %q", got)
	}
	remote.AdminCode = "320100"
	if got := snapshot.ClassifyResolvedEndpoints("device-zhejiang", DirectionOut, local, remote); got != CategoryOnNetCrossProvince {
		t.Fatalf("cross-province category = %q", got)
	}
	remote.Country = "HK"
	remote.AdminCode = "810000"
	if got := snapshot.ClassifyResolvedEndpoints("device-zhejiang", DirectionOut, local, remote); got != CategoryOverseas {
		t.Fatalf("cross-border category = %q", got)
	}
	if got := snapshot.ClassifyResolvedEndpoints("missing-device", DirectionOut, local, remote); got != CategoryUnknown {
		t.Fatalf("missing device category = %q", got)
	}
	if got := snapshot.Classify(DirectionOut, remote); got != CategoryUnknown {
		t.Fatalf("v2 global fallback category = %q", got)
	}
}

func TestClassificationSchemaV2DirectionCoversIPv4AndIPv6(t *testing.T) {
	snapshot, err := CompileClassification(ClassificationDefinition{
		Version: 10, EffectiveFrom: testMinute(12, 0), DimensionSnapshotID: "snapshot-10",
		DeviceProfiles: []ClassificationDeviceProfile{{DeviceID: "device-a", SourcePrefixes: []ClassificationSourcePrefix{
			{ID: "customer-v4", CIDR: "10.0.0.0/8"},
			{ID: "customer-v6", CIDR: "2001:db8:100::/48"},
		}}},
		InternalPolicy: RecordPolicyCount, TransitPolicy: RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		source      string
		destination string
		want        BusinessDirection
	}{
		{name: "v4-out", source: "10.1.2.3", destination: "203.0.113.10", want: DirectionOut},
		{name: "v4-in", source: "203.0.113.10", destination: "10.1.2.3", want: DirectionIn},
		{name: "v4-internal", source: "10.1.2.3", destination: "10.2.3.4", want: DirectionInternal},
		{name: "v4-transit", source: "192.0.2.1", destination: "203.0.113.10", want: DirectionTransit},
		{name: "v6-out", source: "2001:db8:100::10", destination: "2001:db8:200::20", want: DirectionOut},
		{name: "v6-in", source: "2001:db8:200::20", destination: "2001:db8:100::10", want: DirectionIn},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, configured := snapshot.DeviceDirection("device-a", netip.MustParseAddr(test.source), netip.MustParseAddr(test.destination))
			if !configured || got != test.want {
				t.Fatalf("direction = %q configured=%v, want %q", got, configured, test.want)
			}
		})
	}
}

func TestClassificationSchemaV2ReturnsCustomerAttribution(t *testing.T) {
	snapshot, err := CompileClassification(ClassificationDefinition{
		Version: 10, EffectiveFrom: testMinute(12, 0), DimensionSnapshotID: "snapshot-10",
		DeviceProfiles: []ClassificationDeviceProfile{{DeviceID: "device-a", SourcePrefixes: []ClassificationSourcePrefix{
			{ID: "customer-a-prefix-1", CIDR: "10.1.0.0/16", CustomerID: "customer-a", CustomerName: "Customer A"},
			{ID: "customer-b-prefix-1", CIDR: "10.2.0.0/16", CustomerID: "customer-b", CustomerName: "Customer B"},
		}}},
		InternalPolicy: RecordPolicyCount, TransitPolicy: RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, source, destination, customer string
		wantDirection                       BusinessDirection
	}{
		{"customer-a-out", "10.1.2.3", "203.0.113.1", "Customer A", DirectionOut},
		{"customer-b-in", "203.0.113.1", "10.2.3.4", "Customer B", DirectionIn},
		{"internal-prefers-source", "10.1.2.3", "10.2.3.4", "Customer A", DirectionInternal},
		{"transit", "192.0.2.1", "203.0.113.1", "", DirectionTransit},
	} {
		t.Run(test.name, func(t *testing.T) {
			direction, customer, configured := snapshot.DeviceDirectionAttribution(
				"device-a", netip.MustParseAddr(test.source), netip.MustParseAddr(test.destination),
			)
			if !configured || direction != test.wantDirection || customer != test.customer {
				t.Fatalf("direction=%q customer=%q configured=%v, want %q/%q", direction, customer, configured, test.wantDirection, test.customer)
			}
		})
	}
}

func TestClassificationSchemaV2ReturnsDualStackCustomerAttribution(t *testing.T) {
	snapshot, err := CompileClassification(ClassificationDefinition{
		Version: 11, EffectiveFrom: testMinute(12, 0), DimensionSnapshotID: "snapshot-11",
		DeviceProfiles: []ClassificationDeviceProfile{
			{DeviceID: "device-qiniu", SourcePrefixes: []ClassificationSourcePrefix{
				{ID: "qiniu-v4", CIDR: "120.199.32.128/25", CustomerID: "qiniu", CustomerName: "七牛"},
				{ID: "qiniu-v6", CIDR: "2409:8728:8ff:1077::/64", CustomerID: "qiniu", CustomerName: "七牛"},
				{ID: "other-v6", CIDR: "2409:8728:8ff::/48", CustomerID: "other", CustomerName: "Other"},
			}},
			{DeviceID: "device-other", SourcePrefixes: []ClassificationSourcePrefix{
				{ID: "other-device-v6", CIDR: "2409:8728:8ff:1077::/64", CustomerID: "other-device", CustomerName: "Other device"},
			}},
		},
		InternalPolicy: RecordPolicyCount, TransitPolicy: RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, device, source, destination, customer string
		wantDirection                               BusinessDirection
	}{
		{"qiniu-v4", "device-qiniu", "120.199.32.129", "203.0.113.1", "七牛", DirectionOut},
		{"qiniu-v4-mapped", "device-qiniu", "::ffff:120.199.32.129", "203.0.113.1", "七牛", DirectionOut},
		{"qiniu-v6", "device-qiniu", "2409:8728:8ff:1077::1", "2001:db8::1", "七牛", DirectionOut},
		{"qiniu-v6-in", "device-qiniu", "2001:db8::1", "2409:8728:8ff:1077:ffff::1", "七牛", DirectionIn},
		{"v6-longest-prefix", "device-qiniu", "2409:8728:8ff:1077:1::1", "2001:db8::1", "七牛", DirectionOut},
		{"v6-adjacent-prefix", "device-qiniu", "2409:8728:8ff:1078::1", "2001:db8::1", "Other", DirectionOut},
		{"same-prefix-device-scope", "device-other", "2409:8728:8ff:1077::1", "2001:db8::1", "Other device", DirectionOut},
	} {
		t.Run(test.name, func(t *testing.T) {
			direction, customer, configured := snapshot.DeviceDirectionAttribution(
				test.device, netip.MustParseAddr(test.source), netip.MustParseAddr(test.destination),
			)
			if !configured || direction != test.wantDirection || customer != test.customer {
				t.Fatalf("direction=%q customer=%q configured=%v, want %q/%q", direction, customer, configured, test.wantDirection, test.customer)
			}
		})
	}
}

func TestClassificationSchemaV2DefaultLimitSupportsManyDevices(t *testing.T) {
	profiles := make([]ClassificationDeviceProfile, 1_000)
	for index := range profiles {
		profiles[index] = ClassificationDeviceProfile{
			DeviceID: fmt.Sprintf("device-%04d", index),
			SourcePrefixes: []ClassificationSourcePrefix{{
				ID:   fmt.Sprintf("customer-prefix-%04d", index),
				CIDR: "10.0.0.0/8",
			}},
		}
	}
	data, checksum, err := EncodeClassificationBundle(ClassificationDefinition{
		Version: 12, EffectiveFrom: testMinute(12, 0), DimensionSnapshotID: "snapshot-12",
		DeviceProfiles: profiles, InternalPolicy: RecordPolicyCount, TransitPolicy: RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(data) <= 64<<10 {
		t.Fatalf("test bundle is only %d bytes", len(data))
	}
	if _, err := DecodeAndCompileClassificationBundle(data, checksum, ClassificationCompileLimits{}); err != nil {
		t.Fatalf("default limit rejected %d-byte global device catalog: %v", len(data), err)
	}
}

func TestCompileClassificationRejectsInvalidDeviceSources(t *testing.T) {
	valid := ClassificationDefinition{
		Version: 11, EffectiveFrom: testMinute(12, 0), DimensionSnapshotID: "snapshot-11",
		DeviceProfiles: []ClassificationDeviceProfile{{DeviceID: "device-a", SourcePrefixes: []ClassificationSourcePrefix{{ID: "prefix-a", CIDR: "10.0.0.0/8"}}}},
		InternalPolicy: RecordPolicyCount, TransitPolicy: RecordPolicyCount,
	}
	for name, mutate := range map[string]func(*ClassificationDefinition){
		"missing prefix": func(value *ClassificationDefinition) { value.DeviceProfiles[0].SourcePrefixes = nil },
		"duplicate device": func(value *ClassificationDefinition) {
			value.DeviceProfiles = append(value.DeviceProfiles, value.DeviceProfiles[0])
		},
		"duplicate prefix id": func(value *ClassificationDefinition) {
			value.DeviceProfiles[0].SourcePrefixes = append(value.DeviceProfiles[0].SourcePrefixes, ClassificationSourcePrefix{ID: "prefix-a", CIDR: "172.16.0.0/12"})
		},
		"duplicate prefix cidr": func(value *ClassificationDefinition) {
			value.DeviceProfiles[0].SourcePrefixes = append(value.DeviceProfiles[0].SourcePrefixes, ClassificationSourcePrefix{ID: "prefix-b", CIDR: "10.0.0.0/8"})
		},
		"noncanonical prefix": func(value *ClassificationDefinition) { value.DeviceProfiles[0].SourcePrefixes[0].CIDR = "10.1.2.3/8" },
		"legacy mix":          func(value *ClassificationDefinition) { value.HomeProvince = "330000" },
	} {
		t.Run(name, func(t *testing.T) {
			definition := valid
			definition.DeviceProfiles = append([]ClassificationDeviceProfile(nil), valid.DeviceProfiles...)
			definition.DeviceProfiles[0].SourcePrefixes = append([]ClassificationSourcePrefix(nil), valid.DeviceProfiles[0].SourcePrefixes...)
			mutate(&definition)
			if _, err := CompileClassification(definition); err == nil {
				t.Fatal("invalid device source definition was accepted")
			}
		})
	}
}

func TestClassificationCatalogSelectsImmutableEventTimeProfiles(t *testing.T) {
	definition := testClassificationDefinition(1, testMinute(12, 0), "snapshot-1")
	first, err := CompileClassification(definition)
	if err != nil {
		t.Fatal(err)
	}
	definition.HomeISPIDs[0] = 999
	secondDefinition := testClassificationDefinition(2, testMinute(13, 0), "snapshot-2")
	secondDefinition.HomeCity = "330200"
	second, err := CompileClassification(secondDefinition)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewClassificationCatalog(second, first)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := catalog.Select(testMinute(12, 59))
	if err != nil || selected.Metadata().Version != 1 || selected.Metadata().DimensionSnapshotID != "snapshot-1" {
		t.Fatalf("selected=%+v err=%v", selected.Metadata(), err)
	}
	if category := selected.Classify(DirectionOut, GeoInfo{Country: "CN", AdminCode: "330100", ISPID: 3}); category != CategoryOnNetLocalCity {
		t.Fatalf("immutable profile category = %q", category)
	}
	selected, err = catalog.Select(testMinute(13, 0))
	if err != nil || selected.Metadata().Version != 2 {
		t.Fatalf("selected=%+v err=%v", selected.Metadata(), err)
	}
	if _, err := catalog.Select(testMinute(11, 59)); err != ErrNoClassificationSnapshot {
		t.Fatalf("pre-history error = %v", err)
	}
	if err := catalog.Install(first); err != nil {
		t.Fatalf("same snapshot must be idempotent: %v", err)
	}
}

func TestClassificationPoliciesProduceExplicitDisposition(t *testing.T) {
	definition := testClassificationDefinition(1, testMinute(12, 0), "snapshot-1")
	definition.InternalPolicy = RecordPolicyDrop
	snapshot, err := CompileClassification(definition)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Disposition(DirectionInternal) != DispositionDrop || snapshot.Disposition(DirectionTransit) != DispositionCount || snapshot.Disposition(DirectionOut) != DispositionCount {
		t.Fatalf("unexpected policy disposition: %+v", snapshot.Metadata())
	}
}

func TestCompileClassificationRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ClassificationDefinition)
		want   string
	}{
		{name: "identity", mutate: func(value *ClassificationDefinition) { value.DimensionSnapshotID = "" }, want: "required"},
		{name: "effective-time", mutate: func(value *ClassificationDefinition) { value.EffectiveFrom = value.EffectiveFrom.Add(time.Second) }, want: "minute"},
		{name: "province", mutate: func(value *ClassificationDefinition) { value.HomeProvince = "330100" }, want: "province"},
		{name: "city", mutate: func(value *ClassificationDefinition) { value.HomeCity = "320100" }, want: "inside"},
		{name: "duplicate-isp", mutate: func(value *ClassificationDefinition) { value.HomeISPIDs = []uint16{3, 3} }, want: "unique"},
		{name: "zero-isp", mutate: func(value *ClassificationDefinition) { value.HomeISPIDs = []uint16{0} }, want: "non-zero"},
		{name: "duplicate-asn", mutate: func(value *ClassificationDefinition) { value.HomeASNs = []uint32{4134, 4134} }, want: "unique"},
		{name: "zero-asn", mutate: func(value *ClassificationDefinition) { value.HomeASNs = []uint32{0} }, want: "non-zero"},
		{name: "policy", mutate: func(value *ClassificationDefinition) { value.TransitPolicy = "archive" }, want: "count or drop"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := testClassificationDefinition(1, testMinute(12, 0), "snapshot-1")
			test.mutate(&definition)
			if _, err := CompileClassification(definition); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClassificationCatalogRejectsConflictingTimeline(t *testing.T) {
	first, _ := CompileClassification(testClassificationDefinition(1, testMinute(12, 0), "snapshot-1"))
	catalog, err := NewClassificationCatalog(first)
	if err != nil {
		t.Fatal(err)
	}
	sameVersion, _ := CompileClassification(testClassificationDefinition(1, testMinute(13, 0), "snapshot-2"))
	if err := catalog.Install(sameVersion); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("same version error = %v", err)
	}
	sameTime, _ := CompileClassification(testClassificationDefinition(2, testMinute(12, 0), "snapshot-2"))
	if err := catalog.Install(sameTime); err == nil || !strings.Contains(err.Error(), "effective_from") {
		t.Fatalf("same time error = %v", err)
	}
	nonMonotonic, _ := CompileClassification(testClassificationDefinition(3, testMinute(11, 0), "snapshot-3"))
	if err := catalog.Install(nonMonotonic); err == nil || !strings.Contains(err.Error(), "not monotonic") {
		t.Fatalf("non-monotonic error = %v", err)
	}
}

func testClassificationDefinition(version uint32, effectiveFrom time.Time, dimensionSnapshotID string) ClassificationDefinition {
	return ClassificationDefinition{
		Version: version, EffectiveFrom: effectiveFrom, DimensionSnapshotID: dimensionSnapshotID,
		HomeProvince: "330000", HomeCity: "330100", HomeISPIDs: []uint16{3}, HomeASNs: []uint32{4134}, OverseasIncludesHMT: true,
		InternalPolicy: RecordPolicyCount, TransitPolicy: RecordPolicyCount,
	}
}

func classificationChecksum(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}
