package watchdog

import (
	"errors"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

func TestNormalizeFlowClassificationProfileCanonicalizesSets(t *testing.T) {
	draft, firstDigest, err := normalizeFlowClassificationProfile("tenant-profile", FlowClassificationProfileDraft{
		HomeProvince: " 110000 ", HomeCity: "110100",
		HomeISPIDs: []uint16{9, 3, 9}, HomeASNs: []uint32{4837, 4134, 4837},
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyDrop,
	})
	if err != nil {
		t.Fatal(err)
	}
	if draft.HomeProvince != "110000" || len(draft.HomeISPIDs) != 2 || draft.HomeISPIDs[0] != 3 ||
		len(draft.HomeASNs) != 2 || draft.HomeASNs[0] != 4134 || len(firstDigest) != 64 {
		t.Fatalf("normalized draft=%+v digest=%q", draft, firstDigest)
	}
	_, reorderedDigest, err := normalizeFlowClassificationProfile("tenant-profile", FlowClassificationProfileDraft{
		HomeProvince: "110000", HomeCity: "110100",
		HomeISPIDs: []uint16{3, 9}, HomeASNs: []uint32{4134, 4837},
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyDrop,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reorderedDigest != firstDigest {
		t.Fatalf("canonical digest changed: %s != %s", reorderedDigest, firstDigest)
	}
	empty, _, err := normalizeFlowClassificationProfile("tenant-profile", FlowClassificationProfileDraft{
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	if empty.HomeISPIDs == nil || empty.HomeASNs == nil {
		t.Fatalf("empty sets must remain JSON arrays: %+v", empty)
	}
}

func TestNormalizeFlowClassificationProfileRejectsInvalidGeography(t *testing.T) {
	_, _, err := normalizeFlowClassificationProfile("tenant-profile", FlowClassificationProfileDraft{
		HomeProvince: "110000", HomeCity: "310100",
		InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if !errors.Is(err, ErrFlowClassificationProfileInvalid) {
		t.Fatalf("invalid geography error = %v", err)
	}
}
