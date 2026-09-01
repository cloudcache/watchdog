package watchdog

import (
	"testing"
	"time"
)

func TestStringMapJSONRoundTrip(t *testing.T) {
	encoded, err := encodeStringMapJSON(map[string]string{"side_type": "provider", "rack": "a1"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeStringMapJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded["side_type"] != "provider" || decoded["rack"] != "a1" {
		t.Fatalf("decoded = %#v", decoded)
	}
}

func TestAnyMapJSONRoundTrip(t *testing.T) {
	encoded, err := encodeAnyMapJSON(map[string]any{"rx": -3.2, "module": "QSFP"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeAnyMapJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded["module"] != "QSFP" {
		t.Fatalf("decoded = %#v", decoded)
	}
}

func TestDecodeMapJSONRejectsInvalidShapes(t *testing.T) {
	if _, err := decodeStringMapJSON([]byte(`[]`)); err == nil {
		t.Fatal("expected string map decode error")
	}
	if _, err := decodeAnyMapJSON([]byte(`[]`)); err == nil {
		t.Fatal("expected any map decode error")
	}
}

func TestTrafficPolicyDefaultNormalizeFromSQLShape(t *testing.T) {
	policyDefault := TrafficPolicyDefault{
		SideType:            PortSideProvider,
		BillingBaseBps:      2000,
		SampleStep:          time.Minute,
		CorrectionDirection: CorrectionUp,
		CorrectionMin:       10,
		CorrectionMax:       20,
	}.Normalize(PortSideProvider)
	if policyDefault.SampleStep != time.Minute {
		t.Fatalf("sample step = %s", policyDefault.SampleStep)
	}
	if policyDefault.BillingBaseBps != 2000 {
		t.Fatalf("billing base = %d", policyDefault.BillingBaseBps)
	}
}
