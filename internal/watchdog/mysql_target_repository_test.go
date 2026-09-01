package watchdog

import "testing"

func TestDefaultString(t *testing.T) {
	if got := defaultString("", "pending"); got != "pending" {
		t.Fatalf("default = %s", got)
	}
	if got := defaultString("up", "pending"); got != "up" {
		t.Fatalf("explicit = %s", got)
	}
}

func TestTargetLabelsJSONRoundTrip(t *testing.T) {
	encoded, err := encodeLabelsForTest(map[string]string{"site": "shanghai", "role": "edge"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeStringMapJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded["site"] != "shanghai" || decoded["role"] != "edge" {
		t.Fatalf("labels = %#v", decoded)
	}
}
