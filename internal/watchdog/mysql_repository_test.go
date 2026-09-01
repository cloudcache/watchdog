package watchdog

import "testing"

func TestEncodeDecodeActionsJSON(t *testing.T) {
	encoded, err := encodeActionsJSON([]Action{ActionView, ActionExport, ActionAdmin})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `["view","export","admin"]` {
		t.Fatalf("encoded = %s", encoded)
	}
	decoded, err := decodeActionsJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 3 || decoded[0] != ActionView || decoded[1] != ActionExport || decoded[2] != ActionAdmin {
		t.Fatalf("decoded = %#v", decoded)
	}
}

func TestEncodeActionsJSONEmpty(t *testing.T) {
	encoded, err := encodeActionsJSON(nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `[]` {
		t.Fatalf("encoded = %s", encoded)
	}
}

func TestDecodeActionsJSONRejectsInvalidJSON(t *testing.T) {
	if _, err := decodeActionsJSON([]byte(`{"view":true}`)); err == nil {
		t.Fatal("expected invalid JSON shape error")
	}
}
