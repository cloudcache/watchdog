package watchdog

import (
	"encoding/base64"
	"testing"
)

func TestStringCursorRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		sort string
		id   ID
	}{
		{"plain", "Core Router", "01HXA0"},
		{"empty sort value", "", "01HXB0"},
		{"sort value contains separator", "a|b|c", "01HXC0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSort, gotID, err := decodeStringCursor(encodeStringCursor(tc.sort, tc.id))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if gotSort != tc.sort || gotID != tc.id {
				t.Fatalf("round trip = (%q, %q), want (%q, %q)", gotSort, gotID, tc.sort, tc.id)
			}
		})
	}
}

func TestDecodeStringCursorInvalid(t *testing.T) {
	if _, _, err := decodeStringCursor("!!!not base64!!!"); err == nil {
		t.Fatal("malformed base64 cursor must error")
	}
	// Base64 of a payload with no "|" separator.
	noSep := base64.RawURLEncoding.EncodeToString([]byte("noseparator"))
	if _, _, err := decodeStringCursor(noSep); err == nil {
		t.Fatal("cursor without separator must error")
	}
}
