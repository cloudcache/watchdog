package watchdog

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

func TestEncryptDecryptSecret(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	const url = "https://hooks.example.com/T/B/supersecret"

	enc, err := encryptSecret(url, key)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(enc, encryptedSecretPrefix) {
		t.Fatalf("ciphertext not prefixed: %s", enc)
	}
	if strings.Contains(enc, "supersecret") {
		t.Fatalf("plaintext leaked into ciphertext: %s", enc)
	}
	// Two encryptions differ (random nonce) but both decrypt.
	enc2, _ := encryptSecret(url, key)
	if enc == enc2 {
		t.Fatal("nonce reuse: two encryptions are identical")
	}
	if got, err := decryptSecret(enc, key); err != nil || got != url {
		t.Fatalf("round trip = %q err=%v", got, err)
	}

	// Legacy plaintext (no prefix) passes through unchanged.
	if got, err := decryptSecret("https://plain.example.com/x", key); err != nil || got != "https://plain.example.com/x" {
		t.Fatalf("legacy passthrough = %q err=%v", got, err)
	}
	// No key: encrypt is a passthrough (encryption disabled).
	if got, err := encryptSecret(url, nil); err != nil || got != url {
		t.Fatalf("no-key encrypt = %q err=%v", got, err)
	}
	// Wrong key and missing key both fail on encrypted input.
	if _, err := decryptSecret(enc, bytes.Repeat([]byte("w"), 32)); err == nil {
		t.Fatal("wrong key must fail to decrypt")
	}
	if _, err := decryptSecret(enc, nil); err == nil {
		t.Fatal("encrypted value with no key must error")
	}
}

func TestDecodeEncryptionKey(t *testing.T) {
	if k, err := decodeEncryptionKey(""); err != nil || k != nil {
		t.Fatalf("empty key should disable encryption: %v err=%v", k, err)
	}
	valid := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("k"), 32))
	if k, err := decodeEncryptionKey(valid); err != nil || len(k) != 32 {
		t.Fatalf("valid key = %v err=%v", k, err)
	}
	if _, err := decodeEncryptionKey("not-base64-!!!"); err == nil {
		t.Fatal("non-base64 key must error")
	}
	short := base64.StdEncoding.EncodeToString([]byte("too-short"))
	if _, err := decodeEncryptionKey(short); err == nil {
		t.Fatal("wrong-length key must error")
	}
}
