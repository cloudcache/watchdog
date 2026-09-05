package watchdog

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"strings"
)

// PLAT P0: webhook secret store. Webhook URLs carry bearer tokens in the path,
// so they are encrypted at rest with AES-256-GCM. Encrypted values carry a
// version prefix; values without it are legacy plaintext (written before a key
// was configured) and read back unchanged, so no data migration is needed.

const encryptedSecretPrefix = "enc:v1:"

// decodeEncryptionKey turns the configured base64 key into 32 raw bytes for
// AES-256. An empty configured key disables encryption (returns nil).
func decodeEncryptionKey(configured string) ([]byte, error) {
	configured = strings.TrimSpace(configured)
	if configured == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(configured)
	if err != nil {
		return nil, errors.New("encryption key must be base64-encoded")
	}
	if len(key) != 32 {
		return nil, errors.New("encryption key must decode to 32 bytes (AES-256)")
	}
	return key, nil
}

// encryptSecret encrypts plaintext with AES-256-GCM into a prefixed base64
// string. With no key it returns the plaintext unchanged (encryption disabled).
func encryptSecret(plaintext string, key []byte) (string, error) {
	if len(key) == 0 {
		return plaintext, nil
	}
	gcm, err := newSecretGCM(key)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return encryptedSecretPrefix + base64.StdEncoding.EncodeToString(sealed), nil
}

// decryptSecret reverses encryptSecret. A value without the prefix is legacy
// plaintext and returned unchanged.
func decryptSecret(value string, key []byte) (string, error) {
	if !strings.HasPrefix(value, encryptedSecretPrefix) {
		return value, nil
	}
	if len(key) == 0 {
		return "", errors.New("value is encrypted but no encryption key is configured")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, encryptedSecretPrefix))
	if err != nil {
		return "", err
	}
	gcm, err := newSecretGCM(key)
	if err != nil {
		return "", err
	}
	if len(raw) < gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}
	nonce, ciphertext := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

func newSecretGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
