package agentplan

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type Signer struct {
	KeyID      string
	PrivateKey ed25519.PrivateKey
}

func (s Signer) Sign(metadata Metadata, canonicalPayload []byte) ([]byte, []byte, error) {
	if len(s.PrivateKey) != ed25519.PrivateKeySize || s.KeyID == "" || metadata.SigningKeyID != s.KeyID {
		return nil, nil, errors.New("agent plan signer is not configured")
	}
	canonical, digest, spec, err := CanonicalSpec(canonicalPayload)
	if err != nil {
		return nil, nil, err
	}
	if metadata.PayloadSHA256 != digest || metadata.Kind != spec.Kind || metadata.APIVersion != spec.APIVersion {
		return nil, nil, errors.New("agent plan metadata does not match payload")
	}
	signingPayload, err := SigningPayload(metadata)
	if err != nil {
		return nil, nil, err
	}
	signature := ed25519.Sign(s.PrivateKey, signingPayload)
	envelope := Envelope{SchemaVersion: SchemaVersion, Metadata: metadata, Payload: canonical, Signature: base64.StdEncoding.EncodeToString(signature)}
	encoded, err := json.Marshal(envelope)
	return encoded, signature, err
}

func Verify(envelopeData []byte, publicKey ed25519.PublicKey, now time.Time, enforceValidity bool) (Envelope, Spec, error) {
	if len(envelopeData) == 0 || len(envelopeData) > MaxPayloadSize+(64<<10) || len(publicKey) != ed25519.PublicKeySize {
		return Envelope{}, Spec{}, errors.New("signed agent plan or public key size is invalid")
	}
	var envelope Envelope
	decoder := json.NewDecoder(bytes.NewReader(envelopeData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return Envelope{}, Spec{}, fmt.Errorf("decode signed agent plan: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Envelope{}, Spec{}, errors.New("signed agent plan must contain exactly one JSON document")
	}
	if envelope.SchemaVersion != SchemaVersion {
		return Envelope{}, Spec{}, errors.New("unsupported signed agent plan envelope")
	}
	canonical, digest, spec, err := CanonicalSpec(envelope.Payload)
	if err != nil {
		return Envelope{}, Spec{}, err
	}
	if !bytes.Equal(canonical, envelope.Payload) || digest != envelope.Metadata.PayloadSHA256 || spec.Kind != envelope.Metadata.Kind || spec.APIVersion != envelope.Metadata.APIVersion {
		return Envelope{}, Spec{}, errors.New("signed agent plan payload integrity check failed")
	}
	signingPayload, err := SigningPayload(envelope.Metadata)
	if err != nil {
		return Envelope{}, Spec{}, err
	}
	signature, err := base64.StdEncoding.DecodeString(envelope.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(publicKey, signingPayload, signature) {
		return Envelope{}, Spec{}, errors.New("agent plan signature verification failed")
	}
	if enforceValidity {
		if err := MetadataValidAt(envelope.Metadata, now); err != nil {
			return Envelope{}, Spec{}, err
		}
	}
	return envelope, spec, nil
}

func LoadOrCreateSigner(path, keyID string) (Signer, ed25519.PublicKey, error) {
	if path == "" || keyID == "" {
		return Signer{}, nil, errors.New("agent plan signing key path and id are required")
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		publicKey, privateKey, genErr := ed25519.GenerateKey(rand.Reader)
		if genErr != nil {
			return Signer{}, nil, genErr
		}
		encoded, marshalErr := x509.MarshalPKCS8PrivateKey(privateKey)
		if marshalErr != nil {
			return Signer{}, nil, marshalErr
		}
		data = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
		if err := atomicWriteFile(path, data, 0o600); err != nil {
			return Signer{}, nil, err
		}
		return Signer{KeyID: keyID, PrivateKey: privateKey}, publicKey, nil
	}
	if err != nil {
		return Signer{}, nil, err
	}
	privateKey, err := parsePrivateKey(data)
	if err != nil {
		return Signer{}, nil, err
	}
	publicKey := append(ed25519.PublicKey(nil), privateKey.Public().(ed25519.PublicKey)...)
	return Signer{KeyID: keyID, PrivateKey: privateKey}, publicKey, nil
}

func LoadPublicKey(path string) (ed25519.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if block, _ := pem.Decode(data); block != nil {
		if key, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
			if publicKey, ok := key.(ed25519.PublicKey); ok {
				return append(ed25519.PublicKey(nil), publicKey...), nil
			}
		}
	}
	raw, err := base64.StdEncoding.DecodeString(string(bytes.TrimSpace(data)))
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, errors.New("agent plan public key is not Ed25519")
	}
	return ed25519.PublicKey(raw), nil
}

func WritePublicKey(path string, key ed25519.PublicKey) error {
	if len(key) != ed25519.PublicKeySize {
		return errors.New("agent plan public key is not Ed25519")
	}
	encoded, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return err
	}
	return atomicWriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded}), 0o644)
}

func parsePrivateKey(data []byte) (ed25519.PrivateKey, error) {
	if block, _ := pem.Decode(data); block != nil {
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err == nil {
			if privateKey, ok := key.(ed25519.PrivateKey); ok {
				return append(ed25519.PrivateKey(nil), privateKey...), nil
			}
		}
	}
	raw, err := base64.StdEncoding.DecodeString(string(bytes.TrimSpace(data)))
	if err != nil || (len(raw) != ed25519.PrivateKeySize && len(raw) != ed25519.SeedSize) {
		return nil, errors.New("agent plan private key is not Ed25519")
	}
	if len(raw) == ed25519.SeedSize {
		return ed25519.NewKeyFromSeed(raw), nil
	}
	return ed25519.PrivateKey(raw), nil
}

func atomicWriteFile(path string, data []byte, mode os.FileMode) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".agent-plan-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err == nil {
		err = dir.Sync()
		_ = dir.Close()
	}
	return err
}
