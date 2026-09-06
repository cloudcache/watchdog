package watchdog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const DefaultAddressArtifactMaxBytes int64 = 2 << 30

type AddressArtifact struct {
	Ref            string
	Format         string
	ChecksumSHA256 string
	SizeBytes      uint64
}

type AddressArtifactStore interface {
	SaveAddressArtifact(ctx context.Context, tenantID, importID ID, originalName string, source io.Reader) (AddressArtifact, error)
	ResolveAddressArtifact(ref string) (string, error)
	RemoveAddressArtifact(ref string) error
}

type DiskAddressArtifactStore struct {
	Dir      string
	MaxBytes int64
}

func (s DiskAddressArtifactStore) SaveAddressArtifact(ctx context.Context, tenantID, importID ID, originalName string, source io.Reader) (AddressArtifact, error) {
	if source == nil || !safeAddressArtifactID(string(tenantID)) || !safeAddressArtifactID(string(importID)) {
		return AddressArtifact{}, errors.New("address artifact tenant, import, and source are required")
	}
	format := strings.TrimPrefix(strings.ToLower(filepath.Ext(strings.TrimSpace(originalName))), ".")
	if format != AddressImportFormatMMDB && format != AddressImportFormatIPDB {
		return AddressArtifact{}, errors.New("address artifact must have a .mmdb or .ipdb extension")
	}
	if strings.TrimSpace(s.Dir) == "" {
		return AddressArtifact{}, errors.New("address artifact directory is required")
	}
	maxBytes := s.MaxBytes
	if maxBytes <= 0 {
		maxBytes = DefaultAddressArtifactMaxBytes
	}
	ref := filepath.ToSlash(filepath.Join("address-imports", string(tenantID), string(importID), "source."+format))
	path, err := s.addressArtifactPath(ref)
	if err != nil {
		return AddressArtifact{}, err
	}
	if _, err := os.Lstat(path); err == nil {
		return AddressArtifact{}, errors.New("address artifact already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return AddressArtifact{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return AddressArtifact{}, err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".upload-*")
	if err != nil {
		return AddressArtifact{}, err
	}
	temporaryName := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(temporaryName)
		}
	}()
	hash := sha256.New()
	written, err := copyAddressArtifact(ctx, io.MultiWriter(temporary, hash), source, maxBytes)
	if err != nil {
		return AddressArtifact{}, err
	}
	if written == 0 {
		return AddressArtifact{}, errors.New("address artifact is empty")
	}
	if err := temporary.Sync(); err != nil {
		return AddressArtifact{}, err
	}
	if err := temporary.Close(); err != nil {
		return AddressArtifact{}, err
	}
	if err := os.Chmod(temporaryName, 0o600); err != nil {
		return AddressArtifact{}, err
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return AddressArtifact{}, err
	}
	keep = true
	return AddressArtifact{Ref: ref, Format: format, ChecksumSHA256: hex.EncodeToString(hash.Sum(nil)), SizeBytes: uint64(written)}, nil
}

func copyAddressArtifact(ctx context.Context, destination io.Writer, source io.Reader, maxBytes int64) (int64, error) {
	limited := io.LimitReader(source, maxBytes+1)
	buffer := make([]byte, 1<<20)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		read, readErr := limited.Read(buffer)
		if read > 0 {
			if written+int64(read) > maxBytes {
				return written, fmt.Errorf("address artifact exceeds %d-byte limit", maxBytes)
			}
			count, writeErr := destination.Write(buffer[:read])
			written += int64(count)
			if writeErr != nil {
				return written, writeErr
			}
			if count != read {
				return written, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return written, nil
		}
		if readErr != nil {
			return written, readErr
		}
	}
}

func (s DiskAddressArtifactStore) ResolveAddressArtifact(ref string) (string, error) {
	path, err := s.addressArtifactPath(ref)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("address artifact is not a regular file")
	}
	return path, nil
}

func (s DiskAddressArtifactStore) RemoveAddressArtifact(ref string) error {
	path, err := s.addressArtifactPath(ref)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s DiskAddressArtifactStore) addressArtifactPath(ref string) (string, error) {
	parts := strings.Split(filepath.ToSlash(strings.TrimSpace(ref)), "/")
	if len(parts) != 4 || parts[0] != "address-imports" || !safeAddressArtifactID(parts[1]) || !safeAddressArtifactID(parts[2]) || (parts[3] != "source.mmdb" && parts[3] != "source.ipdb") {
		return "", errors.New("invalid address artifact reference")
	}
	base, err := filepath.Abs(s.Dir)
	if err != nil {
		return "", err
	}
	path, err := filepath.Abs(filepath.Join(base, filepath.FromSlash(ref)))
	if err != nil {
		return "", err
	}
	if path == base || !strings.HasPrefix(path, base+string(os.PathSeparator)) {
		return "", errors.New("address artifact reference escapes its directory")
	}
	return path, nil
}

func safeAddressArtifactID(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' && char != '-' {
			return false
		}
	}
	return true
}
