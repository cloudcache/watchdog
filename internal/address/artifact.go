package address

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

// defaultAddressArtifactMaxBytes caps an uploaded source database at 2 GiB.
const defaultAddressArtifactMaxBytes int64 = 2 << 30

type addressArtifact struct {
	Ref            string
	Format         string
	ChecksumSHA256 string
	SizeBytes      uint64
}

// diskAddressArtifactStore keeps uploaded MMDB/IPDB source files on local disk
// under Dir. Refs are relative slash paths (address-imports/<importID>/source.<fmt>)
// so ResolveArtifact can be sandboxed to Dir. De-tenanted port of the SaaS store.
type diskAddressArtifactStore struct {
	Dir      string
	MaxBytes int64
}

func (s diskAddressArtifactStore) SaveArtifact(ctx context.Context, importID, originalName string, source io.Reader) (addressArtifact, error) {
	if source == nil || !safeAddressArtifactID(importID) {
		return addressArtifact{}, errors.New("address artifact import id and source are required")
	}
	format := strings.TrimPrefix(strings.ToLower(filepath.Ext(strings.TrimSpace(originalName))), ".")
	if format != AddressImportFormatMMDB && format != AddressImportFormatIPDB {
		return addressArtifact{}, errors.New("address artifact must have a .mmdb or .ipdb extension")
	}
	if strings.TrimSpace(s.Dir) == "" {
		return addressArtifact{}, errors.New("address artifact directory is required")
	}
	maxBytes := s.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultAddressArtifactMaxBytes
	}
	ref := filepath.ToSlash(filepath.Join("address-imports", importID, "source."+format))
	path, err := s.artifactPath(ref)
	if err != nil {
		return addressArtifact{}, err
	}
	if _, err := os.Lstat(path); err == nil {
		return addressArtifact{}, errors.New("address artifact already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return addressArtifact{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return addressArtifact{}, err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".upload-*")
	if err != nil {
		return addressArtifact{}, err
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
		return addressArtifact{}, err
	}
	if written == 0 {
		return addressArtifact{}, errors.New("address artifact is empty")
	}
	if err := temporary.Sync(); err != nil {
		return addressArtifact{}, err
	}
	if err := temporary.Close(); err != nil {
		return addressArtifact{}, err
	}
	if err := os.Chmod(temporaryName, 0o600); err != nil {
		return addressArtifact{}, err
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return addressArtifact{}, err
	}
	keep = true
	return addressArtifact{Ref: ref, Format: format, ChecksumSHA256: hex.EncodeToString(hash.Sum(nil)), SizeBytes: uint64(written)}, nil
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

func (s diskAddressArtifactStore) ResolveArtifact(ref string) (string, error) {
	path, err := s.artifactPath(ref)
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

func (s diskAddressArtifactStore) RemoveArtifact(ref string) error {
	path, err := s.artifactPath(ref)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s diskAddressArtifactStore) artifactPath(ref string) (string, error) {
	parts := strings.Split(filepath.ToSlash(strings.TrimSpace(ref)), "/")
	if len(parts) != 3 || parts[0] != "address-imports" || !safeAddressArtifactID(parts[1]) || (parts[2] != "source.mmdb" && parts[2] != "source.ipdb") {
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
