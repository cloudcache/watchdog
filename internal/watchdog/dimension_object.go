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

type DimensionObject struct {
	Ref      string
	Checksum string
	Size     uint64
}

type DimensionObjectStore interface {
	SaveDimensionObject(context.Context, ID, ID, []byte) (DimensionObject, error)
	ResolveDimensionObject(string) (string, error)
	RemoveDimensionObject(string) error
}

type DiskDimensionObjectStore struct {
	Dir      string
	MaxBytes int
}

func (s DiskDimensionObjectStore) SaveDimensionObject(ctx context.Context, tenantID, snapshotID ID, data []byte) (DimensionObject, error) {
	if err := ctx.Err(); err != nil {
		return DimensionObject{}, err
	}
	maximum := s.MaxBytes
	if maximum <= 0 {
		maximum = 64 << 20
	}
	if len(data) == 0 || len(data) > maximum {
		return DimensionObject{}, fmt.Errorf("dimension object size must be 1..%d bytes", maximum)
	}
	filename := "bundle.json"
	if len(data) >= 4 && string(data[:4]) == "WADS" {
		filename = "address-snapshot.wads"
	}
	ref := filepath.ToSlash(filepath.Join("dimension-snapshots", string(tenantID), string(snapshotID), filename))
	path, err := s.dimensionObjectPath(ref)
	if err != nil {
		return DimensionObject{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return DimensionObject{}, err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".bundle-*.tmp")
	if err != nil {
		return DimensionObject{}, err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o640); err != nil {
		temporary.Close()
		return DimensionObject{}, err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return DimensionObject{}, err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return DimensionObject{}, err
	}
	if err := temporary.Close(); err != nil {
		return DimensionObject{}, err
	}
	// Link instead of rename so a repeated snapshot ID can never replace an
	// immutable object that has already been published.
	if err := os.Link(temporaryPath, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return DimensionObject{}, err
		}
		matches, verifyErr := dimensionObjectMatches(path, data)
		if verifyErr != nil {
			return DimensionObject{}, verifyErr
		}
		if !matches {
			return DimensionObject{}, errors.New("immutable dimension object already exists with different content")
		}
	}
	digest := sha256.Sum256(data)
	return DimensionObject{Ref: ref, Checksum: "sha256:" + hex.EncodeToString(digest[:]), Size: uint64(len(data))}, nil
}

func (s DiskDimensionObjectStore) ResolveDimensionObject(ref string) (string, error) {
	path, err := s.dimensionObjectPath(ref)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("dimension object is not a regular file")
	}
	return path, nil
}

func (s DiskDimensionObjectStore) RemoveDimensionObject(ref string) error {
	path, err := s.dimensionObjectPath(ref)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s DiskDimensionObjectStore) dimensionObjectPath(ref string) (string, error) {
	if strings.TrimSpace(s.Dir) == "" {
		return "", errors.New("dimension object directory is required")
	}
	parts := strings.Split(filepath.ToSlash(ref), "/")
	if len(parts) != 4 || parts[0] != "dimension-snapshots" || !safeAddressArtifactID(parts[1]) || !safeAddressArtifactID(parts[2]) || (parts[3] != "bundle.json" && parts[3] != "address-snapshot.wads") {
		return "", errors.New("invalid dimension object reference")
	}
	root, err := filepath.Abs(s.Dir)
	if err != nil {
		return "", err
	}
	path := filepath.Join(root, filepath.FromSlash(ref))
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("dimension object reference escapes its directory")
	}
	return path, nil
}

func dimensionObjectMatches(path string, expected []byte) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Size() != int64(len(expected)) {
		return false, nil
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return false, err
	}
	digest := sha256.Sum256(expected)
	return string(hash.Sum(nil)) == string(digest[:]), nil
}
