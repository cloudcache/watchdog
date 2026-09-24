package flowworker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
)

const (
	versionLKGTrustFile             = "trust-bundle.json"
	versionLKGMaxTrustBytes         = 1 << 20
	versionLKGMaxPublicationCount   = 65_536
	versionLKGPublicationNameLength = 15
)

// DiskVersionLKG is a content-addressed object cache plus immutable signed
// publication manifests. A manifest is linked into place only after both
// objects have passed their declared SHA boundary and the VersionLoader has
// compiled the complete pair. The loader persists the manifest before the
// catalog pointer is published.
type DiskVersionLKG struct {
	dir string

	mu           sync.RWMutex
	references   map[string]string
	publications map[string]registeredLKGPublication
	deployments  map[string]registeredLKGDeployment
}

type registeredLKGPublication struct {
	version  uint32
	envelope []byte
}

type registeredLKGDeployment struct {
	generation uint64
	envelope   []byte
}

type VersionLKGRestoreResult struct {
	PublicationCount uint32
	HighestVersion   uint32
}

func NewDiskVersionLKG(dir string) (*DiskVersionLKG, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, errors.New("version LKG directory is required")
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	return &DiskVersionLKG{
		dir: absolute, references: make(map[string]string), publications: make(map[string]registeredLKGPublication),
		deployments: make(map[string]registeredLKGDeployment),
	}, nil
}

func (s *DiskVersionLKG) SaveTrustBundle(ctx context.Context, data []byte) error {
	if s == nil || ctx == nil {
		return errors.New("version LKG is not initialized")
	}
	next, _, err := flowplan.ParseTrustBundle(data)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	currentData, err := s.LoadTrustBundle()
	if err == nil {
		current, _, parseErr := flowplan.ParseTrustBundle(currentData)
		if parseErr != nil {
			return parseErr
		}
		if next.Generation < current.Generation {
			return flowplan.ErrTrustBundleRollback
		}
		if next.Generation == current.Generation {
			if bytes.Equal(currentData, data) {
				return nil
			}
			return flowplan.ErrTrustBundleConflict
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return atomicReplaceVersionFile(filepath.Join(s.dir, versionLKGTrustFile), data, versionLKGMaxTrustBytes)
}

func (s *DiskVersionLKG) LoadTrustBundle() ([]byte, error) {
	if s == nil {
		return nil, errors.New("version LKG is not initialized")
	}
	return readVersionFile(filepath.Join(s.dir, versionLKGTrustFile), versionLKGMaxTrustBytes)
}

// RegisterVerifiedPublication binds signed metadata to object refs. Callers
// must first verify the signature against their monotonic TrustStore; this
// method additionally rejects non-canonical or mismatched wire bytes.
func (s *DiskVersionLKG) RegisterVerifiedPublication(envelope SignedEnrichmentVersionPublication, data []byte) error {
	if s == nil {
		return errors.New("version LKG is not initialized")
	}
	canonical, err := MarshalSignedEnrichmentVersionPublication(envelope)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, data) {
		return errors.New("registered enrichment publication is not canonical")
	}
	publication := envelope.Publication
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.publications[publication.PublicationID]; ok && (current.version != publication.ClassificationVersion || !bytes.Equal(current.envelope, data)) {
		return errors.New("enrichment publication identity is immutable")
	}
	for objectRef, checksum := range map[string]string{
		publication.Dimension.ObjectRef:      publication.Dimension.Checksum,
		publication.Classification.ObjectRef: publication.Classification.Checksum,
	} {
		if current, ok := s.references[objectRef]; ok && current != checksum {
			return errors.New("enrichment object reference is immutable")
		}
		s.references[objectRef] = checksum
	}
	s.publications[publication.PublicationID] = registeredLKGPublication{version: publication.ClassificationVersion, envelope: append([]byte(nil), data...)}
	return nil
}

// RegisterVerifiedDeployment binds an already signature-verified manifest to
// its content-addressed artifacts. Artifact IDs are safe local lookup keys;
// the server-side filesystem object refs never cross the machine API.
func (s *DiskVersionLKG) RegisterVerifiedDeployment(envelope SignedWorkerDeploymentManifest, data []byte) error {
	if s == nil {
		return errors.New("version LKG is not initialized")
	}
	canonical, err := MarshalSignedWorkerDeploymentManifest(envelope)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, data) {
		return errors.New("registered worker deployment is not canonical")
	}
	manifest := envelope.Manifest
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.deployments[manifest.DeploymentID]; ok && (current.generation != manifest.Generation || !bytes.Equal(current.envelope, data)) {
		return errors.New("worker deployment identity is immutable")
	}
	for _, artifact := range manifest.Artifacts {
		if current, ok := s.references[artifact.ArtifactID]; ok && current != artifact.Checksum {
			return errors.New("worker deployment artifact identity is immutable")
		}
		s.references[artifact.ArtifactID] = artifact.Checksum
	}
	s.deployments[manifest.DeploymentID] = registeredLKGDeployment{generation: manifest.Generation, envelope: append([]byte(nil), data...)}
	return nil
}

func (s *DiskVersionLKG) StoreObject(ctx context.Context, objectRef, checksum string, maxBytes int, source io.Reader) error {
	if s == nil || ctx == nil || source == nil || !validObjectRef(objectRef) || !validSHA256(checksum) || maxBytes < 1 {
		return errors.New("version LKG object input is invalid")
	}
	path := s.objectPath(checksum)
	if data, err := readVersionFile(path, maxBytes); err == nil {
		if versionObjectDigest(data) != checksum {
			return fmt.Errorf("%w: cached object checksum mismatch", ErrVersionObjectIntegrity)
		}
		return s.registerObjectReference(objectRef, checksum)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".object-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(&contextVersionReader{ctx: ctx, reader: source}, int64(maxBytes)+1))
	if copyErr == nil && written > int64(maxBytes) {
		copyErr = fmt.Errorf("%w: object exceeds %d bytes", ErrVersionObjectIntegrity, maxBytes)
	}
	if copyErr == nil && written == 0 {
		copyErr = fmt.Errorf("%w: object is empty", ErrVersionObjectIntegrity)
	}
	actualChecksum := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if copyErr == nil && actualChecksum != checksum {
		copyErr = fmt.Errorf("%w: object checksum mismatch", ErrVersionObjectIntegrity)
	}
	if copyErr == nil {
		copyErr = temporary.Sync()
	}
	closeErr := temporary.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Link(temporaryPath, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		data, readErr := readVersionFile(path, maxBytes)
		if readErr != nil || versionObjectDigest(data) != checksum {
			return fmt.Errorf("%w: immutable object collision", ErrVersionObjectIntegrity)
		}
	}
	if err := syncVersionDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	return s.registerObjectReference(objectRef, checksum)
}

// objectAvailable validates an already downloaded content-addressed object so
// ACK retries do not transfer it again. Missing is not an error; corruption is.
func (s *DiskVersionLKG) objectAvailable(ctx context.Context, checksum string, maxBytes int) (bool, error) {
	if s == nil || ctx == nil || !validSHA256(checksum) || maxBytes < 1 {
		return false, errors.New("version LKG object request is invalid")
	}
	file, err := os.Open(s.objectPath(checksum))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > int64(maxBytes) {
		return false, fmt.Errorf("%w: cached object size is invalid", ErrVersionObjectIntegrity)
	}
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(&contextVersionReader{ctx: ctx, reader: file}, int64(maxBytes)+1))
	if err != nil {
		return false, err
	}
	if written != info.Size() || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != checksum {
		return false, fmt.Errorf("%w: cached object checksum mismatch", ErrVersionObjectIntegrity)
	}
	return true, nil
}

func (s *DiskVersionLKG) Fetch(ctx context.Context, objectRef string, maxBytes int) ([]byte, error) {
	if s == nil || ctx == nil || !validObjectRef(objectRef) || maxBytes < 1 {
		return nil, errors.New("version LKG object request is invalid")
	}
	s.mu.RLock()
	checksum := s.references[objectRef]
	s.mu.RUnlock()
	if checksum == "" {
		return nil, ErrVersionObjectUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := readVersionFile(s.objectPath(checksum), maxBytes)
	if err != nil {
		return nil, err
	}
	if versionObjectDigest(data) != checksum {
		return nil, fmt.Errorf("%w: cached object checksum mismatch", ErrVersionObjectIntegrity)
	}
	return data, nil
}

func (s *DiskVersionLKG) PersistVersion(ctx context.Context, publication EnrichmentVersionPublication) error {
	if s == nil || ctx == nil {
		return errors.New("version LKG is not initialized")
	}
	s.mu.RLock()
	registered, ok := s.publications[publication.PublicationID]
	s.mu.RUnlock()
	if !ok || registered.version != publication.ClassificationVersion {
		return errors.New("signed enrichment publication was not registered")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Join(s.dir, "publications", versionLKGPublicationFilename(publication.ClassificationVersion))
	return writeImmutableVersionFile(path, registered.envelope, maxEnrichmentVersionEnvelopeBytes)
}

func (s *DiskVersionLKG) PersistDeployment(ctx context.Context, manifest WorkerDeploymentManifest) error {
	if s == nil || ctx == nil {
		return errors.New("version LKG is not initialized")
	}
	s.mu.RLock()
	registered, ok := s.deployments[manifest.DeploymentID]
	s.mu.RUnlock()
	if !ok || registered.generation != manifest.Generation {
		return errors.New("signed worker deployment was not registered")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Join(s.dir, "deployments", deploymentLKGFilename(manifest.Generation))
	return writeImmutableVersionFile(path, registered.envelope, maxWorkerDeploymentEnvelopeBytes)
}

type DeploymentLKGRestoreResult struct {
	DeploymentCount   uint64
	HighestGeneration uint64
}

// RestoreDeployments verifies and installs v2 manifests from oldest to newest.
// It is called during bootstrap before the worker accepts Kafka records, so an
// error fails startup instead of exposing a partially restored history.
func (s *DiskVersionLKG) RestoreDeployments(ctx context.Context, trust *flowplan.TrustStore, catalog *EnrichmentVersionCatalog, limits DeploymentLoaderLimits, now time.Time) (DeploymentLKGRestoreResult, error) {
	if s == nil || ctx == nil || trust == nil || catalog == nil || now.IsZero() {
		return DeploymentLKGRestoreResult{}, errors.New("deployment LKG restore input is invalid")
	}
	paths, err := filepath.Glob(filepath.Join(s.dir, "deployments", "*.json"))
	if err != nil {
		return DeploymentLKGRestoreResult{}, err
	}
	if len(paths) == 0 {
		return DeploymentLKGRestoreResult{}, ErrNoVersionLKG
	}
	if len(paths) > versionLKGMaxPublicationCount {
		return DeploymentLKGRestoreResult{}, errors.New("deployment LKG count is invalid")
	}
	sort.Strings(paths)
	loader, err := NewDeploymentLoader(s, discardDeploymentAcknowledgement{}, catalog, limits, nil)
	if err != nil {
		return DeploymentLKGRestoreResult{}, err
	}
	result := DeploymentLKGRestoreResult{}
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return DeploymentLKGRestoreResult{}, err
		}
		generation, err := parseDeploymentLKGFilename(filepath.Base(path))
		if err != nil {
			return DeploymentLKGRestoreResult{}, err
		}
		data, err := readVersionFile(path, maxWorkerDeploymentEnvelopeBytes)
		if err != nil {
			return DeploymentLKGRestoreResult{}, err
		}
		envelope, err := VerifySignedWorkerDeploymentManifest(data, trust, now)
		if err != nil {
			return DeploymentLKGRestoreResult{}, err
		}
		if envelope.Manifest.Generation != generation {
			return DeploymentLKGRestoreResult{}, errors.New("deployment LKG filename and generation differ")
		}
		if err := s.RegisterVerifiedDeployment(envelope, data); err != nil {
			return DeploymentLKGRestoreResult{}, err
		}
		if err := loader.Install(ctx, envelope.Manifest, WorkerDeploymentManifestChecksum(data)); err != nil {
			return DeploymentLKGRestoreResult{}, err
		}
		result.DeploymentCount++
		result.HighestGeneration = generation
	}
	return result, nil
}

func (s *DiskVersionLKG) Restore(ctx context.Context, trust *flowplan.TrustStore, catalog *EnrichmentVersionCatalog, identity VersionWorkerIdentity, limits VersionLoaderLimits, now time.Time) (VersionLKGRestoreResult, error) {
	if s == nil || ctx == nil || trust == nil || catalog == nil || now.IsZero() {
		return VersionLKGRestoreResult{}, errors.New("version LKG restore input is invalid")
	}
	trustData, err := s.LoadTrustBundle()
	if err != nil {
		return VersionLKGRestoreResult{}, err
	}
	if err := trust.Install(trustData); err != nil {
		return VersionLKGRestoreResult{}, err
	}
	paths, err := filepath.Glob(filepath.Join(s.dir, "publications", "*.json"))
	if err != nil {
		return VersionLKGRestoreResult{}, err
	}
	if len(paths) == 0 || len(paths) > versionLKGMaxPublicationCount {
		if len(paths) == 0 {
			return VersionLKGRestoreResult{}, ErrNoVersionLKG
		}
		return VersionLKGRestoreResult{}, errors.New("version LKG publication count is invalid")
	}
	sort.Strings(paths)
	staged, err := NewEnrichmentVersionCatalog()
	if err != nil {
		return VersionLKGRestoreResult{}, err
	}
	loader, err := NewVersionLoader(s, discardVersionAcknowledgement{}, staged, identity, limits)
	if err != nil {
		return VersionLKGRestoreResult{}, err
	}
	result := VersionLKGRestoreResult{}
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return VersionLKGRestoreResult{}, err
		}
		version, err := parseVersionLKGPublicationFilename(filepath.Base(path))
		if err != nil {
			return VersionLKGRestoreResult{}, err
		}
		data, err := readVersionFile(path, maxEnrichmentVersionEnvelopeBytes)
		if err != nil {
			return VersionLKGRestoreResult{}, err
		}
		envelope, err := VerifySignedEnrichmentVersionPublication(data, trust, now)
		if err != nil {
			return VersionLKGRestoreResult{}, err
		}
		if envelope.Publication.ClassificationVersion != version {
			return VersionLKGRestoreResult{}, errors.New("version LKG filename and publication version differ")
		}
		if err := s.RegisterVerifiedPublication(envelope, data); err != nil {
			return VersionLKGRestoreResult{}, err
		}
		if err := loader.Install(ctx, envelope.Publication); err != nil {
			return VersionLKGRestoreResult{}, err
		}
		result.PublicationCount++
		result.HighestVersion = version
	}
	if err := catalog.publishRestored(staged); err != nil {
		return VersionLKGRestoreResult{}, err
	}
	return result, nil
}

func (s *DiskVersionLKG) registerObjectReference(objectRef, checksum string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.references[objectRef]; current != "" && current != checksum {
		return errors.New("enrichment object reference is immutable")
	}
	s.references[objectRef] = checksum
	return nil
}

func (s *DiskVersionLKG) objectPath(checksum string) string {
	return filepath.Join(s.dir, "objects", strings.TrimPrefix(checksum, "sha256:"))
}

func versionLKGPublicationFilename(version uint32) string {
	return fmt.Sprintf("%010d.json", version)
}

func parseVersionLKGPublicationFilename(name string) (uint32, error) {
	if len(name) != versionLKGPublicationNameLength || !strings.HasSuffix(name, ".json") {
		return 0, errors.New("version LKG publication filename is invalid")
	}
	value, err := strconv.ParseUint(strings.TrimSuffix(name, ".json"), 10, 32)
	if err != nil || value == 0 {
		return 0, errors.New("version LKG publication filename is invalid")
	}
	return uint32(value), nil
}

func deploymentLKGFilename(generation uint64) string {
	return fmt.Sprintf("%020d.json", generation)
}

func parseDeploymentLKGFilename(name string) (uint64, error) {
	if len(name) != 25 || !strings.HasSuffix(name, ".json") {
		return 0, errors.New("deployment LKG filename is invalid")
	}
	value, err := strconv.ParseUint(strings.TrimSuffix(name, ".json"), 10, 64)
	if err != nil || value == 0 {
		return 0, errors.New("deployment LKG filename is invalid")
	}
	return value, nil
}

func atomicReplaceVersionFile(path string, data []byte, maxBytes int) error {
	if len(data) == 0 || len(data) > maxBytes {
		return errors.New("version LKG file size is invalid")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".replace-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncVersionDirectory(filepath.Dir(path))
}

func writeImmutableVersionFile(path string, data []byte, maxBytes int) error {
	if len(data) == 0 || len(data) > maxBytes {
		return errors.New("version LKG file size is invalid")
	}
	if existing, err := readVersionFile(path, maxBytes); err == nil {
		if bytes.Equal(existing, data) {
			return nil
		}
		return errors.New("immutable version LKG file already exists with different content")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".publication-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, path); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := readVersionFile(path, maxBytes)
		if readErr != nil || !bytes.Equal(existing, data) {
			return errors.New("immutable version LKG file collision")
		}
	}
	return syncVersionDirectory(filepath.Dir(path))
}

func readVersionFile(path string, maxBytes int) ([]byte, error) {
	if maxBytes < 1 {
		return nil, errors.New("version LKG read limit is invalid")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > int64(maxBytes) {
		return nil, errors.New("version LKG file size is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(maxBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > maxBytes {
		return nil, errors.New("version LKG file size is invalid")
	}
	return data, nil
}

func syncVersionDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func versionObjectDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

type contextVersionReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextVersionReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

type discardVersionAcknowledgement struct{}

func (discardVersionAcknowledgement) Acknowledge(context.Context, EnrichmentVersionAcknowledgement) error {
	return nil
}

var _ VersionObjectSource = (*DiskVersionLKG)(nil)
var _ EnrichmentVersionPersistence = (*DiskVersionLKG)(nil)
