package flowworker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
)

type testVersionSigner struct {
	now       time.Time
	keyID     string
	private   ed25519.PrivateKey
	trustData []byte
	trust     *flowplan.TrustStore
}

func newTestVersionSigner(t testing.TB) testVersionSigner {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 7, 14, 0, 0, 0, time.UTC)
	trustData, _, err := flowplan.MarshalTrustBundle(flowplan.TrustBundle{
		SchemaVersion: flowplan.TrustBundleSchemaVersion, Generation: 1,
		IssuedAtUnixMilli: now.Add(-time.Hour).UnixMilli(),
		Keys: []flowplan.TrustBundleKey{{
			KeyID: "lkg-test-key", Algorithm: EnrichmentVersionSignatureAlgorithm,
			PublicKey: encodePublicKey(public), Status: "active",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	trust := &flowplan.TrustStore{}
	if err := trust.Install(trustData); err != nil {
		t.Fatal(err)
	}
	return testVersionSigner{now: now, keyID: "lkg-test-key", private: private, trustData: trustData, trust: trust}
}

func (s testVersionSigner) sign(t testing.TB, publication EnrichmentVersionPublication) (SignedEnrichmentVersionPublication, []byte) {
	t.Helper()
	envelope := SignedEnrichmentVersionPublication{
		SchemaVersion: EnrichmentVersionEnvelopeSchemaVersion, Publication: publication,
		SignatureAlgorithm: EnrichmentVersionSignatureAlgorithm, SigningKeyID: s.keyID,
		SignedAtUnixMilli: s.now.Add(-time.Minute).UnixMilli(),
	}
	payload, err := EnrichmentVersionSigningPayload(envelope)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Signature = ed25519.Sign(s.private, payload)
	data, err := MarshalSignedEnrichmentVersionPublication(envelope)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifySignedEnrichmentVersionPublication(data, s.trust, s.now)
	if err != nil {
		t.Fatal(err)
	}
	return verified, data
}

func (s testVersionSigner) signDeployment(t testing.TB, manifest WorkerDeploymentManifest) (SignedWorkerDeploymentManifest, []byte) {
	t.Helper()
	envelope := SignedWorkerDeploymentManifest{
		SchemaVersion: WorkerDeploymentEnvelopeSchemaVersion, Manifest: manifest,
		SignatureAlgorithm: WorkerDeploymentSignatureAlgorithm, SigningKeyID: s.keyID,
		SignedAtUnixMilli: s.now.Add(-time.Minute).UnixMilli(),
	}
	payload, err := WorkerDeploymentSigningPayload(envelope)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Signature = ed25519.Sign(s.private, payload)
	data, err := MarshalSignedWorkerDeploymentManifest(envelope)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifySignedWorkerDeploymentManifest(data, s.trust, s.now)
	if err != nil {
		t.Fatal(err)
	}
	return verified, data
}

type failingVersionPersistence struct{}

func (failingVersionPersistence) PersistVersion(context.Context, EnrichmentVersionPublication) error {
	return errors.New("disk full")
}

type recordingVersionPersistence struct {
	publications []EnrichmentVersionPublication
}

func (p *recordingVersionPersistence) PersistVersion(_ context.Context, publication EnrichmentVersionPublication) error {
	p.publications = append(p.publications, publication)
	return nil
}

func TestPersistentVersionLoaderDoesNotPublishOrAcknowledgeOnPersistenceFailure(t *testing.T) {
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-persist-failure")
	catalog, _ := NewEnrichmentVersionCatalog()
	acks := &recordingVersionAcknowledger{}
	loader, err := NewPersistentVersionLoader(source, acks, catalog, VersionWorkerIdentity{
		WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{}, failingVersionPersistence{})
	if err != nil {
		t.Fatal(err)
	}
	if err := loader.Install(context.Background(), publication); !errors.Is(err, ErrVersionPersistence) {
		t.Fatalf("persistence error = %v", err)
	}
	if _, err := catalog.Select(publication.ClassificationEffectiveFrom); !errors.Is(err, ErrNoEnrichmentVersion) {
		t.Fatalf("non-durable version became visible: %v", err)
	}
	if len(acks.acks) != 0 {
		t.Fatalf("non-durable version was acknowledged: %+v", acks.acks)
	}
}

func TestPersistentVersionLoaderPersistsInstalledVersionBeforeRetryACK(t *testing.T) {
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-preinstalled")
	catalog, _ := NewEnrichmentVersionCatalog()
	bootstrap, _ := NewVersionLoader(source, &recordingVersionAcknowledger{}, catalog, VersionWorkerIdentity{
		WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{})
	if err := bootstrap.Install(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
	source.calls = nil
	persistence := &recordingVersionPersistence{}
	acks := &recordingVersionAcknowledger{}
	loader, _ := NewPersistentVersionLoader(source, acks, catalog, VersionWorkerIdentity{
		WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{}, persistence)
	if err := loader.Install(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
	if len(persistence.publications) != 1 || len(acks.acks) != 1 || len(source.calls) != 0 {
		t.Fatalf("persist=%d ack=%d fetch=%v", len(persistence.publications), len(acks.acks), source.calls)
	}
}

func TestDiskVersionLKGRoundTripRestoresWithoutRemoteOrDatabase(t *testing.T) {
	directory := t.TempDir()
	signer := newTestVersionSigner(t)
	lkg, err := NewDiskVersionLKG(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := lkg.SaveTrustBundle(context.Background(), signer.trustData); err != nil {
		t.Fatal(err)
	}
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-lkg")
	installSignedTestPublication(t, lkg, signer, publication, source)

	restartedLKG, _ := NewDiskVersionLKG(directory)
	restartedCatalog, _ := NewEnrichmentVersionCatalog()
	result, err := restartedLKG.Restore(context.Background(), &flowplan.TrustStore{}, restartedCatalog, VersionWorkerIdentity{
		WorkerID: "worker-restarted", BootID: "boot-restarted", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{}, signer.now)
	if err != nil {
		t.Fatal(err)
	}
	if result.PublicationCount != 1 || result.HighestVersion != 1 {
		t.Fatalf("restore result = %+v", result)
	}
	version, err := restartedCatalog.Select(testMinute(12, 30))
	if err != nil || version.Metadata().ClassificationChecksum != publication.Classification.Checksum {
		t.Fatalf("restored version=%+v error=%v", version.Metadata(), err)
	}
}

func TestDiskVersionLKGRestorePublishesAllOrNothing(t *testing.T) {
	directory := t.TempDir()
	signer := newTestVersionSigner(t)
	lkg, _ := NewDiskVersionLKG(directory)
	if err := lkg.SaveTrustBundle(context.Background(), signer.trustData); err != nil {
		t.Fatal(err)
	}
	first, firstSource := testVersionPublication(t, 1, testMinute(12, 0), "dimension-lkg-1")
	installSignedTestPublication(t, lkg, signer, first, firstSource)
	second, secondSource := testVersionPublication(t, 2, testMinute(13, 0), "dimension-lkg-2")
	installSignedTestPublication(t, lkg, signer, second, secondSource)
	if err := os.WriteFile(lkg.objectPath(second.Classification.Checksum), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}

	restartedLKG, _ := NewDiskVersionLKG(directory)
	restartedCatalog, _ := NewEnrichmentVersionCatalog()
	if _, err := restartedLKG.Restore(context.Background(), &flowplan.TrustStore{}, restartedCatalog, VersionWorkerIdentity{
		WorkerID: "worker-restarted", BootID: "boot-restarted", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{}, signer.now); err == nil {
		t.Fatal("corrupt LKG restore succeeded")
	}
	if _, err := restartedCatalog.Select(testMinute(12, 30)); !errors.Is(err, ErrNoEnrichmentVersion) {
		t.Fatalf("partially restored catalog became visible: %v", err)
	}
}

func TestDiskVersionLKGRejectsObjectMismatchWithoutPublishingFile(t *testing.T) {
	lkg, _ := NewDiskVersionLKG(t.TempDir())
	checksum := "sha256:" + string(bytes.Repeat([]byte{'a'}, 64))
	if err := lkg.StoreObject(context.Background(), "objects/test", checksum, 64, bytes.NewBufferString("wrong")); err == nil {
		t.Fatal("checksum mismatch succeeded")
	}
	if _, err := os.Stat(lkg.objectPath(checksum)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mismatched final object exists: %v", err)
	}
	temporary, err := filepath.Glob(filepath.Join(lkg.dir, "objects", ".object-*.tmp"))
	if err != nil || len(temporary) != 0 {
		t.Fatalf("temporary objects=%v error=%v", temporary, err)
	}
}

func TestDiskVersionLKGTrustBundleIsMonotonic(t *testing.T) {
	signer := newTestVersionSigner(t)
	lkg, _ := NewDiskVersionLKG(t.TempDir())
	var bundle flowplan.TrustBundle
	parsed, _, err := flowplan.ParseTrustBundle(signer.trustData)
	if err != nil {
		t.Fatal(err)
	}
	bundle = parsed
	bundle.Generation = 2
	bundle.IssuedAtUnixMilli++
	newer, _, err := flowplan.MarshalTrustBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := lkg.SaveTrustBundle(context.Background(), newer); err != nil {
		t.Fatal(err)
	}
	if err := lkg.SaveTrustBundle(context.Background(), signer.trustData); !errors.Is(err, flowplan.ErrTrustBundleRollback) {
		t.Fatalf("rollback error = %v", err)
	}
}

func TestDiskVersionLKGRestoresWorkerDeploymentAtomically(t *testing.T) {
	signer := newTestVersionSigner(t)
	directory := t.TempDir()
	lkg, err := NewDiskVersionLKG(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := lkg.SaveTrustBundle(context.Background(), signer.trustData); err != nil {
		t.Fatal(err)
	}

	manifest, _, source := testWorkerDeployment(t, 1, testMinute(12, 0), nil)
	envelope, data := signer.signDeployment(t, manifest)
	if err := lkg.RegisterVerifiedDeployment(envelope, data); err != nil {
		t.Fatal(err)
	}
	for _, artifact := range manifest.Artifacts {
		object := source.objects[artifact.ArtifactID]
		if err := lkg.StoreObject(context.Background(), artifact.ArtifactID, artifact.Checksum, len(object), bytes.NewReader(object)); err != nil {
			t.Fatal(err)
		}
	}
	if err := lkg.PersistDeployment(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}

	restartedLKG, err := NewDiskVersionLKG(directory)
	if err != nil {
		t.Fatal(err)
	}
	restartedCatalog, err := NewEnrichmentVersionCatalog()
	if err != nil {
		t.Fatal(err)
	}
	result, err := restartedLKG.RestoreDeployments(context.Background(), signer.trust, restartedCatalog, DeploymentLoaderLimits{}, signer.now)
	if err != nil {
		t.Fatal(err)
	}
	if result.DeploymentCount != 1 || result.HighestGeneration != 1 {
		t.Fatalf("restore result = %+v", result)
	}
	installed, ok := restartedCatalog.ClassificationVersion(1)
	if !ok || installed.Dimension.Metadata().Version != 1 || installed.Classification.Metadata().Version != 1 {
		t.Fatalf("restored deployment = %+v, exists=%t", installed, ok)
	}
}

func installSignedTestPublication(t testing.TB, lkg *DiskVersionLKG, signer testVersionSigner, publication EnrichmentVersionPublication, source *memoryVersionObjectSource) {
	t.Helper()
	envelope, data := signer.sign(t, publication)
	if err := lkg.RegisterVerifiedPublication(envelope, data); err != nil {
		t.Fatal(err)
	}
	for _, reference := range []VersionObjectReference{publication.Dimension, publication.Classification} {
		object := source.objects[reference.ObjectRef]
		if err := lkg.StoreObject(context.Background(), reference.ObjectRef, reference.Checksum, len(object), bytes.NewReader(object)); err != nil {
			t.Fatal(err)
		}
	}
	catalog, _ := NewEnrichmentVersionCatalog()
	loader, err := NewPersistentVersionLoader(lkg, &recordingVersionAcknowledger{}, catalog, VersionWorkerIdentity{
		WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
	}, VersionLoaderLimits{}, lkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := loader.Install(context.Background(), publication); err != nil {
		t.Fatal(err)
	}
}
