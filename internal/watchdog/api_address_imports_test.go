package watchdog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeAddressImportAPIRepository struct {
	AddressImportRepository
	created         AddressImport
	activatedImport ID
	expectedVersion uint64
}

func (r *fakeAddressImportAPIRepository) CreateAddressImport(_ context.Context, item AddressImport) (AddressImport, error) {
	r.created = item
	return item, nil
}

func (r *fakeAddressImportAPIRepository) FailAddressImport(context.Context, ID, ID, string, string) error {
	return nil
}

func (r *fakeAddressImportAPIRepository) ActivateAddressImport(_ context.Context, tenantID, importID, actorID ID, expected uint64) (AddressImportSlot, error) {
	r.activatedImport = importID
	r.expectedVersion = expected
	return AddressImportSlot{TenantID: tenantID, SourceSlot: AddressImportSlotGeo, ImportID: importID, RowVersion: expected + 1, ActivatedBy: actorID}, nil
}

type fakeAddressArtifactAPIStore struct {
	removed int
}

func (s *fakeAddressArtifactAPIStore) SaveAddressArtifact(_ context.Context, tenantID, importID ID, _ string, source io.Reader) (AddressArtifact, error) {
	data, err := io.ReadAll(source)
	if err != nil {
		return AddressArtifact{}, err
	}
	sum := sha256.Sum256(data)
	return AddressArtifact{
		Ref:    "address-imports/" + string(tenantID) + "/" + string(importID) + "/source.mmdb",
		Format: AddressImportFormatMMDB, ChecksumSHA256: hex.EncodeToString(sum[:]), SizeBytes: uint64(len(data)),
	}, nil
}

func (*fakeAddressArtifactAPIStore) ResolveAddressArtifact(string) (string, error) { return "", nil }
func (s *fakeAddressArtifactAPIStore) RemoveAddressArtifact(string) error {
	s.removed++
	return nil
}

type fakeAddressImportJobQueue struct {
	OperationJobRepository
	job OperationJob
}

func (q *fakeAddressImportJobQueue) EnqueueOperationJob(_ context.Context, job OperationJob) (OperationJob, error) {
	q.job = job
	job.ID = "job-address-import"
	job.Status = OperationJobStatusQueued
	return job, nil
}

func TestAddressImportUploadPersistsArtifactAndEnqueuesTypedJob(t *testing.T) {
	repo := &fakeAddressImportAPIRepository{}
	artifacts := &fakeAddressArtifactAPIStore{}
	jobs := &fakeAddressImportJobQueue{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: addressImportTestAuth, AddressImports: repo, AddressArtifacts: artifacts,
		AddressImportMaxBytes: 1 << 20, OperationJobs: jobs,
	})
	body, contentType := addressImportMultipart(t, map[string]string{"source_slot": AddressImportSlotGeo, "language": "en"}, "GeoLite2-City.mmdb", []byte("fixture"))
	request := httptest.NewRequest(http.MethodPost, "/api/v1/address-imports", body)
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if repo.created.TenantID != "tenant-address-api" || repo.created.CreatedBy != "user-address-api" || repo.created.Status != AddressImportStatusQueued || repo.created.SizeBytes != 7 {
		t.Fatalf("created import = %#v", repo.created)
	}
	if jobs.job.JobType != AddressImportJobType || jobs.job.CreatedBy != "user-address-api" || len(jobs.job.RequestHash) != 64 {
		t.Fatalf("job = %#v", jobs.job)
	}
	var payload addressImportJobPayload
	if err := DecodeJobPayload(jobs.job.CheckpointJSON, AddressImportJobPayloadVersion, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ImportID != repo.created.ID || payload.Language != "en" || payload.Processed != 0 {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestAddressImportUploadRejectsUnknownFieldAndRemovesArtifact(t *testing.T) {
	repo := &fakeAddressImportAPIRepository{}
	artifacts := &fakeAddressArtifactAPIStore{}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: addressImportTestAuth, AddressImports: repo, AddressArtifacts: artifacts,
		AddressImportMaxBytes: 1 << 20, OperationJobs: &fakeAddressImportJobQueue{},
	})
	// Put the file first so the cleanup path is exercised after the later bad field.
	body, contentType := addressImportMultipart(t, map[string]string{"unexpected": "value", "source_slot": AddressImportSlotGeo}, "Geo.mmdb", []byte("fixture"))
	request := httptest.NewRequest(http.MethodPost, "/api/v1/address-imports", body)
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || artifacts.removed != 1 || repo.created.ID != "" {
		t.Fatalf("status=%d removed=%d created=%#v body=%s", response.Code, artifacts.removed, repo.created, response.Body.String())
	}
}

func TestAddressImportActivateUsesSlotETag(t *testing.T) {
	repo := &fakeAddressImportAPIRepository{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressImportTestAuth, AddressImports: repo})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/address-imports/import-a/actions/activate", nil)
	request.Header.Set("If-Match", `"7"`)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("ETag") != `"8"` || repo.activatedImport != "import-a" || repo.expectedVersion != 7 {
		t.Fatalf("status=%d etag=%q import=%q expected=%d body=%s", response.Code, response.Header().Get("ETag"), repo.activatedImport, repo.expectedVersion, response.Body.String())
	}
}

func TestAddressImportUploadJobAndActivationMySQL(t *testing.T) {
	dsn := os.Getenv("WATCHDOG_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("set WATCHDOG_MYSQL_TEST_DSN to run address import end-to-end test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := ApplyMySQLMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	const tenantID = ID("tenant_addr_api_e2e")
	const actorID = ID("user_addr_api_e2e")
	cleanupAddressImportFixture(t, db, tenantID)
	defer cleanupAddressImportFixture(t, db, tenantID)
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants (id, name, status) VALUES (?, 'Address API E2E', 'active')`, tenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO users (id, tenant_id, email, name, status, auth_provider, external_subject_id)
		VALUES (?, ?, 'address-api-e2e@test.invalid', 'Address API E2E', 'active', 'test', 'address-api-e2e')
	`, actorID, tenantID); err != nil {
		t.Fatal(err)
	}
	store := NewMySQLStore(db)
	artifacts := DiskAddressArtifactStore{Dir: t.TempDir(), MaxBytes: 16 << 20}
	auth := func(*http.Request) (AuthContext, error) {
		return AuthContext{TenantID: tenantID, UserID: actorID, IsAdmin: true}, nil
	}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: auth, AddressImports: store, AddressArtifacts: artifacts,
		AddressImportMaxBytes: 16 << 20, OperationJobs: store,
	})
	fixturePath := filepath.Join("..", "..", "akvorado", "orchestrator", "geoip", "testdata", "GeoLite2-City-Test.mmdb")
	fixture, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	body, contentType := addressImportMultipart(t, map[string]string{"source_slot": AddressImportSlotGeo}, "GeoLite2-City-Test.mmdb", fixture)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/address-imports", body)
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("upload status=%d body=%s", response.Code, response.Body.String())
	}
	var upload struct {
		Import AddressImport `json:"import"`
		Job    OperationJob  `json:"job"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &upload); err != nil {
		t.Fatal(err)
	}
	leased, err := store.LeaseNextOperationJob(ctx, AddressImportJobType, "address-e2e-worker", 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	worker := OperationJobWorker{
		Repo: store, JobType: AddressImportJobType, Owner: "address-e2e-worker",
		Handler: NewAddressImportJobHandler(store, artifacts, 100), LeaseFor: 2 * time.Minute,
	}
	worker.runAttempt(ctx, leased)
	job, err := store.GetOperationJob(ctx, tenantID, upload.Job.ID)
	if err != nil || job.Status != OperationJobStatusSucceeded || job.ProgressDone == 0 {
		t.Fatalf("job=%#v err=%v", job, err)
	}
	item, err := store.GetAddressImport(ctx, tenantID, upload.Import.ID)
	if err != nil || item.Status != AddressImportStatusReady || item.RowCountV4+item.RowCountV6 == 0 {
		t.Fatalf("import=%#v err=%v", item, err)
	}
	activate := httptest.NewRequest(http.MethodPost, "/api/v1/address-imports/"+string(item.ID)+"/actions/activate", nil)
	activateResponse := httptest.NewRecorder()
	router.ServeHTTP(activateResponse, activate)
	if activateResponse.Code != http.StatusOK || activateResponse.Header().Get("ETag") != `"1"` {
		t.Fatalf("activate status=%d etag=%q body=%s", activateResponse.Code, activateResponse.Header().Get("ETag"), activateResponse.Body.String())
	}
	slot, err := store.GetAddressImportSlot(ctx, tenantID, AddressImportSlotGeo)
	if err != nil || slot.ImportID != item.ID || slot.RowVersion != 1 {
		t.Fatalf("slot=%#v err=%v", slot, err)
	}
}

func addressImportMultipart(t *testing.T, fields map[string]string, filename string, data []byte) (*bytes.Buffer, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, err := writer.CreateFormFile("file", filename)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(data); err != nil {
		t.Fatal(err)
	}
	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return &body, writer.FormDataContentType()
}

func addressImportTestAuth(*http.Request) (AuthContext, error) {
	return AuthContext{TenantID: "tenant-address-api", UserID: "user-address-api", IsAdmin: true}, nil
}
