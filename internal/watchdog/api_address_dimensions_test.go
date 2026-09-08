package watchdog

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeAddressDimensionJobQueue struct {
	OperationJobRepository
	job OperationJob
}

type fakeAddressDimensionLifecycleAPI struct {
	AddressDimensionPublisher
	AddressDimensionLifecycle
	snapshot        AddressDimensionSnapshot
	approved        bool
	rejected        bool
	activated       bool
	rolledBack      bool
	retired         bool
	approveCalls    int
	expectedVersion uint64
}

func (api *fakeAddressDimensionLifecycleAPI) GetAddressDimensionSnapshot(_ context.Context, tenantID, snapshotID ID) (AddressDimensionSnapshot, error) {
	if tenantID != api.snapshot.TenantID || snapshotID != api.snapshot.ID {
		return AddressDimensionSnapshot{}, fmt.Errorf("unexpected snapshot lookup")
	}
	return api.snapshot, nil
}

func (api *fakeAddressDimensionLifecycleAPI) ApproveAddressDimension(_ context.Context, tenantID, actorID ID, expected uint64, approval AddressDimensionApproval) (AddressDimensionSnapshot, error) {
	api.approveCalls++
	api.approved, api.expectedVersion = true, expected
	result := api.snapshot
	result.RowVersion = expected + 1
	result.ApprovalState = AddressDimensionApprovalApproved
	return result, nil
}

func (api *fakeAddressDimensionLifecycleAPI) RejectAddressDimension(_ context.Context, tenantID, actorID, snapshotID ID, expected uint64, reason string) (AddressDimensionSnapshot, error) {
	api.rejected, api.expectedVersion = reason == "bad data" && snapshotID == api.snapshot.ID, expected
	result := api.snapshot
	result.RowVersion = expected + 1
	return result, nil
}

func (api *fakeAddressDimensionLifecycleAPI) ActivateAddressDimension(_ context.Context, tenantID, actorID ID, request AddressDimensionActivationRequest) (AddressDimensionActivation, error) {
	api.activated, api.expectedVersion = request.SnapshotID == api.snapshot.ID, request.ExpectedRowVersion
	return AddressDimensionActivation{ID: "activation-a", SnapshotID: request.SnapshotID, EffectiveFrom: request.EffectiveFrom}, nil
}

func (api *fakeAddressDimensionLifecycleAPI) RollbackAddressDimension(_ context.Context, tenantID, actorID ID, request AddressDimensionRollbackRequest) (AddressDimensionActivation, error) {
	api.rolledBack, api.expectedVersion = request.SnapshotID == api.snapshot.ID, request.ExpectedRowVersion
	return AddressDimensionActivation{ID: "activation-b", SnapshotID: request.SnapshotID, EffectiveFrom: request.EffectiveFrom}, nil
}

func (api *fakeAddressDimensionLifecycleAPI) RetireAddressDimension(_ context.Context, tenantID, actorID ID, request AddressDimensionRetireRequest) (AddressDimensionSnapshot, error) {
	api.retired, api.expectedVersion = request.Reason == "superseded" && request.SnapshotID == api.snapshot.ID, request.ExpectedRowVersion
	result := api.snapshot
	result.RowVersion = request.ExpectedRowVersion + 1
	return result, nil
}

func (q *fakeAddressDimensionJobQueue) EnqueueOperationJob(_ context.Context, job OperationJob) (OperationJob, error) {
	q.job = job
	job.ID = "job-address-dimension"
	return job, nil
}

func TestAddressDimensionPreviewAndAsyncPublishAPI(t *testing.T) {
	effective := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	digest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	publisher := &fakeAddressDimensionPublisher{preview: AddressDimensionPreview{DraftDigest: digest, EffectiveFrom: effective, PrefixCount: 2}}
	jobs := &fakeAddressDimensionJobQueue{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressDimensionTestAuth, AddressDimensions: publisher, OperationJobs: jobs})

	preview := httptest.NewRecorder()
	router.ServeHTTP(preview, httptest.NewRequest(http.MethodPost, "/api/v1/dimensions/address/preview", strings.NewReader(`{"effective_from":"2026-09-07T00:00:00Z"}`)))
	if preview.Code != http.StatusOK || !strings.Contains(preview.Body.String(), digest) {
		t.Fatalf("preview = %d %s", preview.Code, preview.Body.String())
	}

	publish := httptest.NewRecorder()
	router.ServeHTTP(publish, httptest.NewRequest(http.MethodPost, "/api/v1/dimensions/address/publish", strings.NewReader(`{"effective_from":"2026-09-07T00:00:00Z","preview_digest":"`+digest+`"}`)))
	if publish.Code != http.StatusAccepted {
		t.Fatalf("publish = %d %s", publish.Code, publish.Body.String())
	}
	if jobs.job.JobType != AddressSnapshotBuildJob || jobs.job.TenantID != "tenant-dimension" || jobs.job.CreatedBy != "user-dimension" {
		t.Fatalf("unexpected job: %#v", jobs.job)
	}
	if !strings.HasSuffix(jobs.job.IdempotencyKey, strings.TrimPrefix(digest, "sha256:")) {
		t.Fatalf("idempotency key does not distinguish the draft digest: %q", jobs.job.IdempotencyKey)
	}
	var payload addressDimensionPublishJobPayload
	if err := DecodeJobPayload(jobs.job.CheckpointJSON, AddressDimensionPayloadV1, &payload); err != nil || payload.PreviewDigest != digest {
		t.Fatalf("job payload = %#v, %v", payload, err)
	}
	var body map[string]any
	if err := json.Unmarshal(publish.Body.Bytes(), &body); err != nil || body["job"] == nil {
		t.Fatalf("publish response = %s, %v", publish.Body.String(), err)
	}
}

func TestAddressDimensionRoutesUseSharedScopeAndGlobalAdminWrites(t *testing.T) {
	effective := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	digest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	publisher := &fakeAddressDimensionPublisher{preview: AddressDimensionPreview{DraftDigest: digest, EffectiveFrom: effective}}
	consumerRouter := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-consumer", UserID: "user-consumer", IsAdmin: true}, nil
		},
		AddressLibraryOwner: "tenant-owner", AddressDimensions: publisher,
	})
	response := httptest.NewRecorder()
	consumerRouter.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/dimensions/address/versions", nil))
	if response.Code != http.StatusOK || publisher.tenantID != "tenant-owner" {
		t.Fatalf("shared owner read = %d tenant=%q body=%s", response.Code, publisher.tenantID, response.Body.String())
	}
	publisher.tenantID = ""
	response = httptest.NewRecorder()
	consumerRouter.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/dimensions/address/preview", strings.NewReader(`{"effective_from":"2026-09-07T00:00:00Z"}`)))
	if response.Code != http.StatusOK || publisher.tenantID != "tenant-owner" {
		t.Fatalf("global admin write = %d tenant=%q body=%s", response.Code, publisher.tenantID, response.Body.String())
	}

	memberRouter := NewAPIV1Router(APIV1RouterConfig{
		Auth: func(*http.Request) (AuthContext, error) {
			return AuthContext{TenantID: "tenant-owner", UserID: "user-member"}, nil
		},
		AddressLibraryOwner: "tenant-owner", AddressDimensions: publisher,
	})
	publisher.tenantID = ""
	response = httptest.NewRecorder()
	memberRouter.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/dimensions/address/preview", strings.NewReader(`{"effective_from":"2026-09-07T00:00:00Z"}`)))
	if response.Code != http.StatusForbidden || publisher.tenantID != "" {
		t.Fatalf("non-admin write = %d tenant=%q body=%s", response.Code, publisher.tenantID, response.Body.String())
	}
}

func TestAddressDimensionLifecycleAPIRequiresVerifiedTenantKeyAndIfMatch(t *testing.T) {
	effective := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	snapshot := AddressDimensionSnapshot{
		ID: "snapshot-a", TenantID: "tenant-dimension", ModuleKey: AddressDimensionModuleKey, DimensionKey: AddressDimensionKey,
		Version: 1, EffectiveFrom: effective, ObjectRef: "dimension://sha256/object",
		Checksum:            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		DraftDigest:         "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		BundleSchemaVersion: 1, Status: AddressDimensionStatusActive, ApprovalState: AddressDimensionApprovalPending, RowVersion: 7,
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewStaticAddressDimensionPublicKeyResolver([]AddressDimensionTrustedPublicKey{{TenantID: snapshot.TenantID, KeyID: "publisher-2026", Key: publicKey}})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := &fakeAddressDimensionLifecycleAPI{snapshot: snapshot}
	router := NewAPIV1Router(APIV1RouterConfig{
		Auth: addressDimensionTestAuth, AddressDimensions: lifecycle,
		DimensionLifecycle: lifecycle, DimensionKeys: resolver,
	})
	signedAt := effective.Add(-time.Minute)
	payload, err := AddressDimensionSigningPayload(snapshot, "publisher-2026", signedAt)
	if err != nil {
		t.Fatal(err)
	}
	approveBody, err := json.Marshal(map[string]any{
		"signing_key_id": "publisher-2026", "signed_at": signedAt,
		"signature": base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload)),
	})
	if err != nil {
		t.Fatal(err)
	}
	call := func(path, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		request.Header.Set("If-Match", `"7"`)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		return response
	}
	base := "/api/v1/dimensions/address/versions/snapshot-a/actions/"
	approved := call(base+"approve", string(approveBody))
	if approved.Code != http.StatusOK || approved.Header().Get("ETag") != `"8"` || !lifecycle.approved || lifecycle.expectedVersion != 7 {
		t.Fatalf("approve = %d %s %#v", approved.Code, approved.Body.String(), lifecycle)
	}

	badSignature := ed25519.Sign(privateKey, payload)
	badSignature[0] ^= 0xff
	invalidBody, err := json.Marshal(map[string]any{
		"signing_key_id": "publisher-2026", "signed_at": signedAt,
		"signature": base64.StdEncoding.EncodeToString(badSignature),
	})
	if err != nil {
		t.Fatal(err)
	}
	invalid := call(base+"approve", string(invalidBody))
	if invalid.Code != http.StatusBadRequest || lifecycle.approveCalls != 1 {
		t.Fatalf("invalid signature = %d %s", invalid.Code, invalid.Body.String())
	}

	for _, test := range []struct {
		name string
		body string
		seen *bool
	}{
		{name: "reject", body: `{"reason":"bad data"}`, seen: &lifecycle.rejected},
		{name: "activate", body: `{"effective_from":"2026-09-07T00:00:00Z"}`, seen: &lifecycle.activated},
		{name: "rollback", body: `{"effective_from":"2026-09-07T00:01:00Z"}`, seen: &lifecycle.rolledBack},
		{name: "retire", body: `{"reason":"superseded"}`, seen: &lifecycle.retired},
	} {
		response := call(base+test.name, test.body)
		if response.Code != http.StatusOK || !*test.seen || lifecycle.expectedVersion != 7 {
			t.Fatalf("%s = %d %s %#v", test.name, response.Code, response.Body.String(), lifecycle)
		}
	}

	missingPrecondition := httptest.NewRecorder()
	router.ServeHTTP(missingPrecondition, httptest.NewRequest(http.MethodPost, base+"retire", strings.NewReader(`{"reason":"superseded"}`)))
	if missingPrecondition.Code != http.StatusPreconditionRequired {
		t.Fatalf("missing If-Match = %d %s", missingPrecondition.Code, missingPrecondition.Body.String())
	}
}

func addressDimensionTestAuth(*http.Request) (AuthContext, error) {
	return AuthContext{TenantID: "tenant-dimension", UserID: "user-dimension", IsAdmin: true}, nil
}

func TestAddressDimensionVersionListServerTable(t *testing.T) {
	publisher := &fakeAddressDimensionPublisher{}
	router := NewAPIV1Router(APIV1RouterConfig{Auth: addressDimensionTestAuth, AddressDimensions: publisher})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/dimensions/address/versions?q=sha256&status=active&sort=prefixes&order=desc&limit=25&offset=50", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if filter := publisher.filter; !filter.TableMode || filter.Search != "sha256" || filter.Status != AddressDimensionStatusActive || filter.Sort != "prefixes" || !filter.Desc || filter.Limit != 25 || filter.Offset != 50 {
		t.Fatalf("filter=%#v", filter)
	}
	var page struct {
		Items  []AddressDimensionSnapshot `json:"items"`
		Total  int                        `json:"total"`
		Limit  int                        `json:"limit"`
		Offset int                        `json:"offset"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil || len(page.Items) != 1 || page.Total != 3 || page.Limit != 25 || page.Offset != 50 {
		t.Fatalf("page=%#v err=%v", page, err)
	}
}
