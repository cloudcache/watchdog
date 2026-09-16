package flowworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowtombstone"
)

func TestRemoteVersionSyncRetriesACKWithoutRedownloadingObjects(t *testing.T) {
	signer := newTestVersionSigner(t)
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-http")
	_, envelope := signer.sign(t, publication)

	var mu sync.Mutex
	objectCalls := map[string]int{}
	ackStates := []string{}
	installedAttempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Watchdog-Agent-Token") != "agent-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/v1/flow-workers/worker-a/trust-bundle":
			writeTestTrustBundle(t, w, signer.trustData)
		case "/api/v1/flow-workers/worker-a/enrichment-publications":
			after, _ := strconv.Atoi(r.URL.Query().Get("after_version"))
			if r.URL.Query().Get("limit") != "20" {
				t.Errorf("page limit = %q", r.URL.Query().Get("limit"))
			}
			if after == 0 {
				writeTestVersionPage(t, w, []json.RawMessage{envelope}, 1, false)
			} else {
				writeTestVersionPage(t, w, []json.RawMessage{}, uint32(after), false)
			}
		case "/api/v1/flow-workers/worker-a/enrichment-publications/publication-1/objects/dimension":
			mu.Lock()
			objectCalls["dimension"]++
			mu.Unlock()
			writeTestVersionObject(w, publication.Dimension.Checksum, source.objects[publication.Dimension.ObjectRef])
		case "/api/v1/flow-workers/worker-a/enrichment-publications/publication-1/objects/classification":
			mu.Lock()
			objectCalls["classification"]++
			mu.Unlock()
			writeTestVersionObject(w, publication.Classification.Checksum, source.objects[publication.Classification.ObjectRef])
		case "/api/v1/flow-workers/worker-a/enrichment-publications/publication-1/ack":
			var request versionHTTPAckRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Errorf("decode ACK: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			ackStates = append(ackStates, request.State)
			if request.State == "installed" {
				installedAttempts++
				if installedAttempts == 1 {
					mu.Unlock()
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
			}
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newTestVersionHTTPClient(t, server.URL, server.Client())
	lkg, _ := NewDiskVersionLKG(t.TempDir())
	catalog, _ := NewEnrichmentVersionCatalog()
	syncer, err := NewRemoteVersionSync(client, lkg, &flowplan.TrustStore{}, catalog, VersionLoaderLimits{})
	if err != nil {
		t.Fatal(err)
	}
	syncer.now = func() time.Time { return signer.now }
	if _, err := syncer.SyncOnce(context.Background(), 0); !errors.Is(err, ErrVersionAcknowledgement) {
		t.Fatalf("first sync error = %v", err)
	}
	if _, err := catalog.Select(testMinute(12, 30)); err != nil {
		t.Fatalf("locally durable publication disappeared after ACK failure: %v", err)
	}
	result, err := syncer.SyncOnce(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if result.HighestVersion != 1 || result.Installed != 1 {
		t.Fatalf("retry result = %+v", result)
	}
	mu.Lock()
	defer mu.Unlock()
	if objectCalls["dimension"] != 1 || objectCalls["classification"] != 1 {
		t.Fatalf("object calls = %v", objectCalls)
	}
	wantStates := []string{"downloaded", "installed", "failed", "downloaded", "installed"}
	if fmt.Sprint(ackStates) != fmt.Sprint(wantStates) {
		t.Fatalf("ACK states = %v, want %v", ackStates, wantStates)
	}
}

func TestRemoteVersionSyncRejectsUnverifiedEnvelopeBeforeObjectOrACK(t *testing.T) {
	signer := newTestVersionSigner(t)
	publication, _ := testVersionPublication(t, 1, testMinute(12, 0), "dimension-bad-signature")
	envelopeValue, _ := signer.sign(t, publication)
	envelopeValue.Signature[0] ^= 0xff
	tampered, err := MarshalSignedEnrichmentVersionPublication(envelopeValue)
	if err != nil {
		t.Fatal(err)
	}
	requests := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.Path)
		switch r.URL.Path {
		case "/api/v1/flow-workers/worker-a/trust-bundle":
			writeTestTrustBundle(t, w, signer.trustData)
		case "/api/v1/flow-workers/worker-a/enrichment-publications":
			writeTestVersionPage(t, w, []json.RawMessage{tampered}, 1, false)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	client := newTestVersionHTTPClient(t, server.URL, server.Client())
	lkg, _ := NewDiskVersionLKG(t.TempDir())
	catalog, _ := NewEnrichmentVersionCatalog()
	syncer, _ := NewRemoteVersionSync(client, lkg, &flowplan.TrustStore{}, catalog, VersionLoaderLimits{})
	syncer.now = func() time.Time { return signer.now }
	if _, err := syncer.SyncOnce(context.Background(), 0); !errors.Is(err, ErrVersionRemoteInvalid) {
		t.Fatalf("signature error = %v", err)
	}
	if len(requests) != 2 {
		t.Fatalf("unverified publication triggered object/ACK request: %v", requests)
	}
	if _, err := catalog.Select(testMinute(12, 30)); !errors.Is(err, ErrNoEnrichmentVersion) {
		t.Fatalf("unverified publication became visible: %v", err)
	}
}

func TestRemoteVersionSyncDiscardsInterruptedObjectAndReportsTransportFailure(t *testing.T) {
	signer := newTestVersionSigner(t)
	publication, source := testVersionPublication(t, 1, testMinute(12, 0), "dimension-partial")
	_, envelope := signer.sign(t, publication)
	var failure versionHTTPAckRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/trust-bundle"):
			writeTestTrustBundle(t, w, signer.trustData)
		case strings.HasSuffix(r.URL.Path, "/enrichment-publications"):
			writeTestVersionPage(t, w, []json.RawMessage{envelope}, 1, false)
		case strings.HasSuffix(r.URL.Path, "/objects/dimension"):
			data := source.objects[publication.Dimension.ObjectRef]
			w.Header().Set("X-Watchdog-Object-Checksum", publication.Dimension.Checksum)
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data[:len(data)/2])
		case strings.HasSuffix(r.URL.Path, "/ack"):
			if err := json.NewDecoder(r.Body).Decode(&failure); err != nil {
				t.Errorf("decode failure ACK: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	client := newTestVersionHTTPClient(t, server.URL, server.Client())
	lkg, _ := NewDiskVersionLKG(t.TempDir())
	catalog, _ := NewEnrichmentVersionCatalog()
	syncer, _ := NewRemoteVersionSync(client, lkg, &flowplan.TrustStore{}, catalog, VersionLoaderLimits{})
	syncer.now = func() time.Time { return signer.now }
	if _, err := syncer.SyncOnce(context.Background(), 0); err == nil {
		t.Fatal("interrupted object sync succeeded")
	}
	if failure.State != "failed" || failure.FailureStage != "transport" || failure.FailureCode != "DIMENSION_TRANSFER_FAILED" {
		t.Fatalf("failure ACK = %+v", failure)
	}
	if _, err := os.Stat(lkg.objectPath(publication.Dimension.Checksum)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial object became final: %v", err)
	}
}

func TestVersionHTTPClientRejectsRedirectWithoutForwardingToken(t *testing.T) {
	forwarded := 0
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded++ }))
	defer destination.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client := newTestVersionHTTPClient(t, redirect.URL, redirect.Client())
	if _, err := client.FetchTrustBundle(context.Background()); !errors.Is(err, ErrVersionRemoteUnavailable) {
		t.Fatalf("redirect error = %v", err)
	}
	if forwarded != 0 {
		t.Fatalf("agent token was forwarded across redirect: requests=%d", forwarded)
	}
}

func TestVersionHTTPClientRequiresExactlyOneMachineCredential(t *testing.T) {
	base := VersionHTTPClientConfig{
		BaseURL: "https://watchdog.example", Identity: VersionWorkerIdentity{
			WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3",
		},
	}
	if _, err := NewVersionHTTPClient(base); err == nil {
		t.Fatal("client without token or mTLS was accepted")
	}
	both := base
	both.AgentToken = "secret"
	both.MutualTLS = true
	if _, err := NewVersionHTTPClient(both); err == nil {
		t.Fatal("client with token and mTLS was accepted")
	}
	mtls := base
	mtls.MutualTLS = true
	if _, err := NewVersionHTTPClient(mtls); err != nil {
		t.Fatalf("mTLS-only client: %v", err)
	}
}

func TestVersionHTTPClientFetchesAndAcknowledgesRawDeleteBarrier(t *testing.T) {
	barrier, err := flowtombstone.Advance(nil, time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC), "barrier-http", 7, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	var ack rawDeleteBarrierAckRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Watchdog-Agent-Token") != "agent-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/raw-delete-barrier"):
			_ = json.NewEncoder(w).Encode(barrier)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/raw-delete-barriers/"+barrier.ID+"/ack"):
			if err := json.NewDecoder(r.Body).Decode(&ack); err != nil {
				t.Errorf("decode raw-delete ACK: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client := newTestVersionHTTPClient(t, server.URL, server.Client())
	got, found, err := client.FetchRawDeleteBarrier(context.Background())
	if err != nil || !found || got.ID != barrier.ID || got.Revision != barrier.Revision {
		t.Fatalf("barrier=%+v found=%t err=%v", got, found, err)
	}
	if err := client.AcknowledgeRawDeleteBarrier(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if ack.Revision != barrier.Revision || ack.BootID != "boot-a" || ack.SoftwareVersion != "1.2.3" || ack.State != "installed" {
		t.Fatalf("raw-delete ACK=%+v", ack)
	}
}

func newTestVersionHTTPClient(t testing.TB, baseURL string, transport *http.Client) *VersionHTTPClient {
	t.Helper()
	client, err := NewVersionHTTPClient(VersionHTTPClientConfig{
		BaseURL: baseURL, AgentToken: "agent-secret", Client: transport,
		Identity: VersionWorkerIdentity{WorkerID: "worker-a", BootID: "boot-a", SoftwareVersion: "1.2.3"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func writeTestTrustBundle(t testing.TB, w http.ResponseWriter, data []byte) {
	t.Helper()
	bundle, checksum, err := flowplan.ParseTrustBundle(data)
	if err != nil {
		t.Fatal(err)
	}
	w.Header().Set("X-Watchdog-Trust-Generation", strconv.FormatUint(bundle.Generation, 10))
	w.Header().Set("X-Watchdog-Trust-Checksum", checksum)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func writeTestVersionPage(t testing.TB, w http.ResponseWriter, items []json.RawMessage, next uint32, more bool) {
	t.Helper()
	data, err := json.Marshal(struct {
		Items       []json.RawMessage `json:"items"`
		NextVersion uint32            `json:"next_version"`
		HasMore     bool              `json:"has_more"`
	}{Items: items, NextVersion: next, HasMore: more})
	if err != nil {
		t.Fatal(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func writeTestVersionObject(w http.ResponseWriter, checksum string, data []byte) {
	w.Header().Set("X-Watchdog-Object-Checksum", checksum)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
