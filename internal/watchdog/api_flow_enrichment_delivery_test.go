package watchdog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

type flowEnrichmentDeliveryControllerStub struct {
	page          FlowEnrichmentDeliveryPage
	object        FlowEnrichmentObjectDelivery
	workerID      ID
	publicationID ID
	kind          string
	credential    CollectorMachineCredential
	report        FlowEnrichmentAcknowledgementReport
	afterVersion  uint32
	limit         int
	fetchCalls    int
	objectCalls   int
	ackCalls      int
	err           error
}

func (c *flowEnrichmentDeliveryControllerStub) FetchDesired(_ context.Context, workerID ID, credential CollectorMachineCredential, afterVersion uint32, limit int) (FlowEnrichmentDeliveryPage, error) {
	c.workerID, c.credential, c.afterVersion, c.limit = workerID, credential, afterVersion, limit
	c.fetchCalls++
	return c.page, c.err
}

func (c *flowEnrichmentDeliveryControllerStub) ResolveObject(_ context.Context, workerID ID, credential CollectorMachineCredential, publicationID ID, kind string) (FlowEnrichmentObjectDelivery, error) {
	c.workerID, c.credential, c.publicationID, c.kind = workerID, credential, publicationID, kind
	c.objectCalls++
	return c.object, c.err
}

func (c *flowEnrichmentDeliveryControllerStub) Acknowledge(_ context.Context, workerID ID, credential CollectorMachineCredential, publicationID ID, report FlowEnrichmentAcknowledgementReport) error {
	c.workerID, c.credential, c.publicationID, c.report = workerID, credential, publicationID, report
	c.ackCalls++
	return c.err
}

func TestFlowEnrichmentDeliveryAPIUsesStrictMachineRoutes(t *testing.T) {
	controller := &flowEnrichmentDeliveryControllerStub{page: FlowEnrichmentDeliveryPage{
		Items: []FlowEnrichmentDeliveryItem{{ClassificationVersion: 7, Envelope: []byte(`{"schema_version":1}`)}}, NextVersion: 7, HasMore: true,
	}}
	router := NewAPIV1Router(APIV1RouterConfig{FlowEnrichmentDelivery: controller})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flow-workers/worker-a/enrichment-publications?after_version=3&limit=4", nil)
	req.Header.Set("Authorization", "Bearer secret")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK || controller.fetchCalls != 1 || controller.workerID != "worker-a" || controller.afterVersion != 3 || controller.limit != 4 ||
		controller.credential.Token != "secret" || !strings.Contains(recorder.Body.String(), `"items":[{"schema_version":1}]`) || recorder.Header().Get("X-Watchdog-Next-Classification-Version") != "7" {
		t.Fatalf("status=%d controller=%+v headers=%v body=%s", recorder.Code, controller, recorder.Header(), recorder.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/flow-workers/worker-a/enrichment-publications?tenant_id=forbidden", nil)
	req.Header.Set("X-Watchdog-Agent-Token", "secret")
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadRequest || controller.fetchCalls != 1 {
		t.Fatalf("injected scope status=%d calls=%d", recorder.Code, controller.fetchCalls)
	}

	body := `{"state":"installed","boot_id":"boot-a","software_version":"1.2.3","dimension_snapshot_id":"snapshot-a","dimension_version":4,"dimension_checksum":"sha256:` + strings.Repeat("a", 64) + `","classification_version":7,"classification_checksum":"sha256:` + strings.Repeat("b", 64) + `"}`
	req = httptest.NewRequest(http.MethodPost, "/api/v1/flow-workers/worker-a/enrichment-publications/publication-a/ack", strings.NewReader(body))
	req.Header.Set("X-Watchdog-Agent-Token", "secret")
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusAccepted || controller.ackCalls != 1 || controller.publicationID != "publication-a" || controller.report.State != FlowEnrichmentAckInstalled {
		t.Fatalf("ack status=%d controller=%+v body=%s", recorder.Code, controller, recorder.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/flow-workers/worker-a/enrichment-publications/publication-a/ack", strings.NewReader(strings.TrimSuffix(body, "}")+`,"tenant_id":"forbidden"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "secret")
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadRequest || controller.ackCalls != 1 {
		t.Fatalf("ack scope injection status=%d calls=%d", recorder.Code, controller.ackCalls)
	}
}

func TestFlowEnrichmentDeliveryAPIStreamsOnlyResolvedObject(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "object-*.wads")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("WADS-payload"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	controller := &flowEnrichmentDeliveryControllerStub{object: FlowEnrichmentObjectDelivery{
		Path: file.Name(), Name: "address-snapshot.wads", ContentType: "application/octet-stream",
		Checksum: "sha256:" + strings.Repeat("c", 64), Size: int64(len("WADS-payload")),
	}}
	router := NewAPIV1Router(APIV1RouterConfig{FlowEnrichmentDelivery: controller})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/flow-workers/worker-a/enrichment-publications/publication-a/objects/dimension", nil)
	req.Header.Set("X-Watchdog-Agent-Token", "secret")
	req.Header.Set("Range", "bytes=0-3")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusPartialContent || recorder.Body.String() != "WADS" || controller.objectCalls != 1 || controller.kind != "dimension" ||
		recorder.Header().Get("X-Watchdog-Object-Checksum") != controller.object.Checksum {
		t.Fatalf("status=%d controller=%+v headers=%v body=%q", recorder.Code, controller, recorder.Header(), recorder.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/flow-workers/worker-a/enrichment-publications/publication-a/objects/dimension", nil)
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized || recorder.Header().Get("WWW-Authenticate") != "Watchdog-Flow-Worker" || controller.objectCalls != 1 {
		t.Fatalf("unauthorized status=%d headers=%v calls=%d", recorder.Code, recorder.Header(), controller.objectCalls)
	}
}
