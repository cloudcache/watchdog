package watchdog

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cloudcache/watchdog/internal/flowcollect"
)

type collectorEvidenceAPIController struct {
	collector    ID
	credential   CollectorMachineCredential
	drain        CollectorDrainReport
	restore      CollectorStateRestoreReport
	drainCalls   int
	restoreCalls int
	err          error
}

func (c *collectorEvidenceAPIController) RecordDrain(_ context.Context, collectorID ID, credential CollectorMachineCredential, report CollectorDrainReport) error {
	c.collector, c.credential, c.drain = collectorID, credential, report
	c.drainCalls++
	return c.err
}

func (c *collectorEvidenceAPIController) RecordStateRestore(_ context.Context, collectorID ID, credential CollectorMachineCredential, report CollectorStateRestoreReport) error {
	c.collector, c.credential, c.restore = collectorID, credential, report
	c.restoreCalls++
	return c.err
}

func TestCollectorEvidenceAPIAcceptsTokenDrainWithoutIdentityFields(t *testing.T) {
	controller := &collectorEvidenceAPIController{}
	router := NewAPIV1Router(APIV1RouterConfig{CollectorEvidence: controller})
	path := "/api/v1/collectors/collector-a/ownership-transfers/transfer-a/actions/drain"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"applied_config_version":12,"receipt_nonce":"drain-1"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "secret-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || controller.drainCalls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, controller.drainCalls, rec.Body.String())
	}
	if controller.collector != "collector-a" || controller.credential.Token != "secret-token" || controller.drain.TransferID != "transfer-a" || controller.drain.AppliedConfigVersion != 12 {
		t.Fatalf("collector=%s credential=%+v report=%+v", controller.collector, controller.credential, controller.drain)
	}

	req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"tenant_id":"tenant-a","applied_config_version":12,"receipt_nonce":"drain-1"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "secret-token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || controller.drainCalls != 1 {
		t.Fatalf("identity injection status=%d calls=%d body=%s", rec.Code, controller.drainCalls, rec.Body.String())
	}
}

func TestCollectorEvidenceAPIAcceptsOnlyVerifiedClientCertificate(t *testing.T) {
	controller := &collectorEvidenceAPIController{}
	router := NewAPIV1Router(APIV1RouterConfig{CollectorEvidence: controller})
	identity := sha256.Sum256([]byte("restore-identity"))
	body := `{"state_kind":"decoder","state_identity_key":"` + hex.EncodeToString(identity[:]) + `","applied_config_version":13,"restored_old_ownership_epoch":5,"restored_old_generation":9,"new_epoch_baseline_generation":2,"receipt_nonce":"restore-1"}`
	path := "/api/v1/collectors/collector-b/ownership-transfers/transfer-a/state-restores"
	certificate := &x509.Certificate{Raw: []byte("verified-client-certificate")}
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificate},
		VerifiedChains:   [][]*x509.Certificate{{certificate}},
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	digest := sha256.Sum256(certificate.Raw)
	wantFingerprint := "sha256:" + hex.EncodeToString(digest[:])
	if rec.Code != http.StatusAccepted || controller.restoreCalls != 1 || controller.credential.CertificateFingerprint != wantFingerprint || controller.restore.Kind != flowcollect.StateCheckpointDecoder || controller.restore.TransferID != "transfer-a" {
		t.Fatalf("status=%d calls=%d credential=%+v report=%+v body=%s", rec.Code, controller.restoreCalls, controller.credential, controller.restore, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}}
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") != "Watchdog-Collector" || controller.restoreCalls != 1 {
		t.Fatalf("unverified status=%d calls=%d body=%s", rec.Code, controller.restoreCalls, rec.Body.String())
	}
}

func TestCollectorEvidenceAPIRejectsMixedCredentials(t *testing.T) {
	controller := &collectorEvidenceAPIController{}
	router := NewAPIV1Router(APIV1RouterConfig{CollectorEvidence: controller})
	certificate := &x509.Certificate{Raw: []byte("verified-client-certificate")}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/ownership-transfers/transfer-a/actions/drain", strings.NewReader(`{"applied_config_version":12,"receipt_nonce":"drain-1"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "secret-token")
	req.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificate},
		VerifiedChains:   [][]*x509.Certificate{{certificate}},
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || controller.drainCalls != 0 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, controller.drainCalls, rec.Body.String())
	}
}

func TestCollectorEvidenceAPIRejectsAmbiguousTokenHeaders(t *testing.T) {
	controller := &collectorEvidenceAPIController{}
	router := NewAPIV1Router(APIV1RouterConfig{CollectorEvidence: controller})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/collectors/collector-a/ownership-transfers/transfer-a/actions/drain", strings.NewReader(`{"applied_config_version":12,"receipt_nonce":"drain-1"}`))
	req.Header.Set("X-Watchdog-Agent-Token", "direct-token")
	req.Header.Set("Authorization", "Bearer bearer-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || controller.drainCalls != 0 {
		t.Fatalf("status=%d calls=%d body=%s", rec.Code, controller.drainCalls, rec.Body.String())
	}
}
