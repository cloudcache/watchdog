package watchdog

import (
	"bytes"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

const collectorEvidenceRequestMaxBytes = 8 << 10

type collectorDrainAPIRequest struct {
	AppliedConfigVersion uint64 `json:"applied_config_version"`
	ReceiptNonce         string `json:"receipt_nonce"`
}

type collectorEvidenceAPI struct {
	controller CollectorEvidenceController
}

func registerCollectorEvidenceRoutes(mux *http.ServeMux, controller CollectorEvidenceController) {
	api := collectorEvidenceAPI{controller: controller}
	mux.HandleFunc("POST /api/v1/collectors/{collector_id}/ownership-transfers/{transfer_id}/actions/drain", api.recordDrain)
}

func (api collectorEvidenceAPI) recordDrain(w http.ResponseWriter, r *http.Request) {
	if !validCollectorEvidenceID(ID(r.PathValue("collector_id"))) || !validCollectorEvidenceID(ID(r.PathValue("transfer_id"))) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "collector_id and transfer_id are required", nil)
		return
	}
	credential, err := collectorMachineCredentialFromRequest(r)
	if err != nil {
		writeCollectorEvidenceError(w, err)
		return
	}
	var req collectorDrainAPIRequest
	if err := decodeCollectorEvidenceJSON(r, &req); err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
		return
	}
	if req.AppliedConfigVersion == 0 || !validCollectorReceiptNonce(req.ReceiptNonce) {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "applied_config_version and a printable receipt_nonce are required", nil)
		return
	}
	err = api.controller.RecordDrain(r.Context(), ID(r.PathValue("collector_id")), credential, CollectorDrainReport{
		TransferID: ID(r.PathValue("transfer_id")), AppliedConfigVersion: req.AppliedConfigVersion,
		ReceiptNonce: req.ReceiptNonce,
	})
	if err != nil {
		writeCollectorEvidenceError(w, err)
		return
	}
	WriteAPIJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func collectorMachineCredentialFromRequest(r *http.Request) (CollectorMachineCredential, error) {
	token, err := collectorTokenFromRequest(r)
	if err != nil {
		return CollectorMachineCredential{}, err
	}
	credential := CollectorMachineCredential{Token: token}
	if certificate := verifiedCollectorClientCertificate(r); certificate != nil {
		digest := sha256.Sum256(certificate.Raw)
		credential.CertificateFingerprint = "sha256:" + hex.EncodeToString(digest[:])
	}
	if !validCollectorMachineCredential(credential) {
		return CollectorMachineCredential{}, ErrCollectorMachineUnauthorized
	}
	return credential, nil
}

func collectorTokenFromRequest(r *http.Request) (string, error) {
	if r == nil || len(r.Header.Values("X-Watchdog-Agent-Token")) > 1 || len(r.Header.Values("Authorization")) > 1 {
		return "", ErrCollectorMachineUnauthorized
	}
	direct := r.Header.Get("X-Watchdog-Agent-Token")
	authorization := r.Header.Get("Authorization")
	bearer := ""
	if authorization != "" {
		const prefix = "Bearer "
		if len(authorization) <= len(prefix) || !strings.EqualFold(authorization[:len(prefix)], prefix) {
			return "", ErrCollectorMachineUnauthorized
		}
		bearer = authorization[len(prefix):]
	}
	if direct != "" && bearer != "" {
		return "", ErrCollectorMachineUnauthorized
	}
	if direct != "" {
		return direct, nil
	}
	return bearer, nil
}

func verifiedCollectorClientCertificate(r *http.Request) *x509.Certificate {
	if r == nil || r.TLS == nil || len(r.TLS.PeerCertificates) == 0 || len(r.TLS.VerifiedChains) == 0 {
		return nil
	}
	leaf := r.TLS.PeerCertificates[0]
	for _, chain := range r.TLS.VerifiedChains {
		if len(chain) > 0 && chain[0].Equal(leaf) {
			return leaf
		}
	}
	return nil
}

func decodeCollectorEvidenceJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(r.Body, collectorEvidenceRequestMaxBytes+1))
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > collectorEvidenceRequestMaxBytes {
		return errors.New("request body must be between 1 byte and 8 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("request body must contain one JSON object")
		}
		return err
	}
	return nil
}

func writeCollectorEvidenceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrCollectorMachineUnauthorized):
		w.Header().Set("WWW-Authenticate", "Watchdog-Collector")
		WriteAPIError(w, http.StatusUnauthorized, APIErrorUnauthorized, "Collector authentication failed", nil)
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Collector ownership transfer not found", nil)
	case errors.Is(err, ErrCollectorEvidenceNotReady):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Collector evidence prerequisites are not ready", nil)
	case errors.Is(err, ErrCollectorEvidenceConflict):
		WriteAPIError(w, http.StatusConflict, APIErrorInvalidRequest, "Collector evidence conflicts with the ownership transfer", nil)
	default:
		WriteAPIError(w, http.StatusServiceUnavailable, APIErrorServiceUnavailable, "Collector evidence service is unavailable", nil)
	}
}
