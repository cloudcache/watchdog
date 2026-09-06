package watchdog

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

type addressDraftRevisionAPI struct {
	repo AddressSetRepository
}

func registerAddressDraftRevisionRoutes(mux *http.ServeMux, auth func(http.Handler) http.Handler, repo AddressSetRepository) {
	api := addressDraftRevisionAPI{repo: repo}
	view := RequirePermission(ActionView, TenantResource)
	configure := RequirePermission(ActionConfigure, TenantResource)
	operate := RequirePermission(ActionOperate, TenantResource)
	mux.Handle("GET /api/v1/address-draft-revisions", auth(view(http.HandlerFunc(api.list))))
	mux.Handle("POST /api/v1/address-draft-revisions/preview", auth(configure(http.HandlerFunc(api.preview))))
	mux.Handle("GET /api/v1/address-draft-revisions/{revision_id}", auth(view(http.HandlerFunc(api.get))))
	mux.Handle("POST /api/v1/address-draft-revisions/{revision_id}/apply", auth(operate(http.HandlerFunc(api.apply))))
}

func (api addressDraftRevisionAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	filter := AddressDraftRevisionListFilter{
		Status: strings.TrimSpace(r.URL.Query().Get("status")),
		Cursor: strings.TrimSpace(r.URL.Query().Get("cursor")),
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be a positive integer", nil)
			return
		}
		filter.Limit = limit
	}
	items, cursor, err := api.repo.ListAddressDraftRevisions(r.Context(), auth.TenantID, filter)
	if err != nil {
		writeAddressDraftRevisionError(w, err)
		return
	}
	if items == nil {
		items = []AddressDraftRevision{}
	}
	response := map[string]any{"items": items}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

func (api addressDraftRevisionAPI) preview(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	var input struct {
		Operations []AddressPrefixBatchOperation `json:"operations"`
	}
	if !decodeAddressTaxonomyInput(w, r, &input) {
		return
	}
	revision, err := api.repo.PrepareAddressPrefixRevision(r.Context(), auth.TenantID, auth.UserID, input.Operations)
	if err != nil {
		writeAddressDraftRevisionError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(revision.RowVersion))
	WriteAPIJSON(w, http.StatusCreated, revision)
}

func (api addressDraftRevisionAPI) get(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	revision, err := api.repo.GetAddressDraftRevision(r.Context(), auth.TenantID, ID(r.PathValue("revision_id")))
	if err != nil {
		writeAddressDraftRevisionError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(revision.RowVersion))
	WriteAPIJSON(w, http.StatusOK, revision)
}

func (api addressDraftRevisionAPI) apply(w http.ResponseWriter, r *http.Request) {
	expectedVersion, ok := requireAddressTaxonomyIfMatch(w, r)
	if !ok {
		return
	}
	auth, _ := AuthFromContext(r.Context())
	revision, err := api.repo.ApplyAddressDraftRevision(
		r.Context(), auth.TenantID, auth.UserID, ID(r.PathValue("revision_id")), expectedVersion,
	)
	if err != nil {
		writeAddressDraftRevisionError(w, err)
		return
	}
	w.Header().Set("ETag", quotedRowVersion(revision.RowVersion))
	WriteAPIJSON(w, http.StatusOK, revision)
}

func writeAddressDraftRevisionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		WriteAPIError(w, http.StatusNotFound, APIErrorNotFound, "Address draft revision not found", nil)
	case errors.Is(err, ErrAddressDraftRevisionInvalid), errors.Is(err, ErrAddressTaxonomyInvalid):
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, err.Error(), nil)
	case errors.Is(err, ErrAddressDraftRevisionConflict):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("version_conflict"), "Address draft revision changed since it was read", nil)
	case errors.Is(err, ErrAddressDraftRevisionChanged):
		WriteAPIError(w, http.StatusPreconditionFailed, APIErrorCode("draft_changed"), err.Error(), nil)
	case errors.Is(err, ErrAddressDraftRevisionExpired):
		WriteAPIError(w, http.StatusGone, APIErrorCode("revision_expired"), err.Error(), nil)
	case errors.Is(err, ErrAddressDraftRevisionNotPrepared):
		WriteAPIError(w, http.StatusConflict, APIErrorCode("revision_not_prepared"), err.Error(), nil)
	default:
		WriteAPIError(w, http.StatusInternalServerError, APIErrorInvalidRequest, err.Error(), nil)
	}
}
