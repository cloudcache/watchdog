package watchdog

import (
	"database/sql"
	"errors"
	"net/http"
	"sort"
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
	mux.Handle("GET /api/v1/address-draft-revisions/{revision_id}/changes", auth(view(http.HandlerFunc(api.listChanges))))
	mux.Handle("POST /api/v1/address-draft-revisions/{revision_id}/apply", auth(operate(http.HandlerFunc(api.apply))))
}

func (api addressDraftRevisionAPI) list(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	query := r.URL.Query()
	for key := range query {
		switch key {
		case "q", "status", "sort", "order", "limit", "offset", "cursor":
		default:
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "unsupported query parameter: "+key, nil)
			return
		}
	}
	filter := AddressDraftRevisionListFilter{
		Status: strings.TrimSpace(query.Get("status")),
		Search: strings.TrimSpace(query.Get("q")),
		Sort:   strings.TrimSpace(query.Get("sort")),
		Cursor: strings.TrimSpace(query.Get("cursor")),
	}
	if len(filter.Search) > 255 || len(filter.Status) > 32 {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "address revision filter is too long", nil)
		return
	}
	filter.TableMode = query.Has("sort") || query.Has("order") || query.Has("offset")
	if filter.Cursor != "" && filter.TableMode {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "cursor cannot be combined with table query parameters", nil)
		return
	}
	if _, ok := addressDraftRevisionSortColumns[filter.Sort]; !ok {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid address revision sort", nil)
		return
	}
	order := strings.ToLower(strings.TrimSpace(query.Get("order")))
	if order != "" && order != "asc" && order != "desc" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "order must be asc or desc", nil)
		return
	}
	filter.Desc = order == "desc"
	var err error
	filter.Limit, err = parseAgentPageInteger(query.Get("limit"), 100, 1, 500)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be between 1 and 500", nil)
		return
	}
	filter.Offset, err = parseAgentPageInteger(query.Get("offset"), 0, 0, int(^uint(0)>>1))
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "offset must be zero or greater", nil)
		return
	}
	items, cursor, total, err := api.repo.ListAddressDraftRevisions(r.Context(), auth.TenantID, filter)
	if err != nil {
		writeAddressDraftRevisionError(w, err)
		return
	}
	if items == nil {
		items = []AddressDraftRevision{}
	}
	response := map[string]any{"items": items, "total": total}
	if filter.TableMode {
		response["limit"] = filter.Limit
		response["offset"] = filter.Offset
	}
	if cursor != "" {
		response["next_cursor"] = cursor
	}
	WriteAPIJSON(w, http.StatusOK, response)
}

type addressDraftRevisionChangeItem struct {
	Index    int    `json:"index"`
	Action   string `json:"action"`
	PrefixID string `json:"prefix_id"`
	CIDR     string `json:"cidr"`
}

func (api addressDraftRevisionAPI) listChanges(w http.ResponseWriter, r *http.Request) {
	auth, _ := AuthFromContext(r.Context())
	query := r.URL.Query()
	for key := range query {
		switch key {
		case "q", "action", "sort", "order", "limit", "offset":
		default:
			WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "unsupported query parameter: "+key, nil)
			return
		}
	}
	search := strings.ToLower(strings.TrimSpace(query.Get("q")))
	action := strings.ToLower(strings.TrimSpace(query.Get("action")))
	sortField := strings.TrimSpace(query.Get("sort"))
	if len(search) > 255 || (action != "" && action != "create" && action != "delete") {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid address revision change filter", nil)
		return
	}
	switch sortField {
	case "", "index", "action", "cidr", "prefix_id":
	default:
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "invalid address revision change sort", nil)
		return
	}
	order := strings.ToLower(strings.TrimSpace(query.Get("order")))
	if order != "" && order != "asc" && order != "desc" {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "order must be asc or desc", nil)
		return
	}
	limit, err := parseAgentPageInteger(query.Get("limit"), 100, 1, 500)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "limit must be between 1 and 500", nil)
		return
	}
	offset, err := parseAgentPageInteger(query.Get("offset"), 0, 0, AddressDraftRevisionMaxOperations)
	if err != nil {
		WriteAPIError(w, http.StatusBadRequest, APIErrorInvalidRequest, "offset is outside the revision operation limit", nil)
		return
	}
	revision, err := api.repo.GetAddressDraftRevision(r.Context(), auth.TenantID, ID(r.PathValue("revision_id")))
	if err != nil {
		writeAddressDraftRevisionError(w, err)
		return
	}
	items := make([]addressDraftRevisionChangeItem, 0, len(revision.Preview.Changes))
	for index, change := range revision.Preview.Changes {
		if action != "" && change.Action != action {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(change.Action+" "+change.CIDR+" "+change.PrefixID), search) {
			continue
		}
		items = append(items, addressDraftRevisionChangeItem{Index: index + 1, Action: change.Action, PrefixID: change.PrefixID, CIDR: change.CIDR})
	}
	if sortField == "" {
		sortField = "index"
	}
	desc := order == "desc"
	sort.SliceStable(items, func(left, right int) bool {
		comparison := compareAddressDraftRevisionChanges(items[left], items[right], sortField)
		if desc {
			return comparison > 0
		}
		return comparison < 0
	})
	total := len(items)
	if offset >= total {
		items = []addressDraftRevisionChangeItem{}
	} else {
		end := min(offset+limit, total)
		items = items[offset:end]
	}
	WriteAPIJSON(w, http.StatusOK, map[string]any{"items": items, "total": total, "limit": limit, "offset": offset})
}

func compareAddressDraftRevisionChanges(left, right addressDraftRevisionChangeItem, field string) int {
	switch field {
	case "index":
		return left.Index - right.Index
	case "action":
		return strings.Compare(left.Action, right.Action)
	case "cidr":
		return strings.Compare(left.CIDR, right.CIDR)
	default:
		return strings.Compare(left.PrefixID, right.PrefixID)
	}
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
