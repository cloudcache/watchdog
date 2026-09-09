package server

import (
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/gin-gonic/gin"
)

// Address draft-revision HTTP endpoints (batch prefix edit prepare/apply), a
// faithful port of internal/watchdog api_address_draft_revisions.go over
// internal/address.Store. Preview needs address.manage; apply needs address.publish.
func (s *Server) registerAddressDraftRoutes(auth *gin.RouterGroup) {
	view := s.requirePermission("address.view")
	manage := s.requirePermission("address.manage")
	publish := s.requirePermission("address.publish")

	draft := auth.Group("/address-draft-revisions")
	draft.GET("", view, s.listAddressDraftRevisions)
	draft.POST("/preview", manage, s.previewAddressDraftRevision)
	draft.GET("/:id", view, s.getAddressDraftRevision)
	draft.GET("/:id/changes", view, s.listAddressDraftRevisionChanges)
	draft.POST("/:id/apply", publish, s.applyAddressDraftRevision)
}

func writeAddressDraftError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, address.ErrAddressDraftRevisionInvalid), errors.Is(err, address.ErrAddressTaxonomyInvalid):
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, address.ErrAddressDraftRevisionConflict):
		fail(c, http.StatusPreconditionFailed, "version_conflict", "address draft revision changed since it was read")
	case errors.Is(err, address.ErrAddressDraftRevisionChanged):
		fail(c, http.StatusPreconditionFailed, "draft_changed", err.Error())
	case errors.Is(err, address.ErrAddressDraftRevisionExpired):
		fail(c, http.StatusGone, "revision_expired", err.Error())
	case errors.Is(err, address.ErrAddressDraftRevisionNotPrepared):
		fail(c, http.StatusConflict, "revision_not_prepared", err.Error())
	default:
		writeSQLError(c, err)
	}
}

func (s *Server) listAddressDraftRevisions(c *gin.Context) {
	if _, ok := addressListParam(c, "q", "status", "sort", "order", "limit", "offset", "cursor"); !ok {
		return
	}
	limit, offset, tableMode, cursor, ok := addressListPageParams(c)
	if !ok {
		return
	}
	filter := address.AddressDraftRevisionListFilter{
		Status: strings.TrimSpace(c.Query("status")), Search: strings.TrimSpace(c.Query("q")),
		Sort: strings.TrimSpace(c.Query("sort")), Cursor: cursor,
		Desc: sortDirection(c) == "DESC" && c.Query("order") != "", Limit: limit, Offset: offset, TableMode: tableMode,
	}
	items, next, total, err := s.addressStore.ListAddressDraftRevisions(c.Request.Context(), filter)
	if err != nil {
		writeAddressDraftError(c, err)
		return
	}
	addressListResponse(c, items, next, total, tableMode, limit, offset)
}

func (s *Server) previewAddressDraftRevision(c *gin.Context) {
	var input struct {
		Operations []address.AddressPrefixBatchOperation `json:"operations"`
	}
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	revision, err := s.addressStore.PrepareAddressPrefixRevision(c.Request.Context(), currentPrincipal(c).UserID, input.Operations)
	if err != nil {
		writeAddressDraftError(c, err)
		return
	}
	c.Header("ETag", etag(revision.RowVersion))
	c.JSON(http.StatusCreated, revision)
}

func (s *Server) getAddressDraftRevision(c *gin.Context) {
	revision, err := s.addressStore.GetAddressDraftRevision(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressDraftError(c, err)
		return
	}
	c.Header("ETag", etag(revision.RowVersion))
	c.JSON(http.StatusOK, revision)
}

func (s *Server) applyAddressDraftRevision(c *gin.Context) {
	expected, ok := requireAddressIfMatch(c)
	if !ok {
		return
	}
	revision, err := s.addressStore.ApplyAddressDraftRevision(c.Request.Context(), currentPrincipal(c).UserID, c.Param("id"), expected)
	if err != nil {
		writeAddressDraftError(c, err)
		return
	}
	c.Header("ETag", etag(revision.RowVersion))
	c.JSON(http.StatusOK, revision)
}

type addressDraftRevisionChangeItem struct {
	Index    int    `json:"index"`
	Action   string `json:"action"`
	PrefixID string `json:"prefix_id"`
	CIDR     string `json:"cidr"`
}

// listAddressDraftRevisionChanges filters/sorts/paginates the prepared revision's
// change set in memory (the whole set is bounded by AddressDraftRevisionMaxOperations).
func (s *Server) listAddressDraftRevisionChanges(c *gin.Context) {
	if _, ok := addressListParam(c, "q", "action", "sort", "order", "limit", "offset"); !ok {
		return
	}
	search := strings.ToLower(strings.TrimSpace(c.Query("q")))
	action := strings.ToLower(strings.TrimSpace(c.Query("action")))
	sortField := strings.TrimSpace(c.Query("sort"))
	if len(search) > 255 || (action != "" && action != "create" && action != "delete") {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid address revision change filter")
		return
	}
	switch sortField {
	case "", "index", "action", "cidr", "prefix_id":
	default:
		fail(c, http.StatusBadRequest, "invalid_request", "invalid address revision change sort")
		return
	}
	order := strings.ToLower(strings.TrimSpace(c.Query("order")))
	if order != "" && order != "asc" && order != "desc" {
		fail(c, http.StatusBadRequest, "invalid_request", "order must be asc or desc")
		return
	}
	limit, offset := pageParams(c)
	if offset > address.AddressDraftRevisionMaxOperations {
		fail(c, http.StatusBadRequest, "invalid_request", "offset is outside the revision operation limit")
		return
	}
	revision, err := s.addressStore.GetAddressDraftRevision(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressDraftError(c, err)
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
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
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
