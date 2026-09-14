package server

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
)

// registerAddressImportRoutes wires MMDB/IPDB source imports onto internal/address:
// upload -> opjob import worker -> ready generation -> activate slot, plus base-prefix
// browse/lookup. View is address.view; mutations are address.manage. The upload
// body-size cap is lifted in requestBodyLimit; uploads honour cfg.Address.MaxUploadBytes.
func (s *Server) registerAddressImportRoutes(auth *gin.RouterGroup) {
	view := s.requirePermission("address.view")
	manage := s.requirePermission("address.manage")

	imports := auth.Group("/address-imports")
	imports.GET("", view, s.listImports)
	imports.POST("", manage, s.uploadImport)
	imports.GET("/:id", view, s.getImport)
	imports.GET("/:id/prefixes", view, s.listImportPrefixes)
	imports.GET("/:id/lookup", view, s.lookupImportPrefix)
	imports.POST("/:id/actions/activate", manage, s.activateImport)
	imports.DELETE("/:id", manage, s.deleteImport)
	s.registerAddressImportTusRoutes(auth)

	auth.GET("/address-import-slots/:slot", view, s.getImportSlot)
}

// deleteImport removes an import generation (and its base prefixes) and deletes the
// stored source artifact. An import that still backs an active slot is refused so a
// live generation is never pulled out from under the flow workers.
func (s *Server) deleteImport(c *gin.Context) {
	artifactRef, err := s.addressStore.DeleteAddressImport(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, address.ErrAddressImportActive) {
			fail(c, http.StatusConflict, "invalid_request", "active address import cannot be deleted; activate a different generation into its slot first")
			return
		}
		writeAddressImportError(c, err)
		return
	}
	if artifactRef != "" {
		_ = s.addressArtifacts.RemoveArtifact(artifactRef)
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "address.import.delete", "address_import", c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"id": c.Param("id")})
}

func writeAddressImportError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		fail(c, http.StatusNotFound, "not_found", "address import not found")
	case errors.Is(err, address.ErrAddressImportVersionConflict):
		fail(c, http.StatusPreconditionFailed, "version_conflict", "active address import changed since it was read")
	case errors.Is(err, address.ErrAddressImportNotWritable):
		fail(c, http.StatusConflict, "invalid_request", "address import generation is not writable")
	case errors.Is(err, address.ErrAddressImportInvalid):
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
	default:
		writeSQLError(c, err)
	}
}

// enqueueAddressImportJob (re)schedules the async decode of an import generation.
func (s *Server) enqueueAddressImportJob(c *gin.Context, importID, language string) (opjob.Job, error) {
	return s.enqueueAddressImportJobCtx(c.Request.Context(), importID, language, currentPrincipal(c).UserID)
}

// enqueueAddressImportJobCtx is the context-free core shared by the multipart handler
// and the tus completion callback (which has no gin.Context).
func (s *Server) enqueueAddressImportJobCtx(ctx context.Context, importID, language, createdBy string) (opjob.Job, error) {
	payload, err := address.EncodeAddressImportJobPayload(importID, language, 0)
	if err != nil {
		return opjob.Job{}, err
	}
	return s.jobs.Enqueue(ctx, opjob.Job{
		JobType: address.AddressImportJobType, IdempotencyKey: "address-import:" + importID,
		RequestHash: sha256hex(string(payload)), CheckpointJSON: payload, CreatedBy: createdBy,
	})
}

func (s *Server) listImports(c *gin.Context) {
	limit, offset := pageParams(c)
	items, total, err := listAddressImportsCompat(c, s.addressStore, address.AddressImportListFilter{
		SourceSlot: strings.TrimSpace(c.Query("source_slot")), Status: strings.TrimSpace(c.Query("status")),
		Format: strings.TrimSpace(c.Query("format")), Search: strings.TrimSpace(c.Query("q")),
		Limit: limit, Offset: offset, TableMode: true, Sort: c.Query("sort"), Desc: sortDirection(c) == "DESC",
	})
	if err != nil {
		writeAddressImportError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

func listAddressImportsCompat(c *gin.Context, store *address.Store, filter address.AddressImportListFilter) ([]address.AddressImport, int, error) {
	items, _, total, err := store.ListAddressImports(c.Request.Context(), filter)
	return items, total, err
}

func (s *Server) getImport(c *gin.Context) {
	item, err := s.addressStore.GetAddressImport(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeAddressImportError(c, err)
		return
	}
	c.JSON(http.StatusOK, item)
}

// uploadImport streams a multipart upload (fields: file, source_slot, language)
// to the artifact store, records a queued generation, and enqueues the opjob
// decode. The body is not buffered in memory.
func (s *Server) uploadImport(c *gin.Context) {
	importID := newID()
	maxUpload := s.cfg.Address.MaxUploadBytes
	if maxUpload <= 0 {
		maxUpload = 2 << 30
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxUpload+(1<<20))
	reader, err := c.Request.MultipartReader()
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "Content-Type must be multipart/form-data")
		return
	}
	var sourceSlot, language, originalName string
	var artifact address.Artifact
	retained := false
	defer func() {
		if artifact.Ref != "" && !retained {
			_ = s.addressArtifacts.RemoveArtifact(artifact.Ref)
		}
	}()
	for {
		part, nextErr := reader.NextPart()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			fail(c, http.StatusBadRequest, "invalid_request", nextErr.Error())
			return
		}
		switch part.FormName() {
		case "file":
			if artifact.Ref != "" || part.FileName() == "" {
				part.Close()
				fail(c, http.StatusBadRequest, "invalid_request", "exactly one address database file is required")
				return
			}
			originalName = filepath.Base(strings.TrimSpace(part.FileName()))
			if originalName == "" || len(originalName) > 255 {
				part.Close()
				fail(c, http.StatusBadRequest, "invalid_request", "address database filename is invalid")
				return
			}
			artifact, err = s.addressArtifacts.SaveArtifact(c.Request.Context(), importID, originalName, part)
		case "source_slot":
			sourceSlot, err = readAddressImportFormValue(part)
		case "language":
			language, err = readAddressImportFormValue(part)
		default:
			err = errors.New("unknown multipart field " + part.FormName())
		}
		part.Close()
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}
	if artifact.Ref == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "address database file is required")
		return
	}
	sourceSlot = strings.TrimSpace(sourceSlot)
	language = strings.TrimSpace(language)
	if len(language) > 16 {
		fail(c, http.StatusBadRequest, "invalid_request", "language is invalid")
		return
	}
	item, err := s.addressStore.CreateAddressImport(c.Request.Context(), address.AddressImport{
		ID: importID, SourceSlot: sourceSlot, Format: artifact.Format,
		OriginalName: originalName, ArtifactRef: artifact.Ref, ChecksumSHA256: artifact.ChecksumSHA256,
		SizeBytes: artifact.SizeBytes, Status: address.AddressImportStatusQueued, CreatedBy: currentPrincipal(c).UserID,
	})
	if err != nil {
		writeAddressImportError(c, err)
		return
	}
	retained = true
	job, err := s.enqueueAddressImportJob(c, item.ID, language)
	if err != nil {
		_ = s.addressStore.FailAddressImport(c.Request.Context(), item.ID, "ENQUEUE_FAILED", err.Error())
		fail(c, http.StatusInternalServerError, "internal", "failed to enqueue address import")
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"import": item, "job": job})
}

func readAddressImportFormValue(part *multipart.Part) (string, error) {
	data, err := io.ReadAll(io.LimitReader(part, 4_097))
	if err != nil {
		return "", err
	}
	if len(data) > 4_096 {
		return "", errors.New("multipart field exceeds 4096 bytes")
	}
	return string(data), nil
}

func (s *Server) listImportPrefixes(c *gin.Context) {
	ctx := c.Request.Context()
	if _, err := s.addressStore.GetAddressImport(ctx, c.Param("id")); err != nil {
		writeAddressImportError(c, err)
		return
	}
	limit, offset := pageParams(c)
	filter := address.AddressBasePrefixFilter{
		CountryCode: strings.TrimSpace(c.Query("country_code")), Operator: strings.TrimSpace(c.Query("operator")),
		Search: strings.TrimSpace(c.Query("q")), Limit: limit, Offset: offset, TableMode: true,
		Sort: c.Query("sort"), Desc: sortDirection(c) == "DESC",
	}
	if raw := strings.TrimSpace(c.Query("family")); raw != "" {
		family, err := strconv.ParseUint(raw, 10, 8)
		if err != nil || (family != 4 && family != 6) {
			fail(c, http.StatusBadRequest, "invalid_request", "family must be 4 or 6")
			return
		}
		filter.Family = uint8(family)
	}
	if raw := strings.TrimSpace(c.Query("asn")); raw != "" {
		asn, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "asn must be an unsigned 32-bit integer")
			return
		}
		value := uint32(asn)
		filter.ASN = &value
	}
	items, total, err := listAddressBasePrefixesCompat(c, s.addressStore, c.Param("id"), filter)
	if err != nil {
		writeAddressImportError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

func listAddressBasePrefixesCompat(c *gin.Context, store *address.Store, importID string, filter address.AddressBasePrefixFilter) ([]address.AddressBasePrefix, int, error) {
	items, _, total, err := store.ListAddressBasePrefixes(c.Request.Context(), importID, filter)
	return items, total, err
}

func (s *Server) lookupImportPrefix(c *gin.Context) {
	ctx := c.Request.Context()
	if _, err := s.addressStore.GetAddressImport(ctx, c.Param("id")); err != nil {
		writeAddressImportError(c, err)
		return
	}
	value := strings.TrimSpace(c.Query("ip"))
	if value == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "ip is required")
		return
	}
	limit := 0
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			fail(c, http.StatusBadRequest, "invalid_request", "limit must be a positive integer")
			return
		}
		limit = parsed
	}
	items, err := s.addressStore.LookupAddressBasePrefixes(ctx, c.Param("id"), value, limit)
	if err != nil {
		writeAddressImportError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) activateImport(c *gin.Context) {
	ctx := c.Request.Context()
	importID := c.Param("id")
	expected, supplied, err := ifMatch(c)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	}
	if !supplied {
		item, err := s.addressStore.GetAddressImport(ctx, importID)
		if err != nil {
			writeAddressImportError(c, err)
			return
		}
		if slot, err := s.addressStore.GetAddressImportSlot(ctx, item.SourceSlot); err == nil {
			expected = slot.RowVersion
		} else if !errors.Is(err, sql.ErrNoRows) {
			writeAddressImportError(c, err)
			return
		}
	}
	slot, err := s.addressStore.ActivateAddressImport(ctx, importID, currentPrincipal(c).UserID, expected)
	if err != nil {
		writeAddressImportError(c, err)
		return
	}
	c.Header("ETag", etag(slot.RowVersion))
	c.JSON(http.StatusOK, slot)
}

func (s *Server) getImportSlot(c *gin.Context) {
	name := strings.TrimSpace(c.Param("slot"))
	slot, err := s.addressStore.GetAddressImportSlot(c.Request.Context(), name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// A slot with no activated generation is a normal, expected state — the UI
			// probes every slot on load. Report it as an inactive slot (200) rather than
			// a 404 the browser logs as a client error on every page view.
			c.JSON(http.StatusOK, address.AddressImportSlot{SourceSlot: name})
			return
		}
		writeAddressImportError(c, err)
		return
	}
	c.Header("ETag", etag(slot.RowVersion))
	c.JSON(http.StatusOK, slot)
}
