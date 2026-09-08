package server

import (
	"database/sql"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

// registerAddressImportRoutes wires the address-library source imports: upload an
// MMDB/IPDB generation, watch it decode, browse its base prefixes, and activate a
// ready generation into its slot. View is address.view; mutating an import (upload,
// activate, retry) is address.manage. The POST /address-imports body-size cap is
// lifted in requestBodyLimit — uploads are bounded by cfg.Address.MaxUploadBytes.
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
	imports.POST("/:id/actions/retry", manage, s.retryImport)

	auth.GET("/address-import-slots/:slot", view, s.getImportSlot)
}

func (s *Server) listImports(c *gin.Context) {
	limit, offset := pageParams(c)
	filter := addressImportListFilter{
		SourceSlot: strings.TrimSpace(c.Query("source_slot")),
		Status:     strings.TrimSpace(c.Query("status")),
		Format:     strings.TrimSpace(c.Query("format")),
		Search:     strings.TrimSpace(c.Query("q")),
		Limit:      limit,
		Offset:     offset,
	}
	items, total, err := s.listAddressImports(c.Request.Context(), filter)
	if err != nil {
		if errors.Is(err, errAddressImportInvalid) {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

func (s *Server) getImport(c *gin.Context) {
	item, err := s.getAddressImport(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, item)
}

// uploadImport streams a multipart upload (fields: file, source_slot, language)
// straight to the artifact store, records a queued generation, and enqueues the
// background decode. The body is not buffered in memory.
func (s *Server) uploadImport(c *gin.Context) {
	importID := newID()
	maxUpload := s.cfg.Address.MaxUploadBytes
	if maxUpload <= 0 {
		maxUpload = defaultAddressArtifactMaxBytes
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxUpload+(1<<20))
	reader, err := c.Request.MultipartReader()
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "Content-Type must be multipart/form-data")
		return
	}
	store := s.addressArtifactStore()
	var sourceSlot, language, originalName string
	var artifact addressArtifact
	retained := false
	defer func() {
		if artifact.Ref != "" && !retained {
			_ = store.RemoveArtifact(artifact.Ref)
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
			artifact, err = store.SaveArtifact(c.Request.Context(), importID, originalName, part)
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
	if !validAddressImportSlot(sourceSlot) || len(language) > 16 {
		fail(c, http.StatusBadRequest, "invalid_request", "source_slot or language is invalid")
		return
	}
	item, err := s.createAddressImport(c.Request.Context(), AddressImport{
		ID: importID, SourceSlot: sourceSlot, Format: artifact.Format,
		OriginalName: originalName, ArtifactRef: artifact.Ref, ChecksumSHA256: artifact.ChecksumSHA256,
		SizeBytes: artifact.SizeBytes, Status: AddressImportStatusQueued, CreatedBy: currentPrincipal(c).UserID,
	})
	if err != nil {
		if errors.Is(err, errAddressImportInvalid) {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeSQLError(c, err)
		return
	}
	retained = true
	s.enqueueAddressImport(item.ID, language)
	c.JSON(http.StatusAccepted, gin.H{"import": item})
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
	if _, err := s.getAddressImport(ctx, c.Param("id")); err != nil {
		writeSQLError(c, err)
		return
	}
	limit, offset := pageParams(c)
	filter := addressBasePrefixFilter{
		CountryCode: strings.TrimSpace(c.Query("country_code")),
		Operator:    strings.TrimSpace(c.Query("operator")),
		Search:      strings.TrimSpace(c.Query("q")),
		Limit:       limit,
		Offset:      offset,
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
	items, total, err := s.listAddressBasePrefixes(ctx, c.Param("id"), filter)
	if err != nil {
		if errors.Is(err, errAddressImportInvalid) {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": limit, "offset": offset})
}

func (s *Server) lookupImportPrefix(c *gin.Context) {
	ctx := c.Request.Context()
	if _, err := s.getAddressImport(ctx, c.Param("id")); err != nil {
		writeSQLError(c, err)
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
	items, err := s.lookupAddressBasePrefixes(ctx, c.Param("id"), value, limit)
	if err != nil {
		if errors.Is(err, errAddressImportInvalid) {
			fail(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

// activateImport points the import's slot at this ready generation. If-Match, when
// supplied, must equal the slot's current row_version (0/absent activates an empty
// slot or, when no If-Match is sent, replaces the current generation unconditionally).
func (s *Server) activateImport(c *gin.Context) {
	ctx := c.Request.Context()
	importID := c.Param("id")
	expected, supplied, err := ifMatch(c)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_if_match", "invalid If-Match")
		return
	}
	if !supplied {
		// No precondition: read the slot this import belongs to and reuse its
		// current version so activation is unconditional rather than failing.
		item, err := s.getAddressImport(ctx, importID)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		if slot, err := s.getAddressImportSlot(ctx, item.SourceSlot); err == nil {
			expected = slot.RowVersion
		} else if !errors.Is(err, sql.ErrNoRows) {
			writeSQLError(c, err)
			return
		}
	}
	slot, err := s.activateAddressImport(ctx, importID, currentPrincipal(c).UserID, expected)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			fail(c, http.StatusNotFound, "not_found", "address import not found")
		case errors.Is(err, errAddressImportVersionConflict):
			fail(c, http.StatusPreconditionFailed, "version_conflict", "active address import changed since it was read")
		case errors.Is(err, errAddressImportNotWritable):
			fail(c, http.StatusConflict, "invalid_request", "only a ready address import can be activated")
		default:
			writeSQLError(c, err)
		}
		return
	}
	c.Header("ETag", etag(slot.RowVersion))
	c.JSON(http.StatusOK, slot)
}

// retryImport re-runs a failed or interrupted decode. The batch upsert is
// idempotent, so re-streaming safely reconciles rows.
func (s *Server) retryImport(c *gin.Context) {
	item, err := s.getAddressImport(c.Request.Context(), c.Param("id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	switch item.Status {
	case AddressImportStatusReady:
		fail(c, http.StatusConflict, "invalid_request", "import is already ready")
		return
	case AddressImportStatusRetired:
		fail(c, http.StatusConflict, "invalid_request", "import is retired")
		return
	}
	s.enqueueAddressImport(item.ID, item.Language)
	c.JSON(http.StatusAccepted, gin.H{"import_id": item.ID, "status": AddressImportStatusImporting})
}

func (s *Server) getImportSlot(c *gin.Context) {
	slot, err := s.getAddressImportSlot(c.Request.Context(), strings.TrimSpace(c.Param("slot")))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			fail(c, http.StatusNotFound, "not_found", "address import slot is not active")
			return
		}
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(slot.RowVersion))
	c.JSON(http.StatusOK, slot)
}
