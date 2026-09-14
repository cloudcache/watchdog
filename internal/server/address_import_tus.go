// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/gin-gonic/gin"
	"github.com/tus/tusd/v2/pkg/filestore"
	tushandler "github.com/tus/tusd/v2/pkg/handler"
)

// address_import_tus.go embeds a tus (resumable upload) endpoint for large address
// source databases (MMDB/IPDB). A client such as Uppy creates an upload, PATCHes
// chunks that survive network interruptions, and on completion the assembled file is
// copied into the address artifact store and a queued import generation + decode job
// are created — the same terminal state the multipart uploadImport reaches, so the
// rest of the import pipeline (decode worker, activate, slots) is unchanged.

const addressTusBasePath = "/api/v1/address-imports/uploads/"

// addressTusActorKey carries the authenticated user id from the Gin request into the
// tus completion callback, which runs without a gin.Context.
type addressTusActorKey struct{}

// newAddressImportTusHandler builds the embedded tus handler. Chunks stage under
// <artifact_dir>/.uploads; tus CORS is disabled because s.cors() owns CORS for the
// whole server (extended with the tus header set) — running both would emit duplicate
// Access-Control-* headers.
func (s *Server) newAddressImportTusHandler() (*tushandler.UnroutedHandler, error) {
	stageDir := filepath.Join(s.cfg.Address.ArtifactDir, ".uploads")
	if err := os.MkdirAll(stageDir, 0o750); err != nil {
		return nil, fmt.Errorf("create tus upload dir: %w", err)
	}
	composer := tushandler.NewStoreComposer()
	filestore.New(stageDir).UseIn(composer)
	maxSize := s.cfg.Address.MaxUploadBytes
	if maxSize <= 0 {
		maxSize = 2 << 30
	}
	return tushandler.NewUnroutedHandler(tushandler.Config{
		BasePath:                  addressTusBasePath,
		StoreComposer:             composer,
		MaxSize:                   maxSize,
		Cors:                      &tushandler.CorsConfig{Disable: true},
		PreFinishResponseCallback: s.finishAddressImportUpload,
	})
}

// finishAddressImportUpload turns a completed tus upload into a queued import
// generation. Returning an error rejects the completion and surfaces it to the client.
func (s *Server) finishAddressImportUpload(hook tushandler.HookEvent) (tushandler.HTTPResponse, error) {
	var resp tushandler.HTTPResponse
	path := hook.Upload.Storage[filestore.StorageKeyPath]
	if strings.TrimSpace(path) == "" {
		return resp, errors.New("completed upload has no stored path")
	}
	filename := firstNonEmptyMeta(hook.Upload.MetaData, "filename", "name")
	if filename == "" {
		return resp, errors.New("upload metadata must include a filename")
	}
	sourceSlot := strings.TrimSpace(hook.Upload.MetaData["source_slot"])
	language := strings.TrimSpace(hook.Upload.MetaData["language"])
	if len(language) > 16 {
		return resp, errors.New("language is invalid")
	}
	actor, _ := hook.Context.Value(addressTusActorKey{}).(string)

	file, err := os.Open(path)
	if err != nil {
		return resp, fmt.Errorf("open completed upload: %w", err)
	}
	defer file.Close()

	importID := newID()
	artifact, err := s.addressArtifacts.SaveArtifact(hook.Context, importID, filename, file)
	if err != nil {
		return resp, err
	}
	item, err := s.addressStore.CreateAddressImport(hook.Context, address.AddressImport{
		ID: importID, SourceSlot: sourceSlot, Format: artifact.Format, OriginalName: filename,
		ArtifactRef: artifact.Ref, ChecksumSHA256: artifact.ChecksumSHA256, SizeBytes: artifact.SizeBytes,
		Status: address.AddressImportStatusQueued, CreatedBy: actor,
	})
	if err != nil {
		_ = s.addressArtifacts.RemoveArtifact(artifact.Ref)
		return resp, err
	}
	if _, err := s.enqueueAddressImportJobCtx(hook.Context, item.ID, language, actor); err != nil {
		_ = s.addressStore.FailAddressImport(hook.Context, item.ID, "ENQUEUE_FAILED", err.Error())
		return resp, err
	}
	s.audit(hook.Context, actor, "address.import.upload", "address_import", item.ID)
	// The staged tus copy is now redundant with the artifact-store copy.
	_ = os.Remove(path)
	_ = os.Remove(path + ".info")
	resp.Header = tushandler.HTTPHeader{"X-Watchdog-Import-Id": item.ID}
	return resp, nil
}

func firstNonEmptyMeta(meta map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(meta[key]); value != "" {
			return value
		}
	}
	return ""
}

// registerAddressImportTusRoutes mounts the tus endpoints (address.manage). The
// completion callback needs the actor, so create/append requests carry it into the
// request context.
func (s *Server) registerAddressImportTusRoutes(auth *gin.RouterGroup) {
	if s.addressTus == nil {
		return
	}
	manage := s.requirePermission("address.manage")
	wrap := func(fn http.HandlerFunc) gin.HandlerFunc {
		return gin.WrapH(s.addressTus.Middleware(http.HandlerFunc(fn)))
	}
	uploads := auth.Group("/address-imports/uploads")
	uploads.POST("", manage, s.tusActorContext, wrap(s.addressTus.PostFile))
	uploads.HEAD("/:id", manage, wrap(s.addressTus.HeadFile))
	uploads.PATCH("/:id", manage, s.tusActorContext, wrap(s.addressTus.PatchFile))
	uploads.DELETE("/:id", manage, wrap(s.addressTus.DelFile))
}

// tusActorContext copies the authenticated user id into the request context so the
// tus completion callback (which has no gin.Context) can attribute the import.
func (s *Server) tusActorContext(c *gin.Context) {
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), addressTusActorKey{}, currentPrincipal(c).UserID))
	c.Next()
}
