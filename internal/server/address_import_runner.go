package server

import (
	"context"
	"fmt"
	"log"
)

const defaultAddressImportBatchSize = 1_000

// addressArtifactStore returns the configured on-disk source store.
func (s *Server) addressArtifactStore() diskAddressArtifactStore {
	return diskAddressArtifactStore{Dir: s.cfg.Address.ArtifactDir, MaxBytes: s.cfg.Address.MaxUploadBytes}
}

// enqueueAddressImport starts the import in the background and returns
// immediately. The generation is already persisted as queued; the goroutine
// flips it to importing, streams rows in batches, then to ready or failed.
func (s *Server) enqueueAddressImport(importID, language string) {
	go s.runAddressImport(context.Background(), importID, language)
}

// runAddressImport is the single MMDB/IPDB materialization path. It streams the
// artifact through the reused StreamMMDB/StreamIPDB decoders and upserts decoded
// prefixes in batches. Because the batch insert is an idempotent upsert keyed on
// (import_id, cidr), a retry simply re-streams from the start — no checkpoint is
// needed. An import left importing by a crash can be retried via the retry route.
func (s *Server) runAddressImport(ctx context.Context, importID, language string) {
	item, err := s.getAddressImport(ctx, importID)
	if err != nil {
		log.Printf("address import %s: load failed: %v", importID, err)
		return
	}
	if item.Status == AddressImportStatusReady {
		return
	}
	if err := s.beginAddressImport(ctx, importID); err != nil {
		log.Printf("address import %s: begin failed: %v", importID, err)
		return
	}
	path, err := s.addressArtifactStore().ResolveArtifact(item.ArtifactRef)
	if err != nil {
		_ = s.failAddressImport(ctx, importID, "ARTIFACT_UNAVAILABLE", err.Error())
		return
	}

	batchSize := defaultAddressImportBatchSize
	batch := make([]AddressImportRecord, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := s.insertAddressImportBatch(ctx, importID, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	visit := func(record AddressImportRecord) error {
		batch = append(batch, record)
		if len(batch) == batchSize {
			return flush()
		}
		return nil
	}

	var metadata AddressImportMetadata
	switch item.Format {
	case AddressImportFormatMMDB:
		metadata, err = StreamMMDB(path, visit)
	case AddressImportFormatIPDB:
		metadata, err = StreamIPDB(path, language, visit)
	default:
		err = fmt.Errorf("unsupported address import format %q", item.Format)
	}
	if err == nil {
		err = flush()
	}
	if err != nil {
		_ = s.failAddressImport(ctx, importID, "DECODE_FAILED", err.Error())
		log.Printf("address import %s: decode failed: %v", importID, err)
		return
	}
	if _, err := s.completeAddressImport(ctx, importID, metadata, language); err != nil {
		log.Printf("address import %s: complete failed: %v", importID, err)
	}
}
