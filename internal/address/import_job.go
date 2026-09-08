package address

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cloudcache/watchdog/internal/opjob"
)

const (
	AddressImportJobType           = "address_import"
	AddressImportJobPayloadVersion = 1
	defaultAddressImportBatchSize  = 1_000
)

type addressImportJobPayload struct {
	ImportID  ID     `json:"import_id"`
	Language  string `json:"language,omitempty"`
	Processed uint64 `json:"processed"`
}

// EncodeAddressImportJobPayload builds the versioned checkpoint the enqueue side
// stores so the handler and enqueue agree on the payload contract.
func EncodeAddressImportJobPayload(importID ID, language string, processed uint64) (json.RawMessage, error) {
	return opjob.EncodePayload(AddressImportJobPayloadVersion, addressImportJobPayload{
		ImportID: importID, Language: language, Processed: processed,
	})
}

// ArtifactStore resolves an artifact ref to a local path the reader can open.
type ArtifactStore interface {
	ResolveArtifact(ref string) (string, error)
}

type addressImportJobRepository interface {
	GetAddressImport(ctx context.Context, importID ID) (AddressImport, error)
	BeginAddressImport(ctx context.Context, importID ID) error
	InsertAddressImportBatch(ctx context.Context, importID ID, records []AddressImportRecord) error
	CompleteAddressImport(ctx context.Context, importID ID, metadata AddressImportMetadata, language string) (AddressImport, error)
	FailAddressImport(ctx context.Context, importID ID, code, detail string) error
}

type addressImportBatchWriteError struct{ err error }

func (e addressImportBatchWriteError) Error() string { return e.err.Error() }
func (e addressImportBatchWriteError) Unwrap() error { return e.err }

// NewAddressImportJobHandler builds the only MMDB/IPDB materialization path. The
// operation job's versioned checkpoint remains self-contained: it carries both
// immutable input identity and the last durable source-record ordinal. A retry
// replays only from that ordinal; a crash between batch commit and checkpoint
// update safely re-upserts deterministic rows. De-tenanted port; the reader,
// batch upsert, and checkpoint logic are unchanged.
func NewAddressImportJobHandler(repo addressImportJobRepository, artifacts ArtifactStore, batchSize int) opjob.Handler {
	if batchSize <= 0 || batchSize > maxAddressImportBatch {
		batchSize = defaultAddressImportBatchSize
	}
	return func(ctx context.Context, job opjob.Job) (string, error) {
		if repo == nil || artifacts == nil {
			return "", opjob.TerminalError(errors.New("address import job dependencies are not configured"))
		}
		var payload addressImportJobPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, AddressImportJobPayloadVersion, &payload); err != nil {
			return "", err
		}
		if payload.ImportID == "" {
			return "", opjob.TerminalError(errors.New("address import job payload has no import_id"))
		}
		item, err := repo.GetAddressImport(ctx, payload.ImportID)
		if err != nil {
			return "", err
		}
		if item.Status == AddressImportStatusReady {
			return "address-import:" + string(item.ID), nil
		}
		if err := repo.BeginAddressImport(ctx, payload.ImportID); err != nil {
			return "", err
		}
		path, err := artifacts.ResolveArtifact(item.ArtifactRef)
		if err != nil {
			_ = repo.FailAddressImport(ctx, payload.ImportID, "ARTIFACT_UNAVAILABLE", err.Error())
			return "", opjob.TerminalError(fmt.Errorf("resolve address artifact: %w", err))
		}

		processed := payload.Processed
		seen := uint64(0)
		batch := make([]AddressImportRecord, 0, batchSize)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := repo.InsertAddressImportBatch(ctx, payload.ImportID, batch); err != nil {
				return addressImportBatchWriteError{err: err}
			}
			processed += uint64(len(batch))
			checkpoint, err := EncodeAddressImportJobPayload(payload.ImportID, payload.Language, processed)
			if err != nil {
				return err
			}
			if err := opjob.ReporterFromContext(ctx).Report(ctx, processed, checkpoint); err != nil {
				return err
			}
			batch = batch[:0]
			return nil
		}
		visit := func(record AddressImportRecord) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			seen++
			if seen <= payload.Processed {
				return nil
			}
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
			metadata, err = StreamIPDB(path, payload.Language, visit)
		default:
			err = fmt.Errorf("unsupported address import format %q", item.Format)
		}
		if err == nil {
			err = flush()
		}
		if err != nil {
			var batchWrite addressImportBatchWriteError
			if errors.As(err, &batchWrite) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, opjob.ErrLeaseLost) {
				return "", err
			}
			_ = repo.FailAddressImport(ctx, payload.ImportID, "DECODE_FAILED", err.Error())
			return "", opjob.TerminalError(fmt.Errorf("decode address artifact: %w", err))
		}
		if seen < payload.Processed {
			err := fmt.Errorf("checkpoint processed %d records but artifact contains %d", payload.Processed, seen)
			_ = repo.FailAddressImport(ctx, payload.ImportID, "CHECKPOINT_INVALID", err.Error())
			return "", opjob.TerminalError(err)
		}
		if _, err := repo.CompleteAddressImport(ctx, payload.ImportID, metadata, payload.Language); err != nil {
			return "", err
		}
		return "address-import:" + string(payload.ImportID), nil
	}
}
