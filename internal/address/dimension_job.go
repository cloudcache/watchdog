package address

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/opjob"
)

type addressDimensionPublishJobPayload struct {
	EffectiveFrom string `json:"effective_from"`
	PreviewDigest string `json:"preview_digest"`
}

// EncodeAddressDimensionPublishJobPayload builds the versioned checkpoint the
// publish enqueue stores; the build job id is the snapshot id.
func EncodeAddressDimensionPublishJobPayload(request AddressDimensionPublishRequest) (json.RawMessage, error) {
	if !isUTCMinute(request.EffectiveFrom) || !validSHA256Digest(request.PreviewDigest) {
		return nil, ErrAddressDimensionInvalid
	}
	return opjob.EncodePayload(AddressDimensionPayloadV1, addressDimensionPublishJobPayload{
		EffectiveFrom: request.EffectiveFrom.UTC().Format(time.RFC3339Nano), PreviewDigest: request.PreviewDigest,
	})
}

// NewAddressSnapshotBuildJobHandler is the binary publication writer. The job id
// is also the immutable snapshot id, so a retry after commit but before job
// acknowledgement converges on the same publication.
func NewAddressSnapshotBuildJobHandler(publisher *Publisher) opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		if publisher == nil {
			return "", opjob.TerminalError(fmt.Errorf("address snapshot builder is not configured"))
		}
		var payload addressDimensionPublishJobPayload
		if err := opjob.DecodePayload(job.CheckpointJSON, AddressDimensionPayloadV1, &payload); err != nil {
			return "", err
		}
		effectiveFrom, err := time.Parse(time.RFC3339Nano, payload.EffectiveFrom)
		if err != nil || !isUTCMinute(effectiveFrom) || !validSHA256Digest(payload.PreviewDigest) || job.ID == "" || job.CreatedBy == "" {
			return "", opjob.TerminalError(ErrAddressDimensionInvalid)
		}
		snapshot, err := publisher.BuildAddressSnapshotPublication(ctx, job.CreatedBy, job.ID, AddressDimensionPublishRequest{
			EffectiveFrom: effectiveFrom, PreviewDigest: payload.PreviewDigest,
		})
		if errors.Is(err, ErrAddressDimensionDraftChanged) || errors.Is(err, ErrAddressDimensionInvalid) || errors.Is(err, ErrAddressDimensionConflict) {
			return "", opjob.TerminalError(err)
		}
		if err != nil {
			return "", err
		}
		return "dimension-snapshot:" + string(snapshot.ID), nil
	}
}
