package watchdog

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type addressDimensionPublishJobPayload struct {
	EffectiveFrom string `json:"effective_from"`
	PreviewDigest string `json:"preview_digest"`
}

func EncodeAddressDimensionPublishJobPayload(request AddressDimensionPublishRequest) ([]byte, error) {
	if !isUTCMinute(request.EffectiveFrom) || !validSHA256Digest(request.PreviewDigest) {
		return nil, ErrAddressDimensionInvalid
	}
	return EncodeJobPayload(AddressDimensionPayloadV1, addressDimensionPublishJobPayload{
		EffectiveFrom: request.EffectiveFrom.UTC().Format(time.RFC3339Nano), PreviewDigest: request.PreviewDigest,
	})
}

func NewAddressDimensionPublishJobHandler(publisher AddressDimensionPublisher) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		if publisher == nil {
			return "", TerminalJobError(fmt.Errorf("address dimension publisher is not configured"))
		}
		var payload addressDimensionPublishJobPayload
		if err := DecodeJobPayload(job.CheckpointJSON, AddressDimensionPayloadV1, &payload); err != nil {
			return "", err
		}
		effectiveFrom, err := time.Parse(time.RFC3339Nano, payload.EffectiveFrom)
		if err != nil || !isUTCMinute(effectiveFrom) || !validSHA256Digest(payload.PreviewDigest) || job.TenantID == "" || job.CreatedBy == "" {
			return "", TerminalJobError(ErrAddressDimensionInvalid)
		}
		snapshot, err := publisher.PublishAddressDimension(ctx, job.TenantID, job.CreatedBy, AddressDimensionPublishRequest{
			EffectiveFrom: effectiveFrom, PreviewDigest: payload.PreviewDigest,
		})
		if errors.Is(err, ErrAddressDimensionDraftChanged) || errors.Is(err, ErrAddressDimensionInvalid) || errors.Is(err, ErrAddressDimensionConflict) {
			return "", TerminalJobError(err)
		}
		if err != nil {
			return "", err
		}
		return "dimension-snapshot:" + string(snapshot.ID), nil
	}
}
