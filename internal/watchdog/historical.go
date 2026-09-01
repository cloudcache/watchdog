package watchdog

import (
	"errors"
	"time"
)

type HistoricalAction string

const (
	HistoricalActionPreview HistoricalAction = "preview"
	HistoricalActionArchive HistoricalAction = "archive"
	HistoricalActionDelete  HistoricalAction = "delete"
)

type HistoricalDataRequest struct {
	TenantID ID
	TargetID ID
	DeviceID ID
	PortID   ID
	Action   HistoricalAction
	Start    time.Time
	End      time.Time
}

type HistoricalDataPreview struct {
	Request          HistoricalDataRequest
	EstimatedSamples int
	EstimatedBytes   uint64
}

type HistoricalDataOperation struct {
	ID      ID
	Request HistoricalDataRequest
	Status  string
}

func ValidateHistoricalDataRequest(req HistoricalDataRequest) error {
	if req.TenantID == "" {
		return errors.New("tenant id is required")
	}
	if req.Action != HistoricalActionPreview && req.Action != HistoricalActionArchive && req.Action != HistoricalActionDelete {
		return errors.New("unsupported historical data action")
	}
	if req.TargetID == "" && req.DeviceID == "" && req.PortID == "" {
		return errors.New("historical data resource is required")
	}
	if !req.End.After(req.Start) {
		return errors.New("historical data range end must be after start")
	}
	return nil
}

func PreviewHistoricalData(req HistoricalDataRequest, sampleStep time.Duration, bytesPerSample uint64) (HistoricalDataPreview, error) {
	if err := ValidateHistoricalDataRequest(req); err != nil {
		return HistoricalDataPreview{}, err
	}
	if sampleStep <= 0 {
		return HistoricalDataPreview{}, errors.New("sample step is required")
	}
	if bytesPerSample == 0 {
		bytesPerSample = 128
	}
	samples := expectedSampleCount(req.Start, req.End, sampleStep)
	return HistoricalDataPreview{
		Request:          req,
		EstimatedSamples: samples,
		EstimatedBytes:   uint64(samples) * bytesPerSample,
	}, nil
}

func CreateHistoricalDataOperation(id ID, req HistoricalDataRequest) (HistoricalDataOperation, error) {
	if id == "" {
		return HistoricalDataOperation{}, errors.New("historical operation id is required")
	}
	if req.Action != HistoricalActionArchive && req.Action != HistoricalActionDelete {
		return HistoricalDataOperation{}, errors.New("historical operation must be archive or delete")
	}
	if err := ValidateHistoricalDataRequest(req); err != nil {
		return HistoricalDataOperation{}, err
	}
	return HistoricalDataOperation{
		ID:      id,
		Request: req,
		Status:  "pending_manual_execution",
	}, nil
}
