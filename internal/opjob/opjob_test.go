package opjob

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type failureCaptureRepository struct {
	code   string
	detail string
	retry  bool
}

func (*failureCaptureRepository) Enqueue(context.Context, Job) (Job, error) { return Job{}, nil }
func (*failureCaptureRepository) Get(context.Context, string) (Job, error) {
	return Job{}, sql.ErrNoRows
}
func (*failureCaptureRepository) List(context.Context, Filter) ([]Job, error) { return nil, nil }
func (*failureCaptureRepository) LeaseNext(context.Context, string, string, time.Duration) (Job, error) {
	return Job{}, sql.ErrNoRows
}
func (*failureCaptureRepository) Heartbeat(context.Context, string, string, time.Duration, uint64, json.RawMessage) (bool, error) {
	return false, nil
}
func (*failureCaptureRepository) CompleteSucceeded(context.Context, string, string, string) error {
	return nil
}
func (*failureCaptureRepository) CompleteCanceled(context.Context, string, string) error { return nil }
func (r *failureCaptureRepository) CompleteFailed(_ context.Context, _, _, code, detail string, retry bool, _ time.Time) error {
	r.code, r.detail, r.retry = code, detail, retry
	return nil
}
func (*failureCaptureRepository) RequestCancel(context.Context, string) error { return nil }

func TestEncodeDecodePayloadRoundTrip(t *testing.T) {
	type p struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	raw, err := EncodePayload(3, p{Name: "geo", Count: 7})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out p
	if err := DecodePayload(raw, 3, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Name != "geo" || out.Count != 7 {
		t.Fatalf("round-trip mismatch: %+v", out)
	}
}

func TestDecodePayloadRejectsWrongSchemaVersionTerminally(t *testing.T) {
	raw, err := EncodePayload(1, map[string]string{"a": "b"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out map[string]string
	err = DecodePayload(raw, 2, &out)
	if err == nil {
		t.Fatal("expected version mismatch error")
	}
	if !IsTerminalError(err) {
		t.Fatalf("version mismatch must be terminal, got %v", err)
	}
}

func TestDecodePayloadMalformedIsTerminal(t *testing.T) {
	var out map[string]string
	if err := DecodePayload([]byte("not json"), 1, &out); !IsTerminalError(err) {
		t.Fatalf("malformed envelope must be terminal, got %v", err)
	}
}

func TestTerminalErrorWrapUnwrap(t *testing.T) {
	sentinel := errors.New("bad payload")
	wrapped := TerminalError(sentinel)
	if !IsTerminalError(wrapped) {
		t.Fatal("wrapped error should be terminal")
	}
	if !errors.Is(wrapped, sentinel) {
		t.Fatal("terminal error must unwrap to the cause")
	}
	if TerminalError(nil) != nil {
		t.Fatal("TerminalError(nil) must be nil")
	}
	if IsTerminalError(errors.New("plain")) {
		t.Fatal("a plain error is not terminal")
	}
}

func TestRetryBackoffDoublesAndCaps(t *testing.T) {
	w := &Worker{RetryBase: time.Second}
	// attempt 1 -> base; grows ~2x per attempt; capped at 10m.
	if got := w.retryBackoff(1); got != time.Second {
		t.Fatalf("attempt 1 backoff = %v, want 1s", got)
	}
	if got := w.retryBackoff(3); got != 4*time.Second {
		t.Fatalf("attempt 3 backoff = %v, want 4s", got)
	}
	if got := w.retryBackoff(100); got != 10*time.Minute {
		t.Fatalf("attempt 100 backoff = %v, want 10m cap", got)
	}
}

func TestReporterReportNilIsNoOp(t *testing.T) {
	var r *Reporter
	if err := r.Report(nil, 5, nil); err != nil {
		t.Fatalf("nil reporter Report must be a no-op, got %v", err)
	}
}

func TestWorkerTerminalFailureCallbackReceivesPersistedDetail(t *testing.T) {
	repo := &failureCaptureRepository{}
	var callbackCode, callbackDetail string
	w := &Worker{
		Repo: repo,
		OnTerminalFailure: func(_ Job, code, detail string) {
			callbackCode, callbackDetail = code, detail
		},
	}
	job := Job{ID: "job-1", LeaseToken: "lease-1", AttemptCount: 2}
	w.finishAttempt(context.Background(), job, false, "", TerminalError(errors.New("invalid payload")))

	wantDetail := "attempt 2: invalid payload"
	if repo.code != CodeTerminal || repo.detail != wantDetail || repo.retry {
		t.Fatalf("persisted failure = code %q detail %q retry %v", repo.code, repo.detail, repo.retry)
	}
	if callbackCode != repo.code || callbackDetail != repo.detail {
		t.Fatalf("callback failure = code %q detail %q, want persisted values", callbackCode, callbackDetail)
	}
}
