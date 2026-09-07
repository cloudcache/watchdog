package watchdog

import (
	"context"
	"errors"
	"testing"
	"time"
)

// terminalFailureRepo captures the CompleteOperationJobFailed outcome and lets a
// test force the durable write to fail. Every other repository method is left
// nil (the embedded interface): finishAttempt's failure branch touches only
// CompleteOperationJobFailed, so a call to anything else is a test bug and panics.
type terminalFailureRepo struct {
	OperationJobRepository
	gotCode  string
	gotRetry bool
	writeErr error
}

func (r *terminalFailureRepo) CompleteOperationJobFailed(_ context.Context, _ ID, _, errorCode, _ string, retry bool, _ time.Time) error {
	r.gotCode = errorCode
	r.gotRetry = retry
	return r.writeErr
}

// TestWorkerOnTerminalFailureFiresOnlyWhenDurablyTerminal proves the hook fires
// exactly when a job is durably recorded as terminal: a non-retryable error, or
// a retryable error that exhausted its budget — but not while retries remain,
// and not when the terminal write itself failed (the lease will expire and
// another worker retries, so the job is not yet terminal).
func TestWorkerOnTerminalFailureFiresOnlyWhenDurablyTerminal(t *testing.T) {
	const maxAttempts = 5
	cases := []struct {
		name      string
		attempt   uint32
		handler   error
		writeErr  error
		wantFired bool
		wantCode  string
	}{
		{"non-retryable error is terminal", 1, TerminalJobError(errors.New("bad payload")), nil, true, OperationJobCodeTerminal},
		{"retryable error out of budget is terminal", maxAttempts, errors.New("MEMORY_LIMIT_EXCEEDED"), nil, true, "HANDLER_FAILED"},
		{"retryable error with budget left is not terminal", 1, errors.New("MEMORY_LIMIT_EXCEEDED"), nil, false, ""},
		{"terminal write failure is not counted", 1, TerminalJobError(errors.New("bad payload")), errors.New("db unavailable"), false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &terminalFailureRepo{writeErr: tc.writeErr}
			fired := false
			gotCode := ""
			worker := &OperationJobWorker{
				Repo:        repo,
				JobType:     "flow_rollup",
				MaxAttempts: maxAttempts,
				RetryBase:   time.Nanosecond,
				OnTerminalFailure: func(_ OperationJob, code string) {
					fired = true
					gotCode = code
				},
			}
			job := OperationJob{ID: "job-1", LeaseToken: "lease", AttemptCount: tc.attempt}
			worker.finishAttempt(context.Background(), job, false, "", tc.handler)

			if fired != tc.wantFired {
				t.Fatalf("hook fired=%v, want %v (repo retry=%v code=%q)", fired, tc.wantFired, repo.gotRetry, repo.gotCode)
			}
			if tc.wantFired && gotCode != tc.wantCode {
				t.Fatalf("hook code=%q, want %q", gotCode, tc.wantCode)
			}
		})
	}
}

// TestWorkerFinishAttemptWithoutHookIsSafe proves finishAttempt does not require
// an OnTerminalFailure hook: the nil default must not panic on a terminal finish.
func TestWorkerFinishAttemptWithoutHookIsSafe(t *testing.T) {
	worker := &OperationJobWorker{Repo: &terminalFailureRepo{}, MaxAttempts: 1, RetryBase: time.Nanosecond}
	job := OperationJob{ID: "job-2", LeaseToken: "lease", AttemptCount: 1}
	worker.finishAttempt(context.Background(), job, false, "", errors.New("boom"))
}
