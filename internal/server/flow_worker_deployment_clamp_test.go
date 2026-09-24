package server

import (
	"testing"
	"time"
)

// TestClampFlowDeploymentEffectiveFrom covers the auto-publish-on-activation
// regression: an activation is effective immediately, so its effective_from is
// already in the past by the time the worker deployment is built, and the
// deployment API rejects a past effective_from. The clamp must move a zero or
// past value forward to the next UTC minute boundary while preserving a future
// one.
func TestClampFlowDeploymentEffectiveFrom(t *testing.T) {
	now := time.Date(2026, 9, 21, 6, 20, 30, 0, time.UTC)
	nextMinute := time.Date(2026, 9, 21, 6, 21, 0, 0, time.UTC)

	if got := clampFlowDeploymentEffectiveFrom(time.Time{}, now); !got.Equal(nextMinute) {
		t.Fatalf("zero effective_from: got %v, want %v", got, nextMinute)
	}
	// The exact failure we hit in production: activation effective_from 06:14,
	// deployment built at 06:20 -> must clamp forward, not reject.
	past := time.Date(2026, 9, 21, 6, 14, 0, 0, time.UTC)
	if got := clampFlowDeploymentEffectiveFrom(past, now); !got.Equal(nextMinute) {
		t.Fatalf("past effective_from: got %v, want %v", got, nextMinute)
	}
	// A genuinely future request is preserved (in UTC).
	future := time.Date(2026, 9, 21, 7, 0, 0, 0, time.UTC)
	if got := clampFlowDeploymentEffectiveFrom(future, now); !got.Equal(future) {
		t.Fatalf("future effective_from: got %v, want %v", got, future)
	}
	// Non-minute values are rounded forward rather than escaping the helper and
	// failing later in deployment validation.
	futureSubMinute := time.Date(2026, 9, 21, 7, 0, 1, 0, time.UTC)
	futureCeiling := time.Date(2026, 9, 21, 7, 1, 0, 0, time.UTC)
	if got := clampFlowDeploymentEffectiveFrom(futureSubMinute, now); !got.Equal(futureCeiling) {
		t.Fatalf("future sub-minute effective_from: got %v, want %v", got, futureCeiling)
	}
	// The clamped result must satisfy the deployment validator's own rule:
	// a whole UTC minute that is not before the current minute.
	got := clampFlowDeploymentEffectiveFrom(past, now)
	if !flowUTCMinute(got) {
		t.Fatalf("clamped value %v is not a whole UTC minute", got)
	}
	if got.Before(now.UTC().Truncate(time.Minute)) {
		t.Fatalf("clamped value %v is before the current minute", got)
	}
}
