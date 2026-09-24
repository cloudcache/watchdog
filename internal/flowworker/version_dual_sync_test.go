package flowworker

import (
	"context"
	"errors"
	"testing"
)

type fakeLegacyVersionSync struct {
	calls  int
	result RemoteVersionSyncResult
	err    error
}

func (s *fakeLegacyVersionSync) SyncOnce(context.Context, uint32) (RemoteVersionSyncResult, error) {
	s.calls++
	return s.result, s.err
}

type fakeDeploymentVersionSync struct {
	calls   int
	results []RemoteDeploymentSyncResult
	errors  []error
}

func (s *fakeDeploymentVersionSync) SyncOnce(context.Context, uint64) (RemoteDeploymentSyncResult, error) {
	index := s.calls
	s.calls++
	var result RemoteDeploymentSyncResult
	if index < len(s.results) {
		result = s.results[index]
	}
	if index < len(s.errors) {
		return result, s.errors[index]
	}
	return result, nil
}

func newDualRemoteVersionSyncForTest(legacy legacyVersionSynchronizer, deployments deploymentVersionSynchronizer, active bool) *DualRemoteVersionSync {
	return &DualRemoteVersionSync{legacy: legacy, deployments: deployments, v2Active: active}
}

func TestDualRemoteVersionSyncFallsBackBeforeFirstV2Deployment(t *testing.T) {
	legacy := &fakeLegacyVersionSync{result: RemoteVersionSyncResult{PreviousVersion: 3, HighestVersion: 4, Installed: 1}}
	deployments := &fakeDeploymentVersionSync{errors: []error{ErrDeploymentRemoteUnsupported}}
	syncer := newDualRemoteVersionSyncForTest(legacy, deployments, false)

	result, err := syncer.SyncOnce(context.Background(), 3)
	if err != nil || result.HighestVersion != 4 || legacy.calls != 1 || deployments.calls != 1 {
		t.Fatalf("result=%+v legacy=%d deployments=%d error=%v", result, legacy.calls, deployments.calls, err)
	}
}

func TestDualRemoteVersionSyncPermanentlySwitchesToV2(t *testing.T) {
	legacy := &fakeLegacyVersionSync{result: RemoteVersionSyncResult{PreviousVersion: 7, HighestVersion: 8, Installed: 1}}
	deployments := &fakeDeploymentVersionSync{results: []RemoteDeploymentSyncResult{
		{PreviousGeneration: 7, HighestGeneration: 9, Installed: 1},
		{PreviousGeneration: 9, HighestGeneration: 9},
	}}
	syncer := newDualRemoteVersionSyncForTest(legacy, deployments, false)

	first, err := syncer.SyncOnce(context.Background(), 7)
	if err != nil || first.HighestVersion != 9 || first.Installed != 1 {
		t.Fatalf("first=%+v error=%v", first, err)
	}
	second, err := syncer.SyncOnce(context.Background(), first.HighestVersion)
	if err != nil || second.HighestVersion != 9 || legacy.calls != 0 || deployments.calls != 2 {
		t.Fatalf("second=%+v legacy=%d deployments=%d error=%v", second, legacy.calls, deployments.calls, err)
	}
}

func TestDualRemoteVersionSyncDoesNotHideV2Failure(t *testing.T) {
	want := errors.New("invalid signed deployment")
	legacy := &fakeLegacyVersionSync{}
	deployments := &fakeDeploymentVersionSync{errors: []error{want}}
	syncer := newDualRemoteVersionSyncForTest(legacy, deployments, false)

	result, err := syncer.SyncOnce(context.Background(), 2)
	if !errors.Is(err, want) || result.HighestVersion != 2 || legacy.calls != 0 {
		t.Fatalf("result=%+v legacy=%d error=%v", result, legacy.calls, err)
	}
}
