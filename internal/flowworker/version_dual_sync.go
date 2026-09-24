package flowworker

import (
	"context"
	"errors"
	"sync"
)

// RemoteVersionSynchronizer is the worker runtime contract used by the
// refresh loop. Both the legacy paired publication protocol and the v2
// per-worker deployment protocol report the same monotonic uint32 cursor
// because v2 generations are allocated above the retained legacy horizon.
type RemoteVersionSynchronizer interface {
	SyncOnce(context.Context, uint32) (RemoteVersionSyncResult, error)
}

type legacyVersionSynchronizer interface {
	SyncOnce(context.Context, uint32) (RemoteVersionSyncResult, error)
}

type deploymentVersionSynchronizer interface {
	SyncOnce(context.Context, uint64) (RemoteDeploymentSyncResult, error)
}

// DualRemoteVersionSync implements the v1/v2 migration boundary. Before a
// v2 deployment exists it checks the v2 endpoint and falls back to v1. Once a
// v2 deployment has been restored or installed it never installs v1 again,
// preventing a legacy publication from replacing an explicitly targeted
// worker deployment.
type DualRemoteVersionSync struct {
	mu          sync.Mutex
	legacy      legacyVersionSynchronizer
	deployments deploymentVersionSynchronizer
	v2Active    bool
}

func NewDualRemoteVersionSync(legacy *RemoteVersionSync, deployments *RemoteDeploymentSync, v2Active bool) (*DualRemoteVersionSync, error) {
	if legacy == nil || deployments == nil {
		return nil, errors.New("legacy and deployment synchronizers are required")
	}
	return &DualRemoteVersionSync{legacy: legacy, deployments: deployments, v2Active: v2Active}, nil
}

func (s *DualRemoteVersionSync) SyncOnce(ctx context.Context, cursor uint32) (RemoteVersionSyncResult, error) {
	if s == nil || ctx == nil {
		return RemoteVersionSyncResult{}, errors.New("dual enrichment synchronizer is not initialized")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	deployment, err := s.deployments.SyncOnce(ctx, uint64(cursor))
	if err != nil {
		if !s.v2Active && errors.Is(err, ErrDeploymentRemoteUnsupported) {
			return s.legacy.SyncOnce(ctx, cursor)
		}
		return RemoteVersionSyncResult{PreviousVersion: cursor, HighestVersion: cursor}, err
	}
	if s.v2Active || deployment.Installed != 0 || deployment.HighestGeneration > uint64(cursor) {
		s.v2Active = true
		return RemoteVersionSyncResult{
			PreviousVersion: cursor,
			HighestVersion:  uint32(deployment.HighestGeneration),
			Installed:       uint32(deployment.Installed),
		}, nil
	}
	return s.legacy.SyncOnce(ctx, cursor)
}

var _ RemoteVersionSynchronizer = (*RemoteVersionSync)(nil)
var _ RemoteVersionSynchronizer = (*DualRemoteVersionSync)(nil)
