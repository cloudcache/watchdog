// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
)

type wadsGeoCatalogState struct {
	active    string
	byVersion map[string]*wadsGeoPublication
}

type wadsGeoPublication struct {
	path          string
	version       string
	effectiveFrom time.Time
	rowsV4        uint64
	rowsV6        uint64
	operators     uint64
	index         *flowdimension.AddressSnapshotIndex
	labels        map[string]FlowGeoLabel
	items         []FlowGeoCatalogItem
}

// loadActiveFlowGeoWADS loads labels and lookup data from the same active WADS
// object used by Flow workers. No management database lookup occurs on a Flow
// query: the complete immutable dictionary is decoded once during startup.
func (s *Server) loadActiveFlowGeoWADS(ctx context.Context, geo *flowGeoService) error {
	if s == nil || s.addressPublisher == nil || geo == nil {
		return errors.New("address publication service is unavailable")
	}
	activation, err := s.addressPublisher.GetDimensionPublicationActivationAt(ctx, time.Now().UTC())
	if err != nil {
		return err
	}
	snapshot, err := s.addressPublisher.GetAddressDimensionSnapshot(ctx, activation.SnapshotID)
	if err != nil {
		return err
	}
	path, err := s.addressObjects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		return err
	}
	publication, err := loadWADSGeoPublication(path, snapshot.Checksum)
	if err != nil {
		return err
	}
	geo.installWADS(publication, true)
	geo.lastErr.Store(nil)
	return nil
}

func loadWADSGeoPublication(path, expectedChecksum string) (*wadsGeoPublication, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > flowDimensionObjectMax {
		return nil, errors.New("WADS object size is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(file, flowDimensionObjectMax+1))
	if err != nil {
		return nil, err
	}
	if len(data) < 4 || string(data[:4]) != "WADS" {
		return nil, errors.New("not a WADS address snapshot")
	}
	digest := sha256.Sum256(data)
	checksum := "sha256:" + hex.EncodeToString(digest[:])
	if expectedChecksum != "" && checksum != expectedChecksum {
		return nil, errors.New("WADS address snapshot checksum mismatch")
	}
	artifact, err := flowdimension.DecodeAddressSnapshot(data, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		return nil, err
	}
	index, err := flowdimension.DecodeAndCompileAddressSnapshot(data, checksum, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		return nil, err
	}
	labels, items, err := buildWADSGeoLabels(artifact)
	if err != nil {
		return nil, err
	}
	return &wadsGeoPublication{
		path: path, version: artifact.SnapshotID, effectiveFrom: artifact.EffectiveFrom,
		rowsV4: uint64(len(artifact.IPv4Ranges)), rowsV6: uint64(len(artifact.IPv6Ranges)), operators: uint64(len(artifact.Operators)),
		index: index, labels: labels, items: items,
	}, nil
}

func buildWADSGeoLabels(artifact flowdimension.AddressSnapshotArtifact) (map[string]FlowGeoLabel, []FlowGeoCatalogItem, error) {
	text := func(reference uint32) (string, error) {
		if int(reference) >= len(artifact.Strings) {
			return "", errors.New("WADS Geo node contains an invalid string reference")
		}
		return artifact.Strings[reference], nil
	}
	type nodeKey struct {
		namespace uint8
		id        string
	}
	type nodeValue struct {
		id, name, kind, parent string
		namespace              uint8
	}
	nodes := make(map[nodeKey]nodeValue, len(artifact.GeoNodes))
	for _, node := range artifact.GeoNodes {
		if !node.Enabled {
			continue
		}
		id, err := text(node.ID)
		if err != nil {
			return nil, nil, err
		}
		name, err := text(node.Name)
		if err != nil {
			return nil, nil, err
		}
		kind, err := text(node.Kind)
		if err != nil {
			return nil, nil, err
		}
		parent, err := text(node.ParentID)
		if err != nil {
			return nil, nil, err
		}
		nodes[nodeKey{namespace: node.Namespace, id: id}] = nodeValue{id: id, name: name, kind: kind, parent: parent, namespace: node.Namespace}
	}
	labels := make(map[string]FlowGeoLabel, len(nodes))
	items := make([]FlowGeoCatalogItem, 0, len(nodes))
	for _, node := range nodes {
		chain := make([]nodeValue, 0, 5)
		seen := make(map[string]struct{}, 5)
		current := node
		for {
			if _, exists := seen[current.id]; exists {
				return nil, nil, fmt.Errorf("WADS Geo parent cycle at %s", current.id)
			}
			seen[current.id] = struct{}{}
			chain = append(chain, current)
			if current.parent == "" {
				break
			}
			parent, exists := nodes[nodeKey{namespace: current.namespace, id: current.parent}]
			if !exists {
				return nil, nil, fmt.Errorf("WADS Geo parent %s is missing", current.parent)
			}
			current = parent
		}
		path := make([]FlowGeoPathNode, len(chain))
		breadcrumb := make([]string, len(chain))
		for index := range chain {
			entry := chain[len(chain)-1-index]
			path[index] = FlowGeoPathNode{ID: entry.id, Name: entry.name, Kind: entry.kind}
			breadcrumb[index] = entry.name
		}
		label := FlowGeoLabel{
			Code: node.id, Name: node.name, Kind: node.kind, ParentID: node.parent,
			Path: path, Breadcrumb: breadcrumb, Additive: true, Version: artifact.SnapshotID,
		}
		if existing, exists := labels[node.id]; exists && (existing.Name != label.Name || existing.Kind != label.Kind || existing.ParentID != label.ParentID) {
			return nil, nil, fmt.Errorf("WADS Geo id %s is ambiguous across namespaces", node.id)
		}
		labels[node.id] = label
	}
	for _, label := range labels {
		items = append(items, FlowGeoCatalogItem{
			ID: label.Code, Name: label.Name, Kind: label.Kind, ParentID: label.ParentID,
			Path: label.Path, Additive: label.Additive,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Kind != items[j].Kind {
			return items[i].Kind < items[j].Kind
		}
		if items[i].Name != items[j].Name {
			return items[i].Name < items[j].Name
		}
		return items[i].ID < items[j].ID
	})
	return labels, items, nil
}

func (s *flowGeoService) installWADS(publication *wadsGeoPublication, active bool) {
	if s == nil || publication == nil {
		return
	}
	for {
		current := s.wads.Load()
		if current == nil {
			current = &wadsGeoCatalogState{byVersion: map[string]*wadsGeoPublication{}}
		}
		next := &wadsGeoCatalogState{active: current.active, byVersion: make(map[string]*wadsGeoPublication, len(current.byVersion)+1)}
		for version, existing := range current.byVersion {
			next.byVersion[version] = existing
		}
		next.byVersion[publication.version] = publication
		if active {
			next.active = publication.version
		}
		if s.wads.CompareAndSwap(current, next) {
			return
		}
	}
}

func (p *wadsGeoPublication) catalog(level, parentID string, maximum int) (FlowGeoCatalogResult, error) {
	if p == nil {
		return FlowGeoCatalogResult{}, ErrFlowGeoNotLoaded
	}
	level = strings.TrimSpace(level)
	switch level {
	case "continent", "region", "country", "province", "city":
	default:
		return FlowGeoCatalogResult{}, errors.New("Geo level must be continent, region, country, province, or city")
	}
	items := make([]FlowGeoCatalogItem, 0)
	parentFound := parentID == ""
	for _, item := range p.items {
		if item.ID == parentID {
			parentFound = true
		}
		if item.Kind == level && item.ParentID == parentID {
			items = append(items, item)
			if len(items) > maximum {
				return FlowGeoCatalogResult{}, fmt.Errorf("Geo catalog result exceeds limit %d", maximum)
			}
		}
	}
	if !parentFound {
		return FlowGeoCatalogResult{}, fmt.Errorf("%w: %s", ErrFlowGeoNodeNotFound, parentID)
	}
	return FlowGeoCatalogResult{
		Version: p.version, EffectiveFrom: p.effectiveFrom, Level: level, ParentID: parentID,
		Items: items, Total: len(items),
	}, nil
}
