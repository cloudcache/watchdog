// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowvpn

import (
	"errors"
	"sync"
)

// InstalledRuleSet is one verified immutable VPN publication held by a scorer.
// CompiledRuleSet contains only immutable slices after compilation, so readers
// can safely keep a returned value while a later publication is installed.
type InstalledRuleSet struct {
	Rules    CompiledRuleSet
	Metadata RuleSetBundleMetadata
}

// RuleSetCatalog provides last-known-good installation semantics. Callers must
// decode and verify a publication before Install; a failed decode never reaches
// this catalog and therefore cannot replace the current scorer.
type RuleSetCatalog struct {
	mu      sync.RWMutex
	current *InstalledRuleSet
}

func NewRuleSetCatalog() *RuleSetCatalog { return &RuleSetCatalog{} }

func (c *RuleSetCatalog) Install(rules CompiledRuleSet, metadata RuleSetBundleMetadata) error {
	if c == nil || rules.version == "" || metadata.SnapshotID == "" || metadata.Checksum == "" ||
		metadata.SnapshotID != rules.version || metadata.RuleCount != len(rules.rules) {
		return errors.New("verified VPN rule-set publication is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current != nil && c.current.Metadata.SnapshotID == metadata.SnapshotID {
		if c.current.Metadata.Checksum != metadata.Checksum {
			return errors.New("VPN rule-set snapshot checksum changed")
		}
		return nil
	}
	installed := InstalledRuleSet{Rules: rules, Metadata: metadata}
	c.current = &installed
	return nil
}

func (c *RuleSetCatalog) Current() (InstalledRuleSet, bool) {
	if c == nil {
		return InstalledRuleSet{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.current == nil {
		return InstalledRuleSet{}, false
	}
	return *c.current, true
}

// Restore is used only when the control-plane installed ACK fails after an
// otherwise valid swap. The scoring loop does not run until installation
// returns, so restoring here prevents an unacknowledged generation from being
// used for a window.
func (c *RuleSetCatalog) Restore(previous InstalledRuleSet, existed bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !existed {
		c.current = nil
		return
	}
	restored := previous
	c.current = &restored
}
