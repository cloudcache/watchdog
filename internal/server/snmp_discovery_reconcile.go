package server

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// The SNMP discovery reconcile loop is what makes "bind an SNMP profile to a
// network device" actually start collection. Polling only ever sees devices
// that already have collection recipes, and recipes are created only by
// discovery; nothing else re-runs discovery. This loop closes that gap: it
// discovers network devices that have a profile but no enabled recipes, and —
// like LibreNMS separating discovery from polling — re-discovers healthy
// devices every RediscoverInterval so new interfaces/sensors are picked up.
const (
	snmpDiscoverScanInterval = time.Minute
	snmpDiscoverBatchLimit   = 50
	snmpDiscoverConcurrency  = 4
	snmpDiscoverFailBackoff  = 15 * time.Minute
)

func (s *Server) startSNMPDiscoveryReconcile() {
	if !s.cfg.SNMP.AutoDiscover {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.snmpDiscoverCancel = cancel
	go s.runSNMPDiscoveryReconcileLoop(ctx)
}

func (s *Server) runSNMPDiscoveryReconcileLoop(ctx context.Context) {
	backoff := &snmpDiscoveryBackoff{until: map[string]time.Time{}}
	ticker := time.NewTicker(snmpDiscoverScanInterval)
	defer ticker.Stop()
	for {
		if err := s.reconcileSNMPDiscoveryOnce(ctx, backoff); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("SNMP discovery reconcile pass failed; retrying next interval: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// snmpDiscoveryBackoff keeps failing devices from being retried every scan. It
// is intentionally in-memory: on restart a device is simply retried once, which
// is the desired behaviour.
type snmpDiscoveryBackoff struct {
	mu    sync.Mutex
	until map[string]time.Time
}

func (b *snmpDiscoveryBackoff) ready(id string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	next, ok := b.until[id]
	return !ok || !now.Before(next)
}

func (b *snmpDiscoveryBackoff) fail(id string, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.until[id] = now.Add(snmpDiscoverFailBackoff)
}

func (b *snmpDiscoveryBackoff) ok(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.until, id)
}

// dueSNMPDiscoveryDevices returns network devices that have an SNMP profile but
// need discovery: either they have no enabled recipe (never discovered → the
// MAX is NULL, COALESCEd to the DATETIME floor) or their newest recipe was
// discovered before the re-discovery threshold. Never-discovered devices sort
// first.
func (s *Server) dueSNMPDiscoveryDevices(ctx context.Context, threshold time.Time, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT d.id FROM devices d
		WHERE d.kind='network' AND d.disabled=0 AND d.snmp_profile_id IS NOT NULL
		  AND COALESCE((SELECT MAX(r.discovered_at) FROM snmp_collection_recipes r WHERE r.device_id=d.id AND r.enabled=1), '1000-01-01') < ?
		ORDER BY COALESCE((SELECT MAX(r.discovered_at) FROM snmp_collection_recipes r WHERE r.device_id=d.id AND r.enabled=1), '1000-01-01')
		LIMIT ?`, threshold, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var due []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		due = append(due, id)
	}
	return due, rows.Err()
}

func (s *Server) reconcileSNMPDiscoveryOnce(ctx context.Context, backoff *snmpDiscoveryBackoff) error {
	rediscover := s.cfg.SNMP.RediscoverInterval
	if rediscover <= 0 {
		rediscover = 6 * time.Hour
	}
	threshold := time.Now().UTC().Add(-rediscover)
	due, err := s.dueSNMPDiscoveryDevices(ctx, threshold, snmpDiscoverBatchLimit)
	if err != nil {
		return err
	}

	now := time.Now()
	sem := make(chan struct{}, snmpDiscoverConcurrency)
	var wg sync.WaitGroup
	for _, id := range due {
		if ctx.Err() != nil {
			break
		}
		if !backoff.ready(id, now) {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(id string) {
			defer wg.Done()
			defer func() { <-sem }()
			device, err := s.readDeviceByID(ctx, id)
			if err != nil {
				return
			}
			// "" actor → the discovery is system-initiated (updated_by NULL,
			// audit is best-effort).
			if _, _, err := s.discoverAndImport(ctx, device, ""); err != nil {
				backoff.fail(id, time.Now())
				log.Printf("SNMP auto-discovery for device %s failed: %v", id, err)
				return
			}
			backoff.ok(id)
		}(id)
	}
	wg.Wait()
	return ctx.Err()
}
