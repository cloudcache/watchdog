package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/snmpch"
	"github.com/cloudcache/watchdog/internal/snmpdomain"
)

// SNMPCollectorRuntime is the KISS-03A production data path. It reads global
// device/profile/recipe configuration from MySQL, reuses the existing SNMP
// query engine and poll runner, and writes raw samples directly to ClickHouse.
type SNMPCollectorRuntime struct {
	db         *sql.DB
	clickHouse *flowch.NativeInserter
	store      *snmpch.Store
	repo       *snmpPollRepository
	runner     snmpdomain.PollRunner
	discovery  snmpDiscoveryRunner
	rollupMu   sync.Mutex
	lastRollup time.Time
}

func OpenSNMPCollectorRuntime(ctx context.Context, cfg Config, agentID string) (*SNMPCollectorRuntime, error) {
	return openSNMPCollectorRuntime(ctx, cfg, agentID, snmpdomain.NewGoSNMPQueryEngine())
}

func openSNMPCollectorRuntime(ctx context.Context, cfg Config, agentID string, query snmpdomain.QueryEngine) (*SNMPCollectorRuntime, error) {
	if query == nil {
		return nil, errors.New("SNMP query engine is required")
	}
	db, err := sql.Open("mysql", cfg.MySQL.DSN)
	if err != nil {
		return nil, fmt.Errorf("open MySQL: %w", err)
	}
	db.SetMaxOpenConns(40)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping MySQL: %w", err)
	}
	password, err := clickHousePassword(cfg.ClickHouse.PasswordFile)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	native, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{
		Address: cfg.ClickHouse.Address, Database: cfg.ClickHouse.Database,
		User: cfg.ClickHouse.Username, Password: password,
		ClientName: "watchdog-snmp-collector", OperationTimeout: cfg.ClickHouse.OperationTimeout,
		MaxConns: 8, MinConns: 1,
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store, err := snmpch.New(native)
	if err != nil {
		native.Close()
		_ = db.Close()
		return nil, err
	}
	if err := store.Ready(ctx); err != nil {
		native.Close()
		_ = db.Close()
		return nil, fmt.Errorf("SNMP ClickHouse schema is not migrated: %w", err)
	}
	discovery, err := newSNMPDiscoveryRunner(cfg.SNMP)
	if err != nil {
		native.Close()
		_ = db.Close()
		return nil, err
	}
	repo := &snmpPollRepository{db: db}
	return &SNMPCollectorRuntime{
		db: db, clickHouse: native, store: store, repo: repo, discovery: discovery,
		runner: snmpdomain.PollRunner{
			Recipes: repo, Devices: repo, Targets: repo, Profiles: repo,
			Poller: snmpdomain.Poller{
				Query:  query,
				Writer: snmpdomain.ClickHouseRawWriter{Store: store, AgentID: strings.TrimSpace(agentID)},
			},
			GlobalConcurrency: cfg.SNMP.PollConcurrency,
		},
	}, nil
}

func (r *SNMPCollectorRuntime) Close() error {
	if r == nil {
		return nil
	}
	if r.clickHouse != nil {
		r.clickHouse.Close()
	}
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

func (r *SNMPCollectorRuntime) RunDue(ctx context.Context, limit int) (snmpdomain.PollRunnerResult, error) {
	if r == nil || r.repo == nil {
		return snmpdomain.PollRunnerResult{}, errors.New("SNMP collector runtime is not initialized")
	}
	result, err := r.runner.RunDue(ctx, limit)
	if err != nil {
		return result, err
	}
	now := time.Now().UTC()
	closed := now.Truncate(5 * time.Minute).Add(-5 * time.Minute)
	r.rollupMu.Lock()
	defer r.rollupMu.Unlock()
	if closed.After(r.lastRollup) {
		if err := r.store.RebuildClosedInterfaceBucket(ctx, closed, now, uint64(now.UnixMilli())); err != nil {
			return result, err
		}
		r.lastRollup = closed
	}
	return result, nil
}

func (r *SNMPCollectorRuntime) RunLoop(ctx context.Context, interval time.Duration, limit int) error {
	if interval <= 0 {
		return errors.New("SNMP poll interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := r.RunDue(ctx, limit); err != nil && !errors.Is(err, context.Canceled) {
			// MySQL, the device, or ClickHouse may be temporarily unavailable. The
			// standalone collector is a long-running process: preserve backpressure
			// by finishing one pass at a time, report the failure, and retry on the
			// next bounded scheduler tick instead of silently stopping collection.
			log.Printf("SNMP poll pass failed; retrying on next interval: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *SNMPCollectorRuntime) DiscoverDevice(ctx context.Context, deviceID string) error {
	device, err := r.repo.GetDevice(ctx, strings.TrimSpace(deviceID))
	if err != nil {
		return err
	}
	target, err := r.repo.GetTarget(ctx, device.TargetID)
	if err != nil {
		return err
	}
	profile, err := r.repo.GetProfile(ctx, device.SNMPProfileID)
	if err != nil {
		return err
	}
	profile = snmpdomain.ApplyDeviceOverrides(profile, device)
	result, err := r.discovery.Discover(ctx, snmpdomain.DiscoveryRequest{
		TargetID: device.TargetID,
		Target:   snmpdomain.QueryTarget{Host: target.Host, Port: device.SNMPPort},
		Device:   device,
		Profile:  profile,
	})
	if err != nil {
		_, _ = r.db.ExecContext(ctx, "UPDATE devices SET status='down',status_reason=?,last_polled_at=UTC_TIMESTAMP(3),row_version=row_version+1 WHERE id=?", truncateUTF8(err.Error(), 64), deviceID)
		return err
	}
	server := &Server{db: r.db}
	_, err = server.importSNMPDiscovery(ctx, deviceID, "", result)
	return err
}

func clickHousePassword(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read ClickHouse password file: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

type snmpPollRepository struct{ db *sql.DB }

func (r *snmpPollRepository) ListDueDevices(ctx context.Context, limit int, now time.Time) ([]string, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := r.db.QueryContext(ctx, `SELECT r.device_id
		FROM snmp_collection_recipes r JOIN devices d ON d.id=r.device_id
		WHERE r.enabled=1 AND d.disabled=0 AND d.kind='network' AND d.snmp_profile_id IS NOT NULL
		  AND (r.last_polled_at IS NULL OR TIMESTAMPDIFF(SECOND,r.last_polled_at,?)>=r.sample_interval_seconds)
		GROUP BY r.device_id ORDER BY MIN(r.last_polled_at IS NOT NULL),MIN(r.last_polled_at),r.device_id LIMIT ?`, now.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
}

func (r *snmpPollRepository) ListRecipesByDevice(ctx context.Context, deviceID string) ([]snmpdomain.Recipe, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT id,device_id,entity_kind,entity_id,module_name,metric,value_kind,
		oid,numeric_oid,oid_index,mib,context_name,divisor,multiplier,sample_interval_seconds,
		labels_json,options_json,enabled,discovered_at,last_polled_at,last_error,created_at,updated_at
		FROM snmp_collection_recipes WHERE device_id=? AND enabled=1 ORDER BY context_name,numeric_oid,id`, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []snmpdomain.Recipe
	for rows.Next() {
		var recipe snmpdomain.Recipe
		var divisor, multiplier sql.NullFloat64
		var lastPolled sql.NullTime
		var labels, options []byte
		if err := rows.Scan(&recipe.ID, &recipe.DeviceID, &recipe.EntityType, &recipe.EntityID, &recipe.ModuleName, &recipe.MetricName, &recipe.ValueType,
			&recipe.OID, &recipe.NumericOID, &recipe.OIDIndex, &recipe.MIB, &recipe.ContextName, &divisor, &multiplier, &recipe.SampleIntervalSeconds,
			&labels, &options, &recipe.Enabled, &recipe.DiscoveredAt, &lastPolled, &recipe.LastError, &recipe.CreatedAt, &recipe.UpdatedAt); err != nil {
			return nil, err
		}
		recipe.HasDivisor, recipe.Divisor = divisor.Valid, divisor.Float64
		recipe.HasMultiplier, recipe.Multiplier = multiplier.Valid, multiplier.Float64
		if lastPolled.Valid {
			recipe.LastPolledAt = lastPolled.Time
		}
		if err := json.Unmarshal(labels, &recipe.Labels); err != nil {
			return nil, fmt.Errorf("decode SNMP recipe labels %s: %w", recipe.ID, err)
		}
		if err := json.Unmarshal(options, &recipe.Options); err != nil {
			return nil, fmt.Errorf("decode SNMP recipe options %s: %w", recipe.ID, err)
		}
		result = append(result, recipe)
	}
	return result, rows.Err()
}

func (r *snmpPollRepository) MarkRecipePollResult(ctx context.Context, recipeID string, polledAt time.Time, lastError string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE snmp_collection_recipes SET last_polled_at=?,last_error=? WHERE id=?`, polledAt.UTC(), truncateUTF8(lastError, 2048), recipeID)
	return err
}

func (r *snmpPollRepository) MarkDevicePollResult(ctx context.Context, deviceID string, polledAt time.Time, lastError string) error {
	status := "up"
	if lastError != "" {
		status = "down"
	}
	_, err := r.db.ExecContext(ctx, `UPDATE devices
		SET status=?,status_reason=?,last_polled_at=?,row_version=row_version+1
		WHERE id=?`, status, truncateUTF8(lastError, 64), polledAt.UTC(), deviceID)
	return err
}

func (r *snmpPollRepository) GetDevice(ctx context.Context, deviceID string) (snmpdomain.Device, error) {
	var result snmpdomain.Device
	var profileID sql.NullString
	var port sql.NullInt64
	var overrides []byte
	var uptime uint64
	var host string
	err := r.db.QueryRowContext(ctx, `SELECT id,host,vendor,model,platform,os,os_version,sys_object_id,sys_name,
		COALESCE(sys_descr,''),sys_location,uptime_seconds,snmp_profile_id,snmp_port,COALESCE(snmp_security_json,JSON_OBJECT()),updated_at
		FROM devices WHERE id=? AND kind='network' AND disabled=0`, deviceID).Scan(
		&result.ID, &host, &result.Vendor, &result.Model, &result.Platform, &result.OSName, &result.OSVersion,
		&result.SysObjectID, &result.SysName, &result.SysDescr, &result.SysLocation, &uptime, &profileID, &port, &overrides, &result.UpdatedAt)
	if err != nil {
		return result, err
	}
	result.TargetID = result.ID
	result.Uptime = time.Duration(uptime) * time.Second
	if profileID.Valid {
		result.SNMPProfileID = profileID.String
	}
	if port.Valid {
		result.SNMPPort = uint16(port.Int64)
	}
	if err := json.Unmarshal(overrides, &result.SNMPSecurity); err != nil {
		return result, err
	}
	return result, nil
}

func (r *snmpPollRepository) GetTarget(ctx context.Context, targetID string) (snmpdomain.Target, error) {
	var result snmpdomain.Target
	err := r.db.QueryRowContext(ctx, `SELECT id,COALESCE(NULLIF(display_name,''),NULLIF(sys_name,''),host),host,status,created_at,updated_at FROM devices WHERE id=?`, targetID).
		Scan(&result.ID, &result.Name, &result.Host, &result.Status, &result.CreatedAt, &result.UpdatedAt)
	return result, err
}

func (r *snmpPollRepository) GetProfile(ctx context.Context, profileID string) (snmpdomain.Profile, error) {
	var result snmpdomain.Profile
	var version string
	var security []byte
	var timeoutMS uint32
	err := r.db.QueryRowContext(ctx, `SELECT id,name,version,security_json,timeout_ms,retries,created_at,updated_at FROM snmp_profiles WHERE id=?`, profileID).
		Scan(&result.ID, &result.Name, &version, &security, &timeoutMS, &result.Retries, &result.CreatedAt, &result.UpdatedAt)
	if err != nil {
		return result, err
	}
	switch strings.ToLower(strings.TrimSpace(version)) {
	case "v1", "1":
		result.Version = snmpdomain.Version1
	case "v2c", "2c":
		result.Version = snmpdomain.Version2c
	case "v3", "3":
		result.Version = snmpdomain.Version3
	default:
		return result, fmt.Errorf("unsupported SNMP version %q", version)
	}
	result.Timeout = time.Duration(timeoutMS) * time.Millisecond
	if err := json.Unmarshal(security, &result.Security); err != nil {
		return result, err
	}
	return result, nil
}
