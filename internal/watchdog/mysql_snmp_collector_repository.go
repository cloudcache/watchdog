package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

func (s *MySQLStore) ListSNMPOSDefinitions(ctx context.Context) ([]SNMPCollectorOSDefinition, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, os_name, os_group, vendor, class, definition_json, source, source_version, created_at, updated_at
		FROM snmp_os_definitions
		ORDER BY source, os_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var definitions []SNMPCollectorOSDefinition
	for rows.Next() {
		definition, err := scanSNMPCollectorOSDefinition(rows)
		if err != nil {
			return nil, err
		}
		definitions = append(definitions, definition)
	}
	return definitions, rows.Err()
}

func (s *MySQLStore) UpsertSNMPOSDefinition(ctx context.Context, definition SNMPCollectorOSDefinition) (SNMPCollectorOSDefinition, error) {
	if definition.ID == "" {
		definition.ID = collectorStableID("snmp-os", definition.Source, definition.OSName)
	}
	if definition.Source == "" {
		definition.Source = "librenms"
	}
	definitionJSON, err := encodeAnyMapJSON(definition.Definition)
	if err != nil {
		return definition, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO snmp_os_definitions (
			id, os_name, os_group, vendor, class, definition_json, source, source_version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			os_group = VALUES(os_group),
			vendor = VALUES(vendor),
			class = VALUES(class),
			definition_json = VALUES(definition_json),
			source_version = VALUES(source_version),
			updated_at = CURRENT_TIMESTAMP(3)
	`, definition.ID, definition.OSName, definition.OSGroup, definition.Vendor, definition.Class, definitionJSON, definition.Source, definition.SourceVersion)
	return definition, err
}

func (s *MySQLStore) ListSNMPModuleDefinitions(ctx context.Context, moduleType SNMPCollectorModuleType) ([]SNMPCollectorModuleDefinition, error) {
	query := `
		SELECT id, module_name, module_type, definition_json, source, source_version, created_at, updated_at
		FROM snmp_module_definitions
	`
	args := []any{}
	if moduleType != "" {
		query += " WHERE module_type = ?"
		args = append(args, moduleType)
	}
	query += " ORDER BY module_type, source, module_name"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var definitions []SNMPCollectorModuleDefinition
	for rows.Next() {
		definition, err := scanSNMPCollectorModuleDefinition(rows)
		if err != nil {
			return nil, err
		}
		definitions = append(definitions, definition)
	}
	return definitions, rows.Err()
}

func (s *MySQLStore) UpsertSNMPModuleDefinition(ctx context.Context, definition SNMPCollectorModuleDefinition) (SNMPCollectorModuleDefinition, error) {
	if definition.ID == "" {
		definition.ID = collectorStableID("snmp-module", definition.Source, string(definition.ModuleType), definition.ModuleName)
	}
	if definition.Source == "" {
		definition.Source = "librenms"
	}
	definitionJSON, err := encodeAnyMapJSON(definition.Definition)
	if err != nil {
		return definition, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO snmp_module_definitions (
			id, module_name, module_type, definition_json, source, source_version
		) VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			definition_json = VALUES(definition_json),
			source_version = VALUES(source_version),
			updated_at = CURRENT_TIMESTAMP(3)
	`, definition.ID, definition.ModuleName, definition.ModuleType, definitionJSON, definition.Source, definition.SourceVersion)
	return definition, err
}

func (s *MySQLStore) UpsertSNMPDeviceModule(ctx context.Context, module SNMPCollectorDeviceModule) (SNMPCollectorDeviceModule, error) {
	if module.ID == "" {
		module.ID = collectorStableID("snmp-device-module", string(module.TenantID), string(module.DeviceID), module.ModuleName)
	}
	metadataJSON, err := encodeStringMapJSON(module.Metadata)
	if err != nil {
		return module, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO snmp_device_modules (
			id, tenant_id, device_id, module_name, discovery_enabled, polling_enabled,
			discovery_status, polling_status, last_discovered_at, last_polled_at, last_error, metadata_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			discovery_enabled = VALUES(discovery_enabled),
			polling_enabled = VALUES(polling_enabled),
			discovery_status = VALUES(discovery_status),
			polling_status = VALUES(polling_status),
			last_discovered_at = VALUES(last_discovered_at),
			last_polled_at = VALUES(last_polled_at),
			last_error = VALUES(last_error),
			metadata_json = VALUES(metadata_json),
			updated_at = CURRENT_TIMESTAMP(3)
	`, module.ID, module.TenantID, module.DeviceID, module.ModuleName, module.DiscoveryEnabled, module.PollingEnabled, defaultString(string(module.DiscoveryStatus), string(SNMPCollectorModuleUnknown)), defaultString(string(module.PollingStatus), string(SNMPCollectorModuleUnknown)), nullTime(module.LastDiscoveredAt), nullTime(module.LastPolledAt), module.LastError, metadataJSON)
	return module, err
}

func (s *MySQLStore) UpsertSNMPStateTranslation(ctx context.Context, translation SNMPStateTranslation) (SNMPStateTranslation, error) {
	if translation.ID == "" {
		translation.ID = collectorStableID("snmp-state", translation.Source, translation.Name)
	}
	if translation.Source == "" {
		translation.Source = "librenms"
	}
	statesJSON, err := json.Marshal(translation.States)
	if err != nil {
		return translation, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO snmp_state_translations (
			id, name, source, states_json
		) VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			states_json = VALUES(states_json),
			updated_at = CURRENT_TIMESTAMP(3)
	`, translation.ID, translation.Name, translation.Source, statesJSON)
	return translation, err
}

func (s *MySQLStore) UpsertSNMPCollectionRecipes(ctx context.Context, recipes []SNMPCollectionRecipe) error {
	for _, recipe := range recipes {
		if recipe.ID == "" {
			recipe.ID = collectorStableID("snmp-recipe", string(recipe.TenantID), string(recipe.DeviceID), recipe.ModuleName, string(recipe.EntityType), string(recipe.EntityID), recipe.MetricName, recipe.OIDIndex, recipe.ContextName)
		}
		if recipe.SampleIntervalSeconds == 0 {
			recipe.SampleIntervalSeconds = 60
		}
		now := time.Now().UTC()
		if recipe.DiscoveredAt.IsZero() {
			recipe.DiscoveredAt = now
		}
		if recipe.LastSeenAt.IsZero() {
			recipe.LastSeenAt = now
		}
		if recipe.PollerType == "" {
			recipe.PollerType = "snmp"
		}
		labelsJSON, err := encodeStringMapJSON(recipe.Labels)
		if err != nil {
			return err
		}
		optionsJSON, err := encodeStringMapJSON(recipe.Options)
		if err != nil {
			return err
		}
		_, err = s.db.ExecContext(ctx, `
			INSERT INTO snmp_collection_recipes (
				id, tenant_id, device_id, entity_type, entity_id, module_name, metric_name, value_type,
				oid, numeric_oid, oid_index, mib, context_name, poller_type, divisor, multiplier,
				user_func, state_map_id, unit, sample_interval_seconds, labels_json, options_json,
				enabled, discovered_at, last_seen_at, last_polled_at, last_error
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				value_type = VALUES(value_type),
				oid = VALUES(oid),
				numeric_oid = VALUES(numeric_oid),
				mib = VALUES(mib),
				poller_type = VALUES(poller_type),
				divisor = VALUES(divisor),
				multiplier = VALUES(multiplier),
				user_func = VALUES(user_func),
				state_map_id = VALUES(state_map_id),
				unit = VALUES(unit),
				sample_interval_seconds = VALUES(sample_interval_seconds),
				labels_json = VALUES(labels_json),
				options_json = VALUES(options_json),
				enabled = VALUES(enabled),
				last_seen_at = VALUES(last_seen_at),
				last_error = VALUES(last_error),
				updated_at = CURRENT_TIMESTAMP(3)
		`, recipe.ID, recipe.TenantID, recipe.DeviceID, recipe.EntityType, recipe.EntityID, recipe.ModuleName, recipe.MetricName, recipe.ValueType, recipe.OID, recipe.NumericOID, recipe.OIDIndex, recipe.MIB, recipe.ContextName, recipe.PollerType, nullFloat(recipe.Divisor, recipe.HasDivisor), nullFloat(recipe.Multiplier, recipe.HasMultiplier), recipe.UserFunc, nullID(recipe.StateMapID), recipe.Unit, recipe.SampleIntervalSeconds, labelsJSON, optionsJSON, recipe.Enabled, recipe.DiscoveredAt, recipe.LastSeenAt, nullTime(recipe.LastPolledAt), recipe.LastError)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *MySQLStore) ListDueSNMPCollectionRecipes(ctx context.Context, tenantID ID, limit int, now time.Time) ([]SNMPCollectionRecipe, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, device_id, entity_type, entity_id, module_name, metric_name, value_type,
		       oid, numeric_oid, oid_index, mib, context_name, poller_type, divisor, multiplier,
		       user_func, state_map_id, unit, sample_interval_seconds, labels_json, options_json,
		       enabled, discovered_at, last_seen_at, last_polled_at, last_error, created_at, updated_at
		FROM snmp_collection_recipes
		WHERE tenant_id = ?
		  AND enabled = 1
		  AND (
		    last_polled_at IS NULL
		    OR TIMESTAMPDIFF(SECOND, last_polled_at, ?) >= sample_interval_seconds
		  )
		ORDER BY last_polled_at IS NULL DESC, last_polled_at ASC, id ASC
		LIMIT ?
	`, tenantID, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var recipes []SNMPCollectionRecipe
	for rows.Next() {
		recipe, err := scanSNMPCollectionRecipe(rows)
		if err != nil {
			return nil, err
		}
		recipes = append(recipes, recipe)
	}
	return recipes, rows.Err()
}

func (s *MySQLStore) MarkSNMPRecipePollResult(ctx context.Context, recipeID ID, polledAt time.Time, lastError string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE snmp_collection_recipes
		SET last_polled_at = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ?
	`, polledAt, lastError, recipeID)
	return err
}

func (s *MySQLStore) ListDueSNMPDevices(ctx context.Context, tenantID ID, limit int, now time.Time) ([]ID, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT device_id
		FROM snmp_collection_recipes
		WHERE tenant_id = ?
		  AND enabled = 1
		  AND (
		    last_polled_at IS NULL
		    OR TIMESTAMPDIFF(SECOND, last_polled_at, ?) >= sample_interval_seconds
		  )
		ORDER BY device_id
		LIMIT ?
	`, tenantID, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deviceIDs []ID
	for rows.Next() {
		var id ID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		deviceIDs = append(deviceIDs, id)
	}
	return deviceIDs, rows.Err()
}

func (s *MySQLStore) ListSNMPCollectionRecipesByDevice(ctx context.Context, tenantID, deviceID ID) ([]SNMPCollectionRecipe, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, device_id, entity_type, entity_id, module_name, metric_name, value_type,
		       oid, numeric_oid, oid_index, mib, context_name, poller_type, divisor, multiplier,
		       user_func, state_map_id, unit, sample_interval_seconds, labels_json, options_json,
		       enabled, discovered_at, last_seen_at, last_polled_at, last_error, created_at, updated_at
		FROM snmp_collection_recipes
		WHERE tenant_id = ? AND device_id = ? AND enabled = 1
	`, tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var recipes []SNMPCollectionRecipe
	for rows.Next() {
		recipe, err := scanSNMPCollectionRecipe(rows)
		if err != nil {
			return nil, err
		}
		recipes = append(recipes, recipe)
	}
	return recipes, rows.Err()
}

func (s *MySQLStore) GetSNMPDeviceLastPolledAt(ctx context.Context, tenantID, deviceID ID) (time.Time, error) {
	var lastPolled sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT MAX(last_polled_at)
		FROM snmp_collection_recipes
		WHERE tenant_id = ? AND device_id = ? AND last_error = ''
	`, tenantID, deviceID).Scan(&lastPolled)
	if err != nil {
		return time.Time{}, err
	}
	if !lastPolled.Valid {
		return time.Time{}, sql.ErrNoRows
	}
	return lastPolled.Time, nil
}

func (s *MySQLStore) ListSNMPTrapHandlers(ctx context.Context) ([]SNMPTrapHandlerDefinition, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, trap_oid, handler_key, enabled, COALESCE(options_json, JSON_OBJECT()), created_at, updated_at
		FROM snmp_trap_handlers
		WHERE enabled = 1
		ORDER BY trap_oid
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var handlers []SNMPTrapHandlerDefinition
	for rows.Next() {
		handler, err := scanSNMPTrapHandlerDefinition(rows)
		if err != nil {
			return nil, err
		}
		handlers = append(handlers, handler)
	}
	return handlers, rows.Err()
}

func (s *MySQLStore) UpsertSNMPTrapHandler(ctx context.Context, handler SNMPTrapHandlerDefinition) (SNMPTrapHandlerDefinition, error) {
	if handler.ID == "" {
		handler.ID = collectorStableID("snmp-trap", handler.TrapOID)
	}
	optionsJSON, err := encodeStringMapJSON(handler.Options)
	if err != nil {
		return handler, err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO snmp_trap_handlers (
			id, trap_oid, handler_key, enabled, options_json
		) VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			handler_key = VALUES(handler_key),
			enabled = VALUES(enabled),
			options_json = VALUES(options_json),
			updated_at = CURRENT_TIMESTAMP(3)
	`, handler.ID, handler.TrapOID, handler.HandlerKey, handler.Enabled, optionsJSON)
	return handler, err
}

func (s *MySQLStore) CreateSNMPEvent(ctx context.Context, event SNMPEvent) error {
	if event.ID == "" {
		event.ID = collectorStableID("snmp-event", string(event.TenantID), string(event.DeviceID), event.EventType, event.OccurredAt.String(), event.Message)
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	rawJSON, err := encodeAnyMapJSON(event.Raw)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO snmp_events (
			id, tenant_id, device_id, entity_type, entity_id, source, severity, event_type, message, raw_json, occurred_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, event.ID, event.TenantID, event.DeviceID, event.EntityType, event.EntityID, event.Source, event.Severity, event.EventType, event.Message, rawJSON, event.OccurredAt)
	return err
}

// PruneSNMPCollectionRecipes deletes the device's recipes that are not part
// of the latest discovery, so removed entities stop being polled forever.
func (s *MySQLStore) PruneSNMPCollectionRecipes(ctx context.Context, tenantID, deviceID ID, keep []ID) (int64, error) {
	query := "DELETE FROM snmp_collection_recipes WHERE tenant_id = ? AND device_id = ?"
	args := []any{tenantID, deviceID}
	if len(keep) > 0 {
		placeholders := strings.Repeat("?,", len(keep))
		query += " AND id NOT IN (" + placeholders[:len(placeholders)-1] + ")"
		for _, id := range keep {
			args = append(args, id)
		}
	}
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *MySQLStore) ListSNMPEvents(ctx context.Context, tenantID, deviceID ID, limit int) ([]SNMPEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, device_id, entity_type, entity_id, source, severity, event_type, message, occurred_at
		FROM snmp_events
		WHERE tenant_id = ? AND device_id = ?
		ORDER BY occurred_at DESC
		LIMIT ?
	`, tenantID, deviceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []SNMPEvent
	for rows.Next() {
		var event SNMPEvent
		if err := rows.Scan(&event.ID, &event.TenantID, &event.DeviceID, &event.EntityType, &event.EntityID, &event.Source, &event.Severity, &event.EventType, &event.Message, &event.OccurredAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func scanSNMPCollectorOSDefinition(row rowScanner) (SNMPCollectorOSDefinition, error) {
	var definition SNMPCollectorOSDefinition
	var definitionJSON []byte
	if err := row.Scan(&definition.ID, &definition.OSName, &definition.OSGroup, &definition.Vendor, &definition.Class, &definitionJSON, &definition.Source, &definition.SourceVersion, &definition.CreatedAt, &definition.UpdatedAt); err != nil {
		return definition, err
	}
	values, err := decodeAnyMapJSON(definitionJSON)
	if err != nil {
		return definition, err
	}
	definition.Definition = values
	return definition, nil
}

func scanSNMPCollectorModuleDefinition(row rowScanner) (SNMPCollectorModuleDefinition, error) {
	var definition SNMPCollectorModuleDefinition
	var definitionJSON []byte
	if err := row.Scan(&definition.ID, &definition.ModuleName, &definition.ModuleType, &definitionJSON, &definition.Source, &definition.SourceVersion, &definition.CreatedAt, &definition.UpdatedAt); err != nil {
		return definition, err
	}
	values, err := decodeAnyMapJSON(definitionJSON)
	if err != nil {
		return definition, err
	}
	definition.Definition = values
	return definition, nil
}

func scanSNMPCollectionRecipe(row rowScanner) (SNMPCollectionRecipe, error) {
	var recipe SNMPCollectionRecipe
	var divisor, multiplier sql.NullFloat64
	var stateMapID sql.NullString
	var labelsJSON, optionsJSON []byte
	var lastPolledAt sql.NullTime
	err := row.Scan(
		&recipe.ID, &recipe.TenantID, &recipe.DeviceID, &recipe.EntityType, &recipe.EntityID,
		&recipe.ModuleName, &recipe.MetricName, &recipe.ValueType, &recipe.OID, &recipe.NumericOID,
		&recipe.OIDIndex, &recipe.MIB, &recipe.ContextName, &recipe.PollerType, &divisor, &multiplier,
		&recipe.UserFunc, &stateMapID, &recipe.Unit, &recipe.SampleIntervalSeconds, &labelsJSON, &optionsJSON,
		&recipe.Enabled, &recipe.DiscoveredAt, &recipe.LastSeenAt, &lastPolledAt, &recipe.LastError,
		&recipe.CreatedAt, &recipe.UpdatedAt,
	)
	if err != nil {
		return recipe, err
	}
	recipe.Divisor, recipe.HasDivisor = divisor.Float64, divisor.Valid
	recipe.Multiplier, recipe.HasMultiplier = multiplier.Float64, multiplier.Valid
	if stateMapID.Valid {
		recipe.StateMapID = ID(stateMapID.String)
	}
	if lastPolledAt.Valid {
		recipe.LastPolledAt = lastPolledAt.Time
	}
	labels, err := decodeStringMapJSON(labelsJSON)
	if err != nil {
		return recipe, err
	}
	options, err := decodeStringMapJSON(optionsJSON)
	if err != nil {
		return recipe, err
	}
	recipe.Labels = labels
	recipe.Options = options
	return recipe, nil
}

func scanSNMPTrapHandlerDefinition(row rowScanner) (SNMPTrapHandlerDefinition, error) {
	var handler SNMPTrapHandlerDefinition
	var optionsJSON []byte
	if err := row.Scan(&handler.ID, &handler.TrapOID, &handler.HandlerKey, &handler.Enabled, &optionsJSON, &handler.CreatedAt, &handler.UpdatedAt); err != nil {
		return handler, err
	}
	options, err := decodeStringMapJSON(optionsJSON)
	if err != nil {
		return handler, err
	}
	handler.Options = options
	return handler, nil
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func nullFloat(value float64, valid bool) any {
	if !valid {
		return nil
	}
	return value
}

func nullID(value ID) any {
	if value == "" {
		return nil
	}
	return value
}

var _ SNMPCollectorRepository = (*MySQLStore)(nil)
