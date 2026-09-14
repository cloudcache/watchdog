-- 0001 already created user_preferences with the legacy prefs_json column,
-- therefore 0026's CREATE TABLE IF NOT EXISTS could not converge its shape.
-- Rename the stored JSON and add the optimistic-concurrency version used by
-- GET/PUT /api/v1/me/preferences.
ALTER TABLE user_preferences
  CHANGE COLUMN prefs_json settings_json JSON NOT NULL,
  ADD COLUMN row_version BIGINT UNSIGNED NOT NULL DEFAULT 1 AFTER settings_json;
