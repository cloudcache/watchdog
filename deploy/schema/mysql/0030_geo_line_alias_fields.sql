-- P3c region groups (大区组): explicit CIDR members (unioned with the selector)
-- and exclude_line_ids (lines whose effective set is subtracted, recursively).
-- The include selector's operator_ids/asns live in the existing geo_selector JSON.
ALTER TABLE geo_lines
	ADD COLUMN members JSON NULL AFTER geo_selector,
	ADD COLUMN exclude_line_ids JSON NULL AFTER members;
