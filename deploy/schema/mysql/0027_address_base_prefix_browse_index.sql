-- Imported base-prefix browse orders by (family, ip_start, prefix_length, id)
-- within one import. Without an index covering that full ordering MySQL filesorts
-- the whole import (millions of rows) for every page — ~40s on a 6M-row import.
-- This covering index makes it an ordered range scan + LIMIT (tens of ms).
ALTER TABLE address_base_prefixes
  ADD INDEX idx_address_base_browse (import_id, family, ip_start, prefix_length, id),
  ALGORITHM=INPLACE, LOCK=NONE;
