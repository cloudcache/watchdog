-- Device-reported sysLocation is discovery inventory. It is distinct from the
-- administrator-owned locations.id site/POP classification.

ALTER TABLE devices
  ADD COLUMN sys_location VARCHAR(255) NOT NULL DEFAULT '' AFTER sys_descr;
