-- PLAT-04C forward fix: imported generations and active-slot history outlive
-- an individual operator account. The original RESTRICT references also made
-- tenant deletion fail because MySQL could not cascade users and imports in a
-- safe order. Preserve the generation while clearing the departed actor.
ALTER TABLE address_import_slots
  DROP FOREIGN KEY fk_address_import_slots_actor,
  MODIFY activated_by CHAR(26) NULL;
ALTER TABLE address_import_slots
  ADD CONSTRAINT fk_address_import_slots_actor
    FOREIGN KEY (activated_by) REFERENCES users(id) ON DELETE SET NULL;

ALTER TABLE address_imports
  DROP FOREIGN KEY fk_address_imports_creator,
  MODIFY created_by CHAR(26) NULL;
ALTER TABLE address_imports
  ADD CONSTRAINT fk_address_imports_creator
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL;

-- Couple every imported row and slot to the import in the same tenant. This
-- also gives tenant deletion an unambiguous cascade path.
ALTER TABLE address_imports
  ADD UNIQUE KEY uq_address_imports_tenant_id (tenant_id, id);

ALTER TABLE address_base_prefixes
  DROP FOREIGN KEY fk_address_base_import;
ALTER TABLE address_base_prefixes
  ADD CONSTRAINT fk_address_base_import
    FOREIGN KEY (tenant_id, import_id) REFERENCES address_imports(tenant_id, id) ON DELETE CASCADE;

ALTER TABLE address_import_slots
  DROP FOREIGN KEY fk_address_import_slots_import;
ALTER TABLE address_import_slots
  DROP INDEX fk_address_import_slots_import,
  ADD KEY idx_address_import_slots_import (tenant_id, import_id);
ALTER TABLE address_import_slots
  ADD CONSTRAINT fk_address_import_slots_import
    FOREIGN KEY (tenant_id, import_id) REFERENCES address_imports(tenant_id, id) ON DELETE CASCADE;
