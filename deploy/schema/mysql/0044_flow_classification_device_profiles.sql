-- Flow classification schema v2 keeps one immutable global publication. Each
-- device profile stores only address-prefix references defining its customer
-- network boundary; geography/operator/ASN remain in the paired AddressSnap.
-- Legacy scalar columns remain readable for already-published schema v1 data.

ALTER TABLE flow_classification_profiles
  ADD COLUMN device_profiles JSON NULL AFTER home_asns;

UPDATE flow_classification_profiles
SET device_profiles = JSON_ARRAY()
WHERE device_profiles IS NULL;

ALTER TABLE flow_classification_profiles
  MODIFY COLUMN device_profiles JSON NOT NULL,
  ADD CONSTRAINT chk_flow_classification_profile_device_profiles CHECK (JSON_TYPE(device_profiles) = 'ARRAY');

ALTER TABLE flow_enrichment_publications
  DROP CHECK chk_flow_enrichment_publication_classification_schema,
  ADD CONSTRAINT chk_flow_enrichment_publication_classification_schema CHECK (classification_schema_version IN (1,2));
