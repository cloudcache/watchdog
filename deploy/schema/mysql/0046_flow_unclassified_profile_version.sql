-- An unclassified bootstrap publication intentionally has no classification
-- profile row yet, so profile_row_version=0 is part of its wire contract.
-- Precise publications continue to use the positive CAS row version.

ALTER TABLE flow_enrichment_publications
  DROP CHECK chk_flow_enrichment_publication_profile_version;

ALTER TABLE flow_enrichment_publications
  ADD CONSTRAINT chk_flow_enrichment_publication_profile_version
  CHECK (profile_row_version >= 0);
