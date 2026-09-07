package watchdog

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

type MySQLFlowEnrichmentPublisher struct {
	store   *MySQLStore
	objects DimensionObjectStore
	signer  ControlPlanePayloadSigner
	now     func() time.Time
}

func NewMySQLFlowEnrichmentPublisher(store *MySQLStore, objects DimensionObjectStore, signer ControlPlanePayloadSigner) (*MySQLFlowEnrichmentPublisher, error) {
	if store == nil || store.db == nil || objects == nil || signer == nil {
		return nil, errors.New("flow enrichment store, object store, and signer are required")
	}
	if _, ok := signer.(controlPlanePayloadTransactionSigner); !ok {
		return nil, ErrCollectorPlanSigningKeyUnavailable
	}
	return &MySQLFlowEnrichmentPublisher{store: store, objects: objects, signer: signer, now: func() time.Time { return time.Now().UTC() }}, nil
}

func (p *MySQLFlowEnrichmentPublisher) GetClassificationProfile(ctx context.Context, tenantID ID) (FlowClassificationProfile, error) {
	if p == nil || p.store == nil || tenantID == "" {
		return FlowClassificationProfile{}, ErrFlowClassificationProfileInvalid
	}
	return scanFlowClassificationProfile(p.store.db.QueryRowContext(ctx, `
		SELECT tenant_id, home_province, home_city, home_isp_ids, home_asns,
		       overseas_includes_hmt, internal_policy, transit_policy,
		       definition_digest, row_version, created_by, updated_by, created_at, updated_at
		FROM flow_classification_profiles WHERE tenant_id = ?
	`, tenantID))
}

// PutClassificationProfile uses expectedRowVersion=0 for create and a positive
// If-Match value for update. Input sets are sorted and deduplicated before the
// digest and row are written.
func (p *MySQLFlowEnrichmentPublisher) PutClassificationProfile(ctx context.Context, tenantID, actorID ID, expectedRowVersion uint64, draft FlowClassificationProfileDraft) (FlowClassificationProfile, error) {
	if p == nil || p.store == nil || !validCollectorEvidenceID(tenantID) || !validCollectorEvidenceID(actorID) {
		return FlowClassificationProfile{}, ErrFlowClassificationProfileInvalid
	}
	draft, digest, err := normalizeFlowClassificationProfile(tenantID, draft)
	if err != nil {
		return FlowClassificationProfile{}, err
	}
	ispJSON, err := json.Marshal(draft.HomeISPIDs)
	if err != nil {
		return FlowClassificationProfile{}, err
	}
	asnJSON, err := json.Marshal(draft.HomeASNs)
	if err != nil {
		return FlowClassificationProfile{}, err
	}
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return FlowClassificationProfile{}, err
	}
	defer tx.Rollback()
	if err := lockDimensionPublicationTenant(ctx, tx, tenantID); err != nil {
		return FlowClassificationProfile{}, err
	}
	action := "flow.classification_profile.created"
	if expectedRowVersion == 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO flow_classification_profiles (
				tenant_id, home_province, home_city, home_isp_ids, home_asns,
				overseas_includes_hmt, internal_policy, transit_policy,
				definition_digest, created_by, updated_by
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, tenantID, draft.HomeProvince, draft.HomeCity, ispJSON, asnJSON,
			draft.OverseasIncludesHMT, draft.InternalPolicy, draft.TransitPolicy,
			digest, actorID, actorID); err != nil {
			if isMySQLDuplicateKey(err) {
				return FlowClassificationProfile{}, ErrFlowEnrichmentConflict
			}
			return FlowClassificationProfile{}, err
		}
	} else {
		action = "flow.classification_profile.updated"
		result, err := tx.ExecContext(ctx, `
			UPDATE flow_classification_profiles
			SET home_province = ?, home_city = ?, home_isp_ids = ?, home_asns = ?,
			    overseas_includes_hmt = ?, internal_policy = ?, transit_policy = ?,
			    definition_digest = ?, updated_by = ?, row_version = row_version + 1
			WHERE tenant_id = ? AND row_version = ?
		`, draft.HomeProvince, draft.HomeCity, ispJSON, asnJSON,
			draft.OverseasIncludesHMT, draft.InternalPolicy, draft.TransitPolicy,
			digest, actorID, tenantID, expectedRowVersion)
		if err != nil {
			return FlowClassificationProfile{}, err
		}
		if err := requireOneAddressDimensionRow(result); err != nil {
			return FlowClassificationProfile{}, ErrFlowEnrichmentConflict
		}
	}
	if err := insertFlowEnrichmentAudit(ctx, tx, tenantID, actorID, tenantID, action, map[string]any{"definition_digest": digest}); err != nil {
		return FlowClassificationProfile{}, err
	}
	if err := tx.Commit(); err != nil {
		return FlowClassificationProfile{}, err
	}
	return p.GetClassificationProfile(ctx, tenantID)
}

func (p *MySQLFlowEnrichmentPublisher) Publish(ctx context.Context, tenantID, actorID ID, request FlowEnrichmentPublishRequest) (FlowEnrichmentPublication, error) {
	if p == nil || p.store == nil || p.signer == nil || !validCollectorEvidenceID(tenantID) || !validCollectorEvidenceID(actorID) || !isUTCMinute(request.EffectiveFrom) {
		return FlowEnrichmentPublication{}, ErrFlowClassificationProfileInvalid
	}
	transactionSigner, ok := p.signer.(controlPlanePayloadTransactionSigner)
	if !ok {
		return FlowEnrichmentPublication{}, ErrCollectorPlanSigningKeyUnavailable
	}
	effectiveFrom := request.EffectiveFrom.UTC()
	tx, err := p.store.db.BeginTx(ctx, nil)
	if err != nil {
		return FlowEnrichmentPublication{}, err
	}
	committed := false
	var savedClassificationRef string
	var publicationID ID
	defer func() {
		_ = tx.Rollback()
		if !committed && savedClassificationRef != "" {
			p.removeUnreferencedClassificationObject(publicationID, savedClassificationRef)
		}
	}()
	if err := lockDimensionPublicationTenant(ctx, tx, tenantID); err != nil {
		return FlowEnrichmentPublication{}, err
	}
	profile, err := getFlowClassificationProfileTx(ctx, tx, tenantID, true)
	if err != nil {
		return FlowEnrichmentPublication{}, err
	}
	snapshot, err := getActiveFlowAddressSnapshotTx(ctx, tx, tenantID, effectiveFrom)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return FlowEnrichmentPublication{}, ErrFlowEnrichmentDimension
		}
		return FlowEnrichmentPublication{}, err
	}
	if !validFlowEnrichmentDimension(snapshot) {
		return FlowEnrichmentPublication{}, ErrFlowEnrichmentDimension
	}
	if _, err := p.objects.ResolveDimensionObject(snapshot.ObjectRef); err != nil {
		return FlowEnrichmentPublication{}, fmt.Errorf("%w: %v", ErrFlowEnrichmentDimension, err)
	}
	var currentVersion uint32
	var latestEffective sql.NullTime
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(classification_version), 0), MAX(effective_from)
		FROM flow_enrichment_publications WHERE tenant_id = ?
	`, tenantID).Scan(&currentVersion, &latestEffective); err != nil {
		return FlowEnrichmentPublication{}, err
	}
	if currentVersion == ^uint32(0) || (latestEffective.Valid && !effectiveFrom.After(latestEffective.Time.UTC())) {
		return FlowEnrichmentPublication{}, ErrFlowEnrichmentConflict
	}
	publicationID, err = newIdentityID()
	if err != nil {
		return FlowEnrichmentPublication{}, err
	}
	classificationVersion := currentVersion + 1
	classificationData, classificationChecksum, err := flowdimension.EncodeClassificationBundle(flowdimension.ClassificationDefinition{
		TenantID: string(tenantID), Version: classificationVersion, EffectiveFrom: effectiveFrom,
		DimensionSnapshotID: string(snapshot.ID), HomeProvince: profile.Draft.HomeProvince,
		HomeCity: profile.Draft.HomeCity, HomeISPIDs: profile.Draft.HomeISPIDs, HomeASNs: profile.Draft.HomeASNs,
		OverseasIncludesHMT: profile.Draft.OverseasIncludesHMT,
		InternalPolicy:      profile.Draft.InternalPolicy, TransitPolicy: profile.Draft.TransitPolicy,
	})
	if err != nil {
		return FlowEnrichmentPublication{}, err
	}
	classificationObject, err := p.objects.SaveDimensionObject(ctx, tenantID, publicationID, classificationData)
	if err != nil {
		return FlowEnrichmentPublication{}, err
	}
	savedClassificationRef = classificationObject.Ref
	if classificationObject.Checksum != classificationChecksum {
		return FlowEnrichmentPublication{}, errors.New("classification checksum changed while saving")
	}
	signedAt := p.now().UTC().Truncate(time.Millisecond)
	publication := FlowEnrichmentPublication{
		ID: publicationID, TenantID: tenantID, PairSchemaVersion: FlowEnrichmentPairSchemaVersion,
		ClassificationVersion: classificationVersion, EffectiveFrom: effectiveFrom, ProfileRowVersion: profile.RowVersion,
		DimensionSnapshotID: snapshot.ID, DimensionVersion: snapshot.Version, DimensionEffectiveFrom: snapshot.EffectiveFrom.UTC(),
		DimensionObjectRef: snapshot.ObjectRef, DimensionObjectFormat: snapshot.ObjectFormat,
		DimensionObjectFormatVersion: snapshot.ObjectFormatVersion, DimensionChecksum: snapshot.Checksum,
		ClassificationSchemaVersion: flowdimension.ClassificationSchemaVersion,
		ClassificationObjectRef:     classificationObject.Ref, ClassificationChecksum: classificationObject.Checksum,
		SignatureAlgorithm: flowworker.EnrichmentVersionSignatureAlgorithm, SigningKeyID: p.signer.KeyID(),
		SignedAt: signedAt, CreatedBy: actorID,
	}
	payload, err := flowworker.EnrichmentVersionSigningPayload(publication.SignedEnvelope())
	if err != nil {
		return FlowEnrichmentPublication{}, err
	}
	registered, err := getCollectorPlanSigningKeyTx(ctx, tx, p.signer.KeyID(), true)
	if err != nil || registered.Status != CollectorPlanSigningKeyActive || !bytes.Equal(registered.PublicKey, p.signer.PublicKey()) {
		return FlowEnrichmentPublication{}, ErrCollectorPlanSigningKeyUnavailable
	}
	publication.Signature, err = transactionSigner.signControlPlanePayloadVerified(payload)
	if err != nil {
		return FlowEnrichmentPublication{}, err
	}
	if len(publication.Signature) != ed25519.SignatureSize {
		return FlowEnrichmentPublication{}, ErrCollectorPlanSigningKeyUnavailable
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO flow_enrichment_publications (
			id, tenant_id, pair_schema_version, classification_version, effective_from,
			profile_row_version, dimension_snapshot_id, dimension_version, dimension_effective_from,
			dimension_object_ref, dimension_object_format, dimension_object_format_version,
			dimension_checksum, classification_schema_version, classification_object_ref,
			classification_checksum, signature_algorithm, signing_key_id, signature, signed_at, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, publication.ID, publication.TenantID, publication.PairSchemaVersion, publication.ClassificationVersion,
		publication.EffectiveFrom, publication.ProfileRowVersion, publication.DimensionSnapshotID,
		publication.DimensionVersion, publication.DimensionEffectiveFrom, publication.DimensionObjectRef,
		publication.DimensionObjectFormat, publication.DimensionObjectFormatVersion, publication.DimensionChecksum,
		publication.ClassificationSchemaVersion, publication.ClassificationObjectRef, publication.ClassificationChecksum,
		publication.SignatureAlgorithm, publication.SigningKeyID, publication.Signature, publication.SignedAt, publication.CreatedBy); err != nil {
		if isMySQLDuplicateKey(err) {
			return FlowEnrichmentPublication{}, ErrFlowEnrichmentConflict
		}
		return FlowEnrichmentPublication{}, err
	}
	if err := insertFlowEnrichmentAudit(ctx, tx, tenantID, actorID, publication.ID, "flow.enrichment_publication.created", map[string]any{
		"classification_version": publication.ClassificationVersion, "dimension_snapshot_id": publication.DimensionSnapshotID,
		"dimension_version": publication.DimensionVersion, "effective_from": publication.EffectiveFrom,
	}); err != nil {
		return FlowEnrichmentPublication{}, err
	}
	if err := tx.Commit(); err != nil {
		return FlowEnrichmentPublication{}, err
	}
	committed = true
	return p.GetPublication(ctx, tenantID, publication.ID)
}

// A failed or ambiguous commit never blindly deletes an immutable object. If
// MySQL can prove that no publication row references this generated ID, the
// object can be removed; otherwise it is retained for the periodic orphan GC.
func (p *MySQLFlowEnrichmentPublisher) removeUnreferencedClassificationObject(publicationID ID, objectRef string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var exists int
	err := p.store.db.QueryRowContext(ctx, `SELECT 1 FROM flow_enrichment_publications WHERE id = ? LIMIT 1`, publicationID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		_ = p.objects.RemoveDimensionObject(objectRef)
	}
}

func (p *MySQLFlowEnrichmentPublisher) GetPublication(ctx context.Context, tenantID, publicationID ID) (FlowEnrichmentPublication, error) {
	if p == nil || p.store == nil || tenantID == "" || publicationID == "" {
		return FlowEnrichmentPublication{}, ErrFlowClassificationProfileInvalid
	}
	return scanFlowEnrichmentPublication(p.store.db.QueryRowContext(ctx, `SELECT `+flowEnrichmentPublicationColumns+`
		FROM flow_enrichment_publications WHERE tenant_id = ? AND id = ?`, tenantID, publicationID))
}

func getActiveFlowAddressSnapshotTx(ctx context.Context, tx *sql.Tx, tenantID ID, effectiveFrom time.Time) (DimensionPublicationSnapshot, error) {
	var snapshotID ID
	if err := tx.QueryRowContext(ctx, `
		SELECT snapshot_id FROM dimension_snapshot_activations
		WHERE tenant_id = ? AND module_key = 'flow' AND dimension_key = 'address' AND effective_from <= ?
		ORDER BY effective_from DESC LIMIT 1 FOR UPDATE
	`, tenantID, effectiveFrom).Scan(&snapshotID); err != nil {
		return DimensionPublicationSnapshot{}, err
	}
	return getDimensionPublicationSnapshotTx(ctx, tx, addressDimensionPublicationScope, tenantID, snapshotID, true)
}

func validFlowEnrichmentDimension(snapshot DimensionPublicationSnapshot) bool {
	return snapshot.Status == AddressDimensionStatusActive && snapshot.ApprovalState == AddressDimensionApprovalApproved &&
		snapshot.ObjectDeletedAt == nil && snapshot.ObjectFormat == AddressSnapshotObjectFormat && snapshot.ObjectFormatVersion == 1 &&
		snapshot.DecidedAt != nil && snapshot.SignedAt != nil && snapshot.SignatureAlgorithm == AddressDimensionSignatureAlgorithm &&
		snapshot.SigningKeyID != "" && len(snapshot.Signature) == ed25519.SignatureSize && validSHA256Digest(snapshot.Checksum)
}

func getFlowClassificationProfileTx(ctx context.Context, tx *sql.Tx, tenantID ID, lock bool) (FlowClassificationProfile, error) {
	query := `SELECT tenant_id, home_province, home_city, home_isp_ids, home_asns,
		overseas_includes_hmt, internal_policy, transit_policy, definition_digest,
		row_version, created_by, updated_by, created_at, updated_at
		FROM flow_classification_profiles WHERE tenant_id = ?`
	if lock {
		query += " FOR UPDATE"
	}
	return scanFlowClassificationProfile(tx.QueryRowContext(ctx, query, tenantID))
}

func scanFlowClassificationProfile(row rowScanner) (FlowClassificationProfile, error) {
	var profile FlowClassificationProfile
	var ispJSON, asnJSON []byte
	if err := row.Scan(&profile.TenantID, &profile.Draft.HomeProvince, &profile.Draft.HomeCity,
		&ispJSON, &asnJSON, &profile.Draft.OverseasIncludesHMT, &profile.Draft.InternalPolicy,
		&profile.Draft.TransitPolicy, &profile.DefinitionDigest, &profile.RowVersion,
		&profile.CreatedBy, &profile.UpdatedBy, &profile.CreatedAt, &profile.UpdatedAt); err != nil {
		return FlowClassificationProfile{}, err
	}
	if err := json.Unmarshal(ispJSON, &profile.Draft.HomeISPIDs); err != nil {
		return FlowClassificationProfile{}, err
	}
	if err := json.Unmarshal(asnJSON, &profile.Draft.HomeASNs); err != nil {
		return FlowClassificationProfile{}, err
	}
	return profile, nil
}

const flowEnrichmentPublicationColumns = `
	id, tenant_id, pair_schema_version, classification_version, effective_from,
	profile_row_version, dimension_snapshot_id, dimension_version, dimension_effective_from,
	dimension_object_ref, dimension_object_format, dimension_object_format_version,
	dimension_checksum, classification_schema_version, classification_object_ref,
	classification_checksum, signature_algorithm, signing_key_id, signature, signed_at,
	row_version, created_by, created_at, updated_at`

func scanFlowEnrichmentPublication(row rowScanner) (FlowEnrichmentPublication, error) {
	var publication FlowEnrichmentPublication
	err := row.Scan(&publication.ID, &publication.TenantID, &publication.PairSchemaVersion,
		&publication.ClassificationVersion, &publication.EffectiveFrom, &publication.ProfileRowVersion,
		&publication.DimensionSnapshotID, &publication.DimensionVersion, &publication.DimensionEffectiveFrom,
		&publication.DimensionObjectRef, &publication.DimensionObjectFormat, &publication.DimensionObjectFormatVersion,
		&publication.DimensionChecksum, &publication.ClassificationSchemaVersion,
		&publication.ClassificationObjectRef, &publication.ClassificationChecksum,
		&publication.SignatureAlgorithm, &publication.SigningKeyID, &publication.Signature,
		&publication.SignedAt, &publication.RowVersion, &publication.CreatedBy,
		&publication.CreatedAt, &publication.UpdatedAt)
	return publication, err
}

func insertFlowEnrichmentAudit(ctx context.Context, tx *sql.Tx, tenantID, actorID, resourceID ID, action string, detail map[string]any) error {
	payload, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	id, err := newIdentityID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO audit_logs (id, tenant_id, actor_id, action, resource_type, resource_id, detail_json)
		VALUES (?, ?, ?, ?, 'flow_enrichment', ?, ?)
	`, id, tenantID, actorID, action, resourceID, payload)
	return err
}
