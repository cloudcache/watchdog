package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowplan"
	"github.com/cloudcache/watchdog/internal/flowworker"
	"github.com/cloudcache/watchdog/internal/opjob"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
)

const (
	flowEnrichmentPairSchemaVersion = uint16(1)
	flowEnrichmentPageLimit         = 100
	flowClassificationObjectMax     = int64(16 << 20)
	flowDimensionObjectMax          = int64(512 << 20)
	flowTrustSettingKey             = "flow.enrichment.trust_bundle"
)

var (
	errFlowClassificationInvalid = errors.New("flow classification profile is invalid")
	errFlowEnrichmentConflict    = errors.New("flow enrichment publication conflicts with current state")
	errFlowEnrichmentDimension   = errors.New("flow enrichment publication requires an approved WADS snapshot")
	errFlowEnrichmentAckConflict = errors.New("flow enrichment acknowledgement does not match the publication")
	errFlowEnrichmentSuperseded  = errors.New("flow enrichment publish request was superseded by a newer profile")
	errFlowEnrichmentNoTargets   = errors.New("flow enrichment publication has no bound Flow workers")
)

type flowClassificationProfileDraft struct {
	DeviceProfiles []flowClassificationDeviceProfileDraft `json:"device_profiles"`
}

type flowClassificationDeviceProfileDraft struct {
	DeviceID        string   `json:"device_id"`
	SourcePrefixIDs []string `json:"source_prefix_ids"`
}

type flowClassificationProfile struct {
	Definition       flowClassificationProfileDraft `json:"definition"`
	DefinitionDigest string                         `json:"definition_digest"`
	RowVersion       uint64                         `json:"row_version"`
	CreatedBy        string                         `json:"created_by,omitempty"`
	UpdatedBy        string                         `json:"updated_by,omitempty"`
	CreatedAt        time.Time                      `json:"created_at,omitempty"`
	UpdatedAt        time.Time                      `json:"updated_at,omitempty"`
}

type flowEnrichmentPublication struct {
	ID                           string    `json:"id"`
	PairSchemaVersion            uint16    `json:"pair_schema_version"`
	ClassificationVersion        uint32    `json:"classification_version"`
	EffectiveFrom                time.Time `json:"effective_from"`
	ProfileRowVersion            uint64    `json:"profile_row_version"`
	DimensionSnapshotID          string    `json:"dimension_snapshot_id"`
	DimensionVersion             uint64    `json:"dimension_version"`
	DimensionEffectiveFrom       time.Time `json:"dimension_effective_from"`
	DimensionObjectRef           string    `json:"dimension_object_ref"`
	DimensionObjectFormat        string    `json:"dimension_object_format"`
	DimensionObjectFormatVersion uint16    `json:"dimension_object_format_version"`
	DimensionChecksum            string    `json:"dimension_checksum"`
	ClassificationSchemaVersion  uint16    `json:"classification_schema_version"`
	ClassificationObjectRef      string    `json:"classification_object_ref"`
	ClassificationChecksum       string    `json:"classification_checksum"`
	SignatureAlgorithm           string    `json:"signature_algorithm"`
	SigningKeyID                 string    `json:"signing_key_id"`
	Signature                    []byte    `json:"signature,omitempty"`
	SignedAt                     time.Time `json:"signed_at"`
	RowVersion                   uint64    `json:"row_version"`
	CreatedBy                    string    `json:"created_by,omitempty"`
	CreatedAt                    time.Time `json:"created_at"`
}

type flowEnrichmentAckRequest struct {
	State                  string `json:"state"`
	BootID                 string `json:"boot_id"`
	SoftwareVersion        string `json:"software_version"`
	DimensionSnapshotID    string `json:"dimension_snapshot_id"`
	DimensionVersion       uint64 `json:"dimension_version"`
	DimensionChecksum      string `json:"dimension_checksum"`
	ClassificationVersion  uint32 `json:"classification_version"`
	ClassificationChecksum string `json:"classification_checksum"`
	FailureStage           string `json:"failure_stage,omitempty"`
	FailureCode            string `json:"failure_code,omitempty"`
	FailureMessage         string `json:"failure_message,omitempty"`
}

func (p flowEnrichmentPublication) signedEnvelope() flowworker.SignedEnrichmentVersionPublication {
	return flowworker.SignedEnrichmentVersionPublication{
		SchemaVersion: flowworker.EnrichmentVersionEnvelopeSchemaVersion,
		Publication: flowworker.EnrichmentVersionPublication{
			PublicationID: p.ID, DimensionSnapshotID: p.DimensionSnapshotID,
			DimensionVersion: p.DimensionVersion, DimensionEffectiveFrom: p.DimensionEffectiveFrom.UTC(),
			Dimension: flowworker.VersionObjectReference{
				ObjectRef: p.DimensionObjectRef, Checksum: p.DimensionChecksum,
				ObjectFormat: p.DimensionObjectFormat, ObjectFormatVersion: p.DimensionObjectFormatVersion,
			},
			ClassificationVersion: p.ClassificationVersion, ClassificationEffectiveFrom: p.EffectiveFrom.UTC(),
			Classification: flowworker.VersionObjectReference{
				ObjectRef: p.ClassificationObjectRef, Checksum: p.ClassificationChecksum,
				ObjectFormat: flowworker.VersionObjectFormatJSON,
			},
		},
		SignatureAlgorithm: p.SignatureAlgorithm, SigningKeyID: p.SigningKeyID,
		SignedAtUnixMilli: p.SignedAt.UTC().UnixMilli(), Signature: append([]byte(nil), p.Signature...),
	}
}

// startFlowEnrichment materializes a stable, monotonic trust bundle for the
// same Ed25519 key already used by agent plans. MySQL JSON is decoded and
// canonicalized on every restart so lexical reordering cannot change the
// worker-visible checksum at the same generation.
func (s *Server) startFlowEnrichment(ctx context.Context) error {
	if len(s.agentPlanSigner.PrivateKey) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var installationID int
	if err := tx.QueryRowContext(ctx, `SELECT id FROM watchdog_installation WHERE id=1 FOR UPDATE`).Scan(&installationID); err != nil {
		return err
	}
	var stored []byte
	err = tx.QueryRowContext(ctx, `SELECT value_json FROM settings WHERE `+"`key`"+`=?`, flowTrustSettingKey).Scan(&stored)
	var bundle flowplan.TrustBundle
	persist := false
	switch {
	case errors.Is(err, sql.ErrNoRows):
		bundle = newFlowTrustBundle(1, time.Now().UTC(), s.agentPlanSigner.KeyID, s.agentPlanPublic)
		persist = true
	case err != nil:
		return err
	default:
		decoder := json.NewDecoder(bytes.NewReader(stored))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&bundle); err != nil {
			return fmt.Errorf("decode stored flow trust bundle: %w", err)
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return errors.New("stored flow trust bundle contains trailing JSON")
		}
		canonical, _, err := flowplan.MarshalTrustBundle(bundle)
		if err != nil {
			return fmt.Errorf("validate stored flow trust bundle: %w", err)
		}
		bundle, _, _ = flowplan.ParseTrustBundle(canonical)
		matching, reused := flowTrustKeyState(bundle, s.agentPlanSigner.KeyID, s.agentPlanPublic)
		if reused {
			return errors.New("flow enrichment signing key id was reused with different key material")
		}
		if !matching {
			persist = true
			issuedAt := time.Now().UTC().Truncate(time.Millisecond)
			if issuedAt.UnixMilli() <= bundle.IssuedAtUnixMilli {
				issuedAt = time.UnixMilli(bundle.IssuedAtUnixMilli + 1).UTC()
			}
			for index := range bundle.Keys {
				if bundle.Keys[index].Status == "active" {
					bundle.Keys[index].Status = "retiring"
					bundle.Keys[index].TrustUntilUnixMilli = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli()
				}
			}
			bundle.Generation++
			bundle.IssuedAtUnixMilli = issuedAt.UnixMilli()
			bundle.Keys = append(bundle.Keys, flowplan.TrustBundleKey{
				KeyID: s.agentPlanSigner.KeyID, Algorithm: "ed25519",
				PublicKey: base64.StdEncoding.EncodeToString(s.agentPlanPublic), Status: "active",
			})
			flowplan.SortTrustBundle(&bundle)
		}
	}
	canonical, checksum, err := flowplan.MarshalTrustBundle(bundle)
	if err != nil {
		return err
	}
	if persist {
		if _, err := tx.ExecContext(ctx, `INSERT INTO settings (`+"`key`"+`,value_json,description)
			VALUES (?,CAST(? AS JSON),'Monotonic trust bundle for signed Flow enrichment publications')
			ON DUPLICATE KEY UPDATE value_json=VALUES(value_json),description=VALUES(description),row_version=row_version+1`,
			flowTrustSettingKey, canonical); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.flowTrustBundle = append([]byte(nil), canonical...)
	s.flowTrustChecksum = checksum
	s.flowTrustGeneration = bundle.Generation
	// Upgrade/fresh-start reconciliation: if a profile and active WADS already
	// exist, converge them through the same idempotent asynchronous publisher.
	// The operation key prevents a restart from creating duplicate versions.
	if _, err := s.enqueueFlowEnrichmentForActivation(ctx, "", time.Now().UTC().Truncate(time.Minute).Add(time.Minute)); err != nil &&
		!errors.Is(err, sql.ErrNoRows) && !errors.Is(err, errFlowEnrichmentNoTargets) {
		// A drifted content hash on the already-recorded activation job must not
		// brick the whole hub at startup: the running publication stays
		// authoritative and a fresh pair can be published explicitly. Stopgap
		// until the idempotency key incorporates the request content.
		if errors.Is(err, opjob.ErrHashMismatch) {
			log.Printf("watchdog-server: enrichment pair activation skipped; existing job has a different content hash: %v", err)
		} else {
			return fmt.Errorf("enqueue current Flow enrichment pair: %w", err)
		}
	}
	return nil
}

func newFlowTrustBundle(generation uint64, issuedAt time.Time, keyID string, publicKey ed25519.PublicKey) flowplan.TrustBundle {
	return flowplan.TrustBundle{
		SchemaVersion: flowplan.TrustBundleSchemaVersion, Generation: generation,
		IssuedAtUnixMilli: issuedAt.UTC().Truncate(time.Millisecond).UnixMilli(),
		Keys: []flowplan.TrustBundleKey{{
			KeyID: keyID, Algorithm: "ed25519", PublicKey: base64.StdEncoding.EncodeToString(publicKey), Status: "active",
		}},
		RevokedKeyIDs: []string{},
	}
}

func flowTrustKeyState(bundle flowplan.TrustBundle, keyID string, publicKey ed25519.PublicKey) (matching, reused bool) {
	encoded := base64.StdEncoding.EncodeToString(publicKey)
	for _, key := range bundle.Keys {
		if key.KeyID != keyID {
			continue
		}
		return key.PublicKey == encoded && key.Status == "active", key.PublicKey != encoded
	}
	for _, revoked := range bundle.RevokedKeyIDs {
		if revoked == keyID {
			return false, true
		}
	}
	return false, false
}

func normalizeFlowClassificationProfile(draft flowClassificationProfileDraft) (flowClassificationProfileDraft, string, error) {
	draft.DeviceProfiles = append([]flowClassificationDeviceProfileDraft(nil), draft.DeviceProfiles...)
	for index := range draft.DeviceProfiles {
		profile := &draft.DeviceProfiles[index]
		profile.DeviceID = strings.TrimSpace(profile.DeviceID)
		profile.SourcePrefixIDs = canonicalStrings(profile.SourcePrefixIDs)
		if profile.DeviceID == "" || len(profile.DeviceID) > 128 || len(profile.SourcePrefixIDs) == 0 {
			return flowClassificationProfileDraft{}, "", fmt.Errorf("%w: every observation device requires at least one source prefix", errFlowClassificationInvalid)
		}
	}
	sort.Slice(draft.DeviceProfiles, func(left, right int) bool {
		return draft.DeviceProfiles[left].DeviceID < draft.DeviceProfiles[right].DeviceID
	})
	if len(draft.DeviceProfiles) == 0 {
		return flowClassificationProfileDraft{}, "", fmt.Errorf("%w: at least one observation device profile is required", errFlowClassificationInvalid)
	}
	for index := 1; index < len(draft.DeviceProfiles); index++ {
		if draft.DeviceProfiles[index].DeviceID == draft.DeviceProfiles[index-1].DeviceID {
			return flowClassificationProfileDraft{}, "", fmt.Errorf("%w: observation device IDs must be unique", errFlowClassificationInvalid)
		}
	}
	canonical, err := json.Marshal(struct {
		SchemaVersion uint16                         `json:"schema_version"`
		Definition    flowClassificationProfileDraft `json:"definition"`
	}{SchemaVersion: uint16(flowdimension.ClassificationSchemaVersion), Definition: draft})
	if err != nil {
		return flowClassificationProfileDraft{}, "", err
	}
	digest := sha256.Sum256(canonical)
	return draft, hex.EncodeToString(digest[:]), nil
}

func canonicalStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return compactSorted(result)
}

func compactSorted[T comparable](values []T) []T {
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func (s *Server) getFlowClassificationProfile(c *gin.Context) {
	profile, err := s.readFlowClassificationProfile(c.Request.Context())
	if errors.Is(err, sql.ErrNoRows) {
		profile.Definition.DeviceProfiles = []flowClassificationDeviceProfileDraft{}
		c.Header("ETag", etag(0))
		c.JSON(http.StatusOK, profile)
		return
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("ETag", etag(profile.RowVersion))
	c.JSON(http.StatusOK, profile)
}

func (s *Server) putFlowClassificationProfile(c *gin.Context) {
	expected, supplied, err := ifMatch(c)
	if !supplied {
		fail(c, http.StatusPreconditionRequired, "precondition_required", "If-Match is required")
		return
	}
	if err != nil {
		fail(c, http.StatusPreconditionFailed, "version_conflict", "If-Match must be a row version")
		return
	}
	var draft flowClassificationProfileDraft
	if !decodeStrictBody(c, &draft) {
		return
	}
	draft, digest, err := normalizeFlowClassificationProfile(draft)
	if err != nil {
		writeFlowEnrichmentError(c, err)
		return
	}
	profilesJSON, _ := json.Marshal(draft.DeviceProfiles)
	actor := stringValue(principalUserID(c))
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	var current uint64
	err = tx.QueryRowContext(c.Request.Context(), `SELECT row_version FROM flow_classification_profiles WHERE id=1 FOR UPDATE`).Scan(&current)
	switch {
	case errors.Is(err, sql.ErrNoRows) && expected == 0:
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO flow_classification_profiles
			(id,home_province,home_city,home_isp_ids,home_asns,device_profiles,overseas_includes_hmt,internal_policy,transit_policy,definition_digest,created_by,updated_by)
			VALUES (1,'','',JSON_ARRAY(),JSON_ARRAY(),CAST(? AS JSON),0,'count','count',?,NULLIF(?,''),NULLIF(?,''))`,
			profilesJSON, digest, actor, actor)
	case err != nil:
		writeSQLError(c, err)
		return
	case current != expected:
		writeSQLError(c, errVersionConflict)
		return
	default:
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE flow_classification_profiles SET
			home_province='',home_city='',home_isp_ids=JSON_ARRAY(),home_asns=JSON_ARRAY(),device_profiles=CAST(? AS JSON),overseas_includes_hmt=0,
			internal_policy='count',transit_policy='count',definition_digest=?,updated_by=NULLIF(?,''),row_version=row_version+1 WHERE id=1 AND row_version=?`,
			profilesJSON, digest, actor, expected)
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	_, err = resolveFlowClassificationDeviceProfiles(c.Request.Context(), tx, draft.DeviceProfiles, false, nil)
	if err != nil {
		writeFlowEnrichmentError(c, err)
		return
	}
	if err := insertFlowEnrichmentAudit(c.Request.Context(), tx, actor, "flow.classification_profile.saved", "classification_profile", "global", map[string]any{"definition_digest": digest, "device_profile_count": len(draft.DeviceProfiles)}); err != nil {
		writeSQLError(c, err)
		return
	}
	job, err := enqueueFlowEnrichmentPublishTx(c.Request.Context(), tx, actor, time.Time{})
	if err != nil {
		writeFlowEnrichmentError(c, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	profile, err := s.readFlowClassificationProfile(c.Request.Context())
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("X-Watchdog-Operation-ID", job.ID)
	c.Header("ETag", etag(profile.RowVersion))
	c.JSON(http.StatusOK, profile)
}

func (s *Server) readFlowClassificationProfile(ctx context.Context) (flowClassificationProfile, error) {
	return scanFlowClassificationProfile(s.db.QueryRowContext(ctx, `SELECT device_profiles,
		definition_digest,row_version,COALESCE(created_by,''),COALESCE(updated_by,''),created_at,updated_at
		FROM flow_classification_profiles WHERE id=1`))
}

type flowRowScanner interface{ Scan(...any) error }

func scanFlowClassificationProfile(row flowRowScanner) (flowClassificationProfile, error) {
	var profile flowClassificationProfile
	var profilesJSON []byte
	err := row.Scan(&profilesJSON, &profile.DefinitionDigest, &profile.RowVersion,
		&profile.CreatedBy, &profile.UpdatedBy, &profile.CreatedAt, &profile.UpdatedAt)
	if err != nil {
		return profile, err
	}
	if err := json.Unmarshal(profilesJSON, &profile.Definition.DeviceProfiles); err != nil {
		return flowClassificationProfile{}, err
	}
	return profile, nil
}

type flowAddressSnapshotPrefix struct {
	CIDR        string
	HasCity     bool
	HasOperator bool
}

func resolveFlowClassificationDeviceProfiles(ctx context.Context, tx *sql.Tx, profiles []flowClassificationDeviceProfileDraft, requireAll bool, snapshotPrefixes map[string]flowAddressSnapshotPrefix) ([]flowdimension.ClassificationDeviceProfile, error) {
	var enabledDeviceCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT device_id) FROM flow_exporter_bindings WHERE enabled=1`).Scan(&enabledDeviceCount); err != nil {
		return nil, err
	}
	if requireAll && enabledDeviceCount != len(profiles) {
		return nil, fmt.Errorf("%w: every enabled Flow device requires source prefixes", errFlowClassificationInvalid)
	}
	result := make([]flowdimension.ClassificationDeviceProfile, 0, len(profiles))
	for _, profile := range profiles {
		var kind string
		var bindingCount int
		if err := tx.QueryRowContext(ctx, `SELECT d.kind,(SELECT COUNT(*) FROM flow_exporter_bindings f WHERE f.device_id=d.id AND f.enabled=1) FROM devices d WHERE d.id=?`, profile.DeviceID).Scan(&kind, &bindingCount); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("%w: observation device %s does not exist", errFlowClassificationInvalid, profile.DeviceID)
			}
			return nil, err
		}
		if canonicalDeviceKind(kind) != "network" {
			return nil, fmt.Errorf("%w: observation device %s is not a network device", errFlowClassificationInvalid, profile.DeviceID)
		}
		if bindingCount == 0 {
			return nil, fmt.Errorf("%w: observation device %s has no enabled Flow exporter binding", errFlowClassificationInvalid, profile.DeviceID)
		}
		resolved := flowdimension.ClassificationDeviceProfile{DeviceID: profile.DeviceID, SourcePrefixes: make([]flowdimension.ClassificationSourcePrefix, 0, len(profile.SourcePrefixIDs))}
		for _, id := range profile.SourcePrefixIDs {
			var customerCIDR, customerID, customerName string
			err := tx.QueryRowContext(ctx, `SELECT p.cidr,b.customer_id,c.name FROM flow_customer_source_prefixes p
				JOIN flow_device_customers b ON b.id=p.device_customer_id JOIN parties c ON c.id=b.customer_id
				WHERE p.id=? AND b.device_id=? AND c.kind='customer'`, id, profile.DeviceID).
				Scan(&customerCIDR, &customerID, &customerName)
			if err == nil {
				resolved.SourcePrefixes = append(resolved.SourcePrefixes, flowdimension.ClassificationSourcePrefix{
					ID: id, CIDR: customerCIDR, CustomerID: customerID, CustomerName: customerName,
				})
				continue
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			// Compatibility for an already-saved schema-v2 profile. New customer
			// boundaries never enter AddressSnap, but a legacy profile may still
			// reference an address-library prefix until it is edited.
			published, exists := snapshotPrefixes[id]
			cidr := published.CIDR
			if snapshotPrefixes == nil {
				if err := tx.QueryRowContext(ctx, `SELECT cidr FROM address_prefixes WHERE id=?`, id).Scan(&cidr); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						return nil, fmt.Errorf("%w: source prefix %s does not exist", errFlowClassificationInvalid, id)
					}
					return nil, err
				}
				exists = true
			}
			if !exists {
				return nil, fmt.Errorf("%w: source prefix %s is not present in the active address snapshot", errFlowClassificationInvalid, id)
			}
			if snapshotPrefixes != nil && (!published.HasCity || !published.HasOperator) {
				return nil, fmt.Errorf("%w: source prefix %s requires city and operator attributes in the active address snapshot", errFlowClassificationInvalid, id)
			}
			resolved.SourcePrefixes = append(resolved.SourcePrefixes, flowdimension.ClassificationSourcePrefix{ID: id, CIDR: cidr})
		}
		result = append(result, resolved)
	}
	return result, nil
}

func loadFlowAddressSnapshotPrefixes(path, expectedChecksum string) (map[string]flowAddressSnapshotPrefix, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > flowDimensionObjectMax {
		return nil, errors.New("address snapshot object size is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(file, flowDimensionObjectMax+1))
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if "sha256:"+hex.EncodeToString(digest[:]) != expectedChecksum {
		return nil, errors.New("address snapshot checksum mismatch")
	}
	artifact, err := flowdimension.DecodeAddressSnapshot(data, flowdimension.AddressSnapshotLimits{})
	if err != nil {
		return nil, err
	}
	result := make(map[string]flowAddressSnapshotPrefix)
	for _, value := range artifact.Values {
		if value.PrimaryPrefixID == 0 {
			continue
		}
		if int(value.PrimaryPrefixID) >= len(artifact.Strings) || int(value.PrimaryPrefixCIDR) >= len(artifact.Strings) {
			return nil, errors.New("address snapshot contains an invalid source prefix reference")
		}
		id := artifact.Strings[value.PrimaryPrefixID]
		cidr := artifact.Strings[value.PrimaryPrefixCIDR]
		if id == "" || cidr == "" {
			return nil, errors.New("address snapshot contains an incomplete source prefix")
		}
		entry := flowAddressSnapshotPrefix{
			CIDR:        cidr,
			HasCity:     value.CustomerGeo.CityID != 0 || value.CustomerGeo.City != 0,
			HasOperator: value.CustomerISPID != 0,
		}
		if existing, exists := result[id]; exists {
			if existing.CIDR != cidr {
				return nil, fmt.Errorf("address snapshot prefix %s has conflicting CIDRs", id)
			}
			entry.HasCity = existing.HasCity && entry.HasCity
			entry.HasOperator = existing.HasOperator && entry.HasOperator
		}
		result[id] = entry
	}
	return result, nil
}

type flowAddressSnapshotRef struct {
	ID, ObjectRef, ObjectFormat, Checksum, Status, ApprovalState string
	Version, ObjectFormatVersion                                 uint64
	EffectiveFrom                                                time.Time
	ObjectDeletedAt                                              sql.NullTime
}

func getActiveFlowAddressSnapshot(ctx context.Context, tx *sql.Tx, effectiveFrom time.Time) (flowAddressSnapshotRef, error) {
	var snapshot flowAddressSnapshotRef
	err := tx.QueryRowContext(ctx, `SELECT s.id,s.version,s.effective_from,s.object_ref,s.object_format,s.object_format_version,
		s.checksum,s.status,s.approval_state,s.object_deleted_at
		FROM dimension_snapshot_activations a JOIN dimension_snapshots s ON s.id=a.snapshot_id
		WHERE a.module_key='flow' AND a.dimension_key='address' AND a.effective_from<=?
		ORDER BY a.effective_from DESC LIMIT 1 FOR UPDATE`, effectiveFrom).
		Scan(&snapshot.ID, &snapshot.Version, &snapshot.EffectiveFrom, &snapshot.ObjectRef, &snapshot.ObjectFormat,
			&snapshot.ObjectFormatVersion, &snapshot.Checksum, &snapshot.Status, &snapshot.ApprovalState, &snapshot.ObjectDeletedAt)
	return snapshot, err
}

func (s *Server) publishFlowEnrichment(c *gin.Context) {
	s.publishFlowEnrichmentMode(c, false)
}

// bootstrapFlowEnrichment publishes an explicitly unclassified version pair.
// It keeps raw ingestion available before customer source CIDRs are configured;
// a later device-scoped publication supplies the precise six-category rules.
func (s *Server) bootstrapFlowEnrichment(c *gin.Context) {
	s.publishFlowEnrichmentMode(c, true)
}

func (s *Server) publishFlowEnrichmentMode(c *gin.Context, bootstrap bool) {
	if len(s.agentPlanSigner.PrivateKey) != ed25519.PrivateKeySize || s.addressPublisher == nil {
		fail(c, http.StatusServiceUnavailable, "flow_enrichment_unavailable", "Flow enrichment signing or address publication is not configured")
		return
	}
	var request struct {
		EffectiveFrom time.Time `json:"effective_from"`
	}
	if !decodeStrictBody(c, &request) {
		return
	}
	if !bootstrap && !flowUTCMinute(request.EffectiveFrom) {
		fail(c, http.StatusBadRequest, "invalid_request", "effective_from must be a UTC minute boundary")
		return
	}
	selectionTime := request.EffectiveFrom.UTC()
	if bootstrap && selectionTime.IsZero() {
		selectionTime = time.Now().UTC().Truncate(time.Minute)
	}
	if !flowUTCMinute(selectionTime) {
		fail(c, http.StatusBadRequest, "invalid_request", "effective_from must be a UTC minute boundary")
		return
	}
	actor := stringValue(principalUserID(c))
	publication, err := s.createFlowEnrichmentPublication(c.Request.Context(), bootstrap, false, selectionTime, 0, "", nil, actor)
	if err != nil {
		writeFlowEnrichmentError(c, err)
		return
	}
	c.Header("Location", "/api/v1/flow/enrichment-publications/"+publication.ID)
	c.JSON(http.StatusCreated, publication)
}

func (s *Server) createFlowEnrichmentPublication(ctx context.Context, bootstrap, automatic bool, requested time.Time, expectedProfileVersion uint64, expectedDimensionSnapshotID string, expectedWorkerIDs []string, actor string) (publication flowEnrichmentPublication, returnErr error) {
	if len(s.agentPlanSigner.PrivateKey) != ed25519.PrivateKeySize || s.addressPublisher == nil {
		return publication, errors.New("Flow enrichment signing or address publication is not configured")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return publication, err
	}
	publicationID, savedRef, committed := newID(), "", false
	defer func() {
		_ = tx.Rollback()
		if committed || savedRef == "" {
			return
		}
		var exists int
		if err := s.db.QueryRowContext(context.Background(), `SELECT 1 FROM flow_enrichment_publications WHERE id=?`, publicationID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
			_ = s.addressObjects.RemoveDimensionObject(savedRef)
		}
	}()
	if err := lockFlowEnrichment(ctx, tx); err != nil {
		return publication, err
	}
	profile := flowClassificationProfile{}
	if !bootstrap {
		profile, err = scanFlowClassificationProfile(tx.QueryRowContext(ctx, `SELECT device_profiles,
			definition_digest,row_version,COALESCE(created_by,''),COALESCE(updated_by,''),created_at,updated_at
			FROM flow_classification_profiles WHERE id=1 FOR UPDATE`))
		if err != nil {
			return publication, err
		}
		if expectedProfileVersion != 0 && profile.RowVersion != expectedProfileVersion {
			return publication, errFlowEnrichmentSuperseded
		}
	} else {
		var profileCount, publicationCount int
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM flow_classification_profiles WHERE id=1),
			(SELECT COUNT(*) FROM flow_enrichment_publications)`).Scan(&profileCount, &publicationCount); err != nil {
			return publication, err
		}
		if profileCount != 0 || publicationCount != 0 {
			return publication, errFlowEnrichmentConflict
		}
	}
	var currentVersion uint64
	var latestEffective sql.NullTime
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(classification_version),0),MAX(effective_from) FROM flow_enrichment_publications`).Scan(&currentVersion, &latestEffective); err != nil {
		return publication, err
	}
	if currentVersion >= uint64(^uint32(0)) {
		return publication, errFlowEnrichmentConflict
	}
	effectiveFrom := requested.UTC()
	if automatic {
		minimum := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
		if effectiveFrom.Before(minimum) {
			effectiveFrom = minimum
		}
		if latestEffective.Valid && !effectiveFrom.After(latestEffective.Time.UTC()) {
			effectiveFrom = latestEffective.Time.UTC().Truncate(time.Minute).Add(time.Minute)
		}
	} else if latestEffective.Valid && !effectiveFrom.After(latestEffective.Time.UTC()) {
		return publication, errFlowEnrichmentConflict
	}
	snapshot, err := getActiveFlowAddressSnapshot(ctx, tx, effectiveFrom)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return publication, errFlowEnrichmentDimension
		}
		return publication, err
	}
	if bootstrap {
		effectiveFrom = snapshot.EffectiveFrom.UTC()
	}
	if expectedDimensionSnapshotID != "" && snapshot.ID != expectedDimensionSnapshotID {
		return publication, errFlowEnrichmentSuperseded
	}
	if snapshot.Status != address.AddressDimensionStatusActive || snapshot.ApprovalState != address.AddressDimensionApprovalApproved ||
		snapshot.ObjectDeletedAt.Valid || snapshot.ObjectFormat != address.AddressSnapshotObjectFormat ||
		snapshot.ObjectFormatVersion != uint64(flowdimension.AddressSnapshotFormatVersion) || !flowSHA256(snapshot.Checksum) {
		return publication, errFlowEnrichmentDimension
	}
	var deviceProfiles []flowdimension.ClassificationDeviceProfile
	if !bootstrap {
		snapshotPath, err := s.addressObjects.ResolveDimensionObject(snapshot.ObjectRef)
		if err != nil {
			return publication, fmt.Errorf("%w: %v", errFlowEnrichmentDimension, err)
		}
		snapshotPrefixes, err := loadFlowAddressSnapshotPrefixes(snapshotPath, snapshot.Checksum)
		if err != nil {
			return publication, fmt.Errorf("%w: %v", errFlowEnrichmentDimension, err)
		}
		deviceProfiles, err = resolveFlowClassificationDeviceProfiles(ctx, tx, profile.Definition.DeviceProfiles, true, snapshotPrefixes)
		if err != nil {
			return publication, err
		}
	}
	targets, err := flowEnrichmentTargets(ctx, tx, profile.Definition.DeviceProfiles, bootstrap)
	if err != nil {
		return publication, err
	}
	if len(expectedWorkerIDs) != 0 && !equalFlowWorkerTargets(targets, expectedWorkerIDs) {
		return publication, errFlowEnrichmentSuperseded
	}
	if len(targets) == 0 {
		return publication, errFlowEnrichmentNoTargets
	}
	classificationVersion := uint32(currentVersion + 1)
	classificationData, classificationChecksum, err := flowdimension.EncodeClassificationBundle(flowdimension.ClassificationDefinition{
		Version: classificationVersion, EffectiveFrom: effectiveFrom, DimensionSnapshotID: snapshot.ID,
		DeviceProfiles: deviceProfiles, InternalPolicy: flowdimension.RecordPolicyCount, TransitPolicy: flowdimension.RecordPolicyCount,
	})
	if err != nil {
		return publication, err
	}
	object, err := s.addressObjects.SaveDimensionObject(ctx, publicationID, classificationData)
	if err != nil {
		return publication, err
	}
	savedRef = object.Ref
	if object.Checksum != classificationChecksum {
		return publication, errors.New("classification checksum changed while saving")
	}
	publication = flowEnrichmentPublication{
		ID: publicationID, PairSchemaVersion: flowEnrichmentPairSchemaVersion,
		ClassificationVersion: classificationVersion, EffectiveFrom: effectiveFrom, ProfileRowVersion: profile.RowVersion,
		// The pair's effective_from is a publish-time decision, not a property of
		// the (timing-agnostic) address object. Declaring the dimension effective
		// at the same instant as its classification keeps the pair coherent even
		// when the snapshot was activated with a stale (past) effective_from.
		DimensionSnapshotID: snapshot.ID, DimensionVersion: snapshot.Version, DimensionEffectiveFrom: effectiveFrom,
		DimensionObjectRef: snapshot.ObjectRef, DimensionObjectFormat: snapshot.ObjectFormat,
		DimensionObjectFormatVersion: uint16(snapshot.ObjectFormatVersion), DimensionChecksum: snapshot.Checksum,
		ClassificationSchemaVersion: uint16(flowdimension.ClassificationSchemaVersion), ClassificationObjectRef: object.Ref,
		ClassificationChecksum: object.Checksum, SignatureAlgorithm: flowworker.EnrichmentVersionSignatureAlgorithm,
		SigningKeyID: s.agentPlanSigner.KeyID, SignedAt: time.Now().UTC().Truncate(time.Millisecond), CreatedBy: actor,
	}
	if bootstrap {
		publication.ClassificationSchemaVersion = uint16(flowdimension.LegacyClassificationSchemaVersion)
	}
	payload, err := flowworker.EnrichmentVersionSigningPayload(publication.signedEnvelope())
	if err != nil {
		return publication, err
	}
	publication.Signature = ed25519.Sign(s.agentPlanSigner.PrivateKey, payload)
	_, err = tx.ExecContext(ctx, `INSERT INTO flow_enrichment_publications
		(id,pair_schema_version,classification_version,effective_from,profile_row_version,dimension_snapshot_id,dimension_version,
		dimension_effective_from,dimension_object_ref,dimension_object_format,dimension_object_format_version,dimension_checksum,
		classification_schema_version,classification_object_ref,classification_checksum,signature_algorithm,signing_key_id,signature,signed_at,created_by)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULLIF(?,''))`,
		publication.ID, publication.PairSchemaVersion, publication.ClassificationVersion, publication.EffectiveFrom,
		publication.ProfileRowVersion, publication.DimensionSnapshotID, publication.DimensionVersion, publication.DimensionEffectiveFrom,
		publication.DimensionObjectRef, publication.DimensionObjectFormat, publication.DimensionObjectFormatVersion, publication.DimensionChecksum,
		publication.ClassificationSchemaVersion, publication.ClassificationObjectRef, publication.ClassificationChecksum,
		publication.SignatureAlgorithm, publication.SigningKeyID, publication.Signature, publication.SignedAt, actor)
	if err != nil {
		return publication, err
	}
	for _, workerID := range targets {
		if _, err := tx.ExecContext(ctx, `INSERT INTO flow_enrichment_publication_targets (publication_id,worker_id) VALUES (?,?)`, publication.ID, workerID); err != nil {
			return publication, err
		}
	}
	auditAction := "flow.enrichment_publication.created"
	if bootstrap {
		auditAction = "flow.enrichment_publication.bootstrap_created"
	}
	if err := insertFlowEnrichmentAudit(ctx, tx, actor, auditAction, "flow_enrichment", publication.ID, map[string]any{
		"classification_version": publication.ClassificationVersion, "dimension_snapshot_id": publication.DimensionSnapshotID,
		"dimension_version": publication.DimensionVersion, "effective_from": publication.EffectiveFrom, "worker_ids": targets,
	}); err != nil {
		return publication, err
	}
	if err := tx.Commit(); err != nil {
		return publication, err
	}
	committed = true
	return publication, nil
}

func equalFlowWorkerTargets(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func flowEnrichmentTargets(ctx context.Context, tx *sql.Tx, profiles []flowClassificationDeviceProfileDraft, bootstrap bool) ([]string, error) {
	seen := map[string]struct{}{}
	if bootstrap {
		rows, err := tx.QueryContext(ctx, `SELECT DISTINCT b.worker_id FROM flow_worker_device_bindings b JOIN agents a ON a.id=b.worker_id
			WHERE a.kind='flow_worker' AND a.status='active' ORDER BY b.worker_id`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			seen[id] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		// Bootstrap exists only before a device profile can be published. On a
		// fresh installation there may therefore be no device binding yet. A
		// single worker is unambiguous; multiple workers must first be assigned to
		// devices instead of receiving a platform-wide broadcast.
		if len(seen) == 0 {
			workerID, err := soleActiveFlowWorker(ctx, tx)
			if err != nil {
				return nil, err
			}
			seen[workerID] = struct{}{}
		}
	} else {
		for _, profile := range profiles {
			var workerID, kind, status string
			err := tx.QueryRowContext(ctx, `SELECT b.worker_id,a.kind,a.status FROM flow_worker_device_bindings b
				JOIN agents a ON a.id=b.worker_id WHERE b.device_id=?`, profile.DeviceID).Scan(&workerID, &kind, &status)
			if errors.Is(err, sql.ErrNoRows) || (err == nil && (kind != "flow_worker" || status != "active")) {
				// Old installations and workers registered after the schema migration
				// may not have a binding yet. With exactly one active worker the
				// placement is deterministic, so persist it and let that worker pull
				// this immutable publication. Multiple workers remain an explicit
				// administrator choice made when the device boundaries are saved.
				workerID, err = soleActiveFlowWorker(ctx, tx)
				if err != nil {
					if errors.Is(err, errFlowEnrichmentNoTargets) {
						return nil, fmt.Errorf("%w: device %s requires an explicit worker selection", errFlowEnrichmentNoTargets, profile.DeviceID)
					}
					return nil, err
				}
				if _, err = tx.ExecContext(ctx, `INSERT INTO flow_worker_device_bindings (device_id,worker_id)
					VALUES (?,?) ON DUPLICATE KEY UPDATE worker_id=VALUES(worker_id),row_version=row_version+1`, profile.DeviceID, workerID); err != nil {
					return nil, err
				}
			} else if err != nil {
				return nil, err
			}
			seen[workerID] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}

func soleActiveFlowWorker(ctx context.Context, tx *sql.Tx) (string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id FROM agents WHERE kind='flow_worker' AND status='active' ORDER BY id LIMIT 2`)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	workers := make([]string, 0, 2)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return "", err
		}
		workers = append(workers, id)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(workers) != 1 {
		return "", errFlowEnrichmentNoTargets
	}
	return workers[0], nil
}

func lockFlowEnrichment(ctx context.Context, tx *sql.Tx) error {
	var id int
	return tx.QueryRowContext(ctx, `SELECT id FROM watchdog_installation WHERE id=1 FOR UPDATE`).Scan(&id)
}

const flowEnrichmentPublicationColumns = `id,pair_schema_version,classification_version,effective_from,profile_row_version,
	dimension_snapshot_id,dimension_version,dimension_effective_from,dimension_object_ref,dimension_object_format,
	dimension_object_format_version,dimension_checksum,classification_schema_version,classification_object_ref,
	classification_checksum,signature_algorithm,signing_key_id,signature,signed_at,row_version,COALESCE(created_by,''),created_at`

func scanFlowEnrichmentPublication(row flowRowScanner) (flowEnrichmentPublication, error) {
	var item flowEnrichmentPublication
	err := row.Scan(&item.ID, &item.PairSchemaVersion, &item.ClassificationVersion, &item.EffectiveFrom, &item.ProfileRowVersion,
		&item.DimensionSnapshotID, &item.DimensionVersion, &item.DimensionEffectiveFrom, &item.DimensionObjectRef,
		&item.DimensionObjectFormat, &item.DimensionObjectFormatVersion, &item.DimensionChecksum,
		&item.ClassificationSchemaVersion, &item.ClassificationObjectRef, &item.ClassificationChecksum,
		&item.SignatureAlgorithm, &item.SigningKeyID, &item.Signature, &item.SignedAt, &item.RowVersion,
		&item.CreatedBy, &item.CreatedAt)
	return item, err
}

func (s *Server) listFlowEnrichmentPublications(c *gin.Context) {
	page, ok := parseInventoryPage(c, []string{
		"id", "classification_version", "dimension_version", "dimension_snapshot_id", "effective_from",
		"dimension_checksum", "classification_checksum", "signing_key_id", "created_at",
	}, map[string]string{
		"classification_version": "classification_version", "effective_from": "effective_from",
		"dimension_version": "dimension_version", "dimension_snapshot_id": "dimension_snapshot_id",
		"signing_key_id": "signing_key_id", "created_at": "created_at",
	}, "classification_version")
	if !ok {
		return
	}
	where := []string{"1=1"}
	args := []any{}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, `(id LIKE ? OR dimension_snapshot_id LIKE ? OR dimension_checksum LIKE ? OR classification_checksum LIKE ? OR signing_key_id LIKE ?)`)
		args = append(args, like, like, like, like, like)
	}
	for _, filter := range []string{"id", "dimension_snapshot_id", "effective_from", "dimension_checksum", "classification_checksum", "signing_key_id", "created_at"} {
		value := strings.TrimSpace(c.Query(filter))
		if value == "" {
			continue
		}
		if len(value) > 128 {
			fail(c, http.StatusBadRequest, "invalid_filter", filter+" must not exceed 128 characters")
			return
		}
		where = append(where, filter+"=?")
		args = append(args, value)
	}
	for _, filter := range []string{"classification_version", "dimension_version"} {
		value := strings.TrimSpace(c.Query(filter))
		if value == "" {
			continue
		}
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil || parsed == 0 {
			fail(c, http.StatusBadRequest, "invalid_filter", filter+" must be a positive integer")
			return
		}
		where = append(where, filter+"=?")
		args = append(args, parsed)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*) FROM flow_enrichment_publications`+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT `+flowEnrichmentPublicationColumns+`
		FROM flow_enrichment_publications`+clause+` ORDER BY `+page.Sort+` `+page.Order+`,id `+page.Order+` LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]flowEnrichmentPublication, 0, page.Limit)
	for rows.Next() {
		item, err := scanFlowEnrichmentPublication(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) listFlowEnrichmentPublicationFacets(c *gin.Context) {
	for key := range c.Request.URL.Query() {
		if key != "field" && key != "q" && key != "limit" {
			fail(c, http.StatusBadRequest, "invalid_filter", "unsupported query parameter: "+key)
			return
		}
	}
	field := strings.TrimSpace(c.Query("field"))
	columns := map[string]string{
		"id":                     "id",
		"classification_version": "classification_version", "dimension_version": "dimension_version",
		"dimension_snapshot_id": "dimension_snapshot_id", "effective_from": "effective_from",
		"dimension_checksum": "dimension_checksum", "classification_checksum": "classification_checksum",
		"signing_key_id": "signing_key_id", "created_at": "created_at",
	}
	column, ok := columns[field]
	if !ok {
		fail(c, http.StatusBadRequest, "invalid_filter", "unsupported publication facet")
		return
	}
	limit := 50
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			fail(c, http.StatusBadRequest, "invalid_filter", "limit must be between 1 and 100")
			return
		}
		limit = value
	}
	q := strings.TrimSpace(c.Query("q"))
	if len(q) > 128 {
		fail(c, http.StatusBadRequest, "invalid_filter", "q must not exceed 128 characters")
		return
	}
	where := ""
	args := []any{}
	if q != "" {
		where = " WHERE CAST(" + column + " AS CHAR) LIKE ?"
		args = append(args, "%"+escapeLike(q)+"%")
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT CAST(`+column+` AS CHAR),COUNT(*) FROM flow_enrichment_publications`+
		where+` GROUP BY `+column+` ORDER BY COUNT(*) DESC,`+column+` ASC LIMIT ?`, args...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var value string
		var count uint64
		if err := rows.Scan(&value, &count); err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, gin.H{"value": value, "label": value, "count": count})
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) getFlowEnrichmentPublication(c *gin.Context) {
	item, err := s.readFlowEnrichmentPublication(c.Request.Context(), c.Param("publication_id"), false)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, item)
}

func (s *Server) readFlowEnrichmentPublication(ctx context.Context, id string, lock bool) (flowEnrichmentPublication, error) {
	query := `SELECT ` + flowEnrichmentPublicationColumns + ` FROM flow_enrichment_publications WHERE id=?`
	if lock {
		query += " FOR UPDATE"
	}
	return scanFlowEnrichmentPublication(s.db.QueryRowContext(ctx, query, id))
}

func (s *Server) listFlowEnrichmentACKs(c *gin.Context) {
	page, ok := parseInventoryPage(c, []string{"worker_id", "state", "software_version", "error_code"}, map[string]string{
		"worker_name": "g.name", "state": "a.state", "boot_id": "a.boot_id", "software_version": "a.software_version",
		"attempted_at": "a.attempted_at", "installed_at": "a.installed_at", "error_code": "a.error_code",
	}, "attempted_at")
	if !ok {
		return
	}
	where := []string{"a.publication_id=?"}
	args := []any{c.Param("publication_id")}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, `(a.worker_id LIKE ? OR g.name LIKE ? OR a.boot_id LIKE ? OR a.software_version LIKE ? OR a.state LIKE ? OR COALESCE(a.error_code,'') LIKE ? OR COALESCE(a.error_message,'') LIKE ?)`)
		args = append(args, like, like, like, like, like, like, like)
	}
	for _, filter := range []string{"worker_id", "state", "software_version", "error_code"} {
		value := strings.TrimSpace(c.Query(filter))
		if value == "" {
			continue
		}
		if len(value) > 128 {
			fail(c, http.StatusBadRequest, "invalid_filter", filter+" must not exceed 128 characters")
			return
		}
		column := "a." + filter
		if filter == "error_code" {
			column = "COALESCE(a.error_code,'')"
		}
		where = append(where, column+"=?")
		args = append(args, value)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*) FROM flow_enrichment_publication_acks a JOIN agents g ON g.id=a.worker_id`+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	queryArgs := append(append([]any{}, args...), page.Limit, page.Offset)
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT a.publication_id,a.worker_id,g.name,a.boot_id,a.software_version,a.state,
		a.attempted_at,a.downloaded_at,a.installed_at,COALESCE(a.error_code,''),COALESCE(a.error_message,''),a.row_version
		FROM flow_enrichment_publication_acks a JOIN agents g ON g.id=a.worker_id
		`+clause+` ORDER BY `+page.Sort+` `+page.Order+`,a.worker_id `+page.Order+` LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var publicationID, workerID, workerName, bootID, software, state, errorCode, errorMessage string
		var attempted time.Time
		var downloaded, installed sql.NullTime
		var rowVersion uint64
		if err := rows.Scan(&publicationID, &workerID, &workerName, &bootID, &software, &state, &attempted,
			&downloaded, &installed, &errorCode, &errorMessage, &rowVersion); err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, gin.H{"publication_id": publicationID, "worker_id": workerID, "worker_name": workerName,
			"boot_id": bootID, "software_version": software, "state": state, "attempted_at": attempted,
			"downloaded_at": nullableTime(downloaded), "installed_at": nullableTime(installed),
			"error_code": errorCode, "error_message": errorMessage, "row_version": rowVersion})
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) listFlowEnrichmentACKFacets(c *gin.Context) {
	for key := range c.Request.URL.Query() {
		if key != "field" && key != "q" && key != "limit" {
			fail(c, http.StatusBadRequest, "invalid_filter", "unsupported query parameter: "+key)
			return
		}
	}
	columns := map[string]string{
		"worker_id": "a.worker_id", "state": "a.state", "software_version": "a.software_version",
		"error_code": "COALESCE(a.error_code,'')",
	}
	column, ok := columns[strings.TrimSpace(c.Query("field"))]
	if !ok {
		fail(c, http.StatusBadRequest, "invalid_filter", "unsupported acknowledgement facet")
		return
	}
	limit := 50
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			fail(c, http.StatusBadRequest, "invalid_filter", "limit must be between 1 and 100")
			return
		}
		limit = value
	}
	where := []string{"a.publication_id=?"}
	args := []any{c.Param("publication_id")}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		if len(q) > 128 {
			fail(c, http.StatusBadRequest, "invalid_filter", "q must not exceed 128 characters")
			return
		}
		where = append(where, "CAST("+column+" AS CHAR) LIKE ?")
		args = append(args, "%"+escapeLike(q)+"%")
	}
	args = append(args, limit)
	labelColumn := column
	if column == "a.worker_id" {
		labelColumn = "g.name"
	}
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT CAST(`+column+` AS CHAR),MAX(CAST(`+labelColumn+` AS CHAR)),COUNT(*)
		FROM flow_enrichment_publication_acks a JOIN agents g ON g.id=a.worker_id
		WHERE `+strings.Join(where, " AND ")+` GROUP BY `+column+` ORDER BY COUNT(*) DESC,`+column+` ASC LIMIT ?`, args...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []gin.H{}
	for rows.Next() {
		var value, label string
		var count uint64
		if err := rows.Scan(&value, &label, &count); err != nil {
			writeSQLError(c, err)
			return
		}
		items = append(items, gin.H{"value": value, "label": label, "count": count})
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) fetchFlowWorkerTrustBundle(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	if len(s.flowTrustBundle) == 0 {
		fail(c, http.StatusServiceUnavailable, "flow_enrichment_unavailable", "Flow enrichment trust bundle is unavailable")
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Header("Content-Type", "application/json")
	c.Header("X-Watchdog-Trust-Generation", strconv.FormatUint(s.flowTrustGeneration, 10))
	c.Header("X-Watchdog-Trust-Checksum", s.flowTrustChecksum)
	c.Data(http.StatusOK, "application/json", s.flowTrustBundle)
}

func (s *Server) fetchFlowWorkerPublications(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	limit, after, ok := flowEnrichmentPage(c)
	if !ok {
		return
	}
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT `+flowEnrichmentPublicationColumns+`
		FROM flow_enrichment_publications
		WHERE id IN (SELECT publication_id FROM flow_enrichment_publication_targets WHERE worker_id=?)
		  AND classification_version>? ORDER BY classification_version ASC LIMIT ?`, c.Param("id"), after, limit+1)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]json.RawMessage, 0, limit)
	next := uint32(after)
	for rows.Next() {
		publication, err := scanFlowEnrichmentPublication(rows)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		if len(items) == limit {
			c.Header("Cache-Control", "no-store")
			c.JSON(http.StatusOK, gin.H{"items": items, "next_version": next, "has_more": true})
			return
		}
		envelope, err := flowworker.MarshalSignedEnrichmentVersionPublication(publication.signedEnvelope())
		if err != nil {
			fail(c, http.StatusServiceUnavailable, "flow_enrichment_invalid", "stored Flow enrichment publication is invalid")
			return
		}
		items = append(items, envelope)
		next = publication.ClassificationVersion
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Header("X-Watchdog-Next-Classification-Version", strconv.FormatUint(uint64(next), 10))
	c.JSON(http.StatusOK, gin.H{"items": items, "next_version": next, "has_more": false})
}

func flowEnrichmentPage(c *gin.Context) (limit int, after uint64, ok bool) {
	for key, values := range c.Request.URL.Query() {
		if (key != "after_version" && key != "limit") || len(values) != 1 {
			fail(c, http.StatusBadRequest, "invalid_request", "only after_version and limit are accepted")
			return 0, 0, false
		}
	}
	limit = 20
	var err error
	if raw := c.Query("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > flowEnrichmentPageLimit {
			fail(c, http.StatusBadRequest, "invalid_request", "limit must be 1..100")
			return 0, 0, false
		}
	}
	if raw := c.Query("after_version"); raw != "" {
		after, err = strconv.ParseUint(raw, 10, 32)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_request", "after_version must be an unsigned 32-bit integer")
			return 0, 0, false
		}
	}
	return limit, after, true
}

func (s *Server) fetchFlowWorkerObject(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	publication, err := s.readFlowEnrichmentPublication(c.Request.Context(), c.Param("publication_id"), false)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	var targeted int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT 1 FROM flow_enrichment_publication_targets
		WHERE publication_id=? AND worker_id=?`, publication.ID, c.Param("id")).Scan(&targeted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			fail(c, http.StatusNotFound, "not_found", "Flow enrichment publication is not assigned to this worker")
			return
		}
		writeSQLError(c, err)
		return
	}
	var ref, checksum, name, contentType string
	var maximum int64
	switch c.Param("kind") {
	case "dimension":
		ref, checksum, name, contentType, maximum = publication.DimensionObjectRef, publication.DimensionChecksum, "address-snapshot.wads", "application/octet-stream", flowDimensionObjectMax
	case "classification":
		ref, checksum, name, contentType, maximum = publication.ClassificationObjectRef, publication.ClassificationChecksum, "classification.json", "application/json", flowClassificationObjectMax
	default:
		fail(c, http.StatusBadRequest, "invalid_request", "object kind must be dimension or classification")
		return
	}
	path, err := s.addressObjects.ResolveDimensionObject(ref)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "flow_enrichment_object_unavailable", "Flow enrichment object is unavailable")
		return
	}
	file, err := os.Open(path)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "flow_enrichment_object_unavailable", "Flow enrichment object is unavailable")
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		fail(c, http.StatusServiceUnavailable, "flow_enrichment_object_unavailable", "Flow enrichment object is unavailable")
		return
	}
	etagValue := `"` + checksum + `"`
	c.Header("Cache-Control", "private, max-age=31536000, immutable")
	c.Header("Content-Type", contentType)
	c.Header("Content-Disposition", `attachment; filename="`+name+`"`)
	c.Header("X-Content-Type-Options", "nosniff")
	c.Header("X-Watchdog-Object-Checksum", checksum)
	c.Header("ETag", etagValue)
	if c.GetHeader("If-None-Match") == etagValue {
		c.Status(http.StatusNotModified)
		return
	}
	http.ServeContent(c.Writer, c.Request, name, info.ModTime(), file)
}

func (s *Server) acknowledgeFlowWorkerPublication(c *gin.Context) {
	if !s.authenticateFlowWorker(c) {
		return
	}
	var request flowEnrichmentAckRequest
	if !decodeStrictBody(c, &request) {
		return
	}
	if err := validateFlowEnrichmentACK(request); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	publication, err := scanFlowEnrichmentPublication(tx.QueryRowContext(c.Request.Context(), `SELECT `+flowEnrichmentPublicationColumns+`
		FROM flow_enrichment_publications WHERE id=? FOR UPDATE`, c.Param("publication_id")))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if publication.DimensionSnapshotID != request.DimensionSnapshotID || publication.DimensionVersion != request.DimensionVersion ||
		publication.DimensionChecksum != request.DimensionChecksum || publication.ClassificationVersion != request.ClassificationVersion ||
		publication.ClassificationChecksum != request.ClassificationChecksum {
		writeFlowEnrichmentError(c, errFlowEnrichmentAckConflict)
		return
	}
	workerID := c.Param("id")
	var targeted int
	if err := tx.QueryRowContext(c.Request.Context(), `SELECT 1 FROM flow_enrichment_publication_targets
		WHERE publication_id=? AND worker_id=? FOR UPDATE`, publication.ID, workerID).Scan(&targeted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeFlowEnrichmentError(c, errFlowEnrichmentAckConflict)
			return
		}
		writeSQLError(c, err)
		return
	}
	var state string
	var attemptedAt time.Time
	var downloadedAt, installedAt sql.NullTime
	err = tx.QueryRowContext(c.Request.Context(), `SELECT state,attempted_at,downloaded_at,installed_at
		FROM flow_enrichment_publication_acks WHERE publication_id=? AND worker_id=? FOR UPDATE`, publication.ID, workerID).
		Scan(&state, &attemptedAt, &downloadedAt, &installedAt)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		writeSQLError(c, err)
		return
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if exists && !now.After(attemptedAt.UTC()) {
		now = attemptedAt.UTC().Add(time.Millisecond)
	}
	if request.State == "downloaded" || request.State == "installed" {
		if !downloadedAt.Valid {
			downloadedAt = sql.NullTime{Time: now, Valid: true}
		}
	}
	if request.State == "installed" && !installedAt.Valid {
		installedAt = sql.NullTime{Time: now, Valid: true}
	}
	errorCode, errorMessage := "", ""
	if request.State == "failed" {
		errorCode = request.FailureStage + ":" + request.FailureCode
		errorMessage = request.FailureMessage
	}
	if exists {
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE flow_enrichment_publication_acks SET boot_id=?,software_version=?,state=?,
			attempted_at=?,downloaded_at=?,installed_at=?,error_code=NULLIF(?,''),error_message=NULLIF(?,''),row_version=row_version+1
			WHERE publication_id=? AND worker_id=?`, request.BootID, request.SoftwareVersion, request.State, now,
			downloadedAt, installedAt, errorCode, errorMessage, publication.ID, workerID)
	} else {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO flow_enrichment_publication_acks
			(publication_id,worker_id,boot_id,software_version,state,attempted_at,downloaded_at,installed_at,error_code,error_message)
			VALUES (?,?,?,?,?,?,?,?,NULLIF(?,''),NULLIF(?,''))`, publication.ID, workerID, request.BootID,
			request.SoftwareVersion, request.State, now, downloadedAt, installedAt, errorCode, errorMessage)
	}
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"accepted": true})
}

func (s *Server) authenticateFlowWorker(c *gin.Context) bool {
	if !s.authenticateAgent(c, c.Param("id")) {
		return false
	}
	// A draining worker is still processing and must keep reading its current
	// feed and acknowledging it; only revoked (rejected above) is cut off.
	// Publication targeting excludes draining workers separately.
	var kind, status string
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT kind,status FROM agents WHERE id=?`, c.Param("id")).Scan(&kind, &status); err != nil ||
		kind != "flow_worker" || (status != "registered" && status != "active" && status != "draining") {
		fail(c, http.StatusUnauthorized, "unauthorized", "credential does not belong to a Flow worker")
		return false
	}
	return true
}

func validateFlowEnrichmentACK(request flowEnrichmentAckRequest) error {
	if request.State != "downloaded" && request.State != "installed" && request.State != "failed" {
		return errors.New("state must be downloaded, installed, or failed")
	}
	if request.BootID == "" || len(request.BootID) > 64 || !printableASCII(request.BootID) ||
		request.SoftwareVersion == "" || len(request.SoftwareVersion) > 64 || !printableASCII(request.SoftwareVersion) ||
		request.DimensionSnapshotID == "" || len(request.DimensionSnapshotID) > 64 || request.DimensionVersion == 0 ||
		request.ClassificationVersion == 0 || !flowSHA256(request.DimensionChecksum) || !flowSHA256(request.ClassificationChecksum) {
		return errors.New("acknowledgement identity, versions, or checksums are invalid")
	}
	if request.State != "failed" {
		if request.FailureStage != "" || request.FailureCode != "" || request.FailureMessage != "" {
			return errors.New("successful acknowledgement cannot contain failure fields")
		}
		return nil
	}
	validStage := map[string]bool{"transport": true, "verify": true, "compile": true, "persist": true, "activate": true, "ack": true}
	if !validStage[request.FailureStage] || request.FailureCode == "" || len(request.FailureCode) > 48 ||
		request.FailureCode != strings.ToUpper(request.FailureCode) || len(request.FailureMessage) > 512 || !printableASCIIText(request.FailureMessage) {
		return errors.New("failure acknowledgement fields are invalid")
	}
	for _, character := range request.FailureCode {
		if (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' {
			return errors.New("failure_code must contain only A-Z, 0-9, and underscore")
		}
	}
	return nil
}

func flowUTCMinute(value time.Time) bool {
	_, offset := value.Zone()
	return !value.IsZero() && offset == 0 && value.Second() == 0 && value.Nanosecond() == 0
}

func flowSHA256(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}

func insertFlowEnrichmentAudit(ctx context.Context, tx *sql.Tx, actor, action, resource, resourceID string, detail map[string]any) error {
	payload, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO audit_logs (id,actor_id,action,resource,resource_id,detail_json)
		VALUES (?,NULLIF(?,''),?,?,?,CAST(? AS JSON))`, newID(), actor, action, resource, resourceID, payload)
	return err
}

func writeFlowEnrichmentError(c *gin.Context, err error) {
	var mysqlErr *mysql.MySQLError
	switch {
	case errors.Is(err, errFlowClassificationInvalid):
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, errFlowEnrichmentConflict), errors.Is(err, errFlowEnrichmentAckConflict):
		fail(c, http.StatusConflict, "version_conflict", err.Error())
	case errors.Is(err, errFlowEnrichmentDimension):
		fail(c, http.StatusPreconditionFailed, "address_snapshot_unavailable", err.Error())
	case errors.Is(err, errFlowEnrichmentNoTargets):
		fail(c, http.StatusUnprocessableEntity, "flow_worker_required", err.Error())
	case errors.Is(err, sql.ErrNoRows):
		fail(c, http.StatusPreconditionFailed, "classification_profile_unavailable", "add customer source ranges for at least one Flow device before publishing")
	case errors.As(err, &mysqlErr) && mysqlErr.Number == 1062:
		fail(c, http.StatusConflict, "version_conflict", errFlowEnrichmentConflict.Error())
	default:
		writeSQLError(c, err)
	}
}
