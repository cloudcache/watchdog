package watchdog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/cloudcache/watchdog/internal/flowworker"
)

const (
	FlowEnrichmentPairSchemaVersion = uint16(1)
)

var (
	ErrFlowClassificationProfileInvalid = errors.New("flow classification profile is invalid")
	ErrFlowEnrichmentConflict           = errors.New("flow enrichment publication conflicts with current state")
	ErrFlowEnrichmentDimension          = errors.New("flow enrichment publication requires an active signed WADS snapshot")
)

type FlowClassificationProfileDraft struct {
	HomeProvince        string                     `json:"home_province"`
	HomeCity            string                     `json:"home_city"`
	HomeISPIDs          []uint16                   `json:"home_isp_ids"`
	HomeASNs            []uint32                   `json:"home_asns"`
	OverseasIncludesHMT bool                       `json:"overseas_includes_hmt"`
	InternalPolicy      flowdimension.RecordPolicy `json:"internal_policy"`
	TransitPolicy       flowdimension.RecordPolicy `json:"transit_policy"`
}

type FlowClassificationProfile struct {
	TenantID         ID                             `json:"tenant_id"`
	Draft            FlowClassificationProfileDraft `json:"definition"`
	DefinitionDigest string                         `json:"definition_digest"`
	RowVersion       uint64                         `json:"row_version"`
	CreatedBy        ID                             `json:"created_by"`
	UpdatedBy        ID                             `json:"updated_by"`
	CreatedAt        time.Time                      `json:"created_at"`
	UpdatedAt        time.Time                      `json:"updated_at"`
}

type FlowEnrichmentPublishRequest struct {
	EffectiveFrom time.Time `json:"effective_from"`
}

type FlowEnrichmentPublication struct {
	ID                           ID        `json:"id"`
	TenantID                     ID        `json:"tenant_id"`
	PairSchemaVersion            uint16    `json:"pair_schema_version"`
	ClassificationVersion        uint32    `json:"classification_version"`
	EffectiveFrom                time.Time `json:"effective_from"`
	ProfileRowVersion            uint64    `json:"profile_row_version"`
	DimensionSnapshotID          ID        `json:"dimension_snapshot_id"`
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
	Signature                    []byte    `json:"signature"`
	SignedAt                     time.Time `json:"signed_at"`
	RowVersion                   uint64    `json:"row_version"`
	CreatedBy                    ID        `json:"created_by"`
	CreatedAt                    time.Time `json:"created_at"`
	UpdatedAt                    time.Time `json:"updated_at"`
}

func (publication FlowEnrichmentPublication) SignedEnvelope() flowworker.SignedEnrichmentVersionPublication {
	return flowworker.SignedEnrichmentVersionPublication{
		SchemaVersion: flowworker.EnrichmentVersionEnvelopeSchemaVersion,
		Publication: flowworker.EnrichmentVersionPublication{
			PublicationID:       string(publication.ID),
			DimensionSnapshotID: string(publication.DimensionSnapshotID), DimensionVersion: publication.DimensionVersion,
			DimensionEffectiveFrom: publication.DimensionEffectiveFrom.UTC(),
			Dimension: flowworker.VersionObjectReference{
				ObjectRef: publication.DimensionObjectRef, Checksum: publication.DimensionChecksum,
				ObjectFormat: publication.DimensionObjectFormat, ObjectFormatVersion: publication.DimensionObjectFormatVersion,
			},
			ClassificationVersion:       publication.ClassificationVersion,
			ClassificationEffectiveFrom: publication.EffectiveFrom.UTC(),
			Classification: flowworker.VersionObjectReference{
				ObjectRef: publication.ClassificationObjectRef, Checksum: publication.ClassificationChecksum,
				ObjectFormat: flowworker.VersionObjectFormatJSON,
			},
		},
		SignatureAlgorithm: publication.SignatureAlgorithm, SigningKeyID: publication.SigningKeyID,
		SignedAtUnixMilli: publication.SignedAt.UTC().UnixMilli(), Signature: append([]byte(nil), publication.Signature...),
	}
}

func normalizeFlowClassificationProfile(tenantID ID, draft FlowClassificationProfileDraft) (FlowClassificationProfileDraft, string, error) {
	draft.HomeProvince = strings.TrimSpace(draft.HomeProvince)
	draft.HomeCity = strings.TrimSpace(draft.HomeCity)
	draft.HomeISPIDs = canonicalUint16Set(draft.HomeISPIDs)
	draft.HomeASNs = canonicalUint32Set(draft.HomeASNs)
	if _, err := flowdimension.CompileClassification(flowdimension.ClassificationDefinition{
		Version:             1,
		EffectiveFrom:       time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		DimensionSnapshotID: "profile-validation",
		HomeProvince:        draft.HomeProvince, HomeCity: draft.HomeCity,
		HomeISPIDs: draft.HomeISPIDs, HomeASNs: draft.HomeASNs,
		OverseasIncludesHMT: draft.OverseasIncludesHMT,
		InternalPolicy:      draft.InternalPolicy, TransitPolicy: draft.TransitPolicy,
	}); err != nil {
		return FlowClassificationProfileDraft{}, "", errors.Join(ErrFlowClassificationProfileInvalid, err)
	}
	canonical, err := json.Marshal(struct {
		SchemaVersion uint16                         `json:"schema_version"`
		Definition    FlowClassificationProfileDraft `json:"definition"`
	}{SchemaVersion: 1, Definition: draft})
	if err != nil {
		return FlowClassificationProfileDraft{}, "", err
	}
	digest := sha256.Sum256(canonical)
	return draft, hex.EncodeToString(digest[:]), nil
}

func canonicalUint16Set(values []uint16) []uint16 {
	result := append([]uint16(nil), values...)
	if result == nil {
		result = []uint16{}
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return compactUint16(result)
}

func compactUint16(values []uint16) []uint16 {
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func canonicalUint32Set(values []uint32) []uint32 {
	result := append([]uint32(nil), values...)
	if result == nil {
		result = []uint32{}
	}
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	result = compactUint32(result)
	return result
}

func compactUint32(values []uint32) []uint32 {
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}
