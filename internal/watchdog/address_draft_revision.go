package watchdog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	AddressDraftRevisionScopePrefix      = "prefix"
	AddressDraftRevisionStatusPrepared   = "prepared"
	AddressDraftRevisionStatusApplied    = "applied"
	AddressDraftRevisionStatusSuperseded = "superseded"
	AddressDraftRevisionStatusCancelled  = "cancelled"
	AddressDraftRevisionMaxOperations    = 1_000
)

var (
	ErrAddressDraftRevisionInvalid     = errors.New("address draft revision is invalid")
	ErrAddressDraftRevisionConflict    = errors.New("address draft revision version conflict")
	ErrAddressDraftRevisionChanged     = errors.New("address draft changed after revision preview")
	ErrAddressDraftRevisionExpired     = errors.New("address draft revision expired")
	ErrAddressDraftRevisionNotPrepared = errors.New("address draft revision is not prepared")
)

type AddressPrefixBatchOperation struct {
	Action          string            `json:"action"`
	PrefixID        string            `json:"prefix_id,omitempty"`
	ExpectedVersion uint64            `json:"expected_version,omitempty"`
	CIDR            string            `json:"cidr,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	GeoLeafID       ID                `json:"geo_leaf_id,omitempty"`
	OperatorID      ID                `json:"operator_id,omitempty"`
	ASN             *uint32           `json:"asn,omitempty"`
	Source          string            `json:"source,omitempty"`
}

type AddressPrefixBatchChange struct {
	Action   string `json:"action"`
	PrefixID string `json:"prefix_id"`
	CIDR     string `json:"cidr"`
}

type AddressPrefixBatchPreview struct {
	CreateCount          uint32                     `json:"create_count"`
	DeleteCount          uint32                     `json:"delete_count"`
	BeforePrefixCount    uint64                     `json:"before_prefix_count"`
	AfterPrefixCount     uint64                     `json:"after_prefix_count"`
	ExpectedResultDigest string                     `json:"expected_result_digest"`
	Changes              []AddressPrefixBatchChange `json:"changes"`
}

type AddressDraftRevision struct {
	ID             ID                            `json:"id"`
	TenantID       ID                            `json:"tenant_id"`
	Scope          string                        `json:"scope"`
	BaseDigest     string                        `json:"base_digest"`
	RequestDigest  string                        `json:"request_digest"`
	ResultDigest   string                        `json:"result_digest,omitempty"`
	Operations     []AddressPrefixBatchOperation `json:"operations"`
	Preview        AddressPrefixBatchPreview     `json:"preview"`
	OperationCount uint32                        `json:"operation_count"`
	Status         string                        `json:"status"`
	RowVersion     uint64                        `json:"row_version"`
	CreatedBy      ID                            `json:"created_by,omitempty"`
	AppliedBy      ID                            `json:"applied_by,omitempty"`
	CreatedAt      time.Time                     `json:"created_at"`
	ExpiresAt      time.Time                     `json:"expires_at"`
	AppliedAt      *time.Time                    `json:"applied_at,omitempty"`
}

type AddressDraftRevisionListFilter struct {
	Status string
	Limit  int
	Cursor string
}

type preparedAddressPrefixRevision struct {
	BaseDigest    string
	RequestDigest string
	ResultDigest  string
	Operations    []AddressPrefixBatchOperation
	Preview       AddressPrefixBatchPreview
	Result        []AddressPrefix
}

func prepareAddressPrefixRevision(tenantID ID, current []AddressPrefix, input []AddressPrefixBatchOperation) (preparedAddressPrefixRevision, error) {
	if tenantID == "" || len(input) == 0 || len(input) > AddressDraftRevisionMaxOperations {
		return preparedAddressPrefixRevision{}, ErrAddressDraftRevisionInvalid
	}
	baseDigest, err := digestAddressPrefixes(current)
	if err != nil {
		return preparedAddressPrefixRevision{}, err
	}
	operations := make([]AddressPrefixBatchOperation, 0, len(input))
	for _, raw := range input {
		operation, err := normalizeAddressPrefixBatchOperation(tenantID, raw, false)
		if err != nil {
			return preparedAddressPrefixRevision{}, err
		}
		operations = append(operations, operation)
	}
	sortAddressPrefixBatchOperations(operations)
	requestDigest, err := digestAddressPrefixRevisionRequest(baseDigest, operations)
	if err != nil {
		return preparedAddressPrefixRevision{}, err
	}
	createOrdinal := 0
	for index := range operations {
		if operations[index].Action != "create" {
			continue
		}
		name := fmt.Sprintf("%s|%s|%d", tenantID, requestDigest, createOrdinal)
		operations[index].PrefixID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String()
		createOrdinal++
	}
	result, preview, err := evaluateAddressPrefixBatch(current, operations)
	if err != nil {
		return preparedAddressPrefixRevision{}, err
	}
	resultDigest, err := digestAddressPrefixes(result)
	if err != nil {
		return preparedAddressPrefixRevision{}, err
	}
	preview.ExpectedResultDigest = resultDigest
	return preparedAddressPrefixRevision{
		BaseDigest: baseDigest, RequestDigest: requestDigest, ResultDigest: resultDigest,
		Operations: operations, Preview: preview, Result: result,
	}, nil
}

func evaluateAddressPrefixBatch(current []AddressPrefix, operations []AddressPrefixBatchOperation) ([]AddressPrefix, AddressPrefixBatchPreview, error) {
	byID := make(map[string]AddressPrefix, len(current))
	byCIDR := make(map[string]string, len(current))
	for _, prefix := range current {
		byID[prefix.ID] = cloneAddressPrefix(prefix)
		byCIDR[prefix.CIDR] = prefix.ID
	}
	preview := AddressPrefixBatchPreview{BeforePrefixCount: uint64(len(current)), Changes: make([]AddressPrefixBatchChange, 0, len(operations))}
	seenDeletes := make(map[string]struct{})
	for _, operation := range operations {
		if operation.Action != "delete" {
			continue
		}
		if _, duplicate := seenDeletes[operation.PrefixID]; duplicate {
			return nil, AddressPrefixBatchPreview{}, fmt.Errorf("%w: duplicate delete for prefix %s", ErrAddressDraftRevisionInvalid, operation.PrefixID)
		}
		prefix, exists := byID[operation.PrefixID]
		if !exists || prefix.RowVersion != operation.ExpectedVersion {
			return nil, AddressPrefixBatchPreview{}, ErrAddressDraftRevisionChanged
		}
		seenDeletes[operation.PrefixID] = struct{}{}
		delete(byID, operation.PrefixID)
		delete(byCIDR, prefix.CIDR)
		preview.DeleteCount++
		preview.Changes = append(preview.Changes, AddressPrefixBatchChange{Action: "delete", PrefixID: prefix.ID, CIDR: prefix.CIDR})
	}
	for _, operation := range operations {
		if operation.Action != "create" {
			continue
		}
		if operation.PrefixID == "" {
			return nil, AddressPrefixBatchPreview{}, fmt.Errorf("%w: create prefix id is missing", ErrAddressDraftRevisionInvalid)
		}
		if _, exists := byID[operation.PrefixID]; exists {
			return nil, AddressPrefixBatchPreview{}, fmt.Errorf("%w: duplicate prefix id %s", ErrAddressDraftRevisionInvalid, operation.PrefixID)
		}
		if existingID, exists := byCIDR[operation.CIDR]; exists {
			return nil, AddressPrefixBatchPreview{}, fmt.Errorf("%w: CIDR %s already belongs to %s", ErrAddressDraftRevisionInvalid, operation.CIDR, existingID)
		}
		prefix := addressPrefixFromBatchOperation(operation)
		byID[prefix.ID] = prefix
		byCIDR[prefix.CIDR] = prefix.ID
		preview.CreateCount++
		preview.Changes = append(preview.Changes, AddressPrefixBatchChange{Action: "create", PrefixID: prefix.ID, CIDR: prefix.CIDR})
	}
	result := make([]AddressPrefix, 0, len(byID))
	for _, prefix := range byID {
		result = append(result, prefix)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CIDR != result[j].CIDR {
			return result[i].CIDR < result[j].CIDR
		}
		return result[i].ID < result[j].ID
	})
	preview.AfterPrefixCount = uint64(len(result))
	return result, preview, nil
}

func normalizeAddressPrefixBatchOperation(tenantID ID, raw AddressPrefixBatchOperation, persisted bool) (AddressPrefixBatchOperation, error) {
	raw.Action = strings.ToLower(strings.TrimSpace(raw.Action))
	switch raw.Action {
	case "delete":
		raw.PrefixID = strings.TrimSpace(raw.PrefixID)
		if raw.PrefixID == "" || raw.ExpectedVersion == 0 || raw.CIDR != "" || raw.GeoLeafID != "" || raw.OperatorID != "" || raw.ASN != nil || raw.Source != "" || raw.Labels != nil {
			return AddressPrefixBatchOperation{}, fmt.Errorf("%w: delete requires only prefix_id and expected_version", ErrAddressDraftRevisionInvalid)
		}
		return raw, nil
	case "create":
		if (!persisted && raw.PrefixID != "") || (persisted && strings.TrimSpace(raw.PrefixID) == "") || raw.ExpectedVersion != 0 {
			return AddressPrefixBatchOperation{}, fmt.Errorf("%w: create prefix identity", ErrAddressDraftRevisionInvalid)
		}
		if len(raw.GeoLeafID) > 26 || len(raw.OperatorID) > 26 {
			return AddressPrefixBatchOperation{}, fmt.Errorf("%w: address taxonomy identity", ErrAddressDraftRevisionInvalid)
		}
		if raw.ASN != nil && *raw.ASN == 0 {
			raw.ASN = nil
		}
		prefix, _, _, err := normalizeAddressPrefix(AddressPrefix{
			ID: strings.TrimSpace(raw.PrefixID), TenantID: tenantID, CIDR: raw.CIDR,
			Labels: raw.Labels, GeoLeafID: raw.GeoLeafID, OperatorID: raw.OperatorID, ASN: raw.ASN, Source: raw.Source,
		})
		if err != nil {
			return AddressPrefixBatchOperation{}, err
		}
		return AddressPrefixBatchOperation{
			Action: "create", PrefixID: prefix.ID, CIDR: prefix.CIDR, Labels: prefix.Labels,
			GeoLeafID: prefix.GeoLeafID, OperatorID: prefix.OperatorID, ASN: prefix.ASN, Source: prefix.Source,
		}, nil
	default:
		return AddressPrefixBatchOperation{}, fmt.Errorf("%w: action must be create or delete", ErrAddressDraftRevisionInvalid)
	}
}

func addressPrefixFromBatchOperation(operation AddressPrefixBatchOperation) AddressPrefix {
	prefix, _, _, _ := normalizeAddressPrefix(AddressPrefix{
		ID: operation.PrefixID, CIDR: operation.CIDR, Labels: operation.Labels,
		GeoLeafID: operation.GeoLeafID, OperatorID: operation.OperatorID, ASN: operation.ASN, Source: operation.Source,
		RowVersion: 1,
	})
	prefix.RowVersion = 1
	return prefix
}

func digestAddressPrefixes(prefixes []AddressPrefix) (string, error) {
	type digestPrefix struct {
		ID         string            `json:"id"`
		CIDR       string            `json:"cidr"`
		Labels     map[string]string `json:"labels"`
		GeoLeafID  ID                `json:"geo_leaf_id,omitempty"`
		OperatorID ID                `json:"operator_id,omitempty"`
		ASN        *uint32           `json:"asn,omitempty"`
		Source     string            `json:"source"`
		RowVersion uint64            `json:"row_version"`
	}
	canonical := make([]digestPrefix, 0, len(prefixes))
	for _, prefix := range prefixes {
		canonical = append(canonical, digestPrefix{
			ID: prefix.ID, CIDR: prefix.CIDR, Labels: prefix.Labels, GeoLeafID: prefix.GeoLeafID,
			OperatorID: prefix.OperatorID, ASN: prefix.ASN, Source: prefix.Source, RowVersion: prefix.RowVersion,
		})
	}
	sort.Slice(canonical, func(i, j int) bool {
		if canonical[i].CIDR != canonical[j].CIDR {
			return canonical[i].CIDR < canonical[j].CIDR
		}
		return canonical[i].ID < canonical[j].ID
	})
	return digestJSON(canonical)
}

func digestAddressPrefixRevisionRequest(baseDigest string, operations []AddressPrefixBatchOperation) (string, error) {
	return digestJSON(struct {
		Scope      string                        `json:"scope"`
		BaseDigest string                        `json:"base_digest"`
		Operations []AddressPrefixBatchOperation `json:"operations"`
	}{Scope: AddressDraftRevisionScopePrefix, BaseDigest: baseDigest, Operations: operations})
}

func digestJSON(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func sortAddressPrefixBatchOperations(operations []AddressPrefixBatchOperation) {
	sort.Slice(operations, func(i, j int) bool {
		if operations[i].Action != operations[j].Action {
			return operations[i].Action < operations[j].Action
		}
		if operations[i].Action == "delete" {
			return operations[i].PrefixID < operations[j].PrefixID
		}
		return operations[i].CIDR < operations[j].CIDR
	})
}

func cloneAddressPrefix(prefix AddressPrefix) AddressPrefix {
	prefix.Labels = cloneStringMap(prefix.Labels)
	if prefix.ASN != nil {
		asn := *prefix.ASN
		prefix.ASN = &asn
	}
	return prefix
}

func cloneStringMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
