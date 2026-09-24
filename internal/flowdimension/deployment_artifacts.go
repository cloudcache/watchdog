package flowdimension

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"time"
)

const (
	DeviceBoundarySchemaVersion         = uint16(1)
	ClassificationPolicySchemaVersion   = uint16(1)
	ClassificationPolicyAlgorithm       = "six-category-v2"
	defaultMaxDeviceBoundaryBytes       = 16 << 20
	defaultMaxClassificationPolicyBytes = 64 << 10
)

// DeviceCustomerBoundary is the immutable, device-scoped customer address
// artifact. It deliberately contains no AddressSnap or worker identity.
type DeviceCustomerBoundary struct {
	SchemaVersion uint16                   `json:"schema_version"`
	DeviceID      string                   `json:"device_id"`
	Revision      uint64                   `json:"revision"`
	EffectiveFrom time.Time                `json:"effective_from"`
	Customers     []DeviceBoundaryCustomer `json:"customers"`
}

type DeviceBoundaryCustomer struct {
	CustomerID   string                 `json:"customer_id"`
	CustomerName string                 `json:"customer_name"`
	Prefixes     []DeviceBoundaryPrefix `json:"prefixes"`
}

type DeviceBoundaryPrefix struct {
	ID   string `json:"id"`
	CIDR string `json:"cidr"`
}

type ClassificationPolicyArtifact struct {
	SchemaVersion       uint16    `json:"schema_version"`
	Revision            uint64    `json:"revision"`
	Algorithm           string    `json:"algorithm"`
	OverseasIncludesHMT bool      `json:"overseas_includes_hmt"`
	UnmatchedPolicy     string    `json:"unmatched_endpoint_policy"`
	EffectiveFrom       time.Time `json:"effective_from"`
}

type DeploymentArtifactLimits struct {
	MaxDeviceBoundaryBytes       int
	MaxClassificationPolicyBytes int
}

func EncodeDeviceCustomerBoundary(boundary DeviceCustomerBoundary) ([]byte, string, error) {
	canonical, err := canonicalDeviceCustomerBoundary(boundary)
	if err != nil {
		return nil, "", err
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		return nil, "", err
	}
	return data, sha256Checksum(data), nil
}

func DecodeDeviceCustomerBoundary(data []byte, expectedChecksum string, limits DeploymentArtifactLimits) (DeviceCustomerBoundary, error) {
	maximum := limits.MaxDeviceBoundaryBytes
	if maximum == 0 {
		maximum = defaultMaxDeviceBoundaryBytes
	}
	if maximum < 1 || len(data) == 0 || len(data) > maximum {
		return DeviceCustomerBoundary{}, fmt.Errorf("device boundary size must be 1..%d bytes", maximum)
	}
	if err := verifyDeploymentArtifactChecksum(data, expectedChecksum); err != nil {
		return DeviceCustomerBoundary{}, err
	}
	var boundary DeviceCustomerBoundary
	if err := decodeStrictDeploymentArtifact(data, &boundary); err != nil {
		return DeviceCustomerBoundary{}, fmt.Errorf("decode device boundary: %w", err)
	}
	canonical, err := canonicalDeviceCustomerBoundary(boundary)
	if err != nil {
		return DeviceCustomerBoundary{}, err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return DeviceCustomerBoundary{}, err
	}
	if !bytes.Equal(encoded, data) {
		return DeviceCustomerBoundary{}, errors.New("device boundary is not canonical JSON")
	}
	return canonical, nil
}

func EncodeClassificationPolicy(policy ClassificationPolicyArtifact) ([]byte, string, error) {
	canonical, err := canonicalClassificationPolicy(policy)
	if err != nil {
		return nil, "", err
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		return nil, "", err
	}
	return data, sha256Checksum(data), nil
}

func DecodeClassificationPolicy(data []byte, expectedChecksum string, limits DeploymentArtifactLimits) (ClassificationPolicyArtifact, error) {
	maximum := limits.MaxClassificationPolicyBytes
	if maximum == 0 {
		maximum = defaultMaxClassificationPolicyBytes
	}
	if maximum < 1 || len(data) == 0 || len(data) > maximum {
		return ClassificationPolicyArtifact{}, fmt.Errorf("classification policy size must be 1..%d bytes", maximum)
	}
	if err := verifyDeploymentArtifactChecksum(data, expectedChecksum); err != nil {
		return ClassificationPolicyArtifact{}, err
	}
	var policy ClassificationPolicyArtifact
	if err := decodeStrictDeploymentArtifact(data, &policy); err != nil {
		return ClassificationPolicyArtifact{}, fmt.Errorf("decode classification policy: %w", err)
	}
	canonical, err := canonicalClassificationPolicy(policy)
	if err != nil {
		return ClassificationPolicyArtifact{}, err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return ClassificationPolicyArtifact{}, err
	}
	if !bytes.Equal(encoded, data) {
		return ClassificationPolicyArtifact{}, errors.New("classification policy is not canonical JSON")
	}
	return canonical, nil
}

// CompileDeploymentClassification adapts independent policy and boundary
// artifacts to the existing lock-free classification runtime. The wire
// artifacts remain independent; only the already-verified in-memory view is
// assembled here immediately before one atomic catalog swap.
func CompileDeploymentClassification(addressSnapshotID string, generation uint64, effectiveFrom time.Time, policy ClassificationPolicyArtifact, boundaries []DeviceCustomerBoundary, checksum string) (*ClassificationSnapshot, error) {
	if generation == 0 || generation > uint64(^uint32(0)) {
		return nil, errors.New("deployment generation exceeds the compatibility classification range")
	}
	canonicalPolicy, err := canonicalClassificationPolicy(policy)
	if err != nil {
		return nil, err
	}
	if canonicalPolicy.EffectiveFrom.After(effectiveFrom.UTC()) {
		return nil, errors.New("classification policy cannot become effective after its deployment")
	}
	profiles := make([]ClassificationDeviceProfile, 0, len(boundaries))
	seen := make(map[string]struct{}, len(boundaries))
	for _, boundary := range boundaries {
		canonical, err := canonicalDeviceCustomerBoundary(boundary)
		if err != nil {
			return nil, err
		}
		if canonical.EffectiveFrom.After(effectiveFrom.UTC()) {
			return nil, fmt.Errorf("device boundary %s cannot become effective after its deployment", canonical.DeviceID)
		}
		if _, exists := seen[canonical.DeviceID]; exists {
			return nil, errors.New("deployment device boundary IDs must be unique")
		}
		seen[canonical.DeviceID] = struct{}{}
		profile := ClassificationDeviceProfile{DeviceID: canonical.DeviceID}
		for _, customer := range canonical.Customers {
			for _, prefix := range customer.Prefixes {
				profile.SourcePrefixes = append(profile.SourcePrefixes, ClassificationSourcePrefix{
					ID: prefix.ID, CIDR: prefix.CIDR, CustomerID: customer.CustomerID, CustomerName: customer.CustomerName,
				})
			}
		}
		profiles = append(profiles, profile)
	}
	return compileClassification(ClassificationDefinition{
		Version: uint32(generation), EffectiveFrom: effectiveFrom.UTC(), DimensionSnapshotID: addressSnapshotID,
		OverseasIncludesHMT: canonicalPolicy.OverseasIncludesHMT,
		InternalPolicy:      RecordPolicyCount, TransitPolicy: RecordPolicyCount, DeviceProfiles: profiles,
	}, checksum)
}

func canonicalDeviceCustomerBoundary(boundary DeviceCustomerBoundary) (DeviceCustomerBoundary, error) {
	if boundary.SchemaVersion != DeviceBoundarySchemaVersion || !validIdentifier(boundary.DeviceID, 128) || boundary.Revision == 0 || !validUTCMinuteArtifactTime(boundary.EffectiveFrom) {
		return DeviceCustomerBoundary{}, errors.New("device boundary metadata is invalid")
	}
	if len(boundary.Customers) == 0 {
		return DeviceCustomerBoundary{}, errors.New("device boundary requires at least one customer")
	}
	boundary.EffectiveFrom = boundary.EffectiveFrom.UTC()
	boundary.Customers = append([]DeviceBoundaryCustomer(nil), boundary.Customers...)
	type owner struct{ customerID string }
	ranges := make([]struct {
		prefix netip.Prefix
		owner  owner
	}, 0)
	customerIDs := make(map[string]struct{}, len(boundary.Customers))
	prefixIDs := make(map[string]struct{})
	for customerIndex := range boundary.Customers {
		customer := &boundary.Customers[customerIndex]
		customer.CustomerID = strings.TrimSpace(customer.CustomerID)
		customer.CustomerName = strings.TrimSpace(customer.CustomerName)
		if !validIdentifier(customer.CustomerID, 128) || customer.CustomerName == "" || len(customer.CustomerName) > 190 || len(customer.Prefixes) == 0 {
			return DeviceCustomerBoundary{}, errors.New("device boundary customer is invalid")
		}
		if _, exists := customerIDs[customer.CustomerID]; exists {
			return DeviceCustomerBoundary{}, errors.New("device boundary customer IDs must be unique")
		}
		customerIDs[customer.CustomerID] = struct{}{}
		customer.Prefixes = append([]DeviceBoundaryPrefix(nil), customer.Prefixes...)
		for prefixIndex := range customer.Prefixes {
			item := &customer.Prefixes[prefixIndex]
			item.ID = strings.TrimSpace(item.ID)
			if !validIdentifier(item.ID, 128) {
				return DeviceCustomerBoundary{}, errors.New("device boundary prefix ID is invalid")
			}
			if _, exists := prefixIDs[item.ID]; exists {
				return DeviceCustomerBoundary{}, errors.New("device boundary prefix IDs must be unique")
			}
			prefixIDs[item.ID] = struct{}{}
			prefix, err := netip.ParsePrefix(strings.TrimSpace(item.CIDR))
			if err != nil {
				return DeviceCustomerBoundary{}, fmt.Errorf("device boundary CIDR %q is invalid", item.CIDR)
			}
			address := prefix.Addr().Unmap()
			if address.Is4() && prefix.Bits() > 32 {
				prefix = netip.PrefixFrom(address, prefix.Bits()-96)
			} else {
				prefix = netip.PrefixFrom(address, prefix.Bits())
			}
			prefix = prefix.Masked()
			if !prefix.IsValid() {
				return DeviceCustomerBoundary{}, fmt.Errorf("device boundary CIDR %q is invalid", item.CIDR)
			}
			item.CIDR = prefix.String()
			for _, existing := range ranges {
				if existing.prefix.Addr().BitLen() == prefix.Addr().BitLen() && (existing.prefix.Contains(prefix.Addr()) || prefix.Contains(existing.prefix.Addr())) {
					return DeviceCustomerBoundary{}, fmt.Errorf("device boundary CIDRs overlap across %s and %s", existing.owner.customerID, customer.CustomerID)
				}
			}
			ranges = append(ranges, struct {
				prefix netip.Prefix
				owner  owner
			}{prefix: prefix, owner: owner{customerID: customer.CustomerID}})
		}
		sort.Slice(customer.Prefixes, func(left, right int) bool {
			leftPrefix, _ := netip.ParsePrefix(customer.Prefixes[left].CIDR)
			rightPrefix, _ := netip.ParsePrefix(customer.Prefixes[right].CIDR)
			if leftPrefix.Addr().BitLen() != rightPrefix.Addr().BitLen() {
				return leftPrefix.Addr().BitLen() < rightPrefix.Addr().BitLen()
			}
			if comparison := leftPrefix.Addr().Compare(rightPrefix.Addr()); comparison != 0 {
				return comparison < 0
			}
			if leftPrefix.Bits() != rightPrefix.Bits() {
				return leftPrefix.Bits() < rightPrefix.Bits()
			}
			return customer.Prefixes[left].ID < customer.Prefixes[right].ID
		})
	}
	sort.Slice(boundary.Customers, func(left, right int) bool {
		return boundary.Customers[left].CustomerID < boundary.Customers[right].CustomerID
	})
	return boundary, nil
}

func canonicalClassificationPolicy(policy ClassificationPolicyArtifact) (ClassificationPolicyArtifact, error) {
	if policy.SchemaVersion != ClassificationPolicySchemaVersion || policy.Revision == 0 || policy.Algorithm != ClassificationPolicyAlgorithm ||
		policy.UnmatchedPolicy != "unknown" || !validUTCMinuteArtifactTime(policy.EffectiveFrom) {
		return ClassificationPolicyArtifact{}, errors.New("classification policy is invalid")
	}
	policy.EffectiveFrom = policy.EffectiveFrom.UTC()
	return policy, nil
}

func verifyDeploymentArtifactChecksum(data []byte, expected string) error {
	want, err := parseSHA256Checksum(expected)
	if err != nil {
		return err
	}
	got := sha256.Sum256(data)
	if got != want {
		return errors.New("deployment artifact checksum mismatch")
	}
	return nil
}

func decodeStrictDeploymentArtifact(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func validUTCMinuteArtifactTime(value time.Time) bool {
	_, offset := value.Zone()
	return !value.IsZero() && offset == 0 && value.Second() == 0 && value.Nanosecond() == 0
}

func sha256Checksum(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}
