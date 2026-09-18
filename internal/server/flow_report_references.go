package server

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/address"
	"github.com/cloudcache/watchdog/internal/flowdimension"
	"github.com/gin-gonic/gin"
)

// flowReportReferenceItem is a selectable value that is actually present in the
// active AddressSnap. IDs are the same stable IDs written into ClickHouse Flow
// facts; operators additionally expose their stable UInt16 fact value.
type flowReportReferenceItem struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	ShortName string `json:"short_name,omitempty"`
	ParentID  string `json:"parent_id,omitempty"`
	Code      string `json:"code,omitempty"`
	Category  string `json:"category,omitempty"`
	FlowISPID uint16 `json:"flow_isp_id,omitempty"`
}

type flowReportReferenceCatalog struct {
	SnapshotID string
	Checksum   string
	Geo        map[string][]flowReportReferenceItem
	Operators  []flowReportReferenceItem
}

func (s *Server) flowReportReferences(c *gin.Context) {
	kind := strings.TrimSpace(c.Query("kind"))
	switch kind {
	case "country", "province", "city", "operator":
	default:
		fail(c, http.StatusBadRequest, "invalid_request", "kind must be country, province, city, or operator")
		return
	}

	activation, err := s.addressPublisher.GetDimensionPublicationActivationAt(c.Request.Context(), time.Now().UTC())
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	snapshot, err := s.addressPublisher.GetAddressDimensionSnapshot(c.Request.Context(), activation.SnapshotID)
	if err != nil {
		writeAddressDimensionError(c, err)
		return
	}
	catalog, err := s.getFlowReportReferenceCatalog(snapshot)
	if err != nil {
		fail(c, http.StatusServiceUnavailable, "flow_reference_unavailable", "active Flow address catalog is unavailable")
		return
	}

	items := catalog.Operators
	if kind != "operator" {
		items = catalog.Geo[kind]
		parentID := strings.TrimSpace(c.Query("parent_id"))
		if kind != "country" && parentID == "" {
			fail(c, http.StatusBadRequest, "invalid_request", "parent_id is required for province and city")
			return
		}
		if parentID != "" {
			filtered := make([]flowReportReferenceItem, 0, len(items))
			for _, item := range items {
				if item.ParentID == parentID {
					filtered = append(filtered, item)
				}
			}
			items = filtered
		}
	}
	if items == nil {
		items = []flowReportReferenceItem{}
	}
	c.JSON(http.StatusOK, gin.H{"snapshot_id": catalog.SnapshotID, "items": items, "total": len(items)})
}

func (s *Server) getFlowReportReferenceCatalog(snapshot address.AddressDimensionSnapshot) (*flowReportReferenceCatalog, error) {
	s.flowReportReferenceMu.Lock()
	defer s.flowReportReferenceMu.Unlock()
	if cached := s.flowReportReferenceCache; cached != nil && cached.SnapshotID == snapshot.ID && cached.Checksum == snapshot.Checksum {
		return cached, nil
	}
	path, err := s.addressObjects.ResolveDimensionObject(snapshot.ObjectRef)
	if err != nil {
		return nil, err
	}
	artifact, err := loadFlowReportAddressSnapshot(path, snapshot.Checksum)
	if err != nil {
		return nil, err
	}
	catalog, err := buildFlowReportReferenceCatalog(artifact)
	if err != nil {
		return nil, err
	}
	catalog.SnapshotID = snapshot.ID
	catalog.Checksum = snapshot.Checksum
	s.flowReportReferenceCache = catalog
	return catalog, nil
}

func loadFlowReportAddressSnapshot(path, expectedChecksum string) (flowdimension.AddressSnapshotArtifact, error) {
	file, err := os.Open(path)
	if err != nil {
		return flowdimension.AddressSnapshotArtifact{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return flowdimension.AddressSnapshotArtifact{}, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > flowDimensionObjectMax {
		return flowdimension.AddressSnapshotArtifact{}, errors.New("address snapshot object size is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(file, flowDimensionObjectMax+1))
	if err != nil {
		return flowdimension.AddressSnapshotArtifact{}, err
	}
	digest := sha256.Sum256(data)
	if "sha256:"+hex.EncodeToString(digest[:]) != expectedChecksum {
		return flowdimension.AddressSnapshotArtifact{}, errors.New("address snapshot checksum mismatch")
	}
	return flowdimension.DecodeAddressSnapshot(data, flowdimension.AddressSnapshotLimits{})
}

func buildFlowReportReferenceCatalog(artifact flowdimension.AddressSnapshotArtifact) (*flowReportReferenceCatalog, error) {
	text := func(reference uint32) (string, error) {
		if int(reference) >= len(artifact.Strings) {
			return "", errors.New("address snapshot contains an invalid string reference")
		}
		return artifact.Strings[reference], nil
	}
	usedGeo := make(map[string]struct{})
	usedOperators := make(map[uint16]struct{})
	for _, value := range artifact.Values {
		for _, reference := range []uint32{
			value.CustomerGeo.CountryID,
			value.CustomerGeo.ProvinceID,
			value.CustomerGeo.CityID,
		} {
			if reference == 0 {
				continue
			}
			id, err := text(reference)
			if err != nil {
				return nil, err
			}
			usedGeo[id] = struct{}{}
		}
		if value.CustomerISPID != 0 {
			usedOperators[value.CustomerISPID] = struct{}{}
		}
	}

	catalog := &flowReportReferenceCatalog{Geo: map[string][]flowReportReferenceItem{
		"country": {}, "province": {}, "city": {},
	}}
	for _, node := range artifact.GeoNodes {
		if node.Namespace != flowdimension.AddressSnapshotGeoCustomer || !node.Enabled {
			continue
		}
		id, err := text(node.ID)
		if err != nil {
			return nil, err
		}
		if _, exists := usedGeo[id]; !exists {
			continue
		}
		kind, err := text(node.Kind)
		if err != nil {
			return nil, err
		}
		if _, supported := catalog.Geo[kind]; !supported {
			continue
		}
		name, err := text(node.Name)
		if err != nil {
			return nil, err
		}
		code, err := text(node.Code)
		if err != nil {
			return nil, err
		}
		parentID, err := text(node.ParentID)
		if err != nil {
			return nil, err
		}
		catalog.Geo[kind] = append(catalog.Geo[kind], flowReportReferenceItem{
			ID: id, Name: name, ParentID: parentID, Code: code,
		})
	}
	for _, operator := range artifact.Operators {
		if operator.Namespace != flowdimension.AddressSnapshotOperatorCustomer || !operator.Enabled {
			continue
		}
		if _, exists := usedOperators[operator.ID]; !exists {
			continue
		}
		id, err := text(operator.StableID)
		if err != nil {
			return nil, err
		}
		name, err := text(operator.Name)
		if err != nil {
			return nil, err
		}
		shortName, err := text(operator.ShortName)
		if err != nil {
			return nil, err
		}
		code, err := text(operator.Code)
		if err != nil {
			return nil, err
		}
		category, err := text(operator.Category)
		if err != nil {
			return nil, err
		}
		catalog.Operators = append(catalog.Operators, flowReportReferenceItem{
			ID: id, Name: name, ShortName: shortName, Code: code, Category: category, FlowISPID: operator.ID,
		})
	}
	sortReferences := func(items []flowReportReferenceItem) {
		sort.Slice(items, func(i, j int) bool {
			left, right := strings.ToLower(items[i].Name), strings.ToLower(items[j].Name)
			if left != right {
				return left < right
			}
			return items[i].ID < items[j].ID
		})
	}
	for kind := range catalog.Geo {
		sortReferences(catalog.Geo[kind])
	}
	sortReferences(catalog.Operators)
	return catalog, nil
}
