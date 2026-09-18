package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"sort"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/billing"
	"github.com/gin-gonic/gin"
)

type flowCustomerBoundary struct {
	ID           string                     `json:"id"`
	DeviceID     string                     `json:"device_id"`
	DeviceName   string                     `json:"device_name"`
	DeviceHost   string                     `json:"device_host"`
	CustomerID   string                     `json:"customer_id"`
	CustomerName string                     `json:"customer_name"`
	CustomerRef  string                     `json:"customer_ref"`
	SourceRanges []flowCustomerSourcePrefix `json:"source_ranges"`
	RowVersion   uint64                     `json:"row_version"`
	CreatedAt    time.Time                  `json:"created_at"`
	UpdatedAt    time.Time                  `json:"updated_at"`
}

type flowCustomerSourcePrefix struct {
	ID           string `json:"id"`
	CIDR         string `json:"cidr"`
	Family       uint8  `json:"family"`
	PrefixLength uint8  `json:"prefix_length"`
}

type flowCustomerBoundaryMutation struct {
	DeviceID     *string  `json:"device_id"`
	CustomerID   *string  `json:"customer_id"`
	SourceRanges []string `json:"source_ranges"`
}

func (s *Server) listFlowCustomers(c *gin.Context) {
	page, ok := s.billingPage(c, "limit", "offset", "q", "sort", "order")
	if !ok {
		return
	}
	page.Kind = "customer"
	items, total, err := s.billingStore.ListParties(c, page)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func (s *Server) createFlowCustomer(c *gin.Context) {
	var input struct {
		Name  string `json:"name"`
		Ref   string `json:"ref"`
		Notes string `json:"notes"`
	}
	if !addressDecodeStrict(c, &input, 64<<10) {
		return
	}
	item, err := s.billingStore.CreateParty(c, billing.Party{
		Kind: "customer", Name: input.Name, Ref: input.Ref, Notes: input.Notes,
	}, currentPrincipal(c).UserID)
	if err != nil {
		writeBillingError(c, err)
		return
	}
	c.Header("ETag", etag(item.RowVersion))
	c.Header("Location", "/api/v1/flow/customers/"+item.ID)
	c.JSON(http.StatusCreated, item)
}

func (s *Server) listFlowCustomerBoundaries(c *gin.Context) {
	page, ok := parseInventoryPage(c,
		[]string{"device_id", "customer_id"},
		map[string]string{
			"device":      "COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host)",
			"device_host": "d.host", "customer": "p.name", "created_at": "b.created_at", "updated_at": "b.updated_at",
		}, "customer")
	if !ok {
		return
	}
	where := []string{"p.kind='customer'"}
	args := []any{}
	principal := currentPrincipal(c)
	if principal != nil && !principal.can("device.viewAll") {
		where = append(where, `(EXISTS (SELECT 1 FROM user_device_permissions udp WHERE udp.user_id=? AND udp.device_id=b.device_id)
			OR EXISTS (SELECT 1 FROM user_device_group_permissions udgp JOIN device_group_members dgm ON dgm.device_group_id=udgp.device_group_id WHERE udgp.user_id=? AND dgm.device_id=b.device_id))`)
		args = append(args, principal.UserID, principal.UserID)
	}
	for _, filter := range []struct{ param, column string }{{"device_id", "b.device_id"}, {"customer_id", "b.customer_id"}} {
		if value := strings.TrimSpace(c.Query(filter.param)); value != "" {
			if len(value) > 26 {
				fail(c, http.StatusBadRequest, "invalid_filter", filter.param+" must not exceed 26 characters")
				return
			}
			where, args = append(where, filter.column+"=?"), append(args, value)
		}
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, "(p.name LIKE ? OR p.ref LIKE ? OR d.host LIKE ? OR d.display_name LIKE ? OR d.sys_name LIKE ?)")
		args = append(args, like, like, like, like, like)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	join := ` FROM flow_device_customers b JOIN devices d ON d.id=b.device_id JOIN parties p ON p.id=b.customer_id`
	var total int
	if err := s.db.QueryRowContext(c, "SELECT COUNT(*)"+join+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	query := `SELECT b.id,b.device_id,COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host),d.host,
		b.customer_id,p.name,p.ref,b.row_version,b.created_at,b.updated_at` + join + clause +
		fmt.Sprintf(" ORDER BY %s %s,b.id %s LIMIT ? OFFSET ?", page.Sort, page.Order, page.Order)
	rows, err := s.db.QueryContext(c, query, append(append([]any{}, args...), page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]flowCustomerBoundary, 0, page.Limit)
	for rows.Next() {
		var item flowCustomerBoundary
		if err := rows.Scan(&item.ID, &item.DeviceID, &item.DeviceName, &item.DeviceHost, &item.CustomerID,
			&item.CustomerName, &item.CustomerRef, &item.RowVersion, &item.CreatedAt, &item.UpdatedAt); err != nil {
			writeSQLError(c, err)
			return
		}
		item.SourceRanges = []flowCustomerSourcePrefix{}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := loadFlowCustomerPrefixes(c, s.db, items); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items, "total": total, "limit": page.Limit, "offset": page.Offset})
}

func loadFlowCustomerPrefixes(ctx context.Context, db *sql.DB, items []flowCustomerBoundary) error {
	if len(items) == 0 {
		return nil
	}
	placeholders := make([]string, len(items))
	args := make([]any, len(items))
	index := make(map[string]int, len(items))
	for offset := range items {
		placeholders[offset], args[offset], index[items[offset].ID] = "?", items[offset].ID, offset
	}
	rows, err := db.QueryContext(ctx, `SELECT id,device_customer_id,cidr,family,prefix_length
		FROM flow_customer_source_prefixes WHERE device_customer_id IN (`+strings.Join(placeholders, ",")+`)
		ORDER BY family,prefix_length,cidr,id`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var bindingID string
		var item flowCustomerSourcePrefix
		if err := rows.Scan(&item.ID, &bindingID, &item.CIDR, &item.Family, &item.PrefixLength); err != nil {
			return err
		}
		if offset, exists := index[bindingID]; exists {
			items[offset].SourceRanges = append(items[offset].SourceRanges, item)
		}
	}
	return rows.Err()
}

func (s *Server) createFlowCustomerBoundary(c *gin.Context) {
	var input flowCustomerBoundaryMutation
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	deviceID := strings.TrimSpace(valueOr(input.DeviceID, ""))
	customerID := strings.TrimSpace(valueOr(input.CustomerID, ""))
	prefixes, err := normalizeFlowCustomerRanges(input.SourceRanges)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_source_ranges", err.Error())
		return
	}
	actor := currentPrincipal(c).UserID
	tx, err := s.db.BeginTx(c, nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	if err := s.validateFlowCustomerBoundary(c, tx, deviceID, customerID, "", prefixes); err != nil {
		return
	}
	id := newID()
	if _, err := tx.ExecContext(c, `INSERT INTO flow_device_customers (id,device_id,customer_id,created_by,updated_by)
		VALUES (?,?,?,NULLIF(?,''),NULLIF(?,''))`, id, deviceID, customerID, actor, actor); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := replaceFlowCustomerPrefixes(c, tx, id, prefixes); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := s.syncFlowClassificationProfile(c, tx, actor); err != nil {
		writeFlowEnrichmentError(c, err)
		return
	}
	if err := insertFlowEnrichmentAudit(c, tx, actor, "flow.customer_boundary.created", "flow_customer_boundary", id,
		map[string]any{"device_id": deviceID, "customer_id": customerID, "source_range_count": len(prefixes)}); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.Header("Location", "/api/v1/flow/customer-bindings/"+id)
	c.JSON(http.StatusCreated, gin.H{"id": id})
}

func (s *Server) updateFlowCustomerBoundary(c *gin.Context) {
	expected, supplied, err := ifMatch(c)
	if !supplied || err != nil || expected == 0 {
		fail(c, http.StatusPreconditionRequired, "precondition_required", "If-Match with a positive row version is required")
		return
	}
	var input flowCustomerBoundaryMutation
	if !addressDecodeStrict(c, &input, 1<<20) {
		return
	}
	prefixes, err := normalizeFlowCustomerRanges(input.SourceRanges)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_source_ranges", err.Error())
		return
	}
	actor := currentPrincipal(c).UserID
	tx, err := s.db.BeginTx(c, nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	var deviceID, customerID string
	var current uint64
	if err := tx.QueryRowContext(c, `SELECT device_id,customer_id,row_version FROM flow_device_customers WHERE id=? FOR UPDATE`, c.Param("id")).Scan(&deviceID, &customerID, &current); err != nil {
		writeSQLError(c, err)
		return
	}
	if current != expected {
		writeSQLError(c, errVersionConflict)
		return
	}
	if input.DeviceID != nil {
		deviceID = strings.TrimSpace(*input.DeviceID)
	}
	if input.CustomerID != nil {
		customerID = strings.TrimSpace(*input.CustomerID)
	}
	if err := s.validateFlowCustomerBoundary(c, tx, deviceID, customerID, c.Param("id"), prefixes); err != nil {
		return
	}
	if _, err := tx.ExecContext(c, `UPDATE flow_device_customers SET device_id=?,customer_id=?,updated_by=NULLIF(?,''),row_version=row_version+1
		WHERE id=? AND row_version=?`, deviceID, customerID, actor, c.Param("id"), expected); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := replaceFlowCustomerPrefixes(c, tx, c.Param("id"), prefixes); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := s.syncFlowClassificationProfile(c, tx, actor); err != nil {
		writeFlowEnrichmentError(c, err)
		return
	}
	if err := insertFlowEnrichmentAudit(c, tx, actor, "flow.customer_boundary.updated", "flow_customer_boundary", c.Param("id"),
		map[string]any{"device_id": deviceID, "customer_id": customerID, "source_range_count": len(prefixes)}); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (s *Server) deleteFlowCustomerBoundary(c *gin.Context) {
	expected, supplied, err := ifMatch(c)
	if !supplied || err != nil || expected == 0 {
		fail(c, http.StatusPreconditionRequired, "precondition_required", "If-Match with a positive row version is required")
		return
	}
	actor := currentPrincipal(c).UserID
	tx, err := s.db.BeginTx(c, nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(c, `DELETE FROM flow_device_customers WHERE id=? AND row_version=?`, c.Param("id"), expected)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		writeSQLError(c, errVersionConflict)
		return
	}
	if err := s.syncFlowClassificationProfile(c, tx, actor); err != nil {
		writeFlowEnrichmentError(c, err)
		return
	}
	if err := insertFlowEnrichmentAudit(c, tx, actor, "flow.customer_boundary.deleted", "flow_customer_boundary", c.Param("id"), nil); err != nil {
		writeSQLError(c, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func normalizeFlowCustomerRanges(values []string) ([]netip.Prefix, error) {
	unique := make(map[netip.Prefix]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return nil, fmt.Errorf("invalid customer source CIDR %q", value)
		}
		unique[prefix.Masked()] = struct{}{}
	}
	if len(unique) == 0 {
		return nil, errors.New("at least one customer source CIDR is required")
	}
	result := make([]netip.Prefix, 0, len(unique))
	for prefix := range unique {
		result = append(result, prefix)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Addr() != result[j].Addr() {
			return result[i].Addr().Less(result[j].Addr())
		}
		return result[i].Bits() < result[j].Bits()
	})
	return result, nil
}

func (s *Server) validateFlowCustomerBoundary(c *gin.Context, tx *sql.Tx, deviceID, customerID, excludingID string, prefixes []netip.Prefix) error {
	if deviceID == "" || customerID == "" || len(deviceID) > 26 || len(customerID) > 26 {
		fail(c, http.StatusBadRequest, "invalid_request", "device_id and customer_id are required")
		return errors.New("invalid device or customer")
	}
	var kind string
	var bindingCount int
	if err := tx.QueryRowContext(c, `SELECT d.kind,(SELECT COUNT(*) FROM flow_exporter_bindings f WHERE f.device_id=d.id AND f.enabled=1)
		FROM devices d WHERE d.id=?`, deviceID).Scan(&kind, &bindingCount); err != nil {
		writeSQLError(c, err)
		return err
	}
	if canonicalDeviceKind(kind) != "network" || bindingCount == 0 {
		fail(c, http.StatusUnprocessableEntity, "flow_device_required", "device must be a network device with an enabled Flow exporter binding")
		return errors.New("device is not Flow-enabled")
	}
	var partyKind, status string
	if err := tx.QueryRowContext(c, `SELECT kind,status FROM parties WHERE id=?`, customerID).Scan(&partyKind, &status); err != nil {
		writeSQLError(c, err)
		return err
	}
	if partyKind != "customer" || status != "active" {
		fail(c, http.StatusUnprocessableEntity, "active_customer_required", "customer must be an active customer party")
		return errors.New("party is not an active customer")
	}
	rows, err := tx.QueryContext(c, `SELECT p.cidr,b.customer_id FROM flow_customer_source_prefixes p
		JOIN flow_device_customers b ON b.id=p.device_customer_id WHERE b.device_id=? AND b.id<>?`, deviceID, excludingID)
	if err != nil {
		writeSQLError(c, err)
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cidr, owner string
		if err := rows.Scan(&cidr, &owner); err != nil {
			writeSQLError(c, err)
			return err
		}
		existing, err := netip.ParsePrefix(cidr)
		if err != nil {
			writeSQLError(c, err)
			return err
		}
		for _, prefix := range prefixes {
			if existing.Addr().BitLen() == prefix.Addr().BitLen() && (existing.Contains(prefix.Addr()) || prefix.Contains(existing.Addr())) {
				fail(c, http.StatusConflict, "customer_source_overlap", fmt.Sprintf("source range %s overlaps %s owned by another customer on this device", prefix, existing))
				return errors.New("customer source ranges overlap")
			}
		}
	}
	return rows.Err()
}

func replaceFlowCustomerPrefixes(ctx context.Context, tx *sql.Tx, bindingID string, prefixes []netip.Prefix) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM flow_customer_source_prefixes WHERE device_customer_id=?`, bindingID); err != nil {
		return err
	}
	for _, prefix := range prefixes {
		family := 6
		if prefix.Addr().Is4() {
			family = 4
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO flow_customer_source_prefixes
			(id,device_customer_id,cidr,family,prefix_length) VALUES (?,?,?,?,?)`, newID(), bindingID, prefix.String(), family, prefix.Bits()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) syncFlowClassificationProfile(ctx context.Context, tx *sql.Tx, actor string) error {
	rows, err := tx.QueryContext(ctx, `SELECT b.device_id,p.id FROM flow_device_customers b
		JOIN flow_customer_source_prefixes p ON p.device_customer_id=b.id ORDER BY b.device_id,p.id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	byDevice := map[string][]string{}
	for rows.Next() {
		var deviceID, prefixID string
		if err := rows.Scan(&deviceID, &prefixID); err != nil {
			return err
		}
		byDevice[deviceID] = append(byDevice[deviceID], prefixID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(byDevice) == 0 {
		_, err := tx.ExecContext(ctx, `DELETE FROM flow_classification_profiles WHERE id=1`)
		return err
	}
	deviceIDs := make([]string, 0, len(byDevice))
	for deviceID := range byDevice {
		deviceIDs = append(deviceIDs, deviceID)
	}
	sort.Strings(deviceIDs)
	draft := flowClassificationProfileDraft{DeviceProfiles: make([]flowClassificationDeviceProfileDraft, 0, len(deviceIDs))}
	for _, deviceID := range deviceIDs {
		draft.DeviceProfiles = append(draft.DeviceProfiles, flowClassificationDeviceProfileDraft{DeviceID: deviceID, SourcePrefixIDs: byDevice[deviceID]})
	}
	draft, digest, err := normalizeFlowClassificationProfile(draft)
	if err != nil {
		return err
	}
	profilesJSON, _ := json.Marshal(draft.DeviceProfiles)
	_, err = tx.ExecContext(ctx, `INSERT INTO flow_classification_profiles
		(id,home_province,home_city,home_isp_ids,home_asns,device_profiles,overseas_includes_hmt,internal_policy,transit_policy,definition_digest,created_by,updated_by)
		VALUES (1,'','',JSON_ARRAY(),JSON_ARRAY(),CAST(? AS JSON),0,'count','count',?,NULLIF(?,''),NULLIF(?,''))
		ON DUPLICATE KEY UPDATE device_profiles=VALUES(device_profiles),definition_digest=VALUES(definition_digest),
		updated_by=VALUES(updated_by),row_version=row_version+1`, profilesJSON, digest, actor, actor)
	return err
}
