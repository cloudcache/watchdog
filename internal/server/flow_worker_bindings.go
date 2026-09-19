package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

var errFlowWorkerBindingRequired = errors.New("a Flow worker must be selected for the device")

type flowWorkerDeviceBinding struct {
	DeviceID   string    `json:"device_id"`
	DeviceName string    `json:"device_name"`
	DeviceHost string    `json:"device_host"`
	WorkerID   string    `json:"worker_id"`
	WorkerName string    `json:"worker_name"`
	RowVersion uint64    `json:"row_version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (s *Server) registerFlowWorkerBindingRoutes(auth *gin.RouterGroup) {
	routes := auth.Group("/flow/worker-device-bindings")
	routes.GET("", s.requirePermission("address.view"), s.listFlowWorkerDeviceBindings)
	routes.PUT("/:device_id", s.requirePermission("address.manage"), s.putFlowWorkerDeviceBinding)
	routes.DELETE("/:device_id", s.requirePermission("address.manage"), s.deleteFlowWorkerDeviceBinding)
}

func (s *Server) listFlowWorkerDeviceBindings(c *gin.Context) {
	page, ok := parseInventoryPage(c, []string{"device_id", "worker_id"}, map[string]string{
		"device":      "COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host)",
		"device_host": "d.host", "worker": "a.name", "created_at": "b.created_at", "updated_at": "b.updated_at",
	}, "device")
	if !ok {
		return
	}
	where, args := []string{"1=1"}, []any{}
	for _, filter := range []struct{ param, column string }{{"device_id", "b.device_id"}, {"worker_id", "b.worker_id"}} {
		if value := strings.TrimSpace(c.Query(filter.param)); value != "" {
			where, args = append(where, filter.column+"=?"), append(args, value)
		}
	}
	if q := strings.TrimSpace(c.Query("q")); q != "" {
		like := "%" + escapeLike(q) + "%"
		where = append(where, `(d.host LIKE ? OR d.display_name LIKE ? OR d.sys_name LIKE ? OR a.name LIKE ?)`)
		args = append(args, like, like, like, like)
	}
	join := ` FROM flow_worker_device_bindings b JOIN devices d ON d.id=b.device_id JOIN agents a ON a.id=b.worker_id`
	clause := " WHERE " + strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRowContext(c, "SELECT COUNT(*)"+join+clause, args...).Scan(&total); err != nil {
		writeSQLError(c, err)
		return
	}
	rows, err := s.db.QueryContext(c, `SELECT b.device_id,COALESCE(NULLIF(d.display_name,''),NULLIF(d.sys_name,''),d.host),d.host,
		b.worker_id,a.name,b.row_version,b.created_at,b.updated_at`+join+clause+` ORDER BY `+page.Sort+` `+page.Order+`,b.device_id `+page.Order+` LIMIT ? OFFSET ?`,
		append(append([]any{}, args...), page.Limit, page.Offset)...)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := make([]flowWorkerDeviceBinding, 0, page.Limit)
	for rows.Next() {
		var item flowWorkerDeviceBinding
		if err := rows.Scan(&item.DeviceID, &item.DeviceName, &item.DeviceHost, &item.WorkerID, &item.WorkerName,
			&item.RowVersion, &item.CreatedAt, &item.UpdatedAt); err != nil {
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

func (s *Server) putFlowWorkerDeviceBinding(c *gin.Context) {
	var input struct {
		WorkerID string `json:"worker_id"`
	}
	if !decodeStrictBody(c, &input) {
		return
	}
	tx, err := s.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer tx.Rollback()
	var previousWorkerID string
	previousErr := tx.QueryRowContext(c.Request.Context(), `SELECT worker_id FROM flow_worker_device_bindings WHERE device_id=? FOR UPDATE`, c.Param("device_id")).Scan(&previousWorkerID)
	if previousErr != nil && !errors.Is(previousErr, sql.ErrNoRows) {
		writeSQLError(c, previousErr)
		return
	}
	workerID, err := ensureFlowWorkerDeviceBinding(c.Request.Context(), tx, c.Param("device_id"), input.WorkerID, currentPrincipal(c).UserID)
	if err != nil {
		writeFlowWorkerBindingError(c, err)
		return
	}
	var job any
	if previousWorkerID != workerID {
		result, err := tx.ExecContext(c.Request.Context(), `UPDATE flow_classification_profiles
			SET row_version=row_version+1,updated_by=NULLIF(?,'') WHERE id=1`, currentPrincipal(c).UserID)
		if err != nil {
			writeSQLError(c, err)
			return
		}
		if affected, _ := result.RowsAffected(); affected != 0 {
			queued, err := enqueueFlowEnrichmentPublishTx(c.Request.Context(), tx, currentPrincipal(c).UserID, time.Time{})
			if err != nil {
				writeFlowEnrichmentError(c, err)
				return
			}
			job = queued
		}
	}
	if err := tx.Commit(); err != nil {
		writeSQLError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"device_id": c.Param("device_id"), "worker_id": workerID, "publication_job": job})
}

func (s *Server) deleteFlowWorkerDeviceBinding(c *gin.Context) {
	var customerCount int
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT COUNT(*) FROM flow_device_customers WHERE device_id=?`, c.Param("device_id")).Scan(&customerCount); err != nil {
		writeSQLError(c, err)
		return
	}
	if customerCount != 0 {
		fail(c, http.StatusConflict, "flow_worker_in_use", "remove or move the device customer boundaries before removing its Flow worker binding")
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `DELETE FROM flow_worker_device_bindings WHERE device_id=?`, c.Param("device_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		fail(c, http.StatusNotFound, "not_found", "Flow worker binding was not found")
		return
	}
	c.Status(http.StatusNoContent)
}

func ensureFlowWorkerDeviceBinding(ctx context.Context, tx *sql.Tx, deviceID, requestedWorkerID, actor string) (string, error) {
	deviceID, requestedWorkerID = strings.TrimSpace(deviceID), strings.TrimSpace(requestedWorkerID)
	if deviceID == "" {
		return "", errFlowWorkerBindingRequired
	}
	var deviceKind string
	if err := tx.QueryRowContext(ctx, `SELECT kind FROM devices WHERE id=?`, deviceID).Scan(&deviceKind); err != nil {
		return "", err
	}
	if canonicalDeviceKind(deviceKind) != "network" {
		return "", fmt.Errorf("%w: device is not a network device", errFlowWorkerBindingRequired)
	}
	if requestedWorkerID == "" {
		var existing, kind, status string
		err := tx.QueryRowContext(ctx, `SELECT b.worker_id,a.kind,a.status FROM flow_worker_device_bindings b
			JOIN agents a ON a.id=b.worker_id WHERE b.device_id=?`, deviceID).Scan(&existing, &kind, &status)
		if err == nil {
			if kind == "flow_worker" && status == "active" {
				return existing, nil
			}
			err = sql.ErrNoRows
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		requestedWorkerID, err = soleActiveFlowWorker(ctx, tx)
		if errors.Is(err, errFlowEnrichmentNoTargets) {
			return "", errFlowWorkerBindingRequired
		}
		if err != nil {
			return "", err
		}
	}
	var kind, status string
	if err := tx.QueryRowContext(ctx, `SELECT kind,status FROM agents WHERE id=?`, requestedWorkerID).Scan(&kind, &status); err != nil {
		return "", err
	}
	if kind != "flow_worker" || status != "active" {
		return "", fmt.Errorf("%w: selected agent is not an active Flow worker", errFlowWorkerBindingRequired)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO flow_worker_device_bindings (device_id,worker_id,created_by,updated_by)
		VALUES (?,?,NULLIF(?,''),NULLIF(?,'')) ON DUPLICATE KEY UPDATE worker_id=VALUES(worker_id),updated_by=VALUES(updated_by),row_version=row_version+1`,
		deviceID, requestedWorkerID, actor, actor)
	return requestedWorkerID, err
}

func writeFlowWorkerBindingError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errFlowWorkerBindingRequired):
		fail(c, http.StatusUnprocessableEntity, "flow_worker_required", err.Error())
	case errors.Is(err, sql.ErrNoRows):
		fail(c, http.StatusNotFound, "not_found", "device or Flow worker was not found")
	default:
		writeSQLError(c, err)
	}
}
