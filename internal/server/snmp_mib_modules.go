package server

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/cloudcache/watchdog/internal/snmpdomain"
	"github.com/gin-gonic/gin"
)

func (s *Server) listMIBModules(c *gin.Context) {
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT id,name,source,version,checksum,enabled,created_at,updated_at FROM mib_modules ORDER BY source,name`)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []snmpdomain.MIBModule{}
	for rows.Next() {
		item, err := scanMIBModule(rows)
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
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func (s *Server) putMIBModule(c *gin.Context) {
	var module snmpdomain.MIBModule
	if err := c.ShouldBindJSON(&module); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid body")
		return
	}
	module.Name = strings.TrimSpace(module.Name)
	module.Source = strings.TrimSpace(module.Source)
	module.Version = strings.TrimSpace(module.Version)
	module.Checksum = strings.TrimSpace(module.Checksum)
	if module.Name == "" || module.Checksum == "" {
		fail(c, http.StatusBadRequest, "invalid_request", "mib module name and checksum are required")
		return
	}
	if module.Source == "" {
		module.Source = "librenms"
	}
	if snmpdomain.IsEmbeddedSNMPMIBSource(module.Source) {
		fail(c, http.StatusConflict, "protected", "built-in MIB modules are managed by the watchdog binary")
		return
	}
	if len(module.Name) > 190 || len(module.Source) > 64 || len(module.Version) > 64 || len(module.Checksum) > 128 {
		fail(c, http.StatusBadRequest, "invalid_request", "mib module field is too long")
		return
	}
	if module.ID == "" {
		module.ID = snmpdomain.ID(stableManagementID("mib", module.Source, module.Name))
	}
	if len(module.ID) > 26 {
		fail(c, http.StatusBadRequest, "invalid_request", "mib module id must not exceed 26 characters")
		return
	}
	_, err := s.db.ExecContext(c.Request.Context(), `INSERT INTO mib_modules (id,name,source,version,checksum,enabled)
		VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE name=VALUES(name),source=VALUES(source),version=VALUES(version),
		checksum=VALUES(checksum),enabled=VALUES(enabled),row_version=row_version+1`, module.ID, module.Name, module.Source, module.Version, module.Checksum, module.Enabled)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	saved, err := s.readMIBModule(c, string(module.ID))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "snmp.mib_module.upsert", "mib_module", string(saved.ID))
	c.JSON(http.StatusOK, saved)
}

func (s *Server) deleteMIBModule(c *gin.Context) {
	var source string
	if err := s.db.QueryRowContext(c.Request.Context(), `SELECT source FROM mib_modules WHERE id=?`, c.Param("module_id")).Scan(&source); err != nil {
		writeSQLError(c, err)
		return
	}
	if snmpdomain.IsEmbeddedSNMPMIBSource(source) {
		fail(c, http.StatusConflict, "protected", "built-in MIB modules cannot be deleted")
		return
	}
	result, err := s.db.ExecContext(c.Request.Context(), `DELETE FROM mib_modules WHERE id=?`, c.Param("module_id"))
	if err != nil {
		writeSQLError(c, err)
		return
	}
	if changed, _ := result.RowsAffected(); changed == 0 {
		writeSQLError(c, sql.ErrNoRows)
		return
	}
	s.audit(c.Request.Context(), currentPrincipal(c).UserID, "snmp.mib_module.delete", "mib_module", c.Param("module_id"))
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (s *Server) readMIBModule(c *gin.Context, id string) (snmpdomain.MIBModule, error) {
	return scanMIBModule(s.db.QueryRowContext(c.Request.Context(), `SELECT id,name,source,version,checksum,enabled,created_at,updated_at FROM mib_modules WHERE id=?`, id))
}

func scanMIBModule(row rowScanner) (snmpdomain.MIBModule, error) {
	var value snmpdomain.MIBModule
	err := row.Scan(&value.ID, &value.Name, &value.Source, &value.Version, &value.Checksum, &value.Enabled, &value.CreatedAt, &value.UpdatedAt)
	value.Builtin = snmpdomain.IsEmbeddedSNMPMIBSource(value.Source)
	return value, err
}

func (s *Server) ensureBuiltinMIBModules(ctx context.Context) error {
	modules, err := snmpdomain.EmbeddedSNMPMIBModules()
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, module := range modules {
		module.ID = snmpdomain.ID(stableManagementID("mib", module.Source, module.Name))
		if _, err := tx.ExecContext(ctx, `INSERT INTO mib_modules (id,name,source,version,checksum,enabled)
			VALUES (?,?,?,?,?,1) ON DUPLICATE KEY UPDATE version=VALUES(version),checksum=VALUES(checksum)`,
			module.ID, module.Name, module.Source, module.Version, module.Checksum); err != nil {
			return fmt.Errorf("seed embedded mib %s: %w", module.Name, err)
		}
	}
	return tx.Commit()
}
