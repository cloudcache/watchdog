package server

import (
	"database/sql"
	"net/http"
	"strings"

	"github.com/cloudcache/watchdog/internal/watchdog"
	"github.com/gin-gonic/gin"
)

func (s *Server) listMIBModules(c *gin.Context) {
	rows, err := s.db.QueryContext(c.Request.Context(), `SELECT id,name,source,version,checksum,enabled,created_at,updated_at FROM mib_modules ORDER BY source,name`)
	if err != nil {
		writeSQLError(c, err)
		return
	}
	defer rows.Close()
	items := []watchdog.MIBModule{}
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
	var module watchdog.MIBModule
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
	if len(module.Name) > 190 || len(module.Source) > 64 || len(module.Version) > 64 || len(module.Checksum) > 128 {
		fail(c, http.StatusBadRequest, "invalid_request", "mib module field is too long")
		return
	}
	if module.ID == "" {
		module.ID = watchdog.ID(stableManagementID("mib", module.Source, module.Name))
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

func (s *Server) readMIBModule(c *gin.Context, id string) (watchdog.MIBModule, error) {
	return scanMIBModule(s.db.QueryRowContext(c.Request.Context(), `SELECT id,name,source,version,checksum,enabled,created_at,updated_at FROM mib_modules WHERE id=?`, id))
}

func scanMIBModule(row rowScanner) (watchdog.MIBModule, error) {
	var value watchdog.MIBModule
	err := row.Scan(&value.ID, &value.Name, &value.Source, &value.Version, &value.Checksum, &value.Enabled, &value.CreatedAt, &value.UpdatedAt)
	return value, err
}
