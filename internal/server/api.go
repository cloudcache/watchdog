package server

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
)

func pageParams(c *gin.Context) (int, int) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "25"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	if limit < 1 {
		limit = 25
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func sortDirection(c *gin.Context) string {
	if strings.EqualFold(c.Query("order"), "asc") {
		return "ASC"
	}
	return "DESC"
}

func etag(version uint64) string {
	return `"` + strconv.FormatUint(version, 10) + `"`
}

func ifMatch(c *gin.Context) (uint64, bool, error) {
	raw := strings.TrimSpace(c.GetHeader("If-Match"))
	if raw == "" {
		return 0, false, nil
	}
	raw = strings.TrimPrefix(raw, "W/")
	raw = strings.Trim(raw, `"`)
	v, err := strconv.ParseUint(raw, 10, 64)
	return v, true, err
}

func writeSQLError(c *gin.Context, err error) {
	if errors.Is(err, sql.ErrNoRows) {
		fail(c, http.StatusNotFound, "not_found", "record not found")
		return
	}
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) && myErr.Number == 1062 {
		fail(c, http.StatusConflict, "conflict", "record already exists")
		return
	}
	fail(c, http.StatusInternalServerError, "internal", err.Error())
}

func nullableTime(t sql.NullTime) any {
	if !t.Valid {
		return nil
	}
	return t.Time.UTC().Format(time.RFC3339Nano)
}

func principalUserID(c *gin.Context) any {
	if p := currentPrincipal(c); p != nil {
		return p.UserID
	}
	return nil
}

