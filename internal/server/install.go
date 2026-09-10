package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	clickhousemigration "github.com/cloudcache/watchdog/deploy/migration/clickhouse"
	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/gin-gonic/gin"
)

var errAlreadyInstalled = errors.New("watchdog is already installed")

type installRequest struct {
	Username    string `json:"username"`
	Password    string `json:"password"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

func (r *installRequest) normalizeAndValidate() error {
	r.Username = strings.TrimSpace(r.Username)
	r.Email = strings.TrimSpace(r.Email)
	r.DisplayName = strings.TrimSpace(r.DisplayName)
	if r.DisplayName == "" {
		r.DisplayName = "Administrator"
	}
	if r.Username == "" || len([]byte(r.Username)) > 190 {
		return errors.New("username is required and must not exceed 190 bytes")
	}
	if !validPassword(r.Password) {
		return errors.New("password must be 8 to 72 bytes")
	}
	if len([]byte(r.Email)) > 190 || len([]byte(r.DisplayName)) > 190 {
		return errors.New("email and display_name must not exceed 190 bytes")
	}
	return nil
}

// installWatchdog is the only interactive fresh-install mutation. Runtime
// startup is read-only with respect to an empty schema: without an explicit
// install request (or WATCHDOG_ADMIN_PASSWORD for unattended installs), the
// server exposes only its public install/health surface.
func (s *Server) installWatchdog(c *gin.Context) {
	var request installRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "a JSON install request is required")
		return
	}
	if err := request.normalizeAndValidate(); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := s.completeFreshInstall(c.Request.Context(), request); err != nil {
		switch {
		case errors.Is(err, errAlreadyInstalled):
			fail(c, http.StatusConflict, "already_installed", err.Error())
		default:
			fail(c, http.StatusInternalServerError, "install_failed", err.Error())
		}
		return
	}
	status, err := GetInstallStatus(c.Request.Context(), s.db)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	status.RuntimeReady = s.runtimeReady.Load()
	c.JSON(http.StatusCreated, status)
}

func (s *Server) completeFreshInstall(ctx context.Context, request installRequest) error {
	if err := request.normalizeAndValidate(); err != nil {
		return err
	}
	s.installMu.Lock()
	defer s.installMu.Unlock()

	status, err := GetInstallStatus(ctx, s.db)
	if err != nil {
		return fmt.Errorf("read install status: %w", err)
	}
	if status.Installed {
		return errAlreadyInstalled
	}
	if err := s.prepareRuntime(ctx); err != nil {
		s.stopRuntime()
		return err
	}
	if err := s.bootstrapFirstAdmin(ctx, request); err != nil {
		s.stopRuntime()
		return err
	}
	s.installed.Store(true)
	s.runtimeReady.Store(true)
	return nil
}

func (s *Server) bootstrapFirstAdmin(ctx context.Context, request installRequest) error {
	hash, err := hashPassword(request.Password)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var bootstrapped bool
	if err := tx.QueryRowContext(ctx,
		`SELECT admin_bootstrapped FROM watchdog_installation WHERE id = 1 FOR UPDATE`).Scan(&bootstrapped); err != nil {
		return err
	}
	if bootstrapped {
		return errAlreadyInstalled
	}
	var userCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&userCount); err != nil {
		return err
	}
	if userCount != 0 {
		return errors.New("uninstalled database already contains users; resolve the inconsistent state before retrying")
	}
	var roleID string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM roles WHERE name = ?`, roleAdministrator).Scan(&roleID); err != nil {
		return err
	}
	userID := newID()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO users (id, username, email, display_name, password_hash, status)
		VALUES (?, ?, ?, ?, ?, 'active')`,
		userID, request.Username, request.Email, request.DisplayName, hash); err != nil {
		return fmt.Errorf("create administrator: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)`, userID, roleID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE watchdog_installation
		SET admin_bootstrapped = 1, installed_at = UTC_TIMESTAMP(3)
		WHERE id = 1`,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Server) applyClickHouseSchema(ctx context.Context) error {
	if strings.TrimSpace(s.cfg.ClickHouse.Address) == "" || strings.TrimSpace(s.cfg.ClickHouse.Database) == "" {
		return nil
	}
	if s.cfg.ClickHouse.Database != "watchdog_flow" {
		return fmt.Errorf("ClickHouse installation database must be watchdog_flow, got %q", s.cfg.ClickHouse.Database)
	}
	password, err := clickHousePassword(s.cfg.ClickHouse.PasswordFile)
	if err != nil {
		return err
	}
	native, err := flowch.NewNativeInserter(ctx, flowch.NativeConfig{
		Address: s.cfg.ClickHouse.Address, Database: "default", User: s.cfg.ClickHouse.Username, Password: password,
		ClientName: "watchdog-installer", MaxConns: 1, MinConns: 1, OperationTimeout: time.Hour,
	})
	if err != nil {
		return fmt.Errorf("connect ClickHouse installer: %w", err)
	}
	defer native.Close()
	migrations, err := flowch.LoadMigrations(clickhousemigration.Files, ".")
	if err != nil {
		return err
	}
	migrator, err := flowch.NewMigrator(native)
	if err != nil {
		return err
	}
	owner := "install-" + sha256hex(randomToken())[:32]
	if _, err := migrator.Apply(ctx, migrations, flowch.MigrationApplyOptions{LockOwner: owner}); err != nil {
		return fmt.Errorf("apply ClickHouse schema: %w", err)
	}
	return nil
}
