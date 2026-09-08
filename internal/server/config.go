package server

import (
	"os"
	"strings"
)

// Config is the minimal runtime configuration for the KISS watchdog-server.
// MySQL is the only management authority; ClickHouse is optional at this stage
// (its baseline is applied by the SNMP/log work packages).
type Config struct {
	ListenAddr     string   // e.g. "127.0.0.1:8090"
	MySQLDSN       string   // go-sql-driver DSN; multiStatements not required
	ClickHouseAddr string   // optional; native addr host:port
	AllowedOrigins []string // CORS allowlist for the separate frontend build
	StaticDir      string   // optional: serve the built UI (deployment convenience only)
}

// LoadConfig reads configuration from the environment with safe defaults.
func LoadConfig() Config {
	return Config{
		ListenAddr:     getenv("WATCHDOG_LISTEN", "127.0.0.1:8090"),
		MySQLDSN:       getenv("WATCHDOG_MYSQL_DSN", "root:@tcp(127.0.0.1:3306)/watchdog_dev?parseTime=true&loc=UTC&charset=utf8mb4"),
		ClickHouseAddr: getenv("WATCHDOG_CLICKHOUSE_ADDR", ""),
		AllowedOrigins: splitList(getenv("WATCHDOG_ORIGINS", "http://127.0.0.1:8090")),
		StaticDir:      getenv("WATCHDOG_STATIC_DIR", ""),
	}
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitList(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
