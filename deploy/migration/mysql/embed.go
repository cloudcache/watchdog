package mysqlmigrations

import "embed"

// Files is the ordered watchdog MySQL migration set embedded into every
// platform binary so startup does not depend on the process working directory.
//
//go:embed *.sql
var Files embed.FS
