// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package clickhousemigration

import "embed"

// Files binds the immutable migration set to the exact migration binary.
//
//go:embed *.sql
var Files embed.FS
