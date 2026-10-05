// Package migrations holds the PostgreSQL schema as numbered up/down SQL pairs
// (NNNN_name.up.sql, NNNN_name.down.sql). They are embedded into mftiko-migrate, which is the only
// thing that applies them: a deploy step, not something agents do at startup.
package migrations

import "embed"

// FS holds every migration file.
//
//go:embed *.sql
var FS embed.FS
