// Package migrations holds the PostgreSQL schema as numbered up/down SQL pairs
// (NNNN_name.up.sql, NNNN_name.down.sql). mftiko-migrate embeds them and applies the up files
// (E12-D027). Down files are for tests; nothing applies them in a deployment (E12-D028).
// Agents do not migrate at startup.
package migrations

import "embed"

// FS holds every migration file.
//
//go:embed *.sql
var FS embed.FS
