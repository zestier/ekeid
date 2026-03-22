package store

import "errors"

// schemaVersion identifies the current database table structure. Bump this
// value whenever the schema changes (new tables, altered columns, new
// indexes, foreign keys, etc.). On startup the Writer drops and recreates
// all tables when the stored version does not match, and the Reader reports a
// degraded state so the API can return 503.
const schemaVersion = "v3-entity-table"

// ErrSchemaMismatch is returned by Reader methods when the database was
// created with a different schema version. The API server should map this
// to an HTTP 503 response.
var ErrSchemaMismatch = errors.New("database schema mismatch")

// SchemaVersion returns the current schema version string.
func SchemaVersion() string {
	return schemaVersion
}
