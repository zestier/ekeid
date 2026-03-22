package store

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	_ "modernc.org/sqlite"
)

// Writer provides read-write access to the database (used by the watcher).
type Writer struct {
	db *sql.DB
}

// NewWriter opens a SQLite database in read-write mode and initializes the schema.
func NewWriter(dbPath string) (*Writer, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	if err := configureSQLite(db); err != nil {
		db.Close()
		return nil, err
	}

	if err := createSchema(db); err != nil {
		db.Close()
		return nil, err
	}

	return &Writer{db: db}, nil
}

func configureSQLite(db *sql.DB) error {
	// SQLite only allows one writer at a time. Limiting Go's connection pool
	// to a single connection prevents "database is locked" errors when
	// multiple goroutines write concurrently.
	db.SetMaxOpenConns(1)

	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("exec %q: %w", p, err)
		}
	}
	return nil
}

func createSchema(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS entity (
		wikidata_id INTEGER PRIMARY KEY,
		viewed_at   INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS mapping (
		property    INTEGER NOT NULL,
		value       TEXT NOT NULL,
		wikidata_id INTEGER NOT NULL REFERENCES entity(wikidata_id) ON DELETE CASCADE,
		PRIMARY KEY (property, value)
	);

	CREATE INDEX IF NOT EXISTS idx_mapping_wikidata ON mapping(wikidata_id);

	CREATE TABLE IF NOT EXISTS sync_state (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS failed_entity (
		wikidata_id     INTEGER PRIMARY KEY,
		error           TEXT NOT NULL,
		attempts        INTEGER NOT NULL DEFAULT 1,
		first_failed_at TEXT NOT NULL,
		last_failed_at  TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS pending_entity (
		wikidata_id INTEGER PRIMARY KEY
	);`
	_, err := db.Exec(schema)
	if err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return nil
}

// MigrateSchema checks the stored schema version and drops all tables if it
// does not match the current SchemaVersion(). This ensures the database
// structure is always consistent with the running code. After dropping,
// tables are recreated and the new version is stored.
func (w *Writer) MigrateSchema() error {
	expected := SchemaVersion()

	// Try to read the stored version. If sync_state doesn't exist yet
	// (brand-new DB) the query will fail — treat that as a mismatch.
	var stored string
	err := w.db.QueryRow(`SELECT value FROM sync_state WHERE key = 'schema_version'`).Scan(&stored)
	log.Printf("Expected schema version: %s, found schema version: %s", expected, stored)
	if err == nil && stored == expected {
		return nil // schema is current
	}

	// Drop all user tables and recreate. Query sqlite_master so we never
	// leave orphaned tables from a previous schema version.
	rows, err := w.db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	rows.Close()
	for _, t := range tables {
		log.Printf("Schema migration: dropping table %s", t)
		if _, err := w.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", t)); err != nil {
			return fmt.Errorf("drop table %s: %w", t, err)
		}
	}

	if err := createSchema(w.db); err != nil {
		return err
	}

	return w.SetSyncState("schema_version", expected)
}

// EntityRecord holds the data for a single entity upsert within a batch.
type EntityRecord struct {
	WikidataID  string
	ExternalIDs map[int][]string // property ID → values
	Modified    time.Time        // entity's last-modified time (used as viewed_at)
}

// UpsertEntity inserts or updates an entity and its mappings atomically.
func (w *Writer) UpsertEntity(wikidataID string, externalIDs map[int][]string) error {
	return w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  wikidataID,
		ExternalIDs: externalIDs,
	}})
}

// UpsertEntitiesBatch inserts or updates multiple entities in a single transaction.
// This amortises transaction overhead across the entire batch, which is
// dramatically faster than individual UpsertEntity calls during bulk imports.
// Each record's Modified time is used as viewed_at. The stored viewed_at is
// never decreased (SQL max ensures monotonicity).
func (w *Writer) UpsertEntitiesBatch(records []EntityRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	entityStmt, err := tx.Prepare(`
		INSERT INTO entity (wikidata_id, viewed_at) VALUES (?, ?)
		ON CONFLICT(wikidata_id) DO UPDATE SET
			viewed_at = max(excluded.viewed_at, entity.viewed_at)
	`)
	if err != nil {
		return fmt.Errorf("prepare entity upsert: %w", err)
	}
	defer entityStmt.Close()

	deleteStmt, err := tx.Prepare(`DELETE FROM mapping WHERE wikidata_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare delete: %w", err)
	}
	defer deleteStmt.Close()

	idStmt, err := tx.Prepare(`
		INSERT INTO mapping (property, value, wikidata_id)
		VALUES (?, ?, ?)
		ON CONFLICT(property, value) DO UPDATE SET
			wikidata_id = excluded.wikidata_id
	`)
	if err != nil {
		return fmt.Errorf("prepare mapping insert: %w", err)
	}
	defer idStmt.Close()

	for _, rec := range records {
		id, err := qidToInt(rec.WikidataID)
		if err != nil {
			return fmt.Errorf("convert QID %s: %w", rec.WikidataID, err)
		}
		viewedAt := rec.Modified.Unix()
		if viewedAt <= 0 {
			viewedAt = time.Now().Unix()
		}
		if _, err := entityStmt.Exec(id, viewedAt); err != nil {
			return fmt.Errorf("upsert entity %s: %w", rec.WikidataID, err)
		}
		if _, err := deleteStmt.Exec(id); err != nil {
			return fmt.Errorf("delete old mappings for %s: %w", rec.WikidataID, err)
		}
		for property, values := range rec.ExternalIDs {
			for _, val := range values {
				if _, err := idStmt.Exec(property, val, id); err != nil {
					return fmt.Errorf("insert mapping (P%d, %s): %w", property, val, err)
				}
			}
		}
	}

	return tx.Commit()
}

// SeedEntitiesBatch inserts or updates multiple entities during a seed import.
// Unlike UpsertEntitiesBatch, it uses dumpTime (not the entity's Modified time)
// for viewed_at, and only writes mappings when the entity is new or when the
// entity's Modified time is newer than the existing viewed_at. This avoids
// overwriting fresher data that was written by the event stream between seeds.
func (w *Writer) SeedEntitiesBatch(dumpTime time.Time, records []EntityRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	queryStmt, err := tx.Prepare(`SELECT viewed_at FROM entity WHERE wikidata_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare viewed_at query: %w", err)
	}
	defer queryStmt.Close()

	entityStmt, err := tx.Prepare(`
		INSERT INTO entity (wikidata_id, viewed_at) VALUES (?, ?)
		ON CONFLICT(wikidata_id) DO UPDATE SET
			viewed_at = max(excluded.viewed_at, entity.viewed_at)
	`)
	if err != nil {
		return fmt.Errorf("prepare entity upsert: %w", err)
	}
	defer entityStmt.Close()

	deleteStmt, err := tx.Prepare(`DELETE FROM mapping WHERE wikidata_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare delete: %w", err)
	}
	defer deleteStmt.Close()

	idStmt, err := tx.Prepare(`
		INSERT INTO mapping (property, value, wikidata_id)
		VALUES (?, ?, ?)
		ON CONFLICT(property, value) DO UPDATE SET
			wikidata_id = excluded.wikidata_id
	`)
	if err != nil {
		return fmt.Errorf("prepare mapping insert: %w", err)
	}
	defer idStmt.Close()

	dumpUnix := dumpTime.Unix()

	for _, rec := range records {
		id, err := qidToInt(rec.WikidataID)
		if err != nil {
			return fmt.Errorf("convert QID %s: %w", rec.WikidataID, err)
		}

		// Check existing viewed_at to decide what needs updating.
		var existingViewedAt int64
		isNew := true
		if err := queryStmt.QueryRow(id).Scan(&existingViewedAt); err == nil {
			isNew = false
		}

		needMappings := isNew || rec.Modified.Unix() > existingViewedAt
		needEntityUpsert := needMappings || dumpUnix > existingViewedAt

		if !needEntityUpsert && !needMappings {
			continue
		}

		// Set viewed_at = max(dumpTime, in-db viewed_at).
		if needEntityUpsert {
			if _, err := entityStmt.Exec(id, dumpUnix); err != nil {
				return fmt.Errorf("upsert entity %s: %w", rec.WikidataID, err)
			}
		}

		// Only write mappings if entity is new or its Modified time is
		// newer than what was already stored.
		if needMappings {
			if _, err := deleteStmt.Exec(id); err != nil {
				return fmt.Errorf("delete old mappings for %s: %w", rec.WikidataID, err)
			}
			for property, values := range rec.ExternalIDs {
				for _, val := range values {
					if _, err := idStmt.Exec(property, val, id); err != nil {
						return fmt.Errorf("insert mapping (P%d, %s): %w", property, val, err)
					}
				}
			}
		}
	}

	return tx.Commit()
}

// DeleteEntity removes an entity and its mappings.
func (w *Writer) DeleteEntity(wikidataID string) error {
	return w.DeleteEntitiesBatch([]string{wikidataID})
}

// DeleteEntitiesBatch removes multiple entities and their mappings in a
// single transaction. Mapping rows are removed via CASCADE.
func (w *Writer) DeleteEntitiesBatch(qids []string) error {
	if len(qids) == 0 {
		return nil
	}
	tx, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`DELETE FROM entity WHERE wikidata_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare delete: %w", err)
	}
	defer stmt.Close()

	for _, qid := range qids {
		id, err := qidToInt(qid)
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(id); err != nil {
			return fmt.Errorf("delete entity %s: %w", qid, err)
		}
	}
	return tx.Commit()
}

// SweepStaleEntities deletes entities (and their mappings via CASCADE)
// whose viewed_at is older than beforeUnix. Returns the number of entity
// rows removed.
func (w *Writer) SweepStaleEntities(beforeUnix int64) (int64, error) {
	result, err := w.db.Exec(`DELETE FROM entity WHERE viewed_at < ?`, beforeUnix)
	if err != nil {
		return 0, fmt.Errorf("sweep stale entities: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// ClearSyncCursors resets the stream resumption state (dump_time,
// last_event_id) so the next startup triggers a reseed.
func (w *Writer) ClearSyncCursors() error {
	for _, key := range []string{"dump_time", "last_event_id"} {
		if err := w.SetSyncState(key, ""); err != nil {
			return fmt.Errorf("clear %s: %w", key, err)
		}
	}
	return nil
}

// SetSyncState stores a key-value pair in the sync_state table.
func (w *Writer) SetSyncState(key, value string) error {
	_, err := w.db.Exec(`
		INSERT INTO sync_state (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, key, value)
	return err
}

// GetSyncState retrieves a value from the sync_state table.
func (w *Writer) GetSyncState(key string) (string, error) {
	var value string
	err := w.db.QueryRow(`SELECT value FROM sync_state WHERE key = ?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

// RecordFailedEntity records a processing failure for later retry.
// If the entity already has a failure record, it increments the attempt count.
func (w *Writer) RecordFailedEntity(wikidataID, errMsg string) error {
	id, err := qidToInt(wikidataID)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err = w.db.Exec(`
		INSERT INTO failed_entity (wikidata_id, error, attempts, first_failed_at, last_failed_at)
		VALUES (?, ?, 1, ?, ?)
		ON CONFLICT(wikidata_id) DO UPDATE SET
			error = excluded.error,
			attempts = failed_entity.attempts + 1,
			last_failed_at = excluded.last_failed_at
	`, id, errMsg, now, now)
	return err
}

// LastFailedEntities returns up to limit failed entity IDs for retry, oldest first.
func (w *Writer) LastFailedEntities(limit int) ([]string, error) {
	rows, err := w.db.Query(
		`SELECT wikidata_id FROM failed_entity ORDER BY last_failed_at ASC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var qids []string
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		qids = append(qids, intToQid(id))
	}
	return qids, rows.Err()
}

// DeleteFailedEntity removes a failure record after successful retry.
func (w *Writer) DeleteFailedEntity(wikidataID string) error {
	id, err := qidToInt(wikidataID)
	if err != nil {
		return err
	}
	_, err = w.db.Exec(`DELETE FROM failed_entity WHERE wikidata_id = ?`, id)
	return err
}

// EnqueueEntities inserts QIDs into the pending_entity queue.
// Duplicates are silently ignored via INSERT OR IGNORE.
// Returns the number of rows actually inserted.
func (w *Writer) EnqueueEntities(qids []string) (int, error) {
	if len(qids) == 0 {
		return 0, nil
	}
	tx, err := w.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO pending_entity (wikidata_id) VALUES (?)`)
	if err != nil {
		return 0, fmt.Errorf("prepare enqueue: %w", err)
	}
	defer stmt.Close()

	var inserted int
	for _, qid := range qids {
		id, err := qidToInt(qid)
		if err != nil {
			return 0, err
		}
		result, err := stmt.Exec(id)
		if err != nil {
			return 0, fmt.Errorf("enqueue %s: %w", qid, err)
		}
		n, _ := result.RowsAffected()
		inserted += int(n)
	}
	return inserted, tx.Commit()
}

// PeekPendingBatch returns up to limit QIDs from the pending queue
// without removing them. Use DeletePendingEntities to remove after processing.
func (w *Writer) PeekPendingBatch(limit int) ([]string, error) {
	rows, err := w.db.Query(`SELECT wikidata_id FROM pending_entity LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("dequeue pending: %w", err)
	}
	defer rows.Close()

	var qids []string
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan pending: %w", err)
		}
		qids = append(qids, intToQid(id))
	}
	return qids, rows.Err()
}

// DeletePendingEntities removes the given QIDs from the pending queue.
func (w *Writer) DeletePendingEntities(qids []string) error {
	if len(qids) == 0 {
		return nil
	}
	tx, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`DELETE FROM pending_entity WHERE wikidata_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare delete pending: %w", err)
	}
	defer stmt.Close()

	for _, qid := range qids {
		id, err := qidToInt(qid)
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(id); err != nil {
			return fmt.Errorf("delete pending %s: %w", qid, err)
		}
	}
	return tx.Commit()
}

// PendingCount returns the number of entities in the pending queue.
func (w *Writer) PendingCount() (int64, error) {
	var count int64
	err := w.db.QueryRow(`SELECT COUNT(*) FROM pending_entity`).Scan(&count)
	return count, err
}

// Close closes the database connection.
func (w *Writer) Close() error {
	return w.db.Close()
}

// DB returns the underlying database connection (for testing).
func (w *Writer) DB() *sql.DB {
	return w.db
}

// qidToInt converts a Wikidata QID string (e.g., "Q172241") to an integer (e.g., 172241).
func qidToInt(qid string) (int64, error) {
	if len(qid) == 0 || (qid[0] != 'Q' && qid[0] != 'q') {
		return 0, fmt.Errorf("invalid QID format: %s", qid)
	}
	n, err := strconv.ParseInt(qid[1:], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse QID number: %w", err)
	}
	return n, nil
}

// intToQid converts an integer (e.g., 172241) to a Wikidata QID string (e.g., "Q172241").
func intToQid(n int64) string {
	return fmt.Sprintf("Q%d", n)
}
