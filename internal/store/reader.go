package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ekeid/ekeid/internal/model"
	_ "modernc.org/sqlite"
)

// Reader provides read-only access to the database (used by the API server).
type Reader struct {
	db       *sql.DB
	schemaOK bool
}

// NewReader opens a SQLite database in read-only mode.
func NewReader(dbPath string) (*Reader, error) {
	dsn := dbPath + "?mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	for _, p := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("exec %q: %w", p, err)
		}
	}

	return &Reader{db: db, schemaOK: checkSchemaVersion(db)}, nil
}

// NewReaderFromDB creates a Reader wrapping an existing *sql.DB (for testing).
func NewReaderFromDB(db *sql.DB) *Reader {
	return &Reader{db: db, schemaOK: checkSchemaVersion(db)}
}

// checkSchemaVersion returns true if the stored schema_version matches the
// current SchemaVersion(). Returns false on any error (missing table, etc.).
func checkSchemaVersion(db *sql.DB) bool {
	var stored string
	err := db.QueryRow(`SELECT value FROM sync_state WHERE key = 'schema_version'`).Scan(&stored)
	return err == nil && stored == SchemaVersion()
}

// LookupByProperty finds the entity mapped to (property, value) and returns
// all of that entity's mappings.
func (r *Reader) LookupByProperty(property int, value string) (*model.LookupResult, error) {
	if !r.schemaOK {
		return nil, ErrSchemaMismatch
	}
	rows, err := r.db.Query(`
		SELECT wikidata_id, property, value FROM mapping
		WHERE wikidata_id = (
			SELECT wikidata_id FROM mapping
			WHERE property = ? AND value = ?
		)
	`, property, value)
	if err != nil {
		return nil, fmt.Errorf("query mappings: %w", err)
	}
	defer rows.Close()

	var wikidataID int64
	mappings := make(map[int][]string)
	for rows.Next() {
		var p int
		var v string
		if err := rows.Scan(&wikidataID, &p, &v); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		mappings[p] = append(mappings[p], v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	if len(mappings) == 0 {
		return nil, nil
	}

	return &model.LookupResult{
		WikidataID: wikidataID,
		Mappings:   mappings,
	}, nil
}

// GetHealth returns lightweight health information without expensive COUNT queries.
func (r *Reader) GetHealth() (*model.HealthInfo, error) {
	var pageCount, pageSize int64
	r.db.QueryRow(`PRAGMA page_count`).Scan(&pageCount)
	r.db.QueryRow(`PRAGMA page_size`).Scan(&pageSize)

	info := &model.HealthInfo{
		DatabaseSize: pageCount * pageSize,
		SchemaMatch:  r.schemaOK,
	}

	dumpTime, eventTime, err := r.syncTimes()
	if err != nil {
		return nil, err
	}
	info.DumpTime = dumpTime
	info.LastEventSync = eventTime

	var stateStr sql.NullString
	err = r.db.QueryRow(`SELECT value FROM sync_state WHERE key = 'state'`).Scan(&stateStr)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("get state: %w", err)
	}
	if stateStr.Valid {
		info.State = stateStr.String
	}

	return info, nil
}

// GetStats returns database statistics.
func (r *Reader) GetStats() (*model.DBStats, error) {
	var mappingCount int64

	err := r.db.QueryRow(`SELECT COUNT(*) FROM mapping`).Scan(&mappingCount)
	if err != nil {
		return nil, fmt.Errorf("count mappings: %w", err)
	}

	var pageCount, pageSize int64
	r.db.QueryRow(`PRAGMA page_count`).Scan(&pageCount)
	r.db.QueryRow(`PRAGMA page_size`).Scan(&pageSize)

	stats := &model.DBStats{
		MappingCount: mappingCount,
		DatabaseSize: pageCount * pageSize,
	}

	dumpTime, eventTime, err := r.syncTimes()
	if err != nil {
		return nil, err
	}
	stats.DumpTime = dumpTime
	stats.LastEventSync = eventTime

	var stateStr sql.NullString
	err = r.db.QueryRow(`SELECT value FROM sync_state WHERE key = 'state'`).Scan(&stateStr)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("get state: %w", err)
	}
	if stateStr.Valid {
		stats.State = stateStr.String
	}

	r.db.QueryRow(`SELECT COUNT(*) FROM pending_entity`).Scan(&stats.PendingCount)
	r.db.QueryRow(`SELECT COUNT(*) FROM failed_entity`).Scan(&stats.FailedCount)
	r.db.QueryRow(`SELECT COUNT(*) FROM entity`).Scan(&stats.EntityCount)

	return stats, nil
}

// syncTimes reads dump_time and last_event_id from sync_state and returns
// the parsed dump time and event stream time.
func (r *Reader) syncTimes() (dumpTime, eventTime time.Time, err error) {
	var dumpTimeStr, eventIDStr sql.NullString

	err = r.db.QueryRow(`SELECT value FROM sync_state WHERE key = 'dump_time'`).Scan(&dumpTimeStr)
	if err != nil && err != sql.ErrNoRows {
		return time.Time{}, time.Time{}, fmt.Errorf("get dump_time: %w", err)
	}
	err = r.db.QueryRow(`SELECT value FROM sync_state WHERE key = 'last_event_id'`).Scan(&eventIDStr)
	if err != nil && err != sql.ErrNoRows {
		return time.Time{}, time.Time{}, fmt.Errorf("get last_event_id: %w", err)
	}

	if dumpTimeStr.Valid && dumpTimeStr.String != "" {
		if t, parseErr := time.Parse(time.RFC3339, dumpTimeStr.String); parseErr == nil {
			dumpTime = t
		}
	}
	if eventIDStr.Valid && eventIDStr.String != "" {
		eventTime = eventIDToTime(eventIDStr.String)
	}

	return dumpTime, eventTime, nil
}

// eventIDToTime extracts the earliest Kafka timestamp from an SSE event ID.
// Event IDs are JSON arrays like:
//
//	[{"topic":"...","partition":0,"offset":-1},{"topic":"...","partition":0,"timestamp":1773879470875}]
//
// Entries without a timestamp (offset-only, for inactive DCs) are ignored.
// Timestamps are milliseconds since epoch. Returns zero time if no entry
// contains a timestamp.
func eventIDToTime(eventID string) time.Time {
	var offsets []struct {
		Timestamp *int64 `json:"timestamp"`
	}
	if json.Unmarshal([]byte(eventID), &offsets) != nil {
		return time.Time{}
	}
	var minTS int64
	for _, o := range offsets {
		if o.Timestamp == nil || *o.Timestamp <= 0 {
			continue
		}
		if minTS == 0 || *o.Timestamp < minTS {
			minTS = *o.Timestamp
		}
	}
	if minTS == 0 {
		return time.Time{}
	}
	return time.UnixMilli(minTS)
}

// LookupByWikidataID finds the entity by its Wikidata numeric ID and returns
// all of its mappings.
func (r *Reader) LookupByWikidataID(wikidataID int64) (*model.LookupResult, error) {
	if !r.schemaOK {
		return nil, ErrSchemaMismatch
	}
	rows, err := r.db.Query(`
		SELECT property, value FROM mapping
		WHERE wikidata_id = ?
	`, wikidataID)
	if err != nil {
		return nil, fmt.Errorf("query mappings: %w", err)
	}
	defer rows.Close()

	mappings := make(map[int][]string)
	for rows.Next() {
		var p int
		var v string
		if err := rows.Scan(&p, &v); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		mappings[p] = append(mappings[p], v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	if len(mappings) == 0 {
		return nil, nil
	}

	return &model.LookupResult{
		WikidataID: wikidataID,
		Mappings:   mappings,
	}, nil
}

// Close closes the database connection.
func (r *Reader) Close() error {
	return r.db.Close()
}
