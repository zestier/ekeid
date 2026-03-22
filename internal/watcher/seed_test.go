package watcher

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/store"
)

// buildDumpEntityJSON constructs a Wikidata dump-format entity JSON line.
// This is the bare entity format (no {"entities":{...}} wrapper).
// Each property is given datatype "external-id" so extractExternalIDs picks it up.
func buildDumpEntityJSON(qid, label string, properties map[string]string) []byte {
	claims := make(map[string]interface{})
	for propID, value := range properties {
		claims[propID] = []map[string]interface{}{
			{
				"mainsnak": map[string]interface{}{
					"snaktype": "value",
					"property": propID,
					"datatype": "external-id",
					"datavalue": map[string]interface{}{
						"value": value,
						"type":  "string",
					},
				},
			},
		}
	}

	entity := map[string]interface{}{
		"type": "item",
		"id":   qid,
		"labels": map[string]interface{}{
			"en": map[string]string{
				"language": "en",
				"value":    label,
			},
		},
		"claims":   claims,
		"modified": "2026-03-09T00:00:00Z",
	}

	data, _ := json.Marshal(entity)
	return data
}

func TestParseDumpEntity_Movie(t *testing.T) {
	data := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	entity, err := parseDumpEntity(data)
	if err != nil {
		t.Fatalf("parseDumpEntity: %v", err)
	}
	if entity == nil {
		t.Fatal("expected non-nil entity")
	}
	if entity.ID != "Q172241" {
		t.Errorf("ID = %q, want Q172241", entity.ID)
	}
	if v := entity.ExternalIDs[345]; len(v) != 1 || v[0] != "tt0111161" {
		t.Errorf("P345 = %v, want [tt0111161]", v)
	}
	if v := entity.ExternalIDs[4947]; len(v) != 1 || v[0] != "278" {
		t.Errorf("P4947 = %v, want [278]", v)
	}
}

func TestParseDumpEntity_TVSeries(t *testing.T) {
	data := buildDumpEntityJSON("Q1396", "Breaking Bad", map[string]string{
		"P345":  "tt0903747",
		"P4983": "1396",
		"P4835": "81189",
	})

	entity, err := parseDumpEntity(data)
	if err != nil {
		t.Fatalf("parseDumpEntity: %v", err)
	}
	if entity.ID != "Q1396" {
		t.Errorf("ID = %q, want Q1396", entity.ID)
	}
	if v := entity.ExternalIDs[345]; len(v) != 1 || v[0] != "tt0903747" {
		t.Errorf("P345 = %v, want [tt0903747]", v)
	}
	if v := entity.ExternalIDs[4983]; len(v) != 1 || v[0] != "1396" {
		t.Errorf("P4983 = %v, want [1396]", v)
	}
	if v := entity.ExternalIDs[4835]; len(v) != 1 || v[0] != "81189" {
		t.Errorf("P4835 = %v, want [81189]", v)
	}
}

func TestParseDumpEntity_NoExternalIDs(t *testing.T) {
	// Entity with no external-id claims should return nil (skipped).
	entity := map[string]interface{}{
		"type": "item",
		"id":   "Q42",
		"labels": map[string]interface{}{
			"en": map[string]string{"language": "en", "value": "Douglas Adams"},
		},
		"claims": map[string]interface{}{
			"P31": []map[string]interface{}{
				{
					"mainsnak": map[string]interface{}{
						"snaktype": "value",
						"property": "P31",
						"datatype": "wikibase-item",
						"datavalue": map[string]interface{}{
							"value": map[string]string{"id": "Q5"},
							"type":  "wikibase-entityid",
						},
					},
				},
			},
		},
	}
	data, _ := json.Marshal(entity)

	result, err := parseDumpEntity(data)
	if err != nil {
		t.Fatalf("parseDumpEntity: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for entity with no external IDs, got %+v", result)
	}
}

func TestParseDumpEntity_InvalidJSON(t *testing.T) {
	_, err := parseDumpEntity([]byte(`{invalid json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// TestParseDumpEntity_ExtractsModified verifies the modified field is parsed.
func TestParseDumpEntity_ExtractsModified(t *testing.T) {
	data := buildDumpEntityJSON("Q1", "Test", map[string]string{"P345": "tt1"})
	entity, err := parseDumpEntity(data)
	if err != nil {
		t.Fatalf("parseDumpEntity: %v", err)
	}
	if entity == nil {
		t.Fatal("expected non-nil entity")
	}
	expected := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	if !entity.Modified.Equal(expected) {
		t.Errorf("Modified = %v, want %v", entity.Modified, expected)
	}
}

// TestParseDumpEntity_MissingModified verifies zero time when modified is absent.
func TestParseDumpEntity_MissingModified(t *testing.T) {
	entity := map[string]interface{}{
		"type": "item",
		"id":   "Q1",
		"claims": map[string]interface{}{
			"P345": []map[string]interface{}{
				{
					"mainsnak": map[string]interface{}{
						"snaktype": "value",
						"property": "P345",
						"datatype": "external-id",
						"datavalue": map[string]interface{}{
							"value": "tt1",
							"type":  "string",
						},
					},
				},
			},
		},
	}
	data, _ := json.Marshal(entity)

	result, err := parseDumpEntity(data)
	if err != nil {
		t.Fatalf("parseDumpEntity: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil entity")
	}
	if !result.Modified.IsZero() {
		t.Errorf("Modified = %v, want zero time", result.Modified)
	}
}

// buildDumpEntityJSONWithModified constructs a dump-format entity JSON with
// a custom modified timestamp.
func buildDumpEntityJSONWithModified(qid string, properties map[string]string, modified string) []byte {
	claims := make(map[string]interface{})
	for propID, value := range properties {
		claims[propID] = []map[string]interface{}{
			{
				"mainsnak": map[string]interface{}{
					"snaktype": "value",
					"property": propID,
					"datatype": "external-id",
					"datavalue": map[string]interface{}{
						"value": value,
						"type":  "string",
					},
				},
			},
		}
	}

	entity := map[string]interface{}{
		"type":     "item",
		"id":       qid,
		"claims":   claims,
		"modified": modified,
	}

	data, _ := json.Marshal(entity)
	return data
}

// TestProcessDumpStream_ViewedAtMaxDumpTime verifies that when an entity's
// Modified is older than dumpTime, viewed_at is set to dumpTime.
func TestProcessDumpStream_ViewedAtMaxDumpTime(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	// Entity modified in 2025, older than dumpTime
	entity := buildDumpEntityJSONWithModified("Q1", map[string]string{"P345": "tt1"}, "2025-01-01T00:00:00Z")

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(entity)
	dumpData.WriteString("\n]\n")

	seeder := NewSeeder(writer, nil, DumpFormatGZ)
	_, _, err = seeder.processDumpStream(context.Background(), &dumpData, 0, nil, dumpTime, 0)
	if err != nil {
		t.Fatalf("processDumpStream: %v", err)
	}

	// Sweep at dumpTime should NOT remove this entity because
	// viewed_at was set to dumpTime (which is >= dumpTime)
	swept, err := writer.SweepStaleEntities(dumpTime.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 0 {
		t.Errorf("swept = %d, want 0 (viewed_at should be dumpTime)", swept)
	}
}

// TestProcessDumpStream_ViewedAtEntityModified verifies that when an entity's
// Modified is newer than dumpTime, viewed_at is set to entity.Modified.
func TestProcessDumpStream_ViewedAtEntityModified(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	entityModified := "2026-06-01T00:00:00Z"
	entityModifiedTime, _ := time.Parse(time.RFC3339, entityModified)
	entity := buildDumpEntityJSONWithModified("Q1", map[string]string{"P345": "tt1"}, entityModified)

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(entity)
	dumpData.WriteString("\n]\n")

	seeder := NewSeeder(writer, nil, DumpFormatGZ)
	_, _, err = seeder.processDumpStream(context.Background(), &dumpData, 0, nil, dumpTime, 0)
	if err != nil {
		t.Fatalf("processDumpStream: %v", err)
	}

	// Sweep at a time between dumpTime and entityModifiedTime should
	// NOT sweep the entity because viewed_at = entity.Modified (newer)
	midTime := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	swept, err := writer.SweepStaleEntities(midTime.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 0 {
		t.Errorf("swept = %d, want 0 (viewed_at should be entity.Modified=%v)", swept, entityModifiedTime)
	}
}

// TestProcessDumpStream_EventStreamEntitySurvivesSweep tests the key scenario:
// an entity updated via event stream with a newer Modified time should NOT be
// swept by a subsequent dump seed.
func TestProcessDumpStream_EventStreamEntitySurvivesSweep(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	eventStreamTime := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC) // much newer

	// Pre-populate an entity as if it arrived via event stream
	err = writer.UpsertEntitiesBatch([]store.EntityRecord{{
		WikidataID:  "Q999",
		ExternalIDs: map[int][]string{345: {"tt999"}},
		Modified:    eventStreamTime,
	}})
	if err != nil {
		t.Fatalf("event stream upsert: %v", err)
	}

	// Q999 is NOT in the dump (simulating entity not in dump)
	entity1 := buildDumpEntityJSONWithModified("Q1", map[string]string{"P345": "tt1"}, "2026-01-01T00:00:00Z")

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(entity1)
	dumpData.WriteString("\n]\n")

	seeder := NewSeeder(writer, nil, DumpFormatGZ)
	_, _, err = seeder.processDumpStream(context.Background(), &dumpData, 0, nil, dumpTime, 0)
	if err != nil {
		t.Fatalf("processDumpStream: %v", err)
	}

	// Sweep at dumpTime — Q999 should survive because its viewed_at (eventStreamTime)
	// is newer than dumpTime
	swept, err := writer.SweepStaleEntities(dumpTime.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 0 {
		t.Error("expected event-stream entity to survive dump sweep")
	}

	reader := store.NewReaderFromDB(writer.DB())
	result, err := reader.LookupByProperty(345, "tt999")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Error("event-stream entity should still exist after dump sweep")
	}
}

// TestConfigFingerprintDeterministic verifies configFingerprint is stable.
func TestConfigFingerprintDeterministic(t *testing.T) {
	h1 := configFingerprint()
	h2 := configFingerprint()
	if h1 != h2 {
		t.Errorf("configFingerprint not deterministic: %q != %q", h1, h2)
	}
	if len(h1) == 0 {
		t.Error("configFingerprint returned empty string")
	}
}

// TestConfigFingerprintDiffersFromSchemaVersion verifies that config_hash and
// schema_version are independent (different strings).
func TestConfigFingerprintDiffersFromSchemaVersion(t *testing.T) {
	cfgHash := configFingerprint()
	schemaVersion := store.SchemaVersion()
	if cfgHash == schemaVersion {
		t.Errorf("configFingerprint = SchemaVersion = %q; they should be independent", cfgHash)
	}
}

// TestParseDumpEntity_UnparseableModified verifies that an invalid modified
// date is silently treated as zero time, not an error.
func TestParseDumpEntity_UnparseableModified(t *testing.T) {
	entity := map[string]interface{}{
		"type": "item",
		"id":   "Q1",
		"claims": map[string]interface{}{
			"P345": []map[string]interface{}{
				{
					"mainsnak": map[string]interface{}{
						"snaktype": "value",
						"property": "P345",
						"datatype": "external-id",
						"datavalue": map[string]interface{}{
							"value": "tt1",
							"type":  "string",
						},
					},
				},
			},
		},
		"modified": "not-a-real-date",
	}
	data, _ := json.Marshal(entity)

	result, err := parseDumpEntity(data)
	if err != nil {
		t.Fatalf("parseDumpEntity should not error on bad modified: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil entity")
	}
	if !result.Modified.IsZero() {
		t.Errorf("Modified = %v, want zero time for unparseable date", result.Modified)
	}
}

func compressBZ2(t *testing.T, data []byte) []byte {
	// bzip2 package in stdlib is decompress-only, so we use the gzip format
	// for tests that need compression. For bz2-specific tests, we use
	// a real bzip2 stream via exec if available, otherwise skip.
	t.Helper()
	// Try using external bzip2 command
	cmd := exec.Command("bzip2")
	cmd.Stdin = bytes.NewReader(data)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		t.Skipf("bzip2 command not available: %v", err)
	}
	return out.Bytes()
}

func compressGZ(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestProcessDumpStream(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})
	tv := buildDumpEntityJSON("Q1396", "Breaking Bad", map[string]string{
		"P345":  "tt0903747",
		"P4983": "1396",
		"P4835": "81189",
	})

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(movie)
	dumpData.WriteString(",\n")
	dumpData.Write(tv)
	dumpData.WriteString("\n]\n")

	seeder := NewSeeder(writer, nil, DumpFormatGZ)
	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	imported, lines, err := seeder.processDumpStream(context.Background(), &dumpData, 0, nil, dumpTime, 0)
	if err != nil {
		t.Fatalf("processDumpStream: %v", err)
	}
	if imported != 2 {
		t.Errorf("imported = %d, want 2", imported)
	}
	if lines < 4 {
		t.Errorf("lines = %d, want >= 4", lines)
	}

	reader := store.NewReaderFromDB(writer.DB())

	result, err := reader.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected movie to be found")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}

	result, err = reader.LookupByProperty(345, "tt0903747")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected TV series to be found")
	}
	if result.WikidataID != 1396 {
		t.Errorf("WikidataID = %d, want 1396", result.WikidataID)
	}
}

func TestProcessDumpStreamBZ2(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(movie)
	dumpData.WriteString("\n]\n")

	compressed := compressBZ2(t, dumpData.Bytes())
	decompressed := bzip2.NewReader(bytes.NewReader(compressed))

	seeder := NewSeeder(writer, nil, DumpFormatBZ2)
	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	imported, _, err := seeder.processDumpStream(context.Background(), decompressed, 0, nil, dumpTime, 0)
	if err != nil {
		t.Fatalf("processDumpStream: %v", err)
	}
	if imported != 1 {
		t.Errorf("imported = %d, want 1", imported)
	}
}

func TestProcessDumpStreamGZ(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(movie)
	dumpData.WriteString("\n]\n")

	compressed := compressGZ(t, dumpData.Bytes())
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	seeder := NewSeeder(writer, nil, DumpFormatGZ)
	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	imported, _, err := seeder.processDumpStream(context.Background(), reader, 0, nil, dumpTime, 0)
	if err != nil {
		t.Fatalf("processDumpStream: %v", err)
	}
	if imported != 1 {
		t.Errorf("imported = %d, want 1", imported)
	}
}

func TestSeederSeedWithMockServer(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	var dumpBuf bytes.Buffer
	dumpBuf.WriteString("[\n")
	dumpBuf.Write(movie)
	dumpBuf.WriteString("\n]\n")

	compressed := compressGZ(t, dumpBuf.Bytes())

	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Last-Modified", dumpTime.Format(http.TimeFormat))
		w.Write(compressed)
	}))
	defer server.Close()

	seeder := NewSeeder(writer, server.Client(), DumpFormatGZ)
	seeder.dumpURL = server.URL

	err = seeder.Seed(context.Background())
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}

	reader := store.NewReaderFromDB(writer.DB())

	result, err := reader.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected entity to be found")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}

	// Verify sync timestamps
	state, err := writer.GetSyncState("state")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if state != "seeding" {
		t.Errorf("state = %q, want seeding", state)
	}

	dumpTimeStr, err := writer.GetSyncState("dump_time")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	expectedDumpTime := dumpTime.UTC().Format(time.RFC3339)
	if dumpTimeStr != expectedDumpTime {
		t.Errorf("dump_time = %q, want %q", dumpTimeStr, expectedDumpTime)
	}

	configHash, err := writer.GetSyncState("config_hash")
	if err != nil {
		t.Fatalf("GetSyncState config_hash: %v", err)
	}
	if configHash != configFingerprint() {
		t.Errorf("config_hash = %q, want %q", configHash, configFingerprint())
	}
}

func TestSeederNeedsSeed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	seeder := NewSeeder(writer, nil, "")

	// Empty DB should need seed
	needs, err := seeder.NeedsSeed()
	if err != nil {
		t.Fatalf("NeedsSeed: %v", err)
	}
	if !needs {
		t.Error("empty DB should need seed")
	}

	// Recent sync with current config should not need seed
	writer.SetSyncState("dump_time", "2026-03-11T00:00:00Z")
	writer.SetSyncState("config_hash", configFingerprint())
	needs, err = seeder.NeedsSeed()
	if err != nil {
		t.Fatalf("NeedsSeed: %v", err)
	}
	if needs {
		t.Error("recent sync should not need seed")
	}

	// Stale config hash should trigger reseed
	writer.SetSyncState("config_hash", "stale-hash")
	needs, err = seeder.NeedsSeed()
	if err != nil {
		t.Fatalf("NeedsSeed: %v", err)
	}
	if !needs {
		t.Error("stale config hash should need seed")
	}
}

func TestParseDumpTime(t *testing.T) {
	// Valid Last-Modified header: returns the raw timestamp
	got := parseDumpTime("Tue, 10 Mar 2026 12:00:00 GMT")
	want := "2026-03-10T12:00:00Z"
	if got != want {
		t.Errorf("parseDumpTime(valid) = %q, want %q", got, want)
	}

	// Empty header falls back to ~now
	got = parseDumpTime("")
	parsed, err := time.Parse(time.RFC3339, got)
	if err != nil {
		t.Fatalf("failed to parse fallback: %v", err)
	}
	if time.Since(parsed) > 5*time.Second {
		t.Errorf("fallback should be ~now, got %v ago", time.Since(parsed))
	}

	// Unparseable header also falls back to ~now
	got = parseDumpTime("not-a-date")
	parsed, err = time.Parse(time.RFC3339, got)
	if err != nil {
		t.Fatalf("failed to parse fallback: %v", err)
	}
	if time.Since(parsed) > 5*time.Second {
		t.Errorf("fallback should be ~now, got %v ago", time.Since(parsed))
	}
}

func TestSeederSeedServerError(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, "service unavailable")
	}))
	defer server.Close()

	seeder := NewSeeder(writer, server.Client(), DumpFormatBZ2)
	seeder.dumpURL = server.URL

	err = seeder.Seed(context.Background())
	if err == nil {
		t.Fatal("expected error for server error response")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error should mention status code: %v", err)
	}
}

func TestSeederSeedSweepsStaleEntities(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	// Pre-populate a "stale" entity with an old Modified time
	staleTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	err = writer.UpsertEntitiesBatch([]store.EntityRecord{{
		WikidataID:  "Q999999",
		ExternalIDs: map[int][]string{345: {"tt9999999"}},
		Modified:    staleTime,
	}})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch stale: %v", err)
	}

	// Build dump with only one entity
	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	var dumpBuf bytes.Buffer
	dumpBuf.WriteString("[\n")
	dumpBuf.Write(movie)
	dumpBuf.WriteString("\n]\n")

	compressed := compressGZ(t, dumpBuf.Bytes())

	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Last-Modified", dumpTime.Format(http.TimeFormat))
		w.Write(compressed)
	}))
	defer server.Close()

	seeder := NewSeeder(writer, server.Client(), DumpFormatGZ)
	seeder.dumpURL = server.URL

	err = seeder.Seed(context.Background())
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}

	reader := store.NewReaderFromDB(writer.DB())

	// The imported entity should exist
	result, err := reader.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty imported: %v", err)
	}
	if result == nil {
		t.Fatal("expected imported entity to exist")
	}

	// The stale entity should have been swept
	stale, err := reader.LookupByProperty(345, "tt9999999")
	if err != nil {
		t.Fatalf("LookupByProperty stale: %v", err)
	}
	if stale != nil {
		t.Error("expected stale entity to be swept after seed")
	}
}

// --- Resumable download tests ---

// failingReader returns the first n bytes normally, then returns an error.
type failingReader struct {
	data []byte
	pos  int
	fail int // byte offset at which to fail
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	if r.pos >= r.fail {
		return 0, fmt.Errorf("simulated connection drop")
	}
	n := copy(p, r.data[r.pos:])
	if r.pos+n > r.fail {
		n = r.fail - r.pos
	}
	r.pos += n
	return n, nil
}

func (r *failingReader) Close() error { return nil }

func TestResumableBodyResumesOnDrop(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefghij"), 100) // 1000 bytes

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := requestCount.Add(1)
		if r.Header.Get("If-Match") != "" {
			// Resume request — validate ETag
			if r.Header.Get("If-Match") != `"test-etag"` {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start int
			fmt.Sscanf(rangeHeader, "bytes=%d-", &start)
			w.Header().Set("ETag", `"test-etag"`)
			w.WriteHeader(http.StatusPartialContent)
			w.Write(data[start:])
			return
		}
		// First request: serve first half then close
		w.Header().Set("ETag", `"test-etag"`)
		if req == 1 {
			w.Write(data[:500])
			// Connection will close, causing an error on client side
			return
		}
		w.Write(data)
	}))
	defer server.Close()

	// Create a resumableBody with a reader that fails at byte 500
	body := io.NopCloser(&failingReader{data: data, fail: 500})
	rb := newResumableBody(body, server.URL, `"test-etag"`, server.Client())

	result, err := io.ReadAll(rb)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("got %d bytes, want %d bytes", len(result), len(data))
	}
}

func TestResumableBodyETagMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Match") != "" {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", `"original-etag"`)
		w.Write([]byte("data"))
	}))
	defer server.Close()

	// Create a body that fails immediately
	body := io.NopCloser(&failingReader{data: []byte("data"), fail: 0})
	rb := newResumableBody(body, server.URL, `"original-etag"`, server.Client())

	_, err := io.ReadAll(rb)
	if !errors.Is(err, ErrDumpChanged) {
		t.Errorf("expected ErrDumpChanged, got: %v", err)
	}
}

func TestResumableBodyNoETagFallback(t *testing.T) {
	data := []byte("hello world")
	body := io.NopCloser(bytes.NewReader(data))
	// Empty etag means resumption is disabled
	rb := newResumableBody(body, "http://example.com", "", http.DefaultClient)

	result, err := io.ReadAll(rb)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("got %q, want %q", result, data)
	}
}

func TestSeederSeedResumesOnDrop(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	var dumpBuf bytes.Buffer
	dumpBuf.WriteString("[\n")
	dumpBuf.Write(movie)
	dumpBuf.WriteString("\n]\n")

	compressed := compressGZ(t, dumpBuf.Bytes())

	var requestCount atomic.Int32
	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := requestCount.Add(1)
		w.Header().Set("ETag", `"dump-etag-123"`)
		w.Header().Set("Last-Modified", dumpTime.Format(http.TimeFormat))
		w.Header().Set("Content-Type", "application/octet-stream")

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			if r.Header.Get("If-Match") != `"dump-etag-123"` {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			var start int
			fmt.Sscanf(rangeHeader, "bytes=%d-", &start)
			if start >= len(compressed) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			w.Write(compressed[start:])
			return
		}

		// First request: cut off partway through
		if req == 1 {
			cutoff := len(compressed) / 2
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(compressed)))
			w.Write(compressed[:cutoff])
			return
		}
		w.Write(compressed)
	}))
	defer server.Close()

	seeder := NewSeeder(writer, server.Client(), DumpFormatGZ)
	seeder.dumpURL = server.URL

	err = seeder.Seed(context.Background())
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}

	// Should have made more than 1 request (initial + resume)
	if requestCount.Load() < 2 {
		t.Errorf("expected at least 2 requests (initial + resume), got %d", requestCount.Load())
	}

	reader := store.NewReaderFromDB(writer.DB())
	result, err := reader.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected entity to be found")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}
}
