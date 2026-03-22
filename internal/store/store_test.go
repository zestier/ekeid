package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestWriter(t *testing.T) *Writer {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	w, err := NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func newTestReaderFromWriter(t *testing.T, w *Writer) *Reader {
	t.Helper()
	return NewReaderFromDB(w.DB())
}

func TestWriterCreatesSchema(t *testing.T) {
	w := newTestWriter(t)

	var count int
	err := w.db.QueryRow("SELECT COUNT(*) FROM mapping").Scan(&count)
	if err != nil {
		t.Fatalf("mapping table not created: %v", err)
	}

	err = w.db.QueryRow("SELECT COUNT(*) FROM sync_state").Scan(&count)
	if err != nil {
		t.Fatalf("sync_state table not created: %v", err)
	}
}

func TestUpsertAndLookup(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	ids := map[int][]string{
		345:  {"tt0111161"},
		4947: {"278"},
		4835: {"2095"},
		8013: {"the-shawshank-redemption"},
	}
	err := w.UpsertEntity("Q172241", ids)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Lookup by IMDb property
	result, err := r.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}
	if len(result.Mappings) != 4 {
		t.Errorf("len(Mappings) = %d, want 4", len(result.Mappings))
	}
	if vals := result.Mappings[4947]; len(vals) != 1 || vals[0] != "278" {
		t.Errorf("TMDB movie mapping = %v, want [278]", vals)
	}

	// Lookup by TMDB movie property
	result2, err := r.LookupByProperty(4947, "278")
	if err != nil {
		t.Fatalf("LookupByProperty tmdb: %v", err)
	}
	if result2 == nil {
		t.Fatal("expected result for tmdb lookup, got nil")
	}
	if result2.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result2.WikidataID)
	}
}

func TestLookupNotFound(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	result, err := r.LookupByProperty(345, "tt9999999")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %+v", result)
	}
}

func TestUpsertUpdatesEntity(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	// Insert entity with 2 IDs
	ids1 := map[int][]string{345: {"tt0903747"}, 4983: {"1396"}}
	err := w.UpsertEntity("Q1079", ids1)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Upsert with additional IDs
	ids2 := map[int][]string{345: {"tt0903747"}, 4983: {"1396"}, 2638: {"169"}, 4835: {"81189"}}
	err = w.UpsertEntity("Q1079", ids2)
	if err != nil {
		t.Fatalf("UpsertEntity (update): %v", err)
	}

	result, err := r.LookupByProperty(345, "tt0903747")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if len(result.Mappings) != 4 {
		t.Errorf("len(Mappings) = %d, want 4", len(result.Mappings))
	}
	if vals := result.Mappings[2638]; len(vals) != 1 || vals[0] != "169" {
		t.Errorf("TVMaze mapping = %v, want [169]", vals)
	}
}

func TestUpsertRemovesOldIDs(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	// Insert with 3 IDs
	ids1 := map[int][]string{345: {"tt0903747"}, 4983: {"1396"}, 2638: {"169"}}
	err := w.UpsertEntity("Q1079", ids1)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Upsert with only 2 IDs (tvmaze removed)
	ids2 := map[int][]string{345: {"tt0903747"}, 4983: {"1396"}}
	err = w.UpsertEntity("Q1079", ids2)
	if err != nil {
		t.Fatalf("UpsertEntity (update): %v", err)
	}

	result, err := r.LookupByProperty(345, "tt0903747")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if len(result.Mappings) != 2 {
		t.Errorf("len(Mappings) = %d, want 2 (tvmaze should be removed)", len(result.Mappings))
	}
	if _, ok := result.Mappings[2638]; ok {
		t.Error("TVMaze mapping should have been removed")
	}
}

func TestLookupByWikidataID(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	ids := map[int][]string{
		345:  {"tt0111161"},
		4947: {"278"},
	}
	w.UpsertEntity("Q172241", ids)

	result, err := r.LookupByWikidataID(172241)
	if err != nil {
		t.Fatalf("LookupByWikidataID: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}
	if len(result.Mappings) != 2 {
		t.Errorf("len(Mappings) = %d, want 2", len(result.Mappings))
	}
}

func TestLookupByWikidataIDNotFound(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	result, err := r.LookupByWikidataID(999999)
	if err != nil {
		t.Fatalf("LookupByWikidataID: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %+v", result)
	}
}

func TestDeleteEntity(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	ids := map[int][]string{345: {"tt0111161"}, 4947: {"278"}}
	err := w.UpsertEntity("Q172241", ids)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	err = w.DeleteEntity("Q172241")
	if err != nil {
		t.Fatalf("DeleteEntity: %v", err)
	}

	result, err := r.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil after delete, got %+v", result)
	}
}

func TestSweepStaleEntities(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	oldTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Q1 has an old viewed_at — should be swept
	err := w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q1",
		ExternalIDs: map[int][]string{345: {"tt0000001"}},
		Modified:    oldTime,
	}})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch Q1: %v", err)
	}

	// Q2 has viewed_at >= dumpTime — should survive
	err = w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q2",
		ExternalIDs: map[int][]string{345: {"tt0000002"}},
		Modified:    dumpTime,
	}})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch Q2: %v", err)
	}

	swept, err := w.SweepStaleEntities(dumpTime.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 1 {
		t.Errorf("swept = %d, want 1", swept)
	}

	result, err := r.LookupByProperty(345, "tt0000001")
	if err != nil {
		t.Fatalf("LookupByProperty old: %v", err)
	}
	if result != nil {
		t.Error("expected old entity to be swept")
	}

	result, err = r.LookupByProperty(345, "tt0000002")
	if err != nil {
		t.Fatalf("LookupByProperty new: %v", err)
	}
	if result == nil {
		t.Error("expected current entity to survive sweep")
	}
}

// TestSweepCascadesDeleteToMappings verifies that sweeping an entity also
// removes its mapping rows via the FK CASCADE constraint.
func TestSweepCascadesDeleteToMappings(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	oldTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	err := w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q1",
		ExternalIDs: map[int][]string{345: {"tt0000001"}, 4947: {"100"}},
		Modified:    oldTime,
	}})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	// Confirm mappings exist before sweep
	result, err := r.LookupByProperty(345, "tt0000001")
	if err != nil {
		t.Fatalf("LookupByProperty before sweep: %v", err)
	}
	if result == nil {
		t.Fatal("expected entity to exist before sweep")
	}
	if len(result.Mappings) != 2 {
		t.Errorf("mappings before sweep = %d, want 2", len(result.Mappings))
	}

	// Sweep everything
	swept, err := w.SweepStaleEntities(time.Now().Unix() + 1)
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 1 {
		t.Errorf("swept = %d, want 1", swept)
	}

	// Verify mappings are also gone (CASCADE)
	result, err = r.LookupByProperty(345, "tt0000001")
	if err != nil {
		t.Fatalf("LookupByProperty after sweep: %v", err)
	}
	if result != nil {
		t.Error("expected mapping to be cascaded away after entity sweep")
	}
	result, err = r.LookupByProperty(4947, "100")
	if err != nil {
		t.Fatalf("LookupByProperty P4947 after sweep: %v", err)
	}
	if result != nil {
		t.Error("expected second mapping to be cascaded away")
	}
}

// TestSweepNoStaleEntities verifies sweep is a no-op when all entities are current.
func TestSweepNoStaleEntities(t *testing.T) {
	w := newTestWriter(t)

	now := time.Now()
	err := w.UpsertEntitiesBatch([]EntityRecord{
		{WikidataID: "Q1", ExternalIDs: map[int][]string{345: {"tt1"}}, Modified: now},
		{WikidataID: "Q2", ExternalIDs: map[int][]string{345: {"tt2"}}, Modified: now},
	})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	// Sweep with a threshold older than all entities
	swept, err := w.SweepStaleEntities(now.Add(-time.Hour).Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 0 {
		t.Errorf("swept = %d, want 0 (no stale entities)", swept)
	}
}

// TestSweepAllStaleEntities verifies all entities are removed when all are stale.
func TestSweepAllStaleEntities(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	oldTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	err := w.UpsertEntitiesBatch([]EntityRecord{
		{WikidataID: "Q1", ExternalIDs: map[int][]string{345: {"tt1"}}, Modified: oldTime},
		{WikidataID: "Q2", ExternalIDs: map[int][]string{345: {"tt2"}}, Modified: oldTime},
		{WikidataID: "Q3", ExternalIDs: map[int][]string{345: {"tt3"}}, Modified: oldTime},
	})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	swept, err := w.SweepStaleEntities(time.Now().Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 3 {
		t.Errorf("swept = %d, want 3", swept)
	}

	stats, err := r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.EntityCount != 0 {
		t.Errorf("EntityCount = %d, want 0", stats.EntityCount)
	}
	if stats.MappingCount != 0 {
		t.Errorf("MappingCount = %d, want 0", stats.MappingCount)
	}
}

// TestViewedAtMonotonicity verifies that viewed_at never decreases when
// upserting the same entity with an older Modified time.
func TestViewedAtMonotonicity(t *testing.T) {
	w := newTestWriter(t)

	newerTime := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	olderTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Insert with newer timestamp
	err := w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q1",
		ExternalIDs: map[int][]string{345: {"tt1"}},
		Modified:    newerTime,
	}})
	if err != nil {
		t.Fatalf("first UpsertEntitiesBatch: %v", err)
	}

	// Upsert again with older timestamp
	err = w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q1",
		ExternalIDs: map[int][]string{345: {"tt1"}},
		Modified:    olderTime,
	}})
	if err != nil {
		t.Fatalf("second UpsertEntitiesBatch: %v", err)
	}

	// Sweep with a time between older and newer — entity should survive
	// because viewed_at stayed at newerTime (not rolled back to olderTime)
	swept, err := w.SweepStaleEntities(newerTime.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 0 {
		t.Errorf("swept = %d, want 0 (viewed_at should be monotonically increasing)", swept)
	}
}

// TestViewedAtIncreases verifies that viewed_at does increase when
// upserting an entity with a newer Modified time.
func TestViewedAtIncreases(t *testing.T) {
	w := newTestWriter(t)

	olderTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	newerTime := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	// Insert with older timestamp
	err := w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q1",
		ExternalIDs: map[int][]string{345: {"tt1"}},
		Modified:    olderTime,
	}})
	if err != nil {
		t.Fatalf("first UpsertEntitiesBatch: %v", err)
	}

	// Upsert again with newer timestamp
	err = w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q1",
		ExternalIDs: map[int][]string{345: {"tt1"}},
		Modified:    newerTime,
	}})
	if err != nil {
		t.Fatalf("second UpsertEntitiesBatch: %v", err)
	}

	// Sweep with a time between older and newer — entity should survive
	midTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	swept, err := w.SweepStaleEntities(midTime.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 0 {
		t.Errorf("swept = %d, want 0 (viewed_at should have been updated to newerTime)", swept)
	}
}

// TestUpsertEntityWithZeroModified verifies that a zero Modified time
// gets a fallback value (time.Now) rather than epoch 0.
func TestUpsertEntityWithZeroModified(t *testing.T) {
	w := newTestWriter(t)

	err := w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q1",
		ExternalIDs: map[int][]string{345: {"tt1"}},
		// Modified is zero value
	}})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	// Entity should survive a sweep from the distant past because
	// viewed_at got set to ~time.Now(), not 0.
	swept, err := w.SweepStaleEntities(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 0 {
		t.Error("entity with zero Modified should have gotten a fallback viewed_at")
	}
}

// TestDeleteEntityCascadesMappings verifies that deleting from the entity
// table cascades to remove associated mapping rows.
func TestDeleteEntityCascadesMappings(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	err := w.UpsertEntity("Q1", map[int][]string{345: {"tt1"}, 4947: {"100"}, 4835: {"200"}})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Verify entity and mappings exist
	result, err := r.LookupByProperty(345, "tt1")
	if err != nil {
		t.Fatalf("LookupByProperty before delete: %v", err)
	}
	if result == nil || len(result.Mappings) != 3 {
		t.Fatalf("expected 3 mappings before delete, got %+v", result)
	}

	// Delete the entity
	err = w.DeleteEntity("Q1")
	if err != nil {
		t.Fatalf("DeleteEntity: %v", err)
	}

	// All mappings should be gone via CASCADE
	for _, val := range []string{"tt1"} {
		result, err = r.LookupByProperty(345, val)
		if err != nil {
			t.Fatalf("LookupByProperty after delete: %v", err)
		}
		if result != nil {
			t.Errorf("expected mapping %s to be cascade-deleted", val)
		}
	}
	result, err = r.LookupByWikidataID(1)
	if err != nil {
		t.Fatalf("LookupByWikidataID after delete: %v", err)
	}
	if result != nil {
		t.Error("expected entity to be deleted")
	}
}

// TestDeleteEntitiesBatchCascade verifies batch deletion cascades to mappings.
func TestDeleteEntitiesBatchCascade(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	err := w.UpsertEntity("Q1", map[int][]string{345: {"tt1"}})
	if err != nil {
		t.Fatalf("UpsertEntity Q1: %v", err)
	}
	err = w.UpsertEntity("Q2", map[int][]string{345: {"tt2"}})
	if err != nil {
		t.Fatalf("UpsertEntity Q2: %v", err)
	}
	err = w.UpsertEntity("Q3", map[int][]string{345: {"tt3"}})
	if err != nil {
		t.Fatalf("UpsertEntity Q3: %v", err)
	}

	// Delete Q1 and Q3, keep Q2
	err = w.DeleteEntitiesBatch([]string{"Q1", "Q3"})
	if err != nil {
		t.Fatalf("DeleteEntitiesBatch: %v", err)
	}

	// Q1 and Q3 should be gone
	for _, val := range []string{"tt1", "tt3"} {
		result, err := r.LookupByProperty(345, val)
		if err != nil {
			t.Fatalf("LookupByProperty %s: %v", val, err)
		}
		if result != nil {
			t.Errorf("expected %s to be deleted", val)
		}
	}

	// Q2 should survive
	result, err := r.LookupByProperty(345, "tt2")
	if err != nil {
		t.Fatalf("LookupByProperty tt2: %v", err)
	}
	if result == nil || result.WikidataID != 2 {
		t.Errorf("expected Q2 to survive, got %+v", result)
	}
}

// TestMigrateSchemaFreshDB verifies MigrateSchema works on a brand-new database
// where sync_state has no schema_version yet.
func TestMigrateSchemaFreshDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	w, err := NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	// Before MigrateSchema, schema_version is not set
	val, err := w.GetSyncState("schema_version")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty schema_version before migrate, got %q", val)
	}

	// MigrateSchema should succeed and write the version
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	val, err = w.GetSyncState("schema_version")
	if err != nil {
		t.Fatalf("GetSyncState after migrate: %v", err)
	}
	if val != SchemaVersion() {
		t.Errorf("schema_version = %q, want %q", val, SchemaVersion())
	}
}

// TestMigrateSchemaMatchingVersion verifies MigrateSchema is a no-op when
// the stored version already matches.
func TestMigrateSchemaMatchingVersion(t *testing.T) {
	w := newTestWriter(t) // already calls MigrateSchema

	// Insert some data
	err := w.UpsertEntity("Q1", map[int][]string{345: {"tt1"}})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Call MigrateSchema again — should be a no-op
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	// Data should still be there
	r := newTestReaderFromWriter(t, w)
	result, err := r.LookupByProperty(345, "tt1")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Error("expected data to survive no-op MigrateSchema")
	}
}

// TestMigrateSchemaDropsOnMismatch verifies MigrateSchema drops all data
// when the stored schema_version doesn't match.
func TestMigrateSchemaDropsOnMismatch(t *testing.T) {
	w := newTestWriter(t)

	// Insert data and a fake old version
	err := w.UpsertEntity("Q1", map[int][]string{345: {"tt1"}})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}
	w.SetSyncState("schema_version", "old-wrong-version")
	w.SetSyncState("dump_time", "2026-01-01T00:00:00Z")

	// MigrateSchema should drop everything and recreate
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	// Data should be gone
	r := newTestReaderFromWriter(t, w)
	result, err := r.LookupByProperty(345, "tt1")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result != nil {
		t.Error("expected data to be dropped after schema mismatch")
	}

	// dump_time should also be gone (sync_state was recreated)
	val, err := w.GetSyncState("dump_time")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if val != "" {
		t.Errorf("expected dump_time to be cleared, got %q", val)
	}

	// New version should be written
	val, err = w.GetSyncState("schema_version")
	if err != nil {
		t.Fatalf("GetSyncState schema_version: %v", err)
	}
	if val != SchemaVersion() {
		t.Errorf("schema_version = %q, want %q", val, SchemaVersion())
	}
}

// TestReaderSchemaMismatchLookupByProperty verifies that LookupByProperty
// returns ErrSchemaMismatch when schema_version doesn't match.
func TestReaderSchemaMismatchLookupByProperty(t *testing.T) {
	w := newTestWriter(t)

	// Insert data, then corrupt the schema version
	w.UpsertEntity("Q1", map[int][]string{345: {"tt1"}})
	w.SetSyncState("schema_version", "wrong-version")

	// Create a new Reader — it should detect the mismatch
	r := NewReaderFromDB(w.DB())

	_, err := r.LookupByProperty(345, "tt1")
	if err != ErrSchemaMismatch {
		t.Errorf("expected ErrSchemaMismatch, got %v", err)
	}
}

// TestReaderSchemaMismatchLookupByWikidataID verifies that LookupByWikidataID
// returns ErrSchemaMismatch when schema_version doesn't match.
func TestReaderSchemaMismatchLookupByWikidataID(t *testing.T) {
	w := newTestWriter(t)

	w.UpsertEntity("Q1", map[int][]string{345: {"tt1"}})
	w.SetSyncState("schema_version", "wrong-version")

	r := NewReaderFromDB(w.DB())

	_, err := r.LookupByWikidataID(1)
	if err != ErrSchemaMismatch {
		t.Errorf("expected ErrSchemaMismatch, got %v", err)
	}
}

// TestReaderSchemaMismatchHealth verifies that GetHealth reports SchemaMatch
// correctly when the version doesn't match.
func TestReaderSchemaMismatchHealth(t *testing.T) {
	w := newTestWriter(t)

	// With correct version, SchemaMatch should be true
	r := NewReaderFromDB(w.DB())
	info, err := r.GetHealth()
	if err != nil {
		t.Fatalf("GetHealth: %v", err)
	}
	if !info.SchemaMatch {
		t.Error("expected SchemaMatch=true with correct version")
	}

	// Set wrong version
	w.SetSyncState("schema_version", "wrong-version")

	// Need a new Reader to pick up the change
	r2 := NewReaderFromDB(w.DB())
	info2, err := r2.GetHealth()
	if err != nil {
		t.Fatalf("GetHealth: %v", err)
	}
	if info2.SchemaMatch {
		t.Error("expected SchemaMatch=false with wrong version")
	}
}

// TestReaderSchemaVersionMissing verifies that a Reader constructed against
// a DB with no schema_version at all reports SchemaMatch=false.
func TestReaderSchemaVersionMissing(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create writer without MigrateSchema — no schema_version written
	w, err := NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	r := NewReaderFromDB(w.DB())
	info, err := r.GetHealth()
	if err != nil {
		t.Fatalf("GetHealth: %v", err)
	}
	if info.SchemaMatch {
		t.Error("expected SchemaMatch=false when schema_version is missing")
	}

	// Lookups should fail
	_, err = r.LookupByProperty(345, "tt1")
	if err != ErrSchemaMismatch {
		t.Errorf("expected ErrSchemaMismatch, got %v", err)
	}
}

// TestEntityCountInStats verifies that GetStats reports the correct entity count.
func TestEntityCountInStats(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	stats, err := r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.EntityCount != 0 {
		t.Errorf("initial EntityCount = %d, want 0", stats.EntityCount)
	}

	w.UpsertEntity("Q1", map[int][]string{345: {"tt1"}})
	w.UpsertEntity("Q2", map[int][]string{345: {"tt2"}})
	w.UpsertEntity("Q3", map[int][]string{345: {"tt3"}, 4947: {"100"}})

	stats, err = r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.EntityCount != 3 {
		t.Errorf("EntityCount = %d, want 3", stats.EntityCount)
	}
	if stats.MappingCount != 4 {
		t.Errorf("MappingCount = %d, want 4", stats.MappingCount)
	}

	// Delete one, count should decrease
	w.DeleteEntity("Q2")
	stats, err = r.GetStats()
	if err != nil {
		t.Fatalf("GetStats after delete: %v", err)
	}
	if stats.EntityCount != 2 {
		t.Errorf("EntityCount after delete = %d, want 2", stats.EntityCount)
	}
}

// TestSchemaVersionDeterministic verifies SchemaVersion returns the same value.
func TestSchemaVersionDeterministic(t *testing.T) {
	h1 := SchemaVersion()
	h2 := SchemaVersion()
	if h1 != h2 {
		t.Errorf("SchemaVersion not deterministic: %q != %q", h1, h2)
	}
	if h1 == "" {
		t.Error("SchemaVersion must not be empty")
	}
}

// TestEntityTableCreated verifies the entity table is part of the schema.
func TestEntityTableCreated(t *testing.T) {
	w := newTestWriter(t)

	var count int
	err := w.db.QueryRow("SELECT COUNT(*) FROM entity").Scan(&count)
	if err != nil {
		t.Fatalf("entity table not created: %v", err)
	}
}

// TestForeignKeyConstraint verifies that inserting a mapping row without
// a corresponding entity row fails due to the FK constraint.
func TestForeignKeyConstraint(t *testing.T) {
	w := newTestWriter(t)

	_, err := w.db.Exec(`INSERT INTO mapping (property, value, wikidata_id) VALUES (345, 'tt1', 999)`)
	if err == nil {
		t.Error("expected FK constraint violation, got nil")
	}
}

// TestViewedAtSimulatedSeedEventStreamInteraction simulates the scenario
// discussed in conversation: an entity is imported via event stream with a
// newer modified time, then the dump seed runs with an older dump time.
// The entity's viewed_at should remain at the event stream value (not be
// rolled back) and the entity should survive the sweep.
func TestViewedAtSimulatedSeedEventStreamInteraction(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	eventStreamTime := time.Date(2026, 3, 15, 8, 0, 0, 0, time.UTC) // newer

	// Step 1: Entity arrives via event stream with newer modified time
	err := w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q100",
		ExternalIDs: map[int][]string{345: {"tt100"}, 4947: {"50"}},
		Modified:    eventStreamTime,
	}})
	if err != nil {
		t.Fatalf("event stream upsert: %v", err)
	}

	// Step 2: Dump seed runs, same entity appears with dumpTime as viewed_at
	// (simulating max(dumpTime, entity.Modified) where entity.Modified < dumpTime)
	err = w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q100",
		ExternalIDs: map[int][]string{345: {"tt100"}, 4947: {"50"}},
		Modified:    dumpTime, // older than event stream
	}})
	if err != nil {
		t.Fatalf("dump upsert: %v", err)
	}

	// Step 3: Sweep with dumpTime — entity should survive because its
	// viewed_at is max(eventStreamTime, dumpTime) = eventStreamTime
	swept, err := w.SweepStaleEntities(dumpTime.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 0 {
		t.Errorf("swept = %d, want 0 (entity has newer viewed_at from event stream)", swept)
	}

	result, err := r.LookupByProperty(345, "tt100")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Error("expected entity to survive sweep")
	}
}

// TestSweepPreservesEntityUpdatedByEventStreamAfterDump simulates:
// 1. Stale entity with old viewed_at
// 2. Dump entity with dumpTime viewed_at
// 3. Event-stream-updated entity with future viewed_at
// Only the stale entity should be swept.
func TestSweepPreservesEntityUpdatedByEventStreamAfterDump(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	// Stale entity (old viewed_at, not in dump)
	err := w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q1",
		ExternalIDs: map[int][]string{345: {"tt1"}},
		Modified:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}})
	if err != nil {
		t.Fatalf("stale upsert: %v", err)
	}

	// Dump entity
	err = w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q2",
		ExternalIDs: map[int][]string{345: {"tt2"}},
		Modified:    dumpTime,
	}})
	if err != nil {
		t.Fatalf("dump upsert: %v", err)
	}

	// Event-stream-updated entity (future viewed_at)
	err = w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q3",
		ExternalIDs: map[int][]string{345: {"tt3"}},
		Modified:    time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
	}})
	if err != nil {
		t.Fatalf("event stream upsert: %v", err)
	}

	swept, err := w.SweepStaleEntities(dumpTime.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 1 {
		t.Errorf("swept = %d, want 1 (only stale Q1)", swept)
	}

	// Q1 should be gone
	if r1, _ := r.LookupByProperty(345, "tt1"); r1 != nil {
		t.Error("Q1 should be swept")
	}
	// Q2 and Q3 should survive
	if r2, _ := r.LookupByProperty(345, "tt2"); r2 == nil {
		t.Error("Q2 should survive (dump entity)")
	}
	if r3, _ := r.LookupByProperty(345, "tt3"); r3 == nil {
		t.Error("Q3 should survive (event stream entity)")
	}
}

// TestMigrateSchemaDropsOrphanedTables verifies that MigrateSchema drops tables
// from a previous schema version that no longer exist in createSchema, using
// the dynamic sqlite_schema discovery.
func TestMigrateSchemaDropsOrphanedTables(t *testing.T) {
	w := newTestWriter(t)

	// Create an extra table that simulates a table from an old schema version
	_, err := w.db.Exec(`CREATE TABLE old_legacy_table (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("create legacy table: %v", err)
	}

	// Insert some data so it's not empty
	_, err = w.db.Exec(`INSERT INTO old_legacy_table (id, data) VALUES (1, 'old data')`)
	if err != nil {
		t.Fatalf("insert into legacy: %v", err)
	}

	// Corrupt the schema version to force a migration
	w.SetSyncState("schema_version", "old-version")

	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	// The orphaned table should be gone
	var count int
	err = w.db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'old_legacy_table'`).Scan(&count)
	if err != nil {
		t.Fatalf("check orphaned table: %v", err)
	}
	if count != 0 {
		t.Error("expected old_legacy_table to be dropped by MigrateSchema")
	}
}

// TestMigrateSchemaIdempotent verifies calling MigrateSchema twice in a row
// is safe (second call is a no-op).
func TestMigrateSchemaIdempotent(t *testing.T) {
	w := newTestWriter(t) // calls MigrateSchema once

	w.UpsertEntity("Q1", map[int][]string{345: {"tt1"}})

	// Second call should be a no-op
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("second MigrateSchema: %v", err)
	}

	// Third call also a no-op
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("third MigrateSchema: %v", err)
	}

	// Data should survive
	r := newTestReaderFromWriter(t, w)
	result, _ := r.LookupByProperty(345, "tt1")
	if result == nil {
		t.Error("data should survive idempotent MigrateSchema calls")
	}
}

// TestSweepBoundaryViewedAtEqualsThreshold verifies that an entity whose
// viewed_at equals exactly the sweep threshold survives (SQL uses < not <=).
func TestSweepBoundaryViewedAtEqualsThreshold(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	boundary := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	err := w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q1",
		ExternalIDs: map[int][]string{345: {"tt1"}},
		Modified:    boundary,
	}})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	// Sweep with exact same timestamp — entity should survive
	swept, err := w.SweepStaleEntities(boundary.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 0 {
		t.Errorf("swept = %d, want 0 (viewed_at == threshold should survive)", swept)
	}

	result, _ := r.LookupByProperty(345, "tt1")
	if result == nil {
		t.Error("entity at boundary should survive sweep")
	}

	// But one second before the boundary should be swept
	err = w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q2",
		ExternalIDs: map[int][]string{345: {"tt2"}},
		Modified:    boundary.Add(-time.Second),
	}})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch Q2: %v", err)
	}
	swept, err = w.SweepStaleEntities(boundary.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 1 {
		t.Errorf("swept = %d, want 1 (one second before threshold should be swept)", swept)
	}
}

// TestUpsertEntitiesBatchMixedModified verifies that a batch with a mix of
// zero and real Modified values handles each entity independently.
func TestUpsertEntitiesBatchMixedModified(t *testing.T) {
	w := newTestWriter(t)

	realTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) // safely in the past

	err := w.UpsertEntitiesBatch([]EntityRecord{
		{WikidataID: "Q1", ExternalIDs: map[int][]string{345: {"tt1"}}, Modified: realTime},
		{WikidataID: "Q2", ExternalIDs: map[int][]string{345: {"tt2"}}}, // zero Modified → fallback to ~time.Now()
		{WikidataID: "Q3", ExternalIDs: map[int][]string{345: {"tt3"}}, Modified: realTime},
	})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	// Sweep at realTime: Q1 and Q3 have viewed_at = realTime (survives, >= threshold).
	// Q2 has viewed_at = ~time.Now() which is > realTime (survives).
	swept, err := w.SweepStaleEntities(realTime.Unix())
	if err != nil {
		t.Fatalf("SweepStaleEntities: %v", err)
	}
	if swept != 0 {
		t.Errorf("swept = %d, want 0 (all entities should have viewed_at >= realTime)", swept)
	}
}

// TestUpsertEntityCreatesEntityRow verifies the convenience UpsertEntity
// method also populates the entity table (not just mappings).
func TestUpsertEntityCreatesEntityRow(t *testing.T) {
	w := newTestWriter(t)

	err := w.UpsertEntity("Q42", map[int][]string{345: {"tt42"}})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	var count int
	err = w.db.QueryRow(`SELECT COUNT(*) FROM entity WHERE wikidata_id = 42`).Scan(&count)
	if err != nil {
		t.Fatalf("query entity: %v", err)
	}
	if count != 1 {
		t.Errorf("entity row count = %d, want 1", count)
	}

	// viewed_at should be > 0 (fallback from zero Modified)
	var viewedAt int64
	w.db.QueryRow(`SELECT viewed_at FROM entity WHERE wikidata_id = 42`).Scan(&viewedAt)
	if viewedAt <= 0 {
		t.Errorf("viewed_at = %d, want > 0", viewedAt)
	}
}

// TestDeleteEntitiesBatchEmpty verifies empty batch is a safe no-op.
func TestDeleteEntitiesBatchEmpty(t *testing.T) {
	w := newTestWriter(t)

	if err := w.DeleteEntitiesBatch(nil); err != nil {
		t.Fatalf("DeleteEntitiesBatch(nil): %v", err)
	}
	if err := w.DeleteEntitiesBatch([]string{}); err != nil {
		t.Fatalf("DeleteEntitiesBatch(empty): %v", err)
	}
}

// TestSweepWithZeroThreshold verifies that sweep with threshold 0 removes
// nothing because all viewed_at values are > 0 (the zero-Modified fallback
// uses time.Now()).
func TestSweepWithZeroThreshold(t *testing.T) {
	w := newTestWriter(t)

	w.UpsertEntity("Q1", map[int][]string{345: {"tt1"}})
	w.UpsertEntity("Q2", map[int][]string{345: {"tt2"}})

	swept, err := w.SweepStaleEntities(0)
	if err != nil {
		t.Fatalf("SweepStaleEntities(0): %v", err)
	}
	if swept != 0 {
		t.Errorf("swept = %d, want 0 (threshold 0 should match nothing)", swept)
	}
}

func TestSyncState(t *testing.T) {
	w := newTestWriter(t)

	val, err := w.GetSyncState("last_sync")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty, got %q", val)
	}

	err = w.SetSyncState("last_sync", "2026-03-10T14:22:00Z")
	if err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	val, err = w.GetSyncState("last_sync")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if val != "2026-03-10T14:22:00Z" {
		t.Errorf("got %q, want %q", val, "2026-03-10T14:22:00Z")
	}

	err = w.SetSyncState("last_sync", "2026-03-11T10:00:00Z")
	if err != nil {
		t.Fatalf("SetSyncState update: %v", err)
	}

	val, err = w.GetSyncState("last_sync")
	if err != nil {
		t.Fatalf("GetSyncState after update: %v", err)
	}
	if val != "2026-03-11T10:00:00Z" {
		t.Errorf("got %q, want %q", val, "2026-03-11T10:00:00Z")
	}
}

func TestGetStats(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	stats, err := r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.MappingCount != 0 {
		t.Errorf("expected 0 mapping count, got %d", stats.MappingCount)
	}

	w.UpsertEntity("Q1", map[int][]string{345: {"tt0000001"}, 4947: {"1"}})
	w.UpsertEntity("Q2", map[int][]string{345: {"tt0000002"}, 4947: {"2"}, 4835: {"100"}})
	w.SetSyncState("dump_time", "2026-03-01T00:00:00Z")
	w.SetSyncState("last_event_id", `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1773152520000}]`)

	stats, err = r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.MappingCount != 5 {
		t.Errorf("MappingCount = %d, want 5", stats.MappingCount)
	}
	if stats.DumpTime.IsZero() {
		t.Error("DumpTime should not be zero")
	}
	if stats.LastEventSync.IsZero() {
		t.Error("LastEventSync should not be zero")
	}

	w.SetSyncState("state", "streaming")
	stats, err = r.GetStats()
	if err != nil {
		t.Fatalf("GetStats with state: %v", err)
	}
	if stats.State != "streaming" {
		t.Errorf("State = %q, want %q", stats.State, "streaming")
	}
}

func TestNewReaderFromFile(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	w, err := NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	w.UpsertEntity("Q1", map[int][]string{345: {"tt0000001"}})
	w.Close()

	r, err := NewReader(dbPath)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	result, err := r.LookupByProperty(345, "tt0000001")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}
}

func TestUpsertConflictingExternalID(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	// Entity Q100 holds IMDb tt1234567
	err := w.UpsertEntity("Q100", map[int][]string{345: {"tt1234567"}, 4947: {"100"}})
	if err != nil {
		t.Fatalf("UpsertEntity Q100: %v", err)
	}

	// Entity Q200 now claims the same IMDb ID
	err = w.UpsertEntity("Q200", map[int][]string{345: {"tt1234567"}, 4947: {"200"}})
	if err != nil {
		t.Fatalf("UpsertEntity Q200 should succeed but got: %v", err)
	}

	// Lookup by the contested IMDb ID should return Q200 (latest wins)
	result, err := r.LookupByProperty(345, "tt1234567")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result.WikidataID != 200 {
		t.Errorf("WikidataID = %d, want 200 (latest upsert should win)", result.WikidataID)
	}

	// Q100 should still be findable via its TMDB ID
	result2, err := r.LookupByProperty(4947, "100")
	if err != nil {
		t.Fatalf("LookupByProperty Q100 tmdb: %v", err)
	}
	if result2 == nil {
		t.Fatal("Q100 should still exist via TMDB ID")
	}
	if result2.WikidataID != 100 {
		t.Errorf("WikidataID = %d, want 100", result2.WikidataID)
	}
	// Q100 should have lost its IMDb mapping (now owned by Q200)
	if _, ok := result2.Mappings[345]; ok {
		t.Error("Q100 should no longer have an IMDb mapping (reassigned to Q200)")
	}
}

func TestPropertyDisambiguation(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	// TMDB ID "278" as a movie (P4947) and as a TV series (P4983)
	movieIDs := map[int][]string{345: {"tt0111161"}, 4947: {"278"}}
	err := w.UpsertEntity("Q172241", movieIDs)
	if err != nil {
		t.Fatalf("UpsertEntity movie: %v", err)
	}

	tvIDs := map[int][]string{345: {"tt9999999"}, 4983: {"278"}}
	err = w.UpsertEntity("Q999999", tvIDs)
	if err != nil {
		t.Fatalf("UpsertEntity tv: %v", err)
	}

	// Lookup TMDB movie 278 via P4947
	movieResult, err := r.LookupByProperty(4947, "278")
	if err != nil {
		t.Fatalf("LookupByProperty movie: %v", err)
	}
	if movieResult.WikidataID != 172241 {
		t.Errorf("movie WikidataID = %d, want 172241", movieResult.WikidataID)
	}

	// Lookup TMDB TV 278 via P4983
	tvResult, err := r.LookupByProperty(4983, "278")
	if err != nil {
		t.Fatalf("LookupByProperty tv: %v", err)
	}
	if tvResult.WikidataID != 999999 {
		t.Errorf("tv WikidataID = %d, want 999999", tvResult.WikidataID)
	}
}

func TestUpsertEntitiesBatch(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	records := []EntityRecord{
		{WikidataID: "Q172241",
			ExternalIDs: map[int][]string{345: {"tt0111161"}, 4947: {"278"}}},
		{WikidataID: "Q1079",
			ExternalIDs: map[int][]string{345: {"tt0903747"}, 4983: {"1396"}}},
		{WikidataID: "Q47740",
			ExternalIDs: map[int][]string{1733: {"620"}, 5794: {"72"}}},
	}

	if err := w.UpsertEntitiesBatch(records); err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	movie, err := r.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty movie: %v", err)
	}
	if movie == nil || movie.WikidataID != 172241 {
		t.Errorf("expected 172241, got %+v", movie)
	}
	if vals := movie.Mappings[4947]; len(vals) != 1 || vals[0] != "278" {
		t.Errorf("TMDB = %v, want [278]", vals)
	}

	tv, err := r.LookupByProperty(345, "tt0903747")
	if err != nil {
		t.Fatalf("LookupByProperty tv: %v", err)
	}
	if tv == nil || tv.WikidataID != 1079 {
		t.Errorf("expected 1079, got %+v", tv)
	}

	game, err := r.LookupByProperty(1733, "620")
	if err != nil {
		t.Fatalf("LookupByProperty game: %v", err)
	}
	if game == nil || game.WikidataID != 47740 {
		t.Errorf("expected 47740, got %+v", game)
	}
}

func TestUpsertEntitiesBatchEmpty(t *testing.T) {
	w := newTestWriter(t)

	if err := w.UpsertEntitiesBatch(nil); err != nil {
		t.Fatalf("UpsertEntitiesBatch(nil): %v", err)
	}
	if err := w.UpsertEntitiesBatch([]EntityRecord{}); err != nil {
		t.Fatalf("UpsertEntitiesBatch(empty): %v", err)
	}
}

func TestUpsertEntitiesBatchUpdatesExisting(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	if err := w.UpsertEntity("Q172241", map[int][]string{345: {"tt0111161"}}); err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	if err := w.UpsertEntitiesBatch([]EntityRecord{{
		WikidataID:  "Q172241",
		ExternalIDs: map[int][]string{345: {"tt0111161"}, 4947: {"278"}},
	}}); err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	result, err := r.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if len(result.Mappings) != 2 {
		t.Errorf("Mappings = %d, want 2", len(result.Mappings))
	}
}

func TestMultiValuedProperty(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	// Entity with multiple values for the same property (e.g. multiple IMDb IDs)
	ids := map[int][]string{345: {"tt0111161", "tt9999999"}, 4947: {"278"}}
	err := w.UpsertEntity("Q172241", ids)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Lookup by first IMDb value
	result1, err := r.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result1 == nil || result1.WikidataID != 172241 {
		t.Errorf("expected 172241 for first IMDb, got %+v", result1)
	}

	// Lookup by second IMDb value
	result2, err := r.LookupByProperty(345, "tt9999999")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result2 == nil || result2.WikidataID != 172241 {
		t.Errorf("expected 172241 for second IMDb, got %+v", result2)
	}
}

func TestNewReaderNonexistentDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "nonexistent.db")

	_, err := NewReader(dbPath)
	if err == nil {
		_ = os.Remove(dbPath)
	}
}
