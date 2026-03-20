package store

import (
	"os"
	"path/filepath"
	"testing"
)

func newTestWriter(t *testing.T) *Writer {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	w, err := NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
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

func TestSeedTracking(t *testing.T) {
	w := newTestWriter(t)
	r := newTestReaderFromWriter(t, w)

	err := w.UpsertEntity("Q1", map[int][]string{345: {"tt0000001"}})
	if err != nil {
		t.Fatalf("UpsertEntity Q1: %v", err)
	}
	err = w.UpsertEntity("Q2", map[int][]string{345: {"tt0000002"}})
	if err != nil {
		t.Fatalf("UpsertEntity Q2: %v", err)
	}

	// Start seed tracking and only re-upsert Q2
	if err := w.StartSeedTracking(); err != nil {
		t.Fatalf("StartSeedTracking: %v", err)
	}
	err = w.UpsertEntity("Q2", map[int][]string{345: {"tt0000002"}})
	if err != nil {
		t.Fatalf("UpsertEntity Q2 during seed: %v", err)
	}

	swept, err := w.SweepUnseenEntities()
	if err != nil {
		t.Fatalf("SweepUnseenEntities: %v", err)
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
		t.Error("expected seen entity to survive sweep")
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
