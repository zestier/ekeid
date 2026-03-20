package watcher

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/store"
)

// buildTestEntityJSON constructs a Wikidata API-format entity JSON for testing.
// Each property is given datatype "external-id" so extractExternalIDs picks it up.
func buildTestEntityJSON(qid, label string, properties map[string]string) []byte {
	claims := make(map[string]interface{})
	for propID, value := range properties {
		claims[propID] = []map[string]interface{}{
			{
				"mainsnak": map[string]interface{}{
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
		"entities": map[string]interface{}{
			qid: map[string]interface{}{
				"labels": map[string]interface{}{
					"en": map[string]string{"value": label},
				},
				"claims": claims,
			},
		},
	}

	data, _ := json.Marshal(entity)
	return data
}

func TestParseEntityJSON_Movie(t *testing.T) {
	data := buildTestEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
		"P4835": "2095",
		"P8013": "the-shawshank-redemption",
	})

	entity, err := ParseEntityJSON("Q172241", data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
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
	if v := entity.ExternalIDs[4835]; len(v) != 1 || v[0] != "2095" {
		t.Errorf("P4835 = %v, want [2095]", v)
	}
	if v := entity.ExternalIDs[8013]; len(v) != 1 || v[0] != "the-shawshank-redemption" {
		t.Errorf("P8013 = %v, want [the-shawshank-redemption]", v)
	}
}

func TestParseEntityJSON_TVSeries(t *testing.T) {
	data := buildTestEntityJSON("Q1079", "Breaking Bad", map[string]string{
		"P345":  "tt0903747",
		"P4983": "1396",
		"P2638": "169",
		"P4835": "81189",
		"P8013": "breaking-bad",
	})

	entity, err := ParseEntityJSON("Q1079", data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}

	if len(entity.ExternalIDs) != 5 {
		t.Errorf("len(ExternalIDs) = %d, want 5", len(entity.ExternalIDs))
	}
	if v := entity.ExternalIDs[345]; len(v) != 1 || v[0] != "tt0903747" {
		t.Errorf("P345 = %v, want [tt0903747]", v)
	}
	if v := entity.ExternalIDs[4983]; len(v) != 1 || v[0] != "1396" {
		t.Errorf("P4983 = %v, want [1396]", v)
	}
}

func TestParseEntityJSON_NoExternalIDs(t *testing.T) {
	// Entity with no external-id claims should return nil
	entity := map[string]interface{}{
		"entities": map[string]interface{}{
			"Q42": map[string]interface{}{
				"labels": map[string]interface{}{
					"en": map[string]string{"value": "Douglas Adams"},
				},
				"claims": map[string]interface{}{
					"P31": []map[string]interface{}{
						{
							"mainsnak": map[string]interface{}{
								"datatype": "wikibase-item",
								"datavalue": map[string]interface{}{
									"value": map[string]string{"id": "Q5"},
									"type":  "wikibase-entityid",
								},
							},
						},
					},
				},
			},
		},
	}
	data, _ := json.Marshal(entity)

	result, err := ParseEntityJSON("Q42", data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for entity with no external IDs, got %+v", result)
	}
}

func TestParseEntityJSON_SingleID(t *testing.T) {
	data := buildTestEntityJSON("Q999", "Obscure Movie", map[string]string{
		"P345": "tt9999999",
	})

	entity, err := ParseEntityJSON("Q999", data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity with 1 ID, got nil")
	}
	if v := entity.ExternalIDs[345]; len(v) != 1 || v[0] != "tt9999999" {
		t.Errorf("P345 = %v, want [tt9999999]", v)
	}
}

func TestParseEntityJSON_VideoGame(t *testing.T) {
	data := buildTestEntityJSON("Q47740", "Portal 2", map[string]string{
		"P1733": "620",
		"P5794": "72",
		"P1933": "50538",
	})

	entity, err := ParseEntityJSON("Q47740", data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}
	if v := entity.ExternalIDs[1733]; len(v) != 1 || v[0] != "620" {
		t.Errorf("P1733 = %v, want [620]", v)
	}
	if v := entity.ExternalIDs[5794]; len(v) != 1 || v[0] != "72" {
		t.Errorf("P5794 = %v, want [72]", v)
	}
	if v := entity.ExternalIDs[1933]; len(v) != 1 || v[0] != "50538" {
		t.Errorf("P1933 = %v, want [50538]", v)
	}
}

func TestParseEntityJSON_Person(t *testing.T) {
	data := buildTestEntityJSON("Q42", "Douglas Adams", map[string]string{
		"P345":  "nm0010930",
		"P4985": "42",
	})

	entity, err := ParseEntityJSON("Q42", data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}
	if v := entity.ExternalIDs[345]; len(v) != 1 || v[0] != "nm0010930" {
		t.Errorf("P345 = %v, want [nm0010930]", v)
	}
	if v := entity.ExternalIDs[4985]; len(v) != 1 || v[0] != "42" {
		t.Errorf("P4985 = %v, want [42]", v)
	}
}

func TestParseEntityJSON_Book(t *testing.T) {
	data := buildTestEntityJSON("Q8337", "Harry Potter and the Philosopher's Stone", map[string]string{
		"P212":  "978-0747532743",
		"P648":  "OL82563W",
		"P2969": "72193",
	})

	entity, err := ParseEntityJSON("Q8337", data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}
	if v := entity.ExternalIDs[212]; len(v) != 1 || v[0] != "978-0747532743" {
		t.Errorf("P212 = %v, want [978-0747532743]", v)
	}
	if v := entity.ExternalIDs[648]; len(v) != 1 || v[0] != "OL82563W" {
		t.Errorf("P648 = %v, want [OL82563W]", v)
	}
}

func TestParseEntityJSON_MusicGroup(t *testing.T) {
	data := buildTestEntityJSON("Q11036", "Radiohead", map[string]string{
		"P434":  "a74b1b7f-71a5-4011-9441-d0b5e4122711",
		"P1902": "4Z8W4fKeB5YxbusRsdQVPb",
	})

	entity, err := ParseEntityJSON("Q11036", data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}
	if v := entity.ExternalIDs[434]; len(v) != 1 || v[0] != "a74b1b7f-71a5-4011-9441-d0b5e4122711" {
		t.Errorf("P434 = %v, want [UUID]", v)
	}
	if v := entity.ExternalIDs[1902]; len(v) != 1 || v[0] != "4Z8W4fKeB5YxbusRsdQVPb" {
		t.Errorf("P1902 = %v, want [4Z8W4fKeB5YxbusRsdQVPb]", v)
	}
}

func TestParseEntityJSON_MusicAlbum(t *testing.T) {
	data := buildTestEntityJSON("Q190588", "OK Computer", map[string]string{
		"P436": "b1392450-e666-3926-a536-22c65f834433",
	})

	entity, err := ParseEntityJSON("Q190588", data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}
	if v := entity.ExternalIDs[436]; len(v) != 1 || v[0] != "b1392450-e666-3926-a536-22c65f834433" {
		t.Errorf("P436 = %v, want [UUID]", v)
	}
}

func TestParseEntityJSON_InvalidJSON(t *testing.T) {
	_, err := ParseEntityJSON("Q1", []byte("not valid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestProcessorWithMockServer(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	entityJSON := buildTestEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
		"P4835": "2095",
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(entityJSON)
	}))
	defer server.Close()

	client := NewWikidataClient(server.Client())
	client.baseURL = server.URL

	processor := NewProcessor(writer, client)

	err = processor.ProcessEntity("Q172241")
	if err != nil {
		t.Fatalf("ProcessEntity: %v", err)
	}

	reader := store.NewReaderFromDB(writer.DB())
	result, err := reader.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected result after processing, got nil")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}
	// Check that TMDB movie mapping is present
	if v := result.Mappings[4947]; len(v) != 1 || v[0] != "278" {
		t.Errorf("P4947 mapping = %v, want [278]", v)
	}
}

// sseEvent formats a single SSE event with id, data, and trailing blank line.
func sseEvent(id string, data []byte) string {
	return fmt.Sprintf("id: %s\ndata: %s\n\n", id, data)
}

func TestConnectAndProcess_StreamGapTriggersReseed(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	// Set last_sync to 30 days ago so the stream can't possibly go back that far.
	oldDumpTime := time.Now().Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339)
	if err := writer.SetSyncState("dump_time", oldDumpTime); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	// Create SSE server that sends one event with a recent timestamp.
	recentTS := time.Now().Unix()
	recentTSMs := recentTS * 1000
	eventData, _ := json.Marshal(map[string]interface{}{
		"meta":      map[string]string{"domain": "www.wikidata.org"},
		"wiki":      "wikidatawiki",
		"namespace": 0,
		"title":     "Q42",
		"timestamp": recentTS,
	})
	eventID := fmt.Sprintf(`[{"topic":"test","partition":0,"timestamp":%d}]`, recentTSMs)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, sseEvent(eventID, eventData))
	}))
	defer server.Close()

	processor := NewProcessor(writer, nil)
	esWatcher := NewEventStreamWatcher(processor, writer, server.Client())
	esWatcher.streamURL = server.URL

	ctx := context.Background()
	err = esWatcher.connectAndProcess(ctx)

	if !errors.Is(err, ErrStreamTooOld) {
		t.Fatalf("expected ErrStreamTooOld, got: %v", err)
	}

	// Verify dump_time was cleared.
	dumpTime, err := writer.GetSyncState("dump_time")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if dumpTime != "" {
		t.Errorf("dump_time should be cleared, got %q", dumpTime)
	}
}

func TestConnectAndProcess_NoGapWhenStreamIsFresh(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	writer, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer writer.Close()

	// Set dump_time so that after subtracting the 72h safety offset, sinceTime
	// lands close to now — well within the 1h stream-gap threshold.
	recentDumpTime := time.Now().Add(72*time.Hour + 5*time.Minute).UTC().Format(time.RFC3339)
	if err := writer.SetSyncState("dump_time", recentDumpTime); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	// SSE server sends one event with timestamp matching ~now, then closes.
	eventData, _ := json.Marshal(map[string]interface{}{
		"meta":      map[string]string{"domain": "www.wikidata.org"},
		"wiki":      "wikidatawiki",
		"namespace": 0,
		"title":     "Q42",
		"timestamp": time.Now().Unix(),
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, sseEvent("evt1", eventData))
	}))
	defer server.Close()

	processor := NewProcessor(writer, nil)
	esWatcher := NewEventStreamWatcher(processor, writer, server.Client())
	esWatcher.streamURL = server.URL

	ctx := context.Background()
	err = esWatcher.connectAndProcess(ctx)

	// Should get "stream ended" (server closed), NOT ErrStreamTooOld.
	if errors.Is(err, ErrStreamTooOld) {
		t.Fatal("should not get ErrStreamTooOld when stream is fresh")
	}
	if err == nil {
		t.Fatal("expected some error (stream ended), got nil")
	}
}

func TestExtractEventIDTime(t *testing.T) {
	tests := []struct {
		name    string
		eventID string
		wantMS  int64 // expected unix millis, 0 means zero time
	}{
		{
			name:    "real event ID with one timestamp",
			eventID: `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":-1},{"topic":"codfw.mediawiki.recentchange","partition":0,"timestamp":1773879470875}]`,
			wantMS:  1773879470875,
		},
		{
			name:    "both entries have timestamps, picks min",
			eventID: `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1000},{"topic":"codfw.mediawiki.recentchange","partition":0,"timestamp":2000}]`,
			wantMS:  1000,
		},
		{
			name:    "no timestamps at all",
			eventID: `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":-1},{"topic":"codfw.mediawiki.recentchange","partition":0,"offset":-1}]`,
			wantMS:  0,
		},
		{
			name:    "empty array",
			eventID: `[]`,
			wantMS:  0,
		},
		{
			name:    "invalid JSON",
			eventID: `not json`,
			wantMS:  0,
		},
		{
			name:    "empty string",
			eventID: "",
			wantMS:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractEventIDTime(tt.eventID)
			if tt.wantMS == 0 {
				if !got.IsZero() {
					t.Errorf("expected zero time, got %v", got)
				}
			} else {
				want := time.UnixMilli(tt.wantMS)
				if !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			}
		})
	}
}
