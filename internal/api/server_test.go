package api

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/store"
)

func setupTestServer(t *testing.T) (*Server, *store.Writer) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	w, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { w.Close() })

	r := store.NewReaderFromDB(w.DB())
	srv := NewServer(r, "0.1.0-test", nil)
	return srv, w
}

func TestLookupSuccess(t *testing.T) {
	srv, w := setupTestServer(t)
	w.UpsertEntity("Q172241", map[int][]string{
		345:  {"tt0111161"},
		4947: {"278"},
		4835: {"2095"},
		8013: {"the-shawshank-redemption"},
	})

	req := httptest.NewRequest("GET", "/v1/lookup/P345/tt0111161", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp lookupResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.WikidataID != "Q172241" {
		t.Errorf("WikidataID = %q, want %q", resp.WikidataID, "Q172241")
	}
	if v := resp.Mappings["P345"]; len(v) != 1 || v[0] != "tt0111161" {
		t.Errorf("Mappings[P345] = %v, want [tt0111161]", resp.Mappings["P345"])
	}
	if v := resp.Mappings["P4947"]; len(v) != 1 || v[0] != "278" {
		t.Errorf("Mappings[P4947] = %v, want [278]", resp.Mappings["P4947"])
	}

	cc := rec.Header().Get("Cache-Control")
	if cc != "public, max-age=3600" {
		t.Errorf("Cache-Control = %q, want %q", cc, "public, max-age=3600")
	}

	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}
}

func TestLookupNotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("GET", "/v1/lookup/P345/tt9999999", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	var resp errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "not_found" {
		t.Errorf("Error = %q, want %q", resp.Error, "not_found")
	}
}

func TestLookupInvalidProperty(t *testing.T) {
	srv, _ := setupTestServer(t)

	tests := []struct {
		name string
		url  string
	}{
		{"no P prefix", "/v1/lookup/345/tt0111161"},
		{"non-numeric", "/v1/lookup/Pabc/tt0111161"},
		{"zero", "/v1/lookup/P0/tt0111161"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.url, nil)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			var resp errorResponse
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.Error != "invalid_request" {
				t.Errorf("Error = %q, want %q", resp.Error, "invalid_request")
			}
		})
	}
}

func TestWikidataLookupSuccess(t *testing.T) {
	srv, w := setupTestServer(t)
	w.UpsertEntity("Q172241", map[int][]string{
		345:  {"tt0111161"},
		4947: {"278"},
	})

	req := httptest.NewRequest("GET", "/v1/lookup/Q172241", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp lookupResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.WikidataID != "Q172241" {
		t.Errorf("WikidataID = %q, want %q", resp.WikidataID, "Q172241")
	}
	if v := resp.Mappings["P345"]; len(v) != 1 || v[0] != "tt0111161" {
		t.Errorf("Mappings[P345] = %v, want [tt0111161]", resp.Mappings["P345"])
	}
}

func TestWikidataLookupNotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("GET", "/v1/lookup/Q999999", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestWikidataLookupInvalid(t *testing.T) {
	srv, _ := setupTestServer(t)

	tests := []struct {
		name string
		url  string
	}{
		{"no Q prefix", "/v1/lookup/172241"},
		{"non-numeric", "/v1/lookup/Qabc"},
		{"zero", "/v1/lookup/Q0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.url, nil)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
}

func TestLookupPropertyDisambiguation(t *testing.T) {
	srv, w := setupTestServer(t)

	// Same TMDB ID "278" for movie (P4947) and TV (P4983)
	w.UpsertEntity("Q172241", map[int][]string{4947: {"278"}, 345: {"tt0111161"}})
	w.UpsertEntity("Q999999", map[int][]string{4983: {"278"}, 345: {"tt9999999"}})

	req := httptest.NewRequest("GET", "/v1/lookup/P4947/278", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("P4947 status = %d, want %d", rec.Code, http.StatusOK)
	}
	var movieResp lookupResponse
	json.NewDecoder(rec.Body).Decode(&movieResp)
	if movieResp.WikidataID != "Q172241" {
		t.Errorf("P4947 WikidataID = %q, want Q172241", movieResp.WikidataID)
	}

	req = httptest.NewRequest("GET", "/v1/lookup/P4983/278", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("P4983 status = %d, want %d", rec.Code, http.StatusOK)
	}
	var tvResp lookupResponse
	json.NewDecoder(rec.Body).Decode(&tvResp)
	if tvResp.WikidataID != "Q999999" {
		t.Errorf("P4983 WikidataID = %q, want Q999999", tvResp.WikidataID)
	}
}

func TestHealthEndpoint(t *testing.T) {
	srv, w := setupTestServer(t)

	w.UpsertEntity("Q1", map[int][]string{345: {"tt0000001"}, 4947: {"1"}})
	w.SetSyncState("dump_time", "2026-03-01T00:00:00Z")
	w.SetSyncState("last_event_id", `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1773152520000}]`)
	w.SetSyncState("state", "streaming")

	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("Status = %q, want %q", resp.Status, "ok")
	}
	if resp.Version != "0.1.0-test" {
		t.Errorf("Version = %q, want %q", resp.Version, "0.1.0-test")
	}
	if resp.DumpTime != "2026-03-01T00:00:00Z" {
		t.Errorf("DumpTime = %q, want %q", resp.DumpTime, "2026-03-01T00:00:00Z")
	}
	if resp.LastEventSync != "2026-03-10T14:22:00Z" {
		t.Errorf("LastEventSync = %q, want %q", resp.LastEventSync, "2026-03-10T14:22:00Z")
	}
	if resp.State != "streaming" {
		t.Errorf("State = %q, want %q", resp.State, "streaming")
	}
	if resp.DatabaseSize <= 0 {
		t.Errorf("DatabaseSize = %d, want > 0", resp.DatabaseSize)
	}

	cc := rec.Header().Get("Cache-Control")
	if cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want %q", cc, "no-cache")
	}
}

func TestStatsEndpoint(t *testing.T) {
	srv, w := setupTestServer(t)

	w.UpsertEntity("Q1", map[int][]string{345: {"tt0000001"}, 4947: {"1"}})
	w.SetSyncState("dump_time", "2026-03-01T00:00:00Z")
	w.SetSyncState("last_event_id", `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1773152520000}]`)
	w.SetSyncState("state", "streaming")

	req := httptest.NewRequest("GET", "/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.MappingCount != 2 {
		t.Errorf("MappingCount = %d, want 2", resp.MappingCount)
	}
	if resp.DumpTime != "2026-03-01T00:00:00Z" {
		t.Errorf("DumpTime = %q, want %q", resp.DumpTime, "2026-03-01T00:00:00Z")
	}
	if resp.LastEventSync != "2026-03-10T14:22:00Z" {
		t.Errorf("LastEventSync = %q, want %q", resp.LastEventSync, "2026-03-10T14:22:00Z")
	}
	if resp.State != "streaming" {
		t.Errorf("State = %q, want %q", resp.State, "streaming")
	}
}

func TestCORSHeaders(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("CORS origin = %q, want *", rec.Header().Get("Access-Control-Allow-Origin"))
	}
	if rec.Header().Get("Access-Control-Allow-Methods") != "GET, OPTIONS" {
		t.Errorf("CORS methods = %q", rec.Header().Get("Access-Control-Allow-Methods"))
	}
}

func TestCORSPreflight(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("OPTIONS", "/v1/lookup/P345/tt0111161", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("OPTIONS status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("missing CORS headers on preflight")
	}
}

func TestHealthEmptyDB(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp healthResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Status != "ok" {
		t.Errorf("Status = %q, want %q", resp.Status, "ok")
	}
}

func TestLookupCaseInsensitiveProperty(t *testing.T) {
	srv, w := setupTestServer(t)
	w.UpsertEntity("Q172241", map[int][]string{345: {"tt0111161"}})

	// Lowercase "p" should also work
	req := httptest.NewRequest("GET", "/v1/lookup/p345/tt0111161", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

// --- Rate limiting tests ---

func setupRateLimitedServer(t *testing.T, rate float64, burst int) (*Server, *store.Writer, *RateLimiter) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	w, err := store.NewWriter(dbPath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	r := store.NewReaderFromDB(w.DB())
	rl := NewRateLimiter(rate, burst)
	t.Cleanup(func() { rl.Stop() })
	srv := NewServer(r, "0.1.0-test", rl)
	return srv, w, rl
}

func TestRateLimitBurstAllowed(t *testing.T) {
	srv, _, _ := setupRateLimitedServer(t, 1, 3)
	handler := srv.Handler()

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/v1/health", nil)
		req.RemoteAddr = "10.0.0.1:12345"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want %d", i+1, rec.Code, http.StatusOK)
		}
		if rec.Header().Get("X-RateLimit-Limit") != "3" {
			t.Errorf("request %d: X-RateLimit-Limit = %q, want %q", i+1, rec.Header().Get("X-RateLimit-Limit"), "3")
		}
		if rec.Header().Get("X-RateLimit-Remaining") == "" {
			t.Errorf("request %d: X-RateLimit-Remaining header missing", i+1)
		}
		if rec.Header().Get("X-RateLimit-Reset") == "" {
			t.Errorf("request %d: X-RateLimit-Reset header missing", i+1)
		}
	}
}

func TestRateLimitExceeded(t *testing.T) {
	srv, _, _ := setupRateLimitedServer(t, 1, 2)
	handler := srv.Handler()

	// Exhaust the burst
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/v1/health", nil)
		req.RemoteAddr = "10.0.0.1:12345"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want %d", i+1, rec.Code, http.StatusOK)
		}
	}

	// Third request should be rate limited
	req := httptest.NewRequest("GET", "/v1/health", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusTooManyRequests, rec.Body.String())
	}

	var errResp errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode 429 body: %v", err)
	}
	if errResp.Error != "rate_limit_exceeded" {
		t.Errorf("Error = %q, want %q", errResp.Error, "rate_limit_exceeded")
	}
	if errResp.Message == "" {
		t.Error("expected non-empty message on 429 response")
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header on 429 response")
	}
}

func TestRateLimitHeaders(t *testing.T) {
	srv, _, _ := setupRateLimitedServer(t, 1, 3)
	handler := srv.Handler()

	// First request: remaining should be 2 (burst=3, minus 1)
	req := httptest.NewRequest("GET", "/v1/health", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "3" {
		t.Errorf("X-RateLimit-Limit = %q, want %q", got, "3")
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "2" {
		t.Errorf("X-RateLimit-Remaining = %q, want %q", got, "2")
	}
	if got := rec.Header().Get("X-RateLimit-Reset"); got == "" {
		t.Error("X-RateLimit-Reset header missing")
	}

	// Second request: remaining should be 1
	req = httptest.NewRequest("GET", "/v1/health", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "1" {
		t.Errorf("second request: X-RateLimit-Remaining = %q, want %q", got, "1")
	}

	// Third request: remaining should be 0
	req = httptest.NewRequest("GET", "/v1/health", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("third request: X-RateLimit-Remaining = %q, want %q", got, "0")
	}

	// Fourth request: 429, remaining=0
	req = httptest.NewRequest("GET", "/v1/health", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("fourth request: status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("fourth request: X-RateLimit-Remaining = %q, want %q", got, "0")
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "3" {
		t.Errorf("fourth request: X-RateLimit-Limit = %q, want %q", got, "3")
	}
}

func TestRateLimitPerIP(t *testing.T) {
	srv, _, _ := setupRateLimitedServer(t, 1, 1)
	handler := srv.Handler()

	// First IP uses its burst
	req := httptest.NewRequest("GET", "/v1/health", nil)
	req.Header.Set("X-Forwarded-For", "1.1.1.1")
	req.RemoteAddr = "proxy:8080"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("IP1 first request: status = %d, want %d", rec.Code, http.StatusOK)
	}

	// First IP is now exhausted
	req = httptest.NewRequest("GET", "/v1/health", nil)
	req.Header.Set("X-Forwarded-For", "1.1.1.1")
	req.RemoteAddr = "proxy:8080"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("IP1 second request: status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}

	// Second IP should still have its own bucket
	req = httptest.NewRequest("GET", "/v1/health", nil)
	req.Header.Set("X-Forwarded-For", "2.2.2.2")
	req.RemoteAddr = "proxy:8080"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("IP2 first request: status = %d, want %d. IP isolation failed", rec.Code, http.StatusOK)
	}
}

func TestRateLimitCORS429(t *testing.T) {
	srv, _, _ := setupRateLimitedServer(t, 1, 1)
	handler := srv.Handler()

	// Exhaust the burst
	req := httptest.NewRequest("GET", "/v1/health", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Trigger 429
	req = httptest.NewRequest("GET", "/v1/health", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("429 response: Access-Control-Allow-Origin = %q, want %q", got, "*")
	}
}

func TestClientIPExtraction(t *testing.T) {
	tests := []struct {
		name       string
		xff        string
		xRealIP    string
		remoteAddr string
		wantIP     string
	}{
		{
			name:       "X-Forwarded-For single IP",
			xff:        "203.0.113.50",
			remoteAddr: "127.0.0.1:1234",
			wantIP:     "203.0.113.50",
		},
		{
			name:       "X-Forwarded-For multiple IPs uses leftmost",
			xff:        "203.0.113.50, 70.41.3.18, 150.172.238.178",
			remoteAddr: "127.0.0.1:1234",
			wantIP:     "203.0.113.50",
		},
		{
			name:       "X-Real-IP when no X-Forwarded-For",
			xRealIP:    "198.51.100.42",
			remoteAddr: "127.0.0.1:1234",
			wantIP:     "198.51.100.42",
		},
		{
			name:       "X-Forwarded-For takes precedence over X-Real-IP",
			xff:        "203.0.113.50",
			xRealIP:    "198.51.100.42",
			remoteAddr: "127.0.0.1:1234",
			wantIP:     "203.0.113.50",
		},
		{
			name:       "RemoteAddr host:port",
			remoteAddr: "192.168.1.1:54321",
			wantIP:     "192.168.1.1",
		},
		{
			name:       "RemoteAddr bare IP (no port)",
			remoteAddr: "192.168.1.1",
			wantIP:     "192.168.1.1",
		},
		{
			name:       "X-Forwarded-For with spaces",
			xff:        "  203.0.113.50  , 70.41.3.18",
			remoteAddr: "127.0.0.1:1234",
			wantIP:     "203.0.113.50",
		},
		{
			name:       "X-Real-IP with spaces",
			xRealIP:    "  198.51.100.42  ",
			remoteAddr: "127.0.0.1:1234",
			wantIP:     "198.51.100.42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.RemoteAddr = tt.remoteAddr
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			if tt.xRealIP != "" {
				req.Header.Set("X-Real-IP", tt.xRealIP)
			}

			got := clientIP(req)
			if got != tt.wantIP {
				t.Errorf("clientIP() = %q, want %q", got, tt.wantIP)
			}
		})
	}
}

func TestRateLimitStopIdempotent(t *testing.T) {
	rl := NewRateLimiter(1, 5)

	// Should not panic when called multiple times
	rl.Stop()
	rl.Stop()
	rl.Stop()
}
