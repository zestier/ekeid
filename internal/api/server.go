package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/store"
)

// Server holds dependencies for the API handlers.
type Server struct {
	reader      *store.Reader
	version     string
	rateLimiter *RateLimiter
}

// NewServer creates a new API server.
func NewServer(reader *store.Reader, version string, rateLimiter *RateLimiter) *Server {
	return &Server{reader: reader, version: version, rateLimiter: rateLimiter}
}

// Handler returns the top-level HTTP handler with all routes and middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/health", s.handleHealth)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /v1/lookup/{key}/{value...}", s.handleLookup)
	mux.HandleFunc("GET /v1/lookup/{qid}", s.handleWikidataLookup)

	var handler http.Handler = mux
	if s.rateLimiter != nil {
		handler = s.rateLimiter.Middleware(handler)
	}
	return s.applyMiddleware(handler)
}

func (s *Server) applyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleLookup(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value := r.PathValue("value")

	if value == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "Value is required")
		return
	}

	// key must be P followed by digits (e.g. P345)
	if len(key) < 2 || (key[0] != 'P' && key[0] != 'p') {
		writeError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("Invalid property key %q. Must be P followed by digits (e.g. P345)", key))
		return
	}
	property, err := strconv.Atoi(key[1:])
	if err != nil || property <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("Invalid property key %q. Must be P followed by digits (e.g. P345)", key))
		return
	}

	result, err := s.reader.LookupByProperty(property, value)
	if err != nil {
		if errors.Is(err, store.ErrSchemaMismatch) {
			writeError(w, http.StatusServiceUnavailable, "unavailable", "Schema migration in progress")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "An internal error occurred")
		return
	}

	if result == nil {
		writeError(w, http.StatusNotFound, "not_found",
			fmt.Sprintf("No mapping found for %s:%s", strings.ToUpper(key[:1])+key[1:], value))
		return
	}

	// Convert map[int][]string to map[string][]string for JSON output
	mappings := make(map[string][]string, len(result.Mappings))
	for k, v := range result.Mappings {
		mappings[fmt.Sprintf("P%d", k)] = v
	}

	// Convert int64 wikidata_id back to Q-string for JSON response
	wikidataIDStr := fmt.Sprintf("Q%d", result.WikidataID)

	w.Header().Set("Cache-Control", "public, max-age=3600")
	writeJSON(w, http.StatusOK, lookupResponse{
		WikidataID: wikidataIDStr,
		Mappings:   mappings,
	})
}

func (s *Server) handleWikidataLookup(w http.ResponseWriter, r *http.Request) {
	qid := r.PathValue("qid")

	if len(qid) < 2 || (qid[0] != 'Q' && qid[0] != 'q') {
		writeError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("Invalid Wikidata ID %q. Must be Q followed by digits (e.g. Q172241)", qid))
		return
	}
	wikidataID, err := strconv.ParseInt(qid[1:], 10, 64)
	if err != nil || wikidataID <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("Invalid Wikidata ID %q. Must be Q followed by digits (e.g. Q172241)", qid))
		return
	}

	result, err := s.reader.LookupByWikidataID(wikidataID)
	if err != nil {
		if errors.Is(err, store.ErrSchemaMismatch) {
			writeError(w, http.StatusServiceUnavailable, "unavailable", "Schema migration in progress")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "An internal error occurred")
		return
	}

	if result == nil {
		writeError(w, http.StatusNotFound, "not_found",
			fmt.Sprintf("No entity found for %s", strings.ToUpper(qid[:1])+qid[1:]))
		return
	}

	mappings := make(map[string][]string, len(result.Mappings))
	for k, v := range result.Mappings {
		mappings[fmt.Sprintf("P%d", k)] = v
	}

	wikidataIDStr := fmt.Sprintf("Q%d", result.WikidataID)

	w.Header().Set("Cache-Control", "public, max-age=3600")
	writeJSON(w, http.StatusOK, lookupResponse{
		WikidataID: wikidataIDStr,
		Mappings:   mappings,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	info, err := s.reader.GetHealth()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to get health")
		return
	}

	resp := healthResponse{
		Status:       "ok",
		Version:      s.version,
		State:        info.State,
		DatabaseSize: info.DatabaseSize,
		SchemaMatch:  info.SchemaMatch,
	}
	if !info.DumpTime.IsZero() {
		resp.DumpTime = info.DumpTime.Format("2006-01-02T15:04:05Z")
	}
	if !info.LastEventSync.IsZero() {
		resp.LastEventSync = info.LastEventSync.Format("2006-01-02T15:04:05Z")
	}

	w.Header().Set("Cache-Control", "no-cache")
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.reader.GetStats()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to get stats")
		return
	}

	resp := statsResponse{
		State:        stats.State,
		MappingCount: stats.MappingCount,
		EntityCount:  stats.EntityCount,
		PendingCount: stats.PendingCount,
		FailedCount:  stats.FailedCount,
		DatabaseSize: stats.DatabaseSize,
	}
	if !stats.DumpTime.IsZero() {
		resp.DumpTime = stats.DumpTime.Format("2006-01-02T15:04:05Z")
	}
	if !stats.LastEventSync.IsZero() {
		resp.LastEventSync = stats.LastEventSync.Format("2006-01-02T15:04:05Z")
	}

	w.Header().Set("Cache-Control", "no-cache")
	writeJSON(w, http.StatusOK, resp)
}

// Response types

type lookupResponse struct {
	WikidataID string              `json:"wikidata_id"`
	Mappings   map[string][]string `json:"mappings"`
}

type healthResponse struct {
	Status        string `json:"status"`
	Version       string `json:"version"`
	State         string `json:"state,omitempty"`
	DumpTime      string `json:"dump_time,omitempty"`
	LastEventSync string `json:"last_event_sync,omitempty"`
	DatabaseSize  int64  `json:"database_size"`
	SchemaMatch   bool   `json:"schema_match"`
}

type statsResponse struct {
	State         string `json:"state,omitempty"`
	DumpTime      string `json:"dump_time,omitempty"`
	LastEventSync string `json:"last_event_sync,omitempty"`
	MappingCount  int64  `json:"mapping_count"`
	EntityCount   int64  `json:"entity_count"`
	PendingCount  int64  `json:"pending_count"`
	FailedCount   int64  `json:"failed_count"`
	DatabaseSize  int64  `json:"database_size"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// Helpers

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: code, Message: message})
}
