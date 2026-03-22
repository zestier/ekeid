package watcher

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

// ErrEntityNotFound indicates the entity no longer exists on Wikidata (HTTP 404).
var ErrEntityNotFound = errors.New("entity not found")

// RateLimitedError is returned when Wikidata responds with HTTP 429
// or a maxlag error.
type RateLimitedError struct {
	RetryAfter time.Duration
	Maxlag     bool // true if caused by maxlag, false if HTTP 429
}

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("rate limited, retry after %v", e.RetryAfter)
}

// WikidataEntity represents relevant data extracted from a Wikidata entity.
type WikidataEntity struct {
	ID          string
	ExternalIDs map[int][]string // Wikidata property number → values
	Modified    time.Time        // entity's last-modified time
}

// WikidataClient fetches entity data from Wikidata.
// It tracks consecutive maxlag errors and applies exponential backoff
// (capped at maxlagBackoffMax) to avoid hammering a struggling server.
type WikidataClient struct {
	client        *http.Client
	baseURL       string
	maxlagBackoff time.Duration // current backoff; 0 when healthy
}

const (
	maxlagBackoffInit = 10 * time.Second // initial backoff after first maxlag
	maxlagBackoffMax  = 5 * time.Minute  // cap for exponential backoff
)

// NewWikidataClient creates a new Wikidata API client.
// The client's transport should include throttling (ThrottleTransport)
// for production use. Tests can pass a plain httptest server client.
func NewWikidataClient(client *http.Client) *WikidataClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &WikidataClient{
		client:  client,
		baseURL: "https://www.wikidata.org",
	}
}

// EntityResult holds the raw JSON for a single entity from a batch fetch,
// or marks it as missing (deleted on Wikidata).
type EntityResult struct {
	Data    []byte // Raw JSON for the full response (contains all entities)
	Missing bool   // True if the entity was deleted/missing on Wikidata
}

// FetchEntitiesRaw fetches raw JSON for one or more Wikidata entities using
// the wbgetentities API. Returns a map from QID to EntityResult. Missing
// entities (deleted on Wikidata) are returned with Missing=true rather than
// as errors. The qids slice must not exceed 50 entries (Wikidata API limit).
func (c *WikidataClient) FetchEntitiesRaw(qids []string) (map[string]EntityResult, error) {
	if len(qids) == 0 {
		return nil, nil
	}
	if len(qids) > 50 {
		return nil, fmt.Errorf("wbgetentities: batch size %d exceeds limit of 50", len(qids))
	}

	apiURL := fmt.Sprintf("%s/w/api.php?action=wbgetentities&format=json&maxlag=%d&props=claims%%7Cinfo&ids=%s",
		c.baseURL, wikimediaMaxlag, strings.Join(qids, "%7C"))

	req, err := newRequest("GET", apiURL)
	if err != nil {
		return nil, fmt.Errorf("fetch entities: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch entities: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := 30 * time.Second // default backoff
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
				retryAfter = time.Duration(secs) * time.Second
			}
		}
		return nil, &RateLimitedError{RetryAfter: retryAfter}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch entities: status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 50<<20)) // 50 MB limit for batch
	if err != nil {
		return nil, fmt.Errorf("read entities response: %w", err)
	}

	// Check for maxlag error (returned as HTTP 200 with JSON error body).
	var apiErr struct {
		Error *struct {
			Code string  `json:"code"`
			Lag  float64 `json:"lag"`
		} `json:"error"`
	}

	if json.Unmarshal(body, &apiErr) == nil && apiErr.Error != nil && apiErr.Error.Code == "maxlag" {
		retryAfter := 5 * time.Second // default per Wikidata recommendation
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
				retryAfter = time.Duration(secs) * time.Second
			}
		}
		// Exponential backoff for consecutive maxlag errors.
		if c.maxlagBackoff == 0 {
			c.maxlagBackoff = maxlagBackoffInit
		} else {
			c.maxlagBackoff *= 2
		}
		c.maxlagBackoff = min(c.maxlagBackoff, maxlagBackoffMax)
		retryAfter = max(retryAfter, c.maxlagBackoff)
		log.Printf("Maxlag backoff: %v (lag=%.1fs, next will be %v if consecutive)", retryAfter, apiErr.Error.Lag, min(c.maxlagBackoff*2, maxlagBackoffMax))
		return nil, &RateLimitedError{RetryAfter: retryAfter, Maxlag: true}
	}

	// Successful response — reset maxlag backoff.
	c.maxlagBackoff = 0

	// Parse just the top-level structure to identify per-entity data and missing status.
	var envelope struct {
		Entities map[string]json.RawMessage `json:"entities"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("parse entities response: %w", err)
	}

	results := make(map[string]EntityResult, len(qids))
	for _, qid := range qids {
		raw, ok := envelope.Entities[qid]
		if !ok {
			results[qid] = EntityResult{Missing: true}
			continue
		}

		// Check for the "missing" key that wbgetentities uses for deleted entities.
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) == nil {
			if _, hasMissing := fields["missing"]; hasMissing {
				results[qid] = EntityResult{Missing: true}
				continue
			}
		}

		// Re-wrap in the {"entities": {"Q...": ...}} format that ParseEntityJSON expects.
		wrapped := fmt.Sprintf(`{"entities":{%q:%s}}`, qid, raw)
		results[qid] = EntityResult{Data: []byte(wrapped)}
	}

	return results, nil
}

// FetchEntityRaw fetches raw JSON for a single Wikidata entity by Q-number.
// This is a convenience wrapper around FetchEntitiesRaw for single-entity use.
// Returns ErrEntityNotFound if the entity is missing/deleted on Wikidata.
func (c *WikidataClient) FetchEntityRaw(qid string) ([]byte, error) {
	results, err := c.FetchEntitiesRaw([]string{qid})
	if err != nil {
		return nil, fmt.Errorf("fetch entity %s: %w", qid, err)
	}
	result, ok := results[qid]
	if !ok || result.Missing {
		return nil, fmt.Errorf("fetch entity %s: %w", qid, ErrEntityNotFound)
	}
	return result.Data, nil
}

// FetchEntity fetches and parses a Wikidata entity by Q-number.
func (c *WikidataClient) FetchEntity(qid string) (*WikidataEntity, error) {
	body, err := c.FetchEntityRaw(qid)
	if err != nil {
		return nil, err
	}
	return ParseEntityJSON(qid, body)
}

// ParseEntityJSON parses Wikidata entity JSON and extracts all external ID
// claims. Properties with mainsnak.datatype == "external-id" are collected
// automatically — no hardcoded registry needed.
func ParseEntityJSON(qid string, data []byte) (*WikidataEntity, error) {
	var raw struct {
		Entities map[string]struct {
			Claims   claimsMap `json:"claims"`
			Modified string    `json:"modified"`
		} `json:"entities"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse entity %s: %w", qid, err)
	}

	entityData, ok := raw.Entities[qid]
	if !ok {
		return nil, fmt.Errorf("entity %s not found in response", qid)
	}

	externalIDs := extractExternalIDs(entityData.Claims)
	if len(externalIDs) == 0 {
		return nil, nil
	}

	var modified time.Time
	if entityData.Modified != "" {
		modified, _ = time.Parse(time.RFC3339, entityData.Modified)
	}

	return &WikidataEntity{
		ID:          qid,
		ExternalIDs: externalIDs,
		Modified:    modified,
	}, nil
}

// parseDumpEntity parses a single entity from the Wikidata JSON dump format.
// The dump format is a bare entity object (no {"entities":{...}} wrapper).
// Returns nil if the entity has no external IDs.
func parseDumpEntity(data []byte) (*WikidataEntity, error) {
	var raw struct {
		ID       string    `json:"id"`
		Type     string    `json:"type"`
		Claims   claimsMap `json:"claims"`
		Modified string    `json:"modified"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse entity: %w", err)
	}

	// Only process items (Q-entities), skip properties (P-entities) and lexemes
	if !strings.HasPrefix(raw.ID, "Q") {
		return nil, nil
	}

	externalIDs := extractExternalIDs(raw.Claims)
	if len(externalIDs) == 0 {
		return nil, nil
	}

	var modified time.Time
	if raw.Modified != "" {
		modified, _ = time.Parse(time.RFC3339, raw.Modified)
	}

	return &WikidataEntity{
		ID:          raw.ID,
		ExternalIDs: externalIDs,
		Modified:    modified,
	}, nil
}

// claimsMap is the parsed structure of Wikidata entity claims.
type claimsMap map[string][]struct {
	MainSnak struct {
		DataType  string `json:"datatype"`
		DataValue struct {
			Value json.RawMessage `json:"value"`
			Type  string          `json:"type"`
		} `json:"datavalue"`
	} `json:"mainsnak"`
}

// extractExternalIDs collects all claims with datatype "external-id"
// and returns them as a property number → values map.
func extractExternalIDs(claims claimsMap) map[int][]string {
	externalIDs := make(map[int][]string)
	for propKey, propClaims := range claims {
		if len(propKey) < 2 || propKey[0] != 'P' {
			continue
		}
		propNum, err := strconv.Atoi(propKey[1:])
		if err != nil {
			continue
		}
		for _, claim := range propClaims {
			if claim.MainSnak.DataType != "external-id" {
				continue
			}
			value := extractStringValue(claim.MainSnak.DataValue.Value)
			if value != "" {
				externalIDs[propNum] = append(externalIDs[propNum], value)
			}
		}
	}
	return externalIDs
}

// extractStringValue extracts a string value from a Wikidata datavalue.
func extractStringValue(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return ""
}

// extractEntityID extracts a Q-number from a wikibase-entityid datavalue.
func extractEntityID(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var entityRef struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &entityRef); err == nil && strings.HasPrefix(entityRef.ID, "Q") {
		return entityRef.ID
	}
	return ""
}
