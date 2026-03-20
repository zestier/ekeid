package watcher

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/store"
)

// ErrStreamTooOld is returned when the EventStream does not have data going
// back far enough to cover the last sync point. The caller should reseed.
var ErrStreamTooOld = errors.New("event stream does not have data going back far enough")

const (
	eventStreamURL     = "https://stream.wikimedia.org/v2/stream/recentchange"
	reconnectDelay     = 5 * time.Second
	retryInterval      = 10 * time.Minute
	retryBatchSize     = 50
	queuePollInterval  = 15 * time.Second
	enqueueBatchSize   = 1000            // buffer up to N QIDs before flushing to DB
	enqueueFlushDelay  = 5 * time.Second // max delay before flushing a partial batch
	streamGapThreshold = 1 * time.Hour
	dumpSafetyOffset   = 72 * time.Hour // how far before dump_time to start streaming
)

// recentChangeEvent represents a Wikidata recent change event.
type recentChangeEvent struct {
	Meta struct {
		Domain string `json:"domain"`
	} `json:"meta"`
	Wiki      string `json:"wiki"`
	Namespace int    `json:"namespace"`
	Title     string `json:"title"`
	Timestamp int64  `json:"timestamp"`
}

// EventStreamWatcher watches Wikidata EventStreams for relevant changes.
type EventStreamWatcher struct {
	processor  *Processor
	writer     *store.Writer
	httpClient *http.Client
	streamURL  string
}

// NewEventStreamWatcher creates a new EventStreams watcher.
func NewEventStreamWatcher(processor *Processor, writer *store.Writer, httpClient *http.Client) *EventStreamWatcher {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 0, // No timeout for SSE
		}
	}
	return &EventStreamWatcher{
		processor:  processor,
		writer:     writer,
		httpClient: httpClient,
		streamURL:  eventStreamURL,
	}
}

// Watch connects to EventStreams and processes changes until the context is cancelled.
// Events are enqueued into a pending_entity table in the database; a background
// goroutine drains the queue in batches via the wbgetentities API.
// A second background goroutine periodically retries entities from the failed_entity table.
func (w *EventStreamWatcher) Watch(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); w.processQueue(ctx) }()
	go func() { defer wg.Done(); w.retryFailedEntities(ctx) }()
	defer func() {
		cancel()
		wg.Wait()
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := w.connectAndProcess(ctx)
		if err != nil {
			if errors.Is(err, ErrStreamTooOld) {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("EventStream connection error: %v. Reconnecting in %v...", err, reconnectDelay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(reconnectDelay):
			}
		}
	}
}

func (w *EventStreamWatcher) connectAndProcess(ctx context.Context) error {
	url := w.streamURL

	// sinceTime is used for the gap check: did the stream return data
	// going back far enough? Needed for both ?since= (timestamp) and
	// Last-Event-ID (Kafka offset) reconnection — if the offset expired
	// the server silently starts from the earliest available data.
	var sinceTime time.Time

	// Always read last_event_id from the DB so that reconnects follow the
	// same code path as full process restarts.
	lastEventID, err := w.writer.GetSyncState("last_event_id")
	if err != nil {
		return fmt.Errorf("get last_event_id: %w", err)
	}

	if lastEventID != "" {
		// The event ID is a JSON array of Kafka offsets, each containing a
		// millisecond timestamp. Extract the min for gap detection.
		sinceTime = extractEventIDTime(lastEventID)
	} else {
		dumpTime, err := w.writer.GetSyncState("dump_time")
		if err != nil {
			return fmt.Errorf("get dump_time: %w", err)
		}
		if dumpTime == "" {
			return fmt.Errorf("no dump_time; database must be seeded first")
		}
		t, err := time.Parse(time.RFC3339, dumpTime)
		if err != nil {
			return fmt.Errorf("parse dump_time: %w", err)
		}
		sinceTime = t.Add(-dumpSafetyOffset)
		since := sinceTime.UTC().Format(time.RFC3339)
		url = fmt.Sprintf("%s?since=%s", url, since)
	}

	req, err := newRequestWithContext(ctx, "GET", url)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("connect to eventstream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("eventstream returned status %d", resp.StatusCode)
	}

	log.Println("Connected to EventStreams")

	// Channel for async DB writes.
	enqueueCh := make(chan struct {
		qid     string
		eventID string
	}, enqueueBatchSize)

	var wg sync.WaitGroup
	wg.Add(1)

	// Background writer: receives QIDs, batches them, writes to DB
	go func() {
		defer wg.Done()

		batchSet := make(map[string]struct{}, enqueueBatchSize)
		batch := make([]string, 0, enqueueBatchSize)
		var batchLastEventID string
		flush := func() error {
			if len(batchSet) == 0 {
				return nil
			}
			batch = batch[:0]
			for qid := range batchSet {
				batch = append(batch, qid)
			}
			n, err := w.writer.EnqueueEntities(batch)
			if err != nil {
				return fmt.Errorf("enqueue entities: %w", err)
			}
			if batchLastEventID != "" {
				if err := w.writer.SetSyncState("last_event_id", batchLastEventID); err != nil {
					return fmt.Errorf("persist last_event_id: %w", err)
				}
			}
			log.Printf("Enqueued %d entities for processing (%d new)", len(batch), n)
			clear(batchSet)
			batchLastEventID = ""
			return nil
		}

		// Flush periodically even if batch not full
		ticker := time.NewTicker(enqueueFlushDelay)
		defer ticker.Stop()

		for {
			select {
			case item, ok := <-enqueueCh:
				if !ok {
					// Channel closed, flush remaining
					_ = flush()
					return
				}
				batchSet[item.qid] = struct{}{}
				batchLastEventID = item.eventID
				if len(batchSet) >= enqueueBatchSize {
					if err := flush(); err != nil {
						log.Printf("Error flushing enqueue buffer: %v", err)
					}
				}
			case <-ticker.C:
				if err := flush(); err != nil {
					log.Printf("Error flushing enqueue buffer: %v", err)
				}
			}
		}
	}()

	// Ensure channel is closed and writer finishes before returning
	defer func() {
		close(enqueueCh)
		wg.Wait()
	}()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024) // 1MB max line

	gapChecked := sinceTime.IsZero() // skip gap check if no since param
	var dataLines []string
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		line := scanner.Text()

		if strings.HasPrefix(line, "id: ") {
			lastEventID = strings.TrimPrefix(line, "id: ")
			continue
		}

		if strings.HasPrefix(line, "data: ") {
			dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
			continue
		}

		// Empty line = end of event
		if line == "" && len(dataLines) > 0 {
			data := strings.Join(dataLines, "")
			dataLines = nil

			// Check that the stream goes back far enough on the first event.
			if !gapChecked {
				eventTime := extractEventIDTime(lastEventID)
				if !eventTime.IsZero() {
					gap := eventTime.Sub(sinceTime)
					if gap > streamGapThreshold {
						log.Printf("Stream gap detected: requested since %s, first event at %s (gap: %v)",
							sinceTime.Format(time.RFC3339), eventTime.Format(time.RFC3339), gap)
						if err := w.writer.ClearSyncCursors(); err != nil {
							log.Printf("Failed to clear sync cursors: %v", err)
						}
						return ErrStreamTooOld
					}
					gapChecked = true
				}
			}

			qid := w.extractEvent(data)
			if qid == "" {
				continue
			}

			enqueueCh <- struct {
				qid     string
				eventID string
			}{qid: qid, eventID: lastEventID}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	return fmt.Errorf("stream ended")
}

// extractEvent parses an SSE data payload and returns the QID if the event
// is a relevant Wikidata entity change. Returns "" if the event should be
// ignored.
func (w *EventStreamWatcher) extractEvent(data string) string {
	var event recentChangeEvent
	if err := json.Unmarshal([]byte(data), &event); err != nil {
		return ""
	}

	if event.Meta.Domain == "canary" {
		return ""
	}
	if event.Wiki != "wikidatawiki" || event.Namespace != 0 {
		return ""
	}
	if !strings.HasPrefix(event.Title, "Q") {
		return ""
	}

	return event.Title
}

// extractEventIDTime extracts the earliest Kafka timestamp from an SSE event ID.
// Event IDs are JSON arrays like:
//
//	[{"topic":"...","partition":0,"offset":-1},{"topic":"...","partition":0,"timestamp":1773879470875}]
//
// Entries without a timestamp (offset-only, for inactive DCs) are ignored.
// Timestamps are milliseconds since epoch. Returns zero time if no entry
// contains a timestamp.
func extractEventIDTime(eventID string) time.Time {
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

// processQueue drains the pending_entity table in batches, fetching entities
// via the wbgetentities API and processing them. It runs until the context
// is cancelled.
func (w *EventStreamWatcher) processQueue(ctx context.Context) {
	ticker := time.NewTicker(queuePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.drainQueue(ctx)
		}
	}
}

func (w *EventStreamWatcher) drainQueue(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		qids, err := w.writer.PeekPendingBatch(retryBatchSize)
		if err != nil {
			log.Printf("Error reading pending queue: %v", err)
			return
		}
		if len(qids) == 0 {
			return
		}

		perEntityErrors, batchErr := w.processor.ProcessEntities(qids)
		if batchErr != nil {
			var rlErr *RateLimitedError
			if errors.As(batchErr, &rlErr) {
				if !rlErr.Maxlag {
					log.Printf("Rate limited by Wikidata (429), backing off for %v", rlErr.RetryAfter)
				}
				select {
				case <-ctx.Done():
				case <-time.After(rlErr.RetryAfter):
				}
			} else {
				log.Printf("Batch fetch failed: %v", batchErr)
			}
			return
		}

		// Determine which QIDs succeeded and which failed.
		var succeeded []string
		for _, qid := range qids {
			if perr := perEntityErrors[qid]; perr != nil {
				log.Printf("Error processing %s: %v", qid, perr)
				if dlErr := w.writer.RecordFailedEntity(qid, perr.Error()); dlErr != nil {
					log.Printf("Failed to record failed entity %s: %v", qid, dlErr)
				}
			}
			// Remove from pending regardless — failures go to failed_entity table.
			succeeded = append(succeeded, qid)
		}

		if err := w.writer.DeletePendingEntities(succeeded); err != nil {
			log.Printf("Error removing processed entities from pending queue: %v", err)
		}

		// If the batch wasn't full, wait for the next tick rather. This makes the backlog
		// purge faster reducing races with the enqueueing logic that would produce excessive
		// network requests.
		if len(qids) < retryBatchSize {
			return
		}
	}
}

// retryFailedEntities periodically retries entities from the failed_entity table.
func (w *EventStreamWatcher) retryFailedEntities(ctx context.Context) {
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			qids, err := w.writer.LastFailedEntities(retryBatchSize)
			if err != nil {
				log.Printf("Failed to read failed entities: %v", err)
				continue
			}
			if len(qids) == 0 {
				continue
			}

			log.Printf("Retrying %d failed entities", len(qids))

			perEntityErrors, batchErr := w.processor.ProcessEntities(qids)
			if batchErr != nil {
				log.Printf("Retry batch failed: %v", batchErr)
				continue
			}

			for _, qid := range qids {
				if perr := perEntityErrors[qid]; perr != nil {
					log.Printf("Retry failed for %s: %v", qid, perr)
					if dlErr := w.writer.RecordFailedEntity(qid, perr.Error()); dlErr != nil {
						log.Printf("Failed to update failed entity %s: %v", qid, dlErr)
					}
				} else {
					log.Printf("Retry succeeded for %s", qid)
					if err := w.writer.DeleteFailedEntity(qid); err != nil {
						log.Printf("Failed to remove retry record for %s: %v", qid, err)
					}
				}
			}
		}
	}
}

// extractQIDFromError extracts a Q-number from an error of the form "process Q12345: ...".
func extractQIDFromError(err error) string {
	msg := err.Error()
	if !strings.HasPrefix(msg, "process Q") {
		return ""
	}
	qid := strings.TrimPrefix(msg, "process ")
	if idx := strings.Index(qid, ":"); idx > 0 {
		qid = qid[:idx]
	}
	return qid
}
