package watcher

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ekeid/ekeid/internal/store"
)

// DumpFormat selects the compression format for Wikidata dump downloads.
type DumpFormat string

const (
	// DumpFormatGZ uses gzip compression (~130 GB). Faster decompression.
	DumpFormatGZ DumpFormat = "gz"
	// DumpFormatBZ2 uses bzip2 compression (~100 GB). Smaller download, slower decompression.
	DumpFormatBZ2 DumpFormat = "bz2"
)

// ErrDumpChanged is returned when the remote dump file changes (ETag mismatch)
// during a resumable download. Callers should reset any partial state and restart.
var ErrDumpChanged = errors.New("dump file changed during download")

const (
	dumpBaseURL          = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json."
	dumpScannerBufSize   = 16 * 1024 * 1024 // 16 MB max line
	dumpDecompChunkSize  = 256 * 1024       // 256 KB per decompression chunk
	dumpDecompChanCap    = 64               // up to 16 MB buffered ahead
	dumpProgressInterval = 30 * time.Second // log progress every 30s
	dumpBatchSize        = 2000             // entities per DB transaction

	dumpResumeMaxRetries    = 10              // max consecutive resume attempts
	dumpResumeBaseDelay     = 5 * time.Second // initial backoff delay
	dumpResumeMaxDelay      = 2 * time.Minute // maximum backoff delay
	dumpResumeBackoffFactor = 2               // exponential backoff multiplier

	// seedVersion is hashed for configFingerprint. Bump when seed-time
	// configuration changes (e.g. property filters) to trigger a reseed
	// on next startup without dropping all tables.
	seedVersion = "v2-property-based"
)

// countingReader wraps an io.Reader and counts the number of bytes read.
// The counter is safe for concurrent access.
type countingReader struct {
	r     io.Reader
	bytes atomic.Int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.bytes.Add(int64(n))
	return n, err
}

// chanReader implements io.Reader over a channel of byte slices, allowing
// a producer goroutine to run ahead of the consumer with buffering.
// Consumed buffers are returned to pool for reuse.
type chanReader struct {
	ch    <-chan []byte
	errCh <-chan error
	pool  *sync.Pool
	cur   []byte
	full  *[]byte // pointer to the original full-capacity slice for pool return
}

func (r *chanReader) Read(p []byte) (int, error) {
	for len(r.cur) == 0 {
		// Return the previous buffer to the pool
		if r.full != nil {
			r.pool.Put(r.full)
			r.full = nil
		}
		chunk, ok := <-r.ch
		if !ok {
			select {
			case err := <-r.errCh:
				if err != nil {
					return 0, err
				}
			default:
			}
			return 0, io.EOF
		}
		r.cur = chunk
		// Save pointer to the underlying array for pool return
		tmp := chunk[:cap(chunk)]
		r.full = &tmp
	}
	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

// resumableBody wraps an HTTP response body and transparently reconnects
// using Range requests when the underlying connection fails. It validates
// the ETag on each reconnect to detect file changes. This enables resuming
// a 100 GB+ bz2/gzip download across transient network failures without
// restarting the decompressor, since the compressed byte stream is identical.
type resumableBody struct {
	body       io.ReadCloser
	url        string
	etag       string
	offset     int64
	httpClient *http.Client
	retries    int
}

// newResumableBody creates a resumableBody. The etag is the value from the
// initial response. If etag is empty (server doesn't support it), resumption
// is disabled and it behaves like a normal body.
func newResumableBody(body io.ReadCloser, url, etag string, httpClient *http.Client) *resumableBody {
	return &resumableBody{
		body:       body,
		url:        url,
		etag:       etag,
		httpClient: httpClient,
	}
}

func (rb *resumableBody) Read(p []byte) (int, error) {
	n, err := rb.body.Read(p)
	rb.offset += int64(n)
	if n > 0 {
		rb.retries = 0 // successful read resets retry counter
	}
	if err != nil && err != io.EOF && rb.etag != "" {
		// Connection dropped — try to resume
		if resumeErr := rb.reconnect(); resumeErr != nil {
			return n, resumeErr
		}
		// Reconnected successfully; report what we got, next Read will use new body
		return n, nil
	}
	return n, err
}

func (rb *resumableBody) reconnect() error {
	rb.retries++
	if rb.retries > dumpResumeMaxRetries {
		return fmt.Errorf("dump download failed after %d resume attempts", dumpResumeMaxRetries)
	}

	delay := dumpResumeBaseDelay
	for i := 1; i < rb.retries; i++ {
		delay *= time.Duration(dumpResumeBackoffFactor)
		if delay > dumpResumeMaxDelay {
			delay = dumpResumeMaxDelay
			break
		}
	}
	log.Printf("Connection lost at byte %d, retrying in %v (attempt %d/%d)",
		rb.offset, delay, rb.retries, dumpResumeMaxRetries)
	time.Sleep(delay)

	rb.body.Close()

	req, err := newRequest("GET", rb.url)
	if err != nil {
		return fmt.Errorf("create resume request: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", rb.offset))
	req.Header.Set("If-Match", rb.etag)

	resp, err := rb.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("resume request: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		rb.body = resp.Body
		log.Printf("Resumed download at byte %d", rb.offset)
		return nil
	case http.StatusPreconditionFailed:
		resp.Body.Close()
		return ErrDumpChanged
	case http.StatusRequestedRangeNotSatisfiable:
		// Offset past end of file — file may have changed
		resp.Body.Close()
		return ErrDumpChanged
	case http.StatusOK:
		// Server doesn't support Range for this resource; can't resume
		resp.Body.Close()
		return fmt.Errorf("server returned 200 instead of 206, Range requests not supported")
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return fmt.Errorf("resume request returned status %d: %s", resp.StatusCode, string(body))
	}
}

func (rb *resumableBody) Close() error {
	return rb.body.Close()
}

// configFingerprint returns a stable hash of the seed configuration.
// When the seed version changes, this hash changes, triggering a reseed.
func configFingerprint() string {
	h := sha256.Sum256([]byte(seedVersion))
	return fmt.Sprintf("%x", h[:16])
}

// Seeder handles bulk import by streaming a Wikidata JSON dump.
type Seeder struct {
	writer     *store.Writer
	httpClient *http.Client
	dumpURL    string
	dumpFormat DumpFormat
}

// NewSeeder creates a new Seeder. The format parameter selects the
// compression format (DumpFormatGZ or DumpFormatBZ2). An empty value
// defaults to gz.
func NewSeeder(writer *store.Writer, httpClient *http.Client, format DumpFormat) *Seeder {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 0}
	}
	if format == "" {
		format = DumpFormatGZ
	}
	return &Seeder{
		writer:     writer,
		httpClient: httpClient,
		dumpURL:    dumpBaseURL + string(format),
		dumpFormat: format,
	}
}

// newDecompressor returns a reader that decompresses data from r
// using the Seeder's configured format.
func (s *Seeder) newDecompressor(r io.Reader) (io.Reader, error) {
	switch s.dumpFormat {
	case DumpFormatGZ:
		return gzip.NewReader(r)
	case DumpFormatBZ2:
		return bzip2.NewReader(r), nil
	default:
		return nil, fmt.Errorf("unsupported dump format: %q", s.dumpFormat)
	}
}

// NeedsSeed determines whether the database needs seeding.
func (s *Seeder) NeedsSeed() (bool, error) {
	dumpTime, err := s.writer.GetSyncState("dump_time")
	if err != nil {
		return false, err
	}
	if dumpTime == "" {
		return true, nil
	}

	storedHash, err := s.writer.GetSyncState("config_hash")
	if err != nil {
		return false, err
	}
	if storedHash != configFingerprint() {
		log.Println("Seed configuration has changed, reseed required")
		return true, nil
	}

	return false, nil
}

// Seed performs a bulk import by streaming the Wikidata JSON dump.
// It respects ctx cancellation so the process can shut down promptly.
func (s *Seeder) Seed(ctx context.Context) error {
	// Check for resumable seed: if a previous seed was interrupted we can
	// skip already-processed lines when the ETag still matches.
	var skipLines int
	prevState, _ := s.writer.GetSyncState("state")
	if prevState == "seeding" {
		storedEtag, _ := s.writer.GetSyncState("seed_etag")
		lineStr, _ := s.writer.GetSyncState("seed_line")
		if storedEtag != "" && lineStr != "" {
			if n, err := strconv.Atoi(lineStr); err == nil && n > 0 {
				skipLines = n
			}
		}
	}

	log.Printf("Starting seed from Wikidata dump: %s", s.dumpURL)

	seedStart := time.Now()

	resp, err := s.openDumpStream(ctx)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	etag := resp.Header.Get("ETag")

	// Decide whether to resume or restart.
	if skipLines > 0 && etag != "" {
		storedEtag, _ := s.writer.GetSyncState("seed_etag")
		if etag == storedEtag {
			log.Printf("Resuming seed from line %d (ETag matches: %s)", skipLines, etag)
		} else {
			log.Printf("Cannot resume seed: ETag mismatch (stored=%s, current=%s), restarting", storedEtag, etag)
			skipLines = 0
		}
	} else {
		skipLines = 0
	}

	if skipLines == 0 {
		if err := s.writer.ClearSyncCursors(); err != nil {
			return err
		}
		if err := s.writer.SetSyncState("config_hash", configFingerprint()); err != nil {
			return fmt.Errorf("set config hash: %w", err)
		}
	}

	if err := s.writer.SetSyncState("state", "seeding"); err != nil {
		return fmt.Errorf("set state: %w", err)
	}
	if etag != "" {
		if err := s.writer.SetSyncState("seed_etag", etag); err != nil {
			return fmt.Errorf("set seed_etag: %w", err)
		}
	}

	syncTime := parseDumpTime(resp.Header.Get("Last-Modified"))
	log.Printf("Dump Last-Modified: %s → dump_time: %s",
		resp.Header.Get("Last-Modified"), syncTime)

	// Parse the dump time as a time.Time so we can use it for viewed_at.
	dumpTimeT, err := time.Parse(time.RFC3339, syncTime)
	if err != nil {
		return fmt.Errorf("parse dump time %q: %w", syncTime, err)
	}

	counter := &countingReader{r: resp.Body}

	// Run decompression in a background goroutine with a buffered channel
	// so it can run ahead of line parsing (important for CPU-heavy bzip2).
	chunks := make(chan []byte, dumpDecompChanCap)
	errCh := make(chan error, 1)
	chunkPool := &sync.Pool{
		New: func() any {
			b := make([]byte, dumpDecompChunkSize)
			return &b
		},
	}

	go func() {
		defer close(chunks)
		decompressed, err := s.newDecompressor(counter)
		if err != nil {
			errCh <- fmt.Errorf("create decompressor: %w", err)
			return
		}
		if closer, ok := decompressed.(io.Closer); ok {
			defer closer.Close()
		}
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}
			bufp := chunkPool.Get().(*[]byte)
			buf := (*bufp)[:dumpDecompChunkSize]
			n, err := decompressed.Read(buf)
			if n > 0 {
				select {
				case chunks <- buf[:n]:
				case <-ctx.Done():
					chunkPool.Put(bufp)
					errCh <- ctx.Err()
					return
				}
			} else {
				chunkPool.Put(bufp)
			}
			if err != nil {
				if err != io.EOF {
					errCh <- err
				}
				return
			}
		}
	}()

	decompReader := &chanReader{ch: chunks, errCh: errCh, pool: chunkPool}
	imported, lines, err := s.processDumpStream(ctx, decompReader, resp.ContentLength, counter, dumpTimeT, skipLines)
	if err != nil {
		return err
	}

	log.Printf("Sweeping stale entities not present in dump...")
	swept, err := s.writer.SweepStaleEntities(dumpTimeT.Unix())
	if err != nil {
		return fmt.Errorf("sweep stale entities: %w", err)
	}
	log.Printf("Swept %d stale entities not present in dump", swept)

	if err := s.writer.SetSyncState("dump_time", syncTime); err != nil {
		return fmt.Errorf("set dump_time: %w", err)
	}

	log.Printf("Dump seed complete: %d entities imported, %d stale entities swept from %d lines in %s",
		imported, swept, lines, time.Since(seedStart).Truncate(time.Second))
	return nil
}

// parseDumpTime parses the Last-Modified header and returns it as an RFC3339
// timestamp. Falls back to the current time if the header is missing.
func parseDumpTime(lastModified string) string {
	if lastModified != "" {
		if t, err := http.ParseTime(lastModified); err == nil {
			return t.UTC().Format(time.RFC3339)
		}
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func (s *Seeder) openDumpStream(ctx context.Context) (*http.Response, error) {
	req, err := newRequestWithContext(ctx, "GET", s.dumpURL)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download dump: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		return nil, fmt.Errorf("dump download returned status %d: %s", resp.StatusCode, string(body))
	}

	// Wrap body in resumableBody if the server provided an ETag.
	// This enables transparent reconnection via Range requests on network drops.
	etag := resp.Header.Get("ETag")
	if etag != "" {
		log.Printf("Dump ETag: %s (resumable download enabled)", etag)
		resp.Body = newResumableBody(resp.Body, s.dumpURL, etag, s.httpClient)
	} else {
		log.Println("No ETag in response; resumable download disabled")
	}

	return resp, nil
}

// seedItem wraps an entity record with its source line number for seed
// recovery tracking. This keeps line tracking out of the store layer.
type seedItem struct {
	store.EntityRecord
	line int
}

// processDumpStream reads decompressed JSON lines from r and upserts
// relevant entities. It uses a background goroutine to write to the DB
// while parsing continues, overlapping I/O with CPU work.
func (s *Seeder) processDumpStream(ctx context.Context, r io.Reader, totalCompressed int64, counter *countingReader, dumpTime time.Time, skipLines int) (int, int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, dumpScannerBufSize), dumpScannerBufSize)

	lines := 0
	started := time.Now()
	lastLog := started

	if skipLines > 0 {
		log.Printf("Skipping %d already-processed lines...", skipLines)
	}

	// Channel for sending entities to background writer
	entityCh := make(chan seedItem, 10000)

	var wg sync.WaitGroup
	wg.Add(1)

	// Background writer: receives entities, batches them, writes to DB
	go func() {
		defer wg.Done()

		batch := make([]store.EntityRecord, 0, dumpBatchSize)
		maxLine := 0
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			if err := s.writer.SeedEntitiesBatch(dumpTime, batch); err != nil {
				return err
			}
			// Persist the max line number in this batch for seed recovery.
			if maxLine > 0 {
				if err := s.writer.SetSyncState("seed_line", strconv.Itoa(maxLine)); err != nil {
					return err
				}
			}
			batch = batch[:0]
			maxLine = 0
			return nil
		}

		for item := range entityCh {
			batch = append(batch, item.EntityRecord)
			maxLine = max(maxLine, item.line)
			if len(batch) >= dumpBatchSize {
				if err := flush(); err != nil {
					log.Printf("Error in background writer: %v", err)
					return
				}
			}
		}

		// Flush remaining
		if err := flush(); err != nil {
			log.Printf("Error in background writer final flush: %v", err)
		}
	}()

	imported := 0

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			close(entityCh)
			wg.Wait()
			return imported, lines, ctx.Err()
		default:
		}
		line := scanner.Bytes()
		lines++

		// Skip lines already processed in a previous run.
		if lines <= skipLines {
			if now := time.Now(); now.Sub(lastLog) >= dumpProgressInterval {
				rate := float64(lines) / now.Sub(started).Seconds()
				log.Printf("  skip progress: %d/%d lines skipped (%.0f lines/sec)",
					lines, skipLines, rate)
				lastLog = now
			}
			continue
		}

		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == '[' || line[0] == ']' {
			continue
		}
		if line[len(line)-1] == ',' {
			line = line[:len(line)-1]
		}

		entity, err := parseDumpEntity(line)
		if err != nil {
			if lines <= 5 {
				log.Printf("Warning: failed to parse dump line %d: %v", lines, err)
			}
			continue
		}
		if entity == nil {
			continue
		}

		entityCh <- seedItem{
			EntityRecord: store.EntityRecord{
				WikidataID:  entity.ID,
				ExternalIDs: entity.ExternalIDs,
				Modified:    entity.Modified,
			},
			line: lines,
		}
		imported++

		if now := time.Now(); now.Sub(lastLog) >= dumpProgressInterval {
			rate := float64(lines) / now.Sub(started).Seconds()
			if totalCompressed > 0 && counter != nil {
				pct := float64(counter.bytes.Load()) / float64(totalCompressed) * 100
				log.Printf("  dump progress: %.1f%% | %d lines processed, %d entities imported (%.0f lines/sec)",
					pct, lines, imported, rate)
			} else {
				log.Printf("  dump progress: %d lines processed, %d entities imported (%.0f lines/sec)",
					lines, imported, rate)
			}
			lastLog = now
		}
	}

	// Close channel and wait for writer to finish
	close(entityCh)
	wg.Wait()

	if err := scanner.Err(); err != nil {
		return imported, lines, fmt.Errorf("scanner error at line %d: %w", lines, err)
	}

	return imported, lines, nil
}
