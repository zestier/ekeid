package store

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// TODO: https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
const userAgent = "EkeidBot/0.1.0"

// Wikimedia robot policy limits for unauthenticated bots.
// https://wikitech.wikimedia.org/wiki/Robot_policy
const (
	wikimediaRateLimit          = 5               // max requests per second (unauthenticated)
	wikimediaBurst              = 1               // max concurrent requests (unauthenticated)
	wikimediaExpensiveThreshold = 1 * time.Second // response time that triggers cooldown
	wikimediaExpensiveCooldown  = 5 * time.Second // wait after an expensive response
	wikimediaMaxlag             = 5               // seconds; lower = more generous to Wikimedia servers
)

// HTTPDoer executes HTTP requests. Both *http.Client and *ThrottledClient
// satisfy this interface, so callers that accept HTTPDoer cannot bypass
// throttling — the only way to get unthrottled is to pass a raw *http.Client,
// which is only done in tests.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// ThrottledClient wraps an *http.Client with Wikimedia robot policy rate
// limits. It serialises requests (concurrency=1), limits the rate to
// 5 req/sec for unauthenticated bots, and applies a 5-second cooldown
// after any response that takes longer than 1 second (expensive endpoint).
type ThrottledClient struct {
	mu      sync.Mutex
	limiter *rate.Limiter
	client  *http.Client
}

// NewThrottledClient creates a ThrottledClient configured for Wikimedia's
// unauthenticated robot policy (5 req/sec, concurrency 1).
func NewThrottledClient(client *http.Client) *ThrottledClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &ThrottledClient{
		limiter: rate.NewLimiter(rate.Limit(wikimediaRateLimit), wikimediaBurst),
		client:  client,
	}
}

// Do executes an HTTP request through the throttle, enforcing rate limits
// and concurrency=1. If the response takes longer than 1 second,
// an additional 5-second cooldown is applied before the next request.
func (t *ThrottledClient) Do(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := t.client.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		return nil, err
	}

	if elapsed > wikimediaExpensiveThreshold {
		log.Printf("Wikimedia request took %v (>%v), applying %v cooldown",
			elapsed.Truncate(time.Millisecond), wikimediaExpensiveThreshold, wikimediaExpensiveCooldown)
		select {
		case <-ctx.Done():
		case <-time.After(wikimediaExpensiveCooldown):
		}
	}

	return resp, nil
}

// newRequest creates an HTTP request with common headers (User-Agent) pre-set.
func newRequest(method, url string) (*http.Request, error) {
	return newRequestWithContext(context.Background(), method, url)
}

// newRequestWithContext creates an HTTP request with context and common headers pre-set.
func newRequestWithContext(ctx context.Context, method, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	return req, nil
}
