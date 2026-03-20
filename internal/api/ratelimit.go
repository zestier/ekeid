package api

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// visitor tracks token bucket state for a single client IP.
type visitor struct {
	tokens   float64
	lastSeen time.Time
}

// RateLimiter implements a per-IP token bucket rate limiter.
type RateLimiter struct {
	mu       sync.Mutex
	visitors map[string]*visitor
	rate     float64 // tokens replenished per second
	burst    int     // maximum tokens (bucket capacity)
	stop     chan struct{}
	once     sync.Once // ensures Stop() is safe to call multiple times
}

// NewRateLimiter creates a new rate limiter. rate is tokens per second, burst is max bucket capacity.
// Starts a background cleanup goroutine. Call Stop() to release resources.
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	rl := &RateLimiter{
		visitors: make(map[string]*visitor),
		rate:     rate,
		burst:    burst,
		stop:     make(chan struct{}),
	}
	go rl.cleanup(1*time.Minute, 3*time.Minute)
	return rl
}

// Stop signals the cleanup goroutine to exit. Safe to call multiple times.
func (rl *RateLimiter) Stop() {
	rl.once.Do(func() { close(rl.stop) })
}

// allow checks whether a request from the given IP should be allowed.
// Returns: allowed, remaining tokens, Unix timestamp when bucket is full, retry-after seconds (if denied).
func (rl *RateLimiter) allow(ip string) (bool, int, int64, int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	v, exists := rl.visitors[ip]
	if !exists {
		v = &visitor{tokens: float64(rl.burst), lastSeen: now}
		rl.visitors[ip] = v
	}

	// Replenish tokens based on elapsed time
	elapsed := now.Sub(v.lastSeen).Seconds()
	v.tokens = math.Min(v.tokens+elapsed*rl.rate, float64(rl.burst))
	v.lastSeen = now

	// Calculate reset time (when bucket will be full)
	resetSeconds := math.Ceil((float64(rl.burst) - v.tokens) / rl.rate)
	resetUnix := now.Add(time.Duration(resetSeconds) * time.Second).Unix()

	if v.tokens < 1.0 {
		retryAfter := int(math.Ceil((1.0 - v.tokens) / rl.rate))
		if retryAfter < 1 {
			retryAfter = 1
		}
		return false, 0, resetUnix, retryAfter
	}

	v.tokens -= 1.0
	remaining := int(math.Floor(v.tokens))
	if remaining < 0 {
		remaining = 0
	}
	return true, remaining, resetUnix, 0
}

// Middleware returns an HTTP middleware that enforces rate limiting.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		allowed, remaining, resetUnix, retryAfter := rl.allow(ip)

		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(rl.burst))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetUnix, 10))

		if !allowed {
			log.Printf("rate_limited ip=%s path=%s retry_after=%d", ip, r.URL.Path, retryAfter)
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded",
				fmt.Sprintf("Rate limit exceeded. Retry after %d second(s).", retryAfter))
			return
		}

		next.ServeHTTP(w, r)
	})
}

// clientIP extracts the client IP from the request, checking proxy headers first.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if ip, _, _ := strings.Cut(xff, ","); ip != "" {
			return strings.TrimSpace(ip)
		}
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// cleanup periodically removes visitors that have been idle longer than maxIdle.
func (rl *RateLimiter) cleanup(interval, maxIdle time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rl.mu.Lock()
			cutoff := time.Now().Add(-maxIdle)
			for ip, v := range rl.visitors {
				if v.lastSeen.Before(cutoff) {
					delete(rl.visitors, ip)
				}
			}
			rl.mu.Unlock()
		case <-rl.stop:
			return
		}
	}
}
