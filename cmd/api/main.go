package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ekeid/ekeid/internal/api"
	"github.com/ekeid/ekeid/internal/store"
)

const version = "0.1.0"

func main() {
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "/data/ekeid.db"
	}

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	rateLimit := 10.0
	if v := os.Getenv("RATE_LIMIT"); v != "" {
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil || parsed < 0 {
			log.Fatalf("Invalid RATE_LIMIT %q: must be a non-negative number", v)
		}
		rateLimit = parsed
	}

	rateBurst := 20
	if v := os.Getenv("RATE_BURST"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed < 1 {
			log.Fatalf("Invalid RATE_BURST %q: must be a positive integer", v)
		}
		rateBurst = parsed
	}

	reader, err := store.NewReader(dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer reader.Close()

	var rateLimiter *api.RateLimiter
	if rateLimit > 0 {
		rateLimiter = api.NewRateLimiter(rateLimit, rateBurst)
		defer rateLimiter.Stop()
	}

	srv := api.NewServer(reader, version, rateLimiter)

	httpServer := &http.Server{
		Addr:         addr,
		Handler:      srv.Handler(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("API server starting on %s (version %s, rate_limit=%.1f/s, rate_burst=%d)", addr, version, rateLimit, rateBurst)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Shutdown error: %v", err)
	}

	log.Println("Server stopped")
}
