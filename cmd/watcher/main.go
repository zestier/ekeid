package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ekeid/ekeid/internal/store"
	"github.com/ekeid/ekeid/internal/watcher"
)

func main() {
	contact := os.Getenv("CONTACT")
	if contact == "" {
		log.Fatal("CONTACT env var is required (URL or email for Wikimedia User-Agent policy)")
	}
	transport := watcher.NewThrottleTransport(
		watcher.NewUserAgentTransport(contact, http.DefaultTransport),
	)

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "/data/ekeid.db"
	}

	writer, err := store.NewWriter(dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer writer.Close()

	httpClient := &http.Client{Timeout: 5 * time.Minute, Transport: transport}
	wikidataClient := watcher.NewWikidataClient(httpClient)

	processor := watcher.NewProcessor(writer, wikidataClient)
	dumpClient := &http.Client{Timeout: 0, Transport: transport}
	seeder := watcher.NewSeeder(writer, dumpClient, watcher.DumpFormat(os.Getenv("DUMP_FORMAT")))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sseClient := &http.Client{Timeout: 0, Transport: transport}
	esWatcher := watcher.NewEventStreamWatcher(processor, writer, sseClient)

	for {
		needsSeed, err := seeder.NeedsSeed()
		if err != nil {
			log.Fatalf("Failed to check seed state: %v", err)
		}

		if needsSeed {
			log.Println("Database needs seeding...")
			if err := seeder.Seed(); err != nil {
				log.Fatalf("Seed failed: %v", err)
			}
		} else {
			log.Println("Database is up to date, skipping seed")
		}

		if err := writer.SetSyncState("state", "streaming"); err != nil {
			log.Fatalf("Failed to set state: %v", err)
		}

		log.Println("Starting EventStream watcher...")
		err = esWatcher.Watch(ctx)
		if errors.Is(err, watcher.ErrStreamTooOld) {
			log.Println("Stream does not go back far enough, reseeding...")
			continue
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("Watcher error: %v", err)
		}
		break
	}

	log.Println("Watcher stopped")
}
