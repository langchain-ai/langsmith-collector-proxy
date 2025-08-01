package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/aggregator"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/config"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/server"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/uploader"
)

// Version is set at build time via ldflags
var Version = "dev"

func main() {
	cfg := config.Load()

	ch := make(chan *model.Run, 1024)
	trSrv := server.NewRouter(cfg, ch)

	up := uploader.New(uploader.Config{
		BaseURL:        cfg.LangsmithHost,
		MaxAttempts:    cfg.MaxRetries,
		BackoffInitial: cfg.BackoffInitial,
		BackoffMax:     30 * time.Second,
		InFlight:       100,
		APIKey:         cfg.DefaultAPIKey,
	})

	agg := aggregator.New(up, aggregator.Config{
		BatchSize:      cfg.BatchSize,
		FlushInterval:  cfg.FlushInterval,
		MaxBufferBytes: cfg.MaxBufferBytes,
		GCInterval:     2 * time.Minute,
		EntryTTL:       cfg.SpanTTL,
		FilterConfig: aggregator.FilterConfig{
			FilterNonGenAI: cfg.FilterNonGenAI,
		},
	}, ch)
	agg.Start()
	defer agg.Stop()

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      trSrv,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  75 * time.Second,
	}

	go func() {
		log.Printf("collector-proxy v%s listening on %s", Version, srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Printf("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown failed: %v", err)
	}

	close(ch)

	if err := agg.Flush(ctx); err != nil {
		log.Printf("failed to flush traces: %v", err)
	}

	agg.Stop()

	log.Printf("shutdown complete")
}
