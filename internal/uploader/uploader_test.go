package uploader

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWaitForCompletion(t *testing.T) {
	// Create a test server that responds slowly
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	uploader := New(Config{
		BaseURL:        server.URL,
		APIKey:         "test-key",
		MaxAttempts:    1,
		BackoffInitial: 100 * time.Millisecond,
		BackoffMax:     1 * time.Second,
		InFlight:       2,
	})

	batch1 := Batch{Data: []byte("test1"), Boundary: "boundary1"}
	batch2 := Batch{Data: []byte("test2"), Boundary: "boundary2"}

	ctx := context.Background()
	uploader.Send(ctx, batch1)
	uploader.Send(ctx, batch2)

	start := time.Now()
	if err := uploader.WaitForCompletion(ctx); err != nil {
		t.Fatalf("WaitForCompletion failed: %v", err)
	}
	elapsed := time.Since(start)

	// Should have waited for both uploads to complete
	if elapsed < 150*time.Millisecond {
		t.Errorf("WaitForCompletion completed too quickly (%v), expected to wait for uploads", elapsed)
	}
}

func TestWaitForCompletionWithTimeout(t *testing.T) {
	// Create a test server that responds very slowly
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	uploader := New(Config{
		BaseURL:        server.URL,
		APIKey:         "test-key",
		MaxAttempts:    1,
		BackoffInitial: 100 * time.Millisecond,
		BackoffMax:     1 * time.Second,
		InFlight:       1,
	})

	// Send a batch
	batch := Batch{Data: []byte("test"), Boundary: "boundary"}
	uploader.Send(context.Background(), batch)

	// Wait for completion with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := uploader.WaitForCompletion(ctx)
	if err == nil {
		t.Error("Expected WaitForCompletion to timeout, but it succeeded")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
}
