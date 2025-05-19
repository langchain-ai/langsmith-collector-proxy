package uploader

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	"golang.org/x/sync/semaphore"
)

type Batch struct {
	Data     []byte
	Boundary string
}

type Config struct {
	BaseURL        string
	APIKey         string
	MaxAttempts    int
	BackoffInitial time.Duration
	BackoffMax     time.Duration
	InFlight       int
}

// Uploader is a thread-safe wrapper around a semaphore and HTTP client.
// It is used to send batches of runs to the LangSmith API.
//
// It will retry retryable errors with exponential backoff + jitter up to
// MaxAttempts times.
type Uploader struct {
	cfg    Config
	sem    *semaphore.Weighted
	client *http.Client
}

func New(cfg Config) *Uploader {
	return &Uploader{
		cfg: cfg,
		sem: semaphore.NewWeighted(int64(max(1, cfg.InFlight))),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (u *Uploader) Send(ctx context.Context, b Batch) {
	if err := u.sem.Acquire(ctx, 1); err != nil {
		slog.Warn("uploader ctx cancelled before send")
		return
	}
	go func() {
		defer u.sem.Release(1)
		u.send(ctx, b)
	}()
}

func (u *Uploader) send(ctx context.Context, b Batch) {
	url := u.cfg.BaseURL + "/runs/multipart"
	var attempt int
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b.Data))
		req.Header.Set("Content-Type", "multipart/form-data; boundary="+b.Boundary)
		req.Header.Set("Content-Encoding", "zstd")
		req.Header.Set("X-API-Key", u.cfg.APIKey)

		resp, err := u.client.Do(req)
		if err == nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted) {
			if resp != nil {
				resp.Body.Close()
			}
			slog.Info("batch uploaded")
			return
		}

		shouldRetry := false
		if err != nil {
			shouldRetry = true
		} else if resp != nil {
			switch resp.StatusCode {
			case http.StatusBadGateway, // 502
				http.StatusServiceUnavailable,  // 503
				http.StatusGatewayTimeout,      // 504
				http.StatusRequestTimeout,      // 408
				http.StatusTooEarly,            // 425
				http.StatusTooManyRequests,     // 429
				http.StatusInternalServerError, // 500
				499:                            // 499 (client closed request)
				shouldRetry = true
			}
		}

		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			slog.Error("upload failed",
				"attempts", attempt, "err", err,
				"status", resp.StatusCode, "response", string(body),
				"will_retry", shouldRetry)
		}

		if !shouldRetry {
			slog.Error("upload failed; dropping batch (non-retryable error)",
				"attempts", attempt, "err", err)
			return
		}

		attempt++
		if attempt >= u.cfg.MaxAttempts {
			slog.Error("upload failed; dropping batch (max attempts reached)",
				"attempts", attempt, "err", err)
			return
		}
		delay := backoff(u.cfg.BackoffInitial, u.cfg.BackoffMax, attempt)
		slog.Warn("upload retry", "attempt", attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
	}
}

func backoff(base, max time.Duration, attempt int) time.Duration {
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	d := time.Duration(exp)
	if d > max {
		d = max
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	r := binary.BigEndian.Uint64(b[:])
	jitter := time.Duration(r % uint64(d/2))
	return d/2 + jitter
}
