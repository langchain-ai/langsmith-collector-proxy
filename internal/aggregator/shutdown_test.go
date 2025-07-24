package aggregator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/uploader"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/util"
)

type MockUploader struct {
	mu        sync.Mutex
	batches   []uploader.Batch
	sendDelay time.Duration
	wg        sync.WaitGroup
}

func NewMockUploader() *MockUploader {
	return &MockUploader{
		batches: make([]uploader.Batch, 0),
	}
}

func (m *MockUploader) Send(ctx context.Context, b uploader.Batch) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		// Simulate network delay if configured
		if m.sendDelay > 0 {
			select {
			case <-time.After(m.sendDelay):
			case <-ctx.Done():
				return
			}
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		m.batches = append(m.batches, b)
	}()
}

func (m *MockUploader) WaitForCompletion(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *MockUploader) GetBatches() []uploader.Batch {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]uploader.Batch, len(m.batches))
	copy(result, m.batches)
	return result
}

func (m *MockUploader) SetSendDelay(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendDelay = delay
}

func TestGracefulShutdownFlushesData(t *testing.T) {
	mockUploader := NewMockUploader()

	cfg := Config{
		BatchSize:     10,               // High batch size to prevent automatic flushing
		FlushInterval: 10 * time.Second, // Long interval to prevent automatic flushing
		FilterConfig: FilterConfig{
			FilterNonGenAI: false, // Don't filter anything
		},
	}

	ch := make(chan *model.Run, 10)
	agg := New(mockUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	testRun := &model.Run{
		ID:          util.StringPtr("run1"),
		TraceID:     util.StringPtr("trace1"),
		ParentRunID: nil, // Root run
		Name:        util.StringPtr("test.run1"),
	}

	ch <- testRun

	time.Sleep(200 * time.Millisecond)

	batches := mockUploader.GetBatches()
	t.Logf("Batches before flush: %d", len(batches))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := agg.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	batches = mockUploader.GetBatches()
	t.Logf("Batches after flush: %d", len(batches))
	if len(batches) == 0 {
		t.Error("Expected at least 1 batch after flush, got 0")
	}
}

func TestGracefulShutdownWithSlowUploader(t *testing.T) {
	mockUploader := NewMockUploader()
	mockUploader.SetSendDelay(300 * time.Millisecond)

	cfg := Config{
		BatchSize:     1,
		FlushInterval: 10 * time.Second,
		FilterConfig: FilterConfig{
			FilterNonGenAI: false,
		},
	}

	ch := make(chan *model.Run, 10)
	agg := New(mockUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	testRun := &model.Run{
		ID:          util.StringPtr("run1"),
		TraceID:     util.StringPtr("trace1"),
		ParentRunID: nil, // Root run
		Name:        util.StringPtr("test.run1"),
	}

	ch <- testRun

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	if err := agg.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	elapsed := time.Since(start)

	t.Logf("Flush took %v", elapsed)

	if elapsed < 200*time.Millisecond {
		t.Errorf("Flush completed too quickly (%v), expected to wait for slow upload", elapsed)
	}

	batches := mockUploader.GetBatches()
	if len(batches) == 0 {
		t.Error("Expected at least 1 batch after flush, got 0")
	}
}
