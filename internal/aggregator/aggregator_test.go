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

// TestUploader for capturing sent batches
type TestUploader struct {
	mu      sync.Mutex
	batches []uploader.Batch
}

func NewTestUploader() *TestUploader {
	return &TestUploader{batches: make([]uploader.Batch, 0)}
}

func (tu *TestUploader) Send(ctx context.Context, b uploader.Batch) {
	tu.mu.Lock()
	defer tu.mu.Unlock()
	tu.batches = append(tu.batches, b)
}

func (tu *TestUploader) WaitForCompletion(ctx context.Context) error {
	return nil
}

func (tu *TestUploader) GetBatchCount() int {
	tu.mu.Lock()
	defer tu.mu.Unlock()
	return len(tu.batches)
}

func (tu *TestUploader) Clear() {
	tu.mu.Lock()
	defer tu.mu.Unlock()
	tu.batches = tu.batches[:0]
}

func TestBasicRunProcessing(t *testing.T) {
	cfg := Config{
		BatchSize:     1, // Process immediately
		FlushInterval: 10 * time.Second,
		FilterConfig:  FilterConfig{FilterNonGenAI: false},
	}

	ch := make(chan *model.Run, 10)
	testUploader := NewTestUploader()
	agg := New(testUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Send a simple root run
	root := &model.Run{
		ID:      util.StringPtr("root"),
		TraceID: util.StringPtr("trace1"),
		Name:    util.StringPtr("root_span"),
	}

	ch <- root
	time.Sleep(100 * time.Millisecond)

	// Should have sent one batch
	if testUploader.GetBatchCount() != 1 {
		t.Fatalf("Expected 1 batch, got %d", testUploader.GetBatchCount())
	}
}

func TestParentChildOrdering(t *testing.T) {
	cfg := Config{
		BatchSize:     1,
		FlushInterval: 10 * time.Second,
		FilterConfig:  FilterConfig{FilterNonGenAI: false},
	}

	ch := make(chan *model.Run, 10)
	testUploader := NewTestUploader()
	agg := New(testUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Send parent first
	parent := &model.Run{
		ID:      util.StringPtr("parent"),
		TraceID: util.StringPtr("trace1"),
		Name:    util.StringPtr("parent_span"),
	}

	child := &model.Run{
		ID:          util.StringPtr("child"),
		TraceID:     util.StringPtr("trace1"),
		ParentRunID: util.StringPtr("parent"),
		Name:        util.StringPtr("child_span"),
	}

	ch <- parent
	time.Sleep(50 * time.Millisecond)
	ch <- child
	time.Sleep(50 * time.Millisecond)

	// Should have sent two batches
	if testUploader.GetBatchCount() != 2 {
		t.Fatalf("Expected 2 batches, got %d", testUploader.GetBatchCount())
	}
}

func TestChildBeforeParent(t *testing.T) {
	cfg := Config{
		BatchSize:     1, // Process immediately when parent arrives
		FlushInterval: 10 * time.Second,
		FilterConfig:  FilterConfig{FilterNonGenAI: false},
	}

	ch := make(chan *model.Run, 10)
	testUploader := NewTestUploader()
	agg := New(testUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Send child first
	child := &model.Run{
		ID:          util.StringPtr("child"),
		TraceID:     util.StringPtr("trace1"),
		ParentRunID: util.StringPtr("parent"),
		Name:        util.StringPtr("child_span"),
	}

	ch <- child
	time.Sleep(50 * time.Millisecond)

	// No batches should be sent yet (child waiting for parent)
	if testUploader.GetBatchCount() != 0 {
		t.Errorf("Expected 0 batches before parent, got %d", testUploader.GetBatchCount())
	}

	// Send parent
	parent := &model.Run{
		ID:      util.StringPtr("parent"),
		TraceID: util.StringPtr("trace1"),
		Name:    util.StringPtr("parent_span"),
	}

	ch <- parent
	time.Sleep(100 * time.Millisecond)

	// Now both should be processed
	if testUploader.GetBatchCount() != 2 {
		t.Fatalf("Expected 2 batches after parent, got %d", testUploader.GetBatchCount())
	}
}

func TestFilteringBasic(t *testing.T) {
	cfg := Config{
		BatchSize:     1,
		FlushInterval: 10 * time.Second,
		FilterConfig:  FilterConfig{FilterNonGenAI: true},
	}

	ch := make(chan *model.Run, 10)
	testUploader := NewTestUploader()
	agg := New(testUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Send GenAI run (should be kept)
	genaiRun := &model.Run{
		ID:      util.StringPtr("genai"),
		TraceID: util.StringPtr("trace1"),
		Name:    util.StringPtr("gen_ai.completion"),
	}

	// Send HTTP run (should be filtered)
	httpRun := &model.Run{
		ID:      util.StringPtr("http"),
		TraceID: util.StringPtr("trace1"),
		Name:    util.StringPtr("http.request"),
	}

	ch <- genaiRun
	ch <- httpRun
	time.Sleep(100 * time.Millisecond)

	// Only GenAI run should be processed
	if testUploader.GetBatchCount() != 1 {
		t.Fatalf("Expected 1 batch (GenAI only), got %d", testUploader.GetBatchCount())
	}

	// Verify filtered run is tracked
	if _, exists := agg.filteredIDs.Load("http"); !exists {
		t.Error("Expected HTTP run to be tracked as filtered")
	}

	// Verify GenAI run is not tracked as filtered
	if _, exists := agg.filteredIDs.Load("genai"); exists {
		t.Error("Expected GenAI run not to be tracked as filtered")
	}
}

func TestFilteringWithReparenting(t *testing.T) {
	cfg := Config{
		BatchSize:     1,
		FlushInterval: 10 * time.Second,
		FilterConfig:  FilterConfig{FilterNonGenAI: true},
	}

	ch := make(chan *model.Run, 10)
	testUploader := NewTestUploader()
	agg := New(testUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Create hierarchy: genai_root -> http_filtered -> genai_child
	root := &model.Run{
		ID:      util.StringPtr("root"),
		TraceID: util.StringPtr("trace1"),
		Name:    util.StringPtr("gen_ai.root"),
	}

	filtered := &model.Run{
		ID:          util.StringPtr("filtered"),
		TraceID:     util.StringPtr("trace1"),
		ParentRunID: util.StringPtr("root"),
		Name:        util.StringPtr("http.request"), // Will be filtered
	}

	child := &model.Run{
		ID:          util.StringPtr("child"),
		TraceID:     util.StringPtr("trace1"),
		ParentRunID: util.StringPtr("filtered"),
		Name:        util.StringPtr("gen_ai.child"),
	}

	ch <- root
	time.Sleep(50 * time.Millisecond)
	ch <- filtered
	time.Sleep(50 * time.Millisecond)
	ch <- child
	time.Sleep(100 * time.Millisecond)

	// Should have 2 batches (root and child, filtered one excluded)
	if testUploader.GetBatchCount() != 2 {
		t.Fatalf("Expected 2 batches after filtering, got %d", testUploader.GetBatchCount())
	}

	// Verify filtered run is tracked
	if _, exists := agg.filteredIDs.Load("filtered"); !exists {
		t.Error("Expected filtered run to be tracked")
	}
}

func TestGarbageCollection(t *testing.T) {
	cfg := Config{
		BatchSize:     10,
		FlushInterval: 10 * time.Second,
		GCInterval:    50 * time.Millisecond,  // Fast GC for testing
		EntryTTL:      100 * time.Millisecond, // Short TTL for testing
		FilterConfig:  FilterConfig{FilterNonGenAI: false},
	}

	ch := make(chan *model.Run, 10)
	testUploader := NewTestUploader()
	agg := New(testUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Send orphan child (parent never arrives)
	orphan := &model.Run{
		ID:          util.StringPtr("orphan"),
		TraceID:     util.StringPtr("trace1"),
		ParentRunID: util.StringPtr("missing_parent"),
		Name:        util.StringPtr("orphan_span"),
	}

	ch <- orphan
	time.Sleep(50 * time.Millisecond)

	// No batches yet (waiting for parent)
	if testUploader.GetBatchCount() != 0 {
		t.Errorf("Expected 0 batches before GC, got %d", testUploader.GetBatchCount())
	}

	// Wait for GC to process orphan
	time.Sleep(200 * time.Millisecond)

	// Orphan should be processed after GC
	if testUploader.GetBatchCount() != 1 {
		t.Fatalf("Expected 1 batch after GC, got %d", testUploader.GetBatchCount())
	}
}

func TestFlushFunctionality(t *testing.T) {
	cfg := Config{
		BatchSize:     10, // Large batch size to prevent auto-flushing
		FlushInterval: 10 * time.Second,
		FilterConfig:  FilterConfig{FilterNonGenAI: false},
	}

	ch := make(chan *model.Run, 10)
	testUploader := NewTestUploader()
	agg := New(testUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Send a run
	run := &model.Run{
		ID:      util.StringPtr("test"),
		TraceID: util.StringPtr("trace1"),
		Name:    util.StringPtr("test_span"),
	}

	ch <- run
	time.Sleep(100 * time.Millisecond) // Give more time for processing

	// No batches yet (batch size not reached)
	initialCount := testUploader.GetBatchCount()
	t.Logf("Batches before flush: %d", initialCount)

	// Force flush
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := agg.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // Give time for flush to complete

	// Should have at least one batch after flush
	finalCount := testUploader.GetBatchCount()
	t.Logf("Batches after flush: %d", finalCount)

	if finalCount == 0 {
		t.Fatal("Expected at least 1 batch after flush, got 0")
	}
}

func TestNilRunHandling(t *testing.T) {
	cfg := Config{
		BatchSize:     1,
		FlushInterval: 10 * time.Second,
		FilterConfig:  FilterConfig{FilterNonGenAI: false},
	}

	ch := make(chan *model.Run, 10)
	testUploader := NewTestUploader()
	agg := New(testUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Send nil run - should be ignored
	ch <- nil
	time.Sleep(50 * time.Millisecond)

	// Send valid run
	validRun := &model.Run{
		ID:      util.StringPtr("valid"),
		TraceID: util.StringPtr("trace1"),
		Name:    util.StringPtr("valid_span"),
	}
	ch <- validRun
	time.Sleep(50 * time.Millisecond)

	// Should have 1 batch (nil run ignored)
	if testUploader.GetBatchCount() != 1 {
		t.Fatalf("Expected 1 batch (nil should be ignored), got %d", testUploader.GetBatchCount())
	}
}

func TestMultipleTracesInterleaved(t *testing.T) {
	cfg := Config{
		BatchSize:     1,
		FlushInterval: 10 * time.Second,
		FilterConfig:  FilterConfig{FilterNonGenAI: false},
	}

	ch := make(chan *model.Run, 20)
	testUploader := NewTestUploader()
	agg := New(testUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Create runs for multiple traces
	trace1Runs := []*model.Run{
		{ID: util.StringPtr("t1_root"), TraceID: util.StringPtr("trace1"), Name: util.StringPtr("t1_root")},
		{ID: util.StringPtr("t1_child"), TraceID: util.StringPtr("trace1"), ParentRunID: util.StringPtr("t1_root"), Name: util.StringPtr("t1_child")},
	}

	trace2Runs := []*model.Run{
		{ID: util.StringPtr("t2_root"), TraceID: util.StringPtr("trace2"), Name: util.StringPtr("t2_root")},
		{ID: util.StringPtr("t2_child"), TraceID: util.StringPtr("trace2"), ParentRunID: util.StringPtr("t2_root"), Name: util.StringPtr("t2_child")},
	}

	// Interleave runs from different traces
	ch <- trace1Runs[0]
	ch <- trace2Runs[0]
	ch <- trace1Runs[1]
	ch <- trace2Runs[1]

	time.Sleep(200 * time.Millisecond)

	// Should have 4 batches (one for each run)
	if testUploader.GetBatchCount() != 4 {
		t.Fatalf("Expected 4 batches from 2 traces, got %d", testUploader.GetBatchCount())
	}
}

func TestComplexFilteringScenario(t *testing.T) {
	cfg := Config{
		BatchSize:     1,
		FlushInterval: 10 * time.Second,
		FilterConfig: FilterConfig{
			FilterNonGenAI: false,
			CustomFilter: func(run *model.Run) bool {
				// Keep only runs with "keep" in their name
				return run.Name != nil && (*run.Name == "keep_root" || *run.Name == "keep_child")
			},
		},
	}

	ch := make(chan *model.Run, 10)
	testUploader := NewTestUploader()
	agg := New(testUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Create hierarchy where middle span is filtered
	// keep_root -> filter_middle -> keep_child
	runs := []*model.Run{
		{ID: util.StringPtr("root"), TraceID: util.StringPtr("trace1"), Name: util.StringPtr("keep_root")},
		{ID: util.StringPtr("middle"), TraceID: util.StringPtr("trace1"), ParentRunID: util.StringPtr("root"), Name: util.StringPtr("filter_middle")},
		{ID: util.StringPtr("child"), TraceID: util.StringPtr("trace1"), ParentRunID: util.StringPtr("middle"), Name: util.StringPtr("keep_child")},
	}

	for _, run := range runs {
		ch <- run
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(100 * time.Millisecond)

	// Should have 2 batches (root and child, middle filtered)
	if testUploader.GetBatchCount() != 2 {
		t.Fatalf("Expected 2 batches after custom filtering, got %d", testUploader.GetBatchCount())
	}

	// Verify filtered run is tracked
	if _, exists := agg.filteredIDs.Load("middle"); !exists {
		t.Error("Expected middle run to be tracked as filtered")
	}
}
