package aggregator

import (
	"testing"
	"time"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/uploader"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/util"
)

func TestDefaultGenAIFilter(t *testing.T) {
	tests := []struct {
		name     string
		run      *model.Run
		expected bool
	}{
		{
			name: "GenAI run name",
			run: &model.Run{
				Name: util.StringPtr("gen_ai.completion"),
			},
			expected: true,
		},
		{
			name: "LangSmith run name",
			run: &model.Run{
				Name: util.StringPtr("langsmith.trace"),
			},
			expected: true,
		},
		{
			name: "LLM run name",
			run: &model.Run{
				Name: util.StringPtr("llm.openai"),
			},
			expected: true,
		},
		{
			name: "GenAI attribute in Extra",
			run: &model.Run{
				Name: util.StringPtr("regular_run"),
				Extra: map[string]interface{}{
					"gen_ai.system": "openai",
				},
			},
			expected: true,
		},
		{
			name: "Non-GenAI run",
			run: &model.Run{
				Name: util.StringPtr("http.request"),
				Extra: map[string]interface{}{
					"http.method": "GET",
				},
			},
			expected: false,
		},
		{
			name: "AI attribute prefix",
			run: &model.Run{
				Name: util.StringPtr("regular_run"),
				Extra: map[string]interface{}{
					"ai.model_name": "gpt-4",
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DefaultGenAIFilter(tt.run)
			if result != tt.expected {
				t.Errorf("DefaultGenAIFilter() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestFilterConfig_ShouldKeepRun(t *testing.T) {
	genAIRun := &model.Run{
		Name: util.StringPtr("gen_ai.completion"),
	}

	nonGenAIRun := &model.Run{
		Name: util.StringPtr("http.request"),
	}

	tests := []struct {
		name     string
		config   FilterConfig
		run      *model.Run
		expected bool
	}{
		{
			name:     "No filtering - keep all",
			config:   FilterConfig{FilterNonGenAI: false, CustomFilter: nil},
			run:      nonGenAIRun,
			expected: true,
		},
		{
			name:     "Filter non-GenAI - keep GenAI run",
			config:   FilterConfig{FilterNonGenAI: true, CustomFilter: nil},
			run:      genAIRun,
			expected: true,
		},
		{
			name:     "Filter non-GenAI - discard non-GenAI run",
			config:   FilterConfig{FilterNonGenAI: true, CustomFilter: nil},
			run:      nonGenAIRun,
			expected: false,
		},
		{
			name: "Custom filter - keep only http runs",
			config: FilterConfig{
				FilterNonGenAI: false,
				CustomFilter: func(run *model.Run) bool {
					return run.Name != nil && *run.Name == "http.request"
				},
			},
			run:      nonGenAIRun,
			expected: true,
		},
		{
			name: "Custom filter - discard GenAI runs",
			config: FilterConfig{
				FilterNonGenAI: false,
				CustomFilter: func(run *model.Run) bool {
					return run.Name != nil && *run.Name == "http.request"
				},
			},
			run:      genAIRun,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.ShouldKeepRun(tt.run)
			if result != tt.expected {
				t.Errorf("ShouldKeepRun() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestTreeReparenting(t *testing.T) {
	// Test the scenario: root -> parent -> filtered -> child
	// Expected: root -> parent -> child (filtered span removed, child reparented to parent)
	
	mockUploader := uploader.New(uploader.Config{
		BaseURL:     "http://localhost",
		APIKey:      "test-key",
		MaxAttempts: 1,
		InFlight:    1,
	})
	
	cfg := Config{
		BatchSize:     10,
		FlushInterval: 200 * time.Millisecond,
		FilterConfig: FilterConfig{
			FilterNonGenAI: true,
		},
	}

	ch := make(chan *model.Run, 10)
	agg := New(mockUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Send runs in order: root -> genai_parent -> http_filtered -> genai_child
	rootRun := &model.Run{
		ID:      util.StringPtr("root-id"),
		TraceID: util.StringPtr("trace-id"),
		Name:    util.StringPtr("gen_ai.root"),
	}

	genaiParent := &model.Run{
		ID:          util.StringPtr("genai-parent-id"),
		TraceID:     util.StringPtr("trace-id"),
		ParentRunID: util.StringPtr("root-id"),
		Name:        util.StringPtr("gen_ai.parent"),
	}

	httpFiltered := &model.Run{
		ID:          util.StringPtr("http-filtered-id"),
		TraceID:     util.StringPtr("trace-id"),
		ParentRunID: util.StringPtr("genai-parent-id"),
		Name:        util.StringPtr("http.request"), // Should be filtered
	}

	genaiChild := &model.Run{
		ID:          util.StringPtr("genai-child-id"),
		TraceID:     util.StringPtr("trace-id"),
		ParentRunID: util.StringPtr("http-filtered-id"), // Parent will be filtered
		Name:        util.StringPtr("gen_ai.child"),
	}

	// Send runs
	ch <- rootRun
	time.Sleep(10 * time.Millisecond)
	ch <- genaiParent
	time.Sleep(10 * time.Millisecond)
	ch <- httpFiltered
	time.Sleep(10 * time.Millisecond)
	ch <- genaiChild

	// Wait for processing
	time.Sleep(300 * time.Millisecond)

	// Verify filtered run is tracked
	if _, exists := agg.filteredIDs.Load("http-filtered-id"); !exists {
		t.Error("Expected filtered run ID to be tracked")
	}

	// Verify kept runs are not marked as filtered
	if _, exists := agg.filteredIDs.Load("root-id"); exists {
		t.Error("Expected root run not to be marked as filtered")
	}
	if _, exists := agg.filteredIDs.Load("genai-parent-id"); exists {
		t.Error("Expected GenAI parent run not to be marked as filtered")
	}
	if _, exists := agg.filteredIDs.Load("genai-child-id"); exists {
		t.Error("Expected GenAI child run not to be marked as filtered")
	}
}

func TestMultiLevelFiltering(t *testing.T) {
	// Test scenario: root -> filtered1 -> filtered2 -> child
	// Expected: root -> child (both intermediate spans filtered, child reparented to root)
	
	mockUploader := uploader.New(uploader.Config{
		BaseURL:     "http://localhost",
		APIKey:      "test-key",
		MaxAttempts: 1,
		InFlight:    1,
	})
	
	cfg := Config{
		BatchSize:     10,
		FlushInterval: 200 * time.Millisecond,
		FilterConfig: FilterConfig{
			FilterNonGenAI: true,
		},
	}

	ch := make(chan *model.Run, 10)
	agg := New(mockUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	runs := []*model.Run{
		{
			ID:      util.StringPtr("root-id"),
			TraceID: util.StringPtr("trace-id"),
			Name:    util.StringPtr("gen_ai.root"),
		},
		{
			ID:          util.StringPtr("filtered1-id"),
			TraceID:     util.StringPtr("trace-id"),
			ParentRunID: util.StringPtr("root-id"),
			Name:        util.StringPtr("http.request1"), // Filtered
		},
		{
			ID:          util.StringPtr("filtered2-id"),
			TraceID:     util.StringPtr("trace-id"),
			ParentRunID: util.StringPtr("filtered1-id"),
			Name:        util.StringPtr("http.request2"), // Filtered
		},
		{
			ID:          util.StringPtr("child-id"),
			TraceID:     util.StringPtr("trace-id"),
			ParentRunID: util.StringPtr("filtered2-id"),
			Name:        util.StringPtr("gen_ai.child"),
		},
	}

	// Send all runs
	for _, run := range runs {
		ch <- run
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for processing
	time.Sleep(300 * time.Millisecond)

	// Verify both filtered runs are tracked
	if _, exists := agg.filteredIDs.Load("filtered1-id"); !exists {
		t.Error("Expected first filtered run to be tracked")
	}
	if _, exists := agg.filteredIDs.Load("filtered2-id"); !exists {
		t.Error("Expected second filtered run to be tracked")
	}

	// Verify kept runs are not filtered
	if _, exists := agg.filteredIDs.Load("root-id"); exists {
		t.Error("Expected root run not to be marked as filtered")
	}
	if _, exists := agg.filteredIDs.Load("child-id"); exists {
		t.Error("Expected child run not to be marked as filtered")
	}
}

func TestOutOfOrderArrival(t *testing.T) {
	// Test cross-request scenario: child arrives before filtered parent
	
	mockUploader := uploader.New(uploader.Config{
		BaseURL:     "http://localhost",
		APIKey:      "test-key",
		MaxAttempts: 1,
		InFlight:    1,
	})
	
	cfg := Config{
		BatchSize:     10,
		FlushInterval: 200 * time.Millisecond,
		FilterConfig: FilterConfig{
			FilterNonGenAI: true,
		},
	}

	ch := make(chan *model.Run, 10)
	agg := New(mockUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	// Send child first (it will wait for parent)
	genaiChild := &model.Run{
		ID:          util.StringPtr("genai-child-id"),
		TraceID:     util.StringPtr("trace-id"),
		ParentRunID: util.StringPtr("filtered-parent-id"),
		Name:        util.StringPtr("gen_ai.child"),
	}
	ch <- genaiChild
	time.Sleep(50 * time.Millisecond)

	// Send root
	rootRun := &model.Run{
		ID:      util.StringPtr("root-id"),
		TraceID: util.StringPtr("trace-id"),
		Name:    util.StringPtr("gen_ai.root"),
	}
	ch <- rootRun
	time.Sleep(10 * time.Millisecond)

	// Send filtered parent (this should trigger reparenting of waiting child)
	filteredParent := &model.Run{
		ID:          util.StringPtr("filtered-parent-id"),
		TraceID:     util.StringPtr("trace-id"),
		ParentRunID: util.StringPtr("root-id"),
		Name:        util.StringPtr("http.request"), // Will be filtered
	}
	ch <- filteredParent

	// Wait for processing
	time.Sleep(300 * time.Millisecond)

	// Verify filtered parent is tracked
	if _, exists := agg.filteredIDs.Load("filtered-parent-id"); !exists {
		t.Error("Expected filtered parent to be tracked")
	}

	// Child should not be filtered
	if _, exists := agg.filteredIDs.Load("genai-child-id"); exists {
		t.Error("Expected child not to be marked as filtered")
	}
}

func TestCustomFilter(t *testing.T) {
	mockUploader := uploader.New(uploader.Config{
		BaseURL:     "http://localhost",
		APIKey:      "test-key",
		MaxAttempts: 1,
		InFlight:    1,
	})
	
	cfg := Config{
		BatchSize:     10,
		FlushInterval: 200 * time.Millisecond,
		FilterConfig: FilterConfig{
			FilterNonGenAI: false,
			CustomFilter: func(run *model.Run) bool {
				// Keep only runs with "important" in name
				return run.Name != nil && (*run.Name == "important.task" || *run.Name == "gen_ai.completion")
			},
		},
	}

	ch := make(chan *model.Run, 10)
	agg := New(mockUploader, cfg, ch)
	agg.Start()
	defer agg.Stop()

	runs := []*model.Run{
		{
			ID:      util.StringPtr("root-id"),
			TraceID: util.StringPtr("trace-id"),
			Name:    util.StringPtr("important.task"),
		},
		{
			ID:          util.StringPtr("filtered-id"),
			TraceID:     util.StringPtr("trace-id"),
			ParentRunID: util.StringPtr("root-id"),
			Name:        util.StringPtr("regular.task"), // Will be filtered by custom filter
		},
		{
			ID:          util.StringPtr("child-id"),
			TraceID:     util.StringPtr("trace-id"),
			ParentRunID: util.StringPtr("filtered-id"),
			Name:        util.StringPtr("gen_ai.completion"),
		},
	}

	for _, run := range runs {
		ch <- run
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(300 * time.Millisecond)

	// Verify custom filtering worked
	if _, exists := agg.filteredIDs.Load("filtered-id"); !exists {
		t.Error("Expected custom filtered run to be tracked")
	}

	if _, exists := agg.filteredIDs.Load("root-id"); exists {
		t.Error("Expected important task not to be filtered")
	}

	if _, exists := agg.filteredIDs.Load("child-id"); exists {
		t.Error("Expected gen_ai.completion not to be filtered")
	}
}