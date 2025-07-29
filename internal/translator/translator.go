package translator

import (
	"sync"
	"time"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/util"
)

type spanEntry struct {
	trace string
	ts    time.Time
}

type Translator struct {
	converter          *GenAiConverter
	mu                 sync.RWMutex
	span2trace         map[string]spanEntry
	ttl                time.Duration
	stop               chan struct{}
	genericOtelEnabled bool
}

func NewTranslator(genericOtelEnabled bool, ttl time.Duration) *Translator {
	t := &Translator{
		converter:          &GenAiConverter{},
		span2trace:         make(map[string]spanEntry),
		ttl:                ttl,
		stop:               make(chan struct{}),
		genericOtelEnabled: genericOtelEnabled,
	}
	if t.ttl > 0 {
		go t.clean()
	}
	return t
}

func (t *Translator) clean() {
	ticker := time.NewTicker(t.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cutoff := time.Now().Add(-t.ttl)
			t.mu.Lock()
			for k, v := range t.span2trace {
				if v.ts.Before(cutoff) {
					delete(t.span2trace, k)
				}
			}
			t.mu.Unlock()
		case <-t.stop:
			return
		}
	}
}

func (t *Translator) Close() { close(t.stop) }

// Translate converts every OTLP span in the request to a Run slice.
// Creating a new one spins up a new goroutine to clean up stale aliases.
func (t *Translator) Translate(req *collectortracepb.ExportTraceServiceRequest) []*model.Run {
	total := 0
	// count total spans
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			total += len(ss.Spans)
		}
	}

	runs := make([]*model.Run, 0, total)
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				// check if the span is a langsmith root span
				isLangSmithRoot := getBoolAttribute(span.Attributes, LangSmithRoot)

				if len(span.ParentSpanId) == 0 || isLangSmithRoot {
					if spanUUID, err := idToUUID(span.SpanId); err == nil {
						if traceUUID, err := idToUUID(span.TraceId); err == nil {
							t.mu.Lock()
							t.span2trace[spanUUID.String()] = spanEntry{trace: traceUUID.String(), ts: time.Now()}
							t.mu.Unlock()
						}
					}
				}
				run, err := t.converter.ConvertSpan(span, t.genericOtelEnabled)
				if err != nil || run == nil {
					continue
				}
				if len(span.ParentSpanId) == 0 || isLangSmithRoot {
					if spanUUID, err := idToUUID(span.SpanId); err == nil {
						run.RootSpanID = util.StringPtr(spanUUID.String())
						// Only set run.ID to trace ID if it's still the original span ID
						// (meaning it wasn't overridden by LangSmith attributes)
						if run.ID != nil && *run.ID == spanUUID.String() {
							if traceUUID, err := idToUUID(span.TraceId); err == nil {
								traceStr := traceUUID.String()
								run.ID = &traceStr
							}
						}
					}
				} else if run.ParentRunID != nil {
					t.mu.RLock()
					if m, ok := t.span2trace[*run.ParentRunID]; ok {
						run.ParentRunID = util.StringPtr(m.trace)
					}
					t.mu.RUnlock()
				}
				runs = append(runs, run)
			}
		}
	}
	return runs
}

func getBoolAttribute(attrs []*commonpb.KeyValue, key string) bool {
	for _, attr := range attrs {
		if attr.Key == key && attr.Value != nil {
			if v, ok := attr.Value.Value.(*commonpb.AnyValue_BoolValue); ok {
				return v.BoolValue
			}
		}
	}
	return false
}
