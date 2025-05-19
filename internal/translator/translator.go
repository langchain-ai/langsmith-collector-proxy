package translator

import (
	"sync"
	"time"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
)

type spanEntry struct {
	trace string
	ts    time.Time
}

type Translator struct {
	converter  *GenAiConverter
	mu         sync.RWMutex
	span2trace map[string]spanEntry
	ttl        time.Duration
}

func NewTranslator() *Translator {
	return &Translator{
		span2trace: make(map[string]spanEntry),
		ttl:        5 * time.Minute,
	}
}

// Translate converts every OTLP span in the request to a Run slice.
func (t *Translator) Translate(req *collectortracepb.ExportTraceServiceRequest) []*model.Run {
	total := 0
	// gc stale aliases first
	if t.ttl > 0 {
		cutoff := time.Now().Add(-t.ttl)
		t.mu.Lock()
		for k, v := range t.span2trace {
			if v.ts.Before(cutoff) {
				delete(t.span2trace, k)
			}
		}
		t.mu.Unlock()
	}

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
				if len(span.ParentSpanId) == 0 {
					if spanUUID, err := idToUUID(span.SpanId); err == nil {
						if traceUUID, err := idToUUID(span.TraceId); err == nil {
							t.mu.Lock()
							t.span2trace[spanUUID.String()] = spanEntry{trace: traceUUID.String(), ts: time.Now()}
							t.mu.Unlock()
						}
					}
				}
				run, err := t.converter.ConvertSpan(span, false)
				if err != nil || run == nil {
					continue
				}
				if len(span.ParentSpanId) == 0 {
					if spanUUID, err := idToUUID(span.SpanId); err == nil {
						run.RootSpanID = strPointer(spanUUID.String())
					}
					if traceUUID, err := idToUUID(span.TraceId); err == nil {
						traceStr := traceUUID.String()
						run.ID = &traceStr
					}
				} else if run.ParentRunID != nil {
					t.mu.RLock()
					if m, ok := t.span2trace[*run.ParentRunID]; ok {
						run.ParentRunID = strPointer(m.trace)
					}
					t.mu.RUnlock()
				}
				runs = append(runs, run)
			}
		}
	}
	return runs
}
