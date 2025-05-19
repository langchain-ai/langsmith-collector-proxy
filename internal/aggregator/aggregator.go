package aggregator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/serializer"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/uploader"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/util"
)

type Config struct {
	BatchSize      int
	FlushInterval  time.Duration
	MaxBufferBytes int
	GCInterval     time.Duration
	EntryTTL       time.Duration
}

// Aggregator resolves dotted_order for each run and adds them to a compressed
// buffer.
//
// We cannot directly upload runs to the uploader as LangSmith requires
// dotted_order to be set on each run. Dotted_order is a representation of the
// run's position in the trace, and is required for bulk ingestion.
//
// See https://docs.smith.langchain.com/reference/data_formats/run_data_format#what-is-dotted_order
//
// Aggregator will flush runs to the uploader at a regular interval and garbage
// collect runs that have not been updated for a given TTL.
type Aggregator struct {
	ch     chan *model.Run
	cfg    Config
	up     *uploader.Uploader
	cancel context.CancelFunc
}

func New(up *uploader.Uploader, cfg Config, ch chan *model.Run) *Aggregator {
	if cfg.GCInterval == 0 {
		cfg.GCInterval = 2 * time.Minute
	}
	if cfg.EntryTTL == 0 {
		cfg.EntryTTL = 5 * time.Minute
	}
	if cfg.MaxBufferBytes == 0 {
		cfg.MaxBufferBytes = 10 * 1024 * 1024 // 10MB
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 1 * time.Second
	}
	return &Aggregator{up: up, cfg: cfg, ch: ch}
}

func (a *Aggregator) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel

	go a.worker(ctx, a.ch)
}

func (a *Aggregator) Stop() {
	if a.cancel != nil {
		a.cancel()
	}
}

func (a *Aggregator) worker(ctx context.Context, ch <-chan *model.Run) {
	sc := serializer.NewStreamingCompressor()
	var scMu sync.Mutex

	type entry struct {
		dotted string
		ts     time.Time
	}

	dottedByRunID := make(map[string]entry)          // runID → dotted_order
	waitingChildren := make(map[string][]*model.Run) // parentID → parked kids
	waitingSince := make(map[string]time.Time)       // parentID → first‑park time

	flush := func() {
		scMu.Lock()
		defer scMu.Unlock()
		if sc == nil || sc.RunCount() == 0 {
			return
		}
		comp, boundary, _, err := sc.Close()
		if err != nil {
			slog.Error("Failed to flush runs", "err", err)
			return
		}
		if len(comp) > 0 {
			go a.up.Send(context.Background(), uploader.Batch{Data: comp, Boundary: boundary})
		}
	}

	add := func(r *model.Run) {
		needFlush := false

		scMu.Lock()
		if err := sc.AddRun(r); err != nil {
			slog.Error("failed to queue run", "err", err)
			scMu.Unlock()
			return
		}
		needFlush = sc.RunCount() >= a.cfg.BatchSize ||
			sc.Uncompressed() >= a.cfg.MaxBufferBytes
		scMu.Unlock()

		if needFlush {
			flush()
		}
	}

	flushTicker := time.NewTicker(a.cfg.FlushInterval)
	defer flushTicker.Stop()

	var cascade func(parentID, parentDotted string)
	cascade = func(parentID, parentDotted string) {
		kids := waitingChildren[parentID]
		if len(kids) == 0 {
			return
		}
		delete(waitingChildren, parentID)
		delete(waitingSince, parentID)

		for _, child := range kids {
			d := parentDotted + "." + util.NewDottedOrder(*child.ID)
			child.DottedOrder = &d
			dottedByRunID[*child.ID] = entry{dotted: d, ts: time.Now()}
			add(child)
			cascade(*child.ID, d) // grandchildren
		}
	}

	gc := func() {
		if a.cfg.EntryTTL <= 0 {
			return
		}
		cutoff := time.Now().Add(-a.cfg.EntryTTL)

		for id, e := range dottedByRunID {
			if e.ts.Before(cutoff) {
				delete(dottedByRunID, id)
			}
		}

		for parentID, kids := range waitingChildren {
			first := waitingSince[parentID]
			if first.IsZero() || first.After(cutoff) {
				continue
			}
			for _, child := range kids {
				// If we are unable to assign a dotted_order to a run, set the parent_run_id
				// to the trace_id. This will cause the run to be uploaded as one level
				// below the root run.
				child.ParentRunID = child.TraceID
				fallbackPrefix := util.NewDottedOrder(*child.TraceID)
				d := fallbackPrefix + "." + util.NewDottedOrder(*child.ID)
				child.DottedOrder = &d
				add(child)
			}
			delete(waitingChildren, parentID)
			delete(waitingSince, parentID)
		}
		flush()
	}

	gcTicker := time.NewTicker(a.cfg.GCInterval)
	defer gcTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			flush()
			return

		case r := <-ch:
			if r == nil {
				continue
			}
			switch {
			case r.ParentRunID == nil || *r.ParentRunID == "":
				if r.RootSpanID != nil {
					alias := *r.RootSpanID
					if kids, ok := waitingChildren[alias]; ok {
						for _, child := range kids {
							child.ParentRunID = r.ID // trace-uuid
						}
						waitingChildren[*r.ID] = append(waitingChildren[*r.ID], kids...)
						delete(waitingChildren, alias)
						if first, ok := waitingSince[alias]; ok {
							waitingSince[*r.ID] = first
							delete(waitingSince, alias)
						}
					}
				}
				d := util.NewDottedOrder(*r.ID)
				r.DottedOrder = &d
				dottedByRunID[*r.ID] = entry{dotted: d, ts: time.Now()}
				add(r)
				cascade(*r.ID, d)

			default:
				parentID := *r.ParentRunID
				if p, ok := dottedByRunID[parentID]; ok {
					d := p.dotted + "." + util.NewDottedOrder(*r.ID)
					r.DottedOrder = &d
					dottedByRunID[*r.ID] = entry{dotted: d, ts: time.Now()}
					add(r)
					cascade(*r.ID, d)
				} else {
					waitingChildren[parentID] = append(waitingChildren[parentID], r)
					if _, ok := waitingSince[parentID]; !ok {
						waitingSince[parentID] = time.Now()
					}
				}
			}

		case <-flushTicker.C:
			flush()

		case <-gcTicker.C:
			gc()
		}
	}
}
