package aggregator

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/serializer"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/uploader"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/util"
)

type entry struct {
	dotted string
	ts     time.Time
}

type RunFilter func(run *model.Run) bool

type FilterConfig struct {
	// FilterNonGenAI determines if non-GenAI runs should be filtered out
	FilterNonGenAI bool
	// CustomFilter is an optional custom filter function
	CustomFilter RunFilter
}

var IncludePrefixes = []string{"gen_ai.", "langsmith.", "llm.", "ai."}

func DefaultGenAIFilter(run *model.Run) bool {
	if run.Name != nil {
		for _, prefix := range IncludePrefixes {
			if strings.HasPrefix(*run.Name, prefix) {
				return true
			}
		}
	}

	if run.Extra != nil {
		for key := range run.Extra {
			for _, prefix := range IncludePrefixes {
				if strings.HasPrefix(key, prefix) {
					return true
				}
			}
		}
	}

	return false
}

func (fc FilterConfig) ShouldKeepRun(run *model.Run) bool {
	if fc.CustomFilter != nil {
		return fc.CustomFilter(run)
	}

	if fc.FilterNonGenAI {
		return DefaultGenAIFilter(run)
	}

	return true
}

type Config struct {
	BatchSize      int
	FlushInterval  time.Duration
	MaxBufferBytes int
	GCInterval     time.Duration
	EntryTTL       time.Duration
	FilterConfig   FilterConfig
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
	ch          chan *model.Run
	cfg         Config
	up          uploader.UploaderInterface
	cancel      context.CancelFunc
	filteredIDs sync.Map
	flushCh     chan struct{}
}

func New(up uploader.UploaderInterface, cfg Config, ch chan *model.Run) *Aggregator {
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
	return &Aggregator{up: up, cfg: cfg, ch: ch, flushCh: make(chan struct{}, 1)}
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

// force a flush of all pending runs
func (a *Aggregator) Flush(ctx context.Context) error {
	select {
	case a.flushCh <- struct{}{}:
	default:
		// Channel is full, flush already pending
	}

	return a.up.WaitForCompletion(ctx)
}

func (a *Aggregator) worker(ctx context.Context, ch <-chan *model.Run) {
	sc := serializer.NewStreamingCompressor()
	var scMu sync.Mutex

	dottedByRunID := make(map[string]entry)          // runID → dotted_order
	waitingChildren := make(map[string][]*model.Run) // parentID → parked kids
	waitingSince := make(map[string]time.Time)       // parentID → first‑park time
	parentByRunID := make(map[string]string)         // runID → parentID (for filtered runs)

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

			// Track parent relationship before filtering
			if r.ID != nil && r.ParentRunID != nil {
				parentByRunID[*r.ID] = *r.ParentRunID
			}

			// Apply filtering
			shouldKeep := a.cfg.FilterConfig.ShouldKeepRun(r)
			if !shouldKeep {
				// Track this as a filtered run
				if r.ID != nil {
					a.filteredIDs.Store(*r.ID, true)
				}

				// Reparent any waiting children of this filtered run
				if r.ID != nil {
					a.reparentChildrenOfFiltered(*r.ID, waitingChildren, waitingSince, dottedByRunID, add, cascade, parentByRunID)
				}
				continue
			}

			// Check if this run's parent was filtered and reparent if needed
			if r.ParentRunID != nil {
				newParentID := a.findValidParent(*r.ParentRunID, parentByRunID)
				if newParentID != *r.ParentRunID {
					if newParentID == "" {
						r.ParentRunID = r.TraceID
					} else {
						r.ParentRunID = &newParentID
					}
				}
			}

			// If dotted order is already set, pass through directly
			if r.DottedOrder != nil && *r.DottedOrder != "" {
				// Still track this run for potential children
				if r.ID != nil {
					dottedByRunID[*r.ID] = entry{dotted: *r.DottedOrder, ts: time.Now()}
				}
				add(r)
				// Check if this run can resolve any waiting children
				if r.ID != nil {
					cascade(*r.ID, *r.DottedOrder)
				}
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

		case <-a.flushCh:
			flush()

		case <-gcTicker.C:
			gc()
		}
	}
}

func (a *Aggregator) reparentChildrenOfFiltered(
	filteredRunID string,
	waitingChildren map[string][]*model.Run,
	waitingSince map[string]time.Time,
	dottedByRunID map[string]entry,
	add func(*model.Run),
	cascade func(string, string),
	parentByRunID map[string]string,
) {
	children := waitingChildren[filteredRunID]
	if len(children) == 0 {
		return
	}

	// Remove children from the filtered parent
	delete(waitingChildren, filteredRunID)
	delete(waitingSince, filteredRunID)

	// Reparent each child
	for _, child := range children {
		if child.ParentRunID != nil {
			// Find valid grandparent for this child
			newParentID := a.findValidParent(*child.ParentRunID, parentByRunID)
			if newParentID == "" {
				child.ParentRunID = child.TraceID
			} else {
				child.ParentRunID = &newParentID
			}
		}

		// Try to place the child in its new location
		if child.ParentRunID == nil || *child.ParentRunID == "" {
			// Child becomes a root run
			d := util.NewDottedOrder(*child.ID)
			child.DottedOrder = &d
			dottedByRunID[*child.ID] = entry{dotted: d, ts: time.Now()}
			add(child)
			cascade(*child.ID, d)
		} else {
			parentID := *child.ParentRunID
			if p, ok := dottedByRunID[parentID]; ok {
				// New parent exists, assign dotted order and add
				d := p.dotted + "." + util.NewDottedOrder(*child.ID)
				child.DottedOrder = &d
				dottedByRunID[*child.ID] = entry{dotted: d, ts: time.Now()}
				add(child)
				cascade(*child.ID, d)
			} else {
				// New parent doesn't exist yet, wait for it
				waitingChildren[parentID] = append(waitingChildren[parentID], child)
				if _, ok := waitingSince[parentID]; !ok {
					waitingSince[parentID] = time.Now()
				}
			}
		}
	}
}

// findValidParent walks up the ancestry to find the first non-filtered parent
func (a *Aggregator) findValidParent(parentID string, parentByRunID map[string]string) string {
	currentID := parentID

	for currentID != "" {
		// If this parent is filtered, continue up the tree
		if _, isFiltered := a.filteredIDs.Load(currentID); isFiltered {
			// Look for this filtered parent's parent in parentByRunID
			if grandParentID, exists := parentByRunID[currentID]; exists {
				currentID = grandParentID
				continue
			} else {
				// Can't find parent of filtered run, return empty (becomes root)
				return ""
			}
		} else {
			// Found a non-filtered parent
			return currentID
		}
	}
	// No valid parent found
	return ""
}
