package handler

import (
	"context"
	"log/slog"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/contextkey"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/translator"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TraceServiceHandler implements the OTLP TraceService gRPC interface
type TraceServiceHandler struct {
	collectortracepb.UnimplementedTraceServiceServer
	translator *translator.Translator
	channel    chan *model.Run
}

// NewTraceServiceHandler creates a new trace service handler
func NewTraceServiceHandler(tr *translator.Translator, ch chan *model.Run) *TraceServiceHandler {
	return &TraceServiceHandler{
		translator: tr,
		channel:    ch,
	}
}

// Export handles the ExportTrace gRPC call
func (h *TraceServiceHandler) Export(ctx context.Context, req *collectortracepb.ExportTraceServiceRequest) (*collectortracepb.ExportTraceServiceResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	// Log the request for debugging
	slog.Debug("Received gRPC trace export request",
		"resource_spans_count", len(req.ResourceSpans),
	)

	// Translate OTLP traces to LangSmith runs using the same translator as HTTP
	runs := h.translator.Translate(req)

	// Extract project from context (set by auth middleware)
	project := ""
	if projectValue := ctx.Value(contextkey.ProjectIDKey); projectValue != nil {
		if projectStr, ok := projectValue.(string); ok {
			project = projectStr
		}
	}

	// Send runs to the aggregator channel
	for _, run := range runs {
		if project != "" {
			run.SessionName = &project
		}
		
		// Non-blocking send to avoid hanging the gRPC call
		select {
		case h.channel <- run:
			// Successfully sent
		default:
			// Channel is full, log warning but don't fail the request
			slog.Warn("Channel full, dropping run", "run_id", run.ID)
		}
	}

	slog.Debug("Successfully processed gRPC trace export",
		"runs_count", len(runs),
		"project", project,
	)

	// Return success response
	return &collectortracepb.ExportTraceServiceResponse{
		PartialSuccess: &collectortracepb.ExportTracePartialSuccess{
			RejectedSpans: 0,
			ErrorMessage:  "",
		},
	}, nil
}
