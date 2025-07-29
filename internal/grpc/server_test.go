package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/config"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestGRPCServer(t *testing.T) {
	// Create test configuration
	cfg := config.Config{
		GRPCPort:           "0", // Use any available port
		GRPCMaxRecvMsgSize: 4 * 1024 * 1024,
		GRPCMaxSendMsgSize: 4 * 1024 * 1024,
		GRPCEnabled:        true,
		DefaultAPIKey:      "test-api-key",
		DefaultProject:     "test-project",
		GenericOtelEnabled: false,
		SpanTTL:            5 * time.Minute,
	}

	// Create channel for runs
	ch := make(chan *model.Run, 10)

	// Create gRPC server
	server, err := NewServer(cfg, ch)
	if err != nil {
		t.Fatalf("Failed to create gRPC server: %v", err)
	}

	// Start server in background
	go func() {
		if err := server.Start(); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Create client connection
	conn, err := grpc.Dial(server.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	// Create trace service client
	client := collectortracepb.NewTraceServiceClient(conn)

	// Create test request
	req := &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "service.name",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{
									StringValue: "test-service",
								},
							},
						},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{
								TraceId: []byte("test-trace-id-123"),
								SpanId:  []byte("test-span"),
								Name:    "test-span",
							},
						},
					},
				},
			},
		},
	}

	// Create context with metadata (for auth)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"x-api-key", "test-api-key",
		"langsmith-project", "test-project",
	))

	// Send request
	resp, err := client.Export(ctx, req)
	if err != nil {
		t.Fatalf("Failed to export traces: %v", err)
	}

	// Verify response
	if resp == nil {
		t.Fatal("Response is nil")
	}

	if resp.PartialSuccess == nil {
		t.Fatal("PartialSuccess is nil")
	}

	if resp.PartialSuccess.RejectedSpans != 0 {
		t.Errorf("Expected 0 rejected spans, got %d", resp.PartialSuccess.RejectedSpans)
	}

	// Verify run was sent to channel
	select {
	case run := <-ch:
		if run == nil {
			t.Fatal("Received nil run")
		}
		if run.SessionName == nil || *run.SessionName != "test-project" {
			t.Errorf("Expected session name 'test-project', got %v", run.SessionName)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for run in channel")
	}

	// Stop server
	server.Stop()
}
