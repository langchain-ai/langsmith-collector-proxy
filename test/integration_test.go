package test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/config"
	grpcserver "github.com/langchain-ai/langsmith-collector-proxy/internal/grpc"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/server"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestDualProtocolIntegration(t *testing.T) {
	// Create test configuration
	cfg := config.Config{
		Port:               "0", // Use any available port for HTTP
		GRPCPort:           "0", // Use any available port for gRPC
		GRPCMaxRecvMsgSize: 4 * 1024 * 1024,
		GRPCMaxSendMsgSize: 4 * 1024 * 1024,
		GRPCEnabled:        true,
		DefaultAPIKey:      "test-api-key",
		DefaultProject:     "test-project",
		GenericOtelEnabled: false,
		SpanTTL:            5 * time.Minute,
		MaxBodyBytes:       200 * 1024 * 1024,
	}

	// Create channel for runs
	ch := make(chan *model.Run, 100)

	// Start HTTP server
	httpRouter := server.NewRouter(cfg, ch)
	httpServer := &http.Server{
		Addr:    ":0",
		Handler: httpRouter,
	}

	httpListener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to create HTTP listener: %v", err)
	}

	go func() {
		if err := httpServer.Serve(httpListener); err != nil && err != http.ErrServerClosed {
			t.Logf("HTTP server error: %v", err)
		}
	}()

	// Start gRPC server
	grpcSrv, err := grpcserver.NewServer(cfg, ch)
	if err != nil {
		t.Fatalf("Failed to create gRPC server: %v", err)
	}

	go func() {
		if err := grpcSrv.Start(); err != nil {
			t.Logf("gRPC server error: %v", err)
		}
	}()

	// Give servers time to start
	time.Sleep(200 * time.Millisecond)

	// Test HTTP endpoint
	t.Run("HTTP_JSON", func(t *testing.T) {
		testHTTPEndpoint(t, httpListener.Addr().String(), ch)
	})

	// Test gRPC endpoint
	t.Run("gRPC", func(t *testing.T) {
		testGRPCEndpoint(t, grpcSrv.Addr().String(), ch)
	})

	// Cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpServer.Shutdown(ctx)
	grpcSrv.Stop()
}

func testHTTPEndpoint(t *testing.T, httpAddr string, ch chan *model.Run) {
	// Create test OTLP request
	otlpReq := map[string]interface{}{
		"resourceSpans": []map[string]interface{}{
			{
				"resource": map[string]interface{}{
					"attributes": []map[string]interface{}{
						{
							"key":   "service.name",
							"value": map[string]interface{}{"stringValue": "http-test-service"},
						},
					},
				},
				"scopeSpans": []map[string]interface{}{
					{
						"spans": []map[string]interface{}{
							{
								"traceId": "dGVzdC10cmFjZS1pZC0xMjM=", // base64 encoded
								"spanId":  "dGVzdC1zcGFu",             // base64 encoded
								"name":    "http-test-span",
							},
						},
					},
				},
			},
		},
	}

	jsonData, err := json.Marshal(otlpReq)
	if err != nil {
		t.Fatalf("Failed to marshal JSON: %v", err)
	}

	// Send HTTP request
	url := "http://" + httpAddr + "/v1/traces"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "test-api-key")
	req.Header.Set("Langsmith-Project", "http-test-project")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("Expected status 202, got %d", resp.StatusCode)
	}

	// Verify run was sent to channel
	select {
	case run := <-ch:
		if run == nil {
			t.Fatal("Received nil run")
		}
		if run.SessionName == nil || *run.SessionName != "http-test-project" {
			t.Errorf("Expected session name 'http-test-project', got %v", run.SessionName)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for run in channel")
	}
}

func testGRPCEndpoint(t *testing.T, grpcAddr string, ch chan *model.Run) {
	// Create gRPC client connection
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
									StringValue: "grpc-test-service",
								},
							},
						},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{
								TraceId: []byte("grpc-trace-id-123"),
								SpanId:  []byte("grpc-span"),
								Name:    "grpc-test-span",
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
		"langsmith-project", "grpc-test-project",
	))

	// Send request
	resp, err := client.Export(ctx, req)
	if err != nil {
		t.Fatalf("Failed to export traces via gRPC: %v", err)
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
		if run.SessionName == nil || *run.SessionName != "grpc-test-project" {
			t.Errorf("Expected session name 'grpc-test-project', got %v", run.SessionName)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for run in channel")
	}
}
