package grpc

import (
	"context"
	"log/slog"
	"net"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/config"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/contextkey"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/grpc/handler"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/translator"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
)

// Server wraps the gRPC server with configuration and lifecycle management
type Server struct {
	server   *grpc.Server
	listener net.Listener
	cfg      config.Config
}

// NewServer creates a new gRPC server with all necessary interceptors and services
func NewServer(cfg config.Config, ch chan *model.Run) (*Server, error) {
	// Create translator for converting OTLP to LangSmith format
	tr := translator.NewTranslator(cfg.GenericOtelEnabled, cfg.SpanTTL)

	// Create authentication function
	authFunc := createAuthFunc(cfg)

	// Create logging interceptor
	loggerOpts := []grpc_logging.Option{
		grpc_logging.WithLogOnEvents(grpc_logging.StartCall, grpc_logging.FinishCall),
	}

	// Create gRPC server with interceptors
	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpc_recovery.UnaryServerInterceptor(),
			grpc_logging.UnaryServerInterceptor(InterceptorLogger(slog.Default()), loggerOpts...),
			grpc_auth.UnaryServerInterceptor(authFunc),
		),
		grpc.MaxRecvMsgSize(cfg.GRPCMaxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.GRPCMaxSendMsgSize),
	)

	// Register trace service
	traceHandler := handler.NewTraceServiceHandler(tr, ch)
	collectortracepb.RegisterTraceServiceServer(s, traceHandler)

	// Register health service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("otlp.collector.trace.v1.TraceService", grpc_health_v1.HealthCheckResponse_SERVING)

	// Enable reflection for debugging
	reflection.Register(s)

	// Create listener
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		return nil, err
	}

	return &Server{
		server:   s,
		listener: lis,
		cfg:      cfg,
	}, nil
}

// Start begins serving gRPC requests
func (s *Server) Start() error {
	slog.Info("Starting gRPC server", "port", s.cfg.GRPCPort)
	return s.server.Serve(s.listener)
}

// Stop gracefully shuts down the gRPC server
func (s *Server) Stop() {
	slog.Info("Stopping gRPC server")
	s.server.GracefulStop()
}

// Addr returns the server's listening address
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// createAuthFunc creates an authentication function for gRPC interceptor
func createAuthFunc(cfg config.Config) grpc_auth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		// Extract metadata from gRPC context
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		// Get API key from metadata
		var apiKey string
		if keys := md.Get("x-api-key"); len(keys) > 0 {
			apiKey = keys[0]
		}
		if apiKey == "" {
			apiKey = cfg.DefaultAPIKey
		}
		if apiKey == "" {
			slog.Warn("missing X-API-Key in gRPC metadata")
			return nil, status.Error(codes.Unauthenticated, "missing X-API-Key")
		}

		// Get project from metadata
		project := cfg.DefaultProject
		if projects := md.Get("langsmith-project"); len(projects) > 0 {
			project = projects[0]
		}

		// Add to context
		ctx = context.WithValue(ctx, contextkey.APIKeyKey, apiKey)
		ctx = context.WithValue(ctx, contextkey.ProjectIDKey, project)

		return ctx, nil
	}
}

// InterceptorLogger adapts slog logger to interceptor logger interface
func InterceptorLogger(l *slog.Logger) grpc_logging.Logger {
	return grpc_logging.LoggerFunc(func(ctx context.Context, lvl grpc_logging.Level, msg string, fields ...any) {
		l.Log(ctx, slog.Level(lvl), msg, fields...)
	})
}
