package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// HTTP Server Config
	Port         string
	MaxBodyBytes int64
	// gRPC Server Config
	GRPCPort           string
	GRPCMaxRecvMsgSize int
	GRPCMaxSendMsgSize int
	GRPCEnabled        bool
	// LangSmith Config
	LangsmithHost      string
	DefaultAPIKey      string
	DefaultProject     string
	BatchSize          int
	FlushInterval      time.Duration
	MaxBufferBytes     int
	MaxRetries         int
	BackoffInitial     time.Duration
	FilterNonGenAI     bool
	GenericOtelEnabled bool
	// TTL Configuration
	SpanTTL time.Duration
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		return strings.ToLower(v) == "true"
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func Load() Config {
	return Config{
		// HTTP Server Config
		Port:         env("HTTP_PORT", "4318"),
		MaxBodyBytes: envInt64("MAX_BODY_BYTES", 209715200), // 200 MB
		// gRPC Server Config
		GRPCPort:           env("GRPC_PORT", "4317"),                      // Standard OTLP gRPC port
		GRPCMaxRecvMsgSize: envInt("GRPC_MAX_RECV_MSG_SIZE", 4*1024*1024), // 4MB
		GRPCMaxSendMsgSize: envInt("GRPC_MAX_SEND_MSG_SIZE", 4*1024*1024), // 4MB
		GRPCEnabled:        envBool("GRPC_ENABLED", true),
		// LangSmith Config
		LangsmithHost:  env("LANGSMITH_ENDPOINT", "https://api.smith.langchain.com"),
		DefaultAPIKey:  env("LANGSMITH_API_KEY", ""),
		DefaultProject: env("LANGSMITH_PROJECT", ""),
		// Collector Config. The following values control how frequently and how many runs are sent to LangSmith.
		// The collector will buffer zstd compressed runs in memory until one of the following conditions is met:
		// 1. The buffer size exceeds MaxBufferBytes.
		// 2. The flush interval is reached.
		// 3. The batch size is reached.
		// Because zstd compression can be extremely effective, we use the uncompressed size of the buffer to determine how much data
		// will be sent to LangSmith.
		BatchSize:      int(envInt64("BATCH_SIZE", 100)),                                      // default 100 runs
		FlushInterval:  time.Duration(envInt64("FLUSH_INTERVAL_MS", 1000)) * time.Millisecond, // default 1 second
		MaxBufferBytes: int(envInt64("MAX_BUFFER_BYTES", 10*1024*1024)),                       // default 10MB
		// Uploader Config
		MaxRetries:     int(envInt64("MAX_RETRIES", 3)),
		BackoffInitial: time.Duration(envInt64("RETRY_BACKOFF_MS", 100)) * time.Millisecond,
		// Filter Config
		FilterNonGenAI: envBool("FILTER_NON_GENAI", false),
		// Generic OTEL Config
		GenericOtelEnabled: envBool("GENERIC_OTEL_ENABLED", false),
		// TTL Config
		SpanTTL: envDuration("SPAN_TTL", 5*time.Minute),
	}
}
