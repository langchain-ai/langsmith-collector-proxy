package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port             string
	MaxBodyBytes     int64
	LangsmithHost    string
	DefaultAPIKey    string
	DefaultProject   string
	BatchSize        int
	FlushInterval    time.Duration
	MaxBufferBytes   int
	MaxRetries       int
	BackoffInitial   time.Duration
	FilterNonGenAI   bool
	GenericOtelEnabled bool
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

func Load() Config {
	return Config{
		// HTTP Server Config
		Port:         env("HTTP_PORT", "4318"),
		MaxBodyBytes: envInt64("MAX_BODY_BYTES", 209715200), // 200 MB
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
	}
}
