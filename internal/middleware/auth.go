package middleware

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/config"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/contextkey"
)

func Auth(cfg config.Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiKey := r.Header.Get("X-API-Key")
			if apiKey == "" {
				apiKey = cfg.DefaultAPIKey
			}
			if apiKey == "" {
				slog.Warn("missing X-API-Key", "path", r.URL.Path)
				http.Error(w, "missing X-API-Key", http.StatusUnauthorized)
				return
			}

			project := r.Header.Get("Langsmith-Project")
			if project == "" {
				project = cfg.DefaultProject
			}

			ctx := context.WithValue(r.Context(), contextkey.APIKeyKey, apiKey)
			ctx = context.WithValue(ctx, contextkey.ProjectIDKey, project)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
