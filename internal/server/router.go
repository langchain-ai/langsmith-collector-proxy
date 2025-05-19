package server

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/config"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/handler"
	appmw "github.com/langchain-ai/langsmith-collector-proxy/internal/middleware"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/translator"
)

func NewRouter(cfg config.Config, ch chan *model.Run) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(appmw.Auth(cfg))

	tr := translator.NewTranslator()

	// Health probes
	r.Get("/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	r.Get("/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// OTLP ingest
	r.Route("/v1", func(v1 chi.Router) {
		v1.Post("/traces", handler.TracesHandler(cfg.MaxBodyBytes, tr, ch))
	})

	return r
}
