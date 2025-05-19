package handler

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/langchain-ai/langsmith-collector-proxy/internal/contextkey"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/translator"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	errUnsupported         = errors.New("unsupported Content-Type for OTLP")
	errUnsupportedEncoding = errors.New("unsupported Content-Encoding for OTLP")
)

func handleError(w http.ResponseWriter, r *http.Request, status int, err error) {
	msg := err.Error()

	slog.Default().Warn("request error",
		"status", status,
		"method", r.Method,
		"path", r.URL.Path,
		"err", msg,
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
	})
}

// TracesHandler ingests OTLP/HTTP requests.
func TracesHandler(maxBody int64, tr *translator.Translator, ch chan *model.Run) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		defer r.Body.Close()
		var reader io.Reader = r.Body
		switch strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))) {
		case "", "identity":
		case "gzip": // Otel collectors send gzip encoded traces by default.
			gzr, err := gzip.NewReader(reader)
			if err != nil {
				handleError(w, r, http.StatusBadRequest, err)
				return
			}
			defer gzr.Close()
			reader = gzr
		default:
			handleError(w, r, http.StatusUnsupportedMediaType, errUnsupportedEncoding)
			return
		}

		ct := r.Header.Get("Content-Type")
		var req collectortracepb.ExportTraceServiceRequest

		switch {
		case strings.Contains(ct, "json"):
			if err := decodeJSON(reader, &req); err != nil {
				handleError(w, r, http.StatusBadRequest, err)
				return
			}
		case strings.Contains(ct, "protobuf"), strings.HasSuffix(ct, "+proto"):
			if err := decodeProto(reader, &req); err != nil {
				handleError(w, r, http.StatusBadRequest, err)
				return
			}
		default:
			handleError(w, r, http.StatusUnsupportedMediaType, errUnsupported)
			return
		}

		runs := tr.Translate(&req)

		project := r.Context().Value(contextkey.ProjectIDKey).(string)
		for _, run := range runs {
			if project != "" {
				run.SessionName = &project
			}
			ch <- run
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func decodeJSON(r io.Reader, dest proto.Message) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return protojson.Unmarshal(raw, dest)
}

func decodeProto(r io.Reader, dest proto.Message) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return proto.Unmarshal(raw, dest)
}
