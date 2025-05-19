package serializer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strconv"

	"github.com/DataDog/zstd"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
)

// StreamingCompressor uses zstd + multipart/form-data to format and compress traces.
//
// Treat StreamingCompressor as a long-lived object for a multiple uploads. Call Close()
// when done with a single upload to get the final bytes and cleanup resources. You should
// reuse the StreamingCompressor object for multiple uploads.
//
// When interacting with the underlying buffer, the StreamingCompressor is not thread-safe.
// It is the responsibility of the caller to ensure thread-safety by using a mutex.
type StreamingCompressor struct {
	boundary string
	w        io.WriteCloser
	buf      *bytes.Buffer

	uncompressed int
	runCount     int
}

func NewStreamingCompressor() *StreamingCompressor {
	buf := &bytes.Buffer{}
	zw := zstd.NewWriter(buf)
	return &StreamingCompressor{
		boundary: "----LangSmithFormBoundary-" + strconv.FormatUint(rand.Uint64(), 36),
		w:        zw,
		buf:      buf,
	}
}

func (sc *StreamingCompressor) AddRun(r *model.Run) error {
	// emitPart is a helper function to format and compress a run as multipart/form-data.
	emitPart := func(name string, v any) error {
		j, err := json.Marshal(v)
		if err != nil {
			return err
		}
		sc.uncompressed += len(j)
		header := fmt.Sprintf("--%s\r\nContent-Disposition: form-data; name=\"%s\"\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n",
			sc.boundary, name, len(j))
		if _, err := sc.w.Write([]byte(header)); err != nil {
			return err
		}
		if _, err := sc.w.Write(j); err != nil {
			return err
		}
		_, err = sc.w.Write([]byte("\r\n"))
		return err
	}
	id := *r.ID
	c := *r
	c.Inputs, c.Outputs = nil, nil
	if err := emitPart("post."+id, &c); err != nil {
		return err
	}
	if r.Inputs != nil {
		if err := emitPart("post."+id+".inputs", r.Inputs); err != nil {
			return err
		}
	}
	if r.Outputs != nil {
		if err := emitPart("post."+id+".outputs", r.Outputs); err != nil {
			return err
		}
	}
	sc.runCount++
	return nil
}

func (sc *StreamingCompressor) Close() ([]byte, string, int, error) {
	// Write the final multipart boundary and reset state.
	hasData := sc.runCount > 0 || sc.uncompressed > 0
	var (
		outBytes     []byte
		boundary     string
		uncompressed int
		err          error
	)

	if hasData {
		if _, err = sc.w.Write([]byte(fmt.Sprintf("--%s--\r\n", sc.boundary))); err == nil {
			err = sc.w.Close()
		}
		if err == nil {
			outBytes = sc.buf.Bytes()
			boundary = sc.boundary
			uncompressed = sc.uncompressed
		}
	} else {
		slog.Error("Failed to write final multipart boundary", "err", err)
	}
	sc.buf = &bytes.Buffer{}
	sc.boundary = "----LangSmithFormBoundary-" + strconv.FormatUint(rand.Uint64(), 36)
	sc.w = zstd.NewWriter(sc.buf)
	sc.runCount = 0
	return outBytes, boundary, uncompressed, err
}

func (sc *StreamingCompressor) Uncompressed() int {
	return sc.uncompressed
}

func (sc *StreamingCompressor) RunCount() int {
	return sc.runCount
}
