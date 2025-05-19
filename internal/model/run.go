package model

const (
	RunTypeTool      = "tool"
	RunTypeChain     = "chain"
	RunTypeLLM       = "llm"
	RunTypeRetriever = "retriever"
	RunTypeEmbedding = "embedding"
	RunTypePrompt    = "prompt"
	RunTypeParser    = "parser"
)

type Run struct {
	ID          *string                `json:"id"`
	TraceID     *string                `json:"trace_id,omitempty"`
	Name        *string                `json:"name,omitempty"`
	RunType     *string                `json:"run_type,omitempty" validate:"oneof=tool chain llm retriever embedding prompt parser"`
	StartTime   *string                `json:"start_time,omitempty"`
	EndTime     *string                `json:"end_time,omitempty"`
	SessionID   *string                `json:"session_id,omitempty"`
	SessionName *string                `json:"session_name,omitempty"`
	DottedOrder *string                `json:"dotted_order,omitempty"`
	Status      *string                `json:"status,omitempty"`
	Inputs      map[string]interface{} `json:"inputs,omitempty"`
	Outputs     map[string]interface{} `json:"outputs,omitempty"`
	Extra       map[string]interface{} `json:"extra,omitempty"`
	Error       *string                `json:"error,omitempty"`
	ParentRunID *string                `json:"parent_run_id,omitempty"`
	Tags        []string               `json:"tags"`
	// Internal fields. Not serialized into JSON / sent to LangSmith
	RootSpanID *string `json:"-"`
}
