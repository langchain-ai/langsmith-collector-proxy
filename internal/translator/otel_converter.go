package translator

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracesdkpb "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	// See https://www.traceloop.com/docs/openllmetry/contributing/semantic-conventions
	TraceLoopEntityInput           = "traceloop.entity.input"
	TraceLoopEntityOutput          = "traceloop.entity.output"
	TraceLoopEntityName            = "traceloop.entity.name"
	TraceLoopAssociationProperties = "traceloop.association.properties"

	// see https://github.com/traceloop/openllmetry/blob/main/packages/opentelemetry-semantic-conventions-ai/opentelemetry/semconv_ai/__init__.py#L226
	TraceLoopLLMRequestType = "llm.request.type"

	// see https://github.com/traceloop/openllmetry/blob/main/packages/opentelemetry-semantic-conventions-ai/opentelemetry/semconv_ai/__init__.py#L234
	TraceLoopSpanKind           = "traceloop.span.kind"
	TraceLoopGenAISystem        = "gen_ai.system"
	TraceLoopGenAIRequestModel  = "gen_ai.request.model"
	TraceLoopGenAIResponseModel = "gen_ai.response.model"

	// see https://github.com/Arize-ai/openinference/blob/main/spec/semantic_conventions.md
	OpenInferenceSystem         = "llm.system"
	OpenInferenceModelName      = "llm.model_name"
	OpenInferenceSpanKind       = "openinference.span.kind"
	OpenInferenceOutput         = "output.value"
	OpenInferenceInput          = "input.value"
	OpenInferenceMetadataPrefix = "metadata"
	OpenInferenceToolName       = "tool.name"

	// Additional GenAI parameters see: https://opentelemetry.io/docs/specs/semconv/attributes-registry/gen-ai/
	GenAIPrompt            = "gen_ai.prompt"
	GenAICompletion        = "gen_ai.completion"
	GenAIUsageInputTokens  = "gen_ai.usage.input_tokens"
	GenAIUsageOutputTokens = "gen_ai.usage.output_tokens"
	GenAIUsageTotalTokens  = "gen_ai.usage.total_tokens"
	GenAIOperationName     = "gen_ai.operation.name"
	GenAIContentPrompt     = "gen_ai.content.prompt"
	GenAIContentCompletion = "gen_ai.content.completion"
	// Deprecated
	GenAIUsagePromptTokens     = "gen_ai.usage.prompt_tokens"
	GenAIUsageCompletionTokens = "gen_ai.usage.completion_tokens"

	// LangSmith specific overrides
	LangSmithTraceName      = "langsmith.trace.name"
	LangSmithSpanKind       = "langsmith.span.kind"
	LangSmithMetadataPrefix = "langsmith.metadata"
	LangSmithSessionID      = "langsmith.trace.session_id"
	LangSmithSessionName    = "langsmith.trace.session_name"
	LangSmithTags           = "langsmith.span.tags"

	// LangSmith, see: https://docs.smith.langchain.com/observability/how_to_guides/tracing/log_llm_trace
	LangSmithInvocationParams = "invocation_params"
	LangSmithMetadata         = "metadata"
	LangSmithLSProvider       = "ls_provider"
	LangSmithModelName        = "ls_model_name"

	OtelSpanIdKey  = "OTEL_SPAN_ID"
	OtelTraceIdKey = "OTEL_TRACE_ID"

	// Logfire semantic-convention keys
	LogfirePrompt            = "prompt"
	LogfireAllMessagesEvents = "all_messages_events"
	LogfireEvents            = "events"
)

var TraceLoopInvocationParams = []string{
	"gen_ai.request.model",
	"gen_ai.response.model",
	"gen_ai.request.temperature",
	"gen_ai.request.top_p",
	"gen_ai.request.max_tokens",
	"gen_ai.request.frequency_penalty",
	"gen_ai.request.presence_penalty",
	"gen_ai.request.seed",
	"gen_ai.request.stop_sequences",
	"gen_ai.request.top_k",
	"gen_ai.request.encoding_formats",
	"llm.presence_penalty",
	"llm.frequency_penalty",
	"llm.request.functions",
}

func strPointer(s string) *string {
	return &s
}

func idToUUID(id []byte) (uuid.UUID, error) {
	if len(id) < 8 {
		return uuid.Nil, fmt.Errorf("invalid id length: expected >= 8 bytes, got %d", len(id))
	}
	var buf [16]byte
	if len(id) > 16 {
		id = id[len(id)-16:]
	}
	copy(buf[16-len(id):], id)

	result, err := uuid.FromBytes(buf[:])
	if err != nil {
		return uuid.Nil, err
	}
	return result, nil
}

func extractProtobufValue(value interface{}) interface{} {
	switch v := value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.StringValue
	case *commonpb.AnyValue_DoubleValue:
		return v.DoubleValue
	case *commonpb.AnyValue_IntValue:
		return v.IntValue
	case *commonpb.AnyValue_BoolValue:
		return v.BoolValue
	case *commonpb.AnyValue_ArrayValue:
		return v.ArrayValue
	case *commonpb.AnyValue_KvlistValue:
		return v.KvlistValue
	case *commonpb.AnyValue_BytesValue:
		return v.BytesValue
	default:
		return nil
	}
}

type GenAiConverter struct{}

// Helper function to add prefixed attributes to metadata
func addPrefixedAttributesToMetadata(spanAttrs map[string]*commonpb.AnyValue, prefix string, metadata map[string]interface{}) {
	for key, attr := range spanAttrs {
		if strings.HasPrefix(key, prefix+".") {
			metadataKey := strings.TrimPrefix(key, prefix+".")
			if value := extractProtobufValue(attr.Value); value != nil {
				metadata[metadataKey] = value
			}
		}
	}
}

// Helper function to convert []interface{} to []float64 for embeddings
func convertToFloat64Array(input []interface{}) []float64 {
	result := make([]float64, len(input))
	for i, v := range input {
		switch num := v.(type) {
		case float64:
			result[i] = num
		case float32:
			result[i] = float64(num)
		}
	}
	return result
}

// extractModelName looks for a model‑name attribute in order of precedence and
// returns it if found.
func extractModelName(attrs map[string]*commonpb.AnyValue) (string, bool) {
	keys := []string{
		TraceLoopGenAIRequestModel,  // "gen_ai.request.model"
		TraceLoopGenAIResponseModel, // "gen_ai.response.model"
		"model",                     // some SDKs use a bare "model"
		OpenInferenceModelName,      // "llm.model_name"
	}
	for _, k := range keys {
		if v, ok := attrs[k]; ok && v != nil {
			if s, ok := v.Value.(*commonpb.AnyValue_StringValue); ok {
				return s.StringValue, true
			}
		}
	}
	return "", false
}

func (c *GenAiConverter) ConvertSpan(span *tracesdkpb.Span, genericOtelEnabled bool) (*model.Run, error) {
	// Convert timestamps from nanoseconds to time.Time
	startTime := time.Unix(0, int64(span.GetStartTimeUnixNano()))
	endTime := time.Unix(0, int64(span.GetEndTimeUnixNano()))

	// Convert trace_id and span_id to UUIDs
	spanID, err := idToUUID(span.GetSpanId())
	if err != nil {
		return nil, err
	}
	traceID, err := idToUUID(span.GetTraceId())
	if err != nil {
		return nil, err
	}

	var parentID *string
	if len(span.ParentSpanId) > 0 {
		parsed, err := idToUUID(span.ParentSpanId)
		if err != nil {
			return nil, err
		}
		parentID = strPointer(parsed.String())
	}
	// Initialize run fields
	run := &model.Run{
		ID:          strPointer(spanID.String()),
		ParentRunID: parentID,
		TraceID:     strPointer(traceID.String()),
		Name:        &span.Name,
		StartTime:   strPointer(startTime.UTC().Format(time.RFC3339Nano)),
		EndTime:     strPointer(endTime.UTC().Format(time.RFC3339Nano)),
		Extra: map[string]interface{}{
			LangSmithMetadata: map[string]interface{}{},
		},
		Inputs:  nil,
		Outputs: nil,
	}

	run.Extra[LangSmithMetadata].(map[string]interface{})[OtelTraceIdKey] = fmt.Sprintf("%x", span.TraceId)
	run.Extra[LangSmithMetadata].(map[string]interface{})[OtelSpanIdKey] = fmt.Sprintf("%x", span.SpanId)

	// Convert attributes to a map for easier access
	spanAttrs := make(map[string]*commonpb.AnyValue)
	for _, attr := range span.Attributes {
		if attr.Value != nil {
			spanAttrs[attr.Key] = attr.Value
		}
	}

	if len(span.Events) > 0 {
		inputs, outputs := extractFromEvents(span.Events)
		if inputs != nil && run.Inputs == nil {
			run.Inputs = inputs
		}
		if outputs != nil && run.Outputs == nil {
			run.Outputs = outputs
		}
	}

	runType, err := getRunType(spanAttrs)
	if err != nil {
		return nil, err
	} else {
		run.RunType = runType
	}
	if entityName, ok := spanAttrs[LangSmithTraceName]; ok {
		if stringValue, ok := entityName.Value.(*commonpb.AnyValue_StringValue); ok {
			run.Name = &stringValue.StringValue
		}
	} else if entityName, ok := spanAttrs[TraceLoopEntityName]; ok {
		// Override run.name with traceloop.entity.name which is more specific
		if stringValue, ok := entityName.Value.(*commonpb.AnyValue_StringValue); ok {
			run.Name = &stringValue.StringValue
		}
	} else if toolName, ok := spanAttrs[OpenInferenceToolName]; ok &&
		spanAttrs[OpenInferenceSpanKind] != nil &&
		spanAttrs[OpenInferenceSpanKind].Value.(*commonpb.AnyValue_StringValue).StringValue == "TOOL" {
		if stringValue, ok := toolName.Value.(*commonpb.AnyValue_StringValue); ok {
			run.Name = &stringValue.StringValue
		}
	}

	if sessionID, ok := spanAttrs[LangSmithSessionID]; ok {
		if stringValue, ok := sessionID.Value.(*commonpb.AnyValue_StringValue); ok {
			run.SessionID = strPointer(stringValue.StringValue)
		}
	}

	if sessionName, ok := spanAttrs[LangSmithSessionName]; ok {
		if stringValue, ok := sessionName.Value.(*commonpb.AnyValue_StringValue); ok {
			run.SessionName = strPointer(stringValue.StringValue)
		}
	}

	// invocation params
	for _, modelAttrKey := range TraceLoopInvocationParams {
		if modelAttr, ok := spanAttrs[modelAttrKey]; ok {
			if _, ok := run.Extra[LangSmithInvocationParams]; !ok {
				run.Extra[LangSmithInvocationParams] = make(map[string]interface{})
			}
			if modelAttr.Value != nil {
				parts := strings.Split(modelAttrKey, ".")
				key := parts[len(parts)-1]
				if value := extractProtobufValue(modelAttr.Value); value != nil {
					run.Extra[LangSmithInvocationParams].(map[string]interface{})[key] = value
				}
			}
		}
	}
	// handle non-trace loop invocation params
	if ArizeInvocationParams, ok := spanAttrs["llm.invocation_parameters"]; ok {
		if stringValue, ok := ArizeInvocationParams.Value.(*commonpb.AnyValue_StringValue); ok {
			if _, ok := run.Extra[LangSmithInvocationParams]; !ok {
				run.Extra[LangSmithInvocationParams] = make(map[string]interface{})
			}
			var params map[string]interface{}
			if err := json.Unmarshal([]byte(stringValue.StringValue), &params); err == nil {
				for key, value := range params {
					run.Extra[LangSmithInvocationParams].(map[string]interface{})[key] = value
				}
			}
		}
	}

	// Logfire “tools” list -> invocation_params.tools
	if toolsAttr, ok := spanAttrs["tools"]; ok {
		if _, ok := run.Extra[LangSmithInvocationParams]; !ok {
			run.Extra[LangSmithInvocationParams] = make(map[string]interface{})
		}
		switch v := toolsAttr.Value.(type) {
		case *commonpb.AnyValue_ArrayValue:
			toolNames := make([]interface{}, len(v.ArrayValue.Values))
			for i, av := range v.ArrayValue.Values {
				if av != nil {
					toolNames[i] = extractProtobufValue(av.Value)
				}
			}
			run.Extra[LangSmithInvocationParams].(map[string]interface{})["tools"] = toolNames
		case *commonpb.AnyValue_StringValue:
			var arr []interface{}
			if err := json.Unmarshal([]byte(v.StringValue), &arr); err == nil {
				run.Extra[LangSmithInvocationParams].(map[string]interface{})["tools"] = arr
			} else {
				run.Extra[LangSmithInvocationParams].(map[string]interface{})["tools"] = []interface{}{v.StringValue}
			}
		}
	}

	if toolNameAttr, ok := spanAttrs["gen_ai.tool.name"]; ok {
		if _, ok := run.Extra[LangSmithInvocationParams]; !ok {
			run.Extra[LangSmithInvocationParams] = make(map[string]interface{})
		}
		if v := extractProtobufValue(toolNameAttr.Value); v != nil {
			run.Extra[LangSmithInvocationParams].(map[string]interface{})["tool_name"] = v
		}
		run.RunType = strPointer(model.RunTypeTool)
	}

	if argsAttr, ok := spanAttrs["tool_arguments"]; ok {
		if _, ok := run.Extra[LangSmithInvocationParams]; !ok {
			run.Extra[LangSmithInvocationParams] = make(map[string]interface{})
		}

		switch v := argsAttr.Value.(type) {
		case *commonpb.AnyValue_StringValue:
			var obj map[string]interface{}
			if err := json.Unmarshal([]byte(v.StringValue), &obj); err == nil {
				run.Extra[LangSmithInvocationParams].(map[string]interface{})["tool_arguments"] = obj
			} else {
				run.Extra[LangSmithInvocationParams].(map[string]interface{})["tool_arguments"] = v.StringValue
			}

		case *commonpb.AnyValue_KvlistValue:
			kv := make(map[string]interface{})
			for _, kvp := range v.KvlistValue.Values {
				kv[kvp.Key] = extractProtobufValue(kvp.Value)
			}
			run.Extra[LangSmithInvocationParams].(map[string]interface{})["tool_arguments"] = kv
		}
	}

	// Process system from gen_ai.system
	if systemAttr, ok := spanAttrs[TraceLoopGenAISystem]; ok {
		if stringValue, ok := systemAttr.Value.(*commonpb.AnyValue_StringValue); ok {
			run.Extra[LangSmithMetadata].(map[string]interface{})[LangSmithLSProvider] = stringValue.StringValue
		}
	} else if systemAttr, ok := spanAttrs[OpenInferenceSystem]; ok {
		if stringValue, ok := systemAttr.Value.(*commonpb.AnyValue_StringValue); ok {
			run.Extra[LangSmithMetadata].(map[string]interface{})[LangSmithLSProvider] = stringValue.StringValue
		}
	}

	if modelName, ok := extractModelName(spanAttrs); ok {
		run.Extra[LangSmithMetadata].(map[string]interface{})[LangSmithModelName] = modelName
	}

	// Process prompt
	inputAttr, ok := spanAttrs["input.value"]
	templateAttr, ok_prompt := spanAttrs["llm.prompt_template.variables"]
	if ok_prompt {
		run.RunType = strPointer(model.RunTypePrompt)
	}
	if ok && ok_prompt {
		if stringValue, ok := inputAttr.Value.(*commonpb.AnyValue_StringValue); ok {
			// Try to parse as JSON first
			var inputMap map[string]interface{}
			if err := json.Unmarshal([]byte(stringValue.StringValue), &inputMap); err == nil {
				// If successful JSON parse, use the parsed map
				run.Inputs = inputMap
			} else {
				// If not JSON, try to match against template variables
				if templateValue, ok := templateAttr.Value.(*commonpb.AnyValue_StringValue); ok {
					var templateVars map[string]interface{}
					if err := json.Unmarshal([]byte(templateValue.StringValue), &templateVars); err == nil {
						// Find which variable matches the input value
						inputStr := stringValue.StringValue
						for key, val := range templateVars {
							if strVal, ok := val.(string); ok && strVal == inputStr {
								run.Inputs = map[string]interface{}{
									key: inputStr,
								}
								break
							}
						}
					}
				}
			}
		}
	}

	// Process Arize retriever
	if *runType == "retriever" {
		if inputAttr, ok := spanAttrs["input.value"]; ok {
			if stringValue, ok := inputAttr.Value.(*commonpb.AnyValue_StringValue); ok {
				run.Inputs = map[string]interface{}{
					"query": stringValue.StringValue,
				}
			}
		}

		// Handle retrieval outputs
		formattedDocs := make([]interface{}, 0)

		// Look for document content and metadata pairs
		i := 0
		for {
			contentKey := fmt.Sprintf("retrieval.documents.%d.document.content", i)
			metadataKey := fmt.Sprintf("retrieval.documents.%d.document.metadata", i)

			contentAttr, hasContent := spanAttrs[contentKey]
			metadataAttr, hasMetadata := spanAttrs[metadataKey]

			if !hasContent || !hasMetadata {
				break
			}

			if contentValue, ok := contentAttr.Value.(*commonpb.AnyValue_StringValue); ok {
				if metadataValue, ok := metadataAttr.Value.(*commonpb.AnyValue_StringValue); ok {
					var metadata map[string]interface{}
					if err := json.Unmarshal([]byte(metadataValue.StringValue), &metadata); err == nil {
						formattedDocs = append(formattedDocs, map[string]interface{}{
							"page_content": contentValue.StringValue,
							"metadata":     metadata,
							"type":         "Document",
						})
					}
				}
			}
			i++
		}

		if len(formattedDocs) > 0 {
			run.Outputs = map[string]interface{}{
				"documents": formattedDocs,
			}
		}
	}

	// Process input messages
	if prompts := extractMessages("gen_ai.prompt", spanAttrs); len(prompts) > 0 && run.Inputs == nil {
		run.Inputs = map[string]interface{}{
			"messages": prompts,
		}
	} else if inputMessages := extractMessages("llm.input_messages", spanAttrs); len(inputMessages) > 0 && run.Inputs == nil {
		run.Inputs = map[string]interface{}{
			"messages": inputMessages,
		}
	}

	// Process completions
	if completions := extractMessages("gen_ai.completion", spanAttrs); len(completions) > 0 && run.Outputs == nil {
		run.Outputs = map[string]interface{}{
			"messages": completions,
		}
	} else if completions := extractMessages("llm.output_messages", spanAttrs); len(completions) > 0 && run.Outputs == nil {
		run.Outputs = map[string]interface{}{
			"messages": completions,
		}
	}

	// full inputs
	for _, spanInputAttr := range []string{OpenInferenceInput, TraceLoopEntityInput, GenAIPrompt, LogfirePrompt} {
		if spanInput, ok := spanAttrs[spanInputAttr]; ok {
			var input map[string]interface{}
			if stringValue, ok := spanInput.Value.(*commonpb.AnyValue_StringValue); ok {
				if err := json.Unmarshal([]byte(stringValue.StringValue), &input); err == nil {
					if run.Inputs == nil {

						run.Inputs = input
					}
				}
			}
		}
	}

	// full outputs
	for _, spanOutputAttr := range []string{OpenInferenceOutput, TraceLoopEntityOutput, GenAICompletion, LogfireAllMessagesEvents} {
		if spanOutput, ok := spanAttrs[spanOutputAttr]; ok {
			var output map[string]interface{}
			if stringValue, ok := spanOutput.Value.(*commonpb.AnyValue_StringValue); ok {
				outputStr := stringValue.StringValue
				// Try to unmarshal as JSON first
				if err := json.Unmarshal([]byte(outputStr), &output); err != nil {
					// If it fails, wrap the text in a JSON structure
					output = map[string]interface{}{
						"text": outputStr,
					}
				}
				// Convert embedding arrays to []float64 if present
				if data, ok := output["data"].([]interface{}); ok {
					for _, item := range data {
						if embeddingMap, ok := item.(map[string]interface{}); ok {
							if embedding, ok := embeddingMap["embedding"].([]interface{}); ok {
								embeddingMap["embedding"] = convertToFloat64Array(embedding)
							}
						}
					}
				}

				if run.Outputs == nil {
					run.Outputs = output
				}
			}
		}
	}

	if evtAttr, ok := spanAttrs[LogfireEvents]; ok && (run.Inputs == nil || run.Outputs == nil) {
		var rawEvents []interface{}
		switch v := evtAttr.Value.(type) {
		case *commonpb.AnyValue_StringValue:
			_ = json.Unmarshal([]byte(v.StringValue), &rawEvents) // ignore error -> empty slice
		case *commonpb.AnyValue_ArrayValue:
			rawEvents = make([]interface{}, len(v.ArrayValue.Values))
			for i, av := range v.ArrayValue.Values {
				rawEvents[i] = extractProtobufValue(av)
			}
		}

		if len(rawEvents) > 0 {
			var choiceEvent map[string]interface{}
			inputEvents := make([]interface{}, 0)

			for _, ev := range rawEvents {
				if m, ok := ev.(map[string]interface{}); ok {
					if m["event.name"] == "gen_ai.choice" {
						choiceEvent = m
					} else {
						inputEvents = append(inputEvents, m)
					}
				}
			}

			if run.Inputs == nil && len(inputEvents) > 0 {
				run.Inputs = map[string]interface{}{"input": inputEvents}
			}
			if run.Outputs == nil && choiceEvent != nil {
				run.Outputs = choiceEvent
			}
		}
	}

	// Add metadata
	addPrefixedAttributesToMetadata(spanAttrs, TraceLoopAssociationProperties, run.Extra[LangSmithMetadata].(map[string]interface{}))
	addPrefixedAttributesToMetadata(spanAttrs, LangSmithMetadataPrefix, run.Extra[LangSmithMetadata].(map[string]interface{}))

	// Parse and set tags
	if tagsAttr, ok := spanAttrs[LangSmithTags]; ok {
		if stringValue, ok := tagsAttr.Value.(*commonpb.AnyValue_StringValue); ok {
			tagStr := stringValue.StringValue
			tagSlice := strings.Split(tagStr, ",")
			for i, tag := range tagSlice {
				tagSlice[i] = strings.TrimSpace(tag)
			}
			run.Tags = tagSlice
		}
	}
	// Add OpenInference metadata
	if metadataAttr, ok := spanAttrs["metadata"]; ok {
		if stringValue, ok := metadataAttr.Value.(*commonpb.AnyValue_StringValue); ok {
			var metadata map[string]interface{}
			if err := json.Unmarshal([]byte(stringValue.StringValue), &metadata); err == nil {
				// Add parsed metadata to run's metadata map
				for key, value := range metadata {
					run.Extra[LangSmithMetadata].(map[string]interface{})[key] = value
				}
			}
		}
	}

	// Allow generic OpenTelemetry attributes when enabled
	if genericOtelEnabled {
		metadata := run.Extra[LangSmithMetadata].(map[string]interface{})
		for key, attr := range spanAttrs {
			if strings.HasPrefix(key, TraceLoopAssociationProperties) ||
				strings.HasPrefix(key, "gen_ai.") ||
				strings.HasPrefix(key, "llm.") ||
				strings.HasPrefix(key, "langsmith.") ||
				key == TraceLoopEntityInput ||
				key == OpenInferenceInput ||
				key == TraceLoopEntityOutput ||
				key == OpenInferenceOutput ||
				key == TraceLoopEntityName ||
				key == TraceLoopSpanKind ||
				key == OpenInferenceSpanKind {
				continue
			}

			// Add generic attribute to metadata
			if value := extractProtobufValue(attr.Value); value != nil {
				metadata[key] = value
			}
		}
	}

	// Process token usage
	if run.Outputs != nil {
		usageMetadata := map[string]interface{}{}

		if promptTokens, ok := spanAttrs[GenAIUsagePromptTokens]; ok {
			if intValue, ok := promptTokens.Value.(*commonpb.AnyValue_IntValue); ok {
				usageMetadata["input_tokens"] = intValue.IntValue
			}
		} else if inputTokens, ok := spanAttrs[GenAIUsageInputTokens]; ok {
			if intValue, ok := inputTokens.Value.(*commonpb.AnyValue_IntValue); ok {
				usageMetadata["input_tokens"] = intValue.IntValue
			}
		} else if promptTokens, ok := spanAttrs["llm.token_count.prompt"]; ok {
			if intValue, ok := promptTokens.Value.(*commonpb.AnyValue_IntValue); ok {
				usageMetadata["input_tokens"] = intValue.IntValue
			}
		}

		if completionTokens, ok := spanAttrs[GenAIUsageCompletionTokens]; ok {
			if intValue, ok := completionTokens.Value.(*commonpb.AnyValue_IntValue); ok {
				usageMetadata["output_tokens"] = intValue.IntValue
			}
		} else if outputTokens, ok := spanAttrs[GenAIUsageOutputTokens]; ok {
			if intValue, ok := outputTokens.Value.(*commonpb.AnyValue_IntValue); ok {
				usageMetadata["output_tokens"] = intValue.IntValue
			}
		} else if completionTokens, ok := spanAttrs["llm.token_count.completion"]; ok {
			if intValue, ok := completionTokens.Value.(*commonpb.AnyValue_IntValue); ok {
				usageMetadata["output_tokens"] = intValue.IntValue
			}
		}

		if totalTokens, ok := spanAttrs[GenAIUsageTotalTokens]; ok {
			if intValue, ok := totalTokens.Value.(*commonpb.AnyValue_IntValue); ok {
				usageMetadata["total_tokens"] = intValue.IntValue
			}
		} else if totalTokens, ok := spanAttrs["llm.usage.total_tokens"]; ok {
			if intValue, ok := totalTokens.Value.(*commonpb.AnyValue_IntValue); ok {
				usageMetadata["total_tokens"] = intValue.IntValue
			}
		} else if totalTokens, ok := spanAttrs["llm.token_count.total"]; ok {
			if intValue, ok := totalTokens.Value.(*commonpb.AnyValue_IntValue); ok {
				usageMetadata["total_tokens"] = intValue.IntValue
			}
		}

		if len(usageMetadata) > 0 {
			run.Outputs["usage_metadata"] = usageMetadata
		}
	}
	// Process error events
	for _, event := range span.Events {
		if event.Name == "exception" {
			run.Status = strPointer("error")
			for _, attr := range event.Attributes {
				switch attr.Key {
				case "exception.message":
					if stringValue, ok := attr.Value.Value.(*commonpb.AnyValue_StringValue); ok {
						run.Error = strPointer(stringValue.StringValue)
					}
				case "exception.stacktrace":
					if stringValue, ok := attr.Value.Value.(*commonpb.AnyValue_StringValue); ok {
						if run.Error != nil {
							*run.Error += "\n\nStacktrace:\n" + stringValue.StringValue
						} else {
							run.Error = strPointer("Stacktrace:\n" + stringValue.StringValue)
						}
					}
				}
			}
			break
		}
	}
	// Set run.Status to "success" if not already set to "error"
	if run.Status == nil || *run.Status != "error" {
		run.Status = strPointer("success")
	}

	return run, nil
}

// Helper function to extract messages from attributes with a given prefix
func extractMessages(prefix string, spanAttrs map[string]*commonpb.AnyValue) []map[string]interface{} {
	messages := []map[string]interface{}{}
	for i := 0; ; i++ {
		roleKey := fmt.Sprintf("%s.%d.role", prefix, i)
		contentKey := fmt.Sprintf("%s.%d.content", prefix, i)
		roleKeyWithMessage := fmt.Sprintf("%s.%d.message.role", prefix, i)
		contentKeyWithMessage := fmt.Sprintf("%s.%d.message.content", prefix, i)

		// Check both formats for role and content
		roleAttr, roleExists := spanAttrs[roleKey]
		contentAttr, contentExists := spanAttrs[contentKey]

		// If not found, try the nested message format
		if !roleExists {
			roleAttr, roleExists = spanAttrs[roleKeyWithMessage]
		}
		if !contentExists {
			contentAttr, contentExists = spanAttrs[contentKeyWithMessage]
		}

		if !roleExists && !contentExists {
			break
		}

		message := map[string]interface{}{}
		if roleExists && roleAttr != nil && roleAttr.Value != nil {
			if roleValue, ok := roleAttr.Value.(*commonpb.AnyValue_StringValue); ok {
				message["role"] = roleValue.StringValue
			}
		}
		if contentExists && contentAttr != nil && contentAttr.Value != nil {
			if contentValue, ok := contentAttr.Value.(*commonpb.AnyValue_StringValue); ok {
				message["content"] = contentValue.StringValue
			}
		}
		if len(message) > 0 {
			messages = append(messages, message)
		}
	}
	return messages
}

func getRunType(spanAttrs map[string]*commonpb.AnyValue) (*string, error) {
	// check for LangSmith type
	if lsProvider, ok := spanAttrs[LangSmithSpanKind]; ok {
		if stringValue, ok := lsProvider.Value.(*commonpb.AnyValue_StringValue); ok {
			switch strings.ToLower(stringValue.StringValue) {
			case model.RunTypeTool, model.RunTypeChain, model.RunTypeLLM,
				model.RunTypeRetriever, model.RunTypeEmbedding,
				model.RunTypePrompt, model.RunTypeParser:
				return strPointer(strings.ToLower(stringValue.StringValue)), nil
			}
		}
	} else if operationName, ok := spanAttrs[GenAIOperationName]; ok {
		if stringValue, ok := operationName.Value.(*commonpb.AnyValue_StringValue); ok {
			switch strings.ToLower(stringValue.StringValue) {
			case "chat", "completion":
				return strPointer(model.RunTypeLLM), nil
			case "embedding":
				return strPointer(model.RunTypeEmbedding), nil
			}
		}
	} else if requestType, ok := spanAttrs[TraceLoopLLMRequestType]; ok {
		if stringValue, ok := requestType.Value.(*commonpb.AnyValue_StringValue); ok && strings.ToLower(stringValue.StringValue) == "embedding" {
			return strPointer(model.RunTypeEmbedding), nil
		}
		return strPointer(model.RunTypeLLM), nil
	} else {
		// Check for traceloop.span.kind
		for _, spanKindAttr := range []string{OpenInferenceSpanKind, TraceLoopSpanKind} {
			if spanKind, ok := spanAttrs[spanKindAttr]; ok {
				if stringValue, ok := spanKind.Value.(*commonpb.AnyValue_StringValue); ok {
					switch strings.ToLower(stringValue.StringValue) {
					case "tool":
						return strPointer(model.RunTypeTool), nil
					case "workflow", "task", "agent", "chain":
						return strPointer(model.RunTypeChain), nil
					case "llm":
						return strPointer(model.RunTypeLLM), nil
					case "retriever":
						return strPointer(model.RunTypeRetriever), nil
					case "embedding":
						return strPointer(model.RunTypeEmbedding), nil
					}
					return strPointer(model.RunTypeChain), nil
				}
			}
		}
	}
	return strPointer(model.RunTypeChain), nil
}

func extractFromEvents(events []*tracesdkpb.Span_Event) (map[string]interface{}, map[string]interface{}) {
	var inputs, outputs map[string]interface{}

	for _, event := range events {
		eventAttrs := make(map[string]*commonpb.AnyValue)
		for _, attr := range event.Attributes {
			eventAttrs[attr.Key] = attr.Value
		}

		switch event.Name {
		case GenAIContentPrompt:
			if promptAttr, ok := eventAttrs[GenAIPrompt]; ok {
				if stringValue, ok := promptAttr.Value.(*commonpb.AnyValue_StringValue); ok {
					var promptMap map[string]interface{}
					if err := json.Unmarshal([]byte(stringValue.StringValue), &promptMap); err == nil {
						inputs = promptMap
					} else {
						// If not JSON, use as plain string
						inputs = map[string]interface{}{
							"input": stringValue.StringValue,
						}
					}
				}
			}
		case GenAIContentCompletion:
			if completionAttr, ok := eventAttrs[GenAICompletion]; ok {
				if stringValue, ok := completionAttr.Value.(*commonpb.AnyValue_StringValue); ok {
					var completionMap map[string]interface{}
					if err := json.Unmarshal([]byte(stringValue.StringValue), &completionMap); err == nil {
						outputs = completionMap
					} else {
						// If not JSON, use as plain string
						outputs = map[string]interface{}{
							"output": stringValue.StringValue,
						}
					}
				}
			}
		}
	}

	return inputs, outputs
}
