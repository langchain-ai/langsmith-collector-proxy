package translator

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/model"
	"github.com/langchain-ai/langsmith-collector-proxy/internal/util"
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
	// Deprecated
	GenAIUsagePromptTokens     = "gen_ai.usage.prompt_tokens"
	GenAIUsageCompletionTokens = "gen_ai.usage.completion_tokens"

	// LangSmith specific overrides
	LangSmithTraceName          = "langsmith.trace.name"
	LangSmithSpanKind           = "langsmith.span.kind"
	LangSmithMetadataPrefix     = "langsmith.metadata"
	LangSmithRunID              = "langsmith.span.id"
	LangSmithTraceID            = "langsmith.trace.id"
	LangSmithDottedOrder        = "langsmith.span.dotted_order"
	LangSmithParentRunID        = "langsmith.span.parent_id"
	LangSmithSessionID          = "langsmith.trace.session_id"
	LangSmithSessionName        = "langsmith.trace.session_name"
	LangSmithTags               = "langsmith.span.tags"
	LangSmithUsageMetadata      = "langsmith.usage_metadata"
	LangSmithReferenceExampleID = "langsmith.reference_example_id"
	LangSmithRoot               = "langsmith.is_root"

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

	// GenAI event names
	GenAIContentPrompt         = "gen_ai.content.prompt"
	GenAIContentCompletion     = "gen_ai.content.completion"
	GenAISystemMessageEvent    = "gen_ai.system.message"
	GenAIUserMessageEvent      = "gen_ai.user.message"
	GenAIAssistantMessageEvent = "gen_ai.assistant.message"
	GenAIToolMessageEvent      = "gen_ai.tool.message"
	GenAIChoiceEvent           = "gen_ai.choice"
	// Microsoft Semantic Kernel specific
	GenAIEventContent = "gen_ai.event.content"
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

type GenAiConverter struct{}

type spanContext struct {
	span               *tracesdkpb.Span
	attrs              map[string]*commonpb.AnyValue
	run                *model.Run
	genericOtelEnabled bool
}

func (c *GenAiConverter) ConvertSpan(span *tracesdkpb.Span, genericOtelEnabled bool) (*model.Run, error) {
	ctx := &spanContext{
		span:               span,
		attrs:              extractAttributes(span.Attributes),
		genericOtelEnabled: genericOtelEnabled,
	}

	run, err := initializeRun(ctx)
	if err != nil {
		return nil, err
	}
	ctx.run = run

	processSpanEvents(ctx)
	processRunType(ctx)
	processRunName(ctx)
	processLangSmithAttributes(ctx)
	processReferenceExampleID(ctx)
	processInvocationParams(ctx)
	processSystemAndModel(ctx)
	processPromptTemplate(ctx)
	processRetriever(ctx)
	processMessages(ctx)
	processInputsOutputs(ctx)
	processMetadata(ctx)
	processTags(ctx)
	processTokenUsage(ctx)
	processErrors(ctx)
	processGenericAttributes(ctx)
	finalizeRunStatus(ctx)

	return ctx.run, nil
}

func extractAttributes(attrs []*commonpb.KeyValue) map[string]*commonpb.AnyValue {
	result := make(map[string]*commonpb.AnyValue)
	for _, attr := range attrs {
		if attr.Value != nil {
			result[attr.Key] = attr.Value
		}
	}
	return result
}

func initializeRun(ctx *spanContext) (*model.Run, error) {
	spanID, err := idToUUID(ctx.span.GetSpanId())
	if err != nil {
		return nil, err
	}

	traceID, err := idToUUID(ctx.span.GetTraceId())
	if err != nil {
		return nil, err
	}

	var parentID *string
	isLangSmithRoot := getStringValue(ctx.attrs, LangSmithRoot) == "true"
	if !isLangSmithRoot && len(ctx.span.ParentSpanId) > 0 {
		parsed, err := idToUUID(ctx.span.ParentSpanId)
		if err != nil {
			return nil, err
		}
		parentID = util.StringPtr(parsed.String())
	}

	startTime := time.Unix(0, int64(ctx.span.GetStartTimeUnixNano()))
	endTime := time.Unix(0, int64(ctx.span.GetEndTimeUnixNano()))

	run := &model.Run{
		ID:          util.StringPtr(spanID.String()),
		TraceID:     util.StringPtr(traceID.String()),
		ParentRunID: parentID,
		Name:        &ctx.span.Name,
		StartTime:   util.StringPtr(startTime.UTC().Format(time.RFC3339Nano)),
		EndTime:     util.StringPtr(endTime.UTC().Format(time.RFC3339Nano)),
		Extra: map[string]interface{}{
			LangSmithMetadata: map[string]interface{}{
				OtelTraceIdKey: fmt.Sprintf("%x", ctx.span.TraceId),
				OtelSpanIdKey:  fmt.Sprintf("%x", ctx.span.SpanId),
			},
		},
	}

	return run, nil
}

func processSpanEvents(ctx *spanContext) {
	if len(ctx.span.Events) > 0 {
		inputs, outputs := extractFromEvents(ctx.span.Events)
		if inputs != nil && ctx.run.Inputs == nil {
			ctx.run.Inputs = inputs
		}
		if outputs != nil && ctx.run.Outputs == nil {
			ctx.run.Outputs = outputs
		}
	}
}

func processRunType(ctx *spanContext) {
	runType, err := getRunType(ctx.attrs)
	if err == nil {
		ctx.run.RunType = runType
	}
}

func processRunName(ctx *spanContext) {
	nameKeys := []struct {
		key       string
		condition func() bool
	}{
		{LangSmithTraceName, func() bool { return true }},
		{TraceLoopEntityName, func() bool { return true }},
		{OpenInferenceToolName, func() bool {
			return getStringValue(ctx.attrs, OpenInferenceSpanKind) == "TOOL"
		}},
	}

	for _, nk := range nameKeys {
		if name := getStringValue(ctx.attrs, nk.key); name != "" && nk.condition() {
			ctx.run.Name = &name
			return
		}
	}
}

func processLangSmithAttributes(ctx *spanContext) {
	langsmithAttrs := map[string]**string{
		LangSmithTraceID:     &ctx.run.TraceID,
		LangSmithDottedOrder: &ctx.run.DottedOrder,
		LangSmithSessionID:   &ctx.run.SessionID,
		LangSmithSessionName: &ctx.run.SessionName,
		LangSmithRunID:       &ctx.run.ID,
		LangSmithParentRunID: &ctx.run.ParentRunID,
	}

	for key, target := range langsmithAttrs {
		if value := getStringValue(ctx.attrs, key); value != "" {
			*target = util.StringPtr(value)
		}
	}
}

func processInvocationParams(ctx *spanContext) {
	// Process TraceLoop invocation params
	for _, key := range TraceLoopInvocationParams {
		if attr, ok := ctx.attrs[key]; ok && attr.Value != nil {
			parts := strings.Split(key, ".")
			paramKey := parts[len(parts)-1]
			if value := extractProtobufValue(attr.Value); value != nil {
				ensureInvocationParams(ctx)
				ctx.run.Extra[LangSmithInvocationParams].(map[string]interface{})[paramKey] = value
			}
		}
	}

	// Process Arize invocation params
	if params := getStringValue(ctx.attrs, "llm.invocation_parameters"); params != "" {
		var parsedParams map[string]interface{}
		if err := json.Unmarshal([]byte(params), &parsedParams); err == nil {
			ensureInvocationParams(ctx)
			for key, value := range parsedParams {
				ctx.run.Extra[LangSmithInvocationParams].(map[string]interface{})[key] = value
			}
		}
	}

	// Process tools
	processTools(ctx)

	// Process tool name and arguments
	if toolName := extractProtobufValue(getAttrValue(ctx.attrs, "gen_ai.tool.name")); toolName != nil {
		ensureInvocationParams(ctx)
		ctx.run.Extra[LangSmithInvocationParams].(map[string]interface{})["tool_name"] = toolName
		ctx.run.RunType = util.StringPtr(model.RunTypeTool)
	}

	processToolArguments(ctx)
}

func processTools(ctx *spanContext) {
	toolsAttr, ok := ctx.attrs["tools"]
	if !ok {
		return
	}

	switch v := toolsAttr.Value.(type) {
	case *commonpb.AnyValue_ArrayValue:
		toolNames := make([]interface{}, len(v.ArrayValue.Values))
		for i, av := range v.ArrayValue.Values {
			if av != nil {
				toolNames[i] = extractProtobufValue(av.Value)
			}
		}
		ensureInvocationParams(ctx)
		ctx.run.Extra[LangSmithInvocationParams].(map[string]interface{})["tools"] = toolNames
	case *commonpb.AnyValue_StringValue:
		arr := make([]interface{}, 0)
		if err := json.Unmarshal([]byte(v.StringValue), &arr); err == nil {
			ensureInvocationParams(ctx)
			ctx.run.Extra[LangSmithInvocationParams].(map[string]interface{})["tools"] = arr
		} else {
			ensureInvocationParams(ctx)
			ctx.run.Extra[LangSmithInvocationParams].(map[string]interface{})["tools"] = []interface{}{v.StringValue}
		}
	}
}

func processToolArguments(ctx *spanContext) {
	argsAttr, ok := ctx.attrs["tool_arguments"]
	if !ok {
		return
	}

	switch v := argsAttr.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		obj := make(map[string]interface{})
		ensureInvocationParams(ctx)
		if err := json.Unmarshal([]byte(v.StringValue), &obj); err == nil {
			ctx.run.Extra[LangSmithInvocationParams].(map[string]interface{})["tool_arguments"] = obj
		} else {
			ctx.run.Extra[LangSmithInvocationParams].(map[string]interface{})["tool_arguments"] = v.StringValue
		}
	case *commonpb.AnyValue_KvlistValue:
		kv := make(map[string]interface{})
		for _, kvp := range v.KvlistValue.Values {
			kv[kvp.Key] = extractProtobufValue(kvp.Value)
		}
		ensureInvocationParams(ctx)
		ctx.run.Extra[LangSmithInvocationParams].(map[string]interface{})["tool_arguments"] = kv
	}
}

func processSystemAndModel(ctx *spanContext) {
	metadata := ctx.run.Extra[LangSmithMetadata].(map[string]interface{})

	// Process system
	systemKeys := []string{TraceLoopGenAISystem, OpenInferenceSystem}
	for _, key := range systemKeys {
		if system := getStringValue(ctx.attrs, key); system != "" {
			metadata[LangSmithLSProvider] = system
			break
		}
	}

	// Process model
	if modelName, ok := extractModelName(ctx.attrs); ok {
		metadata[LangSmithModelName] = modelName
	}
}

func processPromptTemplate(ctx *spanContext) {
	_, inputOk := ctx.attrs["input.value"]
	_, templateOk := ctx.attrs["llm.prompt_template.variables"]

	if templateOk {
		ctx.run.RunType = util.StringPtr(model.RunTypePrompt)
	}

	if !inputOk || !templateOk {
		return
	}

	inputStr := getStringValue(ctx.attrs, "input.value")
	if inputStr == "" {
		return
	}

	// Try to parse as JSON first
	var inputMap map[string]interface{}
	if err := json.Unmarshal([]byte(inputStr), &inputMap); err == nil {
		ctx.run.Inputs = inputMap
		return
	}

	// If not JSON, try to match against template variables
	templateStr := getStringValue(ctx.attrs, "llm.prompt_template.variables")
	var templateVars map[string]interface{}
	if err := json.Unmarshal([]byte(templateStr), &templateVars); err == nil {
		for key, val := range templateVars {
			if strVal, ok := val.(string); ok && strVal == inputStr {
				ctx.run.Inputs = map[string]interface{}{key: inputStr}
				break
			}
		}
	}
}

func processRetriever(ctx *spanContext) {
	if ctx.run.RunType == nil || *ctx.run.RunType != "retriever" {
		return
	}

	// Process input
	if inputValue := getStringValue(ctx.attrs, "input.value"); inputValue != "" {
		ctx.run.Inputs = map[string]interface{}{"query": inputValue}
	}

	// Process documents
	formattedDocs := extractRetrieverDocuments(ctx.attrs)
	if len(formattedDocs) > 0 {
		ctx.run.Outputs = map[string]interface{}{"documents": formattedDocs}
	}
}

func extractRetrieverDocuments(attrs map[string]*commonpb.AnyValue) []interface{} {
	var docs []interface{}

	for i := 0; ; i++ {
		contentKey := fmt.Sprintf("retrieval.documents.%d.document.content", i)
		metadataKey := fmt.Sprintf("retrieval.documents.%d.document.metadata", i)

		content := getStringValue(attrs, contentKey)
		metadataStr := getStringValue(attrs, metadataKey)

		if content == "" || metadataStr == "" {
			break
		}

		var metadata map[string]interface{}
		if err := json.Unmarshal([]byte(metadataStr), &metadata); err == nil {
			docs = append(docs, map[string]interface{}{
				"page_content": content,
				"metadata":     metadata,
				"type":         "Document",
			})
		}
	}

	return docs
}

func processMessages(ctx *spanContext) {
	// Process input messages
	if ctx.run.Inputs == nil {
		if prompts := extractMessages("gen_ai.prompt", ctx.attrs); len(prompts) > 0 {
			ctx.run.Inputs = map[string]interface{}{"messages": prompts}
		} else if inputMessages := extractMessages("llm.input_messages", ctx.attrs); len(inputMessages) > 0 {
			ctx.run.Inputs = map[string]interface{}{"messages": inputMessages}
		}
	}

	// Process completions
	if ctx.run.Outputs == nil {
		if completions := extractMessages("gen_ai.completion", ctx.attrs); len(completions) > 0 {
			ctx.run.Outputs = map[string]interface{}{"messages": completions}
		} else if completions := extractMessages("llm.output_messages", ctx.attrs); len(completions) > 0 {
			ctx.run.Outputs = map[string]interface{}{"messages": completions}
		}
	}
}

func processInputsOutputs(ctx *spanContext) {
	// Process inputs
	inputKeys := []string{OpenInferenceInput, TraceLoopEntityInput, GenAIPrompt, LogfirePrompt}
	if ctx.run.Inputs == nil {
		for _, key := range inputKeys {
			if inputStr := getStringValue(ctx.attrs, key); inputStr != "" {
				var input map[string]interface{}
				if err := json.Unmarshal([]byte(inputStr), &input); err == nil {
					ctx.run.Inputs = input
					break
				} else {
					ctx.run.Inputs = map[string]interface{}{"input": inputStr}
					break
				}
			}
		}
	}

	// Process outputs
	outputKeys := []string{OpenInferenceOutput, TraceLoopEntityOutput, GenAICompletion, LogfireAllMessagesEvents}
	if ctx.run.Outputs == nil {
		for _, key := range outputKeys {
			if outputStr := getStringValue(ctx.attrs, key); outputStr != "" {
				ctx.run.Outputs = parseOutput(outputStr)
				break
			}
		}
	}

	// Process Logfire events
	processLogfireEvents(ctx)
}

func parseOutput(outputStr string) map[string]interface{} {
	var output map[string]interface{}
	if err := json.Unmarshal([]byte(outputStr), &output); err != nil {
		output = map[string]interface{}{"text": outputStr}
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

	return output
}

func processLogfireEvents(ctx *spanContext) {
	evtAttr, ok := ctx.attrs[LogfireEvents]
	if !ok || (ctx.run.Inputs != nil && ctx.run.Outputs != nil) {
		return
	}

	var rawEvents []interface{}
	switch v := evtAttr.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		_ = json.Unmarshal([]byte(v.StringValue), &rawEvents)
	case *commonpb.AnyValue_ArrayValue:
		rawEvents = make([]interface{}, len(v.ArrayValue.Values))
		for i, av := range v.ArrayValue.Values {
			rawEvents[i] = extractProtobufValue(av)
		}
	}

	if len(rawEvents) == 0 {
		return
	}

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

	if ctx.run.Inputs == nil && len(inputEvents) > 0 {
		ctx.run.Inputs = map[string]interface{}{"input": inputEvents}
	}
	if ctx.run.Outputs == nil && choiceEvent != nil {
		ctx.run.Outputs = choiceEvent
	}
}

func processMetadata(ctx *spanContext) {
	metadata := ctx.run.Extra[LangSmithMetadata].(map[string]interface{})

	// Add prefixed metadata
	addPrefixedAttributesToMetadata(ctx.attrs, TraceLoopAssociationProperties, metadata)
	addPrefixedAttributesToMetadata(ctx.attrs, LangSmithMetadataPrefix, metadata)

	// Add OpenInference metadata
	if metadataStr := getStringValue(ctx.attrs, "metadata"); metadataStr != "" {
		var parsedMetadata map[string]interface{}
		if err := json.Unmarshal([]byte(metadataStr), &parsedMetadata); err == nil {
			for key, value := range parsedMetadata {
				metadata[key] = value
			}
		}
	}
}

func processTags(ctx *spanContext) {
	if tagsStr := getStringValue(ctx.attrs, LangSmithTags); tagsStr != "" {
		tags := strings.Split(tagsStr, ",")
		for i, tag := range tags {
			tags[i] = strings.TrimSpace(tag)
		}
		ctx.run.Tags = tags
	}
}

func processTokenUsage(ctx *spanContext) {
	if ctx.run.Outputs == nil {
		return
	}

	usageMetadata := map[string]interface{}{}

	if usageMetadataStr := getStringValue(ctx.attrs, LangSmithUsageMetadata); usageMetadataStr != "" {
		if err := json.Unmarshal([]byte(usageMetadataStr), &usageMetadata); err == nil {
			ctx.run.Outputs["usage_metadata"] = usageMetadata
			return
		}
	}

	// Input tokens
	inputTokenKeys := []string{GenAIUsagePromptTokens, GenAIUsageInputTokens, "llm.token_count.prompt"}
	for _, key := range inputTokenKeys {
		if tokens := getIntValue(ctx.attrs, key); tokens != nil {
			usageMetadata["input_tokens"] = *tokens
			break
		}
	}

	// Output tokens
	outputTokenKeys := []string{GenAIUsageCompletionTokens, GenAIUsageOutputTokens, "llm.token_count.completion"}
	for _, key := range outputTokenKeys {
		if tokens := getIntValue(ctx.attrs, key); tokens != nil {
			usageMetadata["output_tokens"] = *tokens
			break
		}
	}

	// Total tokens
	totalTokenKeys := []string{GenAIUsageTotalTokens, "llm.usage.total_tokens", "llm.token_count.total"}
	for _, key := range totalTokenKeys {
		if tokens := getIntValue(ctx.attrs, key); tokens != nil {
			usageMetadata["total_tokens"] = *tokens
			break
		}
	}

	if len(usageMetadata) > 0 {
		ctx.run.Outputs["usage_metadata"] = usageMetadata
	}
}

func processErrors(ctx *spanContext) {
	for _, event := range ctx.span.Events {
		if event.Name == "exception" {
			ctx.run.Status = util.StringPtr("error")

			for _, attr := range event.Attributes {
				switch attr.Key {
				case "exception.message":
					if msg := getStringValueFromAttr(attr); msg != "" {
						ctx.run.Error = util.StringPtr(msg)
					}
				case "exception.stacktrace":
					if stacktrace := getStringValueFromAttr(attr); stacktrace != "" {
						if ctx.run.Error != nil {
							*ctx.run.Error += "\n\nStacktrace:\n" + stacktrace
						} else {
							ctx.run.Error = util.StringPtr("Stacktrace:\n" + stacktrace)
						}
					}
				}
			}
			break
		}
	}
}

func processReferenceExampleID(ctx *spanContext) {
	if referenceExampleIDStr := getStringValue(ctx.attrs, LangSmithReferenceExampleID); referenceExampleIDStr != "" {
		ctx.run.ReferenceExampleID = util.StringPtr(referenceExampleIDStr)
	}
}

func processGenericAttributes(ctx *spanContext) {
	if !ctx.genericOtelEnabled {
		return
	}

	// Ensure ctx.run.Extra is initialized
	if ctx.run.Extra == nil {
		ctx.run.Extra = make(map[string]interface{})
	}

	skipPrefixes := []string{
		TraceLoopAssociationProperties,
		"gen_ai.",
		"llm.",
		"langsmith.",
	}

	skipKeys := map[string]bool{
		TraceLoopEntityInput:  true,
		OpenInferenceInput:    true,
		TraceLoopEntityOutput: true,
		OpenInferenceOutput:   true,
		TraceLoopEntityName:   true,
		TraceLoopSpanKind:     true,
		OpenInferenceSpanKind: true,
	}

	for key, attr := range ctx.attrs {
		skip := false

		for _, prefix := range skipPrefixes {
			if strings.HasPrefix(key, prefix) {
				skip = true
				break
			}
		}

		if skip || skipKeys[key] {
			continue
		}

		if value := extractProtobufValue(attr.Value); value != nil {
			ctx.run.Extra[key] = value
		}
	}
}

func finalizeRunStatus(ctx *spanContext) {
	if ctx.run.Status == nil || *ctx.run.Status != "error" {
		ctx.run.Status = util.StringPtr("success")
	}
}

// Helper functions

func ensureInvocationParams(ctx *spanContext) {
	if _, ok := ctx.run.Extra[LangSmithInvocationParams]; !ok {
		ctx.run.Extra[LangSmithInvocationParams] = make(map[string]interface{})
	}
}

func getStringValue(attrs map[string]*commonpb.AnyValue, key string) string {
	if attr, ok := attrs[key]; ok {
		if v, ok := attr.Value.(*commonpb.AnyValue_StringValue); ok {
			return v.StringValue
		}
	}
	return ""
}

func getIntValue(attrs map[string]*commonpb.AnyValue, key string) *int64 {
	if attr, ok := attrs[key]; ok {
		if v, ok := attr.Value.(*commonpb.AnyValue_IntValue); ok {
			return &v.IntValue
		}
	}
	return nil
}

func getAttrValue(attrs map[string]*commonpb.AnyValue, key string) interface{} {
	if attr, ok := attrs[key]; ok {
		return attr.Value
	}
	return nil
}

func getStringValueFromAttr(attr *commonpb.KeyValue) string {
	if attr.Value != nil {
		if v, ok := attr.Value.Value.(*commonpb.AnyValue_StringValue); ok {
			return v.StringValue
		}
	}
	return ""
}

func idToUUID(id []byte) (uuid.UUID, error) {
	if len(id) < 8 {
		return uuid.Nil, fmt.Errorf("invalid id length: expected >= 8 bytes, got %d", len(id))
	}
	uuidBytes := make([]byte, 16)
	// Fill with zeros first
	for i := range uuidBytes {
		uuidBytes[i] = 0
	}
	// Copy the input bytes, up to 16 bytes
	copyLen := len(id)
	if copyLen > 16 {
		copyLen = 16
	}
	copy(uuidBytes[16-copyLen:], id[len(id)-copyLen:])

	result, err := uuid.FromBytes(uuidBytes)
	if err != nil {
		return uuid.Nil, err
	}
	return result, nil
}

func extractProtobufValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
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

func extractModelName(attrs map[string]*commonpb.AnyValue) (string, bool) {
	keys := []string{
		TraceLoopGenAIRequestModel,
		TraceLoopGenAIResponseModel,
		"model",
		OpenInferenceModelName,
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
				return util.StringPtr(strings.ToLower(stringValue.StringValue)), nil
			}
		}
	} else if operationName, ok := spanAttrs[GenAIOperationName]; ok {
		if stringValue, ok := operationName.Value.(*commonpb.AnyValue_StringValue); ok {
			switch strings.ToLower(stringValue.StringValue) {
			case "chat", "completion":
				return util.StringPtr(model.RunTypeLLM), nil
			case "embedding":
				return util.StringPtr(model.RunTypeEmbedding), nil
			}
		}
	} else if requestType, ok := spanAttrs[TraceLoopLLMRequestType]; ok {
		if stringValue, ok := requestType.Value.(*commonpb.AnyValue_StringValue); ok && strings.ToLower(stringValue.StringValue) == "embedding" {
			return util.StringPtr(model.RunTypeEmbedding), nil
		}
		return util.StringPtr(model.RunTypeLLM), nil
	} else {
		// Check for traceloop.span.kind
		for _, spanKindAttr := range []string{OpenInferenceSpanKind, TraceLoopSpanKind} {
			if spanKind, ok := spanAttrs[spanKindAttr]; ok {
				if stringValue, ok := spanKind.Value.(*commonpb.AnyValue_StringValue); ok {
					switch strings.ToLower(stringValue.StringValue) {
					case "tool":
						return util.StringPtr(model.RunTypeTool), nil
					case "workflow", "task", "agent", "chain":
						return util.StringPtr(model.RunTypeChain), nil
					case "llm":
						return util.StringPtr(model.RunTypeLLM), nil
					case "retriever":
						return util.StringPtr(model.RunTypeRetriever), nil
					case "embedding":
						return util.StringPtr(model.RunTypeEmbedding), nil
					}
					return util.StringPtr(model.RunTypeChain), nil
				}
			}
		}
	}
	return util.StringPtr(model.RunTypeChain), nil
}

func extractFromEvents(events []*tracesdkpb.Span_Event) (map[string]interface{}, map[string]interface{}) {
	var inputs, outputs map[string]interface{}
	var messages []map[string]interface{}
	var assistantMessages []map[string]interface{}
	var choice map[string]interface{}

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
						outputs = map[string]interface{}{
							"output": stringValue.StringValue,
						}
					}
				}
			}

		case GenAISystemMessageEvent, GenAIUserMessageEvent:
			message := extractMessageFromEvent(event, eventAttrs)
			if message != nil {
				messages = append(messages, message)
			}

		case GenAIAssistantMessageEvent:
			// Extract assistant responses
			message := extractMessageFromEvent(event, eventAttrs)
			if message != nil {
				assistantMessages = append(assistantMessages, message)
			}

		case GenAIToolMessageEvent:
			// Extract tool responses with special handling
			message := extractToolMessageFromEvent(event, eventAttrs)
			if message != nil {
				assistantMessages = append(assistantMessages, message)
			}

		case GenAIChoiceEvent:
			choice = extractChoiceFromEvent(event, eventAttrs)
		}
	}

	if len(messages) > 0 && inputs == nil {
		inputs = map[string]interface{}{"messages": messages}
	}

	if len(assistantMessages) > 0 {
		if outputs == nil {
			outputs = map[string]interface{}{}
		}
		if _, ok := outputs["messages"]; !ok {
			outputs["messages"] = assistantMessages
		} else if msgs, ok := outputs["messages"].([]map[string]interface{}); ok {
			outputs["messages"] = append(msgs, assistantMessages...)
		}
	}

	if choice != nil {
		if outputs == nil {
			outputs = map[string]interface{}{}
		}
		outputs["choices"] = []map[string]interface{}{choice}
	}

	return inputs, outputs
}

func extractMessageFromEvent(event *tracesdkpb.Span_Event, eventAttrs map[string]*commonpb.AnyValue) map[string]interface{} {
	// Microsoft Semantic Kernel specific
	if eventContent, ok := eventAttrs[GenAIEventContent]; ok {
		if stringValue, ok := eventContent.Value.(*commonpb.AnyValue_StringValue); ok {
			var message map[string]interface{}
			if err := json.Unmarshal([]byte(stringValue.StringValue), &message); err == nil {
				return message
			}
		}
	}

	message := make(map[string]interface{})

	// Extract content
	if content, ok := eventAttrs["content"]; ok {
		if value := extractProtobufValue(content.Value); value != nil {
			message["content"] = value
		}
	}

	// Extract role
	if role, ok := eventAttrs["role"]; ok {
		if value := extractProtobufValue(role.Value); value != nil {
			message["role"] = value
		}
	}

	// For tool messages, extract additional fields
	if event.Name == GenAIToolMessageEvent {
		if id, ok := eventAttrs["id"]; ok {
			if value := extractProtobufValue(id.Value); value != nil {
				message["tool_call_id"] = value
			}
		}
	}

	if len(message) > 0 {
		return message
	}
	return nil
}

func extractToolMessageFromEvent(event *tracesdkpb.Span_Event, eventAttrs map[string]*commonpb.AnyValue) map[string]interface{} {
	// Microsoft Semantic Kernel specific
	if eventContent, ok := eventAttrs[GenAIEventContent]; ok {
		if stringValue, ok := eventContent.Value.(*commonpb.AnyValue_StringValue); ok {
			var message map[string]interface{}
			if err := json.Unmarshal([]byte(stringValue.StringValue), &message); err == nil {
				if _, hasID := message["id"]; hasID {
					return message
				}
				if _, hasContent := message["content"]; hasContent {
					return message
				}
			}
		}
	}

	// Standard extraction
	return extractMessageFromEvent(event, eventAttrs)
}

func extractChoiceFromEvent(event *tracesdkpb.Span_Event, eventAttrs map[string]*commonpb.AnyValue) map[string]interface{} {
	// Microsoft Semantic Kernel specific
	if eventContent, ok := eventAttrs[GenAIEventContent]; ok {
		if stringValue, ok := eventContent.Value.(*commonpb.AnyValue_StringValue); ok {
			var choice map[string]interface{}
			if err := json.Unmarshal([]byte(stringValue.StringValue), &choice); err == nil {
				if _, hasMessage := choice["message"]; hasMessage {
					return choice
				}
			}
		}
	}
	choice := make(map[string]interface{})

	// Extract finish reason
	if finishReason, ok := eventAttrs["finish_reason"]; ok {
		if value := extractProtobufValue(finishReason.Value); value != nil {
			choice["finish_reason"] = value
		}
	}

	// Extract message content
	if messageContent, ok := eventAttrs["message.content"]; ok {
		if value := extractProtobufValue(messageContent.Value); value != nil {
			if _, exists := choice["message"]; !exists {
				choice["message"] = make(map[string]interface{})
			}
			choice["message"].(map[string]interface{})["content"] = value
		}
	}

	// Extract message role
	if messageRole, ok := eventAttrs["message.role"]; ok {
		if value := extractProtobufValue(messageRole.Value); value != nil {
			if _, exists := choice["message"]; !exists {
				choice["message"] = make(map[string]interface{})
			}
			choice["message"].(map[string]interface{})["role"] = value
		}
	}

	if toolCalls := extractToolCallsFromEventAttrs(eventAttrs); len(toolCalls) > 0 {
		choice["tool_calls"] = toolCalls
	}

	if len(choice) > 0 {
		return choice
	}
	return nil
}

func extractToolCallsFromEventAttrs(eventAttrs map[string]*commonpb.AnyValue) []map[string]interface{} {
	var toolCalls []map[string]interface{}

	for i := 0; ; i++ {
		toolCall := make(map[string]interface{})

		idKey := fmt.Sprintf("tool_calls.%d.id", i)
		if id, ok := eventAttrs[idKey]; ok {
			if value := extractProtobufValue(id.Value); value != nil {
				toolCall["id"] = value
			}
		} else {
			break
		}

		// Extract function name
		nameKey := fmt.Sprintf("tool_calls.%d.function.name", i)
		if name, ok := eventAttrs[nameKey]; ok {
			if value := extractProtobufValue(name.Value); value != nil {
				if _, exists := toolCall["function"]; !exists {
					toolCall["function"] = make(map[string]interface{})
				}
				toolCall["function"].(map[string]interface{})["name"] = value
			}
		}

		// Extract function arguments
		argsKey := fmt.Sprintf("tool_calls.%d.function.arguments", i)
		if args, ok := eventAttrs[argsKey]; ok {
			if value := extractProtobufValue(args.Value); value != nil {
				if _, exists := toolCall["function"]; !exists {
					toolCall["function"] = make(map[string]interface{})
				}
				toolCall["function"].(map[string]interface{})["arguments"] = value
			}
		}

		// Extract type
		typeKey := fmt.Sprintf("tool_calls.%d.type", i)
		if toolType, ok := eventAttrs[typeKey]; ok {
			if value := extractProtobufValue(toolType.Value); value != nil {
				toolCall["type"] = value
			}
		}

		if len(toolCall) > 0 {
			toolCalls = append(toolCalls, toolCall)
		}
	}

	return toolCalls
}
