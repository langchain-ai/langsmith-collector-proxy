# LangSmith Collector-Proxy

## Overview

The **LangSmith Collector-Proxy** is a middleware service designed to efficiently aggregate, compress, and bulk-upload tracing data from your applications to LangSmith. It's specifically optimized for large-scale, parallel environments generating high volumes of tracing data.
## Why Use LangSmith Collector-Proxy?

Traditionally, each LangSmith SDK instance pushes traces directly to the LangSmith backend.
When running massively parallel workloads, this approach of individual tracing data directly from each application instance can lead to significant TLS/HTTP overhead and increased egress costs.

The LangSmith Collector-Proxy addresses these issues by batching multiple spans into fewer, larger, and compressed uploads.

## Key Features

* **Efficient Data Transfer**: Significantly reduces the number of requests by aggregating spans.
* **Compression**: Utilizes `zstd` compression to minimize data size.
* **OTLP Support**: Accepts standard OpenTelemetry Protocol (OTLP) data in both JSON and Protocol Buffer formats via HTTP POST requests.
* **Semantic Translation**: Converts GenAI semantic convention attributes to the LangSmith tracing model.
* **Flexible Batching**: Configurable batching based on either the number of spans or a time interval.
* **Span Filtering**: Filter spans based on configurable criteria to reduce noise and focus on relevant traces.

## Configuration

The Collector-Proxy can be customized via environment variables:

| Variable             | Description                                    | Default                           |
| -------------------- | ---------------------------------------------- | --------------------------------- |
| `HTTP_PORT`          | Port to run the proxy server                   | `4318`                            |
| `LANGSMITH_ENDPOINT` | LangSmith backend URL                          | `https://api.smith.langchain.com` |
| `LANGSMITH_API_KEY`  | API key for authenticating with LangSmith      | Required                          |
| `LANGSMITH_PROJECT`  | Default project for tracing data               | Optional                          |
| `BATCH_SIZE`         | Number of spans per upload batch               | `100`                             |
| `FLUSH_INTERVAL_MS`  | Time interval (in milliseconds) to flush spans | `1000` (1 seconds)                |
| `MAX_BUFFER_BYTES`   | Maximum uncompressed buffer size               | `10485760` (10MB)                 |
| `MAX_BODY_BYTES`     | Maximum size of incoming request body          | `209715200` (200MB)               |
| `MAX_RETRIES`        | Number of retry attempts for failed uploads    | `3`                               |
| `RETRY_BACKOFF_MS`   | Initial backoff duration in milliseconds       | `100`                             |
| `FILTER_NON_GENAI`   | Filter out non-GenAI spans (keeps only spans with `gen_ai.`, `langsmith.`, `llm.`, `ai.` prefixes) | `false` |
| `GENERIC_OTEL_ENABLED` | Include all OpenTelemetry attributes in spans (not just GenAI-specific ones) | `false` |

## Usage


Point your OTLP-compatible tracing clients or OpenTelemetry Collector exporter to the Collector-Proxy endpoint:
```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://langsmith-collector-proxy.<your-namespace>.svc.cluster.local:4318/v1/traces
```

Ensure your tracing requests include the necessary headers:

* `X-API-Key`: Your LangSmith API key (not required if set in environment)
* `Langsmith-Project`: Optional, specifies the project name

## Horizontal Scalability Considerations

When deploying multiple instances of the Collector-Proxy for horizontal scaling, it's important to note that all spans belonging to the same trace must be routed to the same Collector-Proxy instance. This is because the Collector-Proxy needs to have access to the complete trace data to properly batch and process it.

To achieve this, you should:

1. Configure your load balancer or routing layer to use consistent hashing based on the trace ID
2. Ensure all spans with the same trace ID are routed to the same Collector-Proxy instance

Failure to properly partition traces may result in incomplete or fragmented traces in LangSmith.

## Span Filtering

The Collector-Proxy supports automatically filtering spans to reduce noise and focus on relevant gen ai traces.

### Built-in GenAI Filtering

The proxy includes a built-in filter that keeps spans with names or attributes starting with:
- `gen_ai.` - Standard GenAI semantic conventions
- `langsmith.` - LangSmith-specific attributes
- `llm.` - LLM-related spans
- `ai.` - General AI-related spans

### Tree Reparenting

When spans are filtered, the proxy automatically maintains the trace hierarchy by:
1. **Reparenting children**: Child spans of filtered spans are reparented to the closest non-filtered ancestor
2. **Fallback to root**: If no valid parent is found, spans are attached to the trace root

### Usage Examples

**Enable GenAI-only filtering via environment variable:**

```bash
# Enable GenAI filtering
FILTER_NON_GENAI=true go run ./cmd/collector
```

**Custom filtering**
If you need to filter spans based on custom criteria, you can use the `CustomFilter` option in the `FilterConfig` struct. In order to do so you will need to modify the code in the `cmd/collector/main.go` file.
```go
filterConfig := aggregator.FilterConfig{
    FilterNonGenAI: false,
    CustomFilter: func(run *model.Run) bool {
        // Keep only runs that contain "important" in the name
        return run.Name != nil && strings.Contains(*run.Name, "important")
    },
}
```

### Limitations

- **Root span filtering**: If the root span of a trace is filtered out, its children may not be properly reparented and could be lost. It's recommended to avoid filtering root spans to maintain trace integrity. If you need to filter root spans, it is recommended to instead use the `langsmith.is_root` attribute to mark a span as a root span.

- **Parent-child relationships**: The proxy automatically maintains the trace hierarchy by reparenting child spans of filtered spans to the closest non-filtered ancestor. This means you must send all spans of a trace to the same collector proxy instance in order to maintain the trace hierarchy and non preemptively filter spans. If the proxy is unable to reparent a span, it will become a root span.

## Generic OpenTelemetry Attributes

By default, the proxy only includes GenAI-specific attributes when converting spans to LangSmith format. To include all OpenTelemetry attributes in the spans, set the `GENERIC_OTEL_ENABLED` environment variable to `true`.

When enabled, this feature will:
- Include all span attributes in the `extra` field of the LangSmith run
- Preserve custom attributes that don't follow GenAI semantic conventions

Note: Enabling this feature may increase the payload size sent to LangSmith.

## Monitoring and Health Checks

The LangSmith Collector-Proxy exposes simple health-check endpoints:

* Liveness check: `/live`
* Readiness check: `/ready`

These endpoints return `HTTP 200` when the service is operational.

## Running Locally

To run the Collector-Proxy locally, follow these steps:
```
export LANGSMITH_API_KEY=lsv2...
go run ./cmd/collector
```
You can send sample OTLP/JSON data using curl:
```
curl -X POST -H "X-API-Key: lsv2..." \
     -H "Content-Type: application/json" \
     --data '{"resourceSpans":[]}' \
     http://localhost:4318/v1/traces
```

---

**License:** Apache License 2.0 © LangChain AI
