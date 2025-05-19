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
curl -X POST -H "X-API-Key: devkey" \
     -H "Content-Type: application/json" \
     --data '{"resourceSpans":[]}' \
     http://localhost:4318/v1/traces
```

---

**License:** Apache License 2.0 © LangChain AI
