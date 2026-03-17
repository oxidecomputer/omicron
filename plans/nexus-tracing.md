# Distributed Tracing for Nexus Metrics Queries

## Goal

Add W3C TraceContext-compliant distributed tracing to nexus for oximeter-related requests, with traces written to disk in OTLP JSON format for later import into Jaeger.

## Scope

- Nexus → oximeter_db::Client → ClickHouse spans
- W3C TraceContext header extraction from incoming HTTP requests
- Traces written to file (not sent to collector)
- Configurable via nexus config (enabled, path, sample rate)

## Architecture Overview

```
External Client (with traceparent header)
    │
    │ HTTP + traceparent: 00-{trace_id}-{span_id}-01
    ▼
Nexus HTTP API (dropshot)
    │ Extract trace context, create root span
    │
    ├── [span: timeseries_query]
    │       │
    │       └── oximeter_db::Client::oxql_query()
    │               │
    │               ├── [span: oxql_query]
    │               │       │
    │               │       ├── [span: select_matching_timeseries_info]
    │               │       │       └── [span: sql_query] (per field table)
    │               │       │
    │               │       └── [span: select_matching_samples]
    │               │               └── [span: sql_query] (per chunk)
    │               │
    │               └── Return OxqlResult
    │
    └── HTTP Response
```

## Implementation Steps

### Step 1: Add Dependencies

**File: `/home/omnios/code/omicron/oximeter/db/Cargo.toml`**

```toml
[dependencies]
tracing = "0.1"

[dev-dependencies]
# None needed for tracing
```

**File: `/home/omnios/code/omicron/nexus/Cargo.toml`**

```toml
[dependencies]
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
opentelemetry = "0.30"
opentelemetry_sdk = { version = "0.30", features = ["rt-tokio"] }
opentelemetry-stdout = { version = "0.30", features = ["trace"] }
tracing-opentelemetry = "0.31"
```

Note: `tracing-opentelemetry` version numbers are offset from `opentelemetry` - 0.31 is compatible with opentelemetry 0.30.

### Step 2: Add Tracing Configuration

**File: `/home/omnios/code/omicron/nexus-config/src/nexus_config.rs`**

Add after `MulticastConfig` (around line 970):

```rust
/// Configuration for distributed tracing
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TracingConfig {
    /// Whether distributed tracing is enabled.
    /// Default: false
    #[serde(default)]
    pub enabled: bool,

    /// Path to write trace spans in OTLP JSON format.
    /// Default: /var/log/nexus-traces.json
    #[serde(default = "TracingConfig::default_output_path")]
    pub output_path: Utf8PathBuf,

    /// Sampling rate (0.0 to 1.0). 1.0 = sample everything.
    /// Default: 0.1 (10% sampling to minimize overhead)
    #[serde(default = "TracingConfig::default_sample_rate")]
    pub sample_rate: f64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            output_path: Self::default_output_path(),
            sample_rate: Self::default_sample_rate(),
        }
    }
}

impl TracingConfig {
    fn default_output_path() -> Utf8PathBuf {
        Utf8PathBuf::from("/var/log/nexus-traces.json")
    }

    fn default_sample_rate() -> f64 {
        0.1
    }
}
```

Add field to `PackageConfig` struct:

```rust
pub struct PackageConfig {
    // ... existing fields ...

    /// Distributed tracing configuration
    #[serde(default)]
    pub tracing: TracingConfig,
}
```

### Step 3: Initialize Tracing in Nexus

**File: `/home/omnios/code/omicron/nexus/src/lib.rs`**

Add new module and initialization function:

```rust
mod tracing_init;

// In run_server(), after slog initialization (around line 638):
if config.pkg.tracing.enabled {
    tracing_init::init_tracing(&config.pkg.tracing, &log)?;
}
```

**New file: `/home/omnios/code/omicron/nexus/src/tracing_init.rs`**

```rust
use nexus_config::TracingConfig;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::{
    trace::{self, Sampler},
    Resource,
};
use opentelemetry_stdout::SpanExporter;
use slog::{Logger, info, warn};
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::sync::Mutex;

pub fn init_tracing(config: &TracingConfig, log: &Logger) -> Result<(), String> {
    // Create/open the output file
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.output_path)
        .map_err(|e| format!("failed to open trace file {:?}: {}", config.output_path, e))?;

    let writer = Mutex::new(BufWriter::new(file));

    // Create the file exporter
    let exporter = SpanExporter::builder()
        .with_writer(writer)
        .build();

    // Configure sampler
    let sampler = if config.sample_rate >= 1.0 {
        Sampler::AlwaysOn
    } else if config.sample_rate <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::TraceIdRatioBased(config.sample_rate)
    };

    // Build the tracer provider
    let provider = trace::TracerProvider::builder()
        .with_sampler(sampler)
        .with_simple_processor(exporter)
        .with_resource(Resource::new(vec![
            opentelemetry::KeyValue::new("service.name", "nexus"),
        ]))
        .build();

    // Set as global provider
    opentelemetry::global::set_tracer_provider(provider);

    // Set up tracing-opentelemetry subscriber layer
    use tracing_subscriber::{layer::SubscriberExt, Registry};
    use tracing_opentelemetry::OpenTelemetryLayer;

    let tracer = opentelemetry::global::tracer("nexus");
    let telemetry_layer = OpenTelemetryLayer::new(tracer);

    let subscriber = Registry::default().with(telemetry_layer);
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| format!("failed to set tracing subscriber: {}", e))?;

    info!(log, "distributed tracing initialized";
        "output_path" => %config.output_path,
        "sample_rate" => config.sample_rate,
    );

    // Set W3C TraceContext propagator for header extraction
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new()
    );

    Ok(())
}
```

### Step 4: Extract Trace Context from Incoming HTTP Requests

Dropshot handlers receive a `RequestContext` that provides access to HTTP headers. We need to extract the `traceparent` header and use it as the parent span.

**File: `/home/omnios/code/omicron/nexus/src/app/metrics.rs`**

Add a helper to extract trace context from dropshot requests:

```rust
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::global;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Extract W3C TraceContext from HTTP headers and set as current span's parent
fn extract_trace_context(headers: &http::HeaderMap) {
    struct HeaderExtractor<'a>(&'a http::HeaderMap);

    impl<'a> opentelemetry::propagation::Extractor for HeaderExtractor<'a> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|v| v.to_str().ok())
        }
        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|k| k.as_str()).collect()
        }
    }

    let propagator = global::get_text_map_propagator(|p| {
        p.extract(&HeaderExtractor(headers))
    });

    tracing::Span::current().set_parent(propagator);
}
```

Then in each handler that should support distributed tracing:

```rust
#[tracing::instrument(skip(self, opctx))]
pub(crate) async fn timeseries_query(
    &self,
    opctx: &OpContext,
    headers: &http::HeaderMap,  // Pass headers from dropshot rqctx
    query: impl AsRef<str>,
) -> Result<OxqlResult, Error> {
    extract_trace_context(headers);
    // ... existing code ...
}
```

**Note**: The exact mechanism to get headers depends on how the dropshot endpoint passes context. The endpoint handler has access to `rqctx: RequestContext<ApiContext>` which contains the request headers.

### Step 5: Add Spans to oximeter_db Client

**File: `/home/omnios/code/omicron/oximeter/db/src/client/mod.rs`**

Add `#[tracing::instrument]` to key methods:

```rust
// Around line 1150, execute_with_block():
#[tracing::instrument(
    skip(self, handle),
    fields(
        db.system = "clickhouse",
        db.operation = "query",
    )
)]
async fn execute_with_block(
    &self,
    handle: &mut Handle,
    sql: &str,
) -> Result<QueryResult, Error> {
    let id = Uuid::new_v4();
    tracing::Span::current().record("db.query.id", id.to_string());
    // ... existing code ...

    // After getting result, record metrics:
    tracing::Span::current().record("db.rows_affected", result.progress.rows_read);
    // ...
}

// Around line 1090, insert_native():
#[tracing::instrument(
    skip(self, handle, block),
    fields(
        db.system = "clickhouse",
        db.operation = "insert",
    )
)]
async fn insert_native(...) { ... }
```

**File: `/home/omnios/code/omicron/oximeter/db/src/client/oxql.rs`**

```rust
// Around line 148, oxql_query():
#[tracing::instrument(
    skip(self),
    fields(
        oxql.query_id,
        oxql.query = %query.as_ref(),
    )
)]
pub async fn oxql_query(
    &self,
    query: impl AsRef<str>,
    authz_scope: QueryAuthzScope,
) -> Result<OxqlResult, Error> {
    let query_id = Uuid::new_v4();
    tracing::Span::current().record("oxql.query_id", query_id.to_string());
    // ... existing code ...
}

// Around line 400, run_oxql_query():
#[tracing::instrument(skip(self, query_log))]
async fn run_oxql_query(...) { ... }

// Around line 600, select_matching_timeseries_info():
#[tracing::instrument(skip(self, query_log, schema))]
async fn select_matching_timeseries_info(...) { ... }

// Around line 750, select_matching_samples():
#[tracing::instrument(skip(self, query_log, info, schema))]
async fn select_matching_samples(...) { ... }
```

### Step 6: Add Spans to Nexus Metrics Handlers

The handlers in metrics.rs already have spans from Step 4's `#[tracing::instrument]`. The trace context extraction in Step 4 ensures these spans become children of the incoming trace.

## Files to Modify

| File | Changes |
|------|---------|
| `oximeter/db/Cargo.toml` | Add `tracing` dependency |
| `nexus/Cargo.toml` | Add tracing ecosystem dependencies |
| `nexus-config/src/nexus_config.rs` | Add `TracingConfig` struct and field |
| `nexus/src/lib.rs` | Add tracing initialization call |
| `nexus/src/tracing_init.rs` | **New file** - tracing setup + W3C propagator |
| `nexus/src/app/metrics.rs` | Add `#[tracing::instrument]`, trace context extraction |
| `oximeter/db/src/client/mod.rs` | Add `#[tracing::instrument]` to execute methods |
| `oximeter/db/src/client/oxql.rs` | Add `#[tracing::instrument]` to query methods |

## Configuration Example

Users configure tracing in the nexus config.toml file (e.g., `nexus/examples/config.toml`):

```toml
# Distributed tracing configuration
[tracing]
# Enable/disable tracing (default: false)
enabled = true

# Path to write OTLP JSON traces (default: /var/log/nexus-traces.json)
output_path = "/var/tmp/nexus-traces.json"

# Sampling rate 0.0-1.0 (default: 0.1)
# 1.0 = sample everything (debugging)
# 0.1 = sample 10% (production)
sample_rate = 0.1
```

**Runtime overhead considerations:**
- At 10% sampling (`sample_rate = 0.1`), 90% of requests have minimal overhead
- Per-span cost: ~1μs (negligible for queries taking 100ms+)
- Memory: ~300 bytes per active span
- Recommended: Keep disabled by default, enable with low sample rate for production monitoring

## Importing Traces into Jaeger

After collecting traces, import via OpenTelemetry Collector:

```yaml
# otel-collector-config.yaml
receivers:
  otlpjsonfile:
    path: /var/tmp/nexus-traces.json

exporters:
  jaeger:
    endpoint: "localhost:14250"

service:
  pipelines:
    traces:
      receivers: [otlpjsonfile]
      exporters: [jaeger]
```

Or use the `otel-cli` tool for quick inspection:
```bash
cat /var/tmp/nexus-traces.json | jq '.resourceSpans[].scopeSpans[].spans[]'
```

## Future Extensions (Not in Scope)

- **ClickHouse trace context propagation**: Pass trace IDs to ClickHouse via query settings (`opentelemetry_start_trace_id`) to correlate client spans with server-side query logs
- Integration with slog logs (correlate log entries with spans)
- Streaming to Jaeger/OTLP collector instead of file
- Tail-based sampling (sample errors, slow queries)
- Propagate trace context to other services nexus calls (e.g., sled-agent)
