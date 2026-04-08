# Endpoint Picker (EPP)

The Endpoint Picker (EPP) is the core scheduling component of llm-d that makes LLM-aware routing decisions for inference requests.

## Functionality

The EPP is the "brains" of an llm-d deployment. It is an extensible, plugin-based component that decides which model server pod in an `InferencePool` should handle each incoming inference request.

Unlike traditional load balancers that route based on connection counts or round-robin, the EPP understands the internal state of LLM inference engines -- KV-cache utilization, prefix cache locality, request queue depth, and active request counts -- to make scheduling decisions that dramatically improve latency and throughput.

The EPP integrates with the proxy layer via Envoy's [External Processing (ext-proc)](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/ext_proc_filter) protocol. When a request arrives at the proxy, the proxy calls the EPP to select a backend endpoint, and the EPP returns the optimal pod address.

## Design

### Lifecycle of a Request

The following diagram shows the end-to-end lifecycle of a request as it flows through the EPP plugin pipeline:

```
                          ┌──────────────────────────────────────────────┐
                          │              EndpointPickerConfig            │
                          │                                              │
  ┌─────────┐    ext-proc │  ┌───────────┐                               │
  │  Proxy  │────────────►│  │ Handlers  │  Prepare request context      │
  │(Gateway)│             │  │(tokenizer,│  (tokenize, set profile, etc) │
  │         │             │  │ profiles) │                               │
  │         │             │  └─────┬─────┘                               │
  │         │             │        │                                      │
  │         │             │        ▼                                      │
  │         │             │  ┌───────────┐  Remove ineligible endpoints  │
  │         │             │  │  Filters  │  (e.g. prefill-only,          │
  │         │             │  │           │   decode-only)                │
  │         │             │  └─────┬─────┘                               │
  │         │             │        │                                      │
  │         │             │        ▼                                      │
  │         │             │  ┌───────────┐  Score remaining endpoints    │
  │         │             │  │  Scorers  │  (cache locality, queue       │
  │         │             │  │ (weighted)│   depth, utilization, etc)    │
  │         │             │  └─────┬─────┘                               │
  │         │             │        │                                      │
  │         │             │        ▼                                      │
  │         │             │  ┌───────────┐  Select highest-scoring       │
  │         │             │  │  Picker   │  endpoint                     │
  │         │◄────────────│  │           │                               │
  │         │  endpoint   │  └───────────┘                               │
  └────┬────┘             └──────────────────────────────────────────────┘
       │
       │  route request
       ▼
  ┌──────────┐
  │  Model   │
  │  Server  │
  │  (vLLM)  │
  └──────────┘
```

The steps are:

1. **Request arrival** -- An inference request (e.g., an OpenAI-compatible chat completion) arrives at the proxy (Gateway).
2. **ext-proc call** -- The proxy invokes the EPP via the ext-proc protocol, passing the request headers and body.
3. **Handler plugins** -- Handler plugins prepare the request context. This may include tokenizing the prompt (for cache-aware scoring), selecting a scheduling profile (e.g., `prefill` vs. `decode` in disaggregated serving), or annotating headers.
4. **Filter plugins** -- Filter plugins narrow the set of candidate endpoints. For example, in disaggregated serving, the `prefill-filter` removes decode-only pods and vice versa.
5. **Scorer plugins** -- Each scorer plugin independently scores all remaining candidate endpoints. Scores are multiplied by configurable weights and summed to produce a final score per endpoint.
6. **Picker plugin** -- The picker selects the endpoint with the best combined score (typically `max-score-picker`).
7. **Response** -- The EPP returns the selected endpoint address to the proxy, which routes the request to that model server pod.

### Plugin Types

The EPP pipeline is composed of four types of plugins, each serving a distinct role:

#### Handlers

Handler plugins prepare the request context before filtering and scoring. They run first in the pipeline and set up the state that downstream plugins depend on.

| Plugin | Description |
|--------|-------------|
| `tokenizer` | Tokenizes the request prompt, enabling cache-aware scorers to compare token sequences across endpoints. |
| `single-profile-handler` | Routes all requests through a single scheduling profile (the default for non-disaggregated deployments). |
| `pd-profile-handler` | Orchestrates prefill/decode disaggregation by selecting the appropriate scheduling profile and coordinating the two-phase request flow. |
| `prefill-header-handler` | Manages prefill request headers and tracking for disaggregated serving. |

#### Filters

Filter plugins remove ineligible endpoints from consideration. They execute after handlers and before scoring.

| Plugin | Description |
|--------|-------------|
| `prefill-filter` | Retains only endpoints capable of serving prefill requests. Used in disaggregated serving. |
| `decode-filter` | Retains only endpoints capable of serving decode requests. Used in disaggregated serving. |

#### Scorers

Scorer plugins assign a numerical score to each candidate endpoint. Multiple scorers run independently, and their scores are combined using configurable weights. The final score for an endpoint is computed as:

```
Final Score = Σ (scorer_weight × scorer_score)
```

The `max-score-picker` then selects the endpoint with the highest final score.

**Available Scorers:**

| Scorer | Description | Parameters |
|--------|-------------|------------|
| `prefix-cache-scorer` | Tracks request traffic patterns to estimate which endpoints are likely to have relevant prefix cache entries in GPU memory. Does not require direct introspection of the model server. | `maxPrefixBlocksToMatch`, `lruCapacityPerServer`, `autoTune` |
| `precise-prefix-cache-scorer` | Provides real-time, precise prefix cache awareness by subscribing to vLLM's KV-Events stream. Requires a tokenizer sidecar and ZMQ connectivity. | `tokenProcessorConfig.blockSize`, `indexerConfig.speculativeIndexing`, `kvEventsConfig.zmqEndpoint`, `kvEventsConfig.concurrency`, `kvEventsConfig.discoverPods` |
| `kv-cache-utilization-scorer` | Scores based on current KV-cache memory utilization. Lower utilization scores higher. | None |
| `queue-scorer` | Scores based on request queue depth. Shorter queues score higher. | None |
| `active-request-scorer` | Scores based on in-flight request count. | None |
| `slo-scorer` | (Experimental) Uses latency predictor sidecars to estimate per-endpoint response latency and scores based on SLO targets. | See [Latency Predictor](../../components/advanced/latency-predictor.md) |

Different deployment scenarios call for different scorer combinations. Higher weights mean the scorer has more influence on the final endpoint selection. Tuning weights lets you trade off between objectives -- for example, increasing `prefix-cache-scorer` weight favors cache locality at the potential cost of queue imbalance.

#### Pickers

Picker plugins make the final endpoint selection based on the aggregated scores.

| Plugin | Description |
|--------|-------------|
| `max-score-picker` | Selects the endpoint with the highest weighted score. This is the default picker for most deployments. |
| `random-picker` | Selects a random endpoint. Used as a fallback or in scenarios like wide expert-parallelism where fine-grained scoring is not yet supported. |

### Flow Control

Flow control is an optional feature that prevents backend overload by shifting intelligent queuing from model servers into the EPP gateway layer. Without flow control, the EPP always routes immediately to the best available endpoint, even if all endpoints are under heavy load -- pushing backpressure into model server queues where the EPP has no visibility or control.

When flow control is enabled, the EPP acts as a centralized admission controller for the entire `InferencePool`, buffering excess requests and dispatching them only when backends have capacity.

#### Architecture

Flow control consists of two cooperating subsystems:

```
                    ┌─────────────────────────────────────────────────────┐
                    │                  Flow Control Layer                  │
                    │                                                     │
  Incoming Request  │  ┌─────────────┐        ┌────────────────────────┐ │
  ─────────────────►│  │  Assign     │        │  Central Request       │ │
                    │  │  FlowKey    │───────►│  Buffer                │ │
                    │  │ (priority + │        │                        │ │
                    │  │  fairness)  │        │  ┌───────────────────┐ │ │
                    │  └─────────────┘        │  │ Priority 0 (high) │ │ │
                    │                         │  ├───────────────────┤ │ │
                    │                         │  │ Priority 1        │ │ │
                    │                         │  ├───────────────────┤ │ │
                    │                         │  │ Priority N (low)  │ │ │
                    │                         │  └───────────────────┘ │ │
                    │                         └───────────┬────────────┘ │
                    │                                     │              │
                    │  ┌──────────────────┐    dispatch   │              │
                    │  │   Saturation     │◄──── check ───┘              │
                    │  │   Detector       │                              │
                    │  │                  │── capacity ──►  Route to     │
                    │  │ (pool health     │   available     Endpoint     │
                    │  │  monitoring)     │                              │
                    │  └──────────────────┘                              │
                    └─────────────────────────────────────────────────────┘
```

**Central Request Buffer** -- Holds incoming requests in priority-ordered queues within the EPP rather than forwarding them directly to model server queues. Each request is assigned a `FlowKey` containing a priority level and a fairness identifier.

**Saturation Detector** -- Continuously monitors the aggregate health of the `InferencePool` (KV-cache utilization, queue depths across endpoints) and gates dispatch decisions. Requests are only released to the scheduling pipeline when the detector confirms available capacity.

#### Request Lifecycle

1. **FlowKey Assignment** -- Each incoming request is assigned a `FlowKey` with two components:
   - **Fairness ID**: Extracted from the `x-gateway-inference-fairness-id` HTTP header. If absent, the request is assigned to a global default bucket.
   - **Priority**: An integer value where lower numbers indicate higher priority. Negative values are permitted for background/batch traffic.

2. **Enqueue** -- The request is placed into the appropriate priority queue in the central buffer. The requesting goroutine blocks, waiting for a dispatch signal.

3. **Policy Evaluation** -- Background dispatch workers continuously evaluate which request to release next using a two-level policy:
   - **Strict Priority**: All buffered requests from higher-priority queues are dispatched before any lower-priority requests are considered.
   - **Intra-Priority Fairness**: Within the same priority level, requests are distributed equitably across fairness IDs during contention. The system is work-conserving -- it does not artificially throttle if GPUs have spare capacity.

4. **Saturation Gate** -- Before a selected request is dispatched, the saturation detector evaluates aggregate pool health:
   - **Capacity available**: The request proceeds immediately through the normal scheduling pipeline (filters → scorers → picker).
   - **Pool saturated**: The dispatch cycle halts. This preserves strict priority ordering -- a lower-priority request is never dispatched while a higher-priority request is waiting, even if the lower-priority request arrived first.

5. **Late Binding** -- Rather than routing requests prematurely to suboptimal backends, flow control delays the scheduling decision (endpoint selection) until the moment of dispatch. This means the endpoint is chosen using the freshest possible state, improving prefix cache hit rates and reducing tail latency.

#### Saturation Detectors

The saturation detector is the gatekeeper that determines when the pool has capacity to accept new requests. Two detector plugins are available:

**`utilization-detector`** (default) -- Monitors KV-cache utilization and model server queue depth across endpoints:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `queueDepthThreshold` | `5` | Maximum allowed pending queue size on model servers. For maximum throughput, use a small non-zero value (a fraction of the max batch size). For maximum fairness, set to `1` to force centralized EPP queuing. |
| `kvCacheUtilThreshold` | `0.8` | Maximum KV-cache memory utilization (0.0--1.0) before the pool is considered saturated. |

The pool is considered saturated when *all* endpoints exceed *either* threshold.

**`concurrency-detector`** -- Monitors total in-flight request concurrency across the pool:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `maxConcurrency` | -- | Maximum concurrent requests across the pool. Recommended: set to 110% of the model server's active batch capacity (e.g., batch size of 100 → `maxConcurrency: 110`). |

#### Priority and Fairness

Flow control provides two dimensions of traffic management:

**Priority (strict ordering)** -- The flow controller always dispatches all buffered requests from higher-priority queues before servicing any requests from lower-priority queues. This ensures that latency-sensitive interactive traffic is never delayed by batch or background workloads. Negative-priority requests are held (not rejected) during saturation, guaranteeing eventual delivery.

**Fairness (equitable sharing)** -- Within the same priority level, flow control distributes capacity equitably across distinct fairness IDs. This prevents noisy-neighbor problems where a single tenant or workload monopolizes the pool. Fairness enforcement activates only during contention -- when the pool has spare capacity, all requests are dispatched without throttling.

Clients set their flow identity via HTTP headers:
- `x-gateway-inference-fairness-id`: Identifies the tenant or workload for fair-share scheduling.
- Priority is assigned via request metadata or gateway-level policy.

#### Global Resource Limits

Flow control supports byte-level admission limits to prevent the request buffer from consuming unbounded memory:

| Parameter | Description |
|-----------|-------------|
| `maxBytes` | Overall HTTP payload capacity limit for the buffer. Supports plain integers and Kubernetes Quantity format (e.g., `10Gi`, `512Mi`). |
| `defaultRequestTTL` | Fallback time-to-live for queued requests if the client does not specify one (default: `30s`). Requests that exceed their TTL are dropped from the buffer. |

Per-priority-band limits can also be configured to prevent lower-priority traffic from consuming the entire buffer:

| Parameter | Description |
|-----------|-------------|
| `priority` | Integer identifier for the priority band. |
| `maxBytes` | HTTP payload limit for this priority band. |
| `fairnessPolicyRef` | Name of the fairness policy to apply (default: `global-strict-fairness-policy`). |
| `orderingPolicyRef` | Name of the ordering policy to apply (default: `fcfs-ordering-policy`). |

#### Late Binding and Performance Trade-offs

A key design principle of flow control is **late binding** -- deferring the endpoint selection decision until the moment of dispatch rather than at request arrival. This has measurable performance implications:

- **Improved cache locality**: By the time a request is dispatched, the EPP has the freshest view of prefix cache state across endpoints, increasing cache hit rates.
- **Reduced tail latency**: Variance in time-to-first-token (TTFT) decreases because requests are not pinned to endpoints that may become overloaded between assignment and execution.
- **Protected decode performance**: Time-per-output-token (TPOT) is shielded from interference because backends are not overwhelmed with queued requests.
- **Trade-off**: Mean TTFT may increase slightly because requests spend time in the EPP buffer rather than in a model server queue. In practice, this is offset by the reduction in P99 latency and improved throughput under load.

#### Scale-to-Zero Support

Flow control enables seamless scale-to-zero for GPU deployments:

- **Request queueing during cold start**: When traffic arrives at a deployment with zero replicas, the flow control layer queues requests in the EPP buffer rather than returning 5xx errors.
- **Late binding dispatch**: The EPP holds queued requests while the autoscaler provisions pods. Once a model server becomes ready and passes health checks, the EPP immediately dispatches buffered requests.
- **User experience**: Clients see a latency spike corresponding to pod startup time, but do not receive errors during the scaling event.

This works with both HPA (via the `HPAScaleToZero` feature gate with `minReplicas: 0`) and KEDA-based autoscalers.

#### Flow Control Metrics

Flow control exposes the following Prometheus metrics:

| Metric | Type | Description | Key Labels |
|--------|------|-------------|------------|
| `inference_extension_flow_control_queue_size` | Gauge | Current count of requests in the flow control buffer. | `fairness_id`, `priority`, `inference_pool` |
| `inference_extension_flow_control_queue_bytes` | Gauge | Current size in bytes of all buffered requests. | `fairness_id`, `priority`, `inference_pool` |
| `inference_extension_flow_control_request_queue_duration_seconds` | Distribution | Total time requests spend in the flow control buffer. | `fairness_id`, `priority`, `outcome`, `inference_pool` |
| `inference_extension_flow_control_request_enqueue_duration_seconds` | Distribution | Time to enqueue a request into the buffer. | `fairness_id`, `priority`, `outcome` |
| `inference_extension_flow_control_dispatch_cycle_duration_seconds` | Distribution | Time duration of each dispatch evaluation cycle. | -- |
| `inference_extension_flow_control_pool_saturation` | Gauge | Current saturation level of the pool (0.0 = empty, 1.0 = fully saturated). | `inference_pool` |

The `inference_extension_flow_control_queue_size` metric is particularly important as it serves as the primary signal for HPA-based autoscaling. See the [workload autoscaling guide](../../../guides/workload-autoscaling/README.hpa-igw.md) for integration details.

### Monitoring

The EPP exposes Prometheus metrics for observability. Key metrics include:

- Request scheduling latency and throughput
- Per-plugin processing times
- Flow control queue depth (when enabled)
- Endpoint scoring distributions

OpenTelemetry distributed tracing is also supported for detailed request-level visibility.

## Configuration

The EPP is configured through the `EndpointPickerConfig` resource, which defines the plugin pipeline and scheduling profiles. The configuration is typically provided as a YAML file referenced from the InferencePool Helm values.

### Configuration Structure

```yaml
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
# Optional feature gates
featureGates:
  - "flowControl"
  - "prepareDataPlugins"
# Plugin definitions
plugins:
  - type: <plugin-type>         # Required: the plugin implementation
    name: <unique-name>         # Optional: override name (needed for multiple instances of same type)
    parameters:                 # Optional: plugin-specific configuration
      key: value
# Scheduling profiles
schedulingProfiles:
  - name: <profile-name>       # Profile name (e.g., "default", "prefill", "decode")
    plugins:
      - pluginRef: <plugin-name>  # References a plugin defined above
        weight: <float>           # Scorer weight (ignored for filters and pickers)
```

The `plugins` section registers all plugins and their parameters. The `schedulingProfiles` section defines one or more named profiles, each specifying which plugins to run and (for scorers) what weight to assign.

### Helm Values

The EPP is deployed alongside the InferencePool via the upstream Helm chart. Key Helm values:

| Field | Description | Example |
|---|---|---|
| `inferenceExtension.replicas` | Number of EPP replicas | `1` |
| `inferenceExtension.image` | Container image for the EPP | `ghcr.io/llm-d/llm-d-inference-scheduler:v0.7.0` |
| `inferenceExtension.extProcPort` | Port the EPP listens on for ext-proc traffic | `9002` |
| `inferenceExtension.pluginsConfigFile` | Filename for the scheduling plugin configuration | `"custom-plugins.yaml"` |
| `inferenceExtension.pluginsCustomConfig` | Inline scheduling plugin configuration | See examples below |
| `inferenceExtension.tracing.enabled` | Enable OpenTelemetry distributed tracing | `false` |
| `inferenceExtension.tracing.otelExporterEndpoint` | OpenTelemetry collector endpoint | `"http://otel-collector:4317"` |
| `inferenceExtension.monitoring.prometheus.enabled` | Enable Prometheus metrics scraping | `true` |
| `inferenceExtension.monitoring.interval` | Prometheus scrape interval | `"10s"` |

### Enabling Flow Control

Enable flow control by adding the `flowControl` feature gate:

```yaml
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
featureGates:
  - "flowControl"
```

See the [flow control configuration guide](https://gateway-api-inference-extension.sigs.k8s.io/guides/flow-control/) for tuning saturation thresholds.

## Example

A standard deployment uses approximate prefix cache scoring balanced against load-signals:

```yaml
pluginsCustomConfig:
  custom-plugins.yaml: |
    apiVersion: inference.networking.x-k8s.io/v1alpha1
    kind: EndpointPickerConfig
    plugins:
      - type: prefix-cache-scorer
      - type: kv-cache-utilization-scorer
      - type: queue-scorer
      - type: max-score-picker
    schedulingProfiles:
      - name: default
        plugins:
          - pluginRef: prefix-cache-scorer
            weight: 3.0
          - pluginRef: kv-cache-utilization-scorer
            weight: 2.0
          - pluginRef: queue-scorer
            weight: 2.0
          - pluginRef: max-score-picker
```
