# EPP Scheduling

## Overview

The Scheduler is the core decision-making engine within the EPP, responsible for selecting the optimal model server endpoint for each incoming inference request. It operates through a modular **Filter → Score → Pick** pipeline, orchestrated by a **ProfileHandler**, enabling pluggable scheduling algorithms that evaluate endpoints based on real-time metrics like KV-cache utilization, prefix cache locality, queue depth, and active request counts.

The scheduler is built on the [Scheduling Subsystem Architecture](https://github.com/kubernetes-sigs/gateway-api-inference-extension/tree/main/docs/proposals/0845-scheduler-architecture-proposal) defined in the upstream Gateway API Inference Extension (GIE) project. General-purpose scheduling features are contributed upstream to GIE, while llm-d-specific innovations (such as [Prefill/Decode disaggregation](../../advanced/disaggregation.md) and precise prefix cache scoring) are developed in the [llm-d-inference-scheduler](https://github.com/llm-d/llm-d-inference-scheduler) repository.

## Design Principles

The scheduler framework is guided by several key design tenets:

- **Framework independence** -- The scheduler framework acts as an independent library with no dependency on EPP packages defined outside of the scheduler. This keeps the scheduling logic clean and testable.

- **Infrastructure agnosticism** -- The framework is deliberately neutral about endpoint types (such as model servers) and Kubernetes concepts. Specific opinions are delegated to plugins rather than embedded in the core framework.

- **Defined API surface** -- Entry and exit points are explicitly defined by the framework, establishing clear boundaries for plugin integration.

- **Multi-profile support** -- Multiple scheduling profiles can execute for a single request, either sequentially with dependencies or in parallel, enabling advanced use cases like disaggregated serving.

## Architecture

### Scheduling Cycle

A **scheduling cycle** is the complete execution of the scheduler for a single request. It encompasses one or more **profile runs**, each evaluating the available endpoints through the Filter → Score → Pick pipeline.

The cycle is driven by the **ProfileHandler** plugin, which determines which profiles to execute and how to interpret their results:

```
                    ┌─────────────────────────────────────────┐
                    │           Scheduling Cycle               │
                    │                                         │
  Request ──────►  │  ProfileHandler.ProfilePick()            │
                    │       │                                  │
                    │       ▼                                  │
                    │  ┌─────────────────────┐                │
                    │  │  Profile Run         │                │
                    │  │  1. Filter           │                │
                    │  │  2. Score            │                │
                    │  │  3. Pick             │                │
                    │  └─────────┬───────────┘                │
                    │            │                             │
                    │            ▼ (repeat if more profiles)   │
                    │                                         │
                    │  ProfileHandler.ProcessProfilesResults() │
                    └──────────────────┬──────────────────────┘
                                       │
                                       ▼
                               Selected Endpoint(s)
```

### Profile Run Phases

Each profile run executes three sequential phases:

#### 1. Filter

Filters run before any scoring and remove endpoints that are not fit for selection. For example, a filter might exclude endpoints that do not have the requested LoRA adapter loaded or that are marked for a different disaggregation role (prefill vs. decode).

- A profile may include zero or more filter plugins.
- If all endpoints are filtered out, the framework returns an error to the client.

#### 2. Score

Scorers assign a numerical score to each remaining endpoint. Scores should be normalized to the range `[0, 1]`. Each scorer's output is multiplied by a configurable **weight** at the profile level, and the weighted scores are summed to produce a final composite score per endpoint.

- A profile may include zero or more scorer plugins.

#### 3. Pick

The picker selects the final endpoint(s) from the scored candidate list. A picker must return at least one endpoint.

- Each profile requires exactly one picker plugin.

### ProfileHandler

The ProfileHandler is a unique plugin type -- only one may exist per scheduler. It provides two extension points:

- **ProfilePick** -- The entry point into the scheduling cycle. It evaluates the request, analyzes results from previously executed profiles, and returns zero or more profiles for execution. The framework calls it repeatedly until no more profiles are returned.

- **ProcessProfilesResults** -- The exit point that aggregates and interprets all profile run results into the final scheduling decision for consumption by calling systems.

## State Management

The scheduler maintains three distinct state layers to support different lifecycle requirements:

| State Layer | Scope | Lifecycle | Example |
|---|---|---|---|
| **CycleState** | Per-request | Created and destroyed with each scheduling cycle | Intermediate scoring data |
| **Plugin State** | Per-plugin instance | Persists across requests (plugins are instantiated once) | Prefix cache index tree |
| **Data Layer State** | Per-endpoint | Continuously updated by data layer scrapers | KV-cache utilization, queue depth metrics |

## Scheduling Profiles

A **scheduling profile** is a named configuration that combines a specific set of filters, scorers (with weights), and a picker. Profiles are defined in the [`schedulingProfiles`](configuration.md) section of the `EndpointPickerConfig`.

The number of profiles depends on the serving mode:

- **Aggregated serving** -- A single profile handles all requests.
- **Disaggregated serving (P/D)** -- Two profiles are required: one for prefill endpoints and one for decode endpoints, each with their own filters and scoring strategies.

### Profile Configuration

Each profile references plugins defined in the global `plugins` section and assigns weights to scorers:

```yaml
schedulingProfiles:
  - name: default
    plugins:
      - pluginRef: prefix-cache-scorer
        weight: 3
      - pluginRef: queue-scorer
        weight: 2
      - pluginRef: kv-cache-utilization-scorer
        weight: 2
      - pluginRef: max-score-picker
```

When no profile is explicitly defined, a `default` profile is created that references all instantiated plugins.

## Built-in Plugins

### Upstream Plugins (GIE)

The following plugins are provided by the upstream Gateway API Inference Extension project and are available in all EPP deployments.

#### Filters

There are currently no upstream filter plugins. Filtering in the upstream GIE is handled implicitly by the InferencePool's label-based endpoint discovery.

#### Scorers

| Plugin Type | Description | Key Parameters |
|---|---|---|
| `prefix-cache-scorer` | Scores pods based on how much of the prompt is believed to be in the pod's KV cache, using block-hash matching against an LRU-based prefix index. | `blockSizeTokens` (default: 64), `maxPrefixBlocksToMatch` (default: 256), `lruCapacityPerServer` (default: 31250) |
| `lora-affinity-scorer` | Scores pods based on whether the requested LoRA adapter is already loaded in the pod's HBM or if the pod can load it on demand. | None |
| `kv-cache-utilization-scorer` | Scores pods based on their KV cache utilization -- pods with lower utilization score higher. | None |
| `queue-scorer` | Scores pods based on the pod's waiting queue size. Pods with shorter queues score higher, as they are more available to serve new requests. | None |
| `running-requests-size-scorer` | Scores pods based on in-flight request count. Scores are normalized across the candidate set (fewest = 1.0, most = 0.0, linearly interpolated). | None |

#### Pickers

| Plugin Type | Description | Key Parameters |
|---|---|---|
| `max-score-picker` | Picks the pod(s) with the highest composite score. This is the default picker if none is specified. | `maxNumOfEndpoints` (default: 1) |
| `random-picker` | Picks pod(s) randomly from the candidate list, ignoring scores. | `maxNumOfEndpoints` (default: 1) |
| `weighted-random-picker` | Picks pod(s) using weighted random sampling (A-Res algorithm), where higher-scored pods are more likely to be selected. | `maxNumOfEndpoints` (default: 1) |

#### ProfileHandlers

| Plugin Type | Description |
|---|---|
| `single-profile-handler` | Selects a single primary profile for all requests. Used for aggregated (non-disaggregated) serving. |

### llm-d Plugins

The following plugins are specific to the [llm-d-inference-scheduler](https://github.com/llm-d/llm-d-inference-scheduler) and provide advanced scheduling capabilities beyond the upstream GIE.

#### Filters

| Plugin Type | Description |
|---|---|
| `by-label-selector` | Filters endpoints by Kubernetes label selectors. Only the matching labels feature of label selectors is supported. Multiple label matches are combined with AND logic. |
| `by-label` | Filters endpoints based on a single label's acceptable values, with configurable handling of unlabeled pods. |
| `prefill-filter` | Retains only pods labeled for prefill work (via the `llm-d.ai/role` label with values `prefill`, `prefill-decode`, or `encode-prefill-decode`). |
| `decode-filter` | Retains only pods labeled for decode work (via the `llm-d.ai/role` label with values `decode`, `prefill-decode`, or `encode-prefill-decode`). |
| `encode-filter` | Retains only pods labeled for encode operations. |

#### Scorers

| Plugin Type | Description |
|---|---|
| `precise-prefix-cache-scorer` | A more accurate prefix cache scorer that tracks real-time KV-cache states across vLLM instances via KV-Events, rather than relying on scheduling history estimates. |
| `load-aware-scorer` | Ranks pods by concurrent request count. Pods with empty queues receive neutral scores (0.5); overloaded pods score between 0.5 and 0. |
| `active-request-scorer` | Tracks individual requests with TTL-based timeout handling. Normalizes scores 0–1, favoring pods with fewer active requests. |
| `session-affinity-scorer` | Gives a higher score to pods that were previously used for the same session, promoting cache reuse for multi-turn conversations. |
| `no-hit-lru-scorer` | For cold requests lacking cache hits, distributes load using least-recently-used ordering to evenly spread cache growth. |
| `context-length-aware` | Routes based on token count, matching requests to pods configured for specific context length ranges. Scores in-range pods higher, with an optional filtering mode. |

#### ProfileHandlers and Disaggregation

| Plugin Type | Description |
|---|---|
| `disagg-profile-handler` | Selects appropriate scheduling profiles for disaggregated prefill/decode execution. Supports optional "decider" plugins that control whether disaggregation is triggered. |
| `disagg-headers-handler` | Sets HTTP headers required for disaggregated prefill/decode execution stages, enabling the vLLM sidecar to orchestrate inter-stage communication. |

#### Deciders

| Plugin Type | Description |
|---|---|
| `prefix-based-pd-decider` | Triggers disaggregated prefill/decode when uncached input tokens exceed a threshold, optimizing for requests with significant new content that benefit from dedicated prefill hardware. |

## Scheduling Flow Example

The following walks through a typical scheduling cycle for an aggregated serving deployment:

1. A request arrives and is parsed by the [Request Handler](request-handling.md).
2. If [Flow Control](flow-control.md) is enabled, the request may be queued until capacity is available.
3. The **ProfileHandler** (`single-profile-handler`) selects the `default` scheduling profile.
4. **Filters** (if any) remove ineligible endpoints from the candidate set.
5. **Scorers** evaluate each remaining endpoint:
   - `prefix-cache-scorer` checks how much of the prompt is cached on each pod.
   - `kv-cache-utilization-scorer` favors pods with lower KV cache utilization.
   - `queue-scorer` favors pods with shorter waiting queues.
   - Each score is multiplied by its configured weight and summed.
6. **Picker** (`max-score-picker`) selects the pod with the highest composite score.
7. The selected endpoint address is returned to the proxy for request forwarding.

For disaggregated serving, the flow is similar but involves two profile runs (one for prefill, one for decode), each with their own filters and scorers. See [Disaggregation](../../advanced/disaggregation.md) for details.

## Speculative Indexing

Speculative indexing is an optimization that closes the gap between a routing decision and the arrival of a KV-cache event from the model server. When enabled, the scheduler immediately writes a predicted cache entry after routing a request to a pod. Subsequent requests matching the same prefix can benefit from the cache without waiting for engine confirmation.

This feature is controlled via a boolean flag and configurable TTL on the predicted cache entries.

## Configuration

For details on configuring the scheduler, including plugin parameters, scheduling profiles, and scorer weights, see the [EPP Configuration](configuration.md) guide.

For the upstream reference, see the [Gateway API Inference Extension EPP Configuration Guide](https://gateway-api-inference-extension.sigs.k8s.io/guides/epp-configuration/config-text/).

## References

- [Scheduler Architecture Proposal (GIE)](https://github.com/kubernetes-sigs/gateway-api-inference-extension/tree/main/docs/proposals/0845-scheduler-architecture-proposal) -- The upstream proposal defining the pluggable scheduling framework.
- [llm-d Inference Scheduler](https://github.com/llm-d/llm-d-inference-scheduler) -- The llm-d-specific scheduler implementation with additional plugins.
- [EPP Configuration](configuration.md) -- Configuration guide for plugins, profiles, and weights.
- [Flow Control](flow-control.md) -- How request queuing and admission control interact with scheduling.
- [Disaggregation](../../advanced/disaggregation.md) -- Prefill/Decode disaggregated serving architecture.
