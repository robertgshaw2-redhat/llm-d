# Workload Variant Autoscaler

This document describes the Workload Variant Autoscaler (WVA), a Kubernetes-based global autoscaler that optimizes replica counts for inference model servers serving LLMs by considering GPU saturation, cost, and performance budgets.

## Introduction

Standard Kubernetes autoscalers like HPA treat all replicas as interchangeable -- they scale based on generic CPU or memory utilization without understanding the nuances of LLM inference workloads. In practice, serving a model involves choosing among multiple **variants**: distinct deployment configurations that differ in hardware (e.g., A100 vs. H100 vs. L4), numerical precision, or runtime settings. Each variant has different cost, capacity, and performance characteristics.

The Workload Variant Autoscaler closes this gap. It monitors live vLLM metrics (KV-cache utilization and queue depth) from Prometheus, detects when model server replicas are approaching saturation, and makes cost-aware scaling decisions across variants. Rather than replacing HPA or KEDA, WVA emits an optimized `wva_desired_replicas` metric that these external autoscalers consume to actuate the actual scaling.

WVA is implemented in the [llm-d-workload-variant-autoscaler](https://github.com/llm-d/llm-d-workload-variant-autoscaler) repository and runs as a Kubernetes controller that manages `VariantAutoscaling` custom resources.

## How It Works

WVA operates as a controller that periodically reconciles all `VariantAutoscaling` resources (default: every 60 seconds). Each reconciliation cycle follows this flow:

### 1. Metrics Collection

The controller queries Prometheus for live vLLM metrics across all pods serving a given model:

- **KV-cache utilization** (`vllm:kv_cache_usage_perc`) -- how full the decode state is on each pod (0.0 to 1.0)
- **Queue depth** (`vllm:num_requests_waiting`) -- how many requests are waiting to be processed on each pod

Queries use `max_over_time[1m]` to capture peak values over the last minute, providing conservative safety-first analysis that prevents missed saturation events between reconciliation cycles.

### 2. Saturation Analysis

The saturation analyzer classifies each replica as **saturated** or **non-saturated** based on configurable thresholds:

- A replica is saturated when `kv_cache_usage >= kvCacheThreshold` OR `queue_length >= queueLengthThreshold`

For non-saturated replicas, the analyzer calculates **spare capacity**:

- `spare_kv = kvCacheThreshold - current_kv_usage`
- `spare_queue = queueLengthThreshold - current_queue_length`

**Scale-up** triggers when average spare capacity across healthy replicas falls below configurable triggers (`kvSpareTrigger`, `queueSpareTrigger`).

**Scale-down** uses a safety simulation: before approving a scale-down, the analyzer simulates redistributing the total load across `N-1` replicas and verifies that the remaining spare capacity still exceeds the trigger thresholds. Scale-down requires at least 2 non-saturated replicas.

### 3. Cost-Aware Variant Selection

When multiple variants serve the same model, WVA uses the `variantCost` field from each `VariantAutoscaling` spec to make cost-optimized decisions:

- **Scale-up**: the cheapest eligible variant scales up (alphabetical tie-breaking)
- **Scale-down**: the most expensive variant scales down

### 4. Cascade Scaling Prevention

WVA tracks pending replicas (pods that exist but are not yet ready) per variant. During scale-up selection, variants with pending replicas are skipped. This prevents repeated scaling of the same variant while previous pods are still initializing (typically 2-7 minutes for model loading).

### 5. Metric Emission and Actuation

WVA does not directly modify Deployment replica counts. Instead, it emits Prometheus metrics:

| Metric | Description |
| :--- | :--- |
| `wva_desired_replicas` | Optimized target replica count per variant |
| `wva_current_replicas` | Current replica count per variant |
| `wva_desired_ratio` | Ratio of desired to current replicas |

An external autoscaler (HPA or KEDA) consumes `wva_desired_replicas` via the Kubernetes external metrics API and actuates the scaling.

## Architecture Modules

| Module | Purpose | Location |
| :--- | :--- | :--- |
| Controller | Reconciles `VariantAutoscaling` CRs on a periodic schedule | `internal/controller` |
| Saturation Analyzer | Detects capacity exhaustion via KV-cache and queue metrics | `internal/saturation` |
| Metrics Collector | Queries Prometheus for vLLM metrics, enriches with pod metadata | `internal/collector` |
| Actuator | Writes optimized replica counts to CRD status | `internal/actuator` |
| Config Cache | Thread-safe, event-driven ConfigMap cache with zero-API-call reconciliation | `internal/config` |
| Discovery | Tracks namespaces containing `VariantAutoscaling` resources | `internal/discovery` |
| Model Analyzer | Queueing-theory-based capacity analyzer with Extended Kalman Filter (experimental) | `internal/modelanalyzer` |

## Configuration

### VariantAutoscaling Custom Resource

```yaml
apiVersion: llmd.ai/v1alpha1
kind: VariantAutoscaling
metadata:
  name: llama-8b-autoscaler
  namespace: llm-inference
spec:
  scaleTargetRef:
    kind: Deployment
    name: llama-8b
  modelID: "meta/llama-3.1-8b"
  minReplicas: 1
  maxReplicas: 10
  variantCost: "10.0"
```

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `scaleTargetRef` | CrossVersionObjectReference | - | Reference to the target Deployment (required). |
| `modelID` | string | - | OpenAI API-compatible model identifier (required, min length 1). |
| `minReplicas` | integer | `1` | Lower replica bound. Set to `0` to enable scale-to-zero. |
| `maxReplicas` | integer | `2` | Upper replica bound (minimum 1). |
| `variantCost` | string | `"10.0"` | Cost per replica for optimization. Must match pattern `^\d+(\.\d+)?$`. |

### Controller Configuration

WVA resolves configuration with this precedence: CLI flags > environment variables > ConfigMap > defaults.

| Parameter | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| Prometheus URL | `PROMETHEUS_BASE_URL` | - | Prometheus server URL (required). |
| Optimization interval | `GLOBAL_OPT_INTERVAL` | `60s` | How often all VAs are reconciled. |
| Scale to zero | `WVA_SCALE_TO_ZERO` | `false` | Enable scale-to-zero support. |
| Limited mode | `WVA_LIMITED_MODE` | `false` | Respect cluster capacity constraints. |
| Metrics bind address | `METRICS_BIND_ADDRESS` | `0` | Endpoint for Prometheus scraping (`:8443` HTTPS, `:8080` HTTP, `0` disabled). |
| Health probe address | `HEALTH_PROBE_BIND_ADDRESS` | `:8081` | Health/readiness probe endpoint. |
| Leader election | `LEADER_ELECT` | `false` | Enable HA leader election. |
| Controller instance | `WVA_CONTROLLER_INSTANCE` | `""` | Instance label for multi-controller isolation. |

### Saturation Scaling Configuration

Saturation thresholds are configured via a ConfigMap with global defaults and per-model overrides:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: capacity-scaling-config
  namespace: workload-variant-autoscaler-system
data:
  default: |
    kvCacheThreshold: 0.80
    queueLengthThreshold: 5
    kvSpareTrigger: 0.10
    queueSpareTrigger: 3
  llama-70b-prod: |
    model_id: meta/llama-70b
    namespace: production
    kvCacheThreshold: 0.85
    kvSpareTrigger: 0.15
```

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `kvCacheThreshold` | float64 | `0.80` | KV-cache saturation point (0.0-1.0). |
| `queueLengthThreshold` | integer | `5` | Queue depth saturation point. |
| `kvSpareTrigger` | float64 | `0.10` | Scale-up trigger when average spare KV capacity falls below this value. |
| `queueSpareTrigger` | integer | `3` | Scale-up trigger when average spare queue capacity falls below this value. |

Per-model overrides must include `model_id` and `namespace`. Unspecified fields inherit from the `default` entry. Changes take effect immediately via ConfigMap watch -- no pod restart required.

Namespace-local overrides are also supported using well-known ConfigMap names (`wva-saturation-scaling-config`) in the workload namespace.

### EPP Threshold Alignment

For optimal performance, WVA and EPP saturation thresholds should be aligned per model:

| Concept | WVA Field | EPP Field | Recommended Default |
| :--- | :--- | :--- | :--- |
| KV-cache saturation | `kvCacheThreshold` | `kvCacheUtilThreshold` | `0.80` |
| Queue saturation | `queueLengthThreshold` | `queueDepthThreshold` | `5` |

Since each model has a dedicated EPP instance, ensure thresholds match for each model individually.

## External Autoscaler Integration

WVA emits metrics but relies on an external autoscaler for actuation. Two integrations are supported:

### HPA Integration

HPA consumes `wva_desired_replicas` via the Prometheus Adapter and the Kubernetes external metrics API:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: llama-8b-hpa
  namespace: llm-inference
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: llama-8b
  minReplicas: 1
  maxReplicas: 10
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 240
    scaleDown:
      stabilizationWindowSeconds: 240
  metrics:
  - type: External
    external:
      metric:
        name: wva_desired_replicas
        selector:
          matchLabels:
            variant_name: llama-8b
      target:
        type: AverageValue
        averageValue: "1"
```

This requires a Prometheus Adapter deployment that translates `wva_desired_replicas` into a Kubernetes external metric.

### KEDA Integration

KEDA queries `wva_desired_replicas` directly from Prometheus and natively supports scale-to-zero:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: llama-8b-scaler
  namespace: llm-inference
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: llama-8b
  pollingInterval: 5
  cooldownPeriod: 30
  maxReplicaCount: 10
  fallback:
    failureThreshold: 3
    replicas: 2
  triggers:
  - type: prometheus
    metadata:
      serverAddress: https://prometheus.monitoring.svc.cluster.local:9090
      query: |
        wva_desired_replicas{
          variant_name="llama-8b",
          exported_namespace="llm-inference"
        }
      threshold: '1'
      activationThreshold: '0'
      metricType: "AverageValue"
```

## Examples

### Single-Variant Deployment

A basic setup with one model variant and HPA actuation:

```yaml
# 1. Deploy the model server
apiVersion: apps/v1
kind: Deployment
metadata:
  name: llama-8b
  namespace: llm-inference
spec:
  replicas: 2
  # ... model server pod spec ...

---
# 2. Create the VariantAutoscaling resource
apiVersion: llmd.ai/v1alpha1
kind: VariantAutoscaling
metadata:
  name: llama-8b-autoscaler
  namespace: llm-inference
spec:
  scaleTargetRef:
    kind: Deployment
    name: llama-8b
  modelID: "meta/llama-3.1-8b"
  minReplicas: 1
  maxReplicas: 10
  variantCost: "10.0"
```

### Multi-Variant Cost Optimization

Serving the same model on different accelerator tiers with cost-aware scaling. WVA prefers scaling the cheaper L4 variant and only scales the expensive A100 variant when L4 capacity is exhausted:

```yaml
# Variant 1: Cost-efficient L4 GPUs
apiVersion: llmd.ai/v1alpha1
kind: VariantAutoscaling
metadata:
  name: llama-70b-l4
  namespace: production
spec:
  scaleTargetRef:
    kind: Deployment
    name: llama-70b-l4
  modelID: "meta/llama-70b"
  minReplicas: 2
  maxReplicas: 20
  variantCost: "5.0"

---
# Variant 2: High-performance A100 GPUs
apiVersion: llmd.ai/v1alpha1
kind: VariantAutoscaling
metadata:
  name: llama-70b-a100
  namespace: production
spec:
  scaleTargetRef:
    kind: Deployment
    name: llama-70b-a100
  modelID: "meta/llama-70b"
  minReplicas: 1
  maxReplicas: 10
  variantCost: "20.0"
```

With per-model saturation overrides:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: capacity-scaling-config
  namespace: workload-variant-autoscaler-system
data:
  default: |
    kvCacheThreshold: 0.80
    queueLengthThreshold: 5
    kvSpareTrigger: 0.10
    queueSpareTrigger: 3
  llama-70b-prod: |
    model_id: meta/llama-70b
    namespace: production
    kvCacheThreshold: 0.85
    kvSpareTrigger: 0.15
```

### Scale-to-Zero with KEDA

Enable scale-to-zero for dev/staging environments where models may have long idle periods:

```yaml
apiVersion: llmd.ai/v1alpha1
kind: VariantAutoscaling
metadata:
  name: llama-8b-dev
  namespace: development
spec:
  scaleTargetRef:
    kind: Deployment
    name: llama-8b
  modelID: "meta/llama-3.1-8b"
  minReplicas: 0
  maxReplicas: 5
  variantCost: "10.0"
```

The controller must have `WVA_SCALE_TO_ZERO=true` set, and the Kubernetes API server requires the `HPAScaleToZero` feature gate when using HPA. KEDA supports scale-to-zero natively.

## Safety and Observability

### Metrics Safety Net

If Prometheus becomes unreachable or capacity analysis fails, WVA uses the previous desired replica count from the CRD status. If no previous value exists, it defaults to the current replica count -- a safe no-op that prevents unintended scaling during outages.

### Workload Profiles

| Workload Type | `kvCacheThreshold` | `queueLengthThreshold` | Rationale |
| :--- | :--- | :--- | :--- |
| Conservative (default) | 0.80 | 5 | Balanced performance and utilization |
| Aggressive | 0.90 | 15 | Maximize GPU usage, higher latency variance |
| Strict | 0.70 | 3 | Prioritize responsiveness, lower utilization |

### Multi-Controller Isolation

In clusters with multiple WVA instances, set `wva.controllerInstance` in the Helm chart. Each controller only manages `VariantAutoscaling` resources labeled with a matching `wva.llmd.ai/controller-instance`, preventing metric conflicts.

## Further Reading

- [llm-d-workload-variant-autoscaler: Configuration Guide](https://github.com/llm-d/llm-d-workload-variant-autoscaler/blob/main/docs/user-guide/configuration.md) -- complete configuration reference
- [llm-d-workload-variant-autoscaler: CRD Reference](https://github.com/llm-d/llm-d-workload-variant-autoscaler/blob/main/docs/user-guide/crd-reference.md) -- full custom resource field reference
- [llm-d-workload-variant-autoscaler: HPA Integration](https://github.com/llm-d/llm-d-workload-variant-autoscaler/blob/main/docs/integrations/hpa-integration.md) -- step-by-step HPA setup guide
- [llm-d-workload-variant-autoscaler: KEDA Integration](https://github.com/llm-d/llm-d-workload-variant-autoscaler/blob/main/docs/integrations/keda-integration.md) -- step-by-step KEDA setup guide
- [llm-d-workload-variant-autoscaler: Saturation Analyzer](https://github.com/llm-d/llm-d-workload-variant-autoscaler/blob/main/docs/saturation-analyzer.md) -- detailed saturation analysis algorithm
