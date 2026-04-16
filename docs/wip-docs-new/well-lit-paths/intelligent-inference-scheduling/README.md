# Intelligent Inference Scheduling

LLM inference request patterns are unique - they are multi-turn and highly heterogeneous.

llm-d's **Endpoint Picker (EPP)** replaces standard round-robin load balancing with LLM-aware scheduling, routing each request to the best model server based on real-time signals -- prefix-cache locality, KV-cache utilization, queue depth.

## Options

There are three paths in "Intelligent Inference Scheduling" family:

| Mode | What It Adds | When To Use |
|---|---|---|
| [**Default**](./default.md) | Load-aware routing + approximate prefix cache scoring | Any workload, start here |
| [**Predicted Latency**](./predicted-latency.md) | ML-predicted request-latency based routing | Highly heterogeneous workloads |
| [**Precise Prefix Cache**](./precise-prefix-cache-aware-routing.md) | Exact block-level cache indexing via KV-Events | When approximate routing is not enough (e.g., near saturation regime) |

> [!NOTE]
> We recommend most users start with the default configuration - and upgrade to the Predicted Latency-based scheduling when the workload is well understood.
