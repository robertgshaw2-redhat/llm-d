#!/usr/bin/env python3
"""
Flow Control Demo
=================================================================

This tool drives closed-loop, multi-tenant LLM inference traffic to demonstrate
the Endpoint Picker (EPP) Flow Control layer. It holds a fixed number of
concurrent requests in flight per tenant and visually proves how the gateway
proxy protects business-level QoS once the backend is oversubscribed.

The playbook runs two phases against a deployment sized for a known concurrency:
  1. Balanced Load (At Capacity): premium and standard traffic each hold half
     of the deployment's concurrency. The backend keeps up and both tiers see
     healthy latency.
  2. Overload (2x Capacity): both tiers ramp to the full deployment concurrency,
     oversubscribing the backend 2x. Flow Control engages and you should see
     premium traffic stay fast while standard traffic absorbs the queueing.

Concurrency model:
    The load is driven entirely by asyncio. Each tenant runs as a coroutine that
    keeps `target` requests in flight by spawning a fresh asyncio.Task whenever
    one completes. HTTP is handled by aiohttp, with a single shared ClientSession
    and an unbounded connection pool so the load generator never throttles itself
    below the target concurrency.

Prerequisites:
    Python 3.9+ and aiohttp (pip install aiohttp)

Usage:
    python3 client.py --url http://localhost:80/v1/completions --capacity 64
"""

import argparse
import asyncio
import collections
import os
import random
import string
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import aiohttp


# ==============================================================================
# 1. CORE DATA MODELS (The Flexible Architecture)
# ==============================================================================

@dataclass
class Tenant:
    """
    Represents a discrete Flow within the EPP.
    """

    # x-gateway-inference-fairness-id: tenant-a
    fairness_id: str
    # x-gateway-inference-objective: premium-traffic, standard-traffic
    inference_objective: str
    # Display-only priority used to order rows in the dashboard.
    priority: int


@dataclass
class Stage:
    """Represents a phase in the demo narrative with specific concurrency targets."""
    name: str
    duration_sec: int
    concurrency_targets: Dict[str, int]


# ==============================================================================
# 2. METRICS COLLECTOR
# ==============================================================================

class MetricsCollector:
    """
    Per-stage metrics aggregator.

    Stats are accumulated across the entire current stage (the windows are
    cleared via reset() at each stage boundary), so get_realtime_stats()
    reflects every completed request observed since the stage began rather than
    a trailing physical time window.

    Under asyncio everything runs on a single thread and none of these methods
    await, so each is effectively atomic with respect to the event loop and no
    locking is required.

    This calculation is vital for observing the Flow Control layer's behavior:
    the P90 TTFT metrics summarize the whole stage so premium vs. standard QoS
    is directly comparable for the stage as a whole.
    """
    def __init__(self):
        self.ttft_window = collections.defaultdict(list)
        self.duration_window = collections.defaultdict(list)
        self.completion_times = collections.defaultdict(list)
        self.status_counts = collections.defaultdict(lambda: collections.defaultdict(int))
        self.active_requests = collections.defaultdict(int)

    def record_start(self, fairness_id: str) -> None:
        self.active_requests[fairness_id] += 1

    def reset(self) -> None:
        """Clear windowed metrics and status counts; preserve active_requests so
        in-flight requests started before the reset still decrement correctly."""
        self.ttft_window.clear()
        self.duration_window.clear()
        self.completion_times.clear()
        self.status_counts.clear()

    def record(self, fairness_id: str, status: str, ttft: Optional[float], duration: float) -> None:
        """Records a completed (or failed) request into the sliding window."""
        self.active_requests[fairness_id] -= 1
        if self.active_requests[fairness_id] < 0:
            self.active_requests[fairness_id] = 0
        self.status_counts[fairness_id][status] += 1
        if status == "200" and ttft is not None:
            now = time.monotonic()
            self.ttft_window[fairness_id].append((now, ttft))
            self.duration_window[fairness_id].append((now, duration))
            self.completion_times[fairness_id].append(now)

    def get_realtime_stats(self, tenant_id: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], float, int, int, int, int, int]:
        """Calculates median + P90 latency and extracts status code counts for the UI.

        Aggregates over every request completed during the current stage (the
        windows are reset at each stage boundary), not just a trailing window.
        """
        now = time.monotonic()

        # TTFT median + P90 over all completions in the stage.
        ttfts = sorted(val for _ts, val in self.ttft_window[tenant_id])
        med_ttft = ttfts[len(ttfts) // 2] if ttfts else None
        p90_ttft = ttfts[int(len(ttfts) * 0.9)] if ttfts else None

        # Duration median + P90 over all completions in the stage.
        durs = sorted(val for _ts, val in self.duration_window[tenant_id])
        med_dur = durs[len(durs) // 2] if durs else None
        p90_dur = durs[int(len(durs) * 0.9)] if durs else None

        stats = self.status_counts[tenant_id]
        s_200 = stats.get("200", 0)
        s_429 = sum(c for k, c in stats.items() if "429" in str(k))
        s_503 = sum(c for k, c in stats.items() if "503" in str(k))

        s_err = sum(c for k, c in stats.items() if "200" not in str(k) and "429" not in str(k) and "503" not in str(k))

        # Achieved QPS averaged over the whole stage so far.
        achieved_qps = 0.0
        times = self.completion_times[tenant_id]

        if len(times) > 1:
            # Bounding division to prevent aggressive spikes.
            window_duration = max(now - times[0], 0.1)
            achieved_qps = len(times) / window_duration

        return med_ttft, p90_ttft, med_dur, p90_dur, achieved_qps, s_200, s_429, s_503, s_err, self.active_requests[tenant_id]

# ==============================================================================
# 3. LOAD GENERATOR ENGINE
# ==============================================================================

class LoadGenerator:
    """
    Manages the closed-loop HTTP sessions, dynamic payload math, and async tasks.

    All requests share one aiohttp.ClientSession (with an unbounded connector) so
    the load generator can hold the full target concurrency without throttling
    itself on the client side.
    """
    REQUEST_TIMEOUT = 90.0

    def __init__(self, args: argparse.Namespace, metrics: MetricsCollector, model_name: str, session: "aiohttp.ClientSession"):
        self.args = args
        self.metrics = metrics
        self.model_name = model_name
        self.session = session
        self.base_phrase = "Flow control demo payload."
        self.isl = 5000
        self.osl = 100
        self.tokens_per_phrase = 5

        # Every in-flight request task, tracked so main() can drain/cancel them.
        self.inflight: Set["asyncio.Task"] = set()

    async def verify_connectivity(self) -> None:
        """Fails fast if the target gateway is unreachable."""
        try:
            # Any HTTP response (even 404/400) means the gateway is up; only a
            # connection-level failure is fatal.
            async with self.session.post(
                self.args.url,
                json={"model": self.model_name, "prompt": ""},
                timeout=aiohttp.ClientTimeout(total=2.0),
            ):
                pass
        except aiohttp.ClientConnectorError:
            print(f"\n[\033[1;31mFATAL\033[0m] Connection Refused to {self.args.url}! Is the EPP running?")
            sys.exit(1)
        except (asyncio.TimeoutError, aiohttp.ServerConnectionError) as e:
            print(f"\n[\033[1;31mFATAL\033[0m] Cannot reach {self.args.url} ({type(e).__name__}). Is the EPP running?")
            sys.exit(1)
        except aiohttp.ClientError:
            pass  # Server responded with something odd, but it is reachable.

    async def _send_request(self, tenant: Tenant) -> None:
        """Constructs and executes a streaming LLM request, injecting FlowKeys."""
        random_prefix = "".join(random.choices(string.ascii_letters + string.digits, k=20))
        n = max(1, self.isl // self.tokens_per_phrase)
        prompt = random_prefix + " " + " ".join([self.base_phrase] * n)
        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "max_tokens": self.osl,
            "stream": True,
            "ignore_eos": True,
        }

        # Apply the FlowKey. Objective maps to the InferenceObjective CRD name.
        headers = {
            "x-gateway-inference-fairness-id": tenant.fairness_id,
            "x-gateway-inference-objective": tenant.inference_objective,
        }

        start_time = time.monotonic()
        ttft: Optional[float] = None
        status_str = "Unknown"

        self.metrics.record_start(tenant.fairness_id)

        try:
            async with self.session.post(
                self.args.url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.REQUEST_TIMEOUT),
            ) as resp:
                if resp.status == 200:
                    status_str = "200"
                    # Stream the body to completion; TTFT is the first byte off the wire.
                    async for _chunk in resp.content.iter_any():
                        if ttft is None:
                            ttft = time.monotonic() - start_time
                else:
                    msg = (await resp.text()).lower()
                    if resp.status == 503 or "timed out" in msg:
                        status_str = "503 (TTL Evict)"
                    elif resp.status == 429 or "rejected" in msg:
                        status_str = "429 (Capacity Rej)"
                    else:
                        status_str = f"{resp.status}"

        except asyncio.TimeoutError:
            status_str = "Timeout (Read)"
        except aiohttp.ClientConnectorError as e:
            status_str = "Error (Conn Refused)" if isinstance(getattr(e, "os_error", None), ConnectionRefusedError) else f"Error ({type(e).__name__})"
        except asyncio.CancelledError:
            # Aborting (Ctrl+C or end-of-narrative drain); still record the attempt.
            self.metrics.record(tenant.fairness_id, "Cancelled", ttft, time.monotonic() - start_time)
            raise
        except aiohttp.ClientError as e:
            status_str = f"Error ({type(e).__name__})"
        except Exception as e:
            status_str = f"Error ({type(e).__name__})"

        duration = time.monotonic() - start_time
        self.metrics.record(tenant.fairness_id, status_str, ttft, duration)

    def _spawn(self, tenant: Tenant, local: Set["asyncio.Task"]) -> None:
        """Launches one request task, tracking it both per-worker and globally."""
        task = asyncio.create_task(self._send_request(tenant))
        local.add(task)
        self.inflight.add(task)

        def _done(t: "asyncio.Task") -> None:
            local.discard(t)
            self.inflight.discard(t)

        task.add_done_callback(_done)

    async def run_tenant_worker(
        self, tenant: Tenant, stages: List[Stage], stop_event: asyncio.Event
    ) -> None:
        """
        Per-flow coroutine running a closed-loop controller that keeps the stage's
        target number of requests in flight: whenever a request completes, a fresh
        one is issued to top the pool back up to target.
        """
        start_time = time.monotonic()
        total_duration = sum(s.duration_sec for s in stages)
        end_time = start_time + total_duration

        # Seconds spent linearly ramping concurrency from the previous stage's
        # target to the new one at each stage boundary (and from 0 at startup).
        RAMP_SEC = 5.0

        local: Set["asyncio.Task"] = set()

        while time.monotonic() < end_time and not stop_event.is_set():
            elapsed = time.monotonic() - start_time

            # Determine the current stage (and when it began) based on elapsed time.
            accumulated = 0.0
            stage_start = 0.0
            current_idx = len(stages) - 1
            for i, stage in enumerate(stages):
                accumulated += stage.duration_sec
                if elapsed < accumulated:
                    current_idx = i
                    break
                stage_start = accumulated

            current_stage = stages[current_idx]
            target = current_stage.concurrency_targets.get(tenant.fairness_id, 0)

            # Gradually ramp concurrency from the previous stage's target up to
            # the new target over the first RAMP_SEC seconds of the stage, so a
            # transition eases in rather than stepping abruptly. The first stage
            # ramps up from zero.
            prev_target = (
                stages[current_idx - 1].concurrency_targets.get(tenant.fairness_id, 0)
                if current_idx > 0 else 0
            )
            ramp = min(1.0, (elapsed - stage_start) / RAMP_SEC)
            target = round(prev_target + (target - prev_target) * ramp)

            # Below target concurrency: launch requests to top the pool back up.
            while len(local) < target:
                self._spawn(tenant, local)

            # Holding the target concurrency; idle briefly before re-checking.
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.01)
                break  # stop_event fired
            except asyncio.TimeoutError:
                pass


# ==============================================================================
# 4. PLAYBOOK CONFIGURATION (The Narrative)
# ==============================================================================

def build_playbook(args: argparse.Namespace) -> Tuple[List[Tenant], List[Stage]]:
    """
    Defines the actors (Flows) and the narrative timeline (Stages).

    Concurrency targets are the number of requests each tenant holds in flight,
    keyed by the tenant's fairness_id. The deployment is sized for `args.capacity`
    concurrent requests total.
    """
    tenants = [
        Tenant(
            fairness_id="premium-tenant",
            inference_objective="premium-traffic",
            priority=100,
        ),
        Tenant(
            fairness_id="standard-tenant",
            inference_objective="standard-traffic",
            priority=0,
        ),
    ]

    DURATION = 60

    stages = [
        Stage(
            "1. 8 Concurrent Each --- should make progress on both", DURATION, {
                "premium-tenant": 8,
                "standard-tenant": 8,
            }
        ),
        Stage(
            "2. 12 Concurrent Each --- should make progress on premium, some progress on standard", DURATION, {
                "premium-tenant": 12,
                "standard-tenant": 12,
            }
        ),
        Stage(
            "3. 16 Concurrent Each --- should make progress only on premium", DURATION, {
                "premium-tenant": 16,
                "standard-tenant": 16,
            }
        ),
    ]

    return tenants, stages


# ==============================================================================
# 5. CLI DASHBOARD & ENTRYPOINT
# ==============================================================================

def draw_dashboard(is_first_render: bool, elapsed: float, total: float, stage: Stage, tenants: List[Tenant], metrics: MetricsCollector, capacity: float) -> None:
    """Renders the real-time terminal UI."""
    target_conc = sum(stage.concurrency_targets.values())
    bp_status = "\033[1;31mSATURATED\033[0m" if target_conc > capacity else "\033[1;32mHEALTHY\033[0m"

    # Reposition terminal cursor exactly to the top of the dashboard
    num_lines = len(tenants) + 6
    if not is_first_render:
        sys.stdout.write(f"\033[{num_lines}A")

    # Buffer lines to prevent terminal flicker
    out = []
    out.append(f"\033[K\033[1;36m[{int(elapsed):03d}s / {int(total):03d}s] {stage.name}\033[0m")
    out.append(f"\033[KSaturation State: {bp_status} | Target Concurrency: {int(target_conc)} (Deployment Capacity: {int(capacity)})")
    out.append("\033[K" + "-" * 145)
    out.append(f"\033[K{'FLOW (FAIRNESS ID)':<20} | {'PRI':<3} | {'TARGET CONC':<11} | {'ACT CONC':<8} | {'QPS':<8} | {'MED TTFT':<10} | {'P90 TTFT':<10} | {'MED TOTAL':<10} | {'P90 TOTAL':<10} | {'200s':<5} | {'429s':<5} | {'503s':<5} | {'ERRs':<5}")
    out.append("\033[K" + "-" * 145)

    for t in sorted(tenants, key=lambda x: x.priority, reverse=True):
        t_conc = stage.concurrency_targets.get(t.fairness_id, 0)
        med_ttft, p90_ttft, med_dur, p90_dur, achieved_qps, s_200, s_429, s_503, s_err, active_concurr = metrics.get_realtime_stats(t.fairness_id)

        # Color code TTFT based on latency to highlight queueing visually
        if p90_ttft is not None:
            # Under flow control, TTFT acts as our queue indicator.
            ttft_color = "\033[1;32m" if p90_ttft < 2.0 else "\033[1;33m" if p90_ttft < 10.0 else "\033[1;31m"
            ttft_str = f"{ttft_color}{p90_ttft:<9.2f}s\033[0m"
        else:
            ttft_str = "  -       "

        if med_ttft is not None:
            med_ttft_color = "\033[1;32m" if med_ttft < 2.0 else "\033[1;33m" if med_ttft < 10.0 else "\033[1;31m"
            med_ttft_str = f"{med_ttft_color}{med_ttft:<9.2f}s\033[0m"
        else:
            med_ttft_str = "  -       "

        dur_str = f"{p90_dur:<9.2f}s" if p90_dur is not None else "  -       "
        med_dur_str = f"{med_dur:<9.2f}s" if med_dur is not None else "  -       "

        out.append(f"\033[K{t.fairness_id:<20} | {t.priority:<3} | {t_conc:<11} | {active_concurr:<8} | {achieved_qps:<8.1f} | {med_ttft_str} | {ttft_str} | {med_dur_str} | {dur_str} | {s_200:<5} | {s_429:<5} | {s_503:<5} | {s_err:<5}")

    out.append("\033[K" + "-" * 145)

    # Render entire buffer at once.
    sys.stdout.write("\n".join(out) + "\n")
    sys.stdout.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gateway API Inference Extension EPP - Flow Control Demo",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # The target IP is injected via the EPP_IP environment variable (e.g. the
    # EPP service clusterIP set by the Justfile). Falls back to localhost.
    default_url = f"http://{os.environ.get('EPP_IP', 'localhost')}:80/v1/completions"

    parser.add_argument("--url", default=default_url, help="Target gateway completions endpoint.")
    parser.add_argument("--capacity", type=int, default=16, help="Deployment concurrency capacity (for the saturation banner).")
    parser.add_argument("--model", default=os.environ.get("MODEL_NAME", "default"), help="Model / InferenceObjective name sent in the payload.")
    return parser.parse_args()


async def run_demo(args: argparse.Namespace) -> None:
    print(f"URL={args.url!r}")

    metrics = MetricsCollector()
    tenants, stages = build_playbook(args)
    total_duration = sum(s.duration_sec for s in stages)

    stop_event = asyncio.Event()
    workers: List["asyncio.Task"] = []
    is_first_render = True
    aborted = False

    # One shared session with an unbounded connector so the load generator can
    # hold the full target concurrency without throttling itself client-side.
    connector = aiohttp.TCPConnector(limit=0)
    session = aiohttp.ClientSession(connector=connector)
    generator = LoadGenerator(args, metrics, args.model, session)

    try:
        await generator.verify_connectivity()

        print("\033[2J\033[H", end="")
        print("┌─────────────────────────────────────────────────────────────┐")
        print("│ EPP Flow Control Layer: Multi-Tenancy & QoS Simulator       │")
        print("└─────────────────────────────────────────────────────────────┘")
        print(f"Deployment Concurrency Capacity: {args.capacity} concurrent requests\n")

        # 1. Start background flow generators (one coroutine per tenant).
        workers = [
            asyncio.create_task(generator.run_tenant_worker(tenant, stages, stop_event))
            for tenant in tenants
        ]

        start_time = time.monotonic()
        # -1 is the termination sentinel. Crossing into it starts a fresh
        # dashboard block (the drain gets its own row), but metrics are NOT
        # reset so the drain stays cumulative with the last stage.
        prev_stage_idx: Optional[int] = None

        # 2. Main UI update loop.
        while True:
            elapsed = time.monotonic() - start_time
            total_active = sum(metrics.get_realtime_stats(t.fairness_id)[9] for t in tenants)

            if elapsed >= total_duration:
                # Narratives are done, signal workers to immediately stop injecting traffic.
                stop_event.set()
                # Retain the final stage's targets so the drain frame keeps the
                # previous stage's TARGET CONC column and saturation banner intact
                # while ACT CONC drains to zero (rather than resetting to 0/HEALTHY).
                current_stage = Stage(f"5. Terminated (Draining - {int(elapsed - total_duration)}s)", 12, stages[-1].concurrency_targets)
                stage_idx = -1

                if total_active == 0 and elapsed >= total_duration + 12.0:
                    break
                elif elapsed >= total_duration + 95.0:
                    break

            else:
                current_stage = stages[-1]
                stage_idx = len(stages) - 1
                accum = 0
                for i, s in enumerate(stages):
                    accum += s.duration_sec
                    if elapsed < accum:
                        current_stage = s
                        stage_idx = i
                        break

            # On any stage transition (including into the terminated drain),
            # leave the previous dashboard in place as a log entry and start a
            # new block. Only reset metrics between real stages; the drain frame
            # keeps the final stage's cumulative metrics intact.
            if prev_stage_idx is not None and stage_idx != prev_stage_idx:
                sys.stdout.write("\n")
                sys.stdout.flush()
                is_first_render = True
                if stage_idx >= 0:
                    metrics.reset()

            prev_stage_idx = stage_idx

            draw_dashboard(is_first_render, elapsed, total_duration + 95.0, current_stage, tenants, metrics, args.capacity)
            is_first_render = False
            await asyncio.sleep(0.5)

        # 3. Graceful Exit
        print("\n\nTest narrative complete. Awaiting socket terminations for any straggling requests...")

    except asyncio.CancelledError:
        # Propagated from asyncio.run() on Ctrl+C.
        aborted = True
        print("\n\n[\033[1;33mABORT\033[0m] Caught Ctrl+C. Force stopping workers...")

    finally:
        # Stop the workers from issuing new traffic, then drain/cancel in-flight tasks.
        stop_event.set()
        for w in workers:
            w.cancel()
        for task in list(generator.inflight):
            task.cancel()
        await asyncio.gather(*workers, *generator.inflight, return_exceptions=True)
        await session.close()

    if aborted:
        # Re-raise so asyncio.run() exits with the interrupted status.
        raise asyncio.CancelledError


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run_demo(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


if __name__ == "__main__":
    main()
