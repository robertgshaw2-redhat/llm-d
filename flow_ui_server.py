#!/usr/bin/env python3
"""
Flow Control Demo - Interactive Web UI
=================================================================

A live, browser-driven front end for the load generator in ``client.py``.
Instead of running through a fixed narrative of stages, this server is driven
interactively from the browser and supports two live-switchable modes:

  * Concurrency (closed loop): hold an adjustable number of requests in flight
    per tenant -- drag the concurrency up and down.
  * QPS (open loop): issue an adjustable number of requests per second per
    tenant regardless of how many are already in flight.

Either way you watch latency move over a trailing time window.

It reuses the exact same building blocks as the CLI demo -- ``Tenant``,
``MetricsCollector`` and ``LoadGenerator`` from ``client.py`` -- so the traffic
shape (5000 ISL / 100 OSL streaming completions, FlowKey headers, single shared
aiohttp session) is identical. The only thing that changes is the *control
loop*: the per-tenant concurrency target is a mutable value driven by the UI
rather than a pre-baked Stage timeline.

Everything runs in one asyncio event loop: the aiohttp web server and the
load-generator coroutines share the loop, so there are no threads or locks. The
browser polls ``/api/stats`` a few times a second and renders a self-contained
canvas chart (no external JS, works offline).

Prerequisites:
    Python 3.9+ and aiohttp (pip install aiohttp)

Usage:
    python3 flow_ui_server.py --url http://localhost:80/v1/completions --capacity 16
    # then open http://localhost:8080
"""

import argparse
import asyncio
import os
import time
from typing import Dict, List, Set

import aiohttp
from aiohttp import web

# Reuse the load-generation engine and metrics from the CLI demo verbatim so the
# two tools drive traffic identically.
from client import LoadGenerator, MetricsCollector, Tenant


# ==============================================================================
# 1. TENANTS
# ==============================================================================
# Same two flows as the CLI playbook: a high-priority "premium" flow and a
# best-effort "standard" flow. The UI exposes one slider per flow (driving
# either concurrency or QPS depending on the selected mode).

def build_tenants() -> List[Tenant]:
    return [
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


# ==============================================================================
# 2. TRAILING-WINDOW STATS
# ==============================================================================
# client.MetricsCollector aggregates over a whole stage. For an interactive,
# never-ending session we instead want a *trailing* window so the numbers track
# whatever concurrency you've currently dialed in. The collector already stores
# (timestamp, value) tuples in ttft_window / duration_window and timestamps in
# completion_times, so we just filter those by time here.


def _percentile(sorted_vals: List[float], q: float):
    """Nearest-rank-ish percentile matching client.py's index convention."""
    if not sorted_vals:
        return None
    if q <= 0:
        return sorted_vals[0]
    idx = min(len(sorted_vals) - 1, int(len(sorted_vals) * q))
    return sorted_vals[idx]


def window_stats(metrics: MetricsCollector, fid: str, window_sec: float, now: float) -> dict:
    """Compute trailing-window latency/throughput stats for one tenant."""
    cutoff = now - window_sec

    ttfts = sorted(v for ts, v in metrics.ttft_window[fid] if ts >= cutoff)
    durs = sorted(v for ts, v in metrics.duration_window[fid] if ts >= cutoff)
    comps = [ts for ts in metrics.completion_times[fid] if ts >= cutoff]

    # Throughput over the window (guard against a degenerate tiny span).
    qps = 0.0
    if len(comps) > 1:
        span = max(now - comps[0], 0.1)
        qps = len(comps) / span

    counts = metrics.status_counts[fid]
    s_200 = counts.get("200", 0)
    s_429 = sum(c for k, c in counts.items() if "429" in str(k))
    s_503 = sum(c for k, c in counts.items() if "503" in str(k))
    s_err = sum(
        c for k, c in counts.items()
        if "200" not in str(k) and "429" not in str(k) and "503" not in str(k)
    )

    return {
        "med_ttft": _percentile(ttfts, 0.5),
        "p95_ttft": _percentile(ttfts, 0.95),
        "max_ttft": _percentile(ttfts, 1.0),
        "med_total": _percentile(durs, 0.5),
        "p95_total": _percentile(durs, 0.95),
        "max_total": _percentile(durs, 1.0),
        "qps": qps,
        "samples": len(ttfts),
        "active": metrics.active_requests[fid],
        # Cumulative since process start (or last reset) -- useful for spotting
        # rejections/evictions as you push past capacity.
        "s_200": s_200,
        "s_429": s_429,
        "s_503": s_503,
        "s_err": s_err,
    }


# ==============================================================================
# 3. INTERACTIVE LOAD CONTROLLER
# ==============================================================================
# One coroutine per tenant. It supports two driving modes, switchable live from
# the UI via the shared ``control`` dict:
#
#   * "concurrency" (closed loop) -- keep ``control["targets"][fid]`` requests in
#     flight. Raising the target spawns more immediately; lowering it lets the
#     surplus drain naturally (we never cancel in-flight work to shrink).
#
#   * "qps" (open loop) -- issue ``control["rates"][fid]`` requests per second
#     regardless of how many are already in flight. A token-bucket "credit"
#     accumulator paces fractional rates smoothly: each tick adds rate*dt credit
#     and every whole credit spawns one request. If the backend can't keep up,
#     in-flight work simply piles up (that's the overload signal in this mode).

TICK_SEC = 0.02  # control-loop granularity (50 Hz); also QPS pacing resolution


async def run_interactive_worker(
    gen: LoadGenerator,
    tenant: Tenant,
    control: dict,
    stop_event: asyncio.Event,
) -> None:
    fid = tenant.fairness_id
    local: Set["asyncio.Task"] = set()
    last = time.monotonic()
    credit = 0.0  # fractional requests owed in QPS mode
    while not stop_event.is_set():
        now = time.monotonic()
        dt = now - last
        last = now

        if control["mode"] == "qps":
            rate = max(0.0, float(control["rates"].get(fid, 0.0)))
            credit += rate * dt
            # Cap pending credit so a stall/idle period can't later unleash a
            # burst: allow at most ~1s of catch-up (or a single request).
            credit = min(credit, max(rate, 1.0))
            while credit >= 1.0:
                gen._spawn(tenant, local)
                credit -= 1.0
        else:  # "concurrency"
            credit = 0.0
            target = max(0, int(control["targets"].get(fid, 0)))
            # Top the in-flight pool back up to target. If target dropped below
            # the current count we simply don't spawn; completed tasks aren't
            # replaced.
            while len(local) < target:
                gen._spawn(tenant, local)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=TICK_SEC)
            break
        except asyncio.TimeoutError:
            pass


async def prune_loop(metrics: MetricsCollector, tenants: List[Tenant], max_window: float, stop_event: asyncio.Event) -> None:
    """Trim timestamped sample buffers so a long-running session stays bounded.

    Status counts are left cumulative on purpose (the UI shows them as running
    totals); only the per-sample latency/throughput buffers are trimmed.
    """
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=2.0)
            break
        except asyncio.TimeoutError:
            pass
        cutoff = time.monotonic() - max_window
        for t in tenants:
            fid = t.fairness_id
            metrics.ttft_window[fid] = [(ts, v) for ts, v in metrics.ttft_window[fid] if ts >= cutoff]
            metrics.duration_window[fid] = [(ts, v) for ts, v in metrics.duration_window[fid] if ts >= cutoff]
            metrics.completion_times[fid] = [ts for ts in metrics.completion_times[fid] if ts >= cutoff]


# ==============================================================================
# 4. HTTP ROUTES
# ==============================================================================

HERE = os.path.dirname(os.path.abspath(__file__))
MAX_BUFFER_WINDOW = 120.0  # seconds of latency samples retained for windowing


async def handle_index(request: web.Request) -> web.Response:
    path = os.path.join(HERE, "flow_ui.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return web.Response(text=f.read(), content_type="text/html")
    except FileNotFoundError:
        return web.Response(status=500, text="flow_ui.html not found next to flow_ui_server.py")


async def handle_config(request: web.Request) -> web.Response:
    app = request.app
    return web.json_response({
        "url": app["args"].url,
        "model": app["args"].model,
        "capacity": app["args"].capacity,
        "max_concurrency": app["args"].max_concurrency,
        "max_qps": app["args"].max_qps,
        "mode": app["control"]["mode"],
        "tenants": [
            {"fairness_id": t.fairness_id, "objective": t.inference_objective, "priority": t.priority}
            for t in app["tenants"]
        ],
    })


async def handle_stats(request: web.Request) -> web.Response:
    app = request.app
    metrics: MetricsCollector = app["metrics"]
    tenants: List[Tenant] = app["tenants"]
    control: dict = app["control"]
    targets: Dict[str, int] = control["targets"]
    rates: Dict[str, float] = control["rates"]
    mode: str = control["mode"]

    try:
        win = float(request.query.get("window", "10"))
    except ValueError:
        win = 10.0
    win = max(1.0, min(win, MAX_BUFFER_WINDOW))

    now = time.monotonic()
    per_tenant = []
    total_target = 0
    total_target_qps = 0.0
    total_active = 0
    total_qps = 0.0
    for t in tenants:
        s = window_stats(metrics, t.fairness_id, win, now)
        tgt = int(targets.get(t.fairness_id, 0))
        tgt_qps = float(rates.get(t.fairness_id, 0.0))
        total_target += tgt
        total_target_qps += tgt_qps
        total_active += s["active"]
        total_qps += s["qps"]
        per_tenant.append({
            "fairness_id": t.fairness_id,
            "objective": t.inference_objective,
            "priority": t.priority,
            "target": tgt,
            "target_qps": tgt_qps,
            **s,
        })

    capacity = app["args"].capacity
    # Saturation signal depends on the mode. In closed-loop concurrency mode the
    # target itself can exceed deployment capacity. In open-loop QPS mode the
    # backpressure shows up as in-flight requests piling past capacity.
    saturated = (total_active > capacity) if mode == "qps" else (total_target > capacity)
    return web.json_response({
        "now": now,
        "ts_ms": int(time.time() * 1000),
        "window": win,
        "mode": mode,
        "capacity": capacity,
        "total_target": total_target,
        "total_target_qps": total_target_qps,
        "total_active": total_active,
        "total_qps": total_qps,
        "saturated": saturated,
        "tenants": per_tenant,
    })


async def handle_set_concurrency(request: web.Request) -> web.Response:
    app = request.app
    targets: Dict[str, int] = app["control"]["targets"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

    fid = body.get("fairness_id")
    if fid not in targets:
        return web.json_response({"error": f"unknown fairness_id {fid!r}"}, status=400)
    try:
        target = int(body.get("target"))
    except (TypeError, ValueError):
        return web.json_response({"error": "target must be an int"}, status=400)

    target = max(0, min(target, app["args"].max_concurrency))
    targets[fid] = target
    return web.json_response({"fairness_id": fid, "target": target})


async def handle_set_qps(request: web.Request) -> web.Response:
    app = request.app
    rates: Dict[str, float] = app["control"]["rates"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

    fid = body.get("fairness_id")
    if fid not in rates:
        return web.json_response({"error": f"unknown fairness_id {fid!r}"}, status=400)
    try:
        target = float(body.get("target"))
    except (TypeError, ValueError):
        return web.json_response({"error": "target must be a number"}, status=400)

    target = max(0.0, min(target, float(app["args"].max_qps)))
    rates[fid] = target
    return web.json_response({"fairness_id": fid, "target": target})


async def handle_set_mode(request: web.Request) -> web.Response:
    app = request.app
    control: dict = app["control"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

    mode = body.get("mode")
    if mode not in ("concurrency", "qps"):
        return web.json_response({"error": "mode must be 'concurrency' or 'qps'"}, status=400)
    control["mode"] = mode
    return web.json_response({"mode": mode})


async def handle_reset(request: web.Request) -> web.Response:
    """Clear cumulative counters and latency buffers without touching targets."""
    app = request.app
    metrics: MetricsCollector = app["metrics"]
    metrics.reset()
    return web.json_response({"ok": True})


# ==============================================================================
# 5. LIFECYCLE
# ==============================================================================

async def on_startup(app: web.Application) -> None:
    args = app["args"]
    metrics = MetricsCollector()
    tenants = build_tenants()
    # Shared, mutable control surface driven by the UI. Both the per-tenant
    # concurrency targets and QPS rates live here so switching modes preserves
    # the other mode's dialed-in values; `mode` selects which one is active.
    control: dict = {
        "mode": "concurrency",
        "targets": {t.fairness_id: 0 for t in tenants},
        "rates": {t.fairness_id: 0.0 for t in tenants},
    }

    connector = aiohttp.TCPConnector(limit=0)
    session = aiohttp.ClientSession(connector=connector)
    generator = LoadGenerator(args, metrics, args.model, session)

    # Best-effort connectivity probe -- warn but keep serving so the UI can come
    # up before the backend is ready.
    try:
        async with session.post(
            args.url,
            json={"model": args.model, "prompt": ""},
            timeout=aiohttp.ClientTimeout(total=2.0),
        ):
            pass
        print(f"[ok] reached {args.url}")
    except Exception as e:  # noqa: BLE001 -- informational only
        print(f"[warn] could not reach {args.url} yet ({type(e).__name__}); the UI will still start.")

    stop_event = asyncio.Event()
    workers = [
        asyncio.create_task(run_interactive_worker(generator, t, control, stop_event))
        for t in tenants
    ]
    pruner = asyncio.create_task(prune_loop(metrics, tenants, MAX_BUFFER_WINDOW, stop_event))

    app["metrics"] = metrics
    app["tenants"] = tenants
    app["control"] = control
    app["session"] = session
    app["generator"] = generator
    app["stop_event"] = stop_event
    app["workers"] = workers
    app["pruner"] = pruner


async def on_cleanup(app: web.Application) -> None:
    stop_event: asyncio.Event = app["stop_event"]
    generator: LoadGenerator = app["generator"]
    session: aiohttp.ClientSession = app["session"]

    stop_event.set()
    for w in app["workers"]:
        w.cancel()
    app["pruner"].cancel()
    for task in list(generator.inflight):
        task.cancel()
    await asyncio.gather(
        *app["workers"], app["pruner"], *generator.inflight, return_exceptions=True
    )
    await session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flow Control Demo - Interactive Web UI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    default_url = f"http://{os.environ.get('EPP_IP', 'localhost')}:80/v1/completions"
    parser.add_argument("--url", default=default_url, help="Target gateway completions endpoint.")
    parser.add_argument("--capacity", type=int, default=16, help="Deployment concurrency capacity (for the saturation banner).")
    parser.add_argument("--model", default=os.environ.get("MODEL_NAME", "default"), help="Model / InferenceObjective name sent in the payload.")
    parser.add_argument("--host", default="0.0.0.0", help="Web UI bind host.")
    parser.add_argument("--port", type=int, default=8080, help="Web UI port.")
    parser.add_argument("--max-concurrency", dest="max_concurrency", type=int, default=32, help="Upper bound for the concurrency sliders.")
    parser.add_argument("--max-qps", dest="max_qps", type=float, default=2.0, help="Upper bound for the QPS sliders.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = web.Application()
    app["args"] = args
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/config", handle_config)
    app.router.add_get("/api/stats", handle_stats)
    app.router.add_post("/api/concurrency", handle_set_concurrency)
    app.router.add_post("/api/qps", handle_set_qps)
    app.router.add_post("/api/mode", handle_set_mode)
    app.router.add_post("/api/reset", handle_reset)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    print(f"Flow Control UI -> http://{args.host}:{args.port}  (target {args.url}, capacity {args.capacity})")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
