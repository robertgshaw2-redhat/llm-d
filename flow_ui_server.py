#!/usr/bin/env python3
"""
Flow Control Demo - Interactive Web UI
=================================================================

A live, browser-driven front end for the closed-loop load generator in
``client.py``. Instead of running through a fixed narrative of stages, this
server holds an *adjustable* number of concurrent requests in flight per tenant
and lets you drag the concurrency up and down from the browser while watching
latency move over a trailing time window.

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
# best-effort "standard" flow. The UI exposes one concurrency slider per flow.

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
        "p90_ttft": _percentile(ttfts, 0.9),
        "p95_ttft": _percentile(ttfts, 0.95),
        "med_total": _percentile(durs, 0.5),
        "p90_total": _percentile(durs, 0.9),
        "p95_total": _percentile(durs, 0.95),
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
# One coroutine per tenant that keeps `targets[fid]` requests in flight. The
# target is a plain mutable int updated by the UI; raising it spawns more
# in-flight requests immediately, lowering it lets the surplus drain naturally
# (closed-loop ramp-down -- we never cancel in-flight work to shrink).


async def run_interactive_worker(
    gen: LoadGenerator,
    tenant: Tenant,
    targets: Dict[str, int],
    stop_event: asyncio.Event,
) -> None:
    local: Set["asyncio.Task"] = set()
    while not stop_event.is_set():
        target = max(0, int(targets.get(tenant.fairness_id, 0)))
        # Top the in-flight pool back up to target. If target dropped below the
        # current count we simply don't spawn; completed tasks aren't replaced.
        while len(local) < target:
            gen._spawn(tenant, local)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=0.02)
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
        "tenants": [
            {"fairness_id": t.fairness_id, "objective": t.inference_objective, "priority": t.priority}
            for t in app["tenants"]
        ],
    })


async def handle_stats(request: web.Request) -> web.Response:
    app = request.app
    metrics: MetricsCollector = app["metrics"]
    tenants: List[Tenant] = app["tenants"]
    targets: Dict[str, int] = app["targets"]

    try:
        win = float(request.query.get("window", "10"))
    except ValueError:
        win = 10.0
    win = max(1.0, min(win, MAX_BUFFER_WINDOW))

    now = time.monotonic()
    per_tenant = []
    total_target = 0
    total_active = 0
    for t in tenants:
        s = window_stats(metrics, t.fairness_id, win, now)
        tgt = int(targets.get(t.fairness_id, 0))
        total_target += tgt
        total_active += s["active"]
        per_tenant.append({
            "fairness_id": t.fairness_id,
            "objective": t.inference_objective,
            "priority": t.priority,
            "target": tgt,
            **s,
        })

    capacity = app["args"].capacity
    return web.json_response({
        "now": now,
        "ts_ms": int(time.time() * 1000),
        "window": win,
        "capacity": capacity,
        "total_target": total_target,
        "total_active": total_active,
        "saturated": total_target > capacity,
        "tenants": per_tenant,
    })


async def handle_set_concurrency(request: web.Request) -> web.Response:
    app = request.app
    targets: Dict[str, int] = app["targets"]
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
    targets: Dict[str, int] = {t.fairness_id: 0 for t in tenants}

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
        asyncio.create_task(run_interactive_worker(generator, t, targets, stop_event))
        for t in tenants
    ]
    pruner = asyncio.create_task(prune_loop(metrics, tenants, MAX_BUFFER_WINDOW, stop_event))

    app["metrics"] = metrics
    app["tenants"] = tenants
    app["targets"] = targets
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
    parser.add_argument("--max-concurrency", dest="max_concurrency", type=int, default=64, help="Upper bound for the concurrency sliders.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = web.Application()
    app["args"] = args
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/config", handle_config)
    app.router.add_get("/api/stats", handle_stats)
    app.router.add_post("/api/concurrency", handle_set_concurrency)
    app.router.add_post("/api/reset", handle_reset)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    print(f"Flow Control UI -> http://{args.host}:{args.port}  (target {args.url}, capacity {args.capacity})")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
