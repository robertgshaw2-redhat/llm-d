# Hybrid saturation detection (queue depth + concurrency)

This directory carries everything needed to gate flow-control admission on the
**maximum of two independent saturation signals** — in-flight concurrency and
scraped queue depth — instead of a single detector:

| File | Purpose |
| --- | --- |
| `0001-feat-flowcontrol-add-max-saturation-detector-composi.patch` | Adds the `max-saturation-detector` plugin to llm-d-router (formerly llm-d-inference-scheduler) |
| `hybrid-saturation.values.yaml` | P/D values wiring `max(pd-concurrency, pd-queue)` as the flow-control saturation detector |

## What the plugin does

`max-saturation-detector` is a composite `SaturationDetector`. Its one parameter,
`detectors`, names child detector plugins; `Saturation()` returns the maximum of
the children's values. Because the flow controller evaluates saturation per stage
and takes `max(prefill, decode)` as the effective signal, the result is
`max(prefill, decode)` over `max(concurrency, queue)` with no extra wiring.

Two deliberate non-features:

- **No `Filter`**: the config loader only auto-injects a gating detector into
  scheduling profiles when it implements `Filter`. Per-endpoint filtering stays
  with the children, which the profiles list explicitly.
- **No `Consumes()`**: the children are configured plugins themselves, so the
  framework validates their data dependencies directly. (Declaring the union on
  the composite trips the data graph's layer-order check, which places a plugin
  with no scheduling/requestcontrol interface before every producer.)

It also adds a `llm_d_epp_flow_control_detector_saturation{detector=...}` gauge,
recorded for each child on every evaluation, so you can tell whether concurrency
or queue depth is the signal limiting dispatch —
`flow_control_pool_saturation{stage=...}` alone only shows the max.

## Build the custom EPP image

The patch applies to [llm-d-router](https://github.com/llm-d/llm-d-router) at
commit `d242a2ec` (it should also apply to nearby commits; resolve normally if
it drifts):

```bash
git clone https://github.com/llm-d/llm-d-router.git
cd llm-d-router
git checkout d242a2ec
git am path/to/0001-feat-flowcontrol-add-max-saturation-detector-composi.patch

# Sanity check
go test ./pkg/epp/framework/plugins/flowcontrol/saturationdetector/... ./cmd/epp/... ./pkg/epp/metrics/...

# Build and push the EPP image (uses Dockerfile.epp)
make image-build-epp image-push-epp \
  IMAGE_REGISTRY=<your-registry> \
  EPP_TAG=hybrid-saturation-d242a2ec
```

Then set `router.epp.image.{registry,repository,tag}` in
`hybrid-saturation.values.yaml` to match what you pushed, and deploy the router
chart with that values file in place of `../pd-disaggregation.values.yaml`.

## Config invariants (will fail fast if violated)

- **Children before the composite** in `plugins:` — the factory resolves
  `detectors` by name at instantiation time and errors on a missing name.
- Every name in `detectors` must implement `SaturationDetector`; wrong-type or
  duplicate references are rejected at startup.
- The plugin registers at **Beta** stability (same as the existing detectors),
  so no `--allow-experimental-plugins` flag is needed.

## Operational notes

- **Stale metrics still halt dispatch.** `pd-queue` makes queue depth a real
  admission gate: under the default `stalenessPolicy: saturated`, a stalled
  metrics scrape scores every endpoint as saturated and stops dispatch. Watch
  `llm_d_epp_flow_control_stale_endpoints`, and set `stalenessPolicy: ignore`
  on `pd-queue` to fail open instead.
- **In-flight eviction pacing** (`deriveEvictionConfirmationGrace`) special-cases
  only the plain `concurrency-detector` type; a composite falls into the
  conservative scraped-sensor branch (`refresh interval + staleness threshold +
  reclaim budget`). That is the safe choice for a composite that includes a
  scraped child, and it only matters if in-flight eviction is enabled (it is not
  in this values file).
- The per-detector gauge, like `flow_control_stale_endpoints`, carries no stage
  label and is overwritten on every evaluation, so it reflects the most recently
  evaluated stage.

## Verification

The plugin ships with unit tests (max-combining, single-child passthrough,
missing/wrong-type/duplicate/empty child errors, and a test pinning the
no-`Filter`/no-`Consumes` design). The exact `pd-config.yaml` embedded in
`hybrid-saturation.values.yaml` was additionally validated end-to-end through
the EPP's production config path (`parseConfigurationPhaseOne` →
`parseConfigurationPhaseTwo`), confirming it loads and wires `pd-saturation`
as the flow-control saturation detector.

## Upstreaming

Nothing upstream conflicts with this (no composite detector exists and no open
PR proposes one), so the patch is a candidate for an llm-d-router PR; once it
merges into a released EPP image, drop the `router.epp.image` override and this
patch, keeping only the values file.
