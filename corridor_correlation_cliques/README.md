# Corridor Correlation Cliques → 3D Voxel Volume

*Self-contained prototype — ship this folder as-is to the dashboard UI developer.*

---

## 1. What this is

Given hourly **Volume** data (vehicles/hour) for a corridor of intersections over several
weeks, this package answers — **independently for every (weekday, hour-of-day) slot**:

> *"Looking only at this weekday + hour, across the analysis weeks, which intersections
> rise and fall together **week-over-week**, and which of those form spatially contiguous
> correlation cliques?"*

It stacks the per-slot answers into a binary **`intersection × day-of-week × hour-of-day`**
volume (here **6 × 7 × 16**) and renders it as a rotatable **3D voxel view** with 3D
connected-component analysis and largest-cuboid "cores".

```
fixture_data.generate_volume_observations()      ← 12 weeks synthetic hourly Volume
        ↓  (production: a DB query returning the same long-format rows)
   long-format DataFrame  →  data/corridor_volume_12wk.csv
        ↓  slot_cliques.build_clique_volume(df, direction=…, threshold=…)
   6 × 7 × 16 binary volume   (1 = intersection is in a clique for that day-hour slot)
        ↓  voxel_viz.render_voxel_volume(volume)
   3D figure  +  connected components  +  largest cuboids
```

The driving notebook is
[`notebooks/corridor_correlation_cliques_prototype.ipynb`](notebooks/corridor_correlation_cliques_prototype.ipynb).

---

## 2. Where this sits in the larger feature

This is the **front half** of the 3D correlation-clique feature. The earlier voxel
prototype built the **visualization first**, feeding it a *synthetic* random volume
(`generate_synthetic_volume`). This package supplies the **real per-slot clique detector**
that produces that volume from data — closing the gap. The visualization module
(`voxel_viz.py`) is carried here **verbatim** so the folder is complete and stand-alone.

### How it differs from the earlier "grouping" prototype (important)

| | Grouping prototype (`synchrony.grouping`) | This package (`slot_cliques`) |
|---|---|---|
| Correlation unit | **one** ρ per pair, pooled across *all* week × weekday × hour rows | **one** ρ per pair **per (weekday, hour) slot** |
| Question | global synchrony ("do A and B generally track?") | week-over-week co-movement at a *specific* time |
| Samples per ρ | ~70 (many slots pooled) | **N_WEEKS = 12** (one per week at that slot) |
| Output | a few corridor groups | a 6 × 7 × 16 volume (112 slot-level answers) |

The **math is the same** (Spearman ρ → threshold → `networkx` cliques → contiguity); only
the *slicing* of the data differs, which is why a new per-slot loop was needed.

---

## 3. The data contract

Every function consumes a **long-format** table — one row per observation. The CSV
(`data/corridor_volume_12wk.csv`) and `RawObservation` share these columns:

| Column | Type | Notes |
|---|---|---|
| `intersection_id` | str | any string id (here `"1240"`…`"1245"`) |
| `corridor_order` | int | 0-based west→east position; sets the x-axis order |
| `direction` | str | `"LR"` (eastbound/phase2) or `"RL"` (westbound/phase6) |
| `week_id` | int | 1-based week within the window (1…12) |
| `date` | ISO date | calendar date of the observation |
| `weekday` | str | `"Mon"`…`"Sun"` (3-letter title-case) |
| `hour_slot` | str | `"HH:MM-HH:MM"`, one hour, e.g. `"08:00-09:00"` |
| `moe_name` | str | `"volume"` for this prototype |
| `raw_value` | float | measured value (veh/hr) |

`RawObservation` and `Direction` are **local copies** (`schemas.py`) — identical to the
wider prototype's, but with **no import dependency** on it.

---

## 4. Public API

```python
import corridor_correlation_cliques as ccc
```

**Data / fixture**
- `generate_volume_observations() -> list[RawObservation]` — full synthetic dataset (both directions)
- `observations_to_dataframe(rows) -> pd.DataFrame`
- `write_csv(path=DEFAULT_CSV_PATH) -> Path` / `load_csv(path=…) -> pd.DataFrame`
- constants: `ORDERED_INTERSECTIONS`, `INTERSECTION_LABELS`, `DAY_LABELS`, `HOUR_SLOTS`,
  `N_WEEKS`, `MOE_NAME`, `CORRIDOR_ID`, `CORRIDOR_NAME`
- `slot_group(direction, weekday_idx, hour_idx)` — the fixture's **ground-truth** intended
  group per slot (used to validate the detector)

**Per-slot cliques (`slot_cliques`)**
- `slot_weekly_vectors(df, *, direction, weekday, hour_slot, ordered_ids, moe_name="volume")`
  → `{id: 12-vector}`
- `slot_correlation_matrix(vectors, ordered_ids, *, labels_by_id=None)` → symmetric ρ DataFrame
- `detect_slot_cliques(vectors, ordered_ids, *, threshold=0.60, …)` → `SlotCliqueResult`
  (`highly_correlated_pairs`, `cliques`, `groups`, `members`)
- `build_clique_volume(df, ordered_ids, *, direction, weekdays, hour_slots, threshold=0.60, moe_name="volume")`
  → `np.ndarray (n_int, n_days, n_hours)` of 0/1 — **the main entry point**
- `build_slot_results(...)` → `{(weekday, hour_slot): SlotCliqueResult}` for inspection
- `safe_spearman(x, y)`, `contiguous_runs(...)` — the copied math primitives
- `DEFAULT_THRESHOLD = 0.60`

**Visualization (`voxel_viz`, copied verbatim)**
- `render_voxel_volume(volume, *, intersection_labels=…, highlight="components"|"cuboids", style="cubes"|"markers", …)` → `go.Figure`
- `label_connected_components(volume)` → `(labels, list[VoxelComponent])` — 6-connectivity
- `find_component_cuboids(volume)` → largest solid cuboid per region
- `components_to_frame(...)`, `hour_range_label(...)`, `generate_synthetic_volume(...)` (legacy stub, unused)

---

## 5. The synthetic fixture — design rationale

The data is **fully deterministic** (seeded, `_SEED = 2026`) and built so the per-slot
clique detector recovers a clean, direction-aware story. Verified: **0 slot mismatches**
against the intended ground truth in both directions at the default 0.60 threshold.

### Intended groups per regime (the "flip")

| Regime (weekdays Mon–Fri) | LR (eastbound) | RL (westbound) |
|---|---|---|
| AM peak (07–10h, slots 1–3) | {A, B, C} | {D, E, F} |
| PM peak (16–19h, slots 10–12) | {D, E, F} | {A, B, C} |
| Midday (12–13h, slot 6, Wed+Thu) | {B, C, D} | — |
| all other hours / Sat / Sun | — | — |

(A=McIntosh, B=Honore, C=Woodmont, D=Cattleridge, E=Cattlemen, F=I-75 SB Ramp.)

This produces three clean connected regions in the LR volume — an AM block (A–C × Mon–Fri ×
7–10am), a PM block (D–F × Mon–Fri × 4–7pm), and a small midday island (B–D × Wed–Thu ×
12–1pm) — and the mirror-image AM/PM blocks in RL.

### How the cliques are engineered

Two layers per observation:

1. **Base diurnal magnitude** — AM/midday/PM Gaussian peaks × day-of-week scaling ×
   per-intersection level × direction asymmetry (eastbound AM-heavy, westbound PM-heavy).
   This base is **constant across the 12 weeks** — *deliberately*. Because the analysis is
   week-over-week, any signal shared across all weeks (e.g. a global weekly trend) would
   make **every** intersection correlate and everything would clique. Keeping the base flat
   across weeks means correlation is created *only* where we inject it.

2. **Weekly anomaly** — a small (±~10%) week-to-week wobble layered on top; this is the
   *only* week-varying part, so it alone drives the Spearman. For each slot, the intended
   group's members **share one weekly driver pattern** (plus a little independent jitter, so
   they're highly correlated ≈ 0.9–0.98 but not identical); every other intersection gets
   its **own** driver, drawn by greedy rejection sampling so the drivers are mutually
   near-uncorrelated (`|Spearman| ≤ 0.30`). Result: only the intended group is mutually
   highly correlated; no spurious cliques.

### Why this is mathematically clean

Spearman depends only on the *rank order* of each intersection's 12 weekly values. Since
the base is constant across weeks, the ranks are fixed entirely by the anomaly, and the
value transform `value = base · (1 + frac · anomaly)` is monotonic for `base > 0`. So
**Spearman(values) = Spearman(anomalies)** exactly — the designed driver structure shows up
unchanged in the detector's output.

Tuning knobs (top of `fixture_data.py`): `_ANOMALY_FRAC` (0.10), `_JITTER_MIX` (0.12),
`_DRIVER_MAX_ABS` (0.30), `_SEED` (2026), plus the diurnal / day-of-week / per-intersection
magnitude tables.

---

## 6. Design notes / decisions (from the build discussion)

- **12 weeks.** Per-slot ρ uses N_WEEKS = 12 samples (one per week at that slot). The
  product will eventually have ~a year of data; 12 is enough for a clean prototype and
  stable-enough correlations. Increase `N_WEEKS` for more statistical stability.
- **Directional.** Volume is a directional count; LR and RL are separate streams, analyzed
  separately — one volume per direction, exactly like the rest of the prototype.
- **Normalization is skipped.** The wider pipeline min-max normalizes before correlating,
  but Spearman is rank-based and min-max is monotonic, so ρ is identical. Skipping it keeps
  this package lean while staying functionally equivalent on the headline metric.
- **Same libraries, not new ones.** Spearman via `scipy.stats.spearmanr` (the same
  `safe_spearman` wrapper, copied), cliques via `networkx.find_cliques`, contiguity via the
  copied `contiguous_runs` — no new correlation/clique machinery introduced.
- **Threshold is the one knob**, exposed in the notebook as a number box (default 0.60,
  positive-only — grouping is about moving *together*).
- **CSV is long-format** (one row per observation), the shape a DB query returns.
- **`moe_name = "volume"`** for now; the pipeline is MoE-agnostic, so any hourly MoE works.
- **6-connectivity** for the 3D components (shared cube face only) — fixed by the feature
  spec; edge/corner touches do not connect.

---

## 7. Wiring real data (production)

Replace the fixture with a query returning the [§3](#3-the-data-contract) columns:

```python
df = your_db_query(corridor_id, start_date, end_date)   # long-format DataFrame
vol_lr = ccc.build_clique_volume(df, ccc.ORDERED_INTERSECTIONS, direction=ccc.Direction.LR,
                                 weekdays=list(ccc.DAY_LABELS), hour_slots=ccc.HOUR_SLOTS,
                                 threshold=user_threshold)
fig = ccc.render_voxel_volume(vol_lr, intersection_labels=[...])   # -> dcc.Graph / st.plotly_chart
```

Everything is read from the data and corridor order — nothing is hard-coded to 6
intersections, 12 weeks, or 16 hours.

---

## 8. Files

```
corridor_correlation_cliques/
├── README.md                  ← this file
├── __init__.py                ← public re-exports
├── schemas.py                 ← local Direction + RawObservation (zero-dependency contract)
├── fixture_data.py            ← 12-week synthetic Volume generator + CSV read/write
├── slot_cliques.py            ← per-slot week-over-week Spearman → cliques → contiguity → volume
├── voxel_viz.py               ← 3D renderer + 6-connectivity components + largest cuboid (copied verbatim)
├── data/
│   └── corridor_volume_12wk.csv   ← generated long-format dataset (16,128 rows)
└── notebooks/
    └── corridor_correlation_cliques_prototype.ipynb
```

**Dependencies** (all already in the environment): `numpy`, `pandas`, `scipy`, `plotly`,
`networkx`, `ipywidgets`. No dependency on any sibling folder.
