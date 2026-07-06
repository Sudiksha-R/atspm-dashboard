# Bottleneck Detection Feature — Final Design

*Author: Claude Sonnet 4.6 | Date: 2026-06-15 (v3 — FINAL, awaiting coding go-ahead)*

---

## 1. Feature summary

Given a user-selected date-time window, a set of corridor intersections, and one MoE
(**AOR% — Arrivals on Red**), this feature answers:

> "For each (intersection, day-of-week, hour-of-day) slot, is the AOR crossing an
> unfavorable threshold — and how severe and how persistent is it?"

Rendered as a **3D voxel volume** (intersection × day × hour) where:
- Only unfavorable voxels (mean AOR ≥ threshold) are shown
- **Color** (YlOrRd) encodes **severity** — pale amber = just above threshold, dark red = extreme
- **Opacity** encodes **persistence** — transparent = sporadic (1/8 weeks), fully solid = every week

A companion **distribution panel** shows the 8 individual weekly AOR values for the
worst voxel, with a threshold line and mean marker overlaid.

---

## 2. What this is NOT (scope boundaries)

| Out of scope | Reason |
|---|---|
| 3D connected components | Each voxel is independently bad or not — no clustering needed here |
| Multi-MoE threshold crossing | One MoE at a time; multi-MoE is a v2 extension |
| Real-time streaming | Prototype only; integration to live DB is documented but not wired |
| Comparison to historical baseline (separate window) | Simplified to one window of 8 weeks |

---

## 3. How it differs from `synchrony/voxel_viz.py`

| Aspect | Correlation voxel (existing) | Bottleneck voxel (new) |
|--------|-------------------------------|------------------------|
| Volume content | Binary 0/1 (in clique or not) | Float intensity [0,1] + float opacity [0.35,1] |
| Question answered | "Are these intersections behaviorally similar?" | "Is this intersection's MoE exceeding the bad threshold?" |
| Color scheme | Qualitative categorical palette (region identity) | `YlOrRd` sequential (severity magnitude) |
| Opacity | Uniform across all voxels | Per-voxel (encodes persistence) |
| Connected components | Yes (core feature) | No (not needed) |
| Reuse from `voxel_viz.py` | N/A | Axis formatting, cube mesh geometry, layout scaffold |

---

## 4. Data pipeline (end to end)

```
USER INPUT (ipywidgets in notebook)
  ├── Threshold value: FloatSlider + FloatText (synced)
  └── Direction: Dropdown  "Above or equal (≥)"  /  "Below or equal (≤)"

        ↓

fixture_data.generate_lr_aor_observations()   # or generate_rl_aor_observations()
  → list[RawObservation], 5 376 rows per direction
    (6 intersections × 8 weeks × 7 days × 16 hours)
    Uses synchrony.schemas.RawObservation — same type as the rest of the pipeline

        ↓  aggregation.aggregate_by_slot(observations, request)

  DataFrame, 672 rows (6 × 7 × 16)
  Columns: intersection_id, weekday, hour_slot,
           mean_value, std_value, n_weeks, weekly_values (list[float])

        ↓  threshold.apply_threshold(agg_df, threshold_spec)

  Same DataFrame enriched with:
  is_bottleneck, pct_exceeding, raw_excess, intensity, opacity

        ↓  volume.build_intensity_volume(slot_stats_df, request)

  BottleneckVolume
    intensity_volume  ndarray (6, 7, 16), float, NaN = non-bottleneck
    opacity_volume    ndarray (6, 7, 16), float, NaN = non-bottleneck
    slot_stats        full enriched DataFrame (for tooltips + distribution panel)

        ↓                                ↓
  viz_3d.render_bottleneck_volume()    viz_distribution.render_worst_voxel_distribution()
  → go.Figure (3D cube plot)           → go.Figure (weekly bar chart)
```

---

## 5. Intensity and opacity formulas

```python
# Stage C — threshold test
if bad_direction == "above":
    is_bottleneck = mean_value >= threshold
    raw_excess    = max(mean_value - threshold, 0.0)
    worst_ref     = 100.0        # AOR% hard upper limit
else:
    is_bottleneck = mean_value <= threshold
    raw_excess    = max(threshold - mean_value, 0.0)
    worst_ref     = 0.0

# Stage D — intensity and opacity
intensity = clamp(raw_excess / (worst_ref - threshold), 0.0, 1.0)
opacity   = 0.35 + 0.65 * pct_exceeding     # range [0.35, 1.0]
```

`pct_exceeding` = fraction of the 8 individual weekly values that cross the threshold.
A voxel with all 8 weeks above 70% gets `opacity = 1.0` (fully solid).
A voxel with only 1 of 8 weeks above gets `opacity = 0.43` (clearly translucent).

---

## 6. Synthetic AOR fixture design

### 6a. Calibrated diurnal parameters

| Direction | Floor | AM amp/ctr/σ | Midday amp/ctr/σ | PM amp/ctr/σ |
|-----------|-------|--------------|-----------------|--------------|
| LR (east) | 40%   | 33 / 9.0h / 0.9h | 15 / 13.0h / 1.2h | 28 / 17.25h / 0.9h |
| RL (west) | 40%   | 15 / 9.0h / 0.9h | 14 / 13.0h / 1.2h | 32 / 17.5h  / 1.2h |

Physical rationale for RL asymmetry:
- RL AM amplitude is 15 vs LR 33 — fewer people drive west during morning commute
- RL PM amplitude is 32 (larger) and ctr is 17.5h (slightly later) — peak homebound flow
- RL PM σ is 1.2h (wider) — ramp-up/cool-down takes longer for the evening return trip

### 6b. Core AOR formula per voxel

```python
am_val  = am_amp  * gauss(h, am_ctr,  am_sig)     # h = slot center hour (e.g. 8.5)
mid_val = mid_amp * gauss(h, mid_ctr, mid_sig)
pm_val  = pm_amp  * gauss(h, pm_ctr,  pm_sig)

# midday boost added to mid_val for intersections C/D on Wed/Thu (see §6d)
mid_val += midday_boost_raw(h, weekday, intersection_id)

aor = (
    floor
    + (am_val * am_mult + mid_val * 1.0 + pm_val * pm_mult) * dow_scale
    + week_trend
    + bad_week_bias
    + noise
)
aor = clamp(aor, 0.0, 100.0)
```

### 6c. Platoon leapfrog — intersection multipliers

The offset timing in a coordinated arterial lets a platoon pass one intersection on
green, but by the time it reaches the next one it has dispersed and arrives on red
again. This creates an **alternating high/low AOR pattern** along the corridor,
different for each direction.

**Eastbound (LR, Phase 2) multipliers** (am_mult, pm_mult):

| id   | Intersection      | am × | pm × | Story |
|------|-------------------|-------|-------|-------|
| 1240 | McIntosh Rd.  (A) | 1.20  | 0.85  | AM bottleneck — first in corridor, no upstream offset benefit |
| 1241 | Honore Ave.   (B) | 0.70  | 1.15  | AM clears — platoon from A arrives on green; PM bottleneck |
| 1242 | Woodmont Dr.  (C) | 1.15  | 0.75  | AM bottleneck — platoon disperses after B |
| 1243 | Cattleridge   (D) | 0.65  | 1.25  | AM clears — offset from C; highest PM bottleneck |
| 1244 | Cattlemen Rd. (E) | 1.25  | 0.90  | AM worst — loses sync again; also has 8-week worsening trend |
| 1245 | I-75 SB Ramp  (F) | 0.80  | 1.10  | AM moderate; PM elevated (ramp merging traffic) |

**Westbound (RL, Phase 6) multipliers** (am_mult, pm_mult):

| id   | Intersection      | am × | pm × | Story |
|------|-------------------|-------|-------|-------|
| 1240 | McIntosh Rd.  (A) | 1.15  | 0.75  | RL: AM moderate, PM clears — good offset from B heading west |
| 1241 | Honore Ave.   (B) | 0.80  | 1.15  | RL: PM bottleneck — platoon disperses after C heading west |
| 1242 | Woodmont Dr.  (C) | 1.10  | 0.70  | RL: PM clears — good offset from D |
| 1243 | Cattleridge   (D) | 0.75  | 1.20  | RL: PM bottleneck — loses sync after E |
| 1244 | Cattlemen Rd. (E) | 1.20  | 0.75  | RL: PM clears — good offset from F |
| 1245 | I-75 SB Ramp  (F) | 0.90  | 1.30  | RL: PM worst — heavy homebound flow off I-75 ramp |

**Expected voxels above 70% threshold (default):**

| Plot | AM bottlenecks | PM bottlenecks | Midday island |
|------|---------------|----------------|---------------|
| Eastbound (LR) | A, C, E (Mon–Fri, 8–10am) | B, D, F (Mon–Fri, 4–7pm) | C, D (Wed/Thu only, 12–2pm) |
| Westbound (RL) | None (AM amplitude too low) | F, D, B (Mon–Fri, 4–7pm) | None |

### 6d. Complexity layers

**Layer 1 — Day-of-week scaling (DOW):**
Applied to peak contributions only (not the floor):

| Day | Scale |
|-----|-------|
| Mon | 1.10  |
| Tue | 0.95  |
| Wed | 1.00  |
| Thu | 0.98  |
| Fri | 1.08  |
| Sat | 0.55  |
| Sun | 0.35  |

Weekends produce few/no voxels above 70% → the 3D plot is dense on weekdays,
nearly empty on Sat/Sun. Engineers immediately see this is a weekday phenomenon.

**Layer 2 — Week-to-week noise:**
Gaussian N(0, σ=5pp) per voxel, from a seeded numpy RNG (seed=2026). Makes
`pct_exceeding` continuous rather than binary — engineers see gradations of
persistence, not just "always bad / never bad."

**Layer 3 — Bad weeks (global):**
Weeks 3 and 7 carry a +4pp global bias simulating a corridor-wide poor-timing period
(construction, timing plan not reoptimised, etc.). Raises `pct_exceeding` for
borderline voxels in those weeks.

**Layer 4 — Worsening trend at E, eastbound only:**
`week_trend = 0.8 × (week_id - 1)` added to intersection E (Cattlemen Rd.) AM
observations in the LR direction. Adds 0pp in week 1, +5.6pp by week 8. The 3D plot
will show E's AM voxels gradually darkening (higher intensity) from front to back
along the week axis — a signal that maintenance intervention is overdue.

**Layer 5 — Midday island at C and D:**
On Wednesdays and Thursdays only, intersections C (Woodmont) and D (Cattleridge) get
an extra midday Gaussian spike (C: +22pp amplitude, D: +18pp amplitude, centered at
13.0h). This pushes their 12pm–2pm slots above 70%, creating a visually distinct
"midday island" disconnected from the AM and PM slabs. Engineers might trace this to
recurring Wednesday/Thursday heavy-vehicle deliveries or a recurring special event.

---

## 7. Module structure

```
bottleneck_detection/
├── __init__.py          public re-exports (see §9)
├── DESIGN.md            this document
├── config.py            MoEConfig catalog: name, bad_direction, threshold, worst_ref
├── schemas.py           ThresholdSpec, BottleneckRequest, SlotStats, BottleneckVolume
│                        re-exports Direction + RawObservation from synchrony.schemas
├── fixture_data.py      generate_lr_aor_observations() + generate_rl_aor_observations()
│                        → list[RawObservation], 5 376 rows each, with all 5 complexity layers
├── aggregation.py       aggregate_by_slot() → DataFrame (mean, std, n_weeks, weekly_values)
├── threshold.py         apply_threshold() → DataFrame + (is_bottleneck, intensity, opacity)
├── volume.py            build_intensity_volume() → BottleneckVolume
├── viz_3d.py            render_bottleneck_volume() → go.Figure
│                        _intensity_to_color(), _cube_mesh, hover text helpers
│                        Re-uses _CUBE_VERTS/_CUBE_FACES + axis-label helpers from voxel_viz.py
└── viz_distribution.py  render_worst_voxel_distribution() → go.Figure
                         (bar chart of 8 weekly values + threshold line + mean marker)
```

---

## 8. Key schemas (column names used throughout)

### aggregation.aggregate_by_slot() output columns

| Column | Type | Description |
|--------|------|-------------|
| intersection_id | str | e.g. "1244" |
| weekday | str | "Mon"…"Sun" |
| hour_slot | str | "06:00-07:00"…"21:00-22:00" |
| mean_value | float | Mean AOR% across weeks for this slot |
| std_value | float | Std dev across weeks |
| n_weeks | int | Number of weeks with observations (should be 8) |
| weekly_values | list[float] | Individual per-week values (needed for pct_exceeding) |

### After threshold.apply_threshold() — additional columns

| Column | Type | Description |
|--------|------|-------------|
| is_bottleneck | bool | True if mean_value crosses the threshold |
| pct_exceeding | float | Fraction of 8 weeks crossing threshold (for opacity) |
| raw_excess | float | |mean_value − threshold|, 0 if not a bottleneck |
| intensity | float\|NaN | [0,1] severity for colorscale; NaN if not a bottleneck |
| opacity | float\|NaN | [0.35,1.0] persistence; NaN if not a bottleneck |

---

## 9. Public API (`__init__.py` exports)

```python
from bottleneck_detection import (
    # fixture
    generate_lr_aor_observations,
    generate_rl_aor_observations,
    ORDERED_INTERSECTIONS, INTERSECTION_LABELS, HOUR_SLOTS, N_WEEKS,
    # request / config
    BottleneckRequest, ThresholdSpec, BottleneckVolume,
    get_moe_config,
    # pipeline steps
    aggregate_by_slot,
    apply_threshold,
    build_intensity_volume,
    # renderers
    render_bottleneck_volume,
    render_worst_voxel_distribution,
)
```

---

## 10. Notebook design

**File:** `notebooks/bottleneck_detection_prototype.ipynb`

### ipywidgets controls (Cell 2 — the only interactive cell)

```python
threshold_slider   FloatSlider(0–100, step=1, default=70)
threshold_text     FloatText(default=70, synced with slider)
direction_dropdown Dropdown(options=["Above or equal (≥)", "Below or equal (≤)"], default="Above")
run_button         Button("Update 3D View")
output             Output widget (3D figures render inside here)
```

When button is clicked: re-runs `aggregate_by_slot → apply_threshold → build_intensity_volume
→ render_bottleneck_volume (LR) → render_bottleneck_volume (RL)` and clears + replaces
the Output. No page reload needed.

### Cells

| Cell | Type | Content |
|------|------|---------|
| 1 | Markdown | Feature intro — what AOR is, what the 3D axes mean, how to use the controls |
| 2 | Code | ipywidgets controls + `run_analysis()` callback wired to button |
| 3 | Code | Generate LR + RL fixture observations; print row counts + value ranges |
| 4 | Code | Show aggregated slot stats for one intersection (Cattlemen / E) as a table |
| 5 | Code | Print bottleneck voxel counts per direction at default threshold |
| 6 | Code | **LR 3D plot** (eastbound) |
| 7 | Code | **RL 3D plot** (westbound) |
| 8 | Code | **Distribution panel** — worst LR voxel |
| 9 | Code | **Distribution panel** — worst RL voxel |
| 10 | Markdown | **Integration notes** — swap fixture for real DB; wire to Dash/Streamlit |

---

## 11. Visualization design details

### 3D voxel plot

- **Axes:** x = intersection (A→F, 0–5), y = day (Mon–Sun, 0–6), z = hour (6a–9p, 0–15)
- **Colorscale:** `YlOrRd` — pale yellow (barely over threshold) → dark red (severely bad)
- **Per-voxel rendering:** one `go.Mesh3d` trace per bottleneck voxel with its own
  `color` (sampled from YlOrRd at `intensity`) and `opacity`
- **Colorbar:** invisible `go.Scatter3d` dummy trace carries the colorscale + colorbar widget
- **Hover tooltip per voxel:**
  ```
  Cattlemen Rd. (E)
  Monday · 8-9 AM
  Mean AOR: 78.4%   Threshold: ≥ 70.0%  (+8.4pp)
  Persistence: 6/8 weeks  (75%)
  Std dev: ±6.2%
  ```
- **Title (dynamic):** `"AOR% Bottleneck Map — Eastbound — Threshold: ≥ 70.0%"`
- **Figure subtitle annotation:** `"Color = severity above threshold | Opacity = weeks exceeded / 8"`

### Distribution panel

- **Bar chart:** 8 bars (one per week) coloured red if that week exceeded the threshold,
  grey otherwise
- **Threshold line:** dashed red vertical at x = threshold
- **Mean marker:** gold star at x = mean_value
- **Annotation box:** `"8-wk mean: 78.4% | Exceeded: 6/8 weeks | Threshold: ≥ 70.0%"`
- **Title:** `"AOR% — Cattlemen Rd. | Monday | 8-9 AM"`

---

## 12. What to reuse from `synchrony/voxel_viz.py`

| Element | Action |
|---------|--------|
| `DAY_LABELS`, `DEFAULT_HOUR_START` | Copy into `viz_3d.py` constants |
| `_fmt_hour`, `_hour_tick_label`, `hour_range_label` | Copy into `viz_3d.py` |
| `default_intersection_labels` | Copy |
| `_CUBE_VERTS`, `_CUBE_FACES`, `_cube_mesh_arrays` | Copy (identical geometry) |
| Scene layout (axis tick arrays, camera, aspectmode) | Adapt (same structure) |
| `generate_synthetic_volume`, `label_connected_components` | **Do not copy** |
| `VOXEL_PALETTE`, component colour logic | **Do not copy** |

---

## 13. Integration notes (for main dashboard developer)

1. **Replace the fixture generator** with a DB query returning `list[RawObservation]`
   (same type from `synchrony.schemas`). The `BottleneckRequest` fields map directly:
   ```sql
   SELECT intersection_id, corridor_order, direction, week_id, obs_date AS date,
          weekday, hour_slot, 'aorPct' AS moe_name, aor_pct AS raw_value
   FROM hourly_moe_staging
   WHERE corridor_id = :corridor_id
     AND direction   = :direction
     AND phase_group = :phase_group
     AND weekday     IN :weekday_filter
     AND hour_slot   IN :hour_slot_filter
     AND obs_date    BETWEEN :start_date AND :end_date
   ```

2. **`ThresholdSpec`** comes from the dashboard UI controls (Dash slider/text, Streamlit
   `st.slider`, etc.) — the notebook's ipywidgets are the prototype equivalent.

3. **`render_bottleneck_volume`** returns `go.Figure` — pass directly to
   `dcc.Graph(figure=fig)` (Dash) or `st.plotly_chart(fig)` (Streamlit).

4. **`render_worst_voxel_distribution`** can be wired to a `clickData` callback from
   the 3D plot so clicking a voxel updates the distribution panel for that slot.

5. **Two calls for LR + RL:** run the full pipeline twice and expose as tabs.

6. **No new Python packages required** beyond what is already in MPD_ENV.
   `ipywidgets 8.1.8` was installed 2026-06-15.

---

## 14. Status

| Item | Status |
|------|--------|
| `config.py` | ✅ Written |
| `schemas.py` | ✅ Written |
| `fixture_data.py` | ✅ Written |
| `aggregation.py` | ✅ Written |
| `threshold.py` | ✅ Written |
| `volume.py` | ✅ Written |
| `viz_3d.py` | ✅ Written |
| `viz_distribution.py` | ✅ Written |
| `__init__.py` | ✅ Written |
| Notebook (`bottleneck_detection/notebooks/`) | ✅ Built + executed, 18 cells, 0 errors |

**Note:** the notebook lives in `bottleneck_detection/notebooks/` (self-contained
inside the designated folder), not the repo's top-level `notebooks/`. It adds the
repo root to `sys.path` (two levels up) so `import bottleneck_detection` works.

**Verified at the default 70% threshold:**
- Eastbound: 50 bottleneck voxels — AM at McIntosh/Woodmont/Cattlemen, PM at
  Honore/Cattleridge/I-75, Wed/Thu midday island at Woodmont/Cattleridge, 0 weekend.
- Westbound: 25 bottleneck voxels — PM only at I-75/Cattleridge/Honore, 0 AM, 0 weekend.
- AOR ranges: eastbound 21–97% (mean 51%), westbound 25–95% (mean 50%) — realistic.
