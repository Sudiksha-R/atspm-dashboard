# Bottleneck Detection — Developer Integration Guide

> **Audience:** A developer receiving this folder as a zip, using an LLM assistant
> (Claude or similar) to integrate the prototype into the main dashboard UI.
>
> **What this folder is:** A self-contained, working Python prototype that takes
> historical hourly Measure-of-Effectiveness (MoE) data for any number of
> intersections on a corridor, and renders a **3D voxel bottleneck map**
> (Intersection × Day-of-Week × Hour-of-Day) plus a companion **weekly distribution
> panel** for the worst-performing slot.
>
> The prototype was built and tested on the Bee Ridge Road demo corridor (6
> intersections), but **nothing in the code is hard-coded to 6 intersections** —
> it scales to any corridor size.

---

## Table of contents

1. [What it produces](#1-what-it-produces)
2. [Dependencies](#2-dependencies)
3. [File structure](#3-file-structure)
4. [Core concept — the dual encoding](#4-core-concept--the-dual-encoding)
5. [Data contract — what the pipeline needs](#5-data-contract--what-the-pipeline-needs)
6. [Public API reference](#6-public-api-reference)
7. [End-to-end pipeline walkthrough](#7-end-to-end-pipeline-walkthrough)
8. [Wiring to a real database](#8-wiring-to-a-real-database)
9. [Scaling to more intersections](#9-scaling-to-more-intersections)
10. [MoE catalog — supported measures and adding new ones](#10-moe-catalog--supported-measures-and-adding-new-ones)
11. [Adjusting the time window](#11-adjusting-the-time-window)
12. [Integrating into Dash / Streamlit](#12-integrating-into-dash--streamlit)
13. [What the fixture_data module is (and isn't)](#13-what-the-fixture_data-module-is-and-isnt)
14. [Performance notes for large corridors](#14-performance-notes-for-large-corridors)
15. [Dependency on the `synchrony` package](#15-dependency-on-the-synchrony-package)

---

## 1. What it produces

### 3D bottleneck cube (`render_bottleneck_volume`)

A Plotly `go.Figure` — a rotatable 3D cube with:
- **x-axis** = intersections in corridor order
- **y-axis** = day of week (Mon → Sun)
- **z-axis** = hour of day (e.g. 6 AM → 10 PM)

Only voxels where the **8-week mean MoE crosses the threshold** are drawn.
Each drawn voxel has two encodings:
- **Color** (YlOrRd amber→red) = **severity** — how far past the threshold the
  mean value sits, normalised to [0, 1] against the MoE's hard physical limit.
- **Opacity** (translucent→solid) = **persistence** — the fraction of the `n`
  analysis weeks in which that individual week also crossed the threshold.

A **solid deep-red cube** = chronic, severe bottleneck.
A **faint pale cube** = occasional, mild exceedance.

### Weekly distribution panel (`render_worst_voxel_distribution`)

A Plotly `go.Figure` — bar chart of the individual per-week MoE values for one
chosen voxel, with the threshold line and the multi-week mean overlaid. Red bars
= weeks that crossed the threshold; grey bars = compliant weeks.

By default shows the worst-intensity voxel. In the dashboard, wire the 3D plot's
`clickData` event to drive which voxel is shown.

---

## 2. Dependencies

All required packages are available in the existing dashboard Python environment.
No new packages are needed beyond what was already installed.

| Package | Minimum version | Purpose |
|---------|----------------|---------|
| `numpy` | 1.24+ | Volume array operations |
| `pandas` | 1.5+ | Slot-stats DataFrame |
| `plotly` | 5.0+ | 3D figure rendering |
| `ipywidgets` | 8.0+ | Prototype notebook controls only — not needed for Dash/Streamlit |

The `synchrony` package (the sibling folder in this repository) **must be
importable**. It provides the `Direction` enum and `RawObservation` dataclass that
the bottleneck module re-exports. See [§15](#15-dependency-on-the-synchrony-package).

---

## 3. File structure

```
bottleneck_detection/
├── INTEGRATION_GUIDE.md   ← you are here
├── DESIGN.md              ← design rationale, parameter calibration, expected outputs
├── __init__.py            ← public re-exports (import from here)
│
├── config.py              ← MoE catalog (thresholds, units, bad-direction)
├── schemas.py             ← all dataclasses: ThresholdSpec, BottleneckRequest,
│                             SlotStats, BottleneckVolume; re-exports Direction +
│                             RawObservation from synchrony
│
├── fixture_data.py        ← PROTOTYPE ONLY — synthetic 8-week AOR data
│                             Replace with real DB query when integrating
│
├── aggregation.py         ← raw observations → per-slot DataFrame
├── threshold.py           ← apply threshold → intensity + opacity columns
├── volume.py              ← DataFrame → 3D numpy arrays (BottleneckVolume)
├── viz_3d.py              ← render_bottleneck_volume → go.Figure
├── viz_distribution.py    ← render_worst_voxel_distribution → go.Figure
│
└── notebooks/
    └── bottleneck_detection_prototype.ipynb   ← working demo (18 cells, 0 errors)
```

---

## 4. Core concept — the dual encoding

### Why two visual channels?

A voxel that is **always slightly bad** (persistent but mild) is a different
operational problem from one that is **occasionally very bad** (sporadic but
severe). Standard heatmaps collapse these into one value. This prototype encodes
both dimensions simultaneously:

```
color   (YlOrRd)     ←  intensity  =  (mean_value − threshold) / (worst_ref − threshold)
opacity (0.35 → 1.0) ←  persistence = fraction of n_weeks the threshold was crossed
```

### Formula details

For an **"above" MoE** (high = bad, e.g. AOR%):

```python
is_bottleneck = mean_value >= threshold
raw_excess    = max(mean_value - threshold, 0.0)
intensity     = clamp(raw_excess / (worst_ref - threshold), 0.0, 1.0)
opacity       = 0.35 + 0.65 * pct_exceeding          # pct_exceeding ∈ [0, 1]
```

For a **"below" MoE** (low = bad, e.g. AOG%, throughput):

```python
is_bottleneck = mean_value <= threshold
raw_excess    = max(threshold - mean_value, 0.0)
intensity     = clamp(raw_excess / (threshold - worst_ref), 0.0, 1.0)
opacity       = 0.35 + 0.65 * pct_exceeding
```

The minimum opacity of **0.35** ensures even a voxel exceeded in only 1 out of
n weeks is still faintly visible — it's not dropped from the view entirely.

---

## 5. Data contract — what the pipeline needs

### The single input record type: `RawObservation`

Every pipeline function ultimately consumes a `list[RawObservation]`. This is a
frozen dataclass defined in `synchrony/schemas.py` and re-exported from
`bottleneck_detection.schemas`.

```python
from bottleneck_detection.schemas import RawObservation, Direction

@dataclass(frozen=True)
class RawObservation:
    intersection_id: str        # e.g. "1240", "1241", "sig_007" — any string ID
    corridor_order: int         # 0-based position along the corridor (west→east or
                                # source→destination); used only for ordering the x-axis
    direction: Direction        # Direction.LR (eastbound/phase2) or Direction.RL (westbound/phase6)
    week_id: int                # 1-based week number within the analysis window
    date: date                  # calendar date of the observation
    weekday: str                # "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"
    hour_slot: str              # "06:00-07:00", "07:00-08:00", … "21:00-22:00"
    moe_name: str               # e.g. "aorPct", "aogPct" — must match a key in MOE_CATALOG
    raw_value: float            # the measured MoE value for this intersection-week-slot
```

**Key points for the DB query:**

- `intersection_id` can be any string — it does not need to be numeric. The
  corridor mapping is driven by `ordered_intersections` in `BottleneckRequest`,
  not by the IDs themselves.
- `week_id` must be **1-based** and monotonically increasing across the analysis
  window. The simplest way to generate it from a date column:
  ```sql
  DENSE_RANK() OVER (ORDER BY DATE_TRUNC('week', obs_date)) AS week_id
  ```
- `weekday` must be a 3-letter abbreviation: `"Mon"`, `"Tue"`, `"Wed"`, `"Thu"`,
  `"Fri"`, `"Sat"`, `"Sun"`. These must match exactly — the aggregation step uses
  them as group keys.
- `hour_slot` must be a string in `"HH:MM-HH:MM"` format, e.g. `"08:00-09:00"`.
  Each slot represents one hour. They must match the `hour_slot_filter` strings
  in the `BottleneckRequest`.
- `direction` takes `Direction.LR` or `Direction.RL`. Run the pipeline separately
  for each direction (calling the full pipeline twice with the respective row sets).
- `raw_value` is the **raw measured value**, not normalised. For AOR% this is a
  percentage in the range [0, 100].

---

## 6. Public API reference

Import everything from the package root:

```python
import bottleneck_detection as bd
```

### Configuration

| Name | Type | Description |
|------|------|-------------|
| `bd.MOE_CATALOG` | `dict[str, MoEConfig]` | All supported MoEs; keys are `moe_name` strings |
| `bd.get_moe_config(moe_name)` | `→ MoEConfig` | Look up a MoE config; raises `KeyError` if unknown |
| `MoEConfig.bad_direction` | `str` | `"above"` or `"below"` |
| `MoEConfig.default_threshold` | `float` | Suggested starting threshold |
| `MoEConfig.worst_ref` | `float` | Hard physical limit for intensity normalisation |
| `MoEConfig.label` | `str` | Human-readable name for plot titles |
| `MoEConfig.unit` | `str` | Unit string for axis labels (e.g. `"%"`, `"sec"`) |

### Dataclasses

#### `ThresholdSpec`

```python
ThresholdSpec(
    moe_name: str,            # must match a key in MOE_CATALOG
    threshold: float,         # the unfavorability cutoff value
    bad_direction: str,       # "above" or "below"
    worst_ref: float | None,  # hard physical limit; None = auto from MOE_CATALOG
)
```

Easiest construction from the catalog:

```python
cfg = bd.get_moe_config("aorPct")
spec = ThresholdSpec(
    moe_name=cfg.name,
    threshold=70.0,                    # user-supplied
    bad_direction=cfg.bad_direction,   # "above"
    worst_ref=cfg.worst_ref,           # 100.0
)
```

#### `BottleneckRequest`

```python
BottleneckRequest(
    corridor_id: str,                   # any corridor identifier string
    direction: Direction,               # Direction.LR or Direction.RL
    ordered_intersections: list[str],   # ALL intersection IDs in corridor order
    selected_intersections: list[str],  # subset to include in this run (can be same as ordered)
    phase_group: str,                   # e.g. "phase2" or "phase6" — metadata only, not filtered
    weekday_filter: list[str],          # e.g. ["Mon","Tue","Wed","Thu","Fri"] or all 7
    hour_slot_filter: list[str],        # e.g. ["06:00-07:00", ..., "21:00-22:00"]
    start_date: date,
    end_date: date,
    threshold_spec: ThresholdSpec,
    min_weeks: int = 2,                 # drop slots with fewer weeks of data than this
)
```

`ordered_intersections` sets the x-axis order of the 3D plot. It is **separate
from** `selected_intersections` so you can, for example, include only the
intersections that are active in a specific phase while still preserving their
relative position in the corridor on the plot.

#### `BottleneckVolume`

Returned by `build_intensity_volume`. Carries the two 3D numpy arrays plus
the full enriched slot DataFrame for downstream use.

```python
@dataclass
class BottleneckVolume:
    intensity_volume: np.ndarray   # shape (n_int, n_days, n_hours); NaN = not a bottleneck
    opacity_volume: np.ndarray     # same shape; NaN = not a bottleneck
    n_intersections: int
    n_days: int
    n_hours: int
    ordered_intersections: list[str]
    weekdays: list[str]
    hour_slots: list[str]
    slot_stats: pd.DataFrame       # full enriched slot statistics (see §slot-stats-columns)

    # Computed properties
    n_bottleneck_voxels: int       # count of non-NaN cells
    worst_voxel() -> tuple[int, int, int] | None   # (i, d, h) of highest-intensity voxel
```

#### Slot stats DataFrame columns

After the full pipeline, `volume.slot_stats` is a DataFrame with these columns
(one row per `(intersection_id, weekday, hour_slot)` combination that had
sufficient data):

| Column | dtype | Description |
|--------|-------|-------------|
| `intersection_id` | str | Intersection identifier |
| `weekday` | str | Day abbreviation ("Mon"…"Sun") |
| `hour_slot` | str | Hour string ("06:00-07:00"…) |
| `mean_value` | float | Mean MoE across all weeks in the window |
| `std_value` | float | Population std dev across weeks |
| `n_weeks` | int | Number of weeks with data |
| `weekly_values` | list[float] | Individual per-week values (length = n_weeks) |
| `is_bottleneck` | bool | True if mean crossed the threshold |
| `pct_exceeding` | float | Fraction of weeks that crossed the threshold [0, 1] |
| `raw_excess` | float | `|mean_value − threshold|`; 0 for non-bottlenecks |
| `intensity` | float or NaN | [0, 1] severity; NaN for non-bottlenecks |
| `opacity` | float or NaN | [0.35, 1.0] persistence; NaN for non-bottlenecks |

### Pipeline functions

| Function | Input | Output |
|----------|-------|--------|
| `bd.aggregate_by_slot(rows, request)` | `Iterable[RawObservation]`, `BottleneckRequest` | `pd.DataFrame` (slot stats, no threshold columns yet) |
| `bd.apply_threshold(slot_stats, spec)` | `pd.DataFrame`, `ThresholdSpec` | `pd.DataFrame` (with `is_bottleneck`, `intensity`, `opacity` added) |
| `bd.build_intensity_volume(slot_stats, request)` | enriched `pd.DataFrame`, `BottleneckRequest` | `BottleneckVolume` |

### Renderers

```python
fig = bd.render_bottleneck_volume(
    volume: BottleneckVolume,
    spec: ThresholdSpec,
    *,
    intersection_labels: dict[str, str] | None = None,  # {id: "Street Name"} for axis ticks
    day_labels: Sequence[str] | None = None,             # override ["Mon"…"Sun"] if needed
    hour_start: int = 6,                                 # first hour slot (6 = 6 AM)
    cube_size: float = 0.82,                             # voxel size in axis units
    title: str | None = None,                            # auto-generated if None
    direction_label: str = "",                           # e.g. "Eastbound (Phase 2)"
    moe_label: str = "AOR%",                             # for plot title and axis
) -> go.Figure
```

```python
fig = bd.render_worst_voxel_distribution(
    volume: BottleneckVolume,
    spec: ThresholdSpec,
    *,
    voxel: tuple[int, int, int] | None = None,  # (i, d, h); None = auto-worst
    intersection_labels: dict[str, str] | None = None,
    moe_label: str = "AOR%",
    hour_start: int = 6,
) -> go.Figure
```

---

## 7. End-to-end pipeline walkthrough

Here is the **minimum viable integration** — six steps, all synchronous, no async
required:

```python
from datetime import date
import bottleneck_detection as bd
from bottleneck_detection.schemas import BottleneckRequest, ThresholdSpec, Direction

# ── Step 1: Fetch your data ────────────────────────────────────────────────────
# Returns list[RawObservation] from your DB (see §8 for the query template).
rows_lr = fetch_observations(corridor_id, Direction.LR, start_date, end_date)
rows_rl = fetch_observations(corridor_id, Direction.RL, start_date, end_date)

# ── Step 2: Define the threshold ──────────────────────────────────────────────
# Get defaults from the catalog, then override threshold with the user's value.
moe_cfg = bd.get_moe_config("aorPct")          # or whatever MoE the user picked
spec = ThresholdSpec(
    moe_name=moe_cfg.name,
    threshold=user_threshold,                   # float from UI slider
    bad_direction=moe_cfg.bad_direction,
    worst_ref=moe_cfg.worst_ref,
)

# ── Step 3: Build the request ─────────────────────────────────────────────────
# ordered_intersections must list ALL corridor intersection IDs in spatial order.
request_lr = BottleneckRequest(
    corridor_id=corridor_id,
    direction=Direction.LR,
    ordered_intersections=["sig_001", "sig_002", "sig_003", "sig_004"],  # your IDs
    selected_intersections=["sig_001", "sig_002", "sig_003", "sig_004"],
    phase_group="phase2",
    weekday_filter=["Mon", "Tue", "Wed", "Thu", "Fri"],   # or add "Sat", "Sun"
    hour_slot_filter=[f"{h:02d}:00-{h+1:02d}:00" for h in range(6, 22)],  # 6am-10pm
    start_date=start_date,
    end_date=end_date,
    threshold_spec=spec,
    min_weeks=2,
)
# Build the RL request identically but with Direction.RL and phase_group="phase6".

# ── Step 4: Aggregate ─────────────────────────────────────────────────────────
agg_lr = bd.aggregate_by_slot(rows_lr, request_lr)

# ── Step 5: Apply threshold → intensities ─────────────────────────────────────
enriched_lr = bd.apply_threshold(agg_lr, spec)

# ── Step 6: Build volume ──────────────────────────────────────────────────────
vol_lr = bd.build_intensity_volume(enriched_lr, request_lr)

# ── Step 7: Render ───────────────────────────────────────────────────────────
LABELS = {
    "sig_001": "Main St. & 1st Ave.",
    "sig_002": "Main St. & 2nd Ave.",
    # ...
}
fig_3d = bd.render_bottleneck_volume(
    vol_lr, spec,
    intersection_labels=LABELS,
    direction_label="Eastbound (Phase 2)",
    moe_label=moe_cfg.label,
)
fig_dist = bd.render_worst_voxel_distribution(
    vol_lr, spec,
    intersection_labels=LABELS,
    moe_label=moe_cfg.label,
)

# Both figures are plain go.Figure objects ready for display.
```

---

## 8. Wiring to a real database

Replace `fixture_data.generate_lr_aor_observations()` with a function that
queries your DB and returns `list[RawObservation]`. The rest of the pipeline is
unchanged.

### SQL template

```sql
SELECT
    i.intersection_id,
    i.corridor_order,
    :direction        AS direction,
    -- week_id: 1-based week number within the analysis window
    DENSE_RANK() OVER (ORDER BY DATE_TRUNC('week', h.obs_date)) AS week_id,
    h.obs_date        AS date,
    TO_CHAR(h.obs_date, 'Dy')                       AS weekday,   -- 'Mon', 'Tue', etc.
    TO_CHAR(h.hour_start, 'HH24') || ':00-' ||
    TO_CHAR(h.hour_start + 1, 'HH24') || ':00'      AS hour_slot, -- '08:00-09:00'
    :moe_name         AS moe_name,
    h.aor_pct         AS raw_value     -- swap column name for other MoEs
FROM
    hourly_moe_staging h
    JOIN intersections i USING (intersection_id)
WHERE
    i.corridor_id  = :corridor_id
    AND h.direction    = :direction         -- 'LR' or 'RL'
    AND h.phase_group  = :phase_group
    AND h.weekday      IN :weekday_filter
    AND h.hour_slot    IN :hour_slot_filter
    AND h.obs_date     BETWEEN :start_date AND :end_date
ORDER BY
    week_id, h.obs_date, i.corridor_order, h.hour_start
```

### Python wrapper

```python
from datetime import date
from bottleneck_detection.schemas import Direction, RawObservation

def fetch_observations(
    db_conn,
    corridor_id: str,
    direction: Direction,
    phase_group: str,
    weekday_filter: list[str],
    hour_slot_filter: list[str],
    start_date: date,
    end_date: date,
    moe_name: str,
) -> list[RawObservation]:
    rows = db_conn.execute(
        QUERY_TEMPLATE,
        {
            "corridor_id": corridor_id,
            "direction": direction.value,    # "LR" or "RL"
            "phase_group": phase_group,
            "weekday_filter": weekday_filter,
            "hour_slot_filter": hour_slot_filter,
            "start_date": start_date,
            "end_date": end_date,
            "moe_name": moe_name,
        }
    ).fetchall()

    return [
        RawObservation(
            intersection_id=r["intersection_id"],
            corridor_order=r["corridor_order"],
            direction=direction,
            week_id=r["week_id"],
            date=r["date"],
            weekday=r["weekday"],
            hour_slot=r["hour_slot"],
            moe_name=r["moe_name"],
            raw_value=float(r["raw_value"]),
        )
        for r in rows
    ]
```

**Weekday string format:** Many databases return full names ("Monday") or
abbreviations with different casing ("MON"). Normalise to 3-letter title-case
before building the `RawObservation`:

```python
WEEKDAY_MAP = {
    "Monday": "Mon", "Tuesday": "Tue", "Wednesday": "Wed",
    "Thursday": "Thu", "Friday": "Fri", "Saturday": "Sat", "Sunday": "Sun",
    "MON": "Mon", "TUE": "Tue",  # add your DB's format
}
weekday = WEEKDAY_MAP[r["weekday"]]
```

---

## 9. Scaling to more intersections

The prototype uses 6 intersections but **every component scales to N**.

### What changes

| Component | How it scales |
|-----------|--------------|
| `ordered_intersections` in `BottleneckRequest` | Pass all N intersection IDs in corridor order |
| `intensity_volume` / `opacity_volume` shape | `(N, 7, n_hours)` — numpy handles any N |
| 3D plot | x-axis tick count grows with N; Plotly renders one `go.Mesh3d` trace per bottleneck voxel |
| `intersection_labels` dict | Add a `{id: "Street Name"}` entry for each intersection |

### Performance with large N

Each bottleneck voxel is one `go.Mesh3d` trace. Plotly handles hundreds of
traces smoothly, but very large corridors (N > 30) with many bottleneck voxels
(say, > 500) may make the 3D view sluggish in a browser. If that happens:

- **Reduce the hour range** via `hour_slot_filter` — e.g. limit to 6am–8pm
  instead of 6am–10pm.
- **Reduce the day range** — e.g. weekdays only (`weekday_filter` excludes Sat/Sun).
- **Use `selected_intersections`** to show a subset of the corridor in one view
  while keeping `ordered_intersections` as the full list (so positions are
  consistent if you later show different subsets side by side).
- **Increase `min_weeks`** — slots with few data weeks drop out of the volume.

### Axis labels on large corridors

If intersection street names are long (e.g. "Cattleridge Blvd."), the x-axis
tick labels on the 3D plot will crowd. You can pass abbreviated labels to
`render_bottleneck_volume`:

```python
# Use short labels for the plot, keep full names for hover text
SHORT_LABELS = {
    "1240": "McIntosh",
    "1241": "Honore",
    # ...
}
FULL_LABELS = {
    "1240": "McIntosh Rd.",
    # ...
}
# intersection_labels drives the axis ticks AND the hover header
fig = bd.render_bottleneck_volume(vol, spec, intersection_labels=SHORT_LABELS)
```

Alternatively, show only a subset of tick labels by overriding the figure layout
after the renderer returns it:

```python
fig = bd.render_bottleneck_volume(vol, spec, intersection_labels=FULL_LABELS)
# Show tick every 3 intersections to reduce crowding
fig.update_layout(scene=dict(xaxis=dict(tickmode="linear", tick0=0, dtick=3)))
```

---

## 10. MoE catalog — supported measures and adding new ones

`config.MOE_CATALOG` maps `moe_name` strings to `MoEConfig` objects:

| `moe_name` | Label | Unit | Bad when | Default threshold | `worst_ref` |
|-----------|-------|------|----------|------------------|-------------|
| `aorPct` | Arrival on Red % | % | above | 70% | 100 |
| `aogPct` | Arrival on Green % | % | below | 40% | 0 |
| `splitFail` | Split Failures | per hr | above | 3 | 20 |
| `maxOuts` | Max-Outs | per hr | above | 5 | 30 |
| `avgWait` | Average Wait | sec | above | 55 | 180 |
| `throughput` | Throughput | veh/hr | below | 300 | 0 |

`worst_ref` is the hard physical bound used to normalise excess to [0, 1].
For example, AOR% can never exceed 100%, so `worst_ref = 100.0`. A slot at
`mean_value = 85%` with `threshold = 70%` gets
`intensity = (85 − 70) / (100 − 70) = 0.5` (medium amber).

### Adding a new MoE

Edit `config.py` — add one entry to `MOE_CATALOG`:

```python
"queueLength": MoEConfig(
    name="queueLength",
    label="Queue Length",
    unit="veh",
    bad_direction="above",
    default_threshold=10.0,
    worst_ref=50.0,
    description="Average number of vehicles queued at the stop bar. "
                "High values indicate capacity problems.",
),
```

No other code changes needed. The threshold, aggregation, volume, and rendering
modules all read `bad_direction` and `worst_ref` from the `ThresholdSpec` at
runtime.

---

## 11. Adjusting the time window

### Hour range

The prototype uses slots 06:00–22:00 (16 one-hour slots). To narrow or widen this:

```python
# Only show AM and PM peaks
hour_slot_filter = (
    [f"{h:02d}:00-{h+1:02d}:00" for h in range(7, 10)]   # 7am-10am
    + [f"{h:02d}:00-{h+1:02d}:00" for h in range(15, 19)] # 3pm-7pm
)
```

The volume's z-axis will automatically reflect only the requested slots — the
`build_intensity_volume` function takes an explicit `hour_slots` argument that
defaults to the fixture's 16-slot grid but can be any list you pass.

```python
vol = bd.build_intensity_volume(
    enriched, request,
    hour_slots=hour_slot_filter,     # z-axis will have only these slots
)
```

### Number of weeks (analysis window)

The pipeline uses however many weeks are in your data. `min_weeks=2` (the default)
drops any slot with fewer than 2 weeks of observations — raise this if you want
more statistical stability. The distribution panel dynamically reads `n_weeks`
from the data, so the bar count adjusts automatically.

### Weekday filter

Pass any subset of `["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]`. The volume's
y-axis reflects exactly the days you include:

```python
# Weekday-only view
weekday_filter = ["Mon", "Tue", "Wed", "Thu", "Fri"]

# Full week
weekday_filter = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
```

---

## 12. Integrating into Dash / Streamlit

The renderers return standard `plotly.graph_objects.Figure` objects. Drop them
directly into your framework's graph component.

### Dash

```python
import dash
from dash import dcc, html, Input, Output, State
import bottleneck_detection as bd

app = dash.Dash(__name__)
app.layout = html.Div([
    dcc.Slider(id="threshold", min=0, max=100, step=1, value=70),
    dcc.Graph(id="bottleneck-3d"),
    dcc.Graph(id="bottleneck-dist"),
    dcc.Store(id="volume-store"),   # optional: cache the volume between callbacks
])

@app.callback(
    Output("bottleneck-3d", "figure"),
    Output("bottleneck-dist", "figure"),
    Input("threshold", "value"),
)
def update(threshold):
    rows = fetch_observations(...)   # your DB call
    spec = build_spec("aorPct", threshold)
    req = build_request(Direction.LR, spec)
    agg = bd.aggregate_by_slot(rows, req)
    enriched = bd.apply_threshold(agg, spec)
    vol = bd.build_intensity_volume(enriched, req)
    return (
        bd.render_bottleneck_volume(vol, spec, intersection_labels=LABELS),
        bd.render_worst_voxel_distribution(vol, spec, intersection_labels=LABELS),
    )

# Wire distribution panel to 3D click:
@app.callback(
    Output("bottleneck-dist", "figure"),
    Input("bottleneck-3d", "clickData"),
    State("volume-store", "data"),
)
def update_dist(click_data, volume_json):
    if click_data is None:
        raise dash.exceptions.PreventUpdate
    # The clicked point's x/y/z correspond to intersection/day/hour indices
    pt = click_data["points"][0]
    voxel = (round(pt["x"]), round(pt["y"]), round(pt["z"]))
    vol = deserialise_volume(volume_json)   # your serialisation helper
    return bd.render_worst_voxel_distribution(vol, spec, voxel=voxel,
                                              intersection_labels=LABELS)
```

### Streamlit

```python
import streamlit as st
import bottleneck_detection as bd

threshold = st.slider("AOR% threshold", 0, 100, 70)
direction = st.selectbox("Direction", ["Eastbound", "Westbound"])

rows = fetch_observations(...)
spec = ThresholdSpec(moe_name="aorPct", threshold=threshold,
                     bad_direction="above", worst_ref=100.0)
req = build_request(direction, spec)
agg = bd.aggregate_by_slot(rows, req)
enriched = bd.apply_threshold(agg, spec)
vol = bd.build_intensity_volume(enriched, req)

st.plotly_chart(
    bd.render_bottleneck_volume(vol, spec, intersection_labels=LABELS),
    use_container_width=True,
)
st.plotly_chart(
    bd.render_worst_voxel_distribution(vol, spec, intersection_labels=LABELS),
    use_container_width=True,
)
```

### Jupyter (existing prototype style)

```python
import bottleneck_detection as bd
bd.render_bottleneck_volume(vol, spec, intersection_labels=LABELS).show()
bd.render_worst_voxel_distribution(vol, spec, intersection_labels=LABELS).show()
```

---

## 13. What the `fixture_data` module is (and isn't)

`fixture_data.py` generates **entirely synthetic AOR% data** — it is a prototype
stand-in for a real database query. It is designed to produce a realistic-looking
dataset that demonstrates the feature clearly:

- Smooth Gaussian diurnal curves (AM, midday, PM peaks) — not step functions
- "Platoon leapfrog" alternating high/low AOR pattern along the corridor
- Day-of-week scaling (Mon/Fri heavier, weekends light)
- Week-to-week Gaussian noise
- Simulated "bad weeks" (corridor-wide poor-timing periods)
- A worsening trend at one intersection over 8 weeks
- A midday anomaly at two intersections on Wed/Thu only

**Do not use `fixture_data.py` in the production integration.** It exists only to
make the prototype notebook self-contained and to let you run and inspect the
pipeline before the DB connection is available. Delete or ignore it once you have
the real data feed wired up.

The public names exported from `fixture_data` that you might see referenced in
the notebook (`generate_lr_aor_observations`, `generate_rl_aor_observations`,
`ORDERED_INTERSECTIONS`, `INTERSECTION_LABELS`, `DAY_LABELS`, `HOUR_SLOTS`,
`N_WEEKS`) are all prototype constants. In production, `DAY_LABELS` and
`HOUR_SLOTS` are the only ones worth keeping as reference — they define the
canonical axis grids used throughout the pipeline.

---

## 14. Performance notes for large corridors

The main rendering function (`render_bottleneck_volume`) creates **one
`go.Mesh3d` Plotly trace per bottleneck voxel**. This is necessary because each
voxel has a distinct color AND a distinct opacity, preventing batching. In
practice:

| Scenario | Approx. bottleneck voxels | Render behaviour |
|----------|--------------------------|-----------------|
| 6 intersections, 70% threshold, weekdays+weekends | 50–100 | Instant |
| 20 intersections, moderately congested corridor | 200–400 | Smooth |
| 50 intersections, all hours, high congestion | 1 000–2 000 | May lag; consider narrowing hour/day range |

The `aggregate_by_slot` and `apply_threshold` steps are pure pandas and handle
millions of raw observation rows efficiently. The bottleneck is always the Plotly
trace count, not the Python computation.

If you need to handle very large corridors, consider:

1. **Segment the corridor** — run the analysis on sub-corridors of 10–15
   intersections and render each as a separate tab.
2. **Coarser time bins** — aggregate to 2-hour slots in SQL before sending to
   `aggregate_by_slot`, then adjust `hour_slot_filter` accordingly.
3. **Filter to active days only** — weekday-only analysis halves the y-axis size.

---

## 15. Dependency on the `synchrony` package

`bottleneck_detection/schemas.py` imports `Direction` and `RawObservation` from
`synchrony.schemas`:

```python
from synchrony.schemas import Direction, RawObservation  # noqa: F401
```

This means **the `synchrony/` sibling package must be on the Python path** when
`bottleneck_detection` is imported. In the current repo layout that happens
automatically when you run from the repository root.

If you are shipping `bottleneck_detection` as a standalone zip **without the
full `synchrony/` package**, you have two options:

**Option A (recommended): include `synchrony/schemas.py` in the zip.**
Only the `schemas.py` file from `synchrony/` is needed — not the whole package.
Copy it to a stub package:

```
your_project/
├── synchrony/
│   ├── __init__.py     # empty is fine
│   └── schemas.py      # just this file from the repo
└── bottleneck_detection/
    └── ...
```

**Option B: inline the two types into `bottleneck_detection/schemas.py`.**
Replace the import line at the top of `schemas.py`:

```python
# Replace this line:
from synchrony.schemas import Direction, RawObservation  # noqa: F401

# With these definitions directly:
from enum import Enum
from dataclasses import dataclass
from datetime import date

class Direction(str, Enum):
    LR = "LR"   # left-to-right / eastbound
    RL = "RL"   # right-to-left / westbound

@dataclass(frozen=True)
class RawObservation:
    intersection_id: str
    corridor_order: int
    direction: Direction
    week_id: int
    date: date
    weekday: str
    hour_slot: str
    moe_name: str
    raw_value: float
```

No other files need to change — all other imports of `Direction` and
`RawObservation` within `bottleneck_detection/` already go through
`bottleneck_detection.schemas`.

---

*End of integration guide.*
*Reference the accompanying `DESIGN.md` for design rationale, fixture calibration
details, and the full intensity/opacity formula derivation.*
