# Integration Guide — `corridor_correlation_cliques`

> **Audience:** an LLM coding agent (or developer) wiring this prototype into the main
> dashboard UI. This document is self-sufficient: it tells you exactly what the package
> does, the precise function signatures and data shapes, the one place you must change
> (the data source), and copy-paste-ready integration snippets for Dash, Streamlit, and a
> plain backend/API. Read [README.md](README.md) too for the *why*; this file is the *how*.

---

## 0. TL;DR for the agent

1. This folder is **self-contained and zero-dependency** — it imports nothing outside
   itself. Drop the whole `corridor_correlation_cliques/` folder into the dashboard repo
   so it is importable (on `PYTHONPATH` / next to the app, or `pip install -e .`).
2. The **only thing you change** is the data source: replace
   `fixture_data.generate_volume_observations()` / `load_csv()` with a function that
   returns a **long-format pandas DataFrame** with the columns in [§3](#3-data-contract).
   Everything downstream is unchanged.
3. The **main entry point** is
   `slot_cliques.build_clique_volume(df, ordered_ids, *, direction, weekdays, hour_slots, threshold)`
   → a `(n_intersections, n_days, n_hours)` numpy array of 0/1.
4. **Render** it with `voxel_viz.render_voxel_volume(volume, intersection_labels=[...])`
   → a `plotly.graph_objects.Figure`. Put that in `dcc.Graph` / `st.plotly_chart`.
5. **Run once per travel direction** (LR and RL); show as two tabs/figures.
6. The **one user knob** is `threshold` (a float, default `0.60`). Wire it to a UI number
   input.

Minimum working call:

```python
import corridor_correlation_cliques as ccc

df = your_query(...)                      # long-format DataFrame, see §3
vol = ccc.build_clique_volume(
    df, ccc.ORDERED_INTERSECTIONS,
    direction=ccc.Direction.LR,
    weekdays=list(ccc.DAY_LABELS),
    hour_slots=ccc.HOUR_SLOTS,
    threshold=0.60,
)
fig = ccc.render_voxel_volume(
    vol, intersection_labels=[ccc.INTERSECTION_LABELS[i] for i in ccc.ORDERED_INTERSECTIONS],
)
# fig is a go.Figure -> hand to dcc.Graph(figure=fig) or st.plotly_chart(fig)
```

---

## 1. What the feature produces

Given hourly **Volume** (or any hourly MoE) for a corridor of intersections over several
weeks, for **each `(weekday, hour-of-day)` slot independently** it finds which
intersections rise and fall together **week-over-week**, forms correlation **cliques**,
keeps the spatially **contiguous** ones, and marks their members. Stacking all slots gives
a binary 3D volume:

```
volume[intersection, day_of_week, hour_of_day] = 1   # this intersection is in a clique at this slot
                                                = 0   # otherwise
```

Rendered as a rotatable **3D voxel view** with **3D connected components** (6-connectivity)
and **largest-cuboid cores**. For the demo corridor the volume is `6 × 7 × 16`
(6 intersections × 7 weekdays × 16 hour slots 06:00–22:00), but **all dimensions are read
from the data** — nothing is hard-coded to those sizes.

> **Conceptual note (do not confuse with the older "grouping" feature):** the grouping
> prototype computed *one* correlation per pair pooled across all time. This feature
> computes a *separate* correlation **per (weekday, hour) slot**, using only that slot's
> per-week values (here 12). Same Spearman/clique/contiguity math; different data slicing.

---

## 2. Install / make importable

The package depends only on libraries already present in the analytics environment:
`numpy`, `pandas`, `scipy`, `plotly`, `networkx`, and (notebook only) `ipywidgets`. No
new packages required.

Pick whichever fits the host repo:

- **Simplest:** copy the `corridor_correlation_cliques/` folder so it sits next to your app
  module (same directory that's on `sys.path`), then `import corridor_correlation_cliques`.
- **Path insert** (scripts/notebooks):
  ```python
  import sys; sys.path.insert(0, "/abs/path/to/parent_of_the_folder")
  import corridor_correlation_cliques as ccc
  ```
- **Packaged:** add a `pyproject.toml`/`setup.cfg` and `pip install -e .`. No code changes
  needed inside the folder.

The notebook ([notebooks/corridor_correlation_cliques_prototype.ipynb](notebooks/corridor_correlation_cliques_prototype.ipynb))
does `sys.path.insert(0, str(Path.cwd().parent.parent))` because it lives two levels deep;
mirror that only if you keep that layout.

---

## 3. Data contract

Every function consumes a **long-format DataFrame** — one row per observation. These are
the required columns (extra columns are ignored):

| Column | Type | Required | Notes |
|---|---|---|---|
| `intersection_id` | str | ✅ | any string id; must match the values in `ordered_ids` |
| `direction` | str | ✅ | `"LR"` or `"RL"` (matches `Direction.LR.value` / `Direction.RL.value`) |
| `week_id` | int | ✅ | week index within the window; only used to **order** the per-week vector |
| `weekday` | str | ✅ | 3-letter title-case `"Mon"`…`"Sun"`; must match `DAY_LABELS` |
| `hour_slot` | str | ✅ | `"HH:MM-HH:MM"`, one hour, e.g. `"08:00-09:00"`; must match `HOUR_SLOTS` |
| `moe_name` | str | ✅ | the MoE being analyzed; pass the same string to `build_clique_volume(moe_name=...)` |
| `raw_value` | float | ✅ | the measured value |
| `corridor_order` | int | ➖ | 0-based west→east position; convenient but ordering is actually taken from `ordered_ids` |
| `date` | ISO str/date | ➖ | not used by the analysis |

**Critical matching rules** (the per-slot filter is exact-string equality):
- `weekday` values **must** equal the entries in `ccc.DAY_LABELS`
  (`"Mon","Tue","Wed","Thu","Fri","Sat","Sun"`). Normalize DB output (`"Monday"`, `"MON"`)
  before building the DataFrame.
- `hour_slot` strings **must** equal the entries in `ccc.HOUR_SLOTS`
  (`"06:00-07:00"`…`"21:00-22:00"`). If your DB stores an hour integer, format it:
  `f"{h:02d}:00-{h+1:02d}:00"`.
- `direction` strings **must** be `"LR"`/`"RL"`.
- `intersection_id` strings **must** match the ids you pass as `ordered_ids`.

The axis sizes of the output volume come from the lengths of the `weekdays`, `hour_slots`,
and `ordered_ids` arguments — **not** from the DataFrame — so you control the grid by what
you pass in (and the DataFrame must contain rows for those slots).

### Replacing the fixture with a DB query

`fixture_data.py` is **prototype-only**. Swap it for a query that returns the columns
above. Conceptual SQL:

```sql
SELECT
    i.intersection_id,
    i.corridor_order,
    :direction                                   AS direction,        -- 'LR' or 'RL'
    DENSE_RANK() OVER (ORDER BY DATE_TRUNC('week', h.obs_date)) AS week_id,
    h.obs_date                                   AS date,
    TO_CHAR(h.obs_date, 'Dy')                    AS weekday,          -- normalize to 'Mon'...
    TO_CHAR(h.hour_start, 'HH24') || ':00-' ||
        TO_CHAR(h.hour_start + 1, 'HH24') || ':00' AS hour_slot,      -- '08:00-09:00'
    :moe_name                                    AS moe_name,         -- 'volume'
    h.volume                                     AS raw_value
FROM hourly_moe h JOIN intersections i USING (intersection_id)
WHERE i.corridor_id = :corridor_id
  AND h.direction   = :direction
  AND h.obs_date BETWEEN :start_date AND :end_date
ORDER BY week_id, i.corridor_order, h.hour_start;
```

Then normalize weekday/hour strings in Python to match `DAY_LABELS` / `HOUR_SLOTS`
([§7](#7-common-pitfalls)). You can either build a DataFrame directly, or build
`RawObservation` objects and call `ccc.observations_to_dataframe(rows)`.

---

## 4. Public API reference

```python
import corridor_correlation_cliques as ccc
```

### 4.1 Constants & types

| Name | Value / type | Use |
|---|---|---|
| `ccc.ORDERED_INTERSECTIONS` | `list[str]` `["1240"…"1245"]` | corridor order; pass as `ordered_ids` |
| `ccc.INTERSECTION_LABELS` | `dict[str,str]` id → street name | axis labels / hover |
| `ccc.DAY_LABELS` | `tuple[str,...]` Mon…Sun | pass `list(DAY_LABELS)` as `weekdays` |
| `ccc.HOUR_SLOTS` | `list[str]` 16 slots | pass as `hour_slots` |
| `ccc.N_WEEKS` | `int` (12) | fixture only |
| `ccc.MOE_NAME` | `str` (`"volume"`) | default MoE |
| `ccc.CORRIDOR_ID`, `ccc.CORRIDOR_NAME` | `str` | metadata |
| `ccc.DEFAULT_THRESHOLD` | `float` (0.60) | default clique threshold |
| `ccc.Direction` | `str` Enum: `Direction.LR`, `Direction.RL` | direction argument |
| `ccc.RawObservation` | frozen dataclass | optional record type (see [README §3]) |

### 4.2 Data / fixture (replace in production)

```python
ccc.generate_volume_observations() -> list[RawObservation]
ccc.observations_to_dataframe(rows: list[RawObservation]) -> pd.DataFrame
ccc.write_csv(path=ccc.DEFAULT_CSV_PATH) -> Path
ccc.load_csv(path=ccc.DEFAULT_CSV_PATH) -> pd.DataFrame      # generates if missing
ccc.slot_group(direction, weekday_idx, hour_idx) -> frozenset[int]   # fixture ground-truth (validation only)
```

### 4.3 Per-slot clique detection (`slot_cliques`)

```python
ccc.build_clique_volume(
    df: pd.DataFrame,
    ordered_ids: list[str],
    *,
    direction: Direction,
    weekdays: list[str],
    hour_slots: list[str],
    threshold: float = 0.60,
    moe_name: str = "volume",
) -> np.ndarray                      # shape (len(ordered_ids), len(weekdays), len(hour_slots)), dtype int 0/1
```

> **THE main entry point.** Loops every `(weekday, hour)` slot, detects contiguous cliques,
> and sets member voxels to 1. Call once per `direction`.

Supporting functions (for drill-downs, tables, a "click a voxel" panel):

```python
ccc.slot_weekly_vectors(df, *, direction, weekday, hour_slot, ordered_ids, moe_name="volume")
    -> dict[str, np.ndarray]         # {intersection_id: per-week value vector, ordered by week_id}

ccc.slot_correlation_matrix(vectors, ordered_ids, *, labels_by_id=None)
    -> pd.DataFrame                  # symmetric Spearman ρ matrix (diagonal 1.0, None->NaN)

ccc.detect_slot_cliques(vectors, ordered_ids, *, threshold=0.60,
                        direction=Direction.LR, weekday="", hour_slot="")
    -> SlotCliqueResult

ccc.build_slot_results(df, ordered_ids, *, direction, weekdays, hour_slots,
                       threshold=0.60, moe_name="volume")
    -> dict[tuple[str,str], SlotCliqueResult]    # keyed by (weekday, hour_slot)

ccc.safe_spearman(x, y) -> float | None          # None on degenerate input
ccc.contiguous_runs(members, order_of) -> list[list[str]]
```

`SlotCliqueResult` (frozen dataclass) fields:

| Field | Type | Meaning |
|---|---|---|
| `direction`, `weekday`, `hour_slot` | | which slot |
| `n_weeks` | int | samples per ρ at this slot |
| `highly_correlated_pairs` | `list[{"a","b","spearman_rho"}]` | pairs ≥ threshold |
| `cliques` | `list[list[str]]` | maximal cliques (pre-contiguity), corridor-ordered |
| `groups` | `list[list[str]]` | contiguous runs (size ≥ 2) — the reported groups |
| `members` | `set[str]` | union of group members → the voxels set to 1 |

### 4.4 Visualization (`voxel_viz`, copied verbatim from the wider prototype)

```python
ccc.render_voxel_volume(
    volume: np.ndarray,
    *,
    intersection_labels: Sequence[str] | None = None,   # x-axis tick text, len == n_intersections
    day_labels: Sequence[str] = DAY_LABELS,
    hour_start: int = 6,
    style: str = "cubes",                # "cubes" | "markers"
    highlight: str = "components",       # "components" | "cuboids"
    opacity: float = 0.85,
    cube_size: float = 0.82,
    show_isolated: bool = True,
    title: str | None = None,
) -> go.Figure
```

- `highlight="components"` — colour each 3D connected region (6-connectivity).
- `highlight="cuboids"` — colour only the largest solid box inside each region, fade the rest.
- `style="markers"` — lighter point-cloud instead of solid cubes (good for dense volumes).

```python
ccc.label_connected_components(volume) -> (labels: np.ndarray, components: list[VoxelComponent])
ccc.find_component_cuboids(volume)     -> (labels, components, cuboids_by_component_id: dict[int, Cuboid])
ccc.components_to_frame(components, *, intersection_labels=None, day_labels=DAY_LABELS, hour_start=6) -> pd.DataFrame
ccc.hour_range_label(h, hour_start=6) -> str          # e.g. "8-9 AM"
```

`VoxelComponent`: `component_id, size, voxels, intersection_span, day_span, hour_span,
is_isolated` (+ `.as_dict()`). `Cuboid`: `component_id, intersection_span, day_span,
hour_span, size` (+ `.voxels()`, `.as_dict()`).

> `ccc.generate_synthetic_volume(...)` exists but is the legacy random-blob stub the real
> `build_clique_volume` replaces — **do not use it** in production.

---

## 5. Axis conventions (read before mapping clicks/voxels)

The volume is indexed `volume[intersection_index, day_index, hour_index]`:

- **intersection_index** `0..n-1` follows the order of `ordered_ids` (west→east); axis label
  via `intersection_labels[i]`.
- **day_index** `0..6` follows `weekdays` (so `0 = "Mon"` when you pass `list(DAY_LABELS)`).
- **hour_index** `0..15`; clock hour `= hour_start + hour_index` (so index 0 = 06:00–07:00).
  Use `hour_range_label(hour_index)` for a friendly label.

In the Plotly figure these map to **x = intersection, y = day, z = hour**. A 3D `clickData`
point's `x/y/z` are these indices (round them) — convert back with
`ordered_ids[x]`, `weekdays[y]`, `hour_slots[z]` to drive a detail panel
(e.g. `slot_weekly_vectors` + `slot_correlation_matrix` for that slot).

---

## 6. Copy-paste integration snippets

### 6.1 Generic backend / API (framework-agnostic)

```python
import corridor_correlation_cliques as ccc

def corridor_clique_volume(df, *, direction_str: str, threshold: float):
    """df: long-format DataFrame (see §3). Returns (volume, figure)."""
    direction = ccc.Direction(direction_str)               # "LR" or "RL"
    vol = ccc.build_clique_volume(
        df, ccc.ORDERED_INTERSECTIONS,
        direction=direction,
        weekdays=list(ccc.DAY_LABELS),
        hour_slots=ccc.HOUR_SLOTS,
        threshold=threshold,
        moe_name="volume",
    )
    labels = [ccc.INTERSECTION_LABELS[i] for i in ccc.ORDERED_INTERSECTIONS]
    fig = ccc.render_voxel_volume(vol, intersection_labels=labels,
                                  title=f"Correlation cliques — {direction.value}")
    return vol, fig

# Serialize the figure for a frontend:  fig.to_json()   /   fig.to_dict()
```

### 6.2 Dash

```python
import dash
from dash import dcc, html, Input, Output
import corridor_correlation_cliques as ccc

DF = load_corridor_df(...)        # your DB call -> long-format DataFrame (cache it)
LABELS = [ccc.INTERSECTION_LABELS[i] for i in ccc.ORDERED_INTERSECTIONS]

app = dash.Dash(__name__)
app.layout = html.Div([
    dcc.Input(id="threshold", type="number", value=0.60, min=-1, max=1, step=0.05),
    dcc.Dropdown(id="direction", value="LR",
                 options=[{"label": "Eastbound (LR)", "value": "LR"},
                          {"label": "Westbound (RL)", "value": "RL"}]),
    dcc.Dropdown(id="highlight", value="components",
                 options=[{"label": "Connected components", "value": "components"},
                          {"label": "Largest cuboids", "value": "cuboids"}]),
    dcc.Graph(id="voxel"),
])

@app.callback(
    Output("voxel", "figure"),
    Input("threshold", "value"), Input("direction", "value"), Input("highlight", "value"),
)
def update(threshold, direction, highlight):
    vol = ccc.build_clique_volume(
        DF, ccc.ORDERED_INTERSECTIONS, direction=ccc.Direction(direction),
        weekdays=list(ccc.DAY_LABELS), hour_slots=ccc.HOUR_SLOTS,
        threshold=float(threshold))
    return ccc.render_voxel_volume(vol, intersection_labels=LABELS, highlight=highlight)
```

Optional click-to-detail (3D `clickData` → per-slot panel):

```python
@app.callback(Output("detail", "figure"), Input("voxel", "clickData"),
              Input("direction", "value"))
def detail(click, direction):
    if not click: raise dash.exceptions.PreventUpdate
    p = click["points"][0]
    i, d, h = round(p["x"]), round(p["y"]), round(p["z"])
    wd, hs = list(ccc.DAY_LABELS)[d], ccc.HOUR_SLOTS[h]
    vecs = ccc.slot_weekly_vectors(DF, direction=ccc.Direction(direction),
                                   weekday=wd, hour_slot=hs,
                                   ordered_ids=ccc.ORDERED_INTERSECTIONS)
    mat = ccc.slot_correlation_matrix(vecs, ccc.ORDERED_INTERSECTIONS,
                                      labels_by_id=ccc.INTERSECTION_LABELS)
    import plotly.graph_objects as go
    return go.Figure(go.Heatmap(z=mat.values, x=list(mat.columns), y=list(mat.index),
                                zmid=0, zmin=-1, zmax=1, colorscale="RdBu"))
```

### 6.3 Streamlit

```python
import streamlit as st
import corridor_correlation_cliques as ccc

DF = load_corridor_df(...)        # long-format DataFrame
LABELS = [ccc.INTERSECTION_LABELS[i] for i in ccc.ORDERED_INTERSECTIONS]

threshold = st.number_input("Spearman threshold", -1.0, 1.0, 0.60, 0.05)
tabs = st.tabs(["Eastbound (LR)", "Westbound (RL)"])
for tab, direction in zip(tabs, [ccc.Direction.LR, ccc.Direction.RL]):
    with tab:
        vol = ccc.build_clique_volume(
            DF, ccc.ORDERED_INTERSECTIONS, direction=direction,
            weekdays=list(ccc.DAY_LABELS), hour_slots=ccc.HOUR_SLOTS, threshold=threshold)
        st.plotly_chart(ccc.render_voxel_volume(vol, intersection_labels=LABELS),
                        use_container_width=True)
```

---

## 7. Common pitfalls

- **Weekday / hour string mismatch → empty volume.** The per-slot filter is exact string
  equality. If `weekday`/`hour_slot` in your DataFrame don't match `DAY_LABELS` /
  `HOUR_SLOTS`, every slot returns no rows and the volume is all zeros. Normalize first:
  ```python
  WD = {"Monday":"Mon","Tuesday":"Tue","Wednesday":"Wed","Thursday":"Thu",
        "Friday":"Fri","Saturday":"Sat","Sunday":"Sun"}
  df["weekday"]  = df["weekday"].map(lambda w: WD.get(w, w[:3].title()))
  df["hour_slot"] = df["hour_start"].map(lambda h: f"{int(h):02d}:00-{int(h)+1:02d}:00")
  df["direction"] = df["direction"].str.upper()              # 'LR'/'RL'
  df["intersection_id"] = df["intersection_id"].astype(str)  # ids are strings
  ```
- **Run per direction.** `build_clique_volume` filters to one `direction`; call it twice.
- **Few weeks → noisy ρ.** With *n* weeks per slot, Spearman is computed on *n* points.
  More weeks = more stable cliques. The prototype uses 12; production should use what's in
  the window. Slots where a series is constant (zero variance) safely yield no edge.
- **Threshold is the only knob.** Lower it → looser/larger groups; raise it → only the
  tightest. It's positive-only by design (grouping = moving *together*).
- **`moe_name` must match.** If your rows use `"volume"`, the default is fine; otherwise
  pass `moe_name=...` to `build_clique_volume` (and filter your DataFrame to one MoE).
- **Performance:** `build_clique_volume` does `n_days × n_hours` clique detections (112 for
  the demo); each is tiny. The cost scales with the number of slots and intersections, not
  weeks. For large corridors this is still sub-second; cache the DataFrame, not the volume.
- **Don't use `generate_synthetic_volume`** — it's the legacy stub, not the detector.

---

## 8. Validating after you wire real data

Sanity checks an agent can run post-integration:

1. `df` non-empty and columns/strings match [§3](#3-data-contract);
   `set(df.weekday) <= set(DAY_LABELS)` and `set(df.hour_slot) <= set(HOUR_SLOTS)`.
2. `vol = build_clique_volume(...)` has shape
   `(len(ORDERED_INTERSECTIONS), 7, 16)` and `vol.sum() > 0` (non-trivial result).
3. `render_voxel_volume(vol, ...)` returns a `plotly.graph_objects.Figure`.
4. (Prototype data only) groups match the fixture ground truth:
   ```python
   from corridor_correlation_cliques import slot_group, build_slot_results
   res = build_slot_results(df, ccc.ORDERED_INTERSECTIONS, direction=ccc.Direction.LR,
                            weekdays=list(ccc.DAY_LABELS), hour_slots=ccc.HOUR_SLOTS)
   for d,wd in enumerate(ccc.DAY_LABELS):
       for h,hs in enumerate(ccc.HOUR_SLOTS):
           intended = {ccc.ORDERED_INTERSECTIONS[i] for i in slot_group(ccc.Direction.LR,d,h)}
           assert res[(wd,hs)].members == intended
   ```
   (This passes on the bundled fixture — 0 mismatches — and is a good smoke test of the
   pipeline wiring before real data is connected.)

---

## 9. File map

```
corridor_correlation_cliques/
├── INTEGRATION_GUIDE.md   ← you are here
├── README.md              ← design rationale, fixture derivation, expected outputs
├── __init__.py            ← public re-exports (import everything from here)
├── schemas.py             ← Direction + RawObservation (local, zero-dependency)
├── fixture_data.py        ← PROTOTYPE-ONLY synthetic generator + CSV io (replace in prod)
├── slot_cliques.py        ← per-slot Spearman → threshold → cliques → contiguity → volume
├── voxel_viz.py           ← 3D renderer + 6-connectivity components + largest cuboid
├── data/corridor_volume_12wk.csv      ← generated demo dataset (16,128 rows)
└── notebooks/corridor_correlation_cliques_prototype.ipynb   ← worked end-to-end demo
```

**Production checklist:** (1) make the folder importable; (2) replace `fixture_data`
calls with your DB query returning the [§3](#3-data-contract) columns; (3) normalize
weekday/hour/direction strings; (4) call `build_clique_volume` per direction with the UI
`threshold`; (5) render with `render_voxel_volume`; (6) optional click→`slot_*` detail panel.
