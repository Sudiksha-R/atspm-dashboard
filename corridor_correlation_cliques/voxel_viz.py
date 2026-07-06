"""3D voxel view of per-day-hour correlation cliques + 3D connected components.

This is the next step after `grouping.py`. The grouping module answers, for a *single*
day-hour regime, "which intersections form correlation cliques?". Run that question
once for **every** (day-of-week, hour-of-day) slot over a chosen window of weeks and the
answers stack into a 3D binary volume:

    volume[intersection, day, hour] = 1   if that intersection is part of *some* clique
                                          for that day-hour slot, else 0

For the Bee Ridge demo corridor that volume is **6 x 7 x 16** -- 6 intersections (A-F,
in corridor order along one travel direction), 7 days (Mon-Sun), 16 one-hour slots
(6am-7am ... 9pm-10pm). One "row" of the volume -- all 6 intersections for a fixed
(day, hour) -- is exactly the 6x1x1 clique-membership vector the grouping notebook
produces; this module assumes that detection has already happened and consumes the
finished volume.

What this module provides (built "from the end", per the spec -- the visualization first):

  * `generate_synthetic_volume`   -- a labelled 6x7x16 test volume with a few solid
                                     dense regions parked around the weekday AM and PM
                                     commute peaks (plus optional isolated noise voxels),
                                     so the renderer can be exercised before the real
                                     per-slot clique detector is wired in.
  * `label_connected_components`  -- 3D connected components under **6-connectivity**
                                     (two filled voxels are linked iff they share a cube
                                     *face*), via scipy.ndimage.label. A dense region is a
                                     component of size >= 2; a lone 1 with no face-neighbour
                                     is "isolated" (size 1) -- filled, but not part of any
                                     connected region.
  * `render_voxel_volume`         -- a rotatable Plotly 3D figure: one solid cube per
                                     filled voxel, coloured by connected component, with
                                     fully labelled Intersection / Day / Hour axes and a
                                     per-region legend. `style="markers"` swaps the solid
                                     cubes for a lighter point cloud.
  * `components_to_frame`         -- a plain DataFrame summary (one row per component:
                                     size, span in each axis, isolated flag) for printing.

Axis convention throughout: x = intersection (the physical corridor line), y = day of
week, z = hour of day (time runs "up"). Volume is indexed `[intersection, day, hour]`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.colors import qualitative
from scipy import ndimage

# --------------------------------------------------------------------------------------
# Axis defaults / labels
# --------------------------------------------------------------------------------------

DAY_LABELS: tuple[str, ...] = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
"""Day-of-week axis labels, index 0 = Monday."""

DEFAULT_HOUR_START = 6
"""First hour slot starts at 06:00 (slot 0 = 6am-7am). With 16 slots the last is
slot 15 = 9pm-10pm, i.e. 6am-10pm inclusive of starts."""

VOXEL_PALETTE: tuple[str, ...] = tuple(qualitative.Bold)
"""Qualitative palette for connected components. Region identity is *categorical*
(which dense blob a voxel belongs to), not a magnitude, so a qualitative palette is
correct here -- mirrors grouping/GROUP_PALETTE's reasoning, not the RdBu sync scale.
Cycled if a volume has more components than colours."""

ISOLATED_COLOR = "rgb(170,170,170)"
"""Grey for size-1 (isolated) voxels: filled, but -- per the connectivity definition --
not part of any connected region, so deliberately not given a region colour."""

NONCUBOID_COLOR = "rgb(205,205,205)"
"""Light grey for filled voxels that are *not* part of a region's largest interior cuboid
(the region's non-cuboidal remainder, isolated voxels, and noise). Drawn faint and
translucent in the cuboid-highlight view so the coloured cuboids stand out clearly."""

NONCUBOID_OPACITY = 0.18
"""Opacity for the faint grey non-cuboid voxels (see NONCUBOID_COLOR)."""


def default_intersection_labels(n: int) -> list[str]:
    """A, B, C, ... for the first 26 intersections, then I27, I28, ... beyond."""

    letters = [chr(ord("A") + i) for i in range(min(n, 26))]
    extra = [f"I{i + 1}" for i in range(26, n)]
    return letters + extra


def _fmt_hour(hour24: int) -> str:
    """24h hour -> compact 12h label: 6 -> '6a', 12 -> '12p', 13 -> '1p', 21 -> '9p'."""

    suffix = "a" if hour24 < 12 else "p"
    h12 = hour24 % 12
    if h12 == 0:
        h12 = 12
    return f"{h12}{suffix}"


def _hour_tick_label(h: int, hour_start: int = DEFAULT_HOUR_START) -> str:
    """Short axis tick for hour slot index `h` (just the slot's start hour)."""

    return _fmt_hour(hour_start + h)


def hour_range_label(h: int, hour_start: int = DEFAULT_HOUR_START) -> str:
    """Full hover label for hour slot index `h`, e.g. '6-7 AM', '4-5 PM'."""

    start = hour_start + h
    end = start + 1
    s12, s_suf = (start % 12 or 12), ("AM" if start < 12 else "PM")
    e12, e_suf = (end % 12 or 12), ("AM" if end < 12 else "PM")
    if s_suf == e_suf:
        return f"{s12}-{e12} {s_suf}"
    return f"{s12} {s_suf}-{e12} {e_suf}"


# --------------------------------------------------------------------------------------
# Synthetic test volume
# --------------------------------------------------------------------------------------


def generate_synthetic_volume(
    *,
    n_intersections: int = 6,
    n_days: int = 7,
    n_hours: int = 16,
    hour_start: int = DEFAULT_HOUR_START,
    seed: int = 42,
    n_bumps: int = 6,
    n_isolated: int = 2,
) -> np.ndarray:
    """Build a labelled binary test volume that *looks like* a plausible clique-membership
    result: a handful of solid dense regions parked around the weekday morning and
    evening commute peaks, everything else zero.

    The blobs are placed deterministically around the AM peak (~7-9am) and PM peak
    (~4-6pm) on weekdays (Mon-Fri); `seed` only jitters their extents slightly so the
    test input isn't perfectly rigid, and `n_isolated` scatters a few lone voxels to
    exercise the "isolated, not connected" path. `n_bumps` sticks single-voxel
    protrusions onto the solid blobs so each connected region has a part that pokes out
    of its largest interior cuboid (these render grey in the cuboid-highlight view).
    Returns an int array shaped `(n_intersections, n_days, n_hours)` with values in {0, 1}.

    Hour indices are relative to `hour_start` (slot 0 = `hour_start`:00-`hour_start+1`:00).
    With the defaults: 7am -> slot 1, 8am -> slot 2, 9am -> slot 3; 4pm -> slot 10,
    5pm -> 11, 6pm -> 12.
    """

    rng = np.random.default_rng(seed)
    vol = np.zeros((n_intersections, n_days, n_hours), dtype=int)

    def hr(clock24: int) -> int:
        return clock24 - hour_start

    def fill(i_rng, d_rng, h_rng) -> None:
        i0, i1 = i_rng
        d0, d1 = d_rng
        h0, h1 = h_rng
        i1 = min(i1, n_intersections - 1)
        d1 = min(d1, n_days - 1)
        h1 = min(max(h1, 0), n_hours - 1)
        h0 = max(h0, 0)
        vol[i0 : i1 + 1, d0 : d1 + 1, h0 : h1 + 1] = 1

    # A small ± jitter on a blob's far edge so extents aren't identical run to run.
    def j() -> int:
        return int(rng.integers(-1, 1))  # -1 or 0

    # --- AM peak (commute in) -------------------------------------------------------
    # West-end cluster, early-week mornings: intersections A-C, Mon-Wed, 7-9am.
    fill((0, 2 + j()), (0, 2), (hr(7), hr(9)))
    # East-end cluster, late-week mornings: intersections D-F, Thu-Fri, 8-9am.
    fill((3, 5), (3, 4), (hr(8), hr(9 + j())))

    # --- PM peak (commute out) ------------------------------------------------------
    # Mid-corridor cluster, all weekdays, 4-6pm -- the big evening block.
    fill((1, 4), (0, 4), (hr(16), hr(18)))
    # West-end cluster, mid-week evenings: A-B, Tue-Wed, 5-6pm.
    fill((0, 1), (1, 2), (hr(17), hr(18)))

    # --- Non-cuboidal bumps: single-voxel protrusions off the solid blobs ------------
    # Each extends exactly one existing region (never bridges two), so the region stays
    # connected but is no longer a perfect cuboid -- the bump pokes out of its largest
    # interior cuboid and renders grey in the cuboid-highlight view.
    _STEPS = ((-1, 0, 0), (1, 0, 0), (0, -1, 0), (0, 1, 0), (0, 0, -1), (0, 0, 1))
    base_labels, _ = ndimage.label(vol, structure=_FACE_STRUCTURE)
    filled = np.argwhere(vol == 1)
    placed = 0
    attempts = 0
    while placed < n_bumps and len(filled) and attempts < 500:
        attempts += 1
        fi, fd, fh = filled[int(rng.integers(0, len(filled)))]
        di, dd, dh = _STEPS[int(rng.integers(0, 6))]
        ni, nd, nh = int(fi + di), int(fd + dd), int(fh + dh)
        if not (0 <= ni < n_intersections and 0 <= nd < n_days and 0 <= nh < n_hours):
            continue
        if vol[ni, nd, nh] == 1:
            continue
        # only extend if this empty voxel touches exactly one original region
        touch = set()
        for si, sd, sh in _STEPS:
            ai, ad, ah = ni + si, nd + sd, nh + sh
            if 0 <= ai < n_intersections and 0 <= ad < n_days and 0 <= ah < n_hours:
                lab = int(base_labels[ai, ad, ah])
                if lab > 0:
                    touch.add(lab)
        if len(touch) != 1:
            continue
        vol[ni, nd, nh] = 1
        placed += 1

    # --- Isolated noise voxels (size-1 components) ----------------------------------
    placed = 0
    attempts = 0
    while placed < n_isolated and attempts < 200:
        attempts += 1
        i = int(rng.integers(0, n_intersections))
        d = int(rng.integers(0, n_days))
        h = int(rng.integers(0, n_hours))
        if vol[i, d, h] == 1:
            continue
        # keep it genuinely isolated: no existing filled face-neighbour
        nbr = vol[max(i - 1, 0) : i + 2, d, h].sum() + vol[i, max(d - 1, 0) : d + 2, h].sum() \
            + vol[i, d, max(h - 1, 0) : h + 2].sum()
        if nbr == 0:
            vol[i, d, h] = 1
            placed += 1

    return vol


# --------------------------------------------------------------------------------------
# 3D connected components (6-connectivity / face-sharing)
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class VoxelComponent:
    """One 3D connected component of filled voxels under 6-connectivity.

    `voxels` are `(intersection, day, hour)` index triples. `is_isolated` marks a
    size-1 component -- a filled voxel with no filled face-neighbour, so by the
    connectivity rule it is *not* part of any connected dense region.
    """

    component_id: int
    size: int
    voxels: tuple[tuple[int, int, int], ...]
    intersection_span: tuple[int, int]
    day_span: tuple[int, int]
    hour_span: tuple[int, int]
    is_isolated: bool

    def as_dict(self) -> dict:
        return {
            "component_id": self.component_id,
            "size": self.size,
            "intersection_span": list(self.intersection_span),
            "day_span": list(self.day_span),
            "hour_span": list(self.hour_span),
            "is_isolated": self.is_isolated,
            "voxels": [list(v) for v in self.voxels],
        }


# 6-connectivity = "shares a cube face": the 3D structuring element with connectivity 1.
_FACE_STRUCTURE = ndimage.generate_binary_structure(3, 1)


def label_connected_components(
    volume: np.ndarray,
) -> tuple[np.ndarray, list[VoxelComponent]]:
    """Label 3D connected components of the filled (==1) voxels under **6-connectivity**
    (two voxels connected iff they share a cube face -- no edge/corner touches).

    Returns `(labels, components)`:
      * `labels` -- int array, same shape as `volume`; 0 = background, 1..K = component id.
      * `components` -- list of `VoxelComponent`, sorted by descending size then id
        (so the biggest dense region is first; size-1 isolated voxels sort last).

    The grid is treated as non-cyclic on every axis: Sunday is not adjacent to Monday,
    and the 10pm slot is not adjacent to the 6am slot.
    """

    binary = (np.asarray(volume) != 0)
    labels, n = ndimage.label(binary, structure=_FACE_STRUCTURE)

    components: list[VoxelComponent] = []
    for cid in range(1, n + 1):
        coords = np.argwhere(labels == cid)
        voxels = tuple((int(i), int(d), int(h)) for i, d, h in coords)
        size = len(voxels)
        components.append(
            VoxelComponent(
                component_id=cid,
                size=size,
                voxels=voxels,
                intersection_span=(int(coords[:, 0].min()), int(coords[:, 0].max())),
                day_span=(int(coords[:, 1].min()), int(coords[:, 1].max())),
                hour_span=(int(coords[:, 2].min()), int(coords[:, 2].max())),
                is_isolated=(size == 1),
            )
        )

    components.sort(key=lambda c: (-c.size, c.component_id))
    return labels, components


def components_to_frame(
    components: Sequence[VoxelComponent],
    *,
    intersection_labels: Sequence[str] | None = None,
    day_labels: Sequence[str] = DAY_LABELS,
    hour_start: int = DEFAULT_HOUR_START,
) -> pd.DataFrame:
    """One row per connected component: size, the label span it covers on each axis,
    and whether it is an isolated (size-1) voxel. Meant for a quick printed summary."""

    if intersection_labels is None:
        max_i = max((c.intersection_span[1] for c in components), default=-1)
        intersection_labels = default_intersection_labels(max_i + 1)

    def span_lbl(span, labels):
        a, b = span
        return labels[a] if a == b else f"{labels[a]} - {labels[b]}"

    def hour_span_lbl(span):
        a, b = span
        return (
            hour_range_label(a, hour_start)
            if a == b
            else f"{hour_range_label(a, hour_start).split('-')[0].strip()}"
            f"-{hour_range_label(b, hour_start).split('-')[-1].strip()}"
        )

    rows = []
    for c in components:
        rows.append(
            {
                "region": c.component_id,
                "size": c.size,
                "intersections": span_lbl(c.intersection_span, intersection_labels),
                "days": span_lbl(c.day_span, day_labels),
                "hours": hour_span_lbl(c.hour_span),
                "isolated": c.is_isolated,
            }
        )
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# Largest solid cuboid inside each connected component
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Cuboid:
    """The largest solid axis-aligned cuboid of filled voxels inside one connected
    component -- i.e. the biggest box `[i0..i1] x [d0..d1] x [h0..h1]` that is entirely
    1s. `size` is its voxel volume. Spans are inclusive index ranges."""

    component_id: int
    intersection_span: tuple[int, int]
    day_span: tuple[int, int]
    hour_span: tuple[int, int]
    size: int

    def voxels(self) -> list[tuple[int, int, int]]:
        (i0, i1), (d0, d1), (h0, h1) = (
            self.intersection_span,
            self.day_span,
            self.hour_span,
        )
        return [
            (i, d, h)
            for i in range(i0, i1 + 1)
            for d in range(d0, d1 + 1)
            for h in range(h0, h1 + 1)
        ]

    def as_dict(self) -> dict:
        return {
            "component_id": self.component_id,
            "intersection_span": list(self.intersection_span),
            "day_span": list(self.day_span),
            "hour_span": list(self.hour_span),
            "size": self.size,
        }


def _largest_cuboid_in_mask(mask: np.ndarray):
    """Largest all-True axis-aligned box inside a 3D boolean `mask`.

    Returns `((i0, i1), (d0, d1), (h0, h1), volume)` or `None` if the mask is empty.
    Uses a 3D integral image so each candidate box's "is it solid?" test is O(1), then
    brute-forces all boxes within the mask's bounding box. The volume here is tiny
    (6x7x16), and a component's bounding box smaller still, so this is comfortably fast;
    the integral-image guard keeps it correct and simple rather than clever.
    """

    A = (np.asarray(mask) != 0).astype(np.int64)
    coords = np.argwhere(A)
    if coords.size == 0:
        return None
    imin, jmin, kmin = coords.min(0)
    imax, jmax, kmax = coords.max(0)

    P = np.zeros((A.shape[0] + 1, A.shape[1] + 1, A.shape[2] + 1), dtype=np.int64)
    P[1:, 1:, 1:] = np.cumsum(np.cumsum(np.cumsum(A, axis=0), axis=1), axis=2)

    def box_sum(i0, i1, d0, d1, h0, h1) -> int:
        return int(
            P[i1 + 1, d1 + 1, h1 + 1]
            - P[i0, d1 + 1, h1 + 1]
            - P[i1 + 1, d0, h1 + 1]
            - P[i1 + 1, d1 + 1, h0]
            + P[i0, d0, h1 + 1]
            + P[i0, d1 + 1, h0]
            + P[i1 + 1, d0, h0]
            - P[i0, d0, h0]
        )

    best = None
    best_vol = 0
    for i0 in range(imin, imax + 1):
        for i1 in range(i0, imax + 1):
            si = i1 - i0 + 1
            for d0 in range(jmin, jmax + 1):
                for d1 in range(d0, jmax + 1):
                    sd = si * (d1 - d0 + 1)
                    for h0 in range(kmin, kmax + 1):
                        for h1 in range(h0, kmax + 1):
                            vol = sd * (h1 - h0 + 1)
                            if vol <= best_vol:
                                continue
                            if box_sum(i0, i1, d0, d1, h0, h1) == vol:
                                best_vol = vol
                                best = (int(i0), int(i1), int(d0), int(d1),
                                        int(h0), int(h1))
    if best is None:
        return None
    i0, i1, d0, d1, h0, h1 = best
    return (i0, i1), (d0, d1), (h0, h1), best_vol


def find_component_cuboids(
    volume: np.ndarray,
) -> tuple[np.ndarray, list[VoxelComponent], dict[int, Cuboid]]:
    """For each *connected* component (size >= 2), find its largest solid interior
    cuboid. Returns `(labels, components, cuboids_by_component_id)`; isolated (size-1)
    components are skipped (they have no cuboid worth highlighting)."""

    labels, components = label_connected_components(volume)
    cuboids: dict[int, Cuboid] = {}
    for comp in components:
        if comp.is_isolated:
            continue
        res = _largest_cuboid_in_mask(labels == comp.component_id)
        if res is None:
            continue
        i_span, d_span, h_span, vol = res
        cuboids[comp.component_id] = Cuboid(
            component_id=comp.component_id,
            intersection_span=i_span,
            day_span=d_span,
            hour_span=h_span,
            size=vol,
        )
    return labels, components, cuboids


# --------------------------------------------------------------------------------------
# Cube-mesh geometry (one solid cube per filled voxel)
# --------------------------------------------------------------------------------------

# Unit cube corners, centred on the origin (±0.5 on each axis).
_CUBE_VERTS = np.array(
    [
        (-0.5, -0.5, -0.5),  # 0
        (0.5, -0.5, -0.5),   # 1
        (0.5, 0.5, -0.5),    # 2
        (-0.5, 0.5, -0.5),   # 3
        (-0.5, -0.5, 0.5),   # 4
        (0.5, -0.5, 0.5),    # 5
        (0.5, 0.5, 0.5),     # 6
        (-0.5, 0.5, 0.5),    # 7
    ]
)
# 12 triangles (2 per face) indexing into the 8 corners above.
_CUBE_FACES = (
    (0, 1, 2), (0, 2, 3),  # bottom  (z-)
    (4, 5, 6), (4, 6, 7),  # top     (z+)
    (0, 1, 5), (0, 5, 4),  # y-
    (2, 3, 7), (2, 7, 6),  # y+
    (1, 2, 6), (1, 6, 5),  # x+
    (0, 3, 7), (0, 7, 4),  # x-
)


def _cube_mesh_arrays(centers: Sequence[tuple[int, int, int]], size: float):
    """Batch many cubes into flat (x, y, z, i, j, k) arrays for a single go.Mesh3d."""

    xs, ys, zs = [], [], []
    ii, jj, kk = [], [], []
    for n, (cx, cy, cz) in enumerate(centers):
        base = 8 * n
        for vx, vy, vz in _CUBE_VERTS:
            xs.append(cx + vx * size)
            ys.append(cy + vy * size)
            zs.append(cz + vz * size)
        for a, b, c in _CUBE_FACES:
            ii.append(base + a)
            jj.append(base + b)
            kk.append(base + c)
    return xs, ys, zs, ii, jj, kk


# --------------------------------------------------------------------------------------
# Renderer
# --------------------------------------------------------------------------------------


def _voxel_hovertext(i, d, h, intersection_labels, day_labels, hour_start, region_txt):
    return (
        f"Intersection <b>{intersection_labels[i]}</b><br>"
        f"{day_labels[d]} · {hour_range_label(h, hour_start)}<br>"
        f"{region_txt}"
    )


def render_voxel_volume(
    volume: np.ndarray,
    *,
    intersection_labels: Sequence[str] | None = None,
    day_labels: Sequence[str] = DAY_LABELS,
    hour_start: int = DEFAULT_HOUR_START,
    style: str = "cubes",
    highlight: str = "components",
    opacity: float = 0.85,
    cube_size: float = 0.82,
    show_isolated: bool = True,
    title: str | None = None,
) -> go.Figure:
    """Rotatable Plotly 3D view of the clique-membership volume.

    Axes: x = Intersection (corridor order), y = Day of week, z = Hour of day. Each
    filled voxel is drawn as a solid cube (`style="cubes"`, default) or a marker
    (`style="markers"`). Hovering any voxel reports its intersection, day, hour slot,
    and which region it belongs to.

    Two `highlight` modes:

    * `"components"` (default) -- colour every voxel by its 3D connected component
      (6-connectivity); isolated (size-1) voxels are greyed under one "isolated" legend
      entry (hidden entirely if `show_isolated=False`).
    * `"cuboids"` -- inside each connected region find the **largest solid axis-aligned
      cuboid** of 1s and colour *only that*; every other filled voxel (the region's
      non-cuboidal remainder, isolated voxels, and noise) is drawn as faint translucent
      grey, so the cuboids inside the regions read clearly.

    All analysis is run internally from the raw 0/1 `volume`.
    """

    volume = np.asarray(volume)
    n_int, n_days_v, n_hours = volume.shape
    if intersection_labels is None:
        intersection_labels = default_intersection_labels(n_int)
    day_labels = list(day_labels)[:n_days_v]
    if title is None:
        title = (
            "Correlation-clique volume — largest cuboid per region"
            if highlight == "cuboids"
            else "Correlation-clique volume — 3D connected components"
        )

    labels, components = label_connected_components(volume)
    size_by_id = {c.component_id: c.size for c in components}

    fig = go.Figure()

    # Stable colour order: connected regions first (sorted = size desc), isolated last.
    color_for_id: dict[int, str] = {}
    legend_order = 0
    for comp in components:
        if comp.is_isolated:
            color_for_id[comp.component_id] = ISOLATED_COLOR
        else:
            color_for_id[comp.component_id] = VOXEL_PALETTE[legend_order % len(VOXEL_PALETTE)]
            legend_order += 1

    connected = [c for c in components if not c.is_isolated]
    isolated = [c for c in components if c.is_isolated]

    # ---- a solid-cube / marker trace for an arbitrary voxel list ---------------------
    def add_voxels(voxels, name, color, legendgroup, showlegend, *,
                   trace_opacity=opacity, trace_cube=cube_size):
        if not voxels:
            return
        if style == "markers":
            fig.add_trace(
                go.Scatter3d(
                    x=[v[0] for v in voxels], y=[v[1] for v in voxels],
                    z=[v[2] for v in voxels],
                    mode="markers",
                    marker=dict(size=10 * trace_cube / cube_size, color=color,
                                opacity=trace_opacity, symbol="square",
                                line=dict(width=0)),
                    name=name, legendgroup=legendgroup, showlegend=showlegend,
                    hoverinfo="skip",
                )
            )
        else:
            xs, ys, zs, ii, jj, kk = _cube_mesh_arrays(voxels, trace_cube)
            fig.add_trace(
                go.Mesh3d(
                    x=xs, y=ys, z=zs, i=ii, j=jj, k=kk,
                    color=color, opacity=trace_opacity, flatshading=True,
                    name=name, legendgroup=legendgroup, showlegend=showlegend,
                    hoverinfo="skip",
                    lighting=dict(ambient=0.6, diffuse=0.8, specular=0.1),
                )
            )

    cuboid_voxels: set[tuple[int, int, int]] = set()

    if highlight == "cuboids":
        # largest interior cuboid per connected region -> coloured; everything else grey
        for comp in connected:
            res = _largest_cuboid_in_mask(labels == comp.component_id)
            if res is None:
                continue
            (i0, i1), (d0, d1), (h0, h1), vol = res
            cub = Cuboid(comp.component_id, (i0, i1), (d0, d1), (h0, h1), vol)
            vox = cub.voxels()
            cuboid_voxels.update(vox)
            add_voxels(
                vox,
                name=f"Region {comp.component_id} cuboid (n={vol})",
                color=color_for_id[comp.component_id],
                legendgroup=f"cuboid{comp.component_id}",
                showlegend=True,
            )
        # all remaining filled voxels: faint translucent grey, smaller so they recede
        grey = [(i, d, h) for c in components for (i, d, h) in c.voxels
                if (i, d, h) not in cuboid_voxels]
        if grey and show_isolated:
            add_voxels(
                grey, name=f"non-cuboid / noise (n={len(grey)})",
                color=NONCUBOID_COLOR, legendgroup="noncuboid", showlegend=True,
                trace_opacity=NONCUBOID_OPACITY, trace_cube=cube_size * 0.5,
            )
    else:
        # colour every voxel by connected component
        for comp in connected:
            add_voxels(
                list(comp.voxels),
                name=f"Region {comp.component_id} (n={comp.size})",
                color=color_for_id[comp.component_id],
                legendgroup=f"region{comp.component_id}",
                showlegend=True,
            )
        if isolated and show_isolated:
            n_iso = sum(c.size for c in isolated)
            for idx, comp in enumerate(isolated):
                add_voxels(
                    list(comp.voxels), name=f"isolated (n={n_iso})",
                    color=ISOLATED_COLOR, legendgroup="isolated",
                    showlegend=(idx == 0),
                )

    # ---- one lightweight hover layer over every filled voxel -------------------------
    if show_isolated:
        hover_voxels = [(i, d, h) for c in components for (i, d, h) in c.voxels]
    else:
        hover_voxels = [(i, d, h) for c in connected for (i, d, h) in c.voxels]
    if hover_voxels:
        htext = []
        hcolor = []
        for (i, d, h) in hover_voxels:
            cid = int(labels[i, d, h])
            size = size_by_id.get(cid, 1)
            in_cuboid = (i, d, h) in cuboid_voxels
            if highlight == "cuboids":
                region_txt = (
                    f"Region {cid} · <b>largest cuboid</b>"
                    if in_cuboid
                    else (
                        "isolated voxel (not connected)"
                        if size == 1
                        else f"Region {cid} · outside largest cuboid"
                    )
                )
                vcolor = color_for_id[cid] if in_cuboid else NONCUBOID_COLOR
            else:
                region_txt = (
                    "isolated voxel (not connected)"
                    if size == 1
                    else f"Region {cid} · size {size}"
                )
                vcolor = color_for_id.get(cid, ISOLATED_COLOR)
            htext.append(
                _voxel_hovertext(i, d, h, intersection_labels, day_labels,
                                 hour_start, region_txt)
            )
            hcolor.append(vcolor)
        fig.add_trace(
            go.Scatter3d(
                x=[v[0] for v in hover_voxels],
                y=[v[1] for v in hover_voxels],
                z=[v[2] for v in hover_voxels],
                mode="markers",
                marker=dict(size=4, color=hcolor, opacity=0.0),
                hovertext=htext,
                hoverinfo="text",
                showlegend=False,
            )
        )

    # ---- axes: full tick labels on all three dimensions ------------------------------
    fig.update_layout(
        title=title,
        height=720,
        margin=dict(l=0, r=0, t=50, b=0),
        legend=dict(
            title="Largest cuboid" if highlight == "cuboids" else "Connected region",
            itemsizing="constant",
        ),
        scene=dict(
            aspectmode="cube",
            xaxis=dict(
                title="Intersection",
                tickmode="array",
                tickvals=list(range(n_int)),
                ticktext=list(intersection_labels),
                range=[-0.6, n_int - 0.4],
            ),
            yaxis=dict(
                title="Day of week",
                tickmode="array",
                tickvals=list(range(len(day_labels))),
                ticktext=list(day_labels),
                range=[-0.6, len(day_labels) - 0.4],
            ),
            zaxis=dict(
                title="Hour of day",
                tickmode="array",
                tickvals=list(range(n_hours)),
                ticktext=[_hour_tick_label(h, hour_start) for h in range(n_hours)],
                range=[-0.6, n_hours - 0.4],
            ),
            camera=dict(eye=dict(x=1.7, y=1.7, z=1.2)),
        ),
    )
    return fig
