"""Plotly 3D voxel renderer for the bottleneck volume.

One rotatable cube per bottleneck voxel, with a **dual encoding**:
  * color   (YlOrRd) = severity  -> sampled from the intensity_volume
  * opacity            = persistence -> read from the opacity_volume

Because every voxel has a *different* color AND a *different* opacity, voxels
can't be batched into one Mesh3d the way synchrony/voxel_viz.py batches by
component color — here each voxel gets its own go.Mesh3d trace. The colorbar is
provided by a single invisible go.Scatter3d carrying the YlOrRd colorscale
(Mesh3d traces can't show a colorbar on their own). A second invisible
go.Scatter3d at the voxel centers carries the hover text.

Cube geometry (_CUBE_VERTS / _CUBE_FACES) and the axis-label helpers are copied
from synchrony/voxel_viz.py so the two 3D views read identically; the connected-
component / cuboid logic from that module is intentionally NOT reused (a
bottleneck voxel is independently bad — no clustering step).

Color scale is YlOrRd, deliberately distinct from the project's other palettes
(RdBu geo map, RdYlGn abstract views, qualitative grouping/voxel-component).
"""

from __future__ import annotations

from typing import Sequence

import numpy as np
import plotly.graph_objects as go
from plotly.colors import sample_colorscale

from .schemas import BottleneckVolume, ThresholdSpec

# ── Axis labels (mirrors synchrony/voxel_viz.py) ───────────────────────────────

DAY_LABELS: tuple[str, ...] = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
DEFAULT_HOUR_START = 6

COLORSCALE = "YlOrRd"
"""Sequential amber→red. Magnitude (severity) is ordinal, so a sequential scale
is correct — and YlOrRd is reserved for this view across the project."""


def _fmt_hour(hour24: int) -> str:
    suffix = "a" if hour24 < 12 else "p"
    h12 = hour24 % 12 or 12
    return f"{h12}{suffix}"


def _hour_tick_label(h: int, hour_start: int = DEFAULT_HOUR_START) -> str:
    return _fmt_hour(hour_start + h)


def hour_range_label(h: int, hour_start: int = DEFAULT_HOUR_START) -> str:
    start = hour_start + h
    end = start + 1
    s12, s_suf = (start % 12 or 12), ("AM" if start < 12 else "PM")
    e12, e_suf = (end % 12 or 12), ("AM" if end < 12 else "PM")
    if s_suf == e_suf:
        return f"{s12}-{e12} {s_suf}"
    return f"{s12} {s_suf}-{e12} {e_suf}"


# ── Cube geometry (copied verbatim from synchrony/voxel_viz.py) ────────────────

_CUBE_VERTS = np.array(
    [
        (-0.5, -0.5, -0.5), (0.5, -0.5, -0.5), (0.5, 0.5, -0.5), (-0.5, 0.5, -0.5),
        (-0.5, -0.5, 0.5), (0.5, -0.5, 0.5), (0.5, 0.5, 0.5), (-0.5, 0.5, 0.5),
    ]
)
_CUBE_FACES = (
    (0, 1, 2), (0, 2, 3), (4, 5, 6), (4, 6, 7), (0, 1, 5), (0, 5, 4),
    (2, 3, 7), (2, 7, 6), (1, 2, 6), (1, 6, 5), (0, 3, 7), (0, 7, 4),
)


def _single_cube_arrays(cx: float, cy: float, cz: float, size: float):
    xs = [cx + vx * size for vx, _, _ in _CUBE_VERTS]
    ys = [cy + vy * size for _, vy, _ in _CUBE_VERTS]
    zs = [cz + vz * size for _, _, vz in _CUBE_VERTS]
    ii = [a for a, _, _ in _CUBE_FACES]
    jj = [b for _, b, _ in _CUBE_FACES]
    kk = [c for _, _, c in _CUBE_FACES]
    return xs, ys, zs, ii, jj, kk


def _intensity_to_color(intensity: float) -> str:
    """Sample the YlOrRd colorscale at ``intensity`` in [0, 1]."""
    t = float(min(max(intensity, 0.0), 1.0))
    return sample_colorscale(COLORSCALE, [t])[0]


def _voxel_hover(
    row,
    spec: ThresholdSpec,
    intersection_label: str,
    day_label: str,
    hour_label: str,
) -> str:
    arrow = "≥" if spec.bad_direction == "above" else "≤"
    n_weeks = int(row["n_weeks"])
    n_cross = int(round(row["pct_exceeding"] * n_weeks))
    delta = row["mean_value"] - spec.threshold
    sign = "+" if delta >= 0 else "−"
    return (
        f"<b>{intersection_label}</b><br>"
        f"{day_label} · {hour_label}<br>"
        f"Mean: {row['mean_value']:.1f}{_unit(spec)}  "
        f"(thr {arrow} {spec.threshold:.0f}{_unit(spec)}, {sign}{abs(delta):.1f}pp)<br>"
        f"Persistence: {n_cross}/{n_weeks} weeks ({row['pct_exceeding'] * 100:.0f}%)<br>"
        f"Std dev: ±{row['std_value']:.1f}"
    )


def _unit(spec: ThresholdSpec) -> str:
    return "%" if spec.moe_name in ("aorPct", "aogPct") else ""


def render_bottleneck_volume(
    volume: BottleneckVolume,
    spec: ThresholdSpec,
    *,
    intersection_labels: dict[str, str] | None = None,
    day_labels: Sequence[str] | None = None,
    hour_start: int = DEFAULT_HOUR_START,
    cube_size: float = 0.82,
    title: str | None = None,
    direction_label: str = "",
    moe_label: str = "AOR%",
) -> go.Figure:
    """Render the bottleneck volume as a rotatable 3D cube plot.

    Only bottleneck voxels (non-NaN in ``volume.intensity_volume``) are drawn.
    Each is one go.Mesh3d coloured by severity (YlOrRd) and made translucent by
    persistence. A dummy invisible scatter provides the colorbar; a second
    invisible scatter provides per-voxel hover text.

    Axes: x = intersection (corridor order), y = day of week, z = hour of day.
    """
    iv = volume.intensity_volume
    ov = volume.opacity_volume
    n_int, n_days, n_hours = iv.shape

    ints = volume.ordered_intersections
    if intersection_labels is None:
        int_lbls = list(ints)
    else:
        int_lbls = [intersection_labels.get(i, i) for i in ints]
    day_lbls = list(day_labels) if day_labels is not None else list(volume.weekdays)

    if title is None:
        arrow = "≥" if spec.bad_direction == "above" else "≤"
        dir_part = f" — {direction_label}" if direction_label else ""
        title = (
            f"{moe_label} Bottleneck Map{dir_part}"
            f"<br><sub>Threshold {arrow} {spec.threshold:.0f}{_unit(spec)}  ·  "
            f"color = severity  ·  opacity = weeks exceeded / {_max_weeks(volume)}</sub>"
        )

    # Fast slot lookup for hover text.
    stats = volume.slot_stats
    stat_lookup = {}
    if stats is not None and not stats.empty:
        for _, r in stats.iterrows():
            stat_lookup[(r["intersection_id"], r["weekday"], r["hour_slot"])] = r

    fig = go.Figure()

    hover_x, hover_y, hover_z, hover_text = [], [], [], []
    for i in range(n_int):
        for d in range(n_days):
            for h in range(n_hours):
                intensity = iv[i, d, h]
                if np.isnan(intensity):
                    continue
                opacity = float(ov[i, d, h])
                color = _intensity_to_color(intensity)
                xs, ys, zs, ii, jj, kk = _single_cube_arrays(i, d, h, cube_size)
                fig.add_trace(
                    go.Mesh3d(
                        x=xs, y=ys, z=zs, i=ii, j=jj, k=kk,
                        color=color, opacity=opacity, flatshading=True,
                        hoverinfo="skip", showlegend=False,
                        lighting=dict(ambient=0.65, diffuse=0.8, specular=0.1),
                    )
                )
                key = (ints[i], volume.weekdays[d], volume.hour_slots[h])
                row = stat_lookup.get(key)
                hover_x.append(i)
                hover_y.append(d)
                hover_z.append(h)
                if row is not None:
                    hover_text.append(
                        _voxel_hover(
                            row, spec, int_lbls[i], day_lbls[d],
                            hour_range_label(h, hour_start),
                        )
                    )
                else:
                    hover_text.append(
                        f"<b>{int_lbls[i]}</b><br>{day_lbls[d]} · "
                        f"{hour_range_label(h, hour_start)}<br>severity {intensity:.2f}"
                    )

    # Hover layer (invisible markers at voxel centers).
    if hover_x:
        fig.add_trace(
            go.Scatter3d(
                x=hover_x, y=hover_y, z=hover_z, mode="markers",
                marker=dict(size=6, color="rgba(0,0,0,0)"),
                hovertext=hover_text, hoverinfo="text", showlegend=False,
            )
        )

    # Dummy colorbar trace (carries the YlOrRd scale; otherwise invisible).
    fig.add_trace(
        go.Scatter3d(
            x=[hover_x[0]] if hover_x else [0],
            y=[hover_y[0]] if hover_y else [0],
            z=[hover_z[0]] if hover_z else [0],
            mode="markers",
            marker=dict(
                size=0.1,
                color=[0.0, 1.0],
                colorscale=COLORSCALE,
                cmin=0.0, cmax=1.0,
                showscale=True,
                colorbar=dict(
                    title="Severity<br>(excess<br>above thr)",
                    tickvals=[0.0, 0.5, 1.0],
                    ticktext=["just over", "mid", "extreme"],
                    len=0.6, x=1.02,
                ),
                opacity=0.0,
            ),
            hoverinfo="skip", showlegend=False,
        )
    )

    fig.update_layout(
        title=title,
        height=720,
        margin=dict(l=0, r=0, t=70, b=0),
        scene=dict(
            aspectmode="cube",
            xaxis=dict(
                title="Intersection",
                tickmode="array",
                tickvals=list(range(n_int)),
                ticktext=int_lbls,
                range=[-0.6, n_int - 0.4],
            ),
            yaxis=dict(
                title="Day of week",
                tickmode="array",
                tickvals=list(range(n_days)),
                ticktext=day_lbls,
                range=[-0.6, n_days - 0.4],
            ),
            zaxis=dict(
                title="Hour of day",
                tickmode="array",
                tickvals=list(range(n_hours)),
                ticktext=[_hour_tick_label(h, hour_start) for h in range(n_hours)],
                range=[-0.6, n_hours - 0.4],
            ),
            camera=dict(eye=dict(x=1.8, y=1.8, z=1.2)),
        ),
    )
    return fig


def _max_weeks(volume: BottleneckVolume) -> int:
    stats = volume.slot_stats
    if stats is None or stats.empty:
        return 8
    return int(stats["n_weeks"].max())
