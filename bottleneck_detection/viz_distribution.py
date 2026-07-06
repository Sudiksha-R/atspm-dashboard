"""Companion distribution panel for a single voxel.

The 3D plot answers "where are the bottlenecks"; this panel answers "what does
one voxel actually look like week to week". It plots the per-week MoE values for
a chosen voxel as a bar chart, with the threshold line and the 8-week mean
overlaid — the prototype equivalent of the histogram shown in the proposal
slides. In the dashboard this would be driven by a click on a 3D voxel.

Bars that cross the threshold are drawn in the YlOrRd "alert" red; compliant
weeks are grey, so persistence is visible at a glance.
"""

from __future__ import annotations

import numpy as np
import plotly.graph_objects as go

from .schemas import BottleneckVolume, ThresholdSpec
from .viz_3d import _unit, hour_range_label

_ALERT_COLOR = "rgb(189,0,38)"      # YlOrRd dark red
_OK_COLOR = "rgb(150,150,150)"
_MEAN_COLOR = "rgb(255,193,7)"       # gold


def _lookup_row(volume: BottleneckVolume, i: int, d: int, h: int):
    iid = volume.ordered_intersections[i]
    wd = volume.weekdays[d]
    hs = volume.hour_slots[h]
    stats = volume.slot_stats
    mask = (
        (stats["intersection_id"] == iid)
        & (stats["weekday"] == wd)
        & (stats["hour_slot"] == hs)
    )
    sub = stats[mask]
    if sub.empty:
        return None, iid, wd, h
    return sub.iloc[0], iid, wd, h


def render_worst_voxel_distribution(
    volume: BottleneckVolume,
    spec: ThresholdSpec,
    *,
    voxel: tuple[int, int, int] | None = None,
    intersection_labels: dict[str, str] | None = None,
    moe_label: str = "AOR%",
    hour_start: int = 6,
) -> go.Figure:
    """Bar chart of the per-week MoE values for one voxel.

    If ``voxel`` is None the volume's worst (highest-intensity) voxel is used.
    Returns an empty annotated figure if there are no bottleneck voxels.
    """
    if voxel is None:
        voxel = volume.worst_voxel()
    if voxel is None:
        fig = go.Figure()
        fig.add_annotation(
            text="No bottleneck voxels at this threshold.",
            showarrow=False, font=dict(size=16),
        )
        fig.update_layout(height=360, title=f"{moe_label} — distribution")
        return fig

    i, d, h = voxel
    row, iid, wd, _ = _lookup_row(volume, i, d, h)
    label = iid if intersection_labels is None else intersection_labels.get(iid, iid)
    hour_lbl = hour_range_label(h, hour_start)
    unit = _unit(spec)

    if row is None:
        fig = go.Figure()
        fig.add_annotation(text="Voxel has no aggregated data.", showarrow=False)
        fig.update_layout(height=360)
        return fig

    weekly = np.asarray(row["weekly_values"], dtype=float)
    weeks = list(range(1, len(weekly) + 1))
    mean_value = float(row["mean_value"])

    if spec.bad_direction == "above":
        crossed = weekly >= spec.threshold
        arrow = "≥"
    else:
        crossed = weekly <= spec.threshold
        arrow = "≤"
    colors = [_ALERT_COLOR if c else _OK_COLOR for c in crossed]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=weeks, y=weekly, marker_color=colors,
            hovertemplate="Week %{x}<br>" + moe_label + ": %{y:.1f}" + unit + "<extra></extra>",
            name=moe_label,
        )
    )

    # Threshold line.
    fig.add_hline(
        y=spec.threshold, line=dict(color=_ALERT_COLOR, width=2, dash="dash"),
        annotation_text=f"threshold {arrow} {spec.threshold:.0f}{unit}",
        annotation_position="top left",
    )
    # Mean marker.
    fig.add_trace(
        go.Scatter(
            x=weeks, y=[mean_value] * len(weeks), mode="lines",
            line=dict(color=_MEAN_COLOR, width=2),
            name=f"8-wk mean ({mean_value:.1f}{unit})",
            hoverinfo="skip",
        )
    )

    n_cross = int(crossed.sum())
    fig.update_layout(
        title=(
            f"{moe_label} — {label} · {wd} · {hour_lbl}"
            f"<br><sub>{n_cross}/{len(weekly)} weeks exceeded  ·  "
            f"mean {mean_value:.1f}{unit}  ·  threshold {arrow} {spec.threshold:.0f}{unit}</sub>"
        ),
        xaxis=dict(title="Week", tickmode="array", tickvals=weeks),
        yaxis=dict(title=f"{moe_label}", range=[0, 100] if unit == "%" else None),
        height=380,
        bargap=0.25,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    return fig
