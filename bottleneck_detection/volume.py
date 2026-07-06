"""Assemble the enriched slot stats into a 3D (intersection x day x hour) volume.

Two float arrays of shape ``(n_intersections, n_days, n_hours)`` are produced:

  * intensity_volume[i, d, h] — severity in [0, 1], or NaN if the slot is not a
                                bottleneck (or has no data).
  * opacity_volume[i, d, h]   — persistence in [0.35, 1.0], or NaN.

Axis convention matches synchrony/voxel_viz.py exactly:
  x (i) = intersection (corridor order), y (d) = day of week (0=Mon),
  z (h) = hour slot (0 = HOUR_START:00).

The full enriched DataFrame is carried along on the BottleneckVolume so the
renderer's hover text and the distribution panel can look up any voxel's stats.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from . import fixture_data
from .schemas import BottleneckRequest, BottleneckVolume


def build_intensity_volume(
    slot_stats: pd.DataFrame,
    request: BottleneckRequest,
    *,
    weekdays: list[str] | None = None,
    hour_slots: list[str] | None = None,
) -> BottleneckVolume:
    """Build a ``BottleneckVolume`` from threshold-enriched slot stats.

    ``slot_stats`` must already carry the ``intensity`` / ``opacity`` columns
    (i.e. it has been through ``threshold.apply_threshold``). The volume axes are
    laid out over ``request.ordered_intersections`` x ``weekdays`` x
    ``hour_slots``; the latter two default to the canonical Mon-Sun /
    06:00-22:00 grids from ``fixture_data``.
    """
    ordered = list(request.ordered_intersections)
    weekdays = list(weekdays) if weekdays is not None else list(fixture_data.DAY_LABELS)
    hour_slots = list(hour_slots) if hour_slots is not None else list(fixture_data.HOUR_SLOTS)

    n_int, n_days, n_hours = len(ordered), len(weekdays), len(hour_slots)
    intensity_volume = np.full((n_int, n_days, n_hours), np.nan, dtype=float)
    opacity_volume = np.full((n_int, n_days, n_hours), np.nan, dtype=float)

    i_index = {iid: i for i, iid in enumerate(ordered)}
    d_index = {wd: d for d, wd in enumerate(weekdays)}
    h_index = {hs: h for h, hs in enumerate(hour_slots)}

    if not slot_stats.empty:
        for _, row in slot_stats.iterrows():
            if not bool(row.get("is_bottleneck", False)):
                continue
            i = i_index.get(row["intersection_id"])
            d = d_index.get(row["weekday"])
            h = h_index.get(row["hour_slot"])
            if i is None or d is None or h is None:
                continue
            intensity_volume[i, d, h] = float(row["intensity"])
            opacity_volume[i, d, h] = float(row["opacity"])

    return BottleneckVolume(
        intensity_volume=intensity_volume,
        opacity_volume=opacity_volume,
        n_intersections=n_int,
        n_days=n_days,
        n_hours=n_hours,
        ordered_intersections=ordered,
        weekdays=weekdays,
        hour_slots=hour_slots,
        slot_stats=slot_stats,
    )
