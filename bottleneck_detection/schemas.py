"""Data types for the bottleneck detection module.

These are plain frozen dataclasses (no framework dependency) so they can be
dropped into any backend (FastAPI, Flask, Django) unchanged.

Integration note: RawObservation is imported from synchrony.schemas so the
bottleneck module consumes the exact same observation type the rest of the
pipeline produces. When wiring to a real DB, replace the fixture generator
with a query that returns list[RawObservation] — no other change needed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd


# ── Re-export the shared observation type ─────────────────────────────────────
# The bottleneck module consumes standard RawObservation records
# (intersection_id, direction, week_id, weekday, hour_slot, moe_name, raw_value).
# Importing here makes it available from bottleneck_detection.schemas directly.
from synchrony.schemas import Direction, RawObservation  # noqa: F401


# ── Threshold configuration ────────────────────────────────────────────────────

@dataclass(frozen=True)
class ThresholdSpec:
    """Defines what counts as an "unfavorable" MoE value.

    bad_direction:
      "above" — values >= threshold are unfavorable (e.g. AOR%, wait time)
      "below" — values <= threshold are unfavorable (e.g. AOG%, throughput)

    worst_ref:
      The hard physical limit used to normalise intensity to [0, 1].
      For "above" MoEs this is typically 100 (or the measured maximum).
      For "below" MoEs this is 0 (or the measured minimum).
      Pass None to auto-set from the data at analysis time.
    """

    moe_name: str
    threshold: float
    bad_direction: str   # "above" or "below"
    worst_ref: float | None = None


# ── Analysis request ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class BottleneckRequest:
    """Everything needed to run one bottleneck detection analysis.

    Mirrors AnalysisRequest from synchrony.schemas in structure so the two
    modules share the same conceptual API surface.

    Integration note: weekday_filter and hour_slot_filter map directly to
    SQL WHERE clauses:
        WHERE weekday IN (:weekday_filter)
        AND hour_slot IN (:hour_slot_filter)
        AND obs_date BETWEEN :start_date AND :end_date
    """

    corridor_id: str
    direction: Direction
    ordered_intersections: list[str]
    selected_intersections: list[str]
    phase_group: str
    weekday_filter: list[str]        # e.g. ["Mon","Tue","Wed","Thu","Fri"]
    hour_slot_filter: list[str]      # e.g. ["06:00-07:00", ..., "21:00-22:00"]
    start_date: date
    end_date: date
    threshold_spec: ThresholdSpec
    min_weeks: int = 2
    """Suppress a voxel entirely if it has fewer than this many weeks of data."""


# ── Per-slot aggregated statistics ────────────────────────────────────────────

@dataclass
class SlotStats:
    """Statistics for one (intersection, day-of-week, hour) voxel.

    Produced by aggregation.aggregate_by_slot(), enriched with intensity
    and opacity by threshold.apply_threshold(). Both steps return
    pd.DataFrame for convenient bulk operations; this dataclass documents
    the column schema of that DataFrame.
    """

    intersection_id: str
    day_of_week: str       # "Mon", "Tue", ...
    hour_slot: str         # "06:00-07:00", etc.
    mean_value: float      # mean MoE across weeks in the analysis window
    std_value: float       # std dev across weeks (shows week-to-week variability)
    n_weeks: int           # number of weeks with data for this slot
    weekly_values: list[float]   # individual per-week values (for pct_exceeding + distribution panel)
    # Fields added by apply_threshold():
    is_bottleneck: bool = False
    pct_exceeding: float = 0.0   # fraction of weeks crossing the threshold [0, 1]
    raw_excess: float = 0.0      # |mean_value - threshold|, 0 if not a bottleneck
    intensity: float | None = None   # [0, 1] severity; None if not a bottleneck
    opacity: float | None = None     # [0.35, 1.0] persistence; None if not a bottleneck


# ── 3D volume ─────────────────────────────────────────────────────────────────

@dataclass
class BottleneckVolume:
    """The assembled 3D bottleneck volume for one direction.

    intensity_volume[i, d, h] = float [0,1] if voxel (intersection i, day d, hour h)
                                 is a bottleneck, else np.nan
    opacity_volume[i, d, h]   = float [0.35, 1.0] if bottleneck, else np.nan

    Axes (matching synchrony/voxel_viz.py convention):
      x (i) = intersection index, corridor order
      y (d) = day of week index (0=Mon … 6=Sun)
      z (h) = hour slot index (0 = hour_start:00, 1 = hour_start+1:00, …)

    slot_stats is the full enriched DataFrame from aggregation + threshold steps,
    kept here so the distribution panel and hover tooltips can look up any voxel.
    """

    intensity_volume: np.ndarray       # shape (n_int, n_days, n_hours), float, NaN = no data
    opacity_volume: np.ndarray         # same shape
    n_intersections: int
    n_days: int
    n_hours: int
    ordered_intersections: list[str]   # intersection IDs in corridor order
    weekdays: list[str]                # day labels matching axis d
    hour_slots: list[str]              # hour slot strings matching axis h
    slot_stats: pd.DataFrame           # full enriched slot statistics

    @property
    def n_bottleneck_voxels(self) -> int:
        return int(np.sum(~np.isnan(self.intensity_volume)))

    def worst_voxel(self) -> tuple[int, int, int] | None:
        """(i, d, h) index of the voxel with highest intensity, or None if empty."""
        if self.n_bottleneck_voxels == 0:
            return None
        flat = np.nanargmax(self.intensity_volume)
        return tuple(np.unravel_index(flat, self.intensity_volume.shape))
