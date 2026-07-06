"""Apply an unfavorability threshold and compute the dual encoding.

Given the per-slot aggregates from ``aggregation.aggregate_by_slot`` and a
``ThresholdSpec``, this stage decides which slots are bottlenecks and computes
the two visual channels the 3D plot uses:

  * intensity (color)   = how far the mean is past the threshold, normalised to
                          [0, 1] against the MoE's hard physical worst-reference.
  * opacity (alpha)     = persistence = fraction of weeks the threshold was
                          crossed, mapped to [0.35, 1.0] so even a 1-in-8 voxel
                          stays faintly visible.

See DESIGN.md S5 for the formulas. The MoE direction ("above"/"below") is read
from the ThresholdSpec, so the same code handles AOR% (high = bad) and, e.g.,
AOG% / throughput (low = bad) without change.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .schemas import ThresholdSpec

_MIN_OPACITY = 0.35


def _excess(mean_value: float, spec: ThresholdSpec) -> float:
    """Signed-to-zero distance past the threshold in the unfavorable direction."""
    if spec.bad_direction == "above":
        return max(mean_value - spec.threshold, 0.0)
    return max(spec.threshold - mean_value, 0.0)


def _is_bottleneck(mean_value: float, spec: ThresholdSpec) -> bool:
    if spec.bad_direction == "above":
        return mean_value >= spec.threshold
    return mean_value <= spec.threshold


def _intensity(raw_excess: float, spec: ThresholdSpec) -> float:
    """Normalise excess to [0, 1] against the threshold→worst_ref span."""
    span = abs((spec.worst_ref if spec.worst_ref is not None else 0.0) - spec.threshold)
    if span <= 0:
        return 1.0 if raw_excess > 0 else 0.0
    return float(min(max(raw_excess / span, 0.0), 1.0))


def _pct_exceeding(weekly_values: list[float], spec: ThresholdSpec) -> float:
    arr = np.asarray(weekly_values, dtype=float)
    if arr.size == 0:
        return 0.0
    if spec.bad_direction == "above":
        crossed = arr >= spec.threshold
    else:
        crossed = arr <= spec.threshold
    return float(np.mean(crossed))


def apply_threshold(slot_stats: pd.DataFrame, spec: ThresholdSpec) -> pd.DataFrame:
    """Return a copy of ``slot_stats`` enriched with bottleneck/intensity/opacity.

    Added columns:
        is_bottleneck (bool), pct_exceeding (float [0,1]),
        raw_excess (float), intensity (float|NaN), opacity (float|NaN)

    For non-bottleneck slots intensity and opacity are NaN so they're omitted
    from the 3D volume entirely.
    """
    df = slot_stats.copy()
    if df.empty:
        for col in ("is_bottleneck", "pct_exceeding", "raw_excess", "intensity", "opacity"):
            df[col] = pd.Series(dtype="float64" if col != "is_bottleneck" else "bool")
        return df

    is_bn, pct, excess, intensity, opacity = [], [], [], [], []
    for _, row in df.iterrows():
        mean_value = float(row["mean_value"])
        bn = _is_bottleneck(mean_value, spec)
        ex = _excess(mean_value, spec)
        p = _pct_exceeding(row["weekly_values"], spec)
        is_bn.append(bn)
        pct.append(p)
        excess.append(ex)
        if bn:
            intensity.append(_intensity(ex, spec))
            opacity.append(_MIN_OPACITY + (1.0 - _MIN_OPACITY) * p)
        else:
            intensity.append(np.nan)
            opacity.append(np.nan)

    df["is_bottleneck"] = is_bn
    df["pct_exceeding"] = pct
    df["raw_excess"] = excess
    df["intensity"] = intensity
    df["opacity"] = opacity
    return df
