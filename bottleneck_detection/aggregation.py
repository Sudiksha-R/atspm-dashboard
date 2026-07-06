"""Aggregate raw hourly observations into per-(intersection, day, hour) slot stats.

A "slot" is one (intersection, day-of-week, hour-of-day) cell of the 3D volume.
Within the analysis window there are typically several weeks of observations for
the same slot (e.g. 8 Mondays at 8-9am). This module collapses those repeated
weeks into a single record carrying the mean, std, week count, and the raw
per-week values (the per-week list is what threshold.py needs to compute
persistence, and what the distribution panel plots).

Integration note: the input is a plain ``list[RawObservation]`` — swap the
fixture generator for a DB query and this step is unchanged.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from .schemas import BottleneckRequest, RawObservation


def observations_to_frame(rows: Iterable[RawObservation]) -> pd.DataFrame:
    """Flatten ``RawObservation`` records into a tidy DataFrame."""
    return pd.DataFrame(
        {
            "intersection_id": r.intersection_id,
            "corridor_order": r.corridor_order,
            "direction": r.direction.value,
            "week_id": r.week_id,
            "weekday": r.weekday,
            "hour_slot": r.hour_slot,
            "moe_name": r.moe_name,
            "raw_value": r.raw_value,
        }
        for r in rows
    )


def aggregate_by_slot(
    rows: Iterable[RawObservation],
    request: BottleneckRequest,
) -> pd.DataFrame:
    """Collapse repeated weeks into one row per (intersection, weekday, hour_slot).

    Applies the request's selection filters (selected intersections, weekday and
    hour-slot filters, MoE) first, then groups. Slots with fewer than
    ``request.min_weeks`` weeks of data are dropped.

    Returns a DataFrame with columns:
        intersection_id, weekday, hour_slot,
        mean_value, std_value, n_weeks, weekly_values (list[float])
    """
    df = observations_to_frame(rows)

    df = df[df["moe_name"] == request.threshold_spec.moe_name]
    df = df[df["intersection_id"].isin(request.selected_intersections)]
    if request.weekday_filter:
        df = df[df["weekday"].isin(request.weekday_filter)]
    if request.hour_slot_filter:
        df = df[df["hour_slot"].isin(request.hour_slot_filter)]

    if df.empty:
        return pd.DataFrame(
            columns=[
                "intersection_id", "weekday", "hour_slot",
                "mean_value", "std_value", "n_weeks", "weekly_values",
            ]
        )

    grouped = df.groupby(["intersection_id", "weekday", "hour_slot"], sort=False)
    records = []
    for (intersection_id, weekday, hour_slot), g in grouped:
        values = g.sort_values("week_id")["raw_value"].to_numpy(dtype=float)
        if len(values) < request.min_weeks:
            continue
        records.append(
            {
                "intersection_id": intersection_id,
                "weekday": weekday,
                "hour_slot": hour_slot,
                "mean_value": float(np.mean(values)),
                "std_value": float(np.std(values, ddof=0)),
                "n_weeks": int(len(values)),
                "weekly_values": values.tolist(),
            }
        )
    return pd.DataFrame(records)
