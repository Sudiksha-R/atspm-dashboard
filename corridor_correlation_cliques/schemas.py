"""Data contract for the correlation-clique voxel prototype.

This package is shipped **stand-alone** to the dashboard UI developer, so it deliberately
carries its own copy of the two data types it needs rather than importing them from the
sibling `synchrony` package. They are byte-for-byte the same `Direction` / `RawObservation`
used everywhere else in the wider prototype, so a `RawObservation` produced here can be
fed to the `synchrony` pipeline unchanged (and vice versa) -- but this folder has **no
import dependency** on anything outside itself.

`RawObservation` is the single record type the whole pipeline consumes: one hourly MoE
value for one (intersection, week, weekday, hour_slot, direction). A real integration
replaces the synthetic `fixture_data` generator with a DB query that yields these same
rows (or the equivalent long-format DataFrame); nothing downstream changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum


class Direction(str, Enum):
    """Travel direction along the corridor.

    LR = left-to-right (eastbound here, Phase 2); RL = right-to-left (westbound, Phase 6).
    The two are *different physical traffic streams* at the same intersections, so every
    analysis is run once per direction.
    """

    LR = "LR"  # left-to-right / eastbound / Phase 2
    RL = "RL"  # right-to-left / westbound / Phase 6


@dataclass(frozen=True)
class RawObservation:
    """One hourly raw MoE record -- the atomic unit the pipeline consumes.

    Fields match the wider prototype's data contract exactly:

    * `intersection_id`  -- any string id (here the numeric signal ids 1240..1245)
    * `corridor_order`   -- 0-based position along the corridor (west -> east); sets x-axis order
    * `direction`        -- `Direction.LR` or `Direction.RL`
    * `week_id`          -- 1-based week number within the analysis window (1..12 here)
    * `date`             -- calendar date of the observation
    * `weekday`          -- 3-letter title-case abbreviation: "Mon".."Sun"
    * `hour_slot`        -- "HH:MM-HH:MM" one-hour slot, e.g. "08:00-09:00"
    * `moe_name`         -- measure name; "volume" (hourly vehicle counts) for this prototype
    * `raw_value`        -- the measured value (vehicles/hour)
    """

    intersection_id: str
    corridor_order: int
    direction: Direction
    week_id: int
    date: date
    weekday: str
    hour_slot: str
    moe_name: str
    raw_value: float
