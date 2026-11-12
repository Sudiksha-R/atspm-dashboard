from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum


class Direction(str, Enum):
    LR = "LR"
    RL = "RL"


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


@dataclass(frozen=True)
class AnalysisRequest:
    corridor_id: str
    direction: Direction
    ordered_intersections: list[str]
    selected_intersections: list[str]
    phase_group: str
    moe_name: str
    weekday_filter: list[str]
    hour_slot_filter: list[str]
    start_date: date
    end_date: date
