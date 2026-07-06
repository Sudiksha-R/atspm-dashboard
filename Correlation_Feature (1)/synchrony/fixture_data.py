"""Hand-crafted toy fixture dataset for validating the synchrony pipeline.

Every value comes from an explicit, traceable arithmetic formula (no random sampling)
so any cell is hand-recomputable. Two orthogonal base signals drive all series:

  pattern_X(weekday, hour)  -- peak-hour/weekday structure, constant across weeks.
  pattern_Y(week)           -- week-level drift, orthogonal to pattern_X by construction.

Six intersections: 1240-1245 (A through F), 7 weeks x {Mon..Fri} x 2 hour slots.

Driver assignments — deliberately alternating X/Y for LR so only A-B share a driver,
and grouped Y/Y for D/E/F in RL so only E-D and F-E share a driver:

  LR / phase2 (eastbound):      RL / phase6 (westbound):
    A = pattern_X  ──┐ strong     A = pattern_X  -- weak B→A
    B = pattern_X  ──┘            B = pattern_Y  -- weak C→B
    C = pattern_Y  -- weak B-C    C = pattern_X  -- weak D→C
    D = pattern_X  -- weak C-D    D = pattern_Y  ──┐ strong E→D
    E = pattern_Y  -- weak D-E    E = pattern_Y  ──┤ strong F→E
    F = pattern_X  -- weak E-F    F = pattern_Y  ──┘

Expected outcomes (numerically confirmed):

  LR / phase2:
    A-B  (1240__1241): pearson ~ +0.82, spearman ~ +0.80  -> strong_positive_sync
    B-C  (1241__1242): pearson ~ -0.05, spearman ~ -0.06  -> weak_or_no_sync
    C-D  (1242__1243): pearson ~ -0.02, spearman ~ -0.02  -> weak_or_no_sync
    D-E  (1243__1244): pearson ~ -0.02, spearman ~ -0.03  -> weak_or_no_sync
    E-F  (1244__1245): pearson ~ -0.04, spearman ~ -0.03  -> weak_or_no_sync

  RL / phase6:
    B-A  (1241__1240): pearson ~ -0.03, spearman ~ -0.06  -> weak_or_no_sync
    C-B  (1242__1241): pearson ~ -0.02, spearman ~ -0.02  -> weak_or_no_sync
    D-C  (1243__1242): pearson ~ -0.02, spearman ~ -0.02  -> weak_or_no_sync
    E-D  (1244__1243): pearson ~ +0.87, spearman ~ +0.88  -> strong_positive_sync
    F-E  (1245__1244): pearson ~ +0.89, spearman ~ +0.89  -> strong_positive_sync
"""

from __future__ import annotations

from datetime import date, timedelta

from .schemas import Direction, RawObservation

CORRIDOR_ID = "demo_corridor_sr758"
PHASE_GROUP = "phase2"
MOE_NAME = "maxOuts"
DIRECTION = Direction.LR

ORDERED_INTERSECTIONS = ["1240", "1241", "1242", "1243", "1244", "1245"]
"""Corridor order, left-to-right (A through F)."""

INTERSECTION_LABELS = {
    "1240": "A", "1241": "B", "1242": "C",
    "1243": "D", "1244": "E", "1245": "F",
}
"""Friendly labels for notebook narration only -- not part of the data contract."""

WEEKDAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri"]
HOUR_SLOTS = ["08:00-09:00", "09:00-10:00"]
N_WEEKS = 7

WEEK1_MONDAY = date(2026, 1, 5)

MAPPED_INTERSECTIONS = ORDERED_INTERSECTIONS
"""All six intersections carry real-world coordinates."""


def _pattern_x(weekday_idx: int, hour_idx: int) -> int:
    return 3 + (2 if weekday_idx in (0, 4) else 0) + (2 if hour_idx == 0 else 0)


def _pattern_y(week_id: int) -> float:
    return 2 + 1.5 * (week_id % 4)


def _noise(week_id: int, weekday_idx: int, hour_idx: int, intersection_index: int) -> int:
    """Deterministic ±1 perturbation. phase2 uses indices 1-6; phase6 uses 8-13."""
    return ((week_id * 7 + weekday_idx * 13 + hour_idx * 5 + intersection_index * 17) % 3) - 1


def _date_for(week_id: int, weekday_idx: int) -> date:
    return WEEK1_MONDAY + timedelta(weeks=week_id - 1, days=weekday_idx)


def generate_fixture_observations() -> list[RawObservation]:
    """Phase-2 (LR / eastbound) fixture -- 420 rows (6 × 7 × 5 × 2).

    A and B share pattern_X -> strong A-B sync.
    C, D, E, F alternate X/Y so every other adjacent pair is orthogonal -> all weak.
    """

    rows: list[RawObservation] = []
    for week_id in range(1, N_WEEKS + 1):
        for weekday_idx, weekday in enumerate(WEEKDAY_LABELS):
            for hour_idx, hour_slot in enumerate(HOUR_SLOTS):
                px = _pattern_x(weekday_idx, hour_idx)
                py = _pattern_y(week_id)
                obs_date = _date_for(week_id, weekday_idx)
                values = {
                    "1240": round(2.0 * px + 3 + _noise(week_id, weekday_idx, hour_idx, 1)),  # A: X
                    "1241": round(1.5 * px + 6 + _noise(week_id, weekday_idx, hour_idx, 2)),  # B: X -> A-B strong
                    "1242": round(2.0 * py + 4 + _noise(week_id, weekday_idx, hour_idx, 3)),  # C: Y -> B-C weak
                    "1243": round(1.8 * px + 2 + _noise(week_id, weekday_idx, hour_idx, 4)),  # D: X -> C-D weak
                    "1244": round(2.2 * py + 5 + _noise(week_id, weekday_idx, hour_idx, 5)),  # E: Y -> D-E weak
                    "1245": round(1.7 * px + 3 + _noise(week_id, weekday_idx, hour_idx, 6)),  # F: X -> E-F weak
                }
                for corridor_order, intersection_id in enumerate(ORDERED_INTERSECTIONS):
                    rows.append(
                        RawObservation(
                            intersection_id=intersection_id,
                            corridor_order=corridor_order,
                            direction=DIRECTION,
                            week_id=week_id,
                            date=obs_date,
                            weekday=weekday,
                            hour_slot=hour_slot,
                            moe_name=MOE_NAME,
                            raw_value=float(max(0, values[intersection_id])),
                        )
                    )
    return rows


PHASE_GROUP_REVERSE = "phase6"
DIRECTION_REVERSE = Direction.RL


def generate_phase6_observations() -> list[RawObservation]:
    """Phase-6 (RL / westbound) fixture -- 420 rows (6 × 7 × 5 × 2).

    D, E, F all share pattern_Y -> strong E→D and F→E westbound sync.
    A, B, C alternate X/Y so B→A, C→B, D→C are all weak.
    """

    rows: list[RawObservation] = []
    for week_id in range(1, N_WEEKS + 1):
        for weekday_idx, weekday in enumerate(WEEKDAY_LABELS):
            for hour_idx, hour_slot in enumerate(HOUR_SLOTS):
                px = _pattern_x(weekday_idx, hour_idx)
                py = _pattern_y(week_id)
                obs_date = _date_for(week_id, weekday_idx)
                values = {
                    "1240": round(1.8 * px + 5 + _noise(week_id, weekday_idx, hour_idx, 8)),   # A: X -> B→A weak
                    "1241": round(2.0 * py + 3 + _noise(week_id, weekday_idx, hour_idx, 9)),   # B: Y -> C→B weak
                    "1242": round(1.6 * px + 7 + _noise(week_id, weekday_idx, hour_idx, 10)),  # C: X -> D→C weak
                    "1243": round(1.9 * py + 4 + _noise(week_id, weekday_idx, hour_idx, 11)),  # D: Y -> E→D strong
                    "1244": round(2.1 * py + 2 + _noise(week_id, weekday_idx, hour_idx, 12)),  # E: Y -> F→E strong
                    "1245": round(1.7 * py + 6 + _noise(week_id, weekday_idx, hour_idx, 13)),  # F: Y
                }
                for corridor_order, intersection_id in enumerate(MAPPED_INTERSECTIONS):
                    rows.append(
                        RawObservation(
                            intersection_id=intersection_id,
                            corridor_order=corridor_order,
                            direction=DIRECTION_REVERSE,
                            week_id=week_id,
                            date=obs_date,
                            weekday=weekday,
                            hour_slot=hour_slot,
                            moe_name=MOE_NAME,
                            raw_value=float(max(0, values[intersection_id])),
                        )
                    )
    return rows
