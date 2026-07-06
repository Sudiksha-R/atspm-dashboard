"""Synthetic 8-week AOR% fixture for the bottleneck detection prototype.

Produces ``list[RawObservation]`` (the same record type the rest of the pipeline
consumes, re-exported via ``schemas``), one per
(intersection, week, weekday, hour) slot. For the Bee Ridge demo corridor that
is **6 intersections x 8 weeks x 7 days x 16 hours = 5 376 rows per direction**.

Integration note: this whole module is a stand-in for a real database query.
To go live, replace ``generate_lr_aor_observations()`` /
``generate_rl_aor_observations()`` with a query returning ``list[RawObservation]``
for the requested corridor / direction / window. Nothing downstream changes.

------------------------------------------------------------------------------
Design (see DESIGN.md S6 for the full rationale). Each voxel's AOR% is

    aor = floor
        + (am_raw * am_mult + mid_raw + pm_raw * pm_mult) * dow_scale
        + week_trend            # E eastbound only, worsens over the 8 weeks
        + bad_week_bias         # weeks 3 & 7, corridor-wide +4pp
        + noise                 # N(0, 5pp), seeded
    aor = clip(aor, 0, 100)

where am/mid/pm_raw are smooth Gaussians of the slot-center hour (NOT step
functions), and the per-intersection am_mult / pm_mult encode the "platoon
leapfrog" pattern: a platoon clears one intersection on green but disperses and
arrives on red at the next, so high/low AOR alternates along the corridor,
differently for each travel direction.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import numpy as np

from .schemas import Direction, RawObservation

# ── Corridor identity (shared with synchrony.fixture_data) ─────────────────────

CORRIDOR_ID = "demo_corridor_sr758"

ORDERED_INTERSECTIONS = ["1240", "1241", "1242", "1243", "1244", "1245"]
"""Corridor order, west-to-east (A through F)."""

INTERSECTION_LABELS = {
    "1240": "McIntosh Rd.",
    "1241": "Honore Ave.",
    "1242": "Woodmont Dr.",
    "1243": "Cattleridge Blvd.",
    "1244": "Cattlemen Rd.",
    "1245": "I-75 SB Ramp",
}
"""Street names along the Bee Ridge Rd demo corridor (A-F)."""

DAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

HOUR_START = 6
N_HOURS = 16
HOUR_SLOTS = [
    f"{HOUR_START + h:02d}:00-{HOUR_START + h + 1:02d}:00" for h in range(N_HOURS)
]
"""16 one-hour slots: "06:00-07:00" ... "21:00-22:00"."""

N_WEEKS = 8
WEEK1_MONDAY = date(2026, 1, 5)

MOE_NAME = "aorPct"

# ── Diurnal Gaussian parameters (amplitude, center hour, sigma) ────────────────
# RL is physically asymmetric: weaker AM (fewer drive west in the morning),
# stronger + later PM (heavier, longer evening homebound flow). See DESIGN.md S6a.

_DIURNAL = {
    Direction.LR: {
        "floor": 40.0,
        "am": (33.0, 9.0, 0.9),
        "mid": (15.0, 13.0, 1.2),
        "pm": (28.0, 17.25, 0.9),
    },
    Direction.RL: {
        "floor": 40.0,
        "am": (15.0, 9.0, 0.9),
        "mid": (14.0, 13.0, 1.2),
        "pm": (32.0, 17.5, 1.2),
    },
}

# ── Platoon-leapfrog per-intersection multipliers (am_mult, pm_mult) ───────────
# DESIGN.md S6c. Eastbound: AM bottlenecks at A/C/E, PM at B/D/F.
# Westbound: no AM bottlenecks (low amplitude), PM at F/D/B.

_MULT = {
    Direction.LR: {
        "1240": (1.20, 0.85),   # A
        "1241": (0.70, 1.15),   # B
        "1242": (1.15, 0.75),   # C
        "1243": (0.65, 1.25),   # D
        "1244": (1.25, 0.90),   # E
        "1245": (0.80, 1.10),   # F
    },
    Direction.RL: {
        "1240": (1.15, 0.75),   # A
        "1241": (0.80, 1.15),   # B
        "1242": (1.10, 0.70),   # C
        "1243": (0.75, 1.20),   # D
        "1244": (1.20, 0.75),   # E
        "1245": (0.90, 1.30),   # F
    },
}

# ── Day-of-week scaling (applied to peak contributions, not the floor) ─────────
_DOW_SCALE = {
    "Mon": 1.10, "Tue": 0.95, "Wed": 1.00, "Thu": 0.98,
    "Fri": 1.08, "Sat": 0.55, "Sun": 0.35,
}

# ── Complexity-layer constants ─────────────────────────────────────────────────
_BAD_WEEKS = {3, 7}             # corridor-wide +4pp poor-timing periods
_BAD_WEEK_BIAS = 4.0
_TREND_PER_WEEK = 0.8          # E eastbound AM worsening (pp / week, 0 in week 1)
_NOISE_SIGMA = 5.0
_MIDDAY_ISLAND = {            # extra midday Gaussian amplitude, Wed/Thu, LR only
    "1242": 24.0,             # C (Woodmont)
    "1243": 22.0,             # D (Cattleridge)
}
_ISLAND_DAYS = {"Wed", "Thu"}


def _gauss(h: float, center: float, sigma: float) -> float:
    return math.exp(-((h - center) ** 2) / (2.0 * sigma ** 2))


def _slot_center_hour(hour_idx: int) -> float:
    """Center hour of slot ``hour_idx`` (slot 2 = 08:00-09:00 -> 8.5)."""
    return HOUR_START + hour_idx + 0.5


def _date_for(week_id: int, weekday_idx: int) -> date:
    return WEEK1_MONDAY + timedelta(weeks=week_id - 1, days=weekday_idx)


def _base_aor(
    direction: Direction,
    intersection_id: str,
    weekday: str,
    hour_idx: int,
    week_id: int,
) -> float:
    """Deterministic (noise-free) AOR% for one voxel-week."""
    d = _DIURNAL[direction]
    am_mult, pm_mult = _MULT[direction][intersection_id]
    h = _slot_center_hour(hour_idx)

    am_raw = d["am"][0] * _gauss(h, d["am"][1], d["am"][2])
    mid_raw = d["mid"][0] * _gauss(h, d["mid"][1], d["mid"][2])
    pm_raw = d["pm"][0] * _gauss(h, d["pm"][1], d["pm"][2])

    # Midday island (LR only, C/D, Wed/Thu) — an extra midday spike.
    if (
        direction == Direction.LR
        and weekday in _ISLAND_DAYS
        and intersection_id in _MIDDAY_ISLAND
    ):
        mid_raw += _MIDDAY_ISLAND[intersection_id] * _gauss(h, d["mid"][1], d["mid"][2])

    dow = _DOW_SCALE[weekday]
    aor = d["floor"] + (am_raw * am_mult + mid_raw + pm_raw * pm_mult) * dow

    # E eastbound AM worsening trend (0 in week 1, +5.6pp by week 8).
    if direction == Direction.LR and intersection_id == "1244":
        aor += _TREND_PER_WEEK * (week_id - 1) * _gauss(h, d["am"][1], d["am"][2])

    if week_id in _BAD_WEEKS:
        aor += _BAD_WEEK_BIAS

    return aor


def _generate(direction: Direction, phase_group: str, *, seed: int) -> list[RawObservation]:
    rng = np.random.default_rng(seed)
    rows: list[RawObservation] = []
    for week_id in range(1, N_WEEKS + 1):
        for weekday_idx, weekday in enumerate(DAY_LABELS):
            obs_date = _date_for(week_id, weekday_idx)
            for hour_idx, hour_slot in enumerate(HOUR_SLOTS):
                for corridor_order, intersection_id in enumerate(ORDERED_INTERSECTIONS):
                    base = _base_aor(direction, intersection_id, weekday, hour_idx, week_id)
                    value = base + rng.normal(0.0, _NOISE_SIGMA)
                    value = float(min(100.0, max(0.0, value)))
                    rows.append(
                        RawObservation(
                            intersection_id=intersection_id,
                            corridor_order=corridor_order,
                            direction=direction,
                            week_id=week_id,
                            date=obs_date,
                            weekday=weekday,
                            hour_slot=hour_slot,
                            moe_name=MOE_NAME,
                            raw_value=value,
                        )
                    )
    return rows


def generate_lr_aor_observations() -> list[RawObservation]:
    """Eastbound (LR / Phase 2) AOR% fixture — 5 376 rows.

    Expected bottlenecks at the 70% threshold: AM (8-10am) at A/C/E, PM (4-7pm)
    at B/D/F, plus a Wed/Thu midday island at C/D. E's AM voxels darken over the
    8 weeks (worsening trend).
    """
    return _generate(Direction.LR, "phase2", seed=2026)


def generate_rl_aor_observations() -> list[RawObservation]:
    """Westbound (RL / Phase 6) AOR% fixture — 5 376 rows.

    Expected bottlenecks at the 70% threshold: PM only (4-7pm) at F/D/B; AM
    amplitude is too low westbound to cross the threshold.
    """
    return _generate(Direction.RL, "phase6", seed=2027)
