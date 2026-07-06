"""Synthetic 12-week hourly **Volume** dataset for the correlation-clique voxel prototype.

This stands in for what the real dashboard will do: the UI queries the main database for
the selected weeks and gets back a long-format table of hourly Measure-of-Effectiveness
values (here `moe_name == "volume"`, vehicles/hour). Swap `generate_volume_observations`
for that query and nothing downstream changes -- the rest of the package consumes a plain
`list[RawObservation]` / DataFrame.

Grid (per travel direction): 6 intersections x 12 weeks x 7 weekdays x 16 hour slots
(06:00-07:00 ... 21:00-22:00). Two directions (LR eastbound, RL westbound) => 16,128 rows.

How the data is built (and why the cliques come out clean) -- see README.md for the full
rationale. The short version:

  * **Base diurnal shape** (AM/midday/PM Gaussian peaks x day-of-week scaling x
    per-intersection level x direction asymmetry) sets each slot's *typical* magnitude.
    This base is **constant across the 12 weeks** -- on purpose. The per-slot correlation
    we compute downstream is a *week-over-week* Spearman ("at this exact weekday+hour, do
    two intersections rise and fall together across the 12 weeks?"), so if every
    intersection shared one global week-to-week trend they would *all* correlate and
    everything would clique. Keeping the base flat across weeks means correlation is
    created *only* where we deliberately inject a shared weekly signal.

  * **Weekly anomaly** (a small +/-~10% wobble added on top, varying by week) is the
    only week-varying part, so it alone drives the Spearman. For each (slot, direction)
    we pick which intersections should move together and give them a **shared** weekly
    driver pattern (+ a little independent jitter so they're correlated, not identical);
    every other intersection gets its **own** driver, drawn so the drivers are mutually
    near-uncorrelated. Result: only the intended group is mutually highly correlated.

The intended per-regime groups (weekday peaks only; weekends and off-peak stay
uncorrelated) -- this is the direction-aware "flip" the rest of the prototype shows:

    | Regime (weekdays Mon-Fri)        | LR (eastbound) | RL (westbound) |
    |----------------------------------|----------------|----------------|
    | AM peak  (07-10h, slots 1-3)     | {A, B, C}      | {D, E, F}      |
    | PM peak  (16-19h, slots 10-12)   | {D, E, F}      | {A, B, C}      |
    | Midday   (12-13h, slot 6, Wed+Thu)| {B, C, D}     | (none)         |
    | all other hours / Sat / Sun      | (none)         | (none)         |

Letter <-> id <-> street-name key:
    A=1240 McIntosh Rd. · B=1241 Honore Ave. · C=1242 Woodmont Dr. ·
    D=1243 Cattleridge Blvd. · E=1244 Cattlemen Rd. · F=1245 I-75 SB Ramp
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from .schemas import Direction, RawObservation

# --------------------------------------------------------------------------------------
# Corridor constants (mirrors the wider prototype's Bee Ridge demo corridor)
# --------------------------------------------------------------------------------------

CORRIDOR_ID = "demo_corridor_sr758"
CORRIDOR_NAME = (
    "Bee Ridge Rd corridor — McIntosh Rd. to I-75 SB Ramp (intersections 1240-1245)"
)

ORDERED_INTERSECTIONS: list[str] = ["1240", "1241", "1242", "1243", "1244", "1245"]
"""Intersection ids in corridor (west -> east) order. Index = corridor_order."""

INTERSECTION_LABELS: dict[str, str] = {
    "1240": "McIntosh Rd.",
    "1241": "Honore Ave.",
    "1242": "Woodmont Dr.",
    "1243": "Cattleridge Blvd.",
    "1244": "Cattlemen Rd.",
    "1245": "I-75 SB Ramp",
}

DAY_LABELS: tuple[str, ...] = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")

HOUR_START = 6
N_HOURS = 16
HOUR_SLOTS: list[str] = [
    f"{HOUR_START + h:02d}:00-{HOUR_START + h + 1:02d}:00" for h in range(N_HOURS)
]
"""16 one-hour slots: '06:00-07:00' ... '21:00-22:00'."""

N_WEEKS = 12
WEEK1_MONDAY = date(2026, 1, 5)  # a Monday
MOE_NAME = "volume"

# Default location of the generated CSV, alongside this module.
DATA_DIR = Path(__file__).resolve().parent / "data"
DEFAULT_CSV_PATH = DATA_DIR / "corridor_volume_12wk.csv"

# --------------------------------------------------------------------------------------
# Fixture tuning knobs (documented in README.md)
# --------------------------------------------------------------------------------------

_SEED = 2026
_ANOMALY_FRAC = 0.10  # weekly wobble amplitude as a fraction of base volume (+/-~10%)
_JITTER_MIX = 0.12    # share of a member's anomaly that is independent jitter (rest = shared driver)
_DRIVER_MAX_ABS = 0.30  # max |Spearman| allowed between two distinct slot drivers

# Per-direction base diurnal parameters: (floor, AM_amp, MID_amp, PM_amp).
# Commute asymmetry: eastbound (LR) is AM-heavy, westbound (RL) is PM-heavy.
_DIURNAL = {
    Direction.LR: dict(floor=180.0, am=900.0, mid=350.0, pm=520.0),
    Direction.RL: dict(floor=180.0, am=460.0, mid=350.0, pm=940.0),
}
_AM_CTR, _AM_SIG = 8.0, 1.4
_MID_CTR, _MID_SIG = 13.0, 2.6
_PM_CTR, _PM_SIG = 17.5, 1.8

# Day-of-week multiplier applied to the peak contributions (weekends much lighter).
_DOW_SCALE = {"Mon": 1.00, "Tue": 1.00, "Wed": 1.02, "Thu": 1.00,
              "Fri": 1.08, "Sat": 0.60, "Sun": 0.45}

# Per-intersection volume level (different roads carry different traffic).
_INT_MULT = [1.00, 1.15, 0.92, 1.06, 1.22, 0.84]


# --------------------------------------------------------------------------------------
# Regime -> which intersections move together
# --------------------------------------------------------------------------------------

_AM_SLOTS = (1, 2, 3)        # 07:00-08:00, 08:00-09:00, 09:00-10:00
_PM_SLOTS = (10, 11, 12)     # 16:00-17:00, 17:00-18:00, 18:00-19:00
_MIDDAY_SLOT = 6             # 12:00-13:00
_MIDDAY_DAYS = (2, 3)        # Wed, Thu


def slot_group(direction: Direction, weekday_idx: int, hour_idx: int) -> frozenset[int]:
    """The set of intersection indices intended to move together at this (slot, direction).

    Returns a frozenset of corridor-order indices (0=A .. 5=F), or empty for no group.
    Weekends (weekday_idx 5,6) never group. This is the ground-truth story the synthetic
    data is built to reproduce once the per-slot clique detector runs over it.
    """

    if weekday_idx >= 5:  # Sat / Sun
        return frozenset()

    am = hour_idx in _AM_SLOTS
    pm = hour_idx in _PM_SLOTS
    midday = hour_idx == _MIDDAY_SLOT and weekday_idx in _MIDDAY_DAYS

    if direction == Direction.LR:
        if am:
            return frozenset({0, 1, 2})   # A, B, C
        if pm:
            return frozenset({3, 4, 5})   # D, E, F
        if midday:
            return frozenset({1, 2, 3})   # B, C, D
        return frozenset()
    else:  # RL
        if am:
            return frozenset({3, 4, 5})   # D, E, F
        if pm:
            return frozenset({0, 1, 2})   # A, B, C
        return frozenset()


# --------------------------------------------------------------------------------------
# Generative helpers
# --------------------------------------------------------------------------------------


def _gauss(x: float, ctr: float, sig: float) -> float:
    return float(np.exp(-0.5 * ((x - ctr) / sig) ** 2))


def _base_volume(int_idx: int, weekday: str, hour_idx: int, direction: Direction) -> float:
    """Typical (week-invariant) hourly volume for one slot, in vehicles/hour."""

    p = _DIURNAL[direction]
    h = HOUR_START + hour_idx + 0.5  # slot-center clock hour
    peaks = (
        p["am"] * _gauss(h, _AM_CTR, _AM_SIG)
        + p["mid"] * _gauss(h, _MID_CTR, _MID_SIG)
        + p["pm"] * _gauss(h, _PM_CTR, _PM_SIG)
    )
    val = (p["floor"] + peaks * _DOW_SCALE[weekday]) * _INT_MULT[int_idx]
    return val


def _slot_rng(direction: Direction, weekday_idx: int, hour_idx: int) -> np.random.Generator:
    """Deterministic per-slot RNG (so the whole dataset is reproducible from _SEED)."""

    dir_code = 0 if direction == Direction.LR else 1
    return np.random.default_rng([_SEED, dir_code, weekday_idx, hour_idx])


def _make_driver_library(n: int, rng: np.random.Generator) -> list[np.ndarray]:
    """`n` length-N_WEEKS vectors that are mutually near-uncorrelated (|Spearman| small).

    These are the per-slot weekly "driver" signals. Members of one group share a single
    driver; everyone else gets a distinct one, so only the intended group ends up mutually
    correlated. Greedy rejection sampling keeps every pair below `_DRIVER_MAX_ABS`.
    """

    selected: list[np.ndarray] = []
    attempts = 0
    while len(selected) < n and attempts < 4000:
        attempts += 1
        cand = rng.standard_normal(N_WEEKS)
        if all(abs(stats.spearmanr(cand, s).statistic) <= _DRIVER_MAX_ABS for s in selected):
            selected.append(cand)
    while len(selected) < n:  # extremely unlikely fallback
        selected.append(rng.standard_normal(N_WEEKS))
    return selected


def _slot_anomalies(direction: Direction, weekday_idx: int, hour_idx: int) -> dict[int, np.ndarray]:
    """Per-intersection weekly anomaly vectors (length N_WEEKS, mean 0, std 1) for one slot.

    Group members share one driver; non-members each get their own. A small independent
    `_JITTER_MIX` is blended into every intersection so grouped members are highly
    correlated but not identical (realistic) and never produce a degenerate constant.
    """

    rng = _slot_rng(direction, weekday_idx, hour_idx)
    lib = _make_driver_library(len(ORDERED_INTERSECTIONS), rng)
    group = slot_group(direction, weekday_idx, hour_idx)

    assign: dict[int, np.ndarray] = {}
    if group:
        for idx in group:
            assign[idx] = lib[0]                       # shared driver for the group
        others = [i for i in range(len(ORDERED_INTERSECTIONS)) if i not in group]
        for k, idx in enumerate(others):
            assign[idx] = lib[1 + k]                   # distinct driver each
    else:
        for idx in range(len(ORDERED_INTERSECTIONS)):
            assign[idx] = lib[idx]                      # all distinct -> no group

    anomalies: dict[int, np.ndarray] = {}
    for idx in range(len(ORDERED_INTERSECTIONS)):
        jitter = rng.standard_normal(N_WEEKS)
        a = (1.0 - _JITTER_MIX) * assign[idx] + _JITTER_MIX * jitter
        std = a.std()
        anomalies[idx] = (a - a.mean()) / (std if std > 1e-9 else 1.0)
    return anomalies


# --------------------------------------------------------------------------------------
# Public generator
# --------------------------------------------------------------------------------------


def generate_volume_observations() -> list[RawObservation]:
    """Generate the full synthetic dataset as a list of `RawObservation` (both directions).

    16,128 rows: 6 intersections x 12 weeks x 7 weekdays x 16 hour slots x 2 directions.
    Fully deterministic (seeded), so the CSV is reproducible.
    """

    rows: list[RawObservation] = []
    for direction in (Direction.LR, Direction.RL):
        for wd_idx, weekday in enumerate(DAY_LABELS):
            for h_idx, hour_slot in enumerate(HOUR_SLOTS):
                anomalies = _slot_anomalies(direction, wd_idx, h_idx)
                for i_idx, iid in enumerate(ORDERED_INTERSECTIONS):
                    base = _base_volume(i_idx, weekday, h_idx, direction)
                    anom = anomalies[i_idx]
                    for w in range(N_WEEKS):
                        value = base * (1.0 + _ANOMALY_FRAC * anom[w])
                        value = max(0.0, round(value))
                        obs_date = WEEK1_MONDAY + timedelta(weeks=w, days=wd_idx)
                        rows.append(
                            RawObservation(
                                intersection_id=iid,
                                corridor_order=i_idx,
                                direction=direction,
                                week_id=w + 1,
                                date=obs_date,
                                weekday=weekday,
                                hour_slot=hour_slot,
                                moe_name=MOE_NAME,
                                raw_value=float(value),
                            )
                        )
    return rows


def observations_to_dataframe(rows: list[RawObservation]) -> pd.DataFrame:
    """Long-format DataFrame -- the exact shape a DB query would return to the UI."""

    return pd.DataFrame(
        [
            {
                "intersection_id": r.intersection_id,
                "corridor_order": r.corridor_order,
                "direction": r.direction.value,
                "week_id": r.week_id,
                "date": r.date.isoformat(),
                "weekday": r.weekday,
                "hour_slot": r.hour_slot,
                "moe_name": r.moe_name,
                "raw_value": r.raw_value,
            }
            for r in rows
        ]
    )


def write_csv(path: Path | str = DEFAULT_CSV_PATH) -> Path:
    """Generate the dataset and write it to `path` (long format). Returns the path."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = observations_to_dataframe(generate_volume_observations())
    df.to_csv(path, index=False)
    return path


def load_csv(path: Path | str = DEFAULT_CSV_PATH) -> pd.DataFrame:
    """Load the long-format CSV (generating it first if it does not yet exist)."""

    path = Path(path)
    if not path.exists():
        write_csv(path)
    return pd.read_csv(path, dtype={"intersection_id": str})
