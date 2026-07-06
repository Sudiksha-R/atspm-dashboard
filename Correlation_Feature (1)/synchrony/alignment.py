"""Deterministic alignment utility.

Implements the blueprint's "Alignment rules" section -- the most important part of the
implementation. Comparisons must use the *same exact* (week_id, weekday, hour_slot)
instance across intersections; weekdays/hour-slots must never be pooled before matching.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .schemas import RawObservation

ALIGNMENT_KEY_COLUMNS = ["week_id", "weekday", "hour_slot"]
"""Required alignment key per the blueprint: (week_id, weekday, hour_slot)."""

# Deterministic sort order so that "Mon 08:00" always precedes "Mon 09:00" precedes
# "Tue 08:00", regardless of how the raw rows arrived. Without an explicit weekday
# order, a lexicographic sort would put "Friday" before "Monday" and silently produce
# vectors that are internally consistent but not in the expected weekday-then-hour order.
WEEKDAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _weekday_rank(weekday: str) -> int:
    try:
        return WEEKDAY_ORDER.index(weekday)
    except ValueError:
        raise ValueError(
            f"Unknown weekday label {weekday!r}; expected one of {WEEKDAY_ORDER}"
        )


def observations_to_frame(rows: list[RawObservation]) -> pd.DataFrame:
    """Convert raw observation records into a flat, sortable DataFrame."""

    if not rows:
        return pd.DataFrame(
            columns=[
                "intersection_id",
                "corridor_order",
                "direction",
                "week_id",
                "date",
                "weekday",
                "hour_slot",
                "moe_name",
                "raw_value",
            ]
        )

    return pd.DataFrame([vars(r) | {"direction": r.direction.value} for r in rows])


def _sort_for_vector_construction(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the blueprint's deterministic vector-construction order:
    1. week_id, 2. weekday, 3. hour_slot.
    """

    df = df.copy()
    df["_weekday_rank"] = df["weekday"].map(_weekday_rank)
    df = df.sort_values(["week_id", "_weekday_rank", "hour_slot"], kind="mergesort")
    return df.drop(columns="_weekday_rank")


@dataclass(frozen=True)
class MatchedPair:
    """Output of aligning two intersections: matched, ordered observation rows plus
    diagnostics needed by the normalization/similarity layers and the quality-flag logic.
    """

    intersection_a: str
    intersection_b: str
    matched: pd.DataFrame
    """One row per matched (week_id, weekday, hour_slot) instance, columns
    raw_value_a / raw_value_b plus the alignment-key columns, in deterministic order."""
    n_samples: int
    n_unmatched_a: int
    """Rows for A that had no counterpart in B for the same alignment key (dropped by the inner join)."""
    n_unmatched_b: int

    @property
    def raw_vector_a(self) -> list[float]:
        return self.matched["raw_value_a"].tolist()

    @property
    def raw_vector_b(self) -> list[float]:
        return self.matched["raw_value_b"].tolist()


def align_pair(rows_a: pd.DataFrame, rows_b: pd.DataFrame, *, intersection_a: str, intersection_b: str) -> MatchedPair:
    """Inner-join two intersections' observation rows on the alignment key.

    Per the blueprint's "Pairwise inner join" rule: join on (week_id, weekday, hour_slot),
    drop unmatched rows, and record the final matched sample size as n_samples.
    """

    a = rows_a[ALIGNMENT_KEY_COLUMNS + ["raw_value"]].rename(columns={"raw_value": "raw_value_a"})
    b = rows_b[ALIGNMENT_KEY_COLUMNS + ["raw_value"]].rename(columns={"raw_value": "raw_value_b"})

    # `how="inner"` enforces "drop any unmatched rows" -- a key on one side with no
    # counterpart on the other side simply does not appear in the result.
    matched = a.merge(b, on=ALIGNMENT_KEY_COLUMNS, how="inner")
    matched = _sort_for_vector_construction(matched).reset_index(drop=True)

    n_unmatched_a = len(a) - len(matched)
    n_unmatched_b = len(b) - len(matched)

    return MatchedPair(
        intersection_a=intersection_a,
        intersection_b=intersection_b,
        matched=matched,
        n_samples=len(matched),
        n_unmatched_a=n_unmatched_a,
        n_unmatched_b=n_unmatched_b,
    )


def make_adjacent_pairs(ordered_ids: list[str]) -> list[tuple[str, str]]:
    """Generate only adjacent pairs in corridor order, e.g. [A,B,C,D] -> (A,B),(B,C),(C,D).

    Per the blueprint's "Pair generation rules": never (A,C) or (A,D).
    """

    return list(zip(ordered_ids[:-1], ordered_ids[1:]))
