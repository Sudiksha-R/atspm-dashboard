"""Typed request/response data structures for the adjacent-intersection synchrony analysis.

These mirror the "Data contract" and "Data schemas" sections of
DOCS/corridor_synchrony_blueprint.md. Keeping them as plain dataclasses (rather than
framework-specific models) keeps this prototype portable into whatever the main
dashboard backend uses (Flask/FastAPI/Django/etc).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum


class Direction(str, Enum):
    LR = "LR"  # left-to-right, e.g. all Phase 2s
    RL = "RL"  # right-to-left, e.g. all Phase 6s


@dataclass(frozen=True)
class AnalysisRequest:
    """User-selected inputs that define one synchrony analysis run.

    Field names match the blueprint's "Required user inputs" table exactly.
    """

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
    min_sample_size: int = 20
    """Below this matched-sample count, a pair is flagged LOW_SAMPLE_SIZE (configurable, see blueprint Concern 1)."""


@dataclass(frozen=True)
class RawObservation:
    """One hourly raw MoE record, matching the blueprint's "Required raw data fields" table."""

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
class AlignmentKey:
    """The alignment tuple the blueprint mandates: (week_id, weekday, hour_slot).

    This is the single most important invariant in the pipeline -- comparisons must
    use the *same exact* weekday-hour instance across intersections, never pooled.
    """

    week_id: int
    weekday: str
    hour_slot: str

    def sort_key(self) -> tuple:
        return (self.week_id, self.weekday, self.hour_slot)


@dataclass
class PairResult:
    """Per-pair output schema -- mirrors the blueprint's "Pair result schema" JSON exactly."""

    pair_id: str
    intersection_a: str
    intersection_b: str
    direction: Direction
    moe_name: str
    n_samples: int
    raw_vector_a: list[float]
    raw_vector_b: list[float]
    norm_vector_a: list[float]
    norm_vector_b: list[float]
    pearson_r: float | None
    spearman_rho: float | None
    cosine_similarity: float | None
    variance_a: float
    variance_b: float
    quality_flags: list[str] = field(default_factory=list)
    headline_metric: str = "spearman_rho"
    headline_score: float | None = None
    interpretation_label: str = "not_computable"


@dataclass
class AnalysisContext:
    corridor_id: str
    direction: Direction
    moe_name: str
    weekday_filter: list[str]
    hour_slot_filter: list[str]
    weeks_included: int


@dataclass
class UISummary:
    """Top-level payload -- mirrors the blueprint's "UI summary schema"."""

    analysis_context: AnalysisContext
    pairs: list[PairResult]
    rankings: dict[str, list[str]]
    """Maps ranking name (e.g. "strongest_sync", "weakest_sync") to an ordered list of pair_ids."""
