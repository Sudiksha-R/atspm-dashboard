"""Pipeline orchestrator: run_pairwise_synchrony_analysis.

This is a direct implementation of the blueprint's pseudocode (section "Pseudocode"),
wiring together: alignment -> normalization -> similarity -> quality flags ->
interpretation -> pair result -> UI payload.

Kept deliberately thin -- each real step lives in its own module (alignment.py,
normalization.py, similarity.py) so they stay independently testable and swappable,
per the blueprint's "Design rule: Keep these modules independent."
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .alignment import align_pair, make_adjacent_pairs, observations_to_frame
from .normalization import DEFAULT_NORMALIZER, Normalizer
from .schemas import AnalysisContext, AnalysisRequest, PairResult, RawObservation, UISummary
from .similarity import (
    compute_quality_flags,
    map_score_to_label,
    safe_cosine,
    safe_pearson,
    safe_spearman,
)

HEADLINE_METRIC = "spearman_rho"
"""Default headline metric per the blueprint's "Recommendation for v1 defaults"."""


def _sort_intersections_in_direction(request: AnalysisRequest) -> list[str]:
    """Order the user's selected subset according to the corridor's canonical order.

    `ordered_intersections` already encodes the corridor's physical left-to-right
    layout. RL simply walks that list in reverse -- the *selection* does not change,
    only the direction in which adjacent pairs are formed.
    """

    selected = set(request.selected_intersections)
    ordered = [iid for iid in request.ordered_intersections if iid in selected]
    if request.direction.value == "RL":
        ordered = list(reversed(ordered))
    return ordered


def _build_pair_result(
    *,
    intersection_a: str,
    intersection_b: str,
    request: AnalysisRequest,
    raw_rows: pd.DataFrame,
    normalizer: Normalizer,
) -> PairResult:
    rows_a = raw_rows[raw_rows["intersection_id"] == intersection_a]
    rows_b = raw_rows[raw_rows["intersection_id"] == intersection_b]

    matched_pair = align_pair(rows_a, rows_b, intersection_a=intersection_a, intersection_b=intersection_b)

    raw_vec_a = np.asarray(matched_pair.raw_vector_a, dtype=float)
    raw_vec_b = np.asarray(matched_pair.raw_vector_b, dtype=float)

    norm_a = normalizer(raw_vec_a)
    norm_b = normalizer(raw_vec_b)

    quality = compute_quality_flags(
        raw_vector_a=raw_vec_a,
        raw_vector_b=raw_vec_b,
        norm_zero_variance_a=norm_a.is_zero_variance,
        norm_zero_variance_b=norm_b.is_zero_variance,
        n_samples=matched_pair.n_samples,
        n_unmatched_a=matched_pair.n_unmatched_a,
        n_unmatched_b=matched_pair.n_unmatched_b,
        min_sample_size=request.min_sample_size,
    )

    pearson_r = safe_pearson(norm_a.values, norm_b.values)
    spearman_rho = safe_spearman(norm_a.values, norm_b.values)
    cosine_sim = safe_cosine(norm_a.values, norm_b.values)

    headline_score = {"pearson_r": pearson_r, "spearman_rho": spearman_rho, "cosine_similarity": cosine_sim}[HEADLINE_METRIC]
    interpretation_label = map_score_to_label(headline_score)

    return PairResult(
        pair_id=f"{intersection_a}__{intersection_b}",
        intersection_a=intersection_a,
        intersection_b=intersection_b,
        direction=request.direction,
        moe_name=request.moe_name,
        n_samples=matched_pair.n_samples,
        raw_vector_a=raw_vec_a.tolist(),
        raw_vector_b=raw_vec_b.tolist(),
        norm_vector_a=norm_a.values.tolist(),
        norm_vector_b=norm_b.values.tolist(),
        pearson_r=pearson_r,
        spearman_rho=spearman_rho,
        cosine_similarity=cosine_sim,
        variance_a=float(np.var(norm_a.values)) if norm_a.values.size else 0.0,
        variance_b=float(np.var(norm_b.values)) if norm_b.values.size else 0.0,
        quality_flags=quality.flags,
        headline_metric=HEADLINE_METRIC,
        headline_score=headline_score,
        interpretation_label=interpretation_label,
    )


def _build_rankings(pairs: list[PairResult]) -> dict[str, list[str]]:
    """Rank adjacent pairs by the headline metric. Pairs whose headline score could
    not be computed are excluded from ranked lists (they remain visible in the table
    with their quality flags, per "Quality warnings are visible")."""

    scored = [p for p in pairs if p.headline_score is not None]

    strongest_sync = sorted(scored, key=lambda p: p.headline_score, reverse=True)
    weakest_sync = sorted(scored, key=lambda p: abs(p.headline_score))
    strongest_inverse = sorted(scored, key=lambda p: p.headline_score)

    return {
        "strongest_sync": [p.pair_id for p in strongest_sync],
        "weakest_sync": [p.pair_id for p in weakest_sync],
        "strongest_inverse": [p.pair_id for p in strongest_inverse if p.headline_score < 0],
    }


def run_pairwise_synchrony_analysis(
    request: AnalysisRequest,
    rows: list[RawObservation],
    *,
    normalizer: Normalizer = DEFAULT_NORMALIZER,
) -> UISummary:
    """End-to-end pipeline matching the blueprint pseudocode `run_pairwise_synchrony_analysis`.

    `rows` stands in for `fetch_hourly_moe_rows(...)` -- in this prototype the caller
    supplies already-fetched raw observations (from the fixture generator here, or
    from the real retrieval layer once integrated).
    """

    raw_rows = observations_to_frame(rows)

    ordered_ids = _sort_intersections_in_direction(request)
    adjacent_pairs = make_adjacent_pairs(ordered_ids)

    pairs = [
        _build_pair_result(
            intersection_a=a,
            intersection_b=b,
            request=request,
            raw_rows=raw_rows,
            normalizer=normalizer,
        )
        for a, b in adjacent_pairs
    ]

    weeks_included = raw_rows["week_id"].nunique() if not raw_rows.empty else 0

    context = AnalysisContext(
        corridor_id=request.corridor_id,
        direction=request.direction,
        moe_name=request.moe_name,
        weekday_filter=request.weekday_filter,
        hour_slot_filter=request.hour_slot_filter,
        weeks_included=int(weeks_included),
    )

    return UISummary(analysis_context=context, pairs=pairs, rankings=_build_rankings(pairs))
