"""Presentation adapter: turns a UISummary into the three UI-ready data structures
the blueprint specifies (numeric table, corridor-edge map, ranked pair list).

Deliberately framework-agnostic -- returns plain pandas DataFrames / dicts. The
Plotly renderers in viz.py consume these, but the main dashboard's UI layer could
just as easily render them with a different charting library without touching the
analysis core (per the blueprint's "Design rule: Keep these modules independent").
"""

from __future__ import annotations

import pandas as pd

from .schemas import PairResult, UISummary
from .similarity import map_score_to_label

PAIR_LABEL_SEPARATOR = "-"


def pair_label(pair: PairResult, *, friendly_names: dict[str, str] | None = None) -> str:
    """e.g. "1240-1241", or "A-B" if friendly_names is supplied (fixture demo only)."""

    names = friendly_names or {}
    a = names.get(pair.intersection_a, pair.intersection_a)
    b = names.get(pair.intersection_b, pair.intersection_b)
    return f"{a}{PAIR_LABEL_SEPARATOR}{b}"


# --- 1. Numeric summary table ------------------------------------------------------


def build_summary_table(summary: UISummary, *, friendly_names: dict[str, str] | None = None) -> pd.DataFrame:
    """One row per adjacent pair, columns matching the blueprint's "Numeric summary table"."""

    rows = []
    for p in summary.pairs:
        rows.append(
            {
                "Pair": pair_label(p, friendly_names=friendly_names),
                "Direction": p.direction.value,
                "MoE": p.moe_name,
                "n_samples": p.n_samples,
                "Spearman (headline)": p.spearman_rho,
                "Pearson": p.pearson_r,
                "Cosine": p.cosine_similarity,
                "Quality flags": ", ".join(p.quality_flags) if p.quality_flags else "—",
                "Interpretation": p.interpretation_label,
            }
        )
    return pd.DataFrame(rows)


# --- 2. Corridor-edge visualization data -------------------------------------------


def build_corridor_edge_data(
    summary: UISummary,
    ordered_intersections: list[str],
    *,
    friendly_names: dict[str, str] | None = None,
) -> dict:
    """Nodes (in corridor order) + edges (one per adjacent pair) with the metadata
    the edge view needs: headline-metric color encoding, full-metric tooltip, and a
    muted/warning style flag for unstable edges.

    Returns
    -------
    {
        "nodes": [{"id": ..., "label": ..., "position": i}, ...],
        "edges": [{"pair_id", "source", "target", "headline_score", "headline_metric",
                   "tooltip", "is_unstable", "interpretation_label"}, ...],
    }
    """

    names = friendly_names or {}
    pairs_by_id = {p.pair_id: p for p in summary.pairs}

    nodes = [
        {"id": iid, "label": names.get(iid, iid), "position": i}
        for i, iid in enumerate(ordered_intersections)
    ]

    edges = []
    for source, target in zip(ordered_intersections[:-1], ordered_intersections[1:]):
        pair_id = f"{source}__{target}"
        p = pairs_by_id.get(pair_id)
        if p is None:
            continue

        is_unstable = bool(p.quality_flags) or p.headline_score is None
        tooltip_lines = [
            f"Pair {pair_label(p, friendly_names=friendly_names)}",
            f"n_samples = {p.n_samples}",
            f"Spearman ρ = {_fmt(p.spearman_rho)}  (headline)",
            f"Pearson r = {_fmt(p.pearson_r)}",
            f"Cosine sim = {_fmt(p.cosine_similarity)}",
            f"Label: {p.interpretation_label}",
        ]
        if p.quality_flags:
            tooltip_lines.append(f"Flags: {', '.join(p.quality_flags)}")

        edges.append(
            {
                "pair_id": pair_id,
                "source": source,
                "target": target,
                "headline_metric": p.headline_metric,
                "headline_score": p.headline_score,
                "interpretation_label": p.interpretation_label,
                "is_unstable": is_unstable,
                "tooltip": "<br>".join(tooltip_lines),
            }
        )

    return {"nodes": nodes, "edges": edges}


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:+.3f}"


# --- 3. Ranked pair list ------------------------------------------------------------


def build_ranked_list(
    summary: UISummary,
    *,
    ranking: str = "strongest_sync",
    friendly_names: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Side-panel ranking table for one of the rankings the pipeline computed
    ("strongest_sync", "weakest_sync", "strongest_inverse").
    """

    if ranking not in summary.rankings:
        raise ValueError(f"Unknown ranking {ranking!r}; available: {list(summary.rankings)}")

    pairs_by_id = {p.pair_id: p for p in summary.pairs}
    ordered = [pairs_by_id[pid] for pid in summary.rankings[ranking] if pid in pairs_by_id]

    rows = []
    for rank, p in enumerate(ordered, start=1):
        rows.append(
            {
                "Rank": rank,
                "Pair": pair_label(p, friendly_names=friendly_names),
                "Headline metric": p.headline_metric,
                "Headline score": p.headline_score,
                "Interpretation": p.interpretation_label,
                "n_samples": p.n_samples,
                "Quality flags": ", ".join(p.quality_flags) if p.quality_flags else "—",
            }
        )
    return pd.DataFrame(rows)
