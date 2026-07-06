"""Per-slot, week-over-week correlation cliques -> the 6 x 7 x 16 voxel volume.

This is the missing bridge between the grouping idea and the 3D voxel view. It answers
the question, **independently for every (weekday, hour-of-day) slot**:

    "Looking only at THIS weekday+hour, across the N analysis weeks, which intersections
     rise and fall together week-over-week -- and which of those form spatially
     contiguous correlation cliques?"

For each (day, hour) slot it sets `volume[intersection, day, hour] = 1` for every
intersection that lands in some contiguous clique, else 0. Stacking all 7 x 16 = 112
slots gives the 6 x 7 x 16 binary volume that `voxel_viz.render_voxel_volume` draws.

Relationship to the wider prototype's `synchrony.grouping`
----------------------------------------------------------
The original grouping module computes ONE Spearman per pair pooled across *all*
week x weekday x hour rows at once (a global synchrony score). This module asks a
*different* question -- a separate correlation per (weekday, hour) slot using only that
slot's N weekly values -- so it needs its own per-slot loop. But the **math is identical**:

  * same metric -- Spearman rho via `scipy.stats.spearmanr` (the same `safe_spearman`
    wrapper the wider prototype uses, copied here so the package is self-contained);
  * same "highly correlated" rule -- a tunable positive threshold (default +0.60);
  * same clique step -- maximal cliques of the thresholded graph via `networkx`;
  * same contiguity rule -- a reported group must be a run of corridor-consecutive
    intersections (so a scattered clique {A,B,D} with C left out is pared to {A,B}).

Normalization is intentionally skipped: the wider pipeline min-max normalizes before
correlating, but Spearman is rank-based and min-max scaling is monotonic, so it leaves
Spearman rho unchanged. Skipping it yields identical rho values with less machinery.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations

import networkx as nx
import numpy as np
import pandas as pd

from .schemas import Direction

DEFAULT_THRESHOLD = 0.60
"""Minimum Spearman rho for a pair to count as 'highly correlated'. Positive-only:
grouping is about intersections that move *together*. A product knob, not a constant --
exposed in the notebook so the user can retune it."""


# --------------------------------------------------------------------------------------
# Copied math primitives (kept here so the package has zero external-folder dependency)
# --------------------------------------------------------------------------------------


def safe_spearman(x: np.ndarray, y: np.ndarray) -> float | None:
    """Spearman rho that returns None on degenerate input instead of raising / NaN.

    Identical behaviour to the wider prototype's `synchrony.similarity.safe_spearman`:
    too few points or a constant series (zero variance) -> None, so a slot never crashes
    the per-slot loop.
    """

    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if x.size != y.size:
        return None
    finite = np.isfinite(x) & np.isfinite(y)
    x = x[finite]
    y = y[finite]
    if x.size < 2 or y.size < 2 or np.allclose(x, x[0]) or np.allclose(y, y[0]):
        return None
    from scipy import stats

    rho = stats.spearmanr(x, y).statistic
    return float(rho) if np.isfinite(rho) else None


def contiguous_runs(members: list[str], order_of: dict[str, int]) -> list[list[str]]:
    """Split intersections into maximal runs whose corridor-order indices are consecutive.

    {0,1,3,4} -> [[0,1],[3,4]]; {1,3,4,5} -> [[1],[3,4,5]]. This is what enforces
    'a group must be spatially connected'. (Copied from `synchrony.grouping._contiguous_runs`.)
    """

    ordered = sorted(members, key=lambda m: order_of[m])
    runs: list[list[str]] = [[ordered[0]]]
    for prev, cur in zip(ordered, ordered[1:]):
        if order_of[cur] == order_of[prev] + 1:
            runs[-1].append(cur)
        else:
            runs.append([cur])
    return runs


# --------------------------------------------------------------------------------------
# Per-slot weekly vectors
# --------------------------------------------------------------------------------------


def slot_weekly_vectors(
    df: pd.DataFrame,
    *,
    direction: Direction,
    weekday: str,
    hour_slot: str,
    ordered_ids: list[str],
    moe_name: str = "volume",
) -> dict[str, np.ndarray]:
    """The per-week value vector for each intersection at one (direction, weekday, hour).

    Returns `{intersection_id: array of weekly values, ordered by week_id}`. The week
    ordering is shared across intersections (week w of A pairs with week w of B), so a
    later Spearman compares like-to-like -- never shuffled.
    """

    sub = df[
        (df["direction"] == direction.value)
        & (df["weekday"] == weekday)
        & (df["hour_slot"] == hour_slot)
        & (df["moe_name"] == moe_name)
    ]
    if sub.empty:
        return {iid: np.array([], dtype=float) for iid in ordered_ids}

    pivot = sub.pivot_table(
        index="week_id",
        columns="intersection_id",
        values="raw_value",
        aggfunc="mean",
    )
    vectors: dict[str, np.ndarray] = {}
    for iid in ordered_ids:
        if iid not in pivot.columns:
            vectors[iid] = np.full(len(pivot.index), np.nan, dtype=float)
            continue
        vectors[iid] = pivot[iid].sort_index().to_numpy(dtype=float)
    return vectors


# --------------------------------------------------------------------------------------
# Per-slot clique detection
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class SlotCliqueResult:
    """Everything the clique detector found for a single (direction, weekday, hour) slot."""

    direction: Direction
    weekday: str
    hour_slot: str
    n_weeks: int
    highly_correlated_pairs: list[dict] = field(default_factory=list)
    """[{'a','b','spearman_rho'}, ...] for pairs clearing the threshold."""
    cliques: list[list[str]] = field(default_factory=list)
    """Maximal cliques (size >= 2) of the thresholded graph, before contiguity."""
    groups: list[list[str]] = field(default_factory=list)
    """Contiguous runs of size >= 2 -- the reported groups (members in corridor order)."""
    members: set[str] = field(default_factory=set)
    """Union of all group members -- the intersections set to 1 in the volume for this slot."""


def detect_slot_cliques(
    vectors: dict[str, np.ndarray],
    ordered_ids: list[str],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    direction: Direction = Direction.LR,
    weekday: str = "",
    hour_slot: str = "",
) -> SlotCliqueResult:
    """Run the threshold -> cliques -> contiguity pipeline for one slot's weekly vectors.

    Steps (identical in spirit to `synchrony.grouping.find_correlation_groups`, scoped to
    one slot): pairwise Spearman -> keep pairs >= threshold as graph edges -> maximal
    cliques via networkx -> split each clique into corridor-contiguous runs of size >= 2.
    """

    order_of = {iid: i for i, iid in enumerate(ordered_ids)}
    n_weeks = max((len(vectors[i]) for i in ordered_ids), default=0)

    graph = nx.Graph()
    graph.add_nodes_from(ordered_ids)
    highly_correlated: list[dict] = []
    for a, b in combinations(ordered_ids, 2):
        rho = safe_spearman(vectors[a], vectors[b])
        if rho is None:
            continue
        if rho >= threshold:
            graph.add_edge(a, b)
            highly_correlated.append({"a": a, "b": b, "spearman_rho": round(rho, 4)})

    cliques = [sorted(c, key=lambda m: order_of[m]) for c in nx.find_cliques(graph) if len(c) >= 2]

    groups: list[list[str]] = []
    seen: set[frozenset[str]] = set()
    for clique in cliques:
        for run in contiguous_runs(clique, order_of):
            if len(run) >= 2 and frozenset(run) not in seen:
                seen.add(frozenset(run))
                groups.append(run)
    # drop any group that is a subset of another surviving group
    groups = [
        g for g in groups
        if not any(set(g) < set(other) for other in groups)
    ]
    groups.sort(key=lambda g: order_of[g[0]])

    members = {m for g in groups for m in g}
    return SlotCliqueResult(
        direction=direction,
        weekday=weekday,
        hour_slot=hour_slot,
        n_weeks=n_weeks,
        highly_correlated_pairs=highly_correlated,
        cliques=cliques,
        groups=groups,
        members=members,
    )


def slot_correlation_matrix(
    vectors: dict[str, np.ndarray],
    ordered_ids: list[str],
    *,
    labels_by_id: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Symmetric per-slot Spearman matrix (diagonal 1.0, None -> NaN). For heatmaps /
    the notebook's worked example; the detector itself works off the pair list."""

    names = [labels_by_id[i] if labels_by_id else i for i in ordered_ids]
    mat = pd.DataFrame(np.nan, index=names, columns=names)
    pos = {iid: (labels_by_id[iid] if labels_by_id else iid) for iid in ordered_ids}
    for iid in ordered_ids:
        mat.loc[pos[iid], pos[iid]] = 1.0
    for a, b in combinations(ordered_ids, 2):
        rho = safe_spearman(vectors[a], vectors[b])
        if rho is None:
            continue
        mat.loc[pos[a], pos[b]] = rho
        mat.loc[pos[b], pos[a]] = rho
    return mat


# --------------------------------------------------------------------------------------
# Full volume builder
# --------------------------------------------------------------------------------------


def build_clique_volume(
    df: pd.DataFrame,
    ordered_ids: list[str],
    *,
    direction: Direction,
    weekdays: list[str],
    hour_slots: list[str],
    threshold: float = DEFAULT_THRESHOLD,
    moe_name: str = "volume",
) -> np.ndarray:
    """Stack per-slot clique detection into the binary `(n_int, n_days, n_hours)` volume.

    For each (weekday, hour) slot: gather the weekly vectors, detect contiguous cliques,
    and set the voxel to 1 for every intersection in some clique. This is the array
    `voxel_viz.render_voxel_volume` / `label_connected_components` consume. Run once per
    direction (the result is direction-specific).
    """

    n_int, n_days, n_hours = len(ordered_ids), len(weekdays), len(hour_slots)
    pos = {iid: i for i, iid in enumerate(ordered_ids)}
    volume = np.zeros((n_int, n_days, n_hours), dtype=int)

    for d_idx, weekday in enumerate(weekdays):
        for h_idx, hour_slot in enumerate(hour_slots):
            vectors = slot_weekly_vectors(
                df, direction=direction, weekday=weekday, hour_slot=hour_slot,
                ordered_ids=ordered_ids, moe_name=moe_name,
            )
            result = detect_slot_cliques(
                vectors, ordered_ids, threshold=threshold,
                direction=direction, weekday=weekday, hour_slot=hour_slot,
            )
            for member in result.members:
                volume[pos[member], d_idx, h_idx] = 1
    return volume


def build_slot_results(
    df: pd.DataFrame,
    ordered_ids: list[str],
    *,
    direction: Direction,
    weekdays: list[str],
    hour_slots: list[str],
    threshold: float = DEFAULT_THRESHOLD,
    moe_name: str = "volume",
) -> dict[tuple[str, str], SlotCliqueResult]:
    """Same loop as `build_clique_volume` but returns the full `SlotCliqueResult` per
    `(weekday, hour_slot)` -- handy for inspecting *which* groups formed where."""

    results: dict[tuple[str, str], SlotCliqueResult] = {}
    for weekday in weekdays:
        for hour_slot in hour_slots:
            vectors = slot_weekly_vectors(
                df, direction=direction, weekday=weekday, hour_slot=hour_slot,
                ordered_ids=ordered_ids, moe_name=moe_name,
            )
            results[(weekday, hour_slot)] = detect_slot_cliques(
                vectors, ordered_ids, threshold=threshold,
                direction=direction, weekday=weekday, hour_slot=hour_slot,
            )
    return results
