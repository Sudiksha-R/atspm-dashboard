"""Correlation-based intersection grouping.

Where the rest of the pipeline asks "how synced is each *adjacent* pair?", this module
asks "which intersections form *mutually*-synced, spatially-contiguous groups?" -- the
natural next question once a corridor has more than a couple of intersections.

Algorithm (one direction at a time; the caller runs it once per phase/direction):

  1. All-pairs correlation. Compute the Spearman ρ between *every* pair of selected
     intersections (consecutive or not), not just adjacent ones -- reusing the exact
     same align -> normalize -> similarity path the adjacent-pair pipeline uses
     (pipeline._build_pair_result), so a grouping ρ and a map-arc ρ for the same pair
     are guaranteed identical.

  2. Threshold. Keep the pairs whose ρ clears a tunable threshold (default +0.60).
     These are the "highly correlated" pairs -- the edges of a similarity graph whose
     nodes are intersections.

  3. Cliques. A group in which *everyone* moves with *everyone* is exactly a clique of
     that graph. We enumerate all maximal cliques with networkx (Bron-Kerbosch via
     nx.find_cliques) rather than rolling our own.

  4. Contiguity. A clique can be spatially scattered (e.g. {A, B, D, E} with C left
     out). We require a reported group to be physically connected along the corridor,
     so each maximal clique is split into maximal runs of corridor-consecutive members
     ({A,B,D,E} -> {A,B} and {D,E}); runs of size < 2 are dropped, and any run that is
     a subset of another surviving run is discarded.

  5. Result. A GroupingResult (with .as_dict() for a plain, serialisable payload)
     carrying the surviving groups plus every pertinent detail: members + labels,
     corridor span, the supporting intra-group ρ values, and min/mean/max cohesion.

On the bundled fixture this yields {A, B} eastbound (LR/phase2) and {D, E, F} westbound
(RL/phase6) -- the same direction-aware "flip" the rest of the prototype demonstrates,
now expressed as groups instead of pairwise arcs.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from itertools import combinations

import networkx as nx
import pandas as pd

from .alignment import observations_to_frame
from .pipeline import _build_pair_result
from .schemas import AnalysisRequest, Direction, PairResult, RawObservation

DEFAULT_THRESHOLD = 0.60
"""Minimum Spearman ρ for a pair to count as "highly correlated" -- a product choice,
not a statistical constant, so it lives here as a single tunable knob. Positive only:
grouping is about intersections that move *together*, so an inverse pair (ρ <= -0.6) is
deliberately not an edge."""

GROUPING_METRIC = "spearman_rho"
"""The correlation grouping is computed on. Matches the pipeline's headline metric so a
group's edges line up with the map's headline arcs."""


# --- Result schema ----------------------------------------------------------------


@dataclass(frozen=True)
class CorrelationGroup:
    """One contiguous, mutually-highly-correlated cluster of intersections."""

    group_id: str
    direction: Direction
    phase_group: str
    members: list[str]
    """Intersection ids, in corridor (left-to-right) order, regardless of direction."""
    member_labels: list[str]
    corridor_order_span: tuple[int, int]
    """(first, last) corridor-order index spanned -- contiguous, so last - first == size - 1."""
    size: int
    internal_pairs: list[dict]
    """Every within-group pair and its ρ: {"a", "b", "labels", "spearman_rho"}."""
    min_internal_rho: float
    mean_internal_rho: float
    max_internal_rho: float

    def as_dict(self) -> dict:
        d = asdict(self)
        d["direction"] = self.direction.value
        d["corridor_order_span"] = list(self.corridor_order_span)
        return d


@dataclass(frozen=True)
class GroupingResult:
    """Everything the grouping step produced for one direction/phase."""

    direction: Direction
    phase_group: str
    moe_name: str
    metric: str
    threshold: float
    ordered_intersections: list[str]
    labels_by_id: dict[str, str]
    all_pairs: list[PairResult] = field(default_factory=list)
    highly_correlated_pairs: list[dict] = field(default_factory=list)
    groups: list[CorrelationGroup] = field(default_factory=list)
    ungrouped: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        """Plain, JSON-serialisable summary -- the deliverable payload (drops the
        bulky per-pair raw/norm vectors in all_pairs; those stay on the dataclass for
        callers that want them)."""

        return {
            "direction": self.direction.value,
            "phase_group": self.phase_group,
            "moe_name": self.moe_name,
            "metric": self.metric,
            "threshold": self.threshold,
            "n_intersections": len(self.ordered_intersections),
            "highly_correlated_pairs": self.highly_correlated_pairs,
            "groups": [g.as_dict() for g in self.groups],
            "ungrouped": list(self.ungrouped),
        }


# --- Core steps -------------------------------------------------------------------


def _ordered_selected(request: AnalysisRequest) -> list[str]:
    """Selected intersections in canonical corridor (left-to-right) order. Grouping is
    direction-agnostic in its *geometry* -- D, E, F are physically adjacent whether you
    drive east or west -- so contiguity always uses this one canonical order; only the
    correlation *values* differ by direction (they come from that phase's rows)."""

    selected = set(request.selected_intersections)
    return [iid for iid in request.ordered_intersections if iid in selected]


def compute_all_pairs(
    request: AnalysisRequest,
    rows: list[RawObservation],
    *,
    normalizer=None,
) -> list[PairResult]:
    """Full PairResult for every unordered pair of selected intersections (not just
    adjacent ones), each walked in canonical corridor order so pair_id == "a__b" with
    a before b. Reuses pipeline._build_pair_result, so every metric matches what the
    adjacent-pair pipeline would compute for the same two intersections."""

    from .normalization import DEFAULT_NORMALIZER

    normalizer = normalizer or DEFAULT_NORMALIZER
    raw_rows = observations_to_frame(rows)
    ids = _ordered_selected(request)
    return [
        _build_pair_result(
            intersection_a=a,
            intersection_b=b,
            request=request,
            raw_rows=raw_rows,
            normalizer=normalizer,
        )
        for a, b in combinations(ids, 2)
    ]


def build_correlation_matrix(
    pairs: list[PairResult],
    ordered_ids: list[str],
    *,
    metric: str = GROUPING_METRIC,
    labels_by_id: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Symmetric all-pairs correlation matrix (diagonal = 1.0, missing = NaN), indexed
    in corridor order. Purely for inspection/heatmaps -- the grouping itself works off
    the pair list, not this matrix."""

    index = [labels_by_id[i] if labels_by_id else i for i in ordered_ids]
    mat = pd.DataFrame(float("nan"), index=index, columns=index)
    pos = {iid: (labels_by_id[iid] if labels_by_id else iid) for iid in ordered_ids}
    for iid in ordered_ids:
        mat.loc[pos[iid], pos[iid]] = 1.0
    for p in pairs:
        val = getattr(p, metric)
        if val is None:
            continue
        a, b = pos[p.intersection_a], pos[p.intersection_b]
        mat.loc[a, b] = val
        mat.loc[b, a] = val
    return mat


def _contiguous_runs(members: list[str], order_of: dict[str, int]) -> list[list[str]]:
    """Split a set of intersections into maximal runs whose corridor-order indices are
    consecutive. {0,1,3,4} -> [[0,1],[3,4]]; {1,3,4,5} -> [[1],[3,4,5]]. This is what
    enforces "a group must be spatially connected"."""

    ordered = sorted(members, key=lambda m: order_of[m])
    runs: list[list[str]] = [[ordered[0]]]
    for prev, cur in zip(ordered, ordered[1:]):
        if order_of[cur] == order_of[prev] + 1:
            runs[-1].append(cur)
        else:
            runs.append([cur])
    return runs


def find_correlation_groups(
    pairs: list[PairResult],
    ordered_ids: list[str],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    metric: str = GROUPING_METRIC,
    labels_by_id: dict[str, str] | None = None,
    direction: Direction = Direction.LR,
    phase_group: str = "",
    id_prefix: str = "g",
) -> list[CorrelationGroup]:
    """Steps 2-4: threshold -> maximal cliques -> contiguous runs -> CorrelationGroups.

    Returned groups are sorted by corridor position; each is guaranteed contiguous,
    size >= 2, and not a subset of any other returned group.
    """

    labels_by_id = labels_by_id or {i: i for i in ordered_ids}
    order_of = {iid: idx for idx, iid in enumerate(ordered_ids)}
    rho_of: dict[frozenset[str], float] = {}

    # Step 2: build the "highly correlated" similarity graph.
    graph = nx.Graph()
    graph.add_nodes_from(ordered_ids)
    for p in pairs:
        val = getattr(p, metric)
        if val is None:
            continue
        rho_of[frozenset((p.intersection_a, p.intersection_b))] = val
        if val >= threshold:
            graph.add_edge(p.intersection_a, p.intersection_b)

    # Step 3: every "all move with all" group is a clique.
    cliques = [set(c) for c in nx.find_cliques(graph) if len(c) >= 2]

    # Step 4: enforce spatial contiguity, then drop singletons / subsets / duplicates.
    candidates: list[frozenset[str]] = []
    for clique in cliques:
        for run in _contiguous_runs(list(clique), order_of):
            if len(run) >= 2:
                candidates.append(frozenset(run))
    candidates = list(dict.fromkeys(candidates))  # dedupe, keep first-seen order
    survivors = [
        c for c in candidates if not any(c < other for other in candidates)
    ]

    survivors.sort(key=lambda c: min(order_of[m] for m in c))

    groups: list[CorrelationGroup] = []
    for idx, member_set in enumerate(survivors, start=1):
        members = sorted(member_set, key=lambda m: order_of[m])
        internal = []
        rhos = []
        for a, b in combinations(members, 2):
            rho = rho_of.get(frozenset((a, b)))
            if rho is None:
                continue
            rhos.append(rho)
            internal.append(
                {
                    "a": a,
                    "b": b,
                    "labels": [labels_by_id[a], labels_by_id[b]],
                    metric: round(rho, 4),
                }
            )
        groups.append(
            CorrelationGroup(
                group_id=f"{id_prefix}{idx}",
                direction=direction,
                phase_group=phase_group,
                members=members,
                member_labels=[labels_by_id[m] for m in members],
                corridor_order_span=(order_of[members[0]], order_of[members[-1]]),
                size=len(members),
                internal_pairs=internal,
                min_internal_rho=round(min(rhos), 4) if rhos else float("nan"),
                mean_internal_rho=round(sum(rhos) / len(rhos), 4) if rhos else float("nan"),
                max_internal_rho=round(max(rhos), 4) if rhos else float("nan"),
            )
        )
    return groups


def run_grouping_analysis(
    request: AnalysisRequest,
    rows: list[RawObservation],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    metric: str = GROUPING_METRIC,
    labels_by_id: dict[str, str] | None = None,
    normalizer=None,
) -> GroupingResult:
    """End-to-end grouping for one direction/phase: all-pairs ρ -> threshold ->
    cliques -> contiguous groups, wrapped in a GroupingResult (.as_dict() for the
    plain payload). Call once per direction (LR with phase-2 rows, RL with phase-6
    rows) and combine the two .as_dict()s for the full corridor picture."""

    ordered_ids = _ordered_selected(request)
    labels_by_id = labels_by_id or {i: i for i in ordered_ids}

    all_pairs = compute_all_pairs(request, rows, normalizer=normalizer)

    highly_correlated = [
        {
            "a": p.intersection_a,
            "b": p.intersection_b,
            "labels": [labels_by_id[p.intersection_a], labels_by_id[p.intersection_b]],
            metric: round(getattr(p, metric), 4),
        }
        for p in all_pairs
        if getattr(p, metric) is not None and getattr(p, metric) >= threshold
    ]

    groups = find_correlation_groups(
        all_pairs,
        ordered_ids,
        threshold=threshold,
        metric=metric,
        labels_by_id=labels_by_id,
        direction=request.direction,
        phase_group=request.phase_group,
        id_prefix=f"{request.direction.value}_g",
    )

    grouped_members = {m for g in groups for m in g.members}
    ungrouped = [i for i in ordered_ids if i not in grouped_members]

    return GroupingResult(
        direction=request.direction,
        phase_group=request.phase_group,
        moe_name=request.moe_name,
        metric=metric,
        threshold=threshold,
        ordered_intersections=ordered_ids,
        labels_by_id=labels_by_id,
        all_pairs=all_pairs,
        highly_correlated_pairs=highly_correlated,
        groups=groups,
        ungrouped=ungrouped,
    )
