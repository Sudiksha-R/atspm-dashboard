from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping, Sequence

import pandas as pd

from .schemas import AnalysisRequest, RawObservation


@dataclass(frozen=True)
class GroupingResult:
    request: AnalysisRequest
    groups: list[dict]
    ungrouped: list[str]
    adjacent_correlations: list[dict]

    def as_dict(self) -> dict:
        request = asdict(self.request)
        request["direction"] = self.request.direction.value
        request["start_date"] = self.request.start_date.isoformat()
        request["end_date"] = self.request.end_date.isoformat()
        return {
            "request": request,
            "direction": self.request.direction.value,
            "phase_group": self.request.phase_group,
            "moe_name": self.request.moe_name,
            "groups": self.groups,
            "ungrouped": self.ungrouped,
            "adjacent_correlations": self.adjacent_correlations,
        }


def _correlation(left: pd.Series, right: pd.Series) -> float | None:
    paired = pd.concat([left, right], axis=1, join="inner").dropna()
    if len(paired) < 2 or paired.iloc[:, 0].nunique() < 2 or paired.iloc[:, 1].nunique() < 2:
        return None
    value = paired.iloc[:, 0].corr(paired.iloc[:, 1], method="spearman")
    return None if pd.isna(value) else float(value)


def run_grouping_analysis(
    request: AnalysisRequest,
    observations: Sequence[RawObservation],
    *,
    threshold: float = 0.25,
    labels_by_id: Mapping[str, str] | None = None,
) -> GroupingResult:
    labels = labels_by_id or {}
    selected = set(request.selected_intersections)
    rows = [
        row for row in observations
        if row.direction == request.direction
        and row.intersection_id in selected
        and row.moe_name == request.moe_name
        and request.start_date <= row.date <= request.end_date
        and (not request.weekday_filter or row.weekday in request.weekday_filter or row.date.strftime("%A") in request.weekday_filter)
        and (not request.hour_slot_filter or row.hour_slot in request.hour_slot_filter)
    ]
    if not rows:
        return GroupingResult(request, [], list(request.selected_intersections), [])

    frame = pd.DataFrame({
        "intersection_id": [row.intersection_id for row in rows],
        "date": [row.date for row in rows],
        "hour_slot": [row.hour_slot for row in rows],
        "value": [row.raw_value for row in rows],
    })
    series = {
        intersection_id: part.groupby(["date", "hour_slot"])["value"].mean()
        for intersection_id, part in frame.groupby("intersection_id")
    }
    ordered = [item for item in request.ordered_intersections if item in selected]
    edges: list[dict] = []
    for left, right in zip(ordered, ordered[1:]):
        rho = _correlation(series.get(left, pd.Series(dtype=float)), series.get(right, pd.Series(dtype=float)))
        edges.append({"left": left, "right": right, "rho": rho, "passes": rho is not None and rho >= threshold})

    runs: list[list[str]] = []
    current: list[str] = []
    for edge in edges:
        if edge["passes"]:
            if not current:
                current = [edge["left"], edge["right"]]
            elif current[-1] == edge["left"]:
                current.append(edge["right"])
        elif current:
            runs.append(current)
            current = []
    if current:
        runs.append(current)

    groups: list[dict] = []
    grouped_members: set[str] = set()
    for index, members in enumerate(runs, start=1):
        member_set = set(members)
        internal = [edge["rho"] for edge in edges if edge["left"] in member_set and edge["right"] in member_set and edge["rho"] is not None]
        grouped_members.update(members)
        groups.append({
            "group_id": f"{request.direction.value.lower()}-{index}",
            "label": f"{'Eastbound' if request.direction.value == 'LR' else 'Westbound'} Group {index}",
            "members": members,
            "member_labels": [labels.get(member, member) for member in members],
            "size": len(members),
            "mean_internal_rho": float(sum(internal) / len(internal)) if internal else 0.0,
            "corridor_order_span": [ordered.index(members[0]), ordered.index(members[-1])],
        })
    return GroupingResult(request, groups, [item for item in ordered if item not in grouped_members], edges)
