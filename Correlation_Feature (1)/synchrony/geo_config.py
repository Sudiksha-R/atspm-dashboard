"""Typed loader for the corridor geo + pairwise-direction config.

This is the "JSON for future extension" the user asked for: a small, hand-editable
file (synchrony/config/bee_ridge_corridor_directions.json) that lists, for every
adjacent pair of intersections, both directions of travel and which phase_group
governs each one. That (pair, direction) -> phase mapping is intersection-geometry
-specific -- it can't be derived from coordinates or the corridor order alone -- so
it has to live somewhere a human can read and edit it. This module just parses that
file into typed dataclasses; see the JSON's own "notes" field for the extension recipe.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .schemas import Direction

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config" / "bee_ridge_corridor_directions.json"


@dataclass(frozen=True)
class IntersectionLocation:
    """One mapped intersection: id, friendly label, corridor position, and coordinates."""

    intersection_id: str
    label: str
    corridor_order: int
    lat: float
    lon: float


@dataclass(frozen=True)
class TravelDirection:
    """One direction of travel between an adjacent pair, and the phase_group that
    governs it -- the exact "phase for the direction of travel" mapping the user
    asked to make extensible.
    """

    direction: Direction
    travel_from: str
    travel_to: str
    phase_group: str
    travel_label: str

    @property
    def pair_lookup_id(self) -> str:
        """The PairResult.pair_id this direction corresponds to in a UISummary.

        The pipeline builds pair_id as f"{intersection_a}__{intersection_b}" where
        intersection_a/b come from walking the corridor in the *requested* direction
        (forwards for LR, reversed for RL -- see pipeline._sort_intersections_in_direction).
        That's exactly travel_from -> travel_to here, so the two ids line up directly.
        """

        return f"{self.travel_from}__{self.travel_to}"


@dataclass(frozen=True)
class PairwiseDirectionConfig:
    """Both directions of travel between one adjacent pair of intersections."""

    pair_id: str
    intersection_a: str
    intersection_b: str
    directions: list[TravelDirection]


@dataclass(frozen=True)
class CorridorGeoConfig:
    corridor_id: str
    corridor_name: str
    intersections: list[IntersectionLocation]
    pairwise_directions: list[PairwiseDirectionConfig]

    def intersections_by_id(self) -> dict[str, IntersectionLocation]:
        return {i.intersection_id: i for i in self.intersections}

    def ordered_intersection_ids(self) -> list[str]:
        return [i.intersection_id for i in sorted(self.intersections, key=lambda i: i.corridor_order)]


def load_corridor_geo_config(path: Path | str = DEFAULT_CONFIG_PATH) -> CorridorGeoConfig:
    """Parse the JSON config into typed dataclasses.

    Deliberately fails loudly (KeyError/ValueError) on malformed input rather than
    guessing -- this file is meant to be hand-edited, so a clear error on a typo'd
    key beats a silently-wrong map render.
    """

    raw = json.loads(Path(path).read_text())

    intersections = [
        IntersectionLocation(
            intersection_id=i["intersection_id"],
            label=i["label"],
            corridor_order=i["corridor_order"],
            lat=i["lat"],
            lon=i["lon"],
        )
        for i in raw["intersections"]
    ]

    pairwise_directions = [
        PairwiseDirectionConfig(
            pair_id=p["pair_id"],
            intersection_a=p["intersection_a"],
            intersection_b=p["intersection_b"],
            directions=[
                TravelDirection(
                    direction=Direction(d["direction"]),
                    travel_from=d["travel_from"],
                    travel_to=d["travel_to"],
                    phase_group=d["phase_group"],
                    travel_label=d["travel_label"],
                )
                for d in p["directions"]
            ],
        )
        for p in raw["pairwise_directions"]
    ]

    return CorridorGeoConfig(
        corridor_id=raw["corridor_id"],
        corridor_name=raw["corridor_name"],
        intersections=intersections,
        pairwise_directions=pairwise_directions,
    )
