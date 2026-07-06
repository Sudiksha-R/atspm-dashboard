"""MoE catalog for the bottleneck detection module.

Each MoEConfig entry encodes:
  - which direction is "bad" (above / below the threshold)
  - a sensible prototype default threshold
  - the hard physical worst-reference for intensity normalization
  - human-readable label and unit string

These are product choices, not statistical constants. The catalog drives the
ThresholdSpec constructor so callers don't have to remember which direction
applies to each MoE.

Integration note: extend this dict when new MoEs are available in the dashboard.
The module's threshold.py and viz_3d.py are agnostic to the specific MoE — they
only consume ThresholdSpec, which MoEConfig produces.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MoEConfig:
    """Metadata for one Measure of Effectiveness."""

    name: str
    label: str            # human-friendly name for plot titles
    unit: str             # axis/hover unit string
    bad_direction: str    # "above" (high = bad) or "below" (low = bad)
    default_threshold: float
    worst_ref: float      # hard physical limit for intensity normalisation
    description: str      # one-line plain-language explanation for UI tooltips


# ── Catalog ──────────────────────────────────────────────────────────────────
# Keys match the moe_name strings used in RawObservation records.

MOE_CATALOG: dict[str, MoEConfig] = {
    "aorPct": MoEConfig(
        name="aorPct",
        label="Arrival on Red %",
        unit="%",
        bad_direction="above",
        default_threshold=70.0,
        worst_ref=100.0,
        description="Percentage of vehicles arriving during the red phase. "
                    "High values indicate poor signal coordination.",
    ),
    "aogPct": MoEConfig(
        name="aogPct",
        label="Arrival on Green %",
        unit="%",
        bad_direction="below",
        default_threshold=40.0,
        worst_ref=0.0,
        description="Percentage of vehicles arriving during the green phase. "
                    "Low values indicate vehicles are not benefiting from signal timing.",
    ),
    "splitFail": MoEConfig(
        name="splitFail",
        label="Split Failures",
        unit="per hr",
        bad_direction="above",
        default_threshold=3.0,
        worst_ref=20.0,
        description="Number of times the phase ran out of green time per hour. "
                    "High values indicate the phase is under-served.",
    ),
    "maxOuts": MoEConfig(
        name="maxOuts",
        label="Max-Outs",
        unit="per hr",
        bad_direction="above",
        default_threshold=5.0,
        worst_ref=30.0,
        description="Number of times a phase terminated by reaching maximum green "
                    "time per hour. High values indicate demand exceeds capacity.",
    ),
    "avgWait": MoEConfig(
        name="avgWait",
        label="Average Wait",
        unit="sec",
        bad_direction="above",
        default_threshold=55.0,
        worst_ref=180.0,
        description="Average vehicle delay waiting at the stop bar, in seconds. "
                    "High values indicate poor service level.",
    ),
    "throughput": MoEConfig(
        name="throughput",
        label="Throughput",
        unit="veh/hr",
        bad_direction="below",
        default_threshold=300.0,
        worst_ref=0.0,
        description="Number of vehicles processed per hour. Low values indicate "
                    "intersection is not moving traffic efficiently.",
    ),
}


def get_moe_config(moe_name: str) -> MoEConfig:
    """Look up a MoEConfig by name.  Raises KeyError with a helpful message if unknown."""
    if moe_name not in MOE_CATALOG:
        known = ", ".join(sorted(MOE_CATALOG))
        raise KeyError(
            f"Unknown MoE '{moe_name}'. Known MoEs: {known}. "
            "Add a new entry to MOE_CATALOG in config.py to extend."
        )
    return MOE_CATALOG[moe_name]
