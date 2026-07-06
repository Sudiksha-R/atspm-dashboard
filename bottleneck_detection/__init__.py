"""Bottleneck detection prototype.

Threshold-triggered 3D voxel view (intersection x day-of-week x hour-of-day) of
an unfavorable MoE, with a dual encoding — color = severity, opacity =
persistence — plus a companion per-voxel weekly distribution panel.

Pipeline:
    fixture_data.generate_*_aor_observations()   -> list[RawObservation]
        -> aggregation.aggregate_by_slot(rows, request)        -> DataFrame
        -> threshold.apply_threshold(df, spec)                 -> DataFrame
        -> volume.build_intensity_volume(df, request)          -> BottleneckVolume
        -> viz_3d.render_bottleneck_volume(vol, spec)          -> go.Figure
        -> viz_distribution.render_worst_voxel_distribution()  -> go.Figure

See DESIGN.md for the full design and integration notes.
"""

from __future__ import annotations

from .config import MOE_CATALOG, MoEConfig, get_moe_config
from .schemas import (
    BottleneckRequest,
    BottleneckVolume,
    Direction,
    RawObservation,
    SlotStats,
    ThresholdSpec,
)
from .fixture_data import (
    CORRIDOR_ID,
    DAY_LABELS,
    HOUR_SLOTS,
    HOUR_START,
    INTERSECTION_LABELS,
    MOE_NAME,
    N_WEEKS,
    ORDERED_INTERSECTIONS,
    generate_lr_aor_observations,
    generate_rl_aor_observations,
)
from .aggregation import aggregate_by_slot, observations_to_frame
from .threshold import apply_threshold
from .volume import build_intensity_volume
from .viz_3d import render_bottleneck_volume
from .viz_distribution import render_worst_voxel_distribution

__all__ = [
    # config
    "MoEConfig", "MOE_CATALOG", "get_moe_config",
    # schemas
    "Direction", "RawObservation", "ThresholdSpec",
    "BottleneckRequest", "SlotStats", "BottleneckVolume",
    # fixture
    "generate_lr_aor_observations", "generate_rl_aor_observations",
    "ORDERED_INTERSECTIONS", "INTERSECTION_LABELS", "DAY_LABELS",
    "HOUR_SLOTS", "HOUR_START", "N_WEEKS", "MOE_NAME", "CORRIDOR_ID",
    # pipeline
    "observations_to_frame", "aggregate_by_slot",
    "apply_threshold", "build_intensity_volume",
    # renderers
    "render_bottleneck_volume", "render_worst_voxel_distribution",
]
