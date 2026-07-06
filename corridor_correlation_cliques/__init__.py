"""Corridor correlation cliques -> 3D voxel volume (self-contained prototype).

A stand-alone package the dashboard UI developer can ship as-is. It takes hourly
Measure-of-Effectiveness data (here synthetic "volume" / vehicles-per-hour over 12 weeks),
and for every (weekday, hour-of-day) slot independently computes which intersections form
**week-over-week correlation cliques**, stacking the answers into a 6 x 7 x 16 binary
volume that is rendered as a rotatable 3D view with connected-component analysis.

Pipeline:

    fixture_data.generate_volume_observations()   # or a real DB query
        -> long-format DataFrame of hourly observations
    slot_cliques.build_clique_volume(df, ..., direction=...)
        -> (intersection x day x hour) binary numpy volume
    voxel_viz.render_voxel_volume(volume)          # 3D figure
    voxel_viz.label_connected_components / find_component_cuboids

The package has **no import dependency outside this folder** (its `Direction` /
`RawObservation` types are local copies). See README.md for the full design rationale.

Module map:

    schemas        local Direction + RawObservation (the data contract)
    fixture_data   12-week synthetic directional Volume generator + CSV read/write
    slot_cliques   per-slot week-over-week Spearman -> threshold -> cliques -> contiguity -> volume
    voxel_viz      3D voxel renderer + 6-connectivity connected components + largest-cuboid
                   (copied verbatim from the wider prototype's synchrony.voxel_viz)
"""

from .fixture_data import (
    CORRIDOR_ID,
    CORRIDOR_NAME,
    DAY_LABELS,
    DEFAULT_CSV_PATH,
    HOUR_SLOTS,
    INTERSECTION_LABELS,
    MOE_NAME,
    N_WEEKS,
    ORDERED_INTERSECTIONS,
    generate_volume_observations,
    load_csv,
    observations_to_dataframe,
    slot_group,
    write_csv,
)
from .schemas import Direction, RawObservation
from .slot_cliques import (
    DEFAULT_THRESHOLD,
    SlotCliqueResult,
    build_clique_volume,
    build_slot_results,
    contiguous_runs,
    detect_slot_cliques,
    safe_spearman,
    slot_correlation_matrix,
    slot_weekly_vectors,
)
from .voxel_viz import (
    Cuboid,
    VoxelComponent,
    components_to_frame,
    find_component_cuboids,
    generate_synthetic_volume,
    hour_range_label,
    label_connected_components,
    render_voxel_volume,
)

__all__ = [
    # schemas
    "Direction",
    "RawObservation",
    # fixture / data
    "CORRIDOR_ID",
    "CORRIDOR_NAME",
    "DAY_LABELS",
    "HOUR_SLOTS",
    "INTERSECTION_LABELS",
    "MOE_NAME",
    "N_WEEKS",
    "ORDERED_INTERSECTIONS",
    "DEFAULT_CSV_PATH",
    "generate_volume_observations",
    "observations_to_dataframe",
    "write_csv",
    "load_csv",
    "slot_group",
    # per-slot cliques
    "DEFAULT_THRESHOLD",
    "SlotCliqueResult",
    "safe_spearman",
    "slot_weekly_vectors",
    "slot_correlation_matrix",
    "detect_slot_cliques",
    "build_clique_volume",
    "build_slot_results",
    "contiguous_runs",
    # voxel viz
    "render_voxel_volume",
    "label_connected_components",
    "find_component_cuboids",
    "components_to_frame",
    "hour_range_label",
    "generate_synthetic_volume",
    "VoxelComponent",
    "Cuboid",
]
