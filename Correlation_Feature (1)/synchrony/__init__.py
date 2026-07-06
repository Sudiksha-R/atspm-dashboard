"""Adjacent-intersection synchrony analysis prototype.

See DOCS/corridor_synchrony_blueprint.md for the full engineering specification this
package implements. Module map:

  schemas        typed request/response data structures (the data contract)
  alignment      deterministic (week_id, weekday, hour_slot) alignment + adjacent-pair generation
  normalization  swappable within-intersection normalization (default: min-max)
  similarity     Pearson / Spearman / cosine safe wrappers, quality flags, interpretation labels
  pipeline       run_pairwise_synchrony_analysis -- orchestrates the above
  presentation   adapts results into table / corridor-edge / ranked-list data structures
  viz            Plotly renderers for the three UI deliverables
  fixture_data   hand-crafted toy dataset with known, traceable sync patterns
  geo_config     loader for the corridor geo + pairwise-direction JSON (coords, phase mapping)
  geo_viz        Plotly map renderer: real intersection locations + directional similarity arrows
  grouping       all-pairs correlation -> threshold -> cliques -> contiguous groups
"""

from .geo_config import CorridorGeoConfig, load_corridor_geo_config
from .geo_viz import (
    render_corridor_link_table,
    render_corridor_map,
    render_group_arrow_map,
    render_grouping_map,
)
from .grouping import (
    CorrelationGroup,
    GroupingResult,
    build_correlation_matrix,
    compute_all_pairs,
    find_correlation_groups,
    run_grouping_analysis,
)
from .pipeline import run_pairwise_synchrony_analysis
from .schemas import AnalysisRequest, Direction, PairResult, RawObservation, UISummary

__all__ = [
    "run_pairwise_synchrony_analysis",
    "AnalysisRequest",
    "Direction",
    "PairResult",
    "RawObservation",
    "UISummary",
    "CorridorGeoConfig",
    "load_corridor_geo_config",
    "render_corridor_map",
    "render_corridor_link_table",
    "render_grouping_map",
    "render_group_arrow_map",
    "run_grouping_analysis",
    "find_correlation_groups",
    "compute_all_pairs",
    "build_correlation_matrix",
    "CorrelationGroup",
    "GroupingResult",
]
