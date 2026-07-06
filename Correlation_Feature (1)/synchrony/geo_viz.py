"""Plotly map renderer: real intersection locations + pairwise directional-similarity
arrows along a corridor.

This is the geo counterpart to the corridor-edge view in viz.py: instead of an
abstract node/edge diagram, it places intersections at their real lat/lon and draws
each adjacent pair's *two* directions of travel as separate curved arcs -- one bowing
to either side of the straight-line corridor, the way flight-route maps draw outbound
and return paths -- each tipped with a large arrowhead pointing at its destination
intersection and colored by that direction's headline synchrony score.

Like the rest of the package, this is meant as a *reference* the dashboard's UI
developer can lift wholesale -- it consumes the same UISummary/PairResult objects
the rest of the pipeline produces, plus a CorridorGeoConfig (geo_config.py) that
supplies coordinates and the (pair, direction) -> phase_group mapping.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Sequence

import pandas as pd
import plotly.graph_objects as go
from plotly.colors import sample_colorscale

from .geo_config import CorridorGeoConfig, IntersectionLocation, TravelDirection
from .schemas import Direction, PairResult, UISummary

if TYPE_CHECKING:  # avoid a runtime import cycle; render_grouping_map only duck-types these
    from .grouping import CorrelationGroup

MAP_STYLE = "open-street-map"
"""Tile style that needs no Mapbox token -- keeps this runnable standalone."""

ARROW_COLORSCALE = "RdBu"
"""Diverging colorscale for arc/arrowhead colors, centered at 0 (white/neutral):
dark red = strong inverse sync (score -> -1), dark blue = strong positive sync
(score -> +1). Color *saturation* tracks |score|, so "how intense" reads directly
as "how strong the sync is", independent of which way it points -- a deliberate
departure from the rest of the package's RdYlGn (viz._score_to_color), chosen for
the map specifically because red/blue is the more conventional correlation-sign
convention and reads cleanly against OSM's green/cream basemap palette."""


def _score_to_arrow_color(score: float | None) -> str:
    """Headline-score -> color for an arc/arrowhead, on the red<->blue scale above
    (mirrors viz._score_to_color's [-1, 1] -> colorscale-position mapping)."""

    if score is None:
        return "rgba(120,120,120,0.6)"  # neutral grey for not-computable
    t = (max(-1.0, min(1.0, score)) + 1.0) / 2.0
    return sample_colorscale(ARROW_COLORSCALE, [t])[0]

BOW_FRACTION = 0.16
"""Arc height as a fraction of the segment's straight-line length. Each direction's
arc bows toward the right-hand side of *its own* direction of travel, so A->B and
B->A automatically curve to opposite sides of the road -- no sign bookkeeping needed
(B->A's "rightward" is A->B's "leftward"). See _bezier_arc."""

N_CURVE_POINTS = 60
"""Points sampled along each arc -- enough to look smooth at street-level zoom."""

ARROWHEAD_T = 0.90
"""Where along the arc (0 = origin, 1 = destination) the arrowhead's tip sits.
Short of 1.0 so the large arrowhead doesn't sit on top of / get hidden by the
destination intersection's node marker."""

ARROWHEAD_TANGENT_DELTA = 0.06
"""How far back along the arc to sample a second point for the arrowhead's bearing,
so it points along the curve's *local* direction at the tip rather than the
straight-line A->B bearing -- it should look like it's following the curve."""

ARROWHEAD_LENGTH_FRACTION = 0.075
"""Arrowhead tip-to-base length, as a fraction of the segment's straight-line length."""

ARROWHEAD_WIDTH_FRACTION = 0.05
"""Arrowhead base width, as a fraction of the segment's straight-line length."""


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:+.3f}"


def _bearing_degrees(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    """Compass bearing from point A to point B, in degrees [0, 360), where
    0 = due north and 90 = due east (standard great-circle initial-bearing formula)."""

    lat1, lat2 = math.radians(lat_a), math.radians(lat_b)
    dlon = math.radians(lon_b - lon_a)
    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def _segment_length_degrees(lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float:
    """Straight-line distance from A to B, in degrees of latitude-equivalent "true
    distance" (longitude degrees scaled by cos(latitude) so x/y share one scale).
    Used to size arcs and arrowheads proportionally to each segment's real length.
    """

    mean_lat_rad = math.radians((lat_a + lat_b) / 2.0)
    cos_lat = math.cos(mean_lat_rad)
    dx = (lon_b - lon_a) * cos_lat
    dy = lat_b - lat_a
    return math.hypot(dx, dy)


def _offset_point(lat: float, lon: float, bearing_degrees: float, distance_degrees: float) -> tuple[float, float]:
    """The point reached by walking `distance_degrees` (degrees of latitude-equivalent
    true distance) from (lat, lon) in compass direction `bearing_degrees`.

    Converts the bearing into north/east "true distance" components, then converts
    the eastward component back into longitude degrees by dividing out cos(latitude)
    (the same correction _segment_length_degrees applies in reverse). A flat-earth
    approximation -- entirely adequate at the km scale this prototype operates at.
    """

    theta = math.radians(bearing_degrees)
    north = distance_degrees * math.cos(theta)
    east = distance_degrees * math.sin(theta)
    cos_lat = math.cos(math.radians(lat))
    return lat + north, lon + (east / cos_lat if cos_lat else 0.0)


def _bezier_point(
    lat_a: float, lon_a: float, ctrl_lat: float, ctrl_lon: float, lat_b: float, lon_b: float, t: float
) -> tuple[float, float]:
    one_minus_t = 1.0 - t
    lat = one_minus_t**2 * lat_a + 2.0 * one_minus_t * t * ctrl_lat + t**2 * lat_b
    lon = one_minus_t**2 * lon_a + 2.0 * one_minus_t * t * ctrl_lon + t**2 * lon_b
    return lat, lon


def _bezier_arc(
    lat_a: float, lon_a: float, lat_b: float, lon_b: float, *, bow_degrees: float, n_points: int = N_CURVE_POINTS
) -> tuple[list[float], list[float], tuple[float, float, float, float, float, float]]:
    """A smooth quadratic-Bezier arc from A to B that bows toward the right-hand
    side of the A->B direction of travel by `bow_degrees` (latitude-equivalent
    degrees).

    Because "right of A->B" is "left of B->A", calling this with the same positive
    `bow_degrees` for both directions of a pair makes their arcs curve to opposite
    sides of the straight-line corridor automatically -- exactly the visual
    separation flight-route maps use for outbound vs. return paths, and a clean way
    to guarantee the two directions never overlap regardless of segment geometry.

    Returns (lats, lons, control_geometry) where control_geometry is the raw
    (lat_a, lon_a, ctrl_lat, ctrl_lon, lat_b, lon_b) tuple, so callers can evaluate
    further points on the same curve (e.g. for arrowhead placement) without
    recomputing the control point.
    """

    forward_bearing = _bearing_degrees(lat_a, lon_a, lat_b, lon_b)
    mid_lat, mid_lon = (lat_a + lat_b) / 2.0, (lon_a + lon_b) / 2.0
    ctrl_lat, ctrl_lon = _offset_point(mid_lat, mid_lon, forward_bearing + 90.0, bow_degrees)

    geometry = (lat_a, lon_a, ctrl_lat, ctrl_lon, lat_b, lon_b)
    lats, lons = [], []
    for i in range(n_points):
        t = i / (n_points - 1)
        lat, lon = _bezier_point(*geometry, t=t)
        lats.append(lat)
        lons.append(lon)
    return lats, lons, geometry


def _arrowhead_polygon(
    tip_lat: float, tip_lon: float, bearing_degrees: float, *, length_degrees: float, width_degrees: float
) -> tuple[list[float], list[float]]:
    """A filled-triangle arrowhead: tip at (tip_lat, tip_lon), pointing in compass
    direction `bearing_degrees`, `length_degrees` from tip to base, `width_degrees`
    wide at the base. Returns a closed (lats, lons) ring suitable for fill="toself".
    """

    back_lat, back_lon = _offset_point(tip_lat, tip_lon, bearing_degrees + 180.0, length_degrees)
    half_width = width_degrees / 2.0
    c1_lat, c1_lon = _offset_point(back_lat, back_lon, bearing_degrees + 90.0, half_width)
    c2_lat, c2_lon = _offset_point(back_lat, back_lon, bearing_degrees - 90.0, half_width)
    return [tip_lat, c1_lat, c2_lat, tip_lat], [tip_lon, c1_lon, c2_lon, tip_lon]


def _direction_tooltip(travel: TravelDirection, pair: PairResult | None) -> str:
    lines = [
        f"<b>{travel.travel_label}</b>",
        f"Phase group: {travel.phase_group} · Direction: {travel.direction.value}",
    ]
    if pair is None:
        lines.append("No synchrony result loaded for this direction yet.")
    else:
        lines += [
            f"Headline ({pair.headline_metric}): {_fmt(pair.headline_score)} → {pair.interpretation_label}",
            f"Pearson r = {_fmt(pair.pearson_r)}  ·  Spearman ρ = {_fmt(pair.spearman_rho)}  ·  Cosine = {_fmt(pair.cosine_similarity)}",
            f"n_samples = {pair.n_samples}",
        ]
        if pair.quality_flags:
            lines.append(f"Flags: {', '.join(pair.quality_flags)}")
    return "<br>".join(lines)


def _fit_zoom(lats: list[float], lons: list[float], *, map_width_px: int = 1280, map_height_px: int = 480) -> float:
    """Auto-compute a go.Scattermap zoom level that fits all supplied coordinates.

    MapLibre (the engine behind go.Scattermap) uses Web Mercator tiles: at zoom z
    a 512-px tile covers 360/2^z degrees of longitude at the equator. We solve for
    the zoom that keeps the full bounding box within 70% of the viewport (both axes),
    then clamp to [12, 17] for sanity and round to one decimal place.
    """

    lon_span = max(lons) - min(lons) or 1e-4
    lat_span = max(lats) - min(lats) or 1e-4
    TILE_PX = 512
    FILL = 0.70  # use 70% of viewport; leaves breathing room for arcs bowing outside bbox

    zoom_lon = math.log2(360.0 * map_width_px / (TILE_PX * lon_span / FILL))
    zoom_lat = math.log2(180.0 * map_height_px / (TILE_PX * lat_span / FILL))
    return round(max(12.0, min(17.0, min(zoom_lon, zoom_lat))), 1)


def render_corridor_map(
    geo_config: CorridorGeoConfig,
    summaries_by_direction: dict[Direction, UISummary],
) -> go.Figure:
    """Render the corridor on a real basemap with curved directional similarity arrows.

    Parameters
    ----------
    geo_config
        Loaded corridor geo + pairwise-direction config (geo_config.load_corridor_geo_config).
        Supplies intersection coordinates and, per adjacent pair, both directions of
        travel with the phase_group that governs each.
    summaries_by_direction
        One UISummary per Direction that appears in the config (here: LR from a
        phase-2 run, RL from a phase-6 run). Each direction's PairResult is looked
        up by `TravelDirection.pair_lookup_id`, which matches the pair_id the
        pipeline produced for a run in that direction.

    For each adjacent pair, both directions of travel are drawn as separate curved
    arcs that bow to opposite sides of the straight-line corridor (see _bezier_arc --
    this guarantees they never overlap, the same way outbound/return flight routes
    are drawn on a route map):
      - arc color encodes that direction's headline synchrony score on a diverging
        red<->blue scale (ARROW_COLORSCALE = "RdBu"): dark red = strong inverse
        sync (-1), white = neutral (0), dark blue = strong positive sync (+1) --
        a deliberate map-specific departure from the rest of the package's RdYlGn
        (viz._score_to_color), since red/blue is the more conventional
        correlation-sign convention
      - a large filled arrowhead near the destination end points in the curve's
        local direction of travel (computed from the arc's own tangent, so it looks
        like it's following the curve, not just pointing straight at the target)
      - hovering an edge shows the full metric set, sample size, and any quality flags
    """

    locations = geo_config.intersections_by_id()
    ordered_ids = geo_config.ordered_intersection_ids()
    pairs_by_direction = {
        direction: {p.pair_id: p for p in summary.pairs} for direction, summary in summaries_by_direction.items()
    }

    fig = go.Figure()

    # Faint centerline for geographic context (the real corridor alignment).
    fig.add_trace(
        go.Scattermap(
            lat=[locations[i].lat for i in ordered_ids],
            lon=[locations[i].lon for i in ordered_ids],
            mode="lines",
            line=dict(width=1.5, color="rgba(70,70,70,0.35)"),
            hoverinfo="skip",
            showlegend=False,
        )
    )

    for pair_cfg in geo_config.pairwise_directions:
        for travel in pair_cfg.directions:
            loc_from = locations[travel.travel_from]
            loc_to = locations[travel.travel_to]

            pair = pairs_by_direction.get(travel.direction, {}).get(travel.pair_lookup_id)
            score = pair.headline_score if pair is not None else None
            color = _score_to_arrow_color(score)

            segment_length = _segment_length_degrees(loc_from.lat, loc_from.lon, loc_to.lat, loc_to.lon)
            arc_lats, arc_lons, geometry = _bezier_arc(
                loc_from.lat, loc_from.lon, loc_to.lat, loc_to.lon, bow_degrees=BOW_FRACTION * segment_length
            )
            tooltip = _direction_tooltip(travel, pair)

            # Repeating the same hovertext at every sampled point makes the *whole*
            # arc hoverable -- Plotly shows whichever point's hovertext is nearest
            # the cursor, and with N_CURVE_POINTS this is dense enough that "nearest
            # point" tracks the cursor smoothly along the curve, not just at one spot.
            fig.add_trace(
                go.Scattermap(
                    lat=arc_lats,
                    lon=arc_lons,
                    mode="lines",
                    line=dict(width=4, color=color),
                    hovertext=[tooltip] * len(arc_lats),
                    hoverinfo="text",
                    showlegend=False,
                )
            )

            # Arrowhead: tip near (but short of) the destination, oriented along the
            # arc's *local* tangent there (sampled a little further back along the
            # same curve) so it visually "follows" the bow rather than pointing
            # straight through it.
            tip_lat, tip_lon = _bezier_point(*geometry, t=ARROWHEAD_T)
            behind_lat, behind_lon = _bezier_point(*geometry, t=ARROWHEAD_T - ARROWHEAD_TANGENT_DELTA)
            tip_bearing = _bearing_degrees(behind_lat, behind_lon, tip_lat, tip_lon)
            head_lats, head_lons = _arrowhead_polygon(
                tip_lat,
                tip_lon,
                tip_bearing,
                length_degrees=ARROWHEAD_LENGTH_FRACTION * segment_length,
                width_degrees=ARROWHEAD_WIDTH_FRACTION * segment_length,
            )
            fig.add_trace(
                go.Scattermap(
                    lat=head_lats,
                    lon=head_lons,
                    mode="lines",
                    fill="toself",
                    fillcolor=color,
                    line=dict(width=0, color=color),
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

            # Visible "hover here for details" dot at the arc's midpoint -- a clear
            # affordance that this curve carries more information than its color
            # alone, and a generously-sized, easy-to-land-on hover target in its
            # own right (in addition to the now-hoverable arc above). go.Scattermap
            # markers have no `line` (border) property, so the dot is layered: a
            # white halo beneath a smaller colored disc -- otherwise a near-neutral
            # (yellowish) score color would blend straight into the basemap's roads.
            mid_lat, mid_lon = _bezier_point(*geometry, t=0.5)
            fig.add_trace(
                go.Scattermap(
                    lat=[mid_lat],
                    lon=[mid_lon],
                    mode="markers",
                    marker=dict(size=18, color="white"),
                    hoverinfo="skip",
                    showlegend=False,
                )
            )
            fig.add_trace(
                go.Scattermap(
                    lat=[mid_lat],
                    lon=[mid_lon],
                    mode="markers",
                    marker=dict(size=12, color=color),
                    hovertext=[tooltip],
                    hoverinfo="text",
                    showlegend=False,
                )
            )

    # Intersection nodes drawn last so they sit on top of the directional arcs.
    fig.add_trace(
        go.Scattermap(
            lat=[locations[i].lat for i in ordered_ids],
            lon=[locations[i].lon for i in ordered_ids],
            mode="markers+text",
            text=[locations[i].label for i in ordered_ids],
            textposition="top center",
            textfont=dict(size=15, color="#2c3e50"),
            marker=dict(size=16, color="#2c3e50"),
            hovertext=[
                f"Intersection {locations[i].label} (id {i})<br>lat = {locations[i].lat:.6f}, lon = {locations[i].lon:.6f}"
                for i in ordered_ids
            ],
            hoverinfo="text",
            showlegend=False,
        )
    )

    # Bounding-box midpoint -- with unevenly-spaced intersections the coordinate
    # average skews toward the denser end and clips the corridor's far edge.
    # Zoom is auto-computed from the corridor's lon/lat span so the map self-fits
    # regardless of how many intersections the config lists (see _fit_zoom).
    lats = [locations[i].lat for i in ordered_ids]
    lons = [locations[i].lon for i in ordered_ids]
    center_lat = (min(lats) + max(lats)) / 2.0
    center_lon = (min(lons) + max(lons)) / 2.0
    zoom = _fit_zoom(lats, lons)
    fig.update_layout(
        map=dict(style=MAP_STYLE, center=dict(lat=center_lat, lon=center_lon), zoom=zoom),
        title=dict(
            text=(
                f"{geo_config.corridor_name}<br>"
                "<sup>Each adjacent pair gets two curved arcs, one per direction of travel, "
                "bowing to opposite sides so they never overlap (this corridor: LR = phase 2 / "
                "eastbound, RL = phase 6 / westbound) · arc/arrowhead color = headline metric "
                "(Spearman ρ): dark red = strong inverse (−1), white = neutral (0), dark blue = "
                "strong positive sync (+1) · the arrowhead points toward the destination "
                "intersection, following the arc's own curve · hover any arc for full metrics</sup>"
            ),
            x=0.0,
        ),
        height=580,
        margin=dict(l=10, r=10, t=100, b=10),
    )
    return fig


# --- Combined road-link summary table -----------------------------------------------
#
# The map answers "where, and roughly how strong" -- this table is its tabular
# counterpart, answering "give me every number for every link, side by side, that I
# can scan/sort/copy." It merges what would otherwise be two separate per-direction
# render_summary_table outputs (one UISummary each) into a single map-aligned view:
# one row per (adjacent pair, direction of travel), walked in the same
# pairwise_directions order the map draws its arcs in, so a reader can match a row
# to an arc directly. Lives here (not presentation.py/viz.py) because, like
# render_corridor_map, it's keyed on the geo config's pair/direction/phase mapping --
# information the core pipeline's UISummary alone doesn't carry.


def build_corridor_link_table(
    geo_config: CorridorGeoConfig,
    summaries_by_direction: dict[Direction, UISummary],
) -> pd.DataFrame:
    """One row per (adjacent pair, direction of travel) -- every road link this
    corridor has, both directions, with that direction's full metric set.

    Parameters mirror render_corridor_map's exactly (same lookup: each direction's
    PairResult is found via `pairs_by_direction[travel.direction][travel.pair_lookup_id]`),
    so the two functions always describe the same data from two angles.
    """

    pairs_by_direction = {
        direction: {p.pair_id: p for p in summary.pairs} for direction, summary in summaries_by_direction.items()
    }

    rows = []
    for pair_cfg in geo_config.pairwise_directions:
        for travel in pair_cfg.directions:
            pair = pairs_by_direction.get(travel.direction, {}).get(travel.pair_lookup_id)
            rows.append(
                {
                    "Road link (direction of travel)": travel.travel_label,
                    "Phase group": travel.phase_group,
                    "MoE": pair.moe_name if pair is not None else "—",
                    "Headline (Spearman ρ)": pair.headline_score if pair is not None else None,
                    "Pearson r": pair.pearson_r if pair is not None else None,
                    "Cosine": pair.cosine_similarity if pair is not None else None,
                    "n samples": pair.n_samples if pair is not None else None,
                    "Interpretation": pair.interpretation_label if pair is not None else "not_computable",
                    "Quality flags": (", ".join(pair.quality_flags) if pair.quality_flags else "—")
                    if pair is not None
                    else "—",
                }
            )
    return pd.DataFrame(rows)


def _soften(color: str, *, alpha: float = 0.30) -> str:
    """'rgb(r,g,b)' / 'rgba(r,g,b,a)' -> 'rgba(r,g,b,alpha)' -- a translucent wash of
    the given color over a white cell background.

    A flat fill straight from ARROW_COLORSCALE would be near-black at -1 and
    near-navy at +1, swallowing the printed number in white-on-dark or low-contrast
    text. Washing it out to ~30% opacity keeps the same red<->blue *hue* (so the
    table column still reads as "an echo of the map") while leaving every cell light
    enough for the same dark cell-text color to stay readable throughout the range.
    """

    r, g, b = (s.strip() for s in color[color.index("(") + 1 : color.index(")")].split(",")[:3])
    return f"rgba({r},{g},{b},{alpha})"


def render_corridor_link_table(
    geo_config: CorridorGeoConfig,
    summaries_by_direction: dict[Direction, UISummary],
) -> go.Figure:
    """Render the combined road-link summary table -- the map's tabular counterpart.

    One row per (adjacent pair, direction of travel), in the same order the map
    draws its arcs, with the headline-score column washed in that same row's
    arc/arrowhead color (see _soften) -- so a reader can line up a table row with
    a map arc at a glance, then read off every metric for it: phase group, MoE,
    all three similarity metrics, sample size, interpretation label, and quality
    flags. This is the single source of "all the numbers" the two separate
    per-direction render_summary_table views would otherwise split across.
    """

    df = build_corridor_link_table(geo_config, summaries_by_direction)
    row_count = len(df)

    def fmt_metric(col):
        return [_fmt(v) for v in df[col]]

    neutral_fill = "#f4f6f7"
    headline_fill = [_soften(_score_to_arrow_color(s)) for s in df["Headline (Spearman ρ)"]]
    column_fills = [
        [neutral_fill] * row_count,  # Road link
        [neutral_fill] * row_count,  # Phase group
        [neutral_fill] * row_count,  # MoE
        headline_fill,  # Headline score -- echoes the map's red<->blue arc coloring
        [neutral_fill] * row_count,  # Pearson r
        [neutral_fill] * row_count,  # Cosine
        [neutral_fill] * row_count,  # n samples
        [neutral_fill] * row_count,  # Interpretation
        [neutral_fill] * row_count,  # Quality flags
    ]

    fig = go.Figure(
        go.Table(
            columnwidth=[2.1, 1.0, 0.7, 1.2, 0.9, 0.8, 0.8, 1.3, 1.3],
            header=dict(
                values=[
                    "Road link<br>(direction of travel)",
                    "Phase<br>group",
                    "MoE",
                    "Headline<br>(Spearman ρ)",
                    "Pearson r",
                    "Cosine",
                    "n<br>samples",
                    "Interpretation",
                    "Quality flags",
                ],
                fill_color="#2c3e50",
                font=dict(color="white", size=12),
                align="left",
                height=44,
            ),
            cells=dict(
                values=[
                    df["Road link (direction of travel)"],
                    df["Phase group"],
                    df["MoE"],
                    fmt_metric("Headline (Spearman ρ)"),
                    fmt_metric("Pearson r"),
                    fmt_metric("Cosine"),
                    ["n/a" if v is None else str(int(v)) for v in df["n samples"]],
                    df["Interpretation"],
                    df["Quality flags"],
                ],
                fill_color=column_fills,
                align="left",
                height=32,
                font=dict(size=12, color="#2c3e50"),
            ),
        )
    )
    fig.update_layout(
        title=dict(
            text=(
                f"{geo_config.corridor_name}<br>"
                "<sup>Every road link and direction of travel from the map above, side by "
                "side, with all synchrony metrics in one place · the headline-score column "
                "is washed in that same arc's red↔blue map color (darker hue = stronger "
                "magnitude; blue = positive sync, red = inverse) so a row and an arc are "
                "easy to match up</sup>"
            ),
            x=0.0,
        ),
        margin=dict(l=10, r=10, t=85, b=10),
        height=130 + 32 * row_count,
    )
    return fig


# --- Correlation-group overlay map --------------------------------------------------
#
# Where render_corridor_map draws one arc *per pair*, this draws one band *per group*:
# the output of grouping.run_grouping_analysis, painted back onto the real corridor so
# "which stretch of road moves as a unit, in this direction" is visible at a glance.
# Each contiguous group becomes a thick colored band spanning its member intersections;
# intersections in no group stay grey. Like the rest of geo_viz it only consumes the
# typed result objects -- here CorrelationGroups -- plus the geo config's coordinates.

GROUP_PALETTE = ["#6a3d9a", "#e6550d", "#1f78b4", "#c51b8a", "#b15928", "#252525"]
"""Qualitative, high-contrast band colors (deliberately *not* the RdBu sync scale --
group identity is categorical, not a signed magnitude) chosen to read against OSM's
green/cream basemap. Cycled if a direction ever has more groups than colors."""

UNGROUPED_COLOR = "#9aa0a6"
"""Grey for intersections that fell into no group -- present, but visibly "not joined"."""


def _group_tooltip(group: "CorrelationGroup") -> str:
    members = " – ".join(group.member_labels)
    return "<br>".join(
        [
            f"<b>Group {group.group_id}</b>  ({group.direction.value} · {group.phase_group})",
            f"Members: {members}  (n={group.size})",
            f"Spearman ρ within group: min {group.min_internal_rho:+.3f} · "
            f"mean {group.mean_internal_rho:+.3f} · max {group.max_internal_rho:+.3f}",
        ]
    )


def render_grouping_map(
    geo_config: CorridorGeoConfig,
    groups: Sequence["CorrelationGroup"],
    *,
    direction_label: str = "",
) -> go.Figure:
    """Render one direction's correlation groups as thick colored bands on the corridor.

    Parameters
    ----------
    geo_config
        Supplies intersection coordinates / labels / corridor order.
    groups
        The CorrelationGroups for a single direction (one GroupingResult.groups list).
        Each is drawn as a thick band through its members (contiguous by construction),
        in its own palette color, with member nodes filled to match; intersections in
        no group are drawn grey.
    direction_label
        Free-text note for the title, e.g. "LR / phase 2 (eastbound)".
    """

    locations = geo_config.intersections_by_id()
    ordered_ids = geo_config.ordered_intersection_ids()
    color_of: dict[str, str] = {}
    group_of: dict[str, str] = {m: g.group_id for g in groups for m in g.members}

    fig = go.Figure()

    # Faint full-corridor centerline for context.
    fig.add_trace(
        go.Scattermap(
            lat=[locations[i].lat for i in ordered_ids],
            lon=[locations[i].lon for i in ordered_ids],
            mode="lines",
            line=dict(width=1.5, color="rgba(70,70,70,0.35)"),
            hoverinfo="skip",
            showlegend=False,
        )
    )

    # One thick band per group, through its members in corridor order.
    for idx, group in enumerate(groups):
        color = GROUP_PALETTE[idx % len(GROUP_PALETTE)]
        members = sorted(group.members, key=lambda m: locations[m].corridor_order)
        for m in members:
            color_of[m] = color
        tooltip = _group_tooltip(group)
        fig.add_trace(
            go.Scattermap(
                lat=[locations[m].lat for m in members],
                lon=[locations[m].lon for m in members],
                mode="lines",
                line=dict(width=11, color=color),
                opacity=0.55,
                hovertext=[tooltip] * len(members),
                hoverinfo="text",
                name=group.group_id,
                showlegend=True,
            )
        )

    # Intersection nodes on top: white halo + (group color | grey) disc, labels above.
    fig.add_trace(
        go.Scattermap(
            lat=[locations[i].lat for i in ordered_ids],
            lon=[locations[i].lon for i in ordered_ids],
            mode="markers",
            marker=dict(size=22, color="white"),
            hoverinfo="skip",
            showlegend=False,
        )
    )
    fig.add_trace(
        go.Scattermap(
            lat=[locations[i].lat for i in ordered_ids],
            lon=[locations[i].lon for i in ordered_ids],
            mode="markers+text",
            marker=dict(size=15, color=[color_of.get(i, UNGROUPED_COLOR) for i in ordered_ids]),
            text=[locations[i].label for i in ordered_ids],
            textposition="top center",
            textfont=dict(size=15, color="#2c3e50"),
            hovertext=[
                f"Intersection {locations[i].label} (id {i})<br>"
                + (f"group: {group_of[i]}" if i in group_of else "ungrouped")
                for i in ordered_ids
            ],
            hoverinfo="text",
            showlegend=False,
        )
    )

    lats = [locations[i].lat for i in ordered_ids]
    lons = [locations[i].lon for i in ordered_ids]
    center_lat = (min(lats) + max(lats)) / 2.0
    center_lon = (min(lons) + max(lons)) / 2.0
    suffix = f" — {direction_label}" if direction_label else ""
    fig.update_layout(
        map=dict(style=MAP_STYLE, center=dict(lat=center_lat, lon=center_lon), zoom=_fit_zoom(lats, lons)),
        title=dict(
            text=(
                f"Correlation groups{suffix}<br>"
                "<sup>Each thick band is a contiguous group of intersections that are all "
                "mutually synced (Spearman ρ above threshold); grey nodes joined no group. "
                "Hover a band for the group's members and within-group ρ range</sup>"
            ),
            x=0.0,
        ),
        legend=dict(title="Groups", orientation="h", yanchor="bottom", y=0.0, xanchor="left", x=0.0),
        height=560,
        margin=dict(l=10, r=10, t=100, b=10),
    )
    return fig


# --- Directional arrow-banner group overlay -----------------------------------------
#
# A second take on the group map: instead of a thick line through each group, draw a
# filled, arrow-shaped "bounding banner" enclosing the group's intersections and
# pointing in that group's direction of travel -- a swallowtail ribbon (notched tail,
# pointed head, like a route/military map banner). It encodes *both* "which contiguous
# intersections are in the group" (the body spans them) and "which way they coordinate"
# (the arrowhead) in one shape, so LR and RL groups can share a single map: an eastbound
# group's banner points east, a westbound group's points west.

ARROW_BANNER_ALPHA = 0.22
"""Fill opacity for the banner interior -- translucent so the basemap, the corridor
centerline, and the enclosed intersection markers all stay readable through it."""


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """'#rrggbb' -> 'rgba(r,g,b,alpha)' (GROUP_PALETTE is hex; Scattermap fillcolor
    wants an rgba string to carry the alpha)."""

    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _arrow_banner_polygon(
    origin: tuple[float, float],
    dest: tuple[float, float],
    *,
    width_half: float,
    pad_back: float,
    pad_front: float,
    head_len: float,
    notch_depth: float,
) -> tuple[list[float], list[float]]:
    """A closed swallowtail-banner arrow polygon (lats, lons) enclosing the segment
    origin -> dest and pointing from origin toward dest.

    `origin`/`dest` are the (lat, lon) of the first/last enclosed intersection *in the
    direction of travel* (so for an RL group, origin is the easternmost member and the
    arrow points west). The body is a rectangle of half-width `width_half`, extended
    `pad_back` behind origin and `pad_front` past dest; the head tapers from the body's
    full width to a point `head_len` beyond that; the tail carries an inward notch of
    depth `notch_depth` (the ribbon's swallowtail). All distances are latitude-equivalent
    degrees, so the banner sizes consistently regardless of the corridor's bearing.
    """

    o_lat, o_lon = origin
    d_lat, d_lon = dest
    bearing = _bearing_degrees(o_lat, o_lon, d_lat, d_lon)
    left, right = bearing - 90.0, bearing + 90.0

    tail_c = _offset_point(o_lat, o_lon, bearing, -pad_back)           # body back edge, behind origin
    shoulder_c = _offset_point(d_lat, d_lon, bearing, pad_front)       # where the head taper begins, past dest
    tip = _offset_point(shoulder_c[0], shoulder_c[1], bearing, head_len)
    notch = _offset_point(tail_c[0], tail_c[1], bearing, notch_depth)  # swallowtail vertex, inside the body

    tail_top = _offset_point(tail_c[0], tail_c[1], left, width_half)
    tail_bot = _offset_point(tail_c[0], tail_c[1], right, width_half)
    sh_top = _offset_point(shoulder_c[0], shoulder_c[1], left, width_half)
    sh_bot = _offset_point(shoulder_c[0], shoulder_c[1], right, width_half)

    ring = [tail_top, sh_top, tip, sh_bot, tail_bot, notch, tail_top]
    return [p[0] for p in ring], [p[1] for p in ring]


def render_group_arrow_map(
    geo_config: CorridorGeoConfig,
    groups: Sequence["CorrelationGroup"],
    *,
    title_suffix: str = "",
) -> go.Figure:
    """Render correlation groups as directional arrow banners on one corridor map.

    Parameters
    ----------
    geo_config
        Supplies intersection coordinates / labels / corridor order.
    groups
        Any mix of CorrelationGroups (typically both directions concatenated, e.g.
        ``lr.groups + rl.groups``). Each is drawn as a filled swallowtail-arrow banner
        enclosing its member intersections and pointing in its own direction of travel,
        so LR (eastbound) and RL (westbound) groups coexist on a single map and are told
        apart by where their arrowheads point. Member nodes are filled in the banner's
        color; ungrouped nodes stay grey.
    title_suffix
        Optional free-text appended to the map title.

    Banner thickness, head length and tail notch are sized from the corridor's overall
    length (thickness) and each group's own span (head/pads), so every banner reads as
    the same ribbon regardless of how long its road segment is. Opposite-direction
    banners that happen to overlap the *same* stretch will overlap visually -- acceptable
    for this prototype's geography, where the LR and RL groups sit at opposite ends.
    """

    locations = geo_config.intersections_by_id()
    ordered_ids = geo_config.ordered_intersection_ids()
    lats_all = [locations[i].lat for i in ordered_ids]
    lons_all = [locations[i].lon for i in ordered_ids]

    corridor_len = _segment_length_degrees(lats_all[0], lons_all[0], lats_all[-1], lons_all[-1]) or 1e-3
    width_half = 0.025 * corridor_len

    # Pre-assign a palette color per group so the banners and the member node markers match.
    color_by_group = {id(g): GROUP_PALETTE[i % len(GROUP_PALETTE)] for i, g in enumerate(groups)}
    node_color = {m: color_by_group[id(g)] for g in groups for m in g.members}
    node_group = {m: g for g in groups for m in g.members}

    fig = go.Figure()

    # Faint full-corridor centerline for context.
    fig.add_trace(
        go.Scattermap(
            lat=lats_all,
            lon=lons_all,
            mode="lines",
            line=dict(width=1.5, color="rgba(70,70,70,0.35)"),
            hoverinfo="skip",
            showlegend=False,
        )
    )

    for group in groups:
        color = color_by_group[id(group)]
        members = sorted(group.members, key=lambda m: locations[m].corridor_order)
        first, last = locations[members[0]], locations[members[-1]]
        # Travel order: LR points toward higher corridor_order, RL toward lower.
        if group.direction == Direction.RL:
            origin, dest = (last.lat, last.lon), (first.lat, first.lon)
        else:
            origin, dest = (first.lat, first.lon), (last.lat, last.lon)

        span = _segment_length_degrees(origin[0], origin[1], dest[0], dest[1]) or width_half
        head_len = min(max(0.20 * span, 2.5 * width_half), 5.0 * width_half)
        ring_lats, ring_lons = _arrow_banner_polygon(
            origin,
            dest,
            width_half=width_half,
            pad_back=0.08 * span,
            pad_front=0.04 * span,
            head_len=head_len,
            notch_depth=min(0.08 * span, 1.5 * width_half),
        )
        tooltip = _group_tooltip(group)
        fig.add_trace(
            go.Scattermap(
                lat=ring_lats,
                lon=ring_lons,
                mode="lines",
                fill="toself",
                fillcolor=_hex_to_rgba(color, ARROW_BANNER_ALPHA),
                line=dict(width=3, color=color),
                hovertext=[tooltip] * len(ring_lats),
                hoverinfo="text",
                name=f"{group.group_id} ({group.direction.value})",
                showlegend=True,
            )
        )

    # Intersection nodes on top: white halo + (group color | grey) disc, labels above.
    fig.add_trace(
        go.Scattermap(
            lat=lats_all,
            lon=lons_all,
            mode="markers",
            marker=dict(size=20, color="white"),
            hoverinfo="skip",
            showlegend=False,
        )
    )
    fig.add_trace(
        go.Scattermap(
            lat=lats_all,
            lon=lons_all,
            mode="markers+text",
            marker=dict(size=13, color=[node_color.get(i, UNGROUPED_COLOR) for i in ordered_ids]),
            text=[locations[i].label for i in ordered_ids],
            textposition="top center",
            textfont=dict(size=15, color="#2c3e50"),
            hovertext=[
                f"Intersection {locations[i].label} (id {i})<br>"
                + (f"group: {node_group[i].group_id} ({node_group[i].direction.value})" if i in node_group else "ungrouped")
                for i in ordered_ids
            ],
            hoverinfo="text",
            showlegend=False,
        )
    )

    center_lat = (min(lats_all) + max(lats_all)) / 2.0
    center_lon = (min(lons_all) + max(lons_all)) / 2.0
    suffix = f" — {title_suffix}" if title_suffix else ""
    fig.update_layout(
        map=dict(style=MAP_STYLE, center=dict(lat=center_lat, lon=center_lon), zoom=_fit_zoom(lats_all, lons_all)),
        title=dict(
            text=(
                f"Correlation groups — directional arrow banners{suffix}<br>"
                "<sup>Each filled arrow encloses one contiguous group and points in its direction of "
                "travel (eastbound LR arrows point right, westbound RL arrows point left) · the body "
                "spans the member intersections, the head shows the direction they coordinate · grey "
                "nodes joined no group · hover a banner for members and within-group ρ</sup>"
            ),
            x=0.0,
        ),
        legend=dict(title="Groups", orientation="h", yanchor="bottom", y=0.0, xanchor="left", x=0.0),
        height=560,
        margin=dict(l=10, r=10, t=100, b=10),
    )
    return fig
