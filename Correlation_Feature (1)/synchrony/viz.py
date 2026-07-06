"""Plotly renderers for the three UI deliverables in the blueprint:
  1. Numeric summary table
  2. Corridor-edge visualization (color-coded edges between adjacent intersections)
  3. Ranked pair list

These consume the plain data structures from presentation.py. They are meant as a
*reference* implementation -- something the dashboard's UI developer can lift wholesale
or restyle to match the existing app's look and feel, per the user's intent that this
notebook hand them "a fully working realistic prototype to reuse."
"""

from __future__ import annotations

import plotly.graph_objects as go
from plotly.colors import sample_colorscale

from .presentation import build_corridor_edge_data, build_ranked_list, build_summary_table
from .schemas import UISummary

# Diverging colorscale for scores in [-1, +1], centered at 0. RdYlGn reads naturally
# for engineers: green = in sync, yellow = weak/neutral, red = inverse -- consistent
# with "favorable/unfavorable" framing already used elsewhere in the dashboard.
SCORE_COLORSCALE = "RdYlGn"


def _score_to_color(score: float | None) -> str:
    if score is None:
        return "rgba(120,120,120,0.6)"  # neutral grey for not-computable
    # Map [-1, 1] -> [0, 1] for the colorscale lookup.
    t = (max(-1.0, min(1.0, score)) + 1.0) / 2.0
    return sample_colorscale(SCORE_COLORSCALE, [t])[0]


# --- 1. Numeric summary table -------------------------------------------------------


def render_summary_table(summary: UISummary, *, friendly_names: dict[str, str] | None = None) -> go.Figure:
    df = build_summary_table(summary, friendly_names=friendly_names)

    def fmt_metric(col):
        return [f"{v:+.3f}" if isinstance(v, float) else "n/a" for v in df[col]]

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=list(df.columns),
                    fill_color="#2c3e50",
                    font=dict(color="white", size=12),
                    align="left",
                ),
                cells=dict(
                    values=[
                        df["Pair"],
                        df["Direction"],
                        df["MoE"],
                        df["n_samples"],
                        fmt_metric("Spearman (headline)"),
                        fmt_metric("Pearson"),
                        fmt_metric("Cosine"),
                        df["Quality flags"],
                        df["Interpretation"],
                    ],
                    align="left",
                    height=28,
                ),
            )
        ]
    )
    fig.update_layout(
        title=(
            f"Adjacent-pair synchrony summary — corridor {summary.analysis_context.corridor_id} "
            f"({summary.analysis_context.direction.value}), MoE = {summary.analysis_context.moe_name}, "
            f"{summary.analysis_context.weeks_included} weeks"
        ),
        margin=dict(l=10, r=10, t=60, b=10),
    )
    return fig


# --- 2. Corridor-edge visualization --------------------------------------------------


def render_corridor_edge_map(
    summary: UISummary,
    ordered_intersections: list[str],
    *,
    friendly_names: dict[str, str] | None = None,
) -> go.Figure:
    """Corridor as ordered nodes with colored edges between adjacent pairs.

    - Edge color encodes the headline metric (Spearman by default).
    - Edge hover shows all three metrics, sample size, and quality flags.
    - Unstable/flagged edges are rendered dashed with a warning marker, per the
      blueprint's "weak/unstable edges get muted styling or warning icon".
    """

    data = build_corridor_edge_data(summary, ordered_intersections, friendly_names=friendly_names)
    names = friendly_names or {}

    fig = go.Figure()

    # Edges: one colored line trace + one invisible-midpoint hover marker per edge,
    # because Plotly line traces don't support per-segment hover text directly.
    for i, edge in enumerate(data["edges"]):
        x0, x1 = i, i + 1
        color = _score_to_color(edge["headline_score"])
        dash = "dot" if edge["is_unstable"] else "solid"
        width = 4 if not edge["is_unstable"] else 3

        fig.add_trace(
            go.Scatter(
                x=[x0, x1],
                y=[0, 0],
                mode="lines",
                line=dict(color=color, width=width, dash=dash),
                hoverinfo="skip",
                showlegend=False,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[(x0 + x1) / 2],
                y=[0],
                mode="markers" + ("+text" if edge["is_unstable"] else ""),
                text=["⚠"] if edge["is_unstable"] else None,
                textposition="top center",
                marker=dict(size=26, color="rgba(0,0,0,0)"),
                hovertext=[edge["tooltip"]],
                hoverinfo="text",
                showlegend=False,
            )
        )

    # Nodes
    node_x = list(range(len(data["nodes"])))
    node_labels = [names.get(n["id"], n["id"]) for n in data["nodes"]]
    fig.add_trace(
        go.Scatter(
            x=node_x,
            y=[0] * len(node_x),
            mode="markers+text",
            text=node_labels,
            textposition="bottom center",
            marker=dict(size=34, color="#2c3e50", line=dict(color="white", width=2)),
            hovertext=[f"Intersection {lbl} (id {n['id']}, position {n['position']})" for lbl, n in zip(node_labels, data["nodes"])],
            hoverinfo="text",
            showlegend=False,
        )
    )

    direction_arrow = "→" if summary.analysis_context.direction.value == "LR" else "←"
    fig.update_layout(
        title=dict(
            text=(
                f"Corridor edge view — {summary.analysis_context.corridor_id} "
                f"({summary.analysis_context.direction.value} {direction_arrow})<br>"
                f"<sup>Edge color = headline metric (Spearman ρ), red↔green = -1↔+1 · "
                f"⚠ = pair carries a quality flag</sup>"
            ),
            x=0.0,
        ),
        xaxis=dict(visible=False, range=[-0.5, len(node_x) - 0.5]),
        yaxis=dict(visible=False, range=[-1, 1]),
        height=340,
        margin=dict(l=20, r=20, t=90, b=20),
        plot_bgcolor="white",
    )
    return fig


# --- 3. Ranked pair list -------------------------------------------------------------


def render_ranked_list(
    summary: UISummary,
    *,
    ranking: str = "strongest_sync",
    friendly_names: dict[str, str] | None = None,
) -> go.Figure:
    """Horizontal bar chart ranking adjacent pairs by the headline metric."""

    df = build_ranked_list(summary, ranking=ranking, friendly_names=friendly_names)
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title=f"Ranked pairs ({ranking}) — no computable pairs")
        return fig

    colors = [_score_to_color(s) for s in df["Headline score"]]
    text = [f"{s:+.3f}  ({lbl})" if s is not None else "n/a" for s, lbl in zip(df["Headline score"], df["Interpretation"])]

    fig = go.Figure(
        go.Bar(
            x=df["Headline score"],
            y=df["Pair"],
            orientation="h",
            marker=dict(color=colors, line=dict(color="#2c3e50", width=1)),
            text=text,
            textposition="outside",
            hovertext=[
                f"Pair {row['Pair']}<br>{row['Headline metric']} = {row['Headline score']:+.3f}<br>"
                f"n_samples = {row['n_samples']}<br>Flags: {row['Quality flags']}"
                for row in df.to_dict("records")
            ],
            hoverinfo="text",
        )
    )
    fig.update_layout(
        title=f"Ranked adjacent pairs — {ranking.replace('_', ' ')} (headline metric: Spearman ρ)",
        # Widened beyond [-1, 1] so "outside" text labels have room on whichever side
        # the bar points (positive bars label to the right, negative bars to the left)
        # without colliding with the y-axis pair labels.
        xaxis=dict(title="Headline score (Spearman ρ)", range=[-1.6, 1.6], zeroline=True),
        yaxis=dict(title=None, autorange="reversed"),
        height=120 + 70 * len(df),
        margin=dict(l=70, r=10, t=60, b=40),
    )
    return fig
