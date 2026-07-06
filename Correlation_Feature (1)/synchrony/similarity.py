"""Similarity engine: Pearson, Spearman, cosine wrappers with safe failure handling,
quality flags, and the plain-language interpretation layer.

Implements the blueprint's "Similarity engine", "Quality and confidence flags", and
"Interpretation layer" sections.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy import stats

# --- Quality flags (blueprint: "Suggested flags") --------------------------------

LOW_SAMPLE_SIZE = "LOW_SAMPLE_SIZE"
ZERO_VARIANCE_A = "ZERO_VARIANCE_A"
ZERO_VARIANCE_B = "ZERO_VARIANCE_B"
HIGH_MISSINGNESS_BEFORE_JOIN = "HIGH_MISSINGNESS_BEFORE_JOIN"
ALL_ZERO_VECTOR_A = "ALL_ZERO_VECTOR_A"
ALL_ZERO_VECTOR_B = "ALL_ZERO_VECTOR_B"
METRIC_NOT_COMPUTABLE = "METRIC_NOT_COMPUTABLE"

HIGH_MISSINGNESS_THRESHOLD = 0.30
"""If more than 30% of either side's pre-join rows fail to find a match, flag it.
Product choice, not a statistical constant -- documented here so it's easy to retune."""


@dataclass(frozen=True)
class QualityAssessment:
    flags: list[str]
    is_zero_variance_a: bool
    is_zero_variance_b: bool


def compute_quality_flags(
    *,
    raw_vector_a: np.ndarray,
    raw_vector_b: np.ndarray,
    norm_zero_variance_a: bool,
    norm_zero_variance_b: bool,
    n_samples: int,
    n_unmatched_a: int,
    n_unmatched_b: int,
    min_sample_size: int,
) -> QualityAssessment:
    """Derive the machine-readable flag list for one pair.

    This mirrors the blueprint's "Suggested flags" list. Flags are computed
    independently and additively -- a pair can carry several at once (e.g. both
    LOW_SAMPLE_SIZE and ZERO_VARIANCE_A).
    """

    flags: list[str] = []

    if n_samples < min_sample_size:
        flags.append(LOW_SAMPLE_SIZE)

    if norm_zero_variance_a:
        flags.append(ZERO_VARIANCE_A)
    if norm_zero_variance_b:
        flags.append(ZERO_VARIANCE_B)

    pre_join_a = n_samples + n_unmatched_a
    pre_join_b = n_samples + n_unmatched_b
    missing_a = (n_unmatched_a / pre_join_a) if pre_join_a else 0.0
    missing_b = (n_unmatched_b / pre_join_b) if pre_join_b else 0.0
    if max(missing_a, missing_b) > HIGH_MISSINGNESS_THRESHOLD:
        flags.append(HIGH_MISSINGNESS_BEFORE_JOIN)

    if raw_vector_a.size and np.allclose(raw_vector_a, 0.0):
        flags.append(ALL_ZERO_VECTOR_A)
    if raw_vector_b.size and np.allclose(raw_vector_b, 0.0):
        flags.append(ALL_ZERO_VECTOR_B)

    if n_samples < 2 or norm_zero_variance_a or norm_zero_variance_b:
        flags.append(METRIC_NOT_COMPUTABLE)

    return QualityAssessment(
        flags=flags,
        is_zero_variance_a=norm_zero_variance_a,
        is_zero_variance_b=norm_zero_variance_b,
    )


# --- Safe metric wrappers ---------------------------------------------------------
#
# "Safe" here means: never raise on degenerate input (too few points, zero variance,
# zero-norm vectors). Instead return None so the pair result can carry
# METRIC_NOT_COMPUTABLE rather than crashing the whole batch -- per acceptance
# criterion "Zero-variance cases do not crash the pipeline."


def safe_pearson(x: np.ndarray, y: np.ndarray) -> float | None:
    if x.size < 2 or np.allclose(x, x[0]) or np.allclose(y, y[0]):
        return None
    r, _ = stats.pearsonr(x, y)
    return float(r) if np.isfinite(r) else None


def safe_spearman(x: np.ndarray, y: np.ndarray) -> float | None:
    if x.size < 2 or np.allclose(x, x[0]) or np.allclose(y, y[0]):
        return None
    rho, _ = stats.spearmanr(x, y)
    return float(rho) if np.isfinite(rho) else None


def safe_cosine(x: np.ndarray, y: np.ndarray) -> float | None:
    if x.size == 0:
        return None
    norm_x, norm_y = np.linalg.norm(x), np.linalg.norm(y)
    if norm_x <= 1e-12 or norm_y <= 1e-12:
        return None
    cos = float(np.dot(x, y) / (norm_x * norm_y))
    # Guard against floating-point drift pushing |cos| fractionally past 1.
    return max(-1.0, min(1.0, cos))


# --- Interpretation layer (blueprint: "Suggested labels") -------------------------

STRONG_POSITIVE_SYNC = "strong_positive_sync"
MODERATE_POSITIVE_SYNC = "moderate_positive_sync"
WEAK_OR_NO_SYNC = "weak_or_no_sync"
INVERSE_SYNC = "inverse_sync"
NOT_COMPUTABLE = "not_computable"

# Thresholds are explicitly product choices (per blueprint: "Threshold values for
# these labels should be configurable and clearly documented. They are product
# choices, not universal scientific constants.") -- centralized here, in one place,
# so the UI/agent can retune them after seeing real engineer feedback.
INTERPRETATION_THRESHOLDS = {
    "strong": 0.7,
    "moderate": 0.4,
    "inverse": -0.3,
}
"""score >= strong -> strong_positive_sync
moderate <= score < strong -> moderate_positive_sync
inverse < score < moderate -> weak_or_no_sync
score <= inverse -> inverse_sync"""


def map_score_to_label(score: float | None) -> str:
    if score is None:
        return NOT_COMPUTABLE
    if score >= INTERPRETATION_THRESHOLDS["strong"]:
        return STRONG_POSITIVE_SYNC
    if score >= INTERPRETATION_THRESHOLDS["moderate"]:
        return MODERATE_POSITIVE_SYNC
    if score > INTERPRETATION_THRESHOLDS["inverse"]:
        return WEAK_OR_NO_SYNC
    return INVERSE_SYNC


METRIC_EXPLANATIONS = {
    "pearson_r": (
        "Pearson correlation",
        "Do the two intersections rise and fall together in (normalized) value, in a roughly linear way?",
    ),
    "spearman_rho": (
        "Spearman correlation",
        "When one intersection is relatively bad, is the other also relatively bad -- regardless of exact linearity? (default headline metric)",
    ),
    "cosine_similarity": (
        "Cosine similarity",
        "Do the two intersections' selected patterns point in the same overall direction/shape, even if one runs consistently higher?",
    ),
}
"""Plain-language explanations for UI tooltips, per blueprint "Interpretability" criteria."""
