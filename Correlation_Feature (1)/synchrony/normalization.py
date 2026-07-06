"""Normalization service.

Implements the blueprint's "Normalization design" section. The default v1 strategy is
min-max scaling to [0, 1] *within each intersection*, computed on the exact selected
comparison set (not a broader historical window). This is deliberately swappable
(Concern 2 in the blueprint: "Similarity scores may change depending on whether
normalization uses the selected subset, a broader historical window, or a robust
scaling method") -- later strategies (historical-window min-max, z-score, robust/IQR
scaling) can be dropped in by implementing the same `Normalizer` protocol.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

import numpy as np

ZERO_VARIANCE_EPS = 1e-12
"""Below this spread, treat max == min (floating point safe zero-variance check)."""


@dataclass(frozen=True)
class NormalizationResult:
    values: np.ndarray
    is_zero_variance: bool
    """True when max == min for the input vector, i.e. normalization is ill-defined.

    Per the blueprint's "Edge case: zero variance": the caller should mark the pair
    with a quality flag and consider skipping Pearson/Spearman for that side.
    """


class Normalizer(Protocol):
    """Swappable normalization strategy. Each implementation normalizes ONE
    intersection's matched vector, independent of its pair partner, on the exact
    selected comparison set."""

    name: str

    def __call__(self, values: Sequence[float]) -> NormalizationResult: ...


class MinMaxNormalizer:
    """Default v1 strategy: min-max scaling to [0, 1] within the selected comparison set.

    z_i = (x_i - min(x)) / (max(x) - min(x))

    Documented caveat from the blueprint: "This choice is simple and easy to explain,
    but it may be sensitive to outliers." Swap this class out for
    ZScoreNormalizer / RobustIQRNormalizer (future extensions) without touching any
    other layer -- the similarity engine only depends on the `Normalizer` protocol.
    """

    name = "minmax_within_selection"

    def __call__(self, values: Sequence[float]) -> NormalizationResult:
        arr = np.asarray(values, dtype=float)
        if arr.size == 0:
            return NormalizationResult(values=arr, is_zero_variance=True)

        lo, hi = float(arr.min()), float(arr.max())
        spread = hi - lo

        if spread <= ZERO_VARIANCE_EPS:
            # max == min: normalization is ill-defined. Returning a constant vector
            # (rather than raising or producing NaNs) lets downstream cosine
            # similarity still be attempted if both sides are meaningful, while the
            # zero-variance flag tells the caller to skip Pearson/Spearman for this side.
            return NormalizationResult(values=np.zeros_like(arr), is_zero_variance=True)

        return NormalizationResult(values=(arr - lo) / spread, is_zero_variance=False)


DEFAULT_NORMALIZER: Normalizer = MinMaxNormalizer()
