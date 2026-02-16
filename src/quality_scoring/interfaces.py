"""Quality scoring interfaces."""

from typing import Protocol

import pandas as pd

from src.common.types import QualityMetrics, QualityDelta


class QualityScorer(Protocol):
    """Protocol for quality scoring service."""

    def score(self, data: pd.DataFrame, profile: any = None) -> QualityMetrics:
        """
        Calculate quality metrics for the given data.

        Args:
            data: The DataFrame to score.
            profile: Optional DataProfile for additional context.

        Returns:
            QualityMetrics with scores for each dimension.
        """
        ...

    def compare(
        self, before: QualityMetrics, after: QualityMetrics
    ) -> QualityDelta:
        """
        Compare quality metrics before and after transformation.

        Args:
            before: Quality metrics before transformation.
            after: Quality metrics after transformation.

        Returns:
            QualityDelta showing the difference.
        """
        ...
