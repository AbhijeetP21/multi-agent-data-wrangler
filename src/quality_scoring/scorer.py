"""Main quality scorer service."""

import pandas as pd

from src.common.types import DataProfile, QualityMetrics, QualityDelta

from .composite_calculator import CompositeCalculator
from .comparator import Comparator
from .metrics import (
    CompletenessMetric,
    ConsistencyMetric,
    ValidityMetric,
    UniquenessMetric,
)


class QualityScorerService:
    """Service for calculating quality metrics and comparisons."""

    def __init__(
        self,
        composite_calculator: CompositeCalculator | None = None,
        comparator: Comparator | None = None,
    ):
        """
        Initialize the quality scorer service.

        Args:
            composite_calculator: Optional composite calculator.
            comparator: Optional comparator.
        """
        self._composite_calculator = composite_calculator or CompositeCalculator()
        self._comparator = comparator or Comparator()

        # Initialize metric calculators
        self._completeness = CompletenessMetric()
        self._consistency = ConsistencyMetric()
        self._validity = ValidityMetric()
        self._uniqueness = UniquenessMetric()

    def score(self, data: pd.DataFrame, profile: DataProfile | None = None) -> QualityMetrics:
        """
        Calculate quality metrics for the given data.

        Args:
            data: The DataFrame to score.
            profile: Optional DataProfile for additional context.

        Returns:
            QualityMetrics with scores for each dimension.
        """
        # Calculate individual metrics
        completeness = self._completeness.calculate(data, profile)
        consistency = self._consistency.calculate(data, profile)
        validity = self._validity.calculate(data, profile)
        uniqueness = self._uniqueness.calculate(data, profile)

        # Create metrics without overall
        metrics = QualityMetrics(
            completeness=completeness,
            consistency=consistency,
            validity=validity,
            uniqueness=uniqueness,
            overall=0.0,  # Will be calculated next
        )

        # Calculate overall composite score
        metrics_with_overall = self._composite_calculator.calculate_with_overall(metrics)

        return metrics_with_overall

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
        return self._comparator.compare(before, after)
