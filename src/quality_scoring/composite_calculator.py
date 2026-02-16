"""Composite score calculator."""

from dataclasses import dataclass

from src.common.types import QualityMetrics


@dataclass
class Weights:
    """Weights for quality metrics."""

    completeness: float = 0.25
    consistency: float = 0.25
    validity: float = 0.25
    uniqueness: float = 0.25

    def __post_init__(self) -> None:
        """Validate weights sum to 1.0."""
        total = self.completeness + self.consistency + self.validity + self.uniqueness
        if not abs(total - 1.0) < 1e-6:
            raise ValueError(
                f"Weights must sum to 1.0, got {total}"
            )


class CompositeCalculator:
    """Calculator for weighted composite quality scores."""

    def __init__(self, weights: Weights | None = None):
        """
        Initialize the composite calculator.

        Args:
            weights: Optional weights for each metric. Defaults to equal weights.
        """
        self.weights = weights or Weights()

    def calculate(self, metrics: QualityMetrics) -> float:
        """
        Calculate the weighted composite score.

        Args:
            metrics: QualityMetrics with individual scores.

        Returns:
            A float between 0 and 1 representing the overall quality score.
        """
        composite = (
            self.weights.completeness * metrics.completeness
            + self.weights.consistency * metrics.consistency
            + self.weights.validity * metrics.validity
            + self.weights.uniqueness * metrics.uniqueness
        )
        return self._clamp_score(composite)

    def _clamp_score(self, score: float) -> float:
        """Clamp score to be between 0 and 1."""
        return max(0.0, min(1.0, score))

    def calculate_with_overall(self, metrics: QualityMetrics) -> QualityMetrics:
        """
        Calculate composite score and return QualityMetrics with overall filled in.

        Args:
            metrics: QualityMetrics without overall score.

        Returns:
            QualityMetrics with overall score calculated.
        """
        overall = self.calculate(metrics)
        return QualityMetrics(
            completeness=metrics.completeness,
            consistency=metrics.consistency,
            validity=metrics.validity,
            uniqueness=metrics.uniqueness,
            overall=overall,
        )
