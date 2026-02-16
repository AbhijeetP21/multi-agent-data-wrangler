"""Improvement ranking policy."""

from src.common.types.ranking import TransformationCandidate
from src.ranking.policies.base import BaseRankingPolicy


class ImprovementPolicy(BaseRankingPolicy):
    """Ranking policy based on direct quality improvement.

    This policy prioritizes transformations that provide the biggest
    absolute improvement in quality metrics, regardless of final quality.
    """

    def __init__(self, primary_metric: str = "overall"):
        """Initialize the improvement policy.

        Args:
            primary_metric: The primary metric to use for ranking.
                          Options: 'overall', 'completeness', 'consistency',
                          'validity', 'uniqueness'. Defaults to 'overall'.
        """
        self.primary_metric = primary_metric

    def score(self, candidate: TransformationCandidate) -> float:
        """Calculate a score based on quality improvement.

        Args:
            candidate: The transformation candidate to score.

        Returns:
            A score based on quality improvement.
        """
        delta = candidate.quality_delta

        # Use the primary metric for scoring
        if self.primary_metric == "overall":
            return delta.composite_delta
        elif self.primary_metric == "completeness":
            return delta.improvement.completeness
        elif self.primary_metric == "consistency":
            return delta.improvement.consistency
        elif self.primary_metric == "validity":
            return delta.improvement.validity
        elif self.primary_metric == "uniqueness":
            return delta.improvement.uniqueness
        else:
            # Default to composite delta
            return delta.composite_delta

    def get_reasoning(self, candidate: TransformationCandidate, score: float) -> str:
        """Generate reasoning for the ranking.

        Args:
            candidate: The transformation candidate.
            score: The calculated score.

        Returns:
            A string explaining the ranking.
        """
        delta = candidate.quality_delta
        transformation = candidate.transformation

        # Build improvement details
        details = []
        if delta.improvement.completeness != 0:
            details.append(
                f"completeness: {delta.improvement.completeness:+.2%}"
            )
        if delta.improvement.consistency != 0:
            details.append(
                f"consistency: {delta.improvement.consistency:+.2%}"
            )
        if delta.improvement.validity != 0:
            details.append(
                f"validity: {delta.improvement.validity:+.2%}"
            )
        if delta.improvement.uniqueness != 0:
            details.append(
                f"uniqueness: {delta.improvement.uniqueness:+.2%}"
            )

        details_str = ", ".join(details) if details else "no change"

        return (
            f"Transformation '{transformation.type.value}' on columns "
            f"{transformation.target_columns} provides {self.primary_metric} "
            f"improvement of {score:+.3f}. "
            f"Metric changes: {details_str}. "
            f"Composite delta: {delta.composite_delta:+.3f}."
        )
