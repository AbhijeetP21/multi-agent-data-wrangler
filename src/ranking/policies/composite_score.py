"""Composite score ranking policy."""

from src.common.types.ranking import TransformationCandidate
from src.ranking.policies.base import BaseRankingPolicy


class CompositeScorePolicy(BaseRankingPolicy):
    """Ranking policy based on composite quality score improvement.

    This policy calculates the score based on the overall quality improvement
    across all metrics (completeness, consistency, validity, uniqueness).
    """

    def __init__(self, weights: dict[str, float] | None = None):
        """Initialize the composite score policy.

        Args:
            weights: Optional weights for each quality metric.
                    Defaults to equal weights.
        """
        self.weights = weights or {
            "completeness": 0.25,
            "consistency": 0.25,
            "validity": 0.25,
            "uniqueness": 0.25,
        }

    def score(self, candidate: TransformationCandidate) -> float:
        """Calculate a composite score based on quality improvements.

        Args:
            candidate: The transformation candidate to score.

        Returns:
            A composite score based on quality improvements.
        """
        delta = candidate.quality_delta

        # Calculate weighted improvement score
        improvement = (
            delta.improvement.completeness * self.weights.get("completeness", 0.25)
            + delta.improvement.consistency * self.weights.get("consistency", 0.25)
            + delta.improvement.validity * self.weights.get("validity", 0.25)
            + delta.improvement.uniqueness * self.weights.get("uniqueness", 0.25)
        )

        # Also consider the final quality score (not just improvement)
        final_quality = delta.after.overall

        # Combine improvement and final quality (70% improvement, 30% final quality)
        composite = 0.7 * improvement + 0.3 * final_quality

        return composite

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

        improvements = []
        if delta.improvement.completeness > 0:
            improvements.append(
                f"completeness {delta.before.completeness:.2%} → {delta.after.completeness:.2%}"
            )
        if delta.improvement.consistency > 0:
            improvements.append(
                f"consistency {delta.before.consistency:.2%} → {delta.after.consistency:.2%}"
            )
        if delta.improvement.validity > 0:
            improvements.append(
                f"validity {delta.before.validity:.2%} → {delta.after.validity:.2%}"
            )
        if delta.improvement.uniqueness > 0:
            improvements.append(
                f"uniqueness {delta.before.uniqueness:.2%} → {delta.after.uniqueness:.2%}"
            )

        improvement_str = (
            ", ".join(improvements) if improvements else "no measurable improvement"
        )

        return (
            f"Transformation '{transformation.type.value}' on columns "
            f"{transformation.target_columns} achieved composite score {score:.3f}. "
            f"Quality improvements: {improvement_str}. "
            f"Overall quality: {delta.before.overall:.2%} → {delta.after.overall:.2%}."
        )
