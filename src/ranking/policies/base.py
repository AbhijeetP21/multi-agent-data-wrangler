"""Base ranking policy."""

from abc import ABC, abstractmethod

from src.common.types.ranking import TransformationCandidate
from src.ranking.interfaces import RankingPolicy


class BaseRankingPolicy(ABC, RankingPolicy):
    """Abstract base class for ranking policies."""

    @abstractmethod
    def score(self, candidate: TransformationCandidate) -> float:
        """Calculate a score for a transformation candidate.

        Args:
            candidate: The transformation candidate to score.

        Returns:
            A score value for the candidate.
        """
        pass

    @abstractmethod
    def get_reasoning(self, candidate: TransformationCandidate, score: float) -> str:
        """Generate reasoning for the ranking.

        Args:
            candidate: The transformation candidate.
            score: The calculated score.

        Returns:
            A string explaining the ranking.
        """
        pass

    def _format_quality_improvement(self, candidate: TransformationCandidate) -> str:
        """Format quality improvement details."""
        delta = candidate.quality_delta
        improvements = []

        if delta.improvement.completeness > 0:
            improvements.append(
                f"completeness +{delta.improvement.completeness:.2%}"
            )
        if delta.improvement.consistency > 0:
            improvements.append(
                f"consistency +{delta.improvement.consistency:.2%}"
            )
        if delta.improvement.validity > 0:
            improvements.append(
                f"validity +{delta.improvement.validity:.2%}"
            )
        if delta.improvement.uniqueness > 0:
            improvements.append(
                f"uniqueness +{delta.improvement.uniqueness:.2%}"
            )

        return ", ".join(improvements) if improvements else "no improvement"
