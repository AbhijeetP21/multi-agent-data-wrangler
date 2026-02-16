"""Scorer for transformation candidates."""

from src.common.types.ranking import TransformationCandidate
from src.ranking.interfaces import RankingPolicy


class Scorer:
    """Scores transformation candidates using a ranking policy."""

    def __init__(self, policy: RankingPolicy):
        """Initialize the scorer with a ranking policy.

        Args:
            policy: The ranking policy to use for scoring.
        """
        self._policy = policy

    @property
    def policy(self) -> RankingPolicy:
        """Get the current ranking policy."""
        return self._policy

    def set_policy(self, policy: RankingPolicy) -> None:
        """Set the ranking policy.

        Args:
            policy: The new ranking policy to use.
        """
        self._policy = policy

    def score(self, candidate: TransformationCandidate) -> float:
        """Score a single transformation candidate.

        Args:
            candidate: The candidate to score.

        Returns:
            The calculated score.
        """
        return self._policy.score(candidate)

    def score_multiple(self, candidates: list[TransformationCandidate]) -> list[float]:
        """Score multiple transformation candidates.

        Args:
            candidates: The candidates to score.

        Returns:
            List of scores, one for each candidate.
        """
        return [self.score(candidate) for candidate in candidates]

    def score_with_reasoning(
        self, candidate: TransformationCandidate
    ) -> tuple[float, str]:
        """Score a candidate and generate reasoning.

        Args:
            candidate: The candidate to score.

        Returns:
            A tuple of (score, reasoning).
        """
        score = self.score(candidate)
        reasoning = self._policy.get_reasoning(candidate, score)
        return score, reasoning
