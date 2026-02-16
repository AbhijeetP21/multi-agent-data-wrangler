"""Ranker for transformation candidates."""

from src.common.types.ranking import TransformationCandidate, RankedTransformation
from src.ranking.interfaces import RankingEngine, RankingPolicy
from src.ranking.scorer import Scorer


class RankingService(RankingEngine):
    """Service for ranking transformation candidates.

    This service implements the RankingEngine protocol and provides
    functionality to rank transformation candidates based on quality
    improvement using pluggable ranking policies.
    """

    def __init__(self, policy: RankingPolicy | None = None):
        """Initialize the ranking service.

        Args:
            policy: The ranking policy to use. Defaults to None,
                   which requires setting a policy before ranking.
        """
        self._scorer = Scorer(policy) if policy else None

    def rank(self, candidates: list[TransformationCandidate]) -> list[RankedTransformation]:
        """Rank transformation candidates.

        Args:
            candidates: List of transformation candidates to rank.

        Returns:
            List of ranked transformations, sorted by score (highest first).

        Raises:
            ValueError: If no policy has been set.
        """
        if self._scorer is None:
            raise ValueError("No ranking policy set. Use set_policy() first.")

        if not candidates:
            return []

        # Score all candidates
        scored_candidates = []
        for candidate in candidates:
            score, reasoning = self._scorer.score_with_reasoning(candidate)
            scored_candidates.append((candidate, score, reasoning))

        # Sort by score in descending order
        scored_candidates.sort(key=lambda x: x[1], reverse=True)

        # Create ranked transformations
        ranked = []
        for rank, (candidate, score, reasoning) in enumerate(scored_candidates, start=1):
            ranked.append(
                RankedTransformation(
                    rank=rank,
                    candidate=candidate,
                    composite_score=score,
                    reasoning=reasoning,
                )
            )

        return ranked

    def set_policy(self, policy: RankingPolicy) -> None:
        """Set the ranking policy.

        Args:
            policy: The ranking policy to use.
        """
        if self._scorer is None:
            self._scorer = Scorer(policy)
        else:
            self._scorer.set_policy(policy)

    @property
    def policy(self) -> RankingPolicy | None:
        """Get the current ranking policy."""
        return self._scorer.policy if self._scorer else None
