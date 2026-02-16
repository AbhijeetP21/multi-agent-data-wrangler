"""Ranking module interfaces."""

from typing import Protocol

from src.common.types.ranking import TransformationCandidate, RankedTransformation


class RankingPolicy(Protocol):
    """Protocol defining a ranking policy."""

    def score(self, candidate: TransformationCandidate) -> float:
        """Calculate a score for a transformation candidate."""
        ...

    def get_reasoning(self, candidate: TransformationCandidate, score: float) -> str:
        """Generate reasoning for the ranking."""
        ...


class RankingEngine(Protocol):
    """Protocol defining the ranking engine interface."""

    def rank(self, candidates: list[TransformationCandidate]) -> list[RankedTransformation]:
        """Rank transformation candidates."""
        ...

    def set_policy(self, policy: RankingPolicy) -> None:
        """Set the ranking policy."""
        ...
