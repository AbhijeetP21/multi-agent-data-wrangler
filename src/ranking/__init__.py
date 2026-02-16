"""Ranking module for scoring and ranking transformation candidates."""

from src.ranking.interfaces import RankingEngine, RankingPolicy
from src.ranking.policies import (
    BaseRankingPolicy,
    CompositeScorePolicy,
    ImprovementPolicy,
)
from src.ranking.scorer import Scorer
from src.ranking.ranker import RankingService
from src.common.types.ranking import TransformationCandidate, RankedTransformation

__all__ = [
    # Interfaces
    "RankingEngine",
    "RankingPolicy",
    # Policies
    "BaseRankingPolicy",
    "CompositeScorePolicy",
    "ImprovementPolicy",
    # Core classes
    "Scorer",
    "RankingService",
    # Types
    "TransformationCandidate",
    "RankedTransformation",
]
