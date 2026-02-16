"""Quality scoring module."""

from .interfaces import QualityScorer
from .scorer import QualityScorerService
from .composite_calculator import CompositeCalculator, Weights
from .comparator import Comparator
from .metrics import (
    BaseMetric,
    CompletenessMetric,
    ConsistencyMetric,
    ValidityMetric,
    UniquenessMetric,
)
from src.common.types import QualityMetrics, QualityDelta

__all__ = [
    # Interfaces
    "QualityScorer",
    # Main service
    "QualityScorerService",
    # Calculator
    "CompositeCalculator",
    "Weights",
    # Comparator
    "Comparator",
    # Metrics
    "BaseMetric",
    "CompletenessMetric",
    "ConsistencyMetric",
    "ValidityMetric",
    "UniquenessMetric",
    # Types
    "QualityMetrics",
    "QualityDelta",
]
