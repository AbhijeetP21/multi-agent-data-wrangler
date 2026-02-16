"""Base class for quality metrics."""

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from src.common.types import DataProfile


class BaseMetric(ABC):
    """Abstract base class for quality metrics."""

    @abstractmethod
    def calculate(self, data: pd.DataFrame, profile: Any = None) -> float:
        """
        Calculate the metric score for the given data.

        Args:
            data: The DataFrame to calculate the metric for.
            profile: Optional DataProfile for additional context.

        Returns:
            A float between 0 and 1 representing the metric score.
        """
        pass

    def _clamp_score(self, score: float) -> float:
        """Clamp score to be between 0 and 1."""
        return max(0.0, min(1.0, score))
