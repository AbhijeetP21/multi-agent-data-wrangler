"""Completeness metric calculator."""

from typing import Any

import pandas as pd

from .base import BaseMetric


class CompletenessMetric(BaseMetric):
    """Metric that calculates the ratio of non-null values in the dataset."""

    def calculate(self, data: pd.DataFrame, profile: Any = None) -> float:
        """
        Calculate completeness as the ratio of non-null values.

        Completeness = (total non-null values) / (total values)

        Args:
            data: The DataFrame to calculate completeness for.
            profile: Optional DataProfile for additional context.

        Returns:
            A float between 0 and 1 representing the completeness score.
        """
        if data.empty:
            return 1.0

        # Calculate total cells and non-null cells
        total_cells = data.size
        non_null_cells = data.count().sum()

        if total_cells == 0:
            return 1.0

        completeness = non_null_cells / total_cells
        return self._clamp_score(completeness)
