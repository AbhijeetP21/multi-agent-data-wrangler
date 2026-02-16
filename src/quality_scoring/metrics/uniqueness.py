"""Uniqueness metric calculator."""

from typing import Any

import pandas as pd

from .base import BaseMetric


class UniquenessMetric(BaseMetric):
    """Metric that calculates the ratio of unique values."""

    def calculate(self, data: pd.DataFrame, profile: Any = None) -> float:
        """
        Calculate uniqueness as the ratio of unique values.

        Uniqueness = (unique values) / (total values)

        Args:
            data: The DataFrame to calculate uniqueness for.
            profile: Optional DataProfile for additional context.

        Returns:
            A float between 0 and 1 representing the uniqueness score.
        """
        if data.empty:
            return 1.0

        if data.shape[1] == 0:
            return 1.0

        # Calculate uniqueness for each column
        uniqueness_scores = []

        for col in data.columns:
            col_uniqueness = self._calculate_column_uniqueness(
                data[col], profile.columns.get(col) if profile else None
            )
            uniqueness_scores.append(col_uniqueness)

        # Average uniqueness across all columns
        if not uniqueness_scores:
            return 1.0

        overall_uniqueness = sum(uniqueness_scores) / len(uniqueness_scores)
        return self._clamp_score(overall_uniqueness)

    def _calculate_column_uniqueness(
        self, series: pd.Series, column_profile: Any = None
    ) -> float:
        """Calculate uniqueness for a single column."""
        non_null = series.dropna()

        if len(non_null) == 0:
            return 1.0

        total_count = len(non_null)

        # Use profile information if available
        if column_profile is not None and column_profile.unique_count is not None:
            unique_count = column_profile.unique_count
        else:
            # Count unique values directly
            unique_count = non_null.nunique()

        uniqueness = unique_count / total_count if total_count > 0 else 1.0
        return self._clamp_score(uniqueness)
