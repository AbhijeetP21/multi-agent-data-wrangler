"""Completeness metric calculator."""

from typing import Any

import pandas as pd

from .base import BaseMetric


# Performance thresholds
LARGE_DATASET_SAMPLE = 50000  # Use sampling for datasets larger than this


class CompletenessMetric(BaseMetric):
    """Metric that calculates the ratio of non-null values in the dataset."""

    def calculate(self, data: pd.DataFrame, profile: Any = None) -> float:
        """
        Calculate completeness as the ratio of non-null values.

        Completeness = (total non-null values) / (total values)

        Uses sampling for very large datasets to improve performance.

        Args:
            data: The DataFrame to calculate completeness for.
            profile: Optional DataProfile for additional context.

        Returns:
            A float between 0 and 1 representing the completeness score.
        """
        if data.empty:
            return 1.0

        total_rows = len(data)
        
        # Use sampling for very large datasets
        if total_rows > LARGE_DATASET_SAMPLE:
            sample_size = min(10000, total_rows)
            sample = data.sample(n=sample_size, random_state=42)
            
            total_cells = sample.size
            non_null_cells = sample.count().sum()
        else:
            # Calculate total cells and non-null cells
            total_cells = data.size
            non_null_cells = data.count().sum()

        if total_cells == 0:
            return 1.0

        completeness = non_null_cells / total_cells
        return self._clamp_score(completeness)
