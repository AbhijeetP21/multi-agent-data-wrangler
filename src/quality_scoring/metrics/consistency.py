"""Consistency metric calculator."""

from typing import Any

import pandas as pd

from .base import BaseMetric


# Performance thresholds
LARGE_DATASET_SAMPLE = 50000


class ConsistencyMetric(BaseMetric):
    """Metric that calculates data type consistency across columns."""

    def calculate(self, data: pd.DataFrame, profile: Any = None) -> float:
        """
        Calculate consistency based on data type uniformity.

        Checks if values in each column are consistent with their inferred type.
        A column is consistent if most values match the expected dtype.

        Uses sampling for very large datasets to improve performance.

        Args:
            data: The DataFrame to calculate consistency for.
            profile: Optional DataProfile for additional context.

        Returns:
            A float between 0 and 1 representing the consistency score.
        """
        if data.empty:
            return 1.0

        if data.shape[1] == 0:
            return 1.0

        total_rows = len(data)
        
        # Use sampling for very large datasets
        if total_rows > LARGE_DATASET_SAMPLE:
            sample_size = min(10000, total_rows)
            data = data.sample(n=sample_size, random_state=42)
        
        consistency_scores = []

        for col in data.columns:
            col_consistency = self._calculate_column_consistency(data[col])
            consistency_scores.append(col_consistency)

        # Average consistency across all columns
        if not consistency_scores:
            return 1.0

        overall_consistency = sum(consistency_scores) / len(consistency_scores)
        return self._clamp_score(overall_consistency)

    def _calculate_column_consistency(self, series: pd.Series) -> float:
        """Calculate consistency for a single column."""
        non_null = series.dropna()

        if len(non_null) == 0:
            return 1.0

        # Check if values are consistent with their types
        # For numeric columns, check if all non-null values are numeric
        # For object columns, check type consistency

        dtype = series.dtype

        # Check for type consistency based on dtype
        if pd.api.types.is_numeric_dtype(dtype):
            return self._check_numeric_consistency(non_null)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return self._check_datetime_consistency(non_null)
        elif pd.api.types.is_object_dtype(dtype):
            return self._check_object_consistency(non_null)
        elif pd.api.types.is_bool_dtype(dtype):
            return self._check_boolean_consistency(non_null)
        else:
            # For other types, assume consistent
            return 1.0

    def _check_numeric_consistency(self, series: pd.Series) -> float:
        """Check if numeric column values are actually numeric."""
        try:
            # Try to convert to numeric - if most succeed, it's consistent
            converted = pd.to_numeric(series, errors="coerce")
            valid_count = converted.notna().sum()
            return valid_count / len(series)
        except Exception:
            return 0.0

    def _check_datetime_consistency(self, series: pd.Series) -> float:
        """Check if datetime column values are actually datetime."""
        try:
            converted = pd.to_datetime(series, errors="coerce")
            valid_count = converted.notna().sum()
            return valid_count / len(series)
        except Exception:
            return 0.0

    def _check_object_consistency(self, series: pd.Series) -> float:
        """Check consistency for object columns."""
        # For object columns, check if values are mostly of the same type
        type_counts = {}

        for val in series:
            if val is None or (isinstance(val, float) and pd.isna(val)):
                continue

            if isinstance(val, bool):
                type_counts["bool"] = type_counts.get("bool", 0) + 1
            elif isinstance(val, (int, float)) and not isinstance(val, bool):
                type_counts["numeric"] = type_counts.get("numeric", 0) + 1
            elif isinstance(val, str):
                type_counts["string"] = type_counts.get("string", 0) + 1
            else:
                type_counts["other"] = type_counts.get("other", 0) + 1

        if not type_counts:
            return 1.0

        # Consistency is the ratio of the most common type to total
        max_count = max(type_counts.values())
        return max_count / len(series)

    def _check_boolean_consistency(self, series: pd.Series) -> float:
        """Check if boolean column values are actually boolean."""
        try:
            converted = series.astype(bool)
            # Check how many values converted successfully
            valid_count = converted.notna().sum()
            return valid_count / len(series)
        except Exception:
            return 0.0
