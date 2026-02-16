"""Validity metric calculator."""

from typing import Any

import pandas as pd

from .base import BaseMetric


# Performance thresholds
LARGE_DATASET_SAMPLE = 50000


class ValidityMetric(BaseMetric):
    """Metric that calculates value range validity."""

    def calculate(self, data: pd.DataFrame, profile: Any = None) -> float:
        """
        Calculate validity based on value range validity.

        Checks if values fall within expected ranges for their column types.
        Uses profile information if available, otherwise uses general heuristics.

        Uses sampling for very large datasets to improve performance.

        Args:
            data: The DataFrame to calculate validity for.
            profile: Optional DataProfile with column range information.

        Returns:
            A float between 0 and 1 representing the validity score.
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
        
        validity_scores = []

        for col in data.columns:
            col_validity = self._calculate_column_validity(
                data[col], profile.columns.get(col) if profile else None
            )
            validity_scores.append(col_validity)

        # Average validity across all columns
        if not validity_scores:
            return 1.0

        overall_validity = sum(validity_scores) / len(validity_scores)
        return self._clamp_score(overall_validity)

    def _calculate_column_validity(
        self, series: pd.Series, column_profile: Any = None
    ) -> float:
        """Calculate validity for a single column."""
        non_null = series.dropna()

        if len(non_null) == 0:
            return 1.0

        # Use profile information if available
        if column_profile is not None:
            return self._validate_against_profile(non_null, column_profile)

        # Otherwise use general validation
        return self._validate_general(non_null)

    def _validate_against_profile(self, series: pd.Series, profile: Any) -> float:
        """Validate values against column profile ranges."""
        dtype = series.dtype
        valid_count = 0
        total_count = len(series)

        if pd.api.types.is_numeric_dtype(dtype):
            # Check against min/max from profile
            min_val = profile.min_value
            max_val = profile.max_value

            try:
                # Try to convert series to numeric first
                numeric_series = pd.to_numeric(series, errors='coerce')
                
                if min_val is not None and max_val is not None:
                    valid_count = ((numeric_series >= min_val) & (numeric_series <= max_val)).sum()
                elif min_val is not None:
                    valid_count = (numeric_series >= min_val).sum()
                elif max_val is not None:
                    valid_count = (numeric_series <= max_val).sum()
                else:
                    # No constraints, assume valid
                    valid_count = total_count
            except (TypeError, ValueError):
                # If conversion fails, assume valid
                valid_count = total_count

        elif pd.api.types.is_datetime64_any_dtype(dtype):
            # For datetime, check if values are within reasonable range
            # Assume valid if not NaT
            valid_count = series.notna().sum()

        else:
            # For other types, assume valid
            valid_count = total_count

        return valid_count / total_count if total_count > 0 else 1.0

    def _validate_general(self, series: pd.Series) -> float:
        """General validation without profile information."""
        dtype = series.dtype

        if pd.api.types.is_numeric_dtype(dtype):
            return self._validate_numeric(series)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return self._validate_datetime(series)
        elif pd.api.types.is_object_dtype(dtype):
            return self._validate_object(series)
        else:
            return 1.0

    def _validate_numeric(self, series: pd.Series) -> float:
        """Validate numeric values for common issues."""
        # Check for infinite values
        infinite_count = series.isin([float("inf"), float("-inf")]).sum()

        # Check for NaN that might have slipped through
        nan_count = series.isna().sum()

        valid_count = len(series) - infinite_count - nan_count
        return valid_count / len(series) if len(series) > 0 else 1.0

    def _validate_datetime(self, series: pd.Series) -> float:
        """Validate datetime values."""
        # Check for NaT (Not a Time) values
        nat_count = series.isna().sum()
        valid_count = len(series) - nat_count
        return valid_count / len(series) if len(series) > 0 else 1.0

    def _validate_object(self, series: pd.Series) -> float:
        """Validate object/string values."""
        # Check for empty strings
        empty_count = (series == "").sum()

        # Check for None/NaN
        null_count = series.isna().sum()

        valid_count = len(series) - empty_count - null_count
        return valid_count / len(series) if len(series) > 0 else 1.0
