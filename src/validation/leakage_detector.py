"""Information leakage detector module."""

import pandas as pd
import numpy as np
from typing import Optional

from src.common.types import DataProfile, ValidationIssue


class LeakageDetector:
    """Detects information leakage from original to transformed data."""

    def __init__(self, threshold: float = 0.1) -> None:
        """
        Initialize the leakage detector.

        Args:
            threshold: Minimum ratio of matching values to flag as potential leakage
        """
        self.threshold = threshold

    def check_exact_row_leakage(
        self, original: pd.DataFrame, transformed: pd.DataFrame
    ) -> bool:
        """
        Check if transformed data contains exact copies of original rows
        that should have been transformed.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame

        Returns:
            True if exact row leakage detected
        """
        if original.empty or transformed.empty:
            return False

        # Find common rows between original and transformed
        try:
            original_tuples = set(original.apply(tuple, axis=1))
            transformed_tuples = set(transformed.apply(tuple, axis=1))

            # If there's significant overlap, it might be leakage
            if original_tuples and transformed_tuples:
                overlap = len(original_tuples.intersection(transformed_tuples))
                overlap_ratio = overlap / len(transformed_tuples)

                # High overlap could indicate leakage (e.g., encoding that didn't change values)
                if overlap_ratio > 0.5 and len(original) == len(transformed):
                    return True
        except (TypeError, ValueError):
            # Handle unhashable types gracefully
            pass

        return False

    def check_categorical_encoding_leakage(
        self,
        original: pd.DataFrame,
        transformed: pd.DataFrame,
        profile: DataProfile,
    ) -> list[ValidationIssue]:
        """
        Check if categorical encoding introduces reverse-mappable leakage.

        This detects when encoded values can be easily mapped back to original.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame
            profile: Data profile of original

        Returns:
            List of ValidationIssues for potential leakage
        """
        issues: list[ValidationIssue] = []

        # Check columns that were likely encoded
        for col_name, col_profile in profile.columns.items():
            if col_name not in original.columns or col_name not in transformed.columns:
                continue

            # Check for categorical columns with low cardinality
            if col_profile.inferred_type == "categorical" and col_profile.unique_count is not None:
                original_unique = col_profile.unique_count
                transformed_unique = transformed[col_name].nunique()

                # If unique count changed significantly, check for leakage
                if original_unique == transformed_unique:
                    # Same number of unique values - could be direct mapping
                    try:
                        original_values = set(original[col_name].dropna().unique())
                        transformed_values = set(transformed[col_name].dropna().unique())

                        # Check if sets are identical (direct mapping)
                        if original_values == transformed_values:
                            issues.append(
                                ValidationIssue(
                                    severity="warning",
                                    code="POTENTIAL_LEAKAGE",
                                    message=f"Column '{col_name}' appears to have direct "
                                    f"value mapping without transformation",
                                    column=col_name,
                                )
                            )
                    except (TypeError, ValueError):
                        pass

        return issues

    def check_numeric_distribution_leakage(
        self,
        original: pd.DataFrame,
        transformed: pd.DataFrame,
        profile: DataProfile,
    ) -> list[ValidationIssue]:
        """
        Check if numeric transformations preserve too much original information.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame
            profile: Data profile of original

        Returns:
            List of ValidationIssues for potential leakage
        """
        issues: list[ValidationIssue] = []

        for col_name, col_profile in profile.columns.items():
            if col_name not in original.columns or col_name not in transformed.columns:
                continue

            # Check for numeric columns
            if col_profile.inferred_type == "numeric":
                try:
                    orig_vals = original[col_name].dropna().values
                    trans_vals = transformed[col_name].dropna().values

                    if len(orig_vals) > 0 and len(trans_vals) > 0:
                        # Calculate correlation - if very high, might be leakage
                        if len(orig_vals) == len(trans_vals):
                            correlation = np.corrcoef(orig_vals, trans_vals)[0, 1]

                            # Very high correlation (>0.99) might indicate no real transformation
                            if not np.isnan(correlation) and correlation > 0.99:
                                issues.append(
                                    ValidationIssue(
                                        severity="info",
                                        code="HIGH_CORRELATION",
                                        message=f"Column '{col_name}' has very high correlation "
                                        f"({correlation:.4f}) with original - may need transformation",
                                        column=col_name,
                                    )
                                )
                except (TypeError, ValueError, RuntimeWarning):
                    pass

        return issues

    def check_leakage(
        self,
        original: pd.DataFrame,
        transformed: pd.DataFrame,
        profile: Optional[DataProfile] = None,
    ) -> tuple[bool, list[ValidationIssue]]:
        """
        Check for all types of information leakage.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame
            profile: Data profile of original (optional)

        Returns:
            Tuple of (leakage_detected, list of issues)
        """
        issues: list[ValidationIssue] = []
        leakage_detected = False

        # Check exact row leakage
        if self.check_exact_row_leakage(original, transformed):
            leakage_detected = True
            issues.append(
                ValidationIssue(
                    severity="error",
                    code="EXACT_ROW_LEAKAGE",
                    message="Transformed data contains exact copies of original rows",
                )
            )

        # Check categorical encoding leakage
        if profile is not None:
            issues.extend(self.check_categorical_encoding_leakage(original, transformed, profile))
            issues.extend(self.check_numeric_distribution_leakage(original, transformed, profile))

        return leakage_detected, issues
