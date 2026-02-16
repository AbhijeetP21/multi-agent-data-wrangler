"""Data integrity validator module."""

import pandas as pd
from typing import Optional

from src.common.types import DataProfile, ValidationIssue


class IntegrityValidator:
    """Validates data integrity after transformations."""

    DEFAULT_ROW_COUNT_TOLERANCE: float = 0.1  # 10% tolerance

    def __init__(self, row_count_tolerance: float = DEFAULT_ROW_COUNT_TOLERANCE) -> None:
        """
        Initialize the integrity validator.

        Args:
            row_count_tolerance: Maximum allowed ratio of rows lost (default 10%)
        """
        self.row_count_tolerance = row_count_tolerance

    def validate_row_count(
        self, original: pd.DataFrame, transformed: pd.DataFrame
    ) -> Optional[ValidationIssue]:
        """
        Validate that row count is within acceptable tolerance.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame

        Returns:
            ValidationIssue if row count exceeds tolerance, None otherwise
        """
        original_count = len(original)
        transformed_count = len(transformed)

        if original_count == 0:
            return None

        loss_ratio = (original_count - transformed_count) / original_count

        if loss_ratio > self.row_count_tolerance:
            return ValidationIssue(
                severity="error",
                code="EXCESSIVE_ROW_LOSS",
                message=f"Row count decreased by {loss_ratio * 100:.1f}%, "
                f"exceeding tolerance of {self.row_count_tolerance * 100:.1f}%",
            )

        if loss_ratio > 0:
            return ValidationIssue(
                severity="warning",
                code="ROW_LOSS",
                message=f"Row count decreased by {loss_ratio * 100:.1f}%",
            )

        return None

    def validate_null_preservation(
        self, original: pd.DataFrame, transformed: pd.DataFrame, profile: DataProfile
    ) -> list[ValidationIssue]:
        """
        Validate that null values are preserved appropriately.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame
            profile: Data profile of original

        Returns:
            List of ValidationIssues for null preservation problems
        """
        issues: list[ValidationIssue] = []

        for col_name, col_profile in profile.columns.items():
            if col_name not in transformed.columns:
                issues.append(
                    ValidationIssue(
                        severity="error",
                        code="COLUMN_REMOVED",
                        message=f"Column '{col_name}' was removed",
                        column=col_name,
                    )
                )
                continue

            original_null_count = col_profile.null_count
            transformed_null_count = transformed[col_name].isna().sum()

            # Allow some nulls to be filled (not an error)
            # But check if nulls were added unexpectedly
            if transformed_null_count > original_null_count:
                null_increase = transformed_null_count - original_null_count
                issues.append(
                    ValidationIssue(
                        severity="error",
                        code="NULLS_INCREASED",
                        message=f"Null count increased by {null_increase} in column '{col_name}'",
                        column=col_name,
                    )
                )

        return issues

    def validate_type_preservation(
        self, original: pd.DataFrame, transformed: pd.DataFrame, profile: DataProfile
    ) -> list[ValidationIssue]:
        """
        Validate that data types are preserved or intentionally changed.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame
            profile: Data profile of original

        Returns:
            List of ValidationIssues for type preservation problems
        """
        issues: list[ValidationIssue] = []

        for col_name, col_profile in profile.columns.items():
            if col_name not in transformed.columns:
                continue

            original_dtype = col_profile.dtype
            transformed_dtype = str(transformed[col_name].dtype)

            # Check if dtype changed significantly
            if original_dtype != transformed_dtype:
                issues.append(
                    ValidationIssue(
                        severity="warning",
                        code="TYPE_CHANGED",
                        message=f"Column '{col_name}' dtype changed from "
                        f"'{original_dtype}' to '{transformed_dtype}'",
                        column=col_name,
                    )
                )

        return issues

    def validate_all(
        self, original: pd.DataFrame, transformed: pd.DataFrame, profile: DataProfile
    ) -> list[ValidationIssue]:
        """
        Run all integrity validations.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame
            profile: Data profile of original

        Returns:
            List of all ValidationIssues found
        """
        issues: list[ValidationIssue] = []

        # Row count validation
        row_issue = self.validate_row_count(original, transformed)
        if row_issue:
            issues.append(row_issue)

        # Null preservation validation
        issues.extend(self.validate_null_preservation(original, transformed, profile))

        # Type preservation validation
        issues.extend(self.validate_type_preservation(original, transformed, profile))

        return issues
