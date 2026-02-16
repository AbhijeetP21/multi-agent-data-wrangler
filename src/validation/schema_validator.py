"""Schema validator module."""

import pandas as pd

from src.common.types import DataProfile, ValidationIssue


class SchemaValidator:
    """Validates schema compatibility between original and transformed data."""

    def validate_column_existence(
        self, original: pd.DataFrame, transformed: pd.DataFrame
    ) -> list[ValidationIssue]:
        """
        Validate that required columns still exist in transformed data.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame

        Returns:
            List of ValidationIssues for missing columns
        """
        issues: list[ValidationIssue] = []

        original_cols = set(original.columns)
        transformed_cols = set(transformed.columns)

        missing_cols = original_cols - transformed_cols

        for col in missing_cols:
            issues.append(
                ValidationIssue(
                    severity="error",
                    code="MISSING_COLUMN",
                    message=f"Column '{col}' is missing in transformed data",
                    column=col,
                )
            )

        return issues

    def validate_column_types(
        self, original: pd.DataFrame, transformed: pd.DataFrame, profile: DataProfile
    ) -> list[ValidationIssue]:
        """
        Validate that column types are compatible.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame
            profile: Data profile of original

        Returns:
            List of ValidationIssues for type incompatibilities
        """
        issues: list[ValidationIssue] = []

        for col_name, col_profile in profile.columns.items():
            if col_name not in transformed.columns:
                continue

            original_dtype = col_profile.dtype
            transformed_dtype = str(transformed[col_name].dtype)

            # Check for fundamental type incompatibilities
            # e.g., trying to convert numeric to string that's not compatible
            original_is_numeric = original_dtype in ("int64", "float64", "int32", "float32")
            original_is_object = original_dtype == "object"
            transformed_is_numeric = transformed_dtype in ("int64", "float64", "int32", "float32")
            transformed_is_object = transformed_dtype == "object"

            # If original was numeric and transformed is object, check if it's problematic
            if original_is_numeric and transformed_is_object:
                # Try to see if values can be parsed back to numeric
                try:
                    transformed[col_name].astype(float)
                    # Conversion possible, but might indicate data loss
                    issues.append(
                        ValidationIssue(
                            severity="warning",
                            code="TYPE_CONVERSION",
                            message=f"Column '{col_name}' converted from numeric to string",
                            column=col_name,
                        )
                    )
                except (ValueError, TypeError):
                    # Actual type incompatibility
                    issues.append(
                        ValidationIssue(
                            severity="error",
                            code="INCOMPATIBLE_TYPE",
                            message=f"Column '{col_name}' has incompatible type conversion "
                            f"from '{original_dtype}' to '{transformed_dtype}'",
                            column=col_name,
                        )
                    )

        return issues

    def validate_schema_compatibility(
        self, original: pd.DataFrame, transformed: pd.DataFrame, profile: DataProfile
    ) -> tuple[bool, list[ValidationIssue]]:
        """
        Validate overall schema compatibility.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame
            profile: Data profile of original

        Returns:
            Tuple of (is_compatible, list of issues)
        """
        issues: list[ValidationIssue] = []

        # Validate column existence
        issues.extend(self.validate_column_existence(original, transformed))

        # Validate column types
        issues.extend(self.validate_column_types(original, transformed, profile))

        # Schema is compatible if there are no error-level issues
        has_errors = any(issue.severity == "error" for issue in issues)
        is_compatible = not has_errors

        return is_compatible, issues
