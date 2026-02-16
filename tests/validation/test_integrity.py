"""Tests for integrity validation."""

import pandas as pd
import pytest
from datetime import datetime

from src.common.types import DataProfile, ColumnProfile
from src.validation.integrity_validator import IntegrityValidator


class TestIntegrityValidator:
    """Test cases for IntegrityValidator."""

    @pytest.fixture
    def sample_profile(self) -> DataProfile:
        """Create a sample data profile for testing."""
        columns = {
            "id": ColumnProfile(
                name="id",
                dtype="int64",
                null_count=0,
                null_percentage=0.0,
                unique_count=100,
                inferred_type="numeric",
            ),
            "value": ColumnProfile(
                name="value",
                dtype="float64",
                null_count=5,
                null_percentage=5.0,
                unique_count=95,
                min_value=1.0,
                max_value=100.0,
                mean=50.5,
                std=28.87,
                inferred_type="numeric",
            ),
            "category": ColumnProfile(
                name="category",
                dtype="object",
                null_count=0,
                null_percentage=0.0,
                unique_count=3,
                inferred_type="categorical",
            ),
        }
        return DataProfile(
            timestamp=datetime.now(),
            row_count=100,
            column_count=3,
            columns=columns,
            overall_missing_percentage=1.67,
            duplicate_rows=0,
        )

    @pytest.fixture
    def original_df(self) -> pd.DataFrame:
        """Create a sample original DataFrame."""
        import numpy as np
        # Create value column with some nulls to match profile (5 nulls)
        values = [float(i) for i in range(1, 101)]
        for i in range(5):
            values[i] = np.nan
        return pd.DataFrame({
            "id": range(1, 101),
            "value": values,
            "category": ["A"] * 50 + ["B"] * 30 + ["C"] * 20,
        })

    def test_validate_row_count_no_loss(self, original_df: pd.DataFrame, sample_profile: DataProfile) -> None:
        """Test that identical DataFrames pass row count validation."""
        validator = IntegrityValidator()
        issue = validator.validate_row_count(original_df, original_df)
        assert issue is None

    def test_validate_row_count_within_tolerance(self, original_df: pd.DataFrame) -> None:
        """Test that small row loss within tolerance produces warning."""
        validator = IntegrityValidator(row_count_tolerance=0.1)
        transformed = original_df.iloc[:-5]  # Lose 5 rows (5%)
        issue = validator.validate_row_count(original_df, transformed)
        assert issue is not None
        assert issue.severity == "warning"
        assert issue.code == "ROW_LOSS"

    def test_validate_row_count_exceeds_tolerance(self, original_df: pd.DataFrame) -> None:
        """Test that row loss exceeding tolerance produces error."""
        validator = IntegrityValidator(row_count_tolerance=0.1)
        transformed = original_df.iloc[:-50]  # Lose 50 rows (50%)
        issue = validator.validate_row_count(original_df, transformed)
        assert issue is not None
        assert issue.severity == "error"
        assert issue.code == "EXCESSIVE_ROW_LOSS"

    def test_validate_row_count_empty_original(self, sample_profile: DataProfile) -> None:
        """Test handling of empty original DataFrame."""
        validator = IntegrityValidator()
        original = pd.DataFrame()
        transformed = pd.DataFrame({"a": [1, 2, 3]})
        issue = validator.validate_row_count(original, transformed)
        assert issue is None

    def test_validate_null_preservation_no_change(self, original_df: pd.DataFrame, sample_profile: DataProfile) -> None:
        """Test null preservation with no null changes."""
        validator = IntegrityValidator()
        issues = validator.validate_null_preservation(original_df, original_df, sample_profile)
        assert len(issues) == 0

    def test_validate_null_preservation_nulls_added(self, original_df: pd.DataFrame, sample_profile: DataProfile) -> None:
        """Test detection of added nulls."""
        validator = IntegrityValidator()
        transformed = original_df.copy()
        # Add 5 nulls at positions that don't already have nulls (positions 10-14)
        transformed.loc[10:14, "value"] = None
        issues = validator.validate_null_preservation(original_df, transformed, sample_profile)
        assert len(issues) > 0
        assert any(issue.code == "NULLS_INCREASED" for issue in issues)

    def test_validate_null_preservation_column_removed(self, original_df: pd.DataFrame, sample_profile: DataProfile) -> None:
        """Test detection of removed columns."""
        validator = IntegrityValidator()
        transformed = original_df.drop(columns=["category"])
        issues = validator.validate_null_preservation(original_df, transformed, sample_profile)
        assert len(issues) > 0
        assert any(issue.code == "COLUMN_REMOVED" for issue in issues)

    def test_validate_type_preservation_no_change(self, original_df: pd.DataFrame, sample_profile: DataProfile) -> None:
        """Test type preservation with no changes."""
        validator = IntegrityValidator()
        issues = validator.validate_type_preservation(original_df, original_df, sample_profile)
        # Filter out object->str conversion which is a pandas quirk
        critical_issues = [i for i in issues if i.code == "TYPE_CHANGED" and i.column != "category"]
        assert len(critical_issues) == 0

    def test_validate_type_preservation_type_changed(self, original_df: pd.DataFrame, sample_profile: DataProfile) -> None:
        """Test detection of type changes."""
        validator = IntegrityValidator()
        transformed = original_df.copy()
        transformed["id"] = transformed["id"].astype(str)
        issues = validator.validate_type_preservation(original_df, transformed, sample_profile)
        assert len(issues) > 0
        assert any(issue.code == "TYPE_CHANGED" for issue in issues)

    def test_validate_all_passes(self, original_df: pd.DataFrame, sample_profile: DataProfile) -> None:
        """Test complete validation passes for identical DataFrames."""
        validator = IntegrityValidator()
        issues = validator.validate_all(original_df, original_df, sample_profile)
        # Should have no errors (may have warnings for type changes)
        error_count = sum(1 for issue in issues if issue.severity == "error")
        assert error_count == 0

    def test_validate_all_detects_issues(self, original_df: pd.DataFrame, sample_profile: DataProfile) -> None:
        """Test that validate_all detects multiple issues."""
        validator = IntegrityValidator(row_count_tolerance=0.1)
        transformed = original_df.iloc[:-50].copy()  # Excessive row loss
        transformed = transformed.drop(columns=["category"])  # Column removed
        issues = validator.validate_all(original_df, transformed, sample_profile)
        assert len(issues) > 0
