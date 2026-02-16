"""Tests for candidate generator."""

import pytest
from datetime import datetime

from src.common.types.data_profile import DataProfile, ColumnProfile
from src.common.types.transformation import TransformationType
from src.transformation.candidate_generator import CandidateGenerator


class TestCandidateGenerator:
    """Test cases for CandidateGenerator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = CandidateGenerator()

    def test_generate_candidates_empty_profile(self):
        """Test candidate generation with empty profile."""
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=0,
            column_count=0,
            columns={},
            overall_missing_percentage=0.0,
            duplicate_rows=0,
        )

        candidates = self.generator.generate_candidates(profile)
        assert len(candidates) == 0

    def test_generate_fill_missing_candidates(self):
        """Test fill missing candidates are generated."""
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=100,
            column_count=2,
            columns={
                "numeric_col": ColumnProfile(
                    name="numeric_col",
                    dtype="float64",
                    null_count=10,
                    null_percentage=10.0,
                    unique_count=90,
                    min_value=0.0,
                    max_value=100.0,
                    mean=50.0,
                    std=30.0,
                    inferred_type="numeric",
                ),
                "categorical_col": ColumnProfile(
                    name="categorical_col",
                    dtype="object",
                    null_count=5,
                    null_percentage=5.0,
                    unique_count=3,
                    inferred_type="categorical",
                ),
            },
            overall_missing_percentage=7.5,
            duplicate_rows=0,
        )

        candidates = self.generator.generate_candidates(profile)

        # Should have fill_missing candidates for both columns
        fill_missing_candidates = [
            c for c in candidates if c.type == TransformationType.FILL_MISSING
        ]
        assert len(fill_missing_candidates) > 0
        assert any(c.target_columns == ["numeric_col"] for c in fill_missing_candidates)
        assert any(c.target_columns == ["categorical_col"] for c in fill_missing_candidates)

    def test_generate_normalize_candidates(self):
        """Test normalize candidates are generated for numeric columns."""
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=100,
            column_count=1,
            columns={
                "numeric_col": ColumnProfile(
                    name="numeric_col",
                    dtype="float64",
                    null_count=0,
                    null_percentage=0.0,
                    unique_count=100,
                    min_value=0.0,
                    max_value=100.0,
                    mean=50.0,
                    std=30.0,
                    inferred_type="numeric",
                ),
            },
            overall_missing_percentage=0.0,
            duplicate_rows=0,
        )

        candidates = self.generator.generate_candidates(profile)

        # Should have normalize candidates
        normalize_candidates = [
            c for c in candidates if c.type == TransformationType.NORMALIZE
        ]
        assert len(normalize_candidates) > 0

    def test_generate_encode_categorical_candidates(self):
        """Test encode categorical candidates are generated."""
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=100,
            column_count=1,
            columns={
                "categorical_col": ColumnProfile(
                    name="categorical_col",
                    dtype="object",
                    null_count=0,
                    null_percentage=0.0,
                    unique_count=3,
                    inferred_type="categorical",
                ),
            },
            overall_missing_percentage=0.0,
            duplicate_rows=0,
        )

        candidates = self.generator.generate_candidates(profile)

        # Should have encode_categorical candidates
        encode_candidates = [
            c for c in candidates if c.type == TransformationType.ENCODE_CATEGORICAL
        ]
        assert len(encode_candidates) > 0

    def test_generate_drop_duplicates_candidate(self):
        """Test drop duplicates candidate is generated when duplicates exist."""
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=100,
            column_count=1,
            columns={
                "col": ColumnProfile(
                    name="col",
                    dtype="float64",
                    null_count=0,
                    null_percentage=0.0,
                    unique_count=90,
                    inferred_type="numeric",
                ),
            },
            overall_missing_percentage=0.0,
            duplicate_rows=10,
        )

        candidates = self.generator.generate_candidates(profile)

        # Should have drop_duplicates candidate
        drop_dup_candidates = [
            c for c in candidates if c.type == TransformationType.DROP_DUPLICATES
        ]
        assert len(drop_dup_candidates) == 1

    def test_no_drop_duplicates_when_no_duplicates(self):
        """Test no drop duplicates candidate when no duplicates exist."""
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=100,
            column_count=1,
            columns={
                "col": ColumnProfile(
                    name="col",
                    dtype="float64",
                    null_count=0,
                    null_percentage=0.0,
                    unique_count=100,
                    inferred_type="numeric",
                ),
            },
            overall_missing_percentage=0.0,
            duplicate_rows=0,
        )

        candidates = self.generator.generate_candidates(profile)

        # Should NOT have drop_duplicates candidate
        drop_dup_candidates = [
            c for c in candidates if c.type == TransformationType.DROP_DUPLICATES
        ]
        assert len(drop_dup_candidates) == 0

    def test_generate_remove_outliers_candidates(self):
        """Test remove outliers candidates are generated for numeric columns with std."""
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=100,
            column_count=1,
            columns={
                "numeric_col": ColumnProfile(
                    name="numeric_col",
                    dtype="float64",
                    null_count=0,
                    null_percentage=0.0,
                    unique_count=100,
                    min_value=0.0,
                    max_value=100.0,
                    mean=50.0,
                    std=30.0,
                    inferred_type="numeric",
                ),
            },
            overall_missing_percentage=0.0,
            duplicate_rows=0,
        )

        candidates = self.generator.generate_candidates(profile)

        # Should have remove_outliers candidates
        outliers_candidates = [
            c for c in candidates if c.type == TransformationType.REMOVE_OUTLIERS
        ]
        assert len(outliers_candidates) > 0

    def test_generate_cast_type_candidates(self):
        """Test cast type candidates are generated for text columns."""
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=100,
            column_count=1,
            columns={
                "text_col": ColumnProfile(
                    name="text_col",
                    dtype="object",
                    null_count=0,
                    null_percentage=0.0,
                    unique_count=100,
                    inferred_type="text",
                ),
            },
            overall_missing_percentage=0.0,
            duplicate_rows=0,
        )

        candidates = self.generator.generate_candidates(profile)

        # Should have cast_type candidates
        cast_candidates = [
            c for c in candidates if c.type == TransformationType.CAST_TYPE
        ]
        assert len(cast_candidates) > 0
