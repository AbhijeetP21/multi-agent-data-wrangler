"""Tests for individual quality metrics."""

import pandas as pd
import pytest

from src.quality_scoring.metrics import (
    CompletenessMetric,
    ConsistencyMetric,
    ValidityMetric,
    UniquenessMetric,
)


class TestCompletenessMetric:
    """Tests for CompletenessMetric."""

    def test_complete_data(self):
        """Test with complete data (no nulls)."""
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        metric = CompletenessMetric()
        result = metric.calculate(data)
        assert result == 1.0

    def test_partial_nulls(self):
        """Test with some null values."""
        data = pd.DataFrame({"a": [1, None, 3], "b": [4, 5, None]})
        metric = CompletenessMetric()
        result = metric.calculate(data)
        # 4 non-null / 6 total = 0.667
        assert abs(result - 0.667) < 0.01

    def test_all_nulls(self):
        """Test with all null values."""
        data = pd.DataFrame({"a": [None, None, None], "b": [None, None, None]})
        metric = CompletenessMetric()
        result = metric.calculate(data)
        assert result == 0.0

    def test_empty_dataframe(self):
        """Test with empty dataframe."""
        data = pd.DataFrame()
        metric = CompletenessMetric()
        result = metric.calculate(data)
        assert result == 1.0


class TestConsistencyMetric:
    """Tests for ConsistencyMetric."""

    def test_consistent_numeric(self):
        """Test with consistent numeric data."""
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        metric = ConsistencyMetric()
        result = metric.calculate(data)
        assert result == 1.0

    def test_inconsistent_object(self):
        """Test with inconsistent object data (mixed types)."""
        data = pd.DataFrame({"a": [1, "two", 3, 4]})
        metric = ConsistencyMetric()
        result = metric.calculate(data)
        # Most common type is numeric (3 out of 4)
        assert result == 0.75

    def test_empty_dataframe(self):
        """Test with empty dataframe."""
        data = pd.DataFrame()
        metric = ConsistencyMetric()
        result = metric.calculate(data)
        assert result == 1.0


class TestValidityMetric:
    """Tests for ValidityMetric."""

    def test_valid_numeric(self):
        """Test with valid numeric data."""
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        metric = ValidityMetric()
        result = metric.calculate(data)
        assert result == 1.0

    def test_has_infinite(self):
        """Test with infinite values."""
        data = pd.DataFrame({"a": [1, 2, float("inf")]})
        metric = ValidityMetric()
        result = metric.calculate(data)
        # 2 valid / 3 total = 0.667
        assert abs(result - 0.667) < 0.01

    def test_empty_dataframe(self):
        """Test with empty dataframe."""
        data = pd.DataFrame()
        metric = ValidityMetric()
        result = metric.calculate(data)
        assert result == 1.0


class TestUniquenessMetric:
    """Tests for UniquenessMetric."""

    def test_all_unique(self):
        """Test with all unique values."""
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        metric = UniquenessMetric()
        result = metric.calculate(data)
        assert result == 1.0

    def test_all_duplicates(self):
        """Test with all duplicate values."""
        data = pd.DataFrame({"a": [1, 1, 1], "b": [2, 2, 2]})
        metric = UniquenessMetric()
        result = metric.calculate(data)
        # 1 unique / 3 total = 0.333
        assert abs(result - 0.333) < 0.01

    def test_empty_dataframe(self):
        """Test with empty dataframe."""
        data = pd.DataFrame()
        metric = UniquenessMetric()
        result = metric.calculate(data)
        assert result == 1.0
