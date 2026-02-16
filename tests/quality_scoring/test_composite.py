"""Tests for composite scoring."""

import pandas as pd
import pytest

from src.common.types import QualityMetrics
from src.quality_scoring.composite_calculator import CompositeCalculator, Weights
from src.quality_scoring.comparator import Comparator
from src.quality_scoring.scorer import QualityScorerService


class TestCompositeCalculator:
    """Tests for CompositeCalculator."""

    def test_equal_weights(self):
        """Test with equal weights."""
        weights = Weights()
        assert weights.completeness == 0.25
        assert weights.consistency == 0.25
        assert weights.validity == 0.25
        assert weights.uniqueness == 0.25

    def test_custom_weights(self):
        """Test with custom weights."""
        weights = Weights(completeness=0.4, consistency=0.3, validity=0.2, uniqueness=0.1)
        calculator = CompositeCalculator(weights)
        metrics = QualityMetrics(
            completeness=1.0,
            consistency=1.0,
            validity=1.0,
            uniqueness=1.0,
            overall=0.0,
        )
        result = calculator.calculate(metrics)
        assert abs(result - 1.0) < 1e-6

    def test_composite_calculation(self):
        """Test composite score calculation."""
        weights = Weights()
        calculator = CompositeCalculator(weights)
        metrics = QualityMetrics(
            completeness=1.0,
            consistency=0.5,
            validity=0.5,
            uniqueness=0.0,
            overall=0.0,
        )
        # 1.0 * 0.25 + 0.5 * 0.25 + 0.5 * 0.25 + 0.0 * 0.25 = 0.5
        result = calculator.calculate(metrics)
        assert result == 0.5

    def test_invalid_weights(self):
        """Test with invalid weights that don't sum to 1."""
        with pytest.raises(ValueError):
            Weights(completeness=0.5, consistency=0.5, validity=0.5, uniqueness=0.5)


class TestComparator:
    """Tests for Comparator."""

    def test_compare_improvement(self):
        """Test comparison with improvement."""
        comparator = Comparator()
        before = QualityMetrics(
            completeness=0.5,
            consistency=0.5,
            validity=0.5,
            uniqueness=0.5,
            overall=0.5,
        )
        after = QualityMetrics(
            completeness=0.8,
            consistency=0.7,
            validity=0.6,
            uniqueness=0.6,
            overall=0.675,
        )
        delta = comparator.compare(before, after)

        assert abs(delta.composite_delta - 0.175) < 1e-6
        assert abs(delta.improvement.completeness - 0.3) < 1e-6
        assert abs(delta.improvement.consistency - 0.2) < 1e-6
        assert abs(delta.improvement.validity - 0.1) < 1e-6
        assert abs(delta.improvement.uniqueness - 0.1) < 1e-6
        assert abs(delta.improvement.overall - 0.175) < 1e-6

    def test_compare_decline(self):
        """Test comparison with decline."""
        comparator = Comparator()
        before = QualityMetrics(
            completeness=0.8,
            consistency=0.7,
            validity=0.6,
            uniqueness=0.6,
            overall=0.675,
        )
        after = QualityMetrics(
            completeness=0.5,
            consistency=0.5,
            validity=0.5,
            uniqueness=0.5,
            overall=0.5,
        )
        delta = comparator.compare(before, after)

        assert abs(delta.composite_delta - (-0.175)) < 1e-6
        assert abs(delta.improvement.completeness - (-0.3)) < 1e-6


class TestQualityScorerService:
    """Tests for QualityScorerService."""

    def test_score_complete_data(self):
        """Test scoring with complete data."""
        service = QualityScorerService()
        data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        metrics = service.score(data)

        assert metrics.completeness == 1.0
        assert metrics.consistency == 1.0
        assert metrics.validity == 1.0
        assert metrics.uniqueness == 1.0
        assert metrics.overall == 1.0

    def test_score_with_nulls(self):
        """Test scoring with null values."""
        service = QualityScorerService()
        data = pd.DataFrame({"a": [1, None, 3], "b": [4, 5, None]})
        metrics = service.score(data)

        # Completeness: 4/6 = 0.667
        assert abs(metrics.completeness - 0.667) < 0.01

    def test_compare(self):
        """Test compare method."""
        service = QualityScorerService()
        before = QualityMetrics(
            completeness=0.5,
            consistency=0.5,
            validity=0.5,
            uniqueness=0.5,
            overall=0.5,
        )
        after = QualityMetrics(
            completeness=1.0,
            consistency=1.0,
            validity=1.0,
            uniqueness=1.0,
            overall=1.0,
        )
        delta = service.compare(before, after)

        assert delta.composite_delta == 0.5
        assert delta.before == before
        assert delta.after == after
