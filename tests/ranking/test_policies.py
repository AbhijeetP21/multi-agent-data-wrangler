"""Tests for ranking policies."""

import pytest

from src.common.types import (
    QualityMetrics,
    QualityDelta,
    ValidationResult,
    Transformation,
    TransformationType,
)
from src.common.types.ranking import TransformationCandidate
from src.ranking.policies import (
    BaseRankingPolicy,
    CompositeScorePolicy,
    ImprovementPolicy,
)


def create_candidate(
    transformation_type: str,
    target_columns: list[str],
    quality_before: QualityMetrics,
    quality_after: QualityMetrics,
) -> TransformationCandidate:
    """Helper to create a test candidate."""
    transformation = Transformation(
        id=f"test_{transformation_type}",
        type=TransformationType(transformation_type),
        target_columns=target_columns,
        params={},
        reversible=True,
        description=f"Test {transformation_type}",
    )
    validation_result = ValidationResult(
        passed=True,
        issues=[],
        original_row_count=100,
        transformed_row_count=100,
        schema_compatible=True,
    )
    quality_delta = QualityDelta(
        before=quality_before,
        after=quality_after,
        improvement=QualityMetrics(
            completeness=quality_after.completeness - quality_before.completeness,
            consistency=quality_after.consistency - quality_before.consistency,
            validity=quality_after.validity - quality_before.validity,
            uniqueness=quality_after.uniqueness - quality_before.uniqueness,
            overall=quality_after.overall - quality_before.overall,
        ),
        composite_delta=quality_after.overall - quality_before.overall,
    )

    return TransformationCandidate(
        transformation=transformation,
        validation_result=validation_result,
        quality_before=quality_before,
        quality_after=quality_after,
        quality_delta=quality_delta,
    )


class TestCompositeScorePolicy:
    """Tests for CompositeScorePolicy."""

    def test_default_weights(self):
        """Test with default weights."""
        policy = CompositeScorePolicy()
        assert policy.weights["completeness"] == 0.25
        assert policy.weights["consistency"] == 0.25
        assert policy.weights["validity"] == 0.25
        assert policy.weights["uniqueness"] == 0.25

    def test_custom_weights(self):
        """Test with custom weights."""
        weights = {"completeness": 0.4, "consistency": 0.3, "validity": 0.2, "uniqueness": 0.1}
        policy = CompositeScorePolicy(weights)
        assert policy.weights == weights

    def test_score_with_improvement(self):
        """Test scoring with quality improvement."""
        policy = CompositeScorePolicy()

        candidate = create_candidate(
            "fill_missing",
            ["col1"],
            QualityMetrics(
                completeness=0.5,
                consistency=0.5,
                validity=0.5,
                uniqueness=0.5,
                overall=0.5,
            ),
            QualityMetrics(
                completeness=1.0,
                consistency=0.8,
                validity=0.7,
                uniqueness=0.6,
                overall=0.775,
            ),
        )

        score = policy.score(candidate)
        assert score > 0  # Should have positive score due to improvement

    def test_score_no_improvement(self):
        """Test scoring with no quality improvement."""
        policy = CompositeScorePolicy()

        candidate = create_candidate(
            "fill_missing",
            ["col1"],
            QualityMetrics(
                completeness=0.8,
                consistency=0.8,
                validity=0.8,
                uniqueness=0.8,
                overall=0.8,
            ),
            QualityMetrics(
                completeness=0.8,
                consistency=0.8,
                validity=0.8,
                uniqueness=0.8,
                overall=0.8,
            ),
        )

        score = policy.score(candidate)
        assert score >= 0

    def test_reasoning_includes_transformation_info(self):
        """Test that reasoning includes transformation details."""
        policy = CompositeScorePolicy()

        candidate = create_candidate(
            "fill_missing",
            ["col1", "col2"],
            QualityMetrics(
                completeness=0.5,
                consistency=0.5,
                validity=0.5,
                uniqueness=0.5,
                overall=0.5,
            ),
            QualityMetrics(
                completeness=1.0,
                consistency=0.8,
                validity=0.7,
                uniqueness=0.6,
                overall=0.775,
            ),
        )

        score = policy.score(candidate)
        reasoning = policy.get_reasoning(candidate, score)

        assert "fill_missing" in reasoning
        assert "col1" in reasoning
        assert "col2" in reasoning
        assert "composite score" in reasoning.lower()


class TestImprovementPolicy:
    """Tests for ImprovementPolicy."""

    def test_default_primary_metric(self):
        """Test default primary metric."""
        policy = ImprovementPolicy()
        assert policy.primary_metric == "overall"

    def test_custom_primary_metric(self):
        """Test custom primary metric."""
        policy = ImprovementPolicy(primary_metric="completeness")
        assert policy.primary_metric == "completeness"

    def test_score_overall(self):
        """Test scoring with overall metric."""
        policy = ImprovementPolicy(primary_metric="overall")

        candidate = create_candidate(
            "fill_missing",
            ["col1"],
            QualityMetrics(
                completeness=0.5,
                consistency=0.5,
                validity=0.5,
                uniqueness=0.5,
                overall=0.5,
            ),
            QualityMetrics(
                completeness=1.0,
                consistency=0.8,
                validity=0.7,
                uniqueness=0.6,
                overall=0.775,
            ),
        )

        score = policy.score(candidate)
        assert score == candidate.quality_delta.composite_delta

    def test_score_completeness(self):
        """Test scoring with completeness metric."""
        policy = ImprovementPolicy(primary_metric="completeness")

        candidate = create_candidate(
            "fill_missing",
            ["col1"],
            QualityMetrics(
                completeness=0.5,
                consistency=0.5,
                validity=0.5,
                uniqueness=0.5,
                overall=0.5,
            ),
            QualityMetrics(
                completeness=1.0,
                consistency=0.8,
                validity=0.7,
                uniqueness=0.6,
                overall=0.775,
            ),
        )

        score = policy.score(candidate)
        assert score == 0.5  # completeness improvement

    def test_score_consistency(self):
        """Test scoring with consistency metric."""
        policy = ImprovementPolicy(primary_metric="consistency")

        candidate = create_candidate(
            "fill_missing",
            ["col1"],
            QualityMetrics(
                completeness=0.5,
                consistency=0.5,
                validity=0.5,
                uniqueness=0.5,
                overall=0.5,
            ),
            QualityMetrics(
                completeness=0.5,
                consistency=0.9,
                validity=0.5,
                uniqueness=0.5,
                overall=0.6,
            ),
        )

        score = policy.score(candidate)
        assert score == 0.4  # consistency improvement

    def test_reasoning_includes_metric_info(self):
        """Test that reasoning includes metric information."""
        policy = ImprovementPolicy(primary_metric="completeness")

        candidate = create_candidate(
            "fill_missing",
            ["col1"],
            QualityMetrics(
                completeness=0.5,
                consistency=0.5,
                validity=0.5,
                uniqueness=0.5,
                overall=0.5,
            ),
            QualityMetrics(
                completeness=1.0,
                consistency=0.8,
                validity=0.7,
                uniqueness=0.6,
                overall=0.775,
            ),
        )

        score = policy.score(candidate)
        reasoning = policy.get_reasoning(candidate, score)

        assert "completeness" in reasoning
        assert "fill_missing" in reasoning

    def test_invalid_primary_metric(self):
        """Test with invalid primary metric falls back to composite delta."""
        policy = ImprovementPolicy(primary_metric="invalid")

        candidate = create_candidate(
            "fill_missing",
            ["col1"],
            QualityMetrics(
                completeness=0.5,
                consistency=0.5,
                validity=0.5,
                uniqueness=0.5,
                overall=0.5,
            ),
            QualityMetrics(
                completeness=1.0,
                consistency=0.8,
                validity=0.7,
                uniqueness=0.6,
                overall=0.775,
            ),
        )

        score = policy.score(candidate)
        assert score == candidate.quality_delta.composite_delta


class TestBaseRankingPolicy:
    """Tests for BaseRankingPolicy."""

    def test_format_quality_improvement(self):
        """Test quality improvement formatting."""

        class TestPolicy(BaseRankingPolicy):
            def score(self, candidate: TransformationCandidate) -> float:
                return 0.0

            def get_reasoning(self, candidate: TransformationCandidate, score: float) -> str:
                return ""

        policy = TestPolicy()

        candidate = create_candidate(
            "fill_missing",
            ["col1"],
            QualityMetrics(
                completeness=0.5,
                consistency=0.5,
                validity=0.5,
                uniqueness=0.5,
                overall=0.5,
            ),
            QualityMetrics(
                completeness=1.0,
                consistency=0.8,
                validity=0.7,
                uniqueness=0.6,
                overall=0.775,
            ),
        )

        result = policy._format_quality_improvement(candidate)

        assert "completeness" in result
        assert "+" in result  # Should show improvement with + sign

    def test_format_no_improvement(self):
        """Test formatting when there's no improvement."""

        class TestPolicy(BaseRankingPolicy):
            def score(self, candidate: TransformationCandidate) -> float:
                return 0.0

            def get_reasoning(self, candidate: TransformationCandidate, score: float) -> str:
                return ""

        policy = TestPolicy()

        candidate = create_candidate(
            "fill_missing",
            ["col1"],
            QualityMetrics(
                completeness=0.8,
                consistency=0.8,
                validity=0.8,
                uniqueness=0.8,
                overall=0.8,
            ),
            QualityMetrics(
                completeness=0.8,
                consistency=0.8,
                validity=0.8,
                uniqueness=0.8,
                overall=0.8,
            ),
        )

        result = policy._format_quality_improvement(candidate)
        assert "no improvement" in result
