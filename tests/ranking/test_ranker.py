"""Tests for RankingService (ranker)."""

import pytest

from src.common.types import (
    QualityMetrics,
    QualityDelta,
    ValidationResult,
    ValidationIssue,
    Transformation,
    TransformationType,
)
from src.common.types.ranking import TransformationCandidate, RankedTransformation
from src.ranking import RankingService, CompositeScorePolicy, ImprovementPolicy


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


class TestRankingService:
    """Tests for RankingService."""

    def test_rank_with_composite_policy(self):
        """Test ranking with CompositeScorePolicy."""
        policy = CompositeScorePolicy()
        ranker = RankingService(policy)

        # Create candidates with different quality improvements
        candidate1 = create_candidate(
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
                completeness=0.9,
                consistency=0.7,
                validity=0.6,
                uniqueness=0.5,
                overall=0.675,
            ),
        )

        candidate2 = create_candidate(
            "normalize",
            ["col2"],
            QualityMetrics(
                completeness=0.8,
                consistency=0.8,
                validity=0.8,
                uniqueness=0.8,
                overall=0.8,
            ),
            QualityMetrics(
                completeness=0.9,
                consistency=0.9,
                validity=0.9,
                uniqueness=0.8,
                overall=0.875,
            ),
        )

        candidates = [candidate1, candidate2]
        ranked = ranker.rank(candidates)

        assert len(ranked) == 2
        assert ranked[0].rank == 1
        assert ranked[1].rank == 2
        # Candidate 1 has more improvement
        assert ranked[0].composite_score >= ranked[1].composite_score
        assert ranked[0].reasoning is not None
        assert ranked[1].reasoning is not None

    def test_rank_with_improvement_policy(self):
        """Test ranking with ImprovementPolicy."""
        policy = ImprovementPolicy()
        ranker = RankingService(policy)

        candidate1 = create_candidate(
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
                consistency=0.6,
                validity=0.6,
                uniqueness=0.5,
                overall=0.675,
            ),
        )

        candidate2 = create_candidate(
            "drop_duplicates",
            ["col2"],
            QualityMetrics(
                completeness=0.9,
                consistency=0.9,
                validity=0.9,
                uniqueness=0.5,
                overall=0.825,
            ),
            QualityMetrics(
                completeness=0.9,
                consistency=0.9,
                validity=0.9,
                uniqueness=0.9,
                overall=0.9,
            ),
        )

        candidates = [candidate1, candidate2]
        ranked = ranker.rank(candidates)

        assert len(ranked) == 2
        assert ranked[0].rank == 1
        assert ranked[1].rank == 2

    def test_rank_empty_list(self):
        """Test ranking with empty list."""
        policy = CompositeScorePolicy()
        ranker = RankingService(policy)

        ranked = ranker.rank([])

        assert ranked == []

    def test_rank_no_policy_set(self):
        """Test ranking without policy set."""
        ranker = RankingService()

        with pytest.raises(ValueError, match="No ranking policy set"):
            ranker.rank([])

    def test_set_policy(self):
        """Test setting policy after initialization."""
        ranker = RankingService()

        # Initially no policy
        with pytest.raises(ValueError, match="No ranking policy set"):
            ranker.rank([])

        # Set policy
        policy = CompositeScorePolicy()
        ranker.set_policy(policy)

        # Now should work
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
                consistency=0.6,
                validity=0.6,
                uniqueness=0.5,
                overall=0.675,
            ),
        )

        ranked = ranker.rank([candidate])
        assert len(ranked) == 1

    def test_ranked_transformation_structure(self):
        """Test RankedTransformation has correct structure."""
        policy = CompositeScorePolicy()
        ranker = RankingService(policy)

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
                consistency=0.6,
                validity=0.6,
                uniqueness=0.5,
                overall=0.675,
            ),
        )

        ranked = ranker.rank([candidate])

        assert isinstance(ranked[0], RankedTransformation)
        assert ranked[0].rank == 1
        assert ranked[0].candidate == candidate
        assert isinstance(ranked[0].composite_score, float)
        assert isinstance(ranked[0].reasoning, str)

    def test_ranking_sorted_by_score(self):
        """Test that rankings are sorted by score (descending)."""
        policy = CompositeScorePolicy()
        ranker = RankingService(policy)

        # Create candidates with decreasing improvements
        candidate1 = create_candidate(
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
                consistency=1.0,
                validity=1.0,
                uniqueness=1.0,
                overall=1.0,
            ),
        )

        candidate2 = create_candidate(
            "normalize",
            ["col2"],
            QualityMetrics(
                completeness=0.7,
                consistency=0.7,
                validity=0.7,
                uniqueness=0.7,
                overall=0.7,
            ),
            QualityMetrics(
                completeness=0.9,
                consistency=0.9,
                validity=0.9,
                uniqueness=0.7,
                overall=0.85,
            ),
        )

        candidate3 = create_candidate(
            "drop_duplicates",
            ["col3"],
            QualityMetrics(
                completeness=0.9,
                consistency=0.9,
                validity=0.9,
                uniqueness=0.9,
                overall=0.9,
            ),
            QualityMetrics(
                completeness=0.95,
                consistency=0.95,
                validity=0.95,
                uniqueness=0.95,
                overall=0.95,
            ),
        )

        candidates = [candidate3, candidate1, candidate2]  # Out of order
        ranked = ranker.rank(candidates)

        # Should be sorted by score descending
        assert ranked[0].composite_score >= ranked[1].composite_score
        assert ranked[1].composite_score >= ranked[2].composite_score

        # Ranks should be 1, 2, 3
        assert ranked[0].rank == 1
        assert ranked[1].rank == 2
        assert ranked[2].rank == 3
