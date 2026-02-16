"""Tests for common types."""

import pytest
from datetime import datetime

from src.common.types import (
    DataProfile,
    ColumnProfile,
    Transformation,
    TransformationType,
    TransformationResult,
    ValidationResult,
    ValidationIssue,
    QualityMetrics,
    QualityDelta,
    TransformationCandidate,
    RankedTransformation,
    PipelineState,
    PipelineStep,
    PipelineConfig,
)


class TestDataProfileTypes:
    """Tests for data profile types."""

    def test_column_profile_creation(self):
        """Test creating a ColumnProfile."""
        profile = ColumnProfile(
            name="age",
            dtype="int64",
            null_count=5,
            null_percentage=10.0,
            unique_count=30,
            min_value=18.0,
            max_value=65.0,
            mean=35.5,
            std=12.3,
            inferred_type="numeric",
        )
        assert profile.name == "age"
        assert profile.inferred_type == "numeric"

    def test_data_profile_creation(self):
        """Test creating a DataProfile."""
        columns = {
            "name": ColumnProfile(
                name="name",
                dtype="object",
                null_count=0,
                null_percentage=0.0,
                inferred_type="text",
            )
        }
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=100,
            column_count=1,
            columns=columns,
            overall_missing_percentage=0.0,
            duplicate_rows=2,
        )
        assert profile.row_count == 100
        assert profile.column_count == 1

    def test_data_profile_serialization(self):
        """Test serialization/deserialization of DataProfile."""
        columns = {
            "age": ColumnProfile(
                name="age",
                dtype="int64",
                null_count=5,
                null_percentage=10.0,
                inferred_type="numeric",
            )
        }
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=100,
            column_count=1,
            columns=columns,
            overall_missing_percentage=5.0,
            duplicate_rows=2,
        )

        # Serialize to JSON
        json_str = profile.model_dump_json()

        # Deserialize back
        profile_restored = DataProfile.model_validate_json(json_str)

        assert profile_restored.row_count == profile.row_count
        assert profile_restored.columns["age"].name == "age"


class TestTransformationTypes:
    """Tests for transformation types."""

    def test_transformation_type_enum(self):
        """Test TransformationType enum values."""
        assert TransformationType.FILL_MISSING == "fill_missing"
        assert TransformationType.NORMALIZE == "normalize"
        assert TransformationType.ENCODE_CATEGORICAL == "encode_categorical"

    def test_transformation_creation(self):
        """Test creating a Transformation."""
        transformation = Transformation(
            id="trans_1",
            type=TransformationType.FILL_MISSING,
            target_columns=["age", "income"],
            params={"strategy": "mean"},
            reversible=True,
            description="Fill missing values with mean",
        )
        assert transformation.id == "trans_1"
        assert transformation.type == TransformationType.FILL_MISSING


class TestValidationTypes:
    """Tests for validation types."""

    def test_validation_issue_creation(self):
        """Test creating a ValidationIssue."""
        issue = ValidationIssue(
            severity="error",
            code="E001",
            message="Missing required column",
            column="id",
        )
        assert issue.severity == "error"
        assert issue.column == "id"

    def test_validation_result_creation(self):
        """Test creating a ValidationResult."""
        issues = [
            ValidationIssue(
                severity="warning",
                code="W001",
                message="Potential data quality issue",
            )
        ]
        result = ValidationResult(
            passed=True,
            issues=issues,
            original_row_count=100,
            transformed_row_count=98,
            schema_compatible=True,
        )
        assert result.passed is True
        assert len(result.issues) == 1


class TestQualityTypes:
    """Tests for quality types."""

    def test_quality_metrics_creation(self):
        """Test creating QualityMetrics."""
        metrics = QualityMetrics(
            completeness=0.95,
            consistency=0.90,
            validity=0.85,
            uniqueness=0.80,
            overall=0.875,
        )
        assert metrics.completeness == 0.95

    def test_quality_delta_creation(self):
        """Test creating QualityDelta."""
        before = QualityMetrics(
            completeness=0.8,
            consistency=0.7,
            validity=0.75,
            uniqueness=0.9,
            overall=0.7875,
        )
        after = QualityMetrics(
            completeness=0.95,
            consistency=0.85,
            validity=0.90,
            uniqueness=0.88,
            overall=0.895,
        )
        improvement = QualityMetrics(
            completeness=0.15,
            consistency=0.15,
            validity=0.15,
            uniqueness=-0.02,
            overall=0.1075,
        )

        delta = QualityDelta(
            before=before,
            after=after,
            improvement=improvement,
            composite_delta=0.1075,
        )
        assert delta.composite_delta == 0.1075


class TestPipelineTypes:
    """Tests for pipeline types."""

    def test_pipeline_step_enum(self):
        """Test PipelineStep enum values."""
        assert PipelineStep.PROFILING == "profiling"
        assert PipelineStep.GENERATION == "generation"
        assert PipelineStep.VALIDATION == "validation"

    def test_pipeline_state_creation(self):
        """Test creating a PipelineState."""
        state = PipelineState(
            current_step=PipelineStep.PROFILING,
            completed_steps=[],
        )
        assert state.current_step == PipelineStep.PROFILING
        assert len(state.completed_steps) == 0
