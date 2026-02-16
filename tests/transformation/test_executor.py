"""Tests for transformation executor."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from src.common.types.transformation import Transformation, TransformationType
from src.transformation.executor import TransformationExecutor


class TestTransformationExecutor:
    """Test cases for TransformationExecutor."""

    def setup_method(self):
        """Set up test fixtures."""
        self.executor = TransformationExecutor()

    def test_execute_fill_missing_mean(self):
        """Test executing fill missing with mean strategy."""
        # Create test data with missing values
        data = pd.DataFrame({
            "col": [1.0, 2.0, np.nan, 4.0, 5.0]
        })

        transformation = Transformation(
            id="test-1",
            type=TransformationType.FILL_MISSING,
            target_columns=["col"],
            params={"strategy": "mean"},
            reversible=True,
            description="Fill missing with mean",
        )

        result = self.executor.execute(data, transformation)

        assert result.success is True
        assert result.output_data["col"].isna().sum() == 0
        assert result.output_data["col"].iloc[2] == 3.0  # (1+2+4+5)/4

    def test_execute_fill_missing_constant(self):
        """Test executing fill missing with constant."""
        data = pd.DataFrame({
            "col": [1.0, 2.0, np.nan, 4.0, 5.0]
        })

        transformation = Transformation(
            id="test-2",
            type=TransformationType.FILL_MISSING,
            target_columns=["col"],
            params={"strategy": "constant", "fill_value": 0},
            reversible=True,
            description="Fill missing with 0",
        )

        result = self.executor.execute(data, transformation)

        assert result.success is True
        assert result.output_data["col"].isna().sum() == 0
        assert result.output_data["col"].iloc[2] == 0

    def test_execute_normalize_standard(self):
        """Test executing standard normalization."""
        data = pd.DataFrame({
            "col": [1.0, 2.0, 3.0, 4.0, 5.0]
        })

        transformation = Transformation(
            id="test-3",
            type=TransformationType.NORMALIZE,
            target_columns=["col"],
            params={"method": "standard"},
            reversible=True,
            description="Standard normalize",
        )

        result = self.executor.execute(data, transformation)

        assert result.success is True
        # Mean should be ~0 and std should be ~1
        assert abs(result.output_data["col"].mean()) < 1e-10
        assert abs(result.output_data["col"].std() - 1.0) < 1e-10

    def test_execute_normalize_minmax(self):
        """Test executing min-max normalization."""
        data = pd.DataFrame({
            "col": [0.0, 25.0, 50.0, 75.0, 100.0]
        })

        transformation = Transformation(
            id="test-4",
            type=TransformationType.NORMALIZE,
            target_columns=["col"],
            params={"method": "minmax"},
            reversible=True,
            description="Min-max normalize",
        )

        result = self.executor.execute(data, transformation)

        assert result.success is True
        assert result.output_data["col"].min() == 0.0
        assert result.output_data["col"].max() == 1.0

    def test_execute_encode_label(self):
        """Test executing label encoding."""
        data = pd.DataFrame({
            "col": ["a", "b", "c", "a", "b"]
        })

        transformation = Transformation(
            id="test-5",
            type=TransformationType.ENCODE_CATEGORICAL,
            target_columns=["col"],
            params={"method": "label"},
            reversible=True,
            description="Label encode",
        )

        result = self.executor.execute(data, transformation)

        assert result.success is True
        # Should have integer values
        assert result.output_data["col"].dtype in [np.int64, np.int32, int]

    def test_execute_drop_duplicates(self):
        """Test executing drop duplicates."""
        data = pd.DataFrame({
            "col": [1, 2, 2, 3, 3]
        })

        transformation = Transformation(
            id="test-6",
            type=TransformationType.DROP_DUPLICATES,
            target_columns=[],
            params={},
            reversible=False,
            description="Drop duplicates",
        )

        result = self.executor.execute(data, transformation)

        assert result.success is True
        assert len(result.output_data) == 3

    def test_reverse_normalize(self):
        """Test reversing normalization."""
        data = pd.DataFrame({
            "col": [1.0, 2.0, 3.0, 4.0, 5.0]
        })

        transformation = Transformation(
            id="test-7",
            type=TransformationType.NORMALIZE,
            target_columns=["col"],
            params={"method": "standard"},
            reversible=True,
            description="Standard normalize",
        )

        # Execute transformation
        exec_result = self.executor.execute(data, transformation)
        normalized_data = exec_result.output_data

        # Reverse transformation
        reversed_data = self.executor.reverse(normalized_data, transformation)

        # Should be close to original
        pd.testing.assert_frame_equal(
            reversed_data.reset_index(drop=True),
            data.reset_index(drop=True),
            check_dtype=False,
            rtol=1e-10
        )

    def test_can_reverse(self):
        """Test can_reverse method."""
        reversible_trans = Transformation(
            id="test-8",
            type=TransformationType.NORMALIZE,
            target_columns=["col"],
            params={"method": "standard"},
            reversible=True,
            description="Normalize",
        )

        irreversible_trans = Transformation(
            id="test-9",
            type=TransformationType.DROP_DUPLICATES,
            target_columns=[],
            params={},
            reversible=False,
            description="Drop duplicates",
        )

        assert self.executor.can_reverse(reversible_trans) is True
        assert self.executor.can_reverse(irreversible_trans) is False

    def test_execute_sequence(self):
        """Test executing a sequence of transformations."""
        data = pd.DataFrame({
            "num_col": [1.0, 2.0, np.nan, 4.0, 5.0],
            "cat_col": ["a", "b", "a", "b", "c"]
        })

        transformations = [
            Transformation(
                id="seq-1",
                type=TransformationType.FILL_MISSING,
                target_columns=["num_col"],
                params={"strategy": "mean"},
                reversible=True,
                description="Fill missing",
            ),
            Transformation(
                id="seq-2",
                type=TransformationType.NORMALIZE,
                target_columns=["num_col"],
                params={"method": "standard"},
                reversible=True,
                description="Normalize",
            ),
        ]

        results = self.executor.execute_sequence(data, transformations)

        assert len(results) == 2
        assert all(r.success for r in results)
        # Second transformation should use output from first
        assert results[1].output_data["num_col"].std() == pytest.approx(1.0, rel=1e-10)

    def test_execute_cast_type(self):
        """Test executing cast type transformation."""
        data = pd.DataFrame({
            "col": ["2023-01-01", "2023-01-02", "2023-01-03"]
        })

        transformation = Transformation(
            id="test-11",
            type=TransformationType.CAST_TYPE,
            target_columns=["col"],
            params={"target_type": "datetime"},
            reversible=True,
            description="Cast to datetime",
        )

        result = self.executor.execute(data, transformation)

        assert result.success is True
        # Should be datetime type
        assert pd.api.types.is_datetime64_any_dtype(result.output_data["col"])
