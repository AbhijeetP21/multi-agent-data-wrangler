"""Transformation executor."""

import time
from typing import Any

import pandas as pd

from src.common.types.transformation import Transformation, TransformationResult, TransformationType
from src.transformation.transformations import (
    FillMissingTransformation,
    NormalizeTransformation,
    EncodeCategoricalTransformation,
    RemoveOutliersTransformation,
    DropDuplicatesTransformation,
    CastTypeTransformation,
)


class TransformationExecutor:
    """Executes transformations on DataFrames."""

    def __init__(self):
        """Initialize the executor."""
        self._transformation_registry: dict[TransformationType, type] = {
            TransformationType.FILL_MISSING: FillMissingTransformation,
            TransformationType.NORMALIZE: NormalizeTransformation,
            TransformationType.ENCODE_CATEGORICAL: EncodeCategoricalTransformation,
            TransformationType.REMOVE_OUTLIERS: RemoveOutliersTransformation,
            TransformationType.DROP_DUPLICATES: DropDuplicatesTransformation,
            TransformationType.CAST_TYPE: CastTypeTransformation,
        }
        # Store transformation instances to preserve metadata
        self._transformation_instances: dict[str, Any] = {}

    def execute(
        self, data: pd.DataFrame, transformation: Transformation
    ) -> TransformationResult:
        """Execute a transformation on the given data.

        Args:
            data: Input DataFrame.
            transformation: The transformation to execute.

        Returns:
            TransformationResult with the transformed data.
        """
        start_time = time.time()

        try:
            # Get the appropriate transformation class
            transformation_class = self._transformation_registry.get(transformation.type)

            if transformation_class is None:
                raise ValueError(f"Unknown transformation type: {transformation.type}")

            # Create transformation instance
            transformer = transformation_class(transformation)

            # Apply transformation
            output_data = transformer.apply(data)

            # Store the transformer instance for later reversal
            self._transformation_instances[transformation.id] = transformer

            execution_time = (time.time() - start_time) * 1000  # Convert to ms

            return TransformationResult(
                transformation=transformation,
                success=True,
                output_data=output_data,
                execution_time_ms=execution_time,
            )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000

            return TransformationResult(
                transformation=transformation,
                success=False,
                output_data=data,  # Return original data on failure
                error_message=str(e),
                execution_time_ms=execution_time,
            )

    def reverse(
        self, data: pd.DataFrame, transformation: Transformation
    ) -> pd.DataFrame:
        """Reverse a transformation if possible.

        Args:
            data: Transformed DataFrame.
            transformation: The transformation to reverse.

        Returns:
            Original DataFrame if reversible.

        Raises:
            NotImplementedError: If the transformation is not reversible.
            ValueError: If the transformation type is unknown.
        """
        if not transformation.reversible:
            raise NotImplementedError(
                f"Transformation {transformation.type} is not reversible"
            )

        # Get the stored transformation instance which has the metadata
        transformer = self._transformation_instances.get(transformation.id)

        if transformer is None:
            # Fallback: create a new instance if not stored
            transformation_class = self._transformation_registry.get(transformation.type)

            if transformation_class is None:
                raise ValueError(f"Unknown transformation type: {transformation.type}")

            transformer = transformation_class(transformation)

        # Reverse transformation
        return transformer.reverse(data)

    def execute_sequence(
        self, data: pd.DataFrame, transformations: list[Transformation]
    ) -> list[TransformationResult]:
        """Execute a sequence of transformations.

        Args:
            data: Input DataFrame.
            transformations: List of transformations to execute in order.

        Returns:
            List of TransformationResults.
        """
        results = []
        current_data = data

        for transformation in transformations:
            result = self.execute(current_data, transformation)
            results.append(result)

            if result.success:
                current_data = result.output_data
            else:
                # Stop execution on failure
                break

        return results

    def can_reverse(self, transformation: Transformation) -> bool:
        """Check if a transformation can be reversed.

        Args:
            transformation: The transformation to check.

        Returns:
            True if the transformation is reversible.
        """
        return transformation.reversible
