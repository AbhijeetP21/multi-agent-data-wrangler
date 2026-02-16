"""Reversibility checking for transformations."""

from src.common.types.transformation import Transformation, TransformationType


class ReversibilityChecker:
    """Check if transformations can be reversed."""

    # Mapping of transformation types to their reversibility
    REVERSIBLE_TYPES = {
        TransformationType.NORMALIZE,
        TransformationType.ENCODE_CATEGORICAL,
        TransformationType.CAST_TYPE,
    }

    # Types that are not reversible
    IRREVERSIBLE_TYPES = {
        TransformationType.REMOVE_OUTLIERS,
        TransformationType.DROP_DUPLICATES,
    }

    # Types that may be reversible depending on parameters
    CONDITIONAL_REVERSIBLE_TYPES = {
        TransformationType.FILL_MISSING,
    }

    @classmethod
    def is_reversible(cls, transformation: Transformation) -> bool:
        """Check if a transformation is reversible.

        Args:
            transformation: The transformation to check.

        Returns:
            True if the transformation is reversible.
        """
        if transformation.type in cls.REVERSIBLE_TYPES:
            return True

        if transformation.type in cls.IRREVERSIBLE_TYPES:
            return False

        if transformation.type in cls.CONDITIONAL_REVERSIBLE_TYPES:
            return cls._check_conditional_reversibility(transformation)

        return transformation.reversible

    @classmethod
    def _check_conditional_reversibility(
        cls, transformation: Transformation
    ) -> bool:
        """Check conditional reversibility for specific transformation types.

        Args:
            transformation: The transformation to check.

        Returns:
            True if conditionally reversible.
        """
        if transformation.type == TransformationType.FILL_MISSING:
            # Fill missing with constant is technically reversible
            # (we know the fill value), but mean/median is not
            strategy = transformation.params.get("strategy", "")
            return strategy == "constant"

        return transformation.reversible

    @classmethod
    def get_reversibility_reason(
        cls, transformation: Transformation
    ) -> str:
        """Get a human-readable reason for reversibility status.

        Args:
            transformation: The transformation to check.

        Returns:
            Explanation of why the transformation is or isn't reversible.
        """
        if transformation.type in cls.REVERSIBLE_TYPES:
            return f"{transformation.type.value} transformations are reversible"

        if transformation.type == TransformationType.REMOVE_OUTLIERS:
            return "Outlier removal permanently removes rows"

        if transformation.type == TransformationType.DROP_DUPLICATES:
            return "Duplicate removal permanently removes rows"

        if transformation.type == TransformationType.FILL_MISSING:
            strategy = transformation.params.get("strategy", "")
            if strategy == "constant":
                return "Constant fill is reversible (fill value is known)"
            return f"Fill with {strategy} is not reversible (original values lost)"

        if transformation.reversible:
            return "Transformation is marked as reversible"

        return "Transformation is not reversible"


class ReversibilityTracker:
    """Track transformation history for reversal."""

    def __init__(self):
        """Initialize the tracker."""
        self._history: list[Transformation] = []
        self._metadata_history: list[dict] = []

    def record(self, transformation: Transformation, metadata: dict) -> None:
        """Record a transformation and its metadata.

        Args:
            transformation: The transformation that was applied.
            metadata: The transformation metadata.
        """
        self._history.append(transformation)
        self._metadata_history.append(metadata)

    def can_reverse_last(self) -> bool:
        """Check if the last transformation can be reversed.

        Returns:
            True if the last transformation is reversible.
        """
        if not self._history:
            return False

        return ReversibilityChecker.is_reversible(self._history[-1])

    def get_last_transformation(self) -> Transformation:
        """Get the last transformation.

        Returns:
            The last transformation.

        Raises:
            IndexError: If there's no transformation history.
        """
        if not self._history:
            raise IndexError("No transformations in history")
        return self._history[-1]

    def get_last_metadata(self) -> dict:
        """Get metadata for the last transformation.

        Returns:
            The last transformation metadata.

        Raises:
            IndexError: If there's no transformation history.
        """
        if not self._metadata_history:
            raise IndexError("No transformations in history")
        return self._metadata_history[-1]

    def pop(self) -> tuple[Transformation, dict]:
        """Remove and return the last transformation and its metadata.

        Returns:
            Tuple of (transformation, metadata).
        """
        if not self._history:
            raise IndexError("No transformations in history")

        transformation = self._history.pop()
        metadata = self._metadata_history.pop()
        return transformation, metadata

    def clear(self) -> None:
        """Clear the transformation history."""
        self._history.clear()
        self._metadata_history.clear()

    def __len__(self) -> int:
        """Get the number of recorded transformations."""
        return len(self._history)

    def __bool__(self) -> bool:
        """Check if there's any transformation history."""
        return bool(self._history)
