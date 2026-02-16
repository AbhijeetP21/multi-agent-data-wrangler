"""Base transformation class."""

from abc import ABC, abstractmethod
from typing import Any, Optional

import pandas as pd

from src.common.types.transformation import Transformation


class BaseTransformation(ABC):
    """Abstract base class for all transformations."""

    def __init__(self, transformation: Transformation):
        """Initialize the transformation.

        Args:
            transformation: The Transformation object containing parameters.
        """
        self.transformation = transformation
        self._metadata: dict[str, Any] = {}

    @abstractmethod
    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply the transformation to the data.

        Args:
            data: Input DataFrame.

        Returns:
            Transformed DataFrame.
        """
        pass

    @abstractmethod
    def reverse(self, data: pd.DataFrame) -> pd.DataFrame:
        """Reverse the transformation if possible.

        Args:
            data: Transformed DataFrame.

        Returns:
            Original DataFrame if reversible, otherwise raises an error.
        """
        pass

    @property
    def reversible(self) -> bool:
        """Check if the transformation is reversible."""
        return self.transformation.reversible

    @property
    def target_columns(self) -> list[str]:
        """Get the target columns for this transformation."""
        return self.transformation.target_columns

    @property
    def params(self) -> dict[str, Any]:
        """Get the transformation parameters."""
        return self.transformation.params

    def get_metadata(self) -> dict[str, Any]:
        """Get transformation metadata for tracking."""
        return self._metadata.copy()

    def set_metadata(self, key: str, value: Any) -> None:
        """Set transformation metadata."""
        self._metadata[key] = value
