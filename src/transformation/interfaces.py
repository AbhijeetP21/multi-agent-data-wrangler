"""Transformation module interfaces."""

import pandas as pd
from typing import Protocol

from src.common.types.data_profile import DataProfile
from src.common.types.transformation import Transformation, TransformationResult


class TransformationEngine(Protocol):
    """Protocol defining the transformation engine interface."""

    def generate_candidates(self, profile: DataProfile) -> list[Transformation]:
        """Generate transformation candidates based on data profile."""
        ...

    def execute(
        self, data: pd.DataFrame, transformation: Transformation
    ) -> TransformationResult:
        """Execute a transformation on the given data."""
        ...

    def reverse(
        self, data: pd.DataFrame, transformation: Transformation
    ) -> pd.DataFrame:
        """Reverse a transformation if possible."""
        ...
