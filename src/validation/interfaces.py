"""Validation module interfaces."""

import pandas as pd
from typing import Protocol

from src.common.types import DataProfile, ValidationResult


class ValidationEngine(Protocol):
    """Protocol for validation engines."""

    def validate(
        self,
        original: pd.DataFrame,
        transformed: pd.DataFrame,
        profile: DataProfile,
    ) -> ValidationResult:
        """
        Validate transformed data against original.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame
            profile: Data profile of original

        Returns:
            ValidationResult with issues and schema compatibility
        """
        ...

    def check_leakage(self, original: pd.DataFrame, transformed: pd.DataFrame) -> bool:
        """
        Check if there is information leakage from original to transformed.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame

        Returns:
            True if leakage detected, False otherwise
        """
        ...
