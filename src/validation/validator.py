"""Main validation service module."""

import pandas as pd

from src.common.types import DataProfile, ValidationResult
from src.validation.integrity_validator import IntegrityValidator
from src.validation.leakage_detector import LeakageDetector
from src.validation.schema_validator import SchemaValidator
from src.validation.interfaces import ValidationEngine


class ValidationService(ValidationEngine):
    """Main validation service that coordinates all validation checks."""

    def __init__(
        self,
        row_count_tolerance: float = 0.1,
        leakage_threshold: float = 0.1,
    ) -> None:
        """
        Initialize the validation service.

        Args:
            row_count_tolerance: Maximum allowed ratio of rows lost (default 10%)
            leakage_threshold: Threshold for leakage detection
        """
        self.integrity_validator = IntegrityValidator(row_count_tolerance)
        self.leakage_detector = LeakageDetector(leakage_threshold)
        self.schema_validator = SchemaValidator()

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
        all_issues = []

        # Run integrity validation
        integrity_issues = self.integrity_validator.validate_all(
            original, transformed, profile
        )
        all_issues.extend(integrity_issues)

        # Run leakage detection
        _, leakage_issues = self.leakage_detector.check_leakage(
            original, transformed, profile
        )
        all_issues.extend(leakage_issues)

        # Run schema validation
        schema_compatible, schema_issues = self.schema_validator.validate_schema_compatibility(
            original, transformed, profile
        )
        all_issues.extend(schema_issues)

        # Determine if validation passed (no error-level issues)
        has_errors = any(issue.severity == "error" for issue in all_issues)
        passed = not has_errors

        return ValidationResult(
            passed=passed,
            issues=all_issues,
            original_row_count=len(original),
            transformed_row_count=len(transformed),
            schema_compatible=schema_compatible,
        )

    def check_leakage(self, original: pd.DataFrame, transformed: pd.DataFrame) -> bool:
        """
        Check if there is information leakage from original to transformed.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame

        Returns:
            True if leakage detected, False otherwise
        """
        leakage_detected, _ = self.leakage_detector.check_leakage(original, transformed)
        return leakage_detected
