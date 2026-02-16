"""Validation types."""

from pydantic import BaseModel
from typing import Optional, Literal


class ValidationIssue(BaseModel):
    """Represents a validation issue found during data validation."""

    severity: Literal["error", "warning", "info"]
    code: str
    message: str
    column: Optional[str] = None


class ValidationResult(BaseModel):
    """Result of validating transformed data."""

    passed: bool
    issues: list[ValidationIssue]
    original_row_count: int
    transformed_row_count: int
    schema_compatible: bool
