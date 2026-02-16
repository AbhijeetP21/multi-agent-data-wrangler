"""Transformation exceptions."""

from .base import DataWranglerError


class TransformationError(DataWranglerError):
    """Exception raised during data transformation operations."""

    def __init__(self, message: str, details: dict | None = None):
        """Initialize the transformation error.

        Args:
            message: Error message
            details: Optional additional error details
        """
        super().__init__(message, details)
