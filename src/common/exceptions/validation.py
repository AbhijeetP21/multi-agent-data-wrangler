"""Validation exceptions."""

from .base import DataWranglerError


class ValidationError(DataWranglerError):
    """Exception raised during data validation operations."""

    def __init__(self, message: str, details: dict | None = None):
        """Initialize the validation error.

        Args:
            message: Error message
            details: Optional additional error details
        """
        super().__init__(message, details)
