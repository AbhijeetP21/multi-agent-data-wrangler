"""Scoring exceptions."""

from .base import DataWranglerError


class ScoringError(DataWranglerError):
    """Exception raised during quality scoring operations."""

    def __init__(self, message: str, details: dict | None = None):
        """Initialize the scoring error.

        Args:
            message: Error message
            details: Optional additional error details
        """
        super().__init__(message, details)
