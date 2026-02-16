"""Ranking exceptions."""

from .base import DataWranglerError


class RankingError(DataWranglerError):
    """Exception raised during transformation ranking operations."""

    def __init__(self, message: str, details: dict | None = None):
        """Initialize the ranking error.

        Args:
            message: Error message
            details: Optional additional error details
        """
        super().__init__(message, details)
