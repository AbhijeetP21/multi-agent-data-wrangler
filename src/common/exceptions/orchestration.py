"""Orchestration exceptions."""

from .base import DataWranglerError


class OrchestrationError(DataWranglerError):
    """Exception raised during pipeline orchestration operations."""

    def __init__(self, message: str, details: dict | None = None):
        """Initialize the orchestration error.

        Args:
            message: Error message
            details: Optional additional error details
        """
        super().__init__(message, details)
