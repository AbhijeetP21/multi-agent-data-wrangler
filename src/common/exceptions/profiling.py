"""Profiling exceptions."""

from .base import DataWranglerError


class ProfilingError(DataWranglerError):
    """Exception raised during data profiling operations."""

    def __init__(self, message: str, details: dict | None = None):
        """Initialize the profiling error.

        Args:
            message: Error message
            details: Optional additional error details
        """
        super().__init__(message, details)
