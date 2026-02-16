"""Base exception for the data wrangler."""


class DataWranglerError(Exception):
    """Base exception for all data wrangler errors.

    All custom exceptions in the data wrangler should inherit from this class.
    """

    def __init__(self, message: str, details: dict | None = None):
        """Initialize the exception.

        Args:
            message: Error message
            details: Optional additional error details
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        """Return string representation of the error."""
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({details_str})"
        return self.message
